# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Minimal in-cluster Kubernetes API access, and Job-status interpretation.

**No new dependency.** ``httpx`` is already core, so this talks to the API server
directly rather than pulling in a client library whose object model would then
have to be kept in sync with the manifests Eyvan actually sends.

**The bearer token is re-read on every call.** Projected service-account tokens
rotate -- typically hourly -- and a client that cached the file contents at
construction works for exactly one rotation period and then fails in a way that
looks like an unrelated permissions problem.

**A Job is never replaced.** Several fields of a Job spec are immutable, so a
``replace`` of a live Job returns 422 rather than converging. The lifecycle is
create-if-absent, then a single-field patch guarded by a ``resourceVersion``
precondition, then delete.
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass
from pathlib import Path

import httpx

from eyvan._json import JsonObject, is_json_object
from eyvan.capacity import CapacityStatus
from eyvan.drivers.base import CapacityProviderError, CapacityProviderFailureCode

#: Where a projected service-account token is mounted in a pod by default.
DEFAULT_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
DEFAULT_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


@dataclass(frozen=True, slots=True)
class JobStatus:
    """What the control plane needs to know about one cell's Job."""

    exists: bool
    #: Pods that have reported Ready. Endpoints are published only when this
    #: equals ``parallelism``; publishing early makes the readiness probe resolve
    #: a nonexistent DNS name and the cell flaps HEALTHY <-> EXECUTION_CONNECTING.
    ready: int = 0
    active: int = 0
    failed: int = 0
    parallelism: int = 0
    suspended: bool = False
    #: Set when Kueue refused or evicted the workload, with its own wording kept
    #: so an operator can find it in Kueue's own logs.
    kueue_reason: str = ""
    generation_label: str = ""
    fence_fingerprint: str = ""
    resource_version: str = ""


def read_token(path: str = DEFAULT_TOKEN_PATH) -> str:
    """Return the current service-account bearer token.

    Raises:
        CapacityProviderError: If the token is unreadable, which is a
            configuration failure rather than a transient one -- a pod without a
            mounted token will never grow one.
    """

    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise CapacityProviderError(
            f"Kubernetes service-account token is unreadable at {path}: {exc}",
            code=CapacityProviderFailureCode.CONFIGURATION,
            retryable=False,
        ) from exc


def interpret_job(payload: JsonObject | None, *, expected_fingerprint: str = "") -> JobStatus:
    """Map a Job resource to the fields the driver reasons about.

    ``None`` means the Job is absent, which is a legitimate observation rather
    than an error: it is what ``release`` produces and what ``apply`` starts from.
    """

    if payload is None:
        return JobStatus(exists=False)
    metadata = payload.get("metadata") if is_json_object(payload.get("metadata")) else {}
    spec = payload.get("spec") if is_json_object(payload.get("spec")) else {}
    status = payload.get("status") if is_json_object(payload.get("status")) else {}
    assert isinstance(metadata, dict) and isinstance(spec, dict) and isinstance(status, dict)
    labels = metadata.get("labels") if is_json_object(metadata.get("labels")) else {}
    annotations = metadata.get("annotations") if is_json_object(metadata.get("annotations")) else {}
    assert isinstance(labels, dict) and isinstance(annotations, dict)
    del expected_fingerprint  # compared by the driver, which owns the durable record
    return JobStatus(
        exists=True,
        ready=_count(status.get("ready")),
        active=_count(status.get("active")),
        failed=_count(status.get("failed")),
        parallelism=_count(spec.get("parallelism")),
        suspended=spec.get("suspend") is True,
        kueue_reason=_kueue_reason(status),
        generation_label=str(labels.get("eyvan.dev/provider-generation", "")),
        fence_fingerprint=str(annotations.get("eyvan.dev/fence-fingerprint", "")),
        resource_version=str(metadata.get("resourceVersion", "")),
    )


def _count(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _kueue_reason(status: tp.Mapping[str, object]) -> str:
    """Return Kueue's own reason string when it refused or evicted the workload."""

    conditions = status.get("conditions")
    if not isinstance(conditions, list):
        return ""
    for condition in conditions:
        if not is_json_object(condition):
            continue
        reason = str(condition.get("reason", ""))
        if reason in {"Inadmissible", "Evicted", "Preempted"} and condition.get("status") == "True":
            return reason
    return ""


def capacity_state(status: JobStatus, *, provisioning: bool = True) -> tuple[CapacityStatus, str]:
    """Map one Job's status to an Eyvan capacity state and a human detail.

    A suspended Job is WAITING rather than PROVISIONING: it is queued behind
    Kueue's admission and nothing is being built yet, so ``queue_timeout_s``
    applies to it exactly as it does to a queued TPU resource.

    A failed Job with ``backoffLimit: 0`` is NEEDS_RECREATE, not UNAVAILABLE:
    nothing will retry it, so the only path forward is a new incarnation with a
    fresh generation.
    """

    del provisioning
    if not status.exists:
        return CapacityStatus.ABSENT, "job does not exist"
    if status.kueue_reason == "Inadmissible":
        return CapacityStatus.WAITING, "kueue reports the workload is inadmissible"
    if status.kueue_reason in {"Evicted", "Preempted"}:
        return CapacityStatus.UNAVAILABLE, f"kueue {status.kueue_reason.lower()} the workload"
    if status.suspended:
        return CapacityStatus.WAITING, "job is suspended pending kueue admission"
    if status.failed > 0:
        return CapacityStatus.NEEDS_RECREATE, f"{status.failed} pod(s) failed with backoffLimit 0"
    if status.parallelism and status.ready >= status.parallelism:
        return CapacityStatus.HEALTHY, f"{status.ready}/{status.parallelism} workers ready"
    return CapacityStatus.PROVISIONING, f"{status.ready}/{status.parallelism or '?'} workers ready"


class KubernetesApi:
    """Thin request wrapper over the in-cluster Kubernetes API."""

    def __init__(
        self,
        base_url: str = "https://kubernetes.default.svc",
        *,
        token_path: str = DEFAULT_TOKEN_PATH,
        ca_path: str = DEFAULT_CA_PATH,
        client: httpx.Client | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.token_path = token_path
        self._client = client or httpx.Client(verify=ca_path if Path(ca_path).exists() else True, timeout=30.0)

    def _headers(self, extra: tp.Mapping[str, str] | None = None) -> dict[str, str]:
        return {"Authorization": f"Bearer {read_token(self.token_path)}", **(extra or {})}

    def request(
        self,
        method: str,
        path: str,
        *,
        json: JsonObject | None = None,
        params: tp.Mapping[str, str] | None = None,
        content_type: str | None = None,
    ) -> httpx.Response:
        """Send one API request, translating transport failures into driver errors."""

        headers = self._headers({"Content-Type": content_type} if content_type else None)
        try:
            return self._client.request(
                method, f"{self.base_url}{path}", json=json, params=dict(params or {}), headers=headers
            )
        except httpx.HTTPError as exc:
            # Retryable: an API server that is briefly unreachable is the ordinary
            # state of a cluster during an upgrade, and discarding the command
            # would drop capacity the control plane believes it asked for.
            raise CapacityProviderError(
                f"Kubernetes API request failed: {exc}",
                code=CapacityProviderFailureCode.PROVIDER_UNAVAILABLE,
                retryable=True,
            ) from exc

    def get_job(self, namespace: str, name: str) -> JsonObject | None:
        """Return one Job, or ``None`` when it does not exist."""

        response = self.request("GET", f"/apis/batch/v1/namespaces/{namespace}/jobs/{name}")
        if response.status_code == 404:
            return None
        payload = self._decode(response)
        return payload if is_json_object(payload) else None

    def create(self, path: str, manifest: JsonObject) -> bool:
        """Create one object, reporting whether it was newly created.

        A 409 is *not* an error. The names are deterministic, so an existing
        object with the same name is this cell's own previous create -- exactly
        the replay an at-least-once outbox produces.
        """

        response = self.request("POST", path, json=manifest)
        if response.status_code == 409:
            return False
        self._decode(response)
        return True

    def delete(self, path: str) -> None:
        """Delete one object with foreground propagation.

        Foreground so the Job's pods are gone before the Job is: a background
        delete returns while pods are still running, and the control plane would
        believe the capacity was freed while it is still occupied.
        """

        response = self.request("DELETE", path, json={"propagationPolicy": "Foreground"})
        if response.status_code == 404:
            return
        self._decode(response)

    @staticmethod
    def _decode(response: httpx.Response) -> object:
        if response.status_code >= 400:
            retryable = response.status_code >= 500 or response.status_code == 429
            raise CapacityProviderError(
                f"Kubernetes API returned {response.status_code}: {response.text[:400]}",
                code=(
                    CapacityProviderFailureCode.PROVIDER_UNAVAILABLE
                    if retryable
                    else CapacityProviderFailureCode.CONFIGURATION
                ),
                retryable=retryable,
            )
        try:
            return response.json()
        except ValueError:
            return {}


__all__ = (
    "DEFAULT_CA_PATH",
    "DEFAULT_TOKEN_PATH",
    "JobStatus",
    "KubernetesApi",
    "capacity_state",
    "interpret_job",
    "read_token",
)
