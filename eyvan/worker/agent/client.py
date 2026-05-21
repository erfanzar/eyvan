# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""HTTP client for the versioned cell-agent contract."""

from __future__ import annotations

import typing as tp

import httpx
from pydantic import ValidationError

from eyvan._internal.json_types import JsonObject, is_json_object
from eyvan.capacity.drivers.base import CapacityProviderFailureCode
from eyvan.worker.agent.models import (
    AgentCapabilities,
    AgentObservation,
    CapacityProviderFailure,
    CapacityRequest,
    CheckpointRequest,
    GangWorkloadMutationRequest,
    GangWorkloadRequest,
    MutationConflict,
    MutationConflictCode,
    WorkloadMutationRequest,
    WorkloadObservation,
    WorkloadRequest,
)


class CellAgentBusyError(RuntimeError):
    """The cell agent cannot acquire its shared mutation lease yet."""


class CellAgentStaleMutationError(RuntimeError):
    """The cell agent rejected an obsolete generation or fencing token."""


class CellAgentUnsupportedCapabilityError(RuntimeError):
    """The cell agent's backend does not implement the requested operation.

    Terminal, not transient. Without this the agent's deliberate 501 arrived as a
    generic ``HTTPStatusError``, indistinguishable from the agent being broken --
    so "this backend cannot run checkpoint hooks" was retried on every tick and
    spent the workload's retry budget on an answer that will never change.
    """


class CellAgentCapacityProviderError(RuntimeError):
    """The cell agent returned an operator-safe capacity-provider failure."""

    def __init__(self, detail: str, *, code: CapacityProviderFailureCode) -> None:
        self.code = code
        super().__init__(detail)


class CellAgentInvalidConfigurationError(RuntimeError):
    """The agent refused a configuration that can never succeed as written.

    Terminal, like :class:`CellAgentUnsupportedCapabilityError` and for the same
    reason: retrying an unchanged provider option cannot start working. Carrying
    the agent's own sentence matters more here than anywhere else, because the
    fix is a line of YAML and the message names it exactly.
    """


class CellAgentUnavailableError(RuntimeError):
    """The cell agent could not answer, so nothing was learned about the cell.

    The mirror of :class:`CellAgentUnsupportedCapabilityError`: that one exists so
    a deliberate answer is not read as the agent being broken, and this one exists
    so the agent being broken is not read as an answer.

    An unstructured 5xx means the agent itself failed -- its own storage backend
    was unreachable, it is restarting, a proxy is in the way. None of that is a
    statement about the *capacity*, and treating it as one is expensive: the
    control plane records ``CapacityStatus.PROVIDER_ERROR``, restarts the provider
    phase clock, and feeds ``ProvisionOutcome.PROVIDER_ERROR`` into the zone's
    belief -- retiring capacity for a timeout the provider never caused and
    biasing future placement against a zone that did nothing wrong.

    Observed live on 2026-08-01: one transient DNS failure on the control plane's
    own host, while the agent was reading its fleet state, discarded a v5p-16 Spot
    Multislice that Google had just granted after a 100-minute queue wait.

    A structured 502 (:class:`CellAgentCapacityProviderError`) is the opposite
    case and keeps its own handling -- there the agent successfully reported that
    the *provider* failed.
    """


def _raise_for_status(response: httpx.Response) -> None:
    if response.status_code == 409:
        try:
            conflict = MutationConflict.model_validate_json(response.content)
        except (ValidationError, ValueError):
            response.raise_for_status()
            return
        if conflict.code is MutationConflictCode.DRIVER_BUSY:
            raise CellAgentBusyError(conflict.detail)
        if conflict.code is MutationConflictCode.STALE_MUTATION:
            raise CellAgentStaleMutationError(conflict.detail)
    if response.status_code == 400:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if isinstance(payload, dict) and payload.get("code") == "invalid_configuration":
            raise CellAgentInvalidConfigurationError(str(payload.get("detail") or "").strip() or "invalid configuration")
    if response.status_code == 501:
        detail = str(response.text or "").strip() or "the cell agent backend does not support this operation"
        raise CellAgentUnsupportedCapabilityError(detail)
    if response.status_code == 502:
        try:
            failure = CapacityProviderFailure.model_validate_json(response.content)
        except (ValidationError, ValueError):
            response.raise_for_status()
            return
        raise CellAgentCapacityProviderError(failure.detail, code=failure.code)
    if response.status_code >= 500:
        # Everything the agent says deliberately has already been handled above,
        # so anything left in the 5xx range is the agent failing to answer.
        detail = str(response.text or "").strip() or f"the cell agent returned {response.status_code}"
        raise CellAgentUnavailableError(detail)
    response.raise_for_status()


class CellAgentClient:
    """Control-plane client for one mTLS-fronted cell agent."""

    def __init__(self, base_url: str, *, token: str | None = None, client: httpx.Client | None = None):
        headers = {"Authorization": f"Bearer {token}"} if token else None
        self.client = client or httpx.Client(base_url=base_url, headers=headers, timeout=30.0)

    def _send(self, method: str, url: str, **kwargs: tp.Any) -> httpx.Response:
        """Issue one request, mapping "could not ask" onto one typed error.

        A 5xx is a *response*, so `_raise_for_status` can classify it. A dropped
        connection, a DNS failure or a read timeout produces no response at all
        -- httpx raises before `_raise_for_status` is ever reached -- so those
        used to escape as bare transport exceptions and be caught by the
        controller's `except Exception`, which records them as
        `CapacityStatus.PROVIDER_ERROR`.

        That is the same misclassification :class:`CellAgentUnavailableError`
        exists to prevent, arriving by a different door. Observed live on
        2026-08-01: restarting the agent recorded "Server disconnected without
        sending a response" as a provider error against a healthy queued cell.

        Routing every call through here keeps the taxonomy in one place instead
        of twelve `except` clauses that will drift.
        """

        try:
            response = self.client.request(method, url, **kwargs)
        except httpx.TransportError as exc:
            raise CellAgentUnavailableError(f"the cell agent could not be reached: {exc}") from exc
        _raise_for_status(response)
        return response

    def capabilities(self) -> AgentCapabilities:
        response = self._send("GET", "/api/v1/capabilities")
        return AgentCapabilities.model_validate_json(response.content)

    def inventory(self) -> list[dict[str, object]]:
        """Return every cell this agent holds durable state for."""

        response = self._send("GET", "/api/v1/cells")
        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError("cell agent inventory must be a JSON array")
        return payload

    def observe(self, cell_id: str) -> AgentObservation:
        response = self._send("GET", f"/api/v1/cells/{cell_id}")
        return AgentObservation.model_validate(response.json())

    def reconcile(self, cell_id: str, request: WorkloadMutationRequest) -> AgentObservation:
        response = self._send(
            "POST",
            f"/api/v1/cells/{cell_id}/reconcile",
            json=request.model_dump(mode="json"),
            timeout=1800,
        )
        return AgentObservation.model_validate(response.json())

    def apply_capacity(self, cell_id: str, request: CapacityRequest) -> AgentObservation:
        response = self._send(
            "PUT",
            f"/api/v1/cells/{cell_id}/capacity",
            json=request.model_dump(mode="json"),
            timeout=1800,
        )
        return AgentObservation.model_validate(response.json())

    def submit(self, cell_id: str, request: WorkloadRequest) -> WorkloadObservation:
        # Submission holds the cell-agent's durable fence through remote
        # launch. On GCS that requires several CAS calls before the native
        # worker responds, so the generic 30-second HTTP timeout can expire
        # after a job was accepted but before its remote ID is persisted.
        response = self._send(
            "POST",
            f"/api/v1/cells/{cell_id}/workloads",
            json=request.model_dump(mode="json"),
            timeout=1800,
        )
        return WorkloadObservation.model_validate(response.json())

    def submit_gang(self, coordinator_cell_id: str, request: GangWorkloadRequest) -> WorkloadObservation:
        """Submit one workload group through its deterministic leader cell agent."""

        response = self._send(
            "POST",
            f"/api/v1/cells/{coordinator_cell_id}/workload-gangs",
            json=request.model_dump(mode="json"),
            timeout=1800,
        )
        return WorkloadObservation.model_validate(response.json())

    def workload(self, cell_id: str, remote_id: str) -> WorkloadObservation:
        response = self._send("GET", f"/api/v1/cells/{cell_id}/workloads/{remote_id}")
        return WorkloadObservation.model_validate(response.json())

    def gang_workload(
        self,
        coordinator_cell_id: str,
        workload_id: str,
        remote_id: str,
    ) -> WorkloadObservation:
        """Read aggregate status for a previously submitted workload group."""

        response = self._send("GET", f"/api/v1/cells/{coordinator_cell_id}/workload-gangs/{workload_id}/{remote_id}")
        return WorkloadObservation.model_validate(response.json())

    def stop(self, cell_id: str, remote_id: str, request: WorkloadMutationRequest) -> WorkloadObservation:
        response = self._send(
            "POST",
            f"/api/v1/cells/{cell_id}/workloads/{remote_id}/stop",
            json=request.model_dump(mode="json"),
        )
        return WorkloadObservation.model_validate(response.json())

    def stop_gang(
        self,
        coordinator_cell_id: str,
        workload_id: str,
        remote_id: str,
        request: GangWorkloadMutationRequest,
    ) -> WorkloadObservation:
        """Stop every rank in one native workload group."""

        response = self._send(
            "POST",
            f"/api/v1/cells/{coordinator_cell_id}/workload-gangs/{workload_id}/{remote_id}/stop",
            json=request.model_dump(mode="json"),
        )
        return WorkloadObservation.model_validate(response.json())

    def retire(self, cell_id: str, remote_id: str, request: WorkloadMutationRequest) -> WorkloadObservation:
        response = self._send(
            "POST",
            f"/api/v1/cells/{cell_id}/workloads/{remote_id}/retire",
            json=request.model_dump(mode="json"),
            timeout=600,
        )
        return WorkloadObservation.model_validate(response.json())

    def retire_gang(
        self,
        coordinator_cell_id: str,
        workload_id: str,
        remote_id: str,
        request: GangWorkloadMutationRequest,
    ) -> WorkloadObservation:
        """Retire every rank in one native workload group."""

        response = self._send(
            "POST",
            f"/api/v1/cells/{coordinator_cell_id}/workload-gangs/{workload_id}/{remote_id}/retire",
            json=request.model_dump(mode="json"),
            timeout=600,
        )
        return WorkloadObservation.model_validate(response.json())

    def checkpoint(self, cell_id: str, remote_id: str, request: CheckpointRequest) -> WorkloadObservation:
        response = self._send(
            "POST",
            f"/api/v1/cells/{cell_id}/workloads/{remote_id}/checkpoint",
            json=request.model_dump(mode="json"),
        )
        return WorkloadObservation.model_validate(response.json())

    def logs(self, cell_id: str, remote_id: str) -> str:
        response = self._send("GET", f"/api/v1/cells/{cell_id}/workloads/{remote_id}/logs")
        return response.text

    def gang_logs(self, coordinator_cell_id: str, workload_id: str, remote_id: str) -> str:
        """Read rank-prefixed logs from every member of a native workload group."""

        response = self._send(
            "GET", f"/api/v1/cells/{coordinator_cell_id}/workload-gangs/{workload_id}/{remote_id}/logs"
        )
        return response.text

    def exec_in_workload(
        self,
        cell_id: str,
        remote_id: str,
        command: tp.Sequence[str],
        *,
        timeout_s: float = 30.0,
    ) -> JsonObject:
        """Run one diagnostic command beside a live workload on one cell."""

        response = self._send(
            "POST",
            f"/api/v1/cells/{cell_id}/workloads/{remote_id}/exec",
            json={"command": list(command), "timeout_s": timeout_s},
        )
        payload = response.json()
        if not is_json_object(payload):
            raise ValueError("cell agent exec result must be a JSON object")
        return payload

    def log_page(self, cell_id: str, remote_id: str, *, after: str = "", limit: int = 200) -> JsonObject:
        """Read one resumable page of structured log records from one cell."""

        response = self._send(
            "GET",
            f"/api/v1/cells/{cell_id}/workloads/{remote_id}/log-page",
            params={"after": after, "limit": limit},
        )
        payload = response.json()
        if not is_json_object(payload):
            raise ValueError("cell agent log page must be a JSON object")
        return payload

    def process_status(self, cell_id: str, remote_id: str) -> JsonObject:
        """Read live process detail for one workload attempt on one cell."""

        response = self._send("GET", f"/api/v1/cells/{cell_id}/workloads/{remote_id}/process-status")
        payload = response.json()
        if not is_json_object(payload):
            raise ValueError("cell agent process status must be a JSON object")
        return payload

    def drain(self, cell_id: str, request: WorkloadMutationRequest) -> AgentObservation:
        response = self._send("POST", f"/api/v1/cells/{cell_id}/drain", json=request.model_dump(mode="json"))
        return AgentObservation.model_validate(response.json())

    def release(self, cell_id: str, request: WorkloadMutationRequest) -> AgentObservation:
        response = self._send(
            "DELETE",
            f"/api/v1/cells/{cell_id}",
            json=request.model_dump(mode="json"),
            timeout=700,
        )
        return AgentObservation.model_validate(response.json())

    def close(self) -> None:
        self.client.close()
