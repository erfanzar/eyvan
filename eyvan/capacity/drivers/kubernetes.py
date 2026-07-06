# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Kubernetes-backed capacity: one cell is one Indexed Job.

Structurally a near-clone of ``manual.py`` -- the same fencing, the same durable
``FleetStateStore`` record, the same mutation acceptance -- plus calls to the
Kubernetes API. That similarity is the point: the parts that are hard to get
right are backend-neutral, and a second real backend that had to reinvent them
would mean the contracts do not generalize.

**Fencing stays in ``FleetStateStore``.** Kubernetes cannot substitute for it.
``metadata.resourceVersion`` is opaque and bumps on *any* write, including
kubelet status updates, so a compare-and-swap on it produces spurious
``StaleMutationError``s from writes Eyvan did not make. ``metadata.generation``
does not survive recreation. What Kubernetes *does* provide is leak detection: a
generation label and a fencing-token **fingerprint** annotation, so an observed
Job whose fingerprint disagrees with the durable record is a leaked previous
incarnation and gets deleted rather than adopted -- the role ``management_uid``
plays on GCP.

**Kueue coexistence is a boundary, not a compromise.** Two admission controllers
over the same nodes fight twice: Kueue can veto a cell Eyvan already leased, and
Kueue can *evict* an admitted Workload for higher-priority work in another
LocalQueue -- destroying a pod with no checkpoint hook and bypassing Eyvan's
entire ``PreemptionPlan`` / ``CHECKPOINT_AND_STOP`` / ``grace_period_seconds``
machinery. So: **Eyvan delegates capacity admission to Kueue and retains workload
admission, and a ClusterQueue Eyvan uses must be exclusively Eyvan's with
preemption disabled.** The driver verifies that precondition before creating a Job, which is what
turns an invisible correctness hazard into a refusal at the point of use.
``eyvan doctor`` also reports it, but only as a warning: it runs on an
operator's machine and cannot read the cluster.

**Honest cost, stated here and in the README.** Eyvan's budgets and fair share
reason about cells it asked for, not the cluster's real free capacity, so an
Eyvan pool can sit WAITING while the cluster is full of non-Eyvan work.
``CapacityBelief`` will *learn* that -- repeated timeouts raise ``route_priority``
-- but Eyvan will never *plan* around it.
"""

from __future__ import annotations

import base64
import typing as tp
from datetime import datetime
from threading import RLock

from eyvan._internal.compat import UTC
from eyvan._internal.identifiers import stable_id
from eyvan._internal.json_types import JsonObject, JsonValue, is_json_object
from eyvan.capacity import CapacityStatus
from eyvan.capacity.drivers.base import (
    CapacityProviderError,
    CapacityProviderFailureCode,
    CellCapabilities,
    CellObservation,
    MutationContext,
    StaleMutationError,
)
from eyvan.capacity.fleet_state import (
    FleetCellNotRegisteredError,
    FleetCellRecord,
    FleetStateStore,
    StaleFenceError,
)
from eyvan.capacity.providers.kubernetes.api import JobStatus, KubernetesApi, capacity_state, interpret_job
from eyvan.capacity.providers.kubernetes.kueue import inspect_cluster_queue
from eyvan.capacity.providers.kubernetes.workers import (
    API_CLIENT_LABEL,
    CELL_ID_ANNOTATION,
    CELL_LABEL,
    FENCE_ANNOTATION,
    GENERATION_LABEL,
    cell_label_value,
    fence_fingerprint,
    headless_service_manifest,
    job_manifest,
    object_name,
    secret_manifest,
    worker_endpoints,
)

#: Where Kueue publishes its ClusterQueue objects.
_KUEUE_API = "/apis/kueue.x-k8s.io/v1beta1"

#: Most workers one Kubernetes cell may contain. A Job's parallelism is the
#: multislice worker count, and the same ceiling that bounds a TPU multislice
#: bounds this: past it, a single failure unit is too large to reason about.
MAX_WORKERS = 512

_Revalidate = tp.Callable[[str], None]


class KubernetesCapacityDriver:
    """Provision one Indexed Job per Eyvan capacity cell."""

    provider_name = "kubernetes"
    execution_backend = "eyvan-native"
    accelerator_types = ("cpu", "gpu", "tpu")
    capacity_classes = ("on-demand", "reserved", "spot")

    def __init__(
        self,
        state: FleetStateStore | None = None,
        *,
        api: KubernetesApi | None = None,
        namespace: str = "eyvan",
        image: str = "",
        shared_secret: str | None = None,
    ):
        self.state = state or FleetStateStore.from_uri(None)
        self.api = api or KubernetesApi()
        self.namespace = namespace
        self.image = image
        self.shared_secret = shared_secret
        self._mutation_lock = RLock()

    def capabilities(self) -> CellCapabilities:
        """Describe Kubernetes-backed capacity.

        ``resource_enforcement`` is ``"backend"``, and that claim is honest *only
        because of exclusivity*: ``to_core_cell`` sets ``exclusive_workloads`` for
        native executors, so a native cell hosts exactly one workload and the
        pod's cgroup **is** that workload's cgroup. If ``exclusive_workloads``
        ever became false for native cells, this must drop to ``"none"``.
        """

        return CellCapabilities(
            provider=self.provider_name,
            executor=self.execution_backend,
            accelerator_types=self.accelerator_types,
            capacity_classes=self.capacity_classes,
            supports_callables=True,
            supports_checkpoint=True,
            # ``kubectl exec`` exists regardless; routing it through Eyvan is what
            # makes it fenced and audited.
            supports_exec=True,
            resource_enforcement="backend",
        )

    # ---- durable record helpers, shared in shape with manual.py -------------

    @staticmethod
    def _accept_mutation(record: FleetCellRecord, context: MutationContext) -> bool:
        if context.generation < record.generation:
            raise StaleMutationError(
                f"generation {context.generation} is older than cell {record.name!r} generation {record.generation}"
            )
        if (
            context.generation == record.generation
            and record.fencing_token is not None
            and record.fencing_token != context.fencing_token
        ):
            raise StaleMutationError(f"fencing token does not own cell {record.name!r} generation {record.generation}")
        return context.idempotency_key not in record.idempotency_keys

    @staticmethod
    def _stamp(record: FleetCellRecord, context: MutationContext) -> None:
        record.generation = context.generation
        record.fencing_token = context.fencing_token
        record.idempotency_keys = [*record.idempotency_keys, context.idempotency_key][-64:]

    @staticmethod
    def _nested_options(desired: tp.Mapping[str, JsonValue]) -> JsonObject:
        for key in ("provider_config", "provider_options"):
            value = desired.get(key)
            if is_json_object(value):
                return value
        return {}

    @staticmethod
    def _required_string(desired: tp.Mapping[str, JsonValue], name: str) -> str:
        value = desired.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"kubernetes capacity field {name!r} must be a non-empty string")
        return value.strip()

    def _worker_count(self, desired: tp.Mapping[str, JsonValue]) -> int:
        options = self._nested_options(desired)
        raw = desired.get("worker_count", options.get("worker_count", options.get("node_count", 1)))
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 1:
            raise ValueError("kubernetes capacity worker_count must be a positive integer")
        if raw > MAX_WORKERS:
            raise ValueError(f"kubernetes capacity supports at most {MAX_WORKERS} workers per cell")
        return raw

    def _validate_desired(self, desired: tp.Mapping[str, JsonValue]) -> tuple[str, str, int, JsonObject]:
        backend = self._required_string(desired, "capacity_backend")
        if backend not in {"kubernetes", "k8s"}:
            raise ValueError(f"kubernetes capacity requires capacity_backend 'kubernetes', got {backend!r}")
        executor = self._required_string(desired, "executor")
        if executor != self.execution_backend:
            raise ValueError(f"kubernetes capacity requires executor {self.execution_backend!r}, got {executor!r}")
        accelerator_type = self._required_string(desired, "accelerator_type")
        capacity_class = self._required_string(desired, "capacity_class").replace("_", "-")
        if capacity_class not in self.capacity_classes:
            choices = ", ".join(self.capacity_classes)
            raise ValueError(f"unsupported kubernetes capacity class {capacity_class!r}; choose one of {choices}")
        return accelerator_type, capacity_class, self._worker_count(desired), self._nested_options(desired)

    # ---- observation --------------------------------------------------------

    def observe(self, cell_id: str) -> CellObservation:
        """Return the cell's live state, reconciled against the durable record."""

        record = self.state.get(cell_id)
        if record is None:
            return CellObservation(
                cell_id=cell_id,
                state=CapacityStatus.ABSENT,
                generation=0,
                observed_at=datetime.now(UTC),
            )
        payload = self.api.get_job(self.namespace, object_name("eyvan", cell_id))
        status = interpret_job(payload)
        ownership_error = self._job_ownership_error(record, status)
        if ownership_error is not None:
            # Missing metadata is as unsafe as a mismatch. Adopting either would
            # publish endpoints for a Job whose cell generation cannot be proven.
            return self._observation(
                record,
                CapacityStatus.NEEDS_RECREATE,
                ownership_error,
                worker_count=status.parallelism,
                ready=0,
            )
        state, detail = capacity_state(status)
        return self._observation(record, state, detail, worker_count=status.parallelism, ready=status.ready)

    def _job_ownership_error(self, record: FleetCellRecord, status: JobStatus) -> str | None:
        """Return why an observed Job cannot be tied to the durable cell fence."""

        if not status.exists:
            return None
        expected_fingerprint = fence_fingerprint(record.fencing_token) if isinstance(record.fencing_token, str) else ""
        expected = (
            status.api_version == "batch/v1"
            and status.kind == "Job"
            and status.name == object_name("eyvan", record.name)
            and status.namespace == self.namespace
            and bool(status.uid)
            and bool(status.resource_version)
            and status.cell_label == cell_label_value(record.name)
            and status.cell_id_annotation == record.name
            and status.generation_label == str(record.provider_generation)
            and bool(expected_fingerprint)
            and status.fence_fingerprint == expected_fingerprint
        )
        if expected:
            return None
        return "job ownership metadata is missing or does not match the durable cell fence"

    def _observation(
        self,
        record: FleetCellRecord,
        state: CapacityStatus,
        detail: str,
        *,
        worker_count: int,
        ready: int,
    ) -> CellObservation:
        """Build an observation, publishing endpoints only when every worker is ready.

        Mirrors ``drivers/gcp.py:_persist_provider_observation``: publishing early
        makes the agent's readiness probe resolve a DNS name that has no backing
        pod, and the cell flaps HEALTHY <-> EXECUTION_CONNECTING for as long as
        the slowest worker takes to start.
        """

        declared = worker_count or int(record.metadata.get("worker_count", 1) or 1)
        endpoints = worker_endpoints(record.name, self.namespace, declared) if declared and ready >= declared else ()
        metadata: JsonObject = {
            "provider": self.provider_name,
            "capacity_backend": self.provider_name,
            "resource_id": f"kubernetes:{self.namespace}/{object_name('eyvan', record.name)}",
            "desired_state": record.desired_state,
            "provider_generation": record.provider_generation,
            **record.metadata,
            "native_worker_endpoints": list(endpoints),
        }
        return CellObservation(
            cell_id=record.name,
            state=state,
            generation=record.generation,
            execution_endpoint=endpoints[0] if len(endpoints) == 1 else None,
            detail=detail,
            observed_at=datetime.now(UTC),
            metadata=metadata,
            observed_tpu_vms=declared if endpoints else None,
        )

    # ---- lifecycle ----------------------------------------------------------

    @staticmethod
    def _conflict(kind: str, detail: str) -> CapacityProviderError:
        return CapacityProviderError(
            f"Kubernetes {kind} conflict is not a verified Eyvan replay: {detail}",
            code=CapacityProviderFailureCode.CONFIGURATION,
            retryable=False,
        )

    @staticmethod
    def _retry_reconcile(kind: str, detail: str) -> CapacityProviderError:
        return CapacityProviderError(
            f"Kubernetes {kind} reconciliation must be retried: {detail}",
            code=CapacityProviderFailureCode.PROVIDER_UNAVAILABLE,
            retryable=True,
        )

    def _revalidate_record_owner(
        self,
        cell_id: str,
        *,
        generation: int,
        fencing_token: str,
        provider_generation: int,
        phase: str,
        desired_state: str | None = None,
    ) -> None:
        """Require the live fleet lease and the exact durable cell incarnation.

        A renewable lease is useful only while callers notice losing it. Check
        immediately around Kubernetes mutations, and check the durable record in
        between two lease reads so an ownership change cannot be mistaken for a
        successful provider reconciliation.
        """

        try:
            self.state.require_lease()
            record = self.state.get(cell_id)
            self.state.require_lease()
        except StaleFenceError as exc:
            raise StaleMutationError(f"cell {cell_id!r} lost its fleet mutation lease {phase}") from exc
        expected = (
            record is not None
            and record.generation == generation
            and record.fencing_token == fencing_token
            and record.provider_generation == provider_generation
            and (desired_state is None or record.desired_state == desired_state)
        )
        if not expected:
            raise StaleMutationError(f"cell {cell_id!r} changed durable ownership {phase}")

    def _create_revalidated(
        self,
        collection_path: str,
        desired: JsonObject,
        kind: str,
        revalidate: _Revalidate,
    ) -> bool:
        revalidate(f"before {kind} create")
        created = self.api.create(collection_path, desired)
        revalidate(f"after {kind} create")
        return created

    @staticmethod
    def _expected_subset(observed: object, expected: object) -> bool:
        """Compare every requested field while tolerating API-server defaults."""

        if isinstance(expected, dict):
            return isinstance(observed, dict) and all(
                key in observed and KubernetesCapacityDriver._expected_subset(observed[key], value)
                for key, value in expected.items()
            )
        if isinstance(expected, list):
            return (
                isinstance(observed, list)
                and len(observed) == len(expected)
                and all(
                    KubernetesCapacityDriver._expected_subset(actual, wanted)
                    for actual, wanted in zip(observed, expected, strict=True)
                )
            )
        return observed == expected

    @classmethod
    def _resource_identity(
        cls,
        payload: object,
        desired: JsonObject,
        kind: str,
    ) -> tuple[JsonObject, str, str]:
        """Validate an exact GET and return metadata plus delete preconditions."""

        if not is_json_object(payload):
            raise cls._conflict(kind, "GET returned a non-object payload")
        expected_metadata = desired.get("metadata")
        metadata = payload.get("metadata")
        if not is_json_object(expected_metadata) or not is_json_object(metadata):
            raise cls._conflict(kind, "GET omitted object metadata")
        if payload.get("apiVersion") != desired.get("apiVersion") or payload.get("kind") != desired.get("kind"):
            raise cls._conflict(kind, "GET returned the wrong resource kind")
        for field in ("name", "namespace"):
            expected = expected_metadata.get(field)
            if not isinstance(expected, str) or not expected or metadata.get(field) != expected:
                raise cls._conflict(kind, f"GET returned the wrong metadata.{field}")
        uid = metadata.get("uid")
        resource_version = metadata.get("resourceVersion")
        if not isinstance(uid, str) or not uid or not isinstance(resource_version, str) or not resource_version:
            raise cls._conflict(kind, "GET omitted uid/resourceVersion fencing metadata")
        return metadata, uid, resource_version

    @classmethod
    def _metadata_matches(cls, observed: JsonObject, desired: JsonObject) -> bool:
        expected_metadata = desired.get("metadata")
        actual_metadata = observed.get("metadata")
        if not is_json_object(expected_metadata) or not is_json_object(actual_metadata):
            return False
        for field in ("name", "namespace"):
            if actual_metadata.get(field) != expected_metadata.get(field):
                return False
        for field in ("labels", "annotations"):
            expected = expected_metadata.get(field)
            if expected is not None and not cls._expected_subset(actual_metadata.get(field), expected):
                return False
        return True

    @classmethod
    def _service_matches(cls, observed: JsonObject, desired: JsonObject) -> bool:
        if not cls._metadata_matches(observed, desired):
            return False
        expected_spec = desired.get("spec")
        actual_spec = observed.get("spec")
        if not is_json_object(expected_spec) or not is_json_object(actual_spec):
            return False
        if not cls._expected_subset(actual_spec, expected_spec):
            return False
        # A selector with extras silently selects no workers; unexpected exposure
        # fields can turn a private headless Service into a public endpoint.
        if actual_spec.get("selector") != expected_spec.get("selector"):
            return False
        if actual_spec.get("type") not in (None, "ClusterIP"):
            return False
        for field in ("externalIPs", "loadBalancerIP", "loadBalancerSourceRanges", "externalName"):
            if actual_spec.get(field) not in (None, [], ""):
                return False
        return True

    @staticmethod
    def _secret_data(payload: JsonObject) -> JsonObject | None:
        data = payload.get("data")
        if is_json_object(data):
            return data
        # The real API returns data, but accepting stringData keeps strict fakes
        # and admission-free fixtures representative without weakening equality.
        string_data = payload.get("stringData")
        if not is_json_object(string_data):
            return None
        encoded: JsonObject = {}
        for key, value in string_data.items():
            if not isinstance(value, str):
                return None
            encoded[key] = base64.b64encode(value.encode()).decode()
        return encoded

    @classmethod
    def _secret_matches(cls, observed: JsonObject, desired: JsonObject) -> bool:
        if not cls._metadata_matches(observed, desired) or observed.get("type") != desired.get("type"):
            return False
        if observed.get("immutable") not in (None, False):
            return False
        expected = cls._secret_data(desired)
        actual = cls._secret_data(observed)
        return expected is not None and actual == expected

    @classmethod
    def _job_matches(cls, observed: JsonObject, desired: JsonObject) -> bool:
        if not cls._metadata_matches(observed, desired):
            return False
        expected_spec = desired.get("spec")
        actual_spec = observed.get("spec")
        if not is_json_object(expected_spec) or not is_json_object(actual_spec):
            return False
        if not cls._expected_subset(actual_spec, expected_spec):
            return False
        expected_template = expected_spec.get("template")
        actual_template = actual_spec.get("template")
        if not is_json_object(expected_template) or not is_json_object(actual_template):
            return False
        expected_pod = expected_template.get("spec")
        actual_pod = actual_template.get("spec")
        if not is_json_object(expected_pod) or not is_json_object(actual_pod):
            return False
        # Reject admission mutations that could execute different code, expose a
        # host, or mount a different credential. Ordinary Kubernetes defaults
        # (dnsPolicy, schedulerName, terminationGracePeriodSeconds, and generated
        # selector labels) remain allowed.
        for field in ("initContainers", "ephemeralContainers"):
            if field not in expected_pod and actual_pod.get(field) not in (None, []):
                return False
        for field in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"):
            if field not in expected_pod and actual_pod.get(field) not in (None, False):
                return False
        for field in ("nodeSelector", "affinity", "tolerations", "volumes", "imagePullSecrets", "runtimeClassName"):
            if field not in expected_pod and actual_pod.get(field) not in (None, [], {}):
                return False
        expected_containers = expected_pod.get("containers")
        actual_containers = actual_pod.get("containers")
        if not isinstance(expected_containers, list) or not isinstance(actual_containers, list):
            return False
        if len(expected_containers) != len(actual_containers):
            return False
        for actual, expected in zip(actual_containers, expected_containers, strict=True):
            if not is_json_object(actual) or not is_json_object(expected):
                return False
            for field in (
                "command",
                "args",
                "workingDir",
                "envFrom",
                "securityContext",
                "lifecycle",
                "volumeMounts",
                "volumeDevices",
            ):
                if field not in expected and actual.get(field) not in (None, [], {}):
                    return False
        return True

    def _read_conflict(
        self,
        path: str,
        desired: JsonObject,
        kind: str,
    ) -> tuple[JsonObject, str, str]:
        payload = self.api.get(path)
        if payload is None:
            raise self._retry_reconcile(kind, "the conflicting object disappeared before GET")
        _metadata, uid, resource_version = self._resource_identity(payload, desired, kind)
        return payload, uid, resource_version

    def _delete_fenced(
        self,
        path: str,
        desired: JsonObject,
        kind: str,
        *,
        uid: str,
        resource_version: str,
        revalidate: _Revalidate,
    ) -> None:
        revalidate(f"before {kind} delete")
        self.api.delete(path, uid=uid, resource_version=resource_version)
        revalidate(f"after {kind} delete")
        remaining = self.api.get(path)
        revalidate(f"after {kind} deletion observation")
        if remaining is None:
            return
        _metadata, remaining_uid, _remaining_version = self._resource_identity(remaining, desired, kind)
        if remaining_uid != uid:
            raise self._conflict(kind, "a replacement acquired the name during fenced deletion")
        raise self._retry_reconcile(kind, "foreground deletion is still in progress")

    @staticmethod
    def _release_ownership_manifest(
        kind: str,
        cell_id: str,
        namespace: str,
        *,
        provider_generation: int,
        fencing_token: str,
    ) -> JsonObject:
        """Return the complete metadata release must prove before deleting."""

        if kind == "Service":
            return headless_service_manifest(cell_id, namespace)
        if kind == "Job":
            api_version = "batch/v1"
            name = object_name("eyvan", cell_id)
            labels = {
                API_CLIENT_LABEL: "true",
                CELL_LABEL: cell_label_value(cell_id),
                GENERATION_LABEL: str(provider_generation),
            }
        elif kind == "Secret":
            api_version = "v1"
            name = object_name("eyvan-token", cell_id)
            labels = {
                CELL_LABEL: cell_label_value(cell_id),
                GENERATION_LABEL: str(provider_generation),
            }
        else:  # pragma: no cover - closed internal call surface
            raise ValueError(f"unsupported Kubernetes release kind {kind!r}")
        return {
            "apiVersion": api_version,
            "kind": kind,
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": labels,
                "annotations": {
                    CELL_ID_ANNOTATION: cell_id,
                    FENCE_ANNOTATION: fence_fingerprint(fencing_token),
                },
            },
        }

    def _read_release_resource(
        self,
        path: str,
        desired: JsonObject,
        kind: str,
    ) -> tuple[str, str] | None:
        """Exact-GET and prove one object's ownership before any delete starts."""

        observed = self.api.get(path)
        if observed is None:
            return None
        _metadata, uid, resource_version = self._resource_identity(observed, desired, kind)
        matches = (
            self._service_matches(observed, desired) if kind == "Service" else self._metadata_matches(observed, desired)
        )
        if not matches:
            raise self._conflict(kind, "ownership metadata is missing or differs from the durable cell fence")
        return uid, resource_version

    @classmethod
    def _has_cell_ownership(cls, observed: JsonObject, desired: JsonObject) -> bool:
        actual_metadata = observed.get("metadata")
        expected_metadata = desired.get("metadata")
        if not is_json_object(actual_metadata) or not is_json_object(expected_metadata):
            return False
        actual_labels = actual_metadata.get("labels")
        expected_labels = expected_metadata.get("labels")
        actual_annotations = actual_metadata.get("annotations")
        expected_annotations = expected_metadata.get("annotations")
        return (
            is_json_object(actual_labels)
            and is_json_object(expected_labels)
            and is_json_object(actual_annotations)
            and is_json_object(expected_annotations)
            and isinstance(expected_labels.get(CELL_LABEL), str)
            and isinstance(expected_annotations.get(CELL_ID_ANNOTATION), str)
            and actual_labels.get(CELL_LABEL) == expected_labels.get(CELL_LABEL)
            and actual_annotations.get(CELL_ID_ANNOTATION) == expected_annotations.get(CELL_ID_ANNOTATION)
        )

    @classmethod
    def _stale_secret_token(cls, observed: JsonObject, desired: JsonObject) -> bool:
        if not cls._has_cell_ownership(observed, desired) or observed.get("type") != "Opaque":
            return False
        if observed.get("immutable") not in (None, False):
            return False
        data = cls._secret_data(observed)
        if not is_json_object(data) or set(data) != {"token"} or not isinstance(data.get("token"), str):
            return False
        actual_metadata = observed.get("metadata")
        expected_metadata = desired.get("metadata")
        assert is_json_object(actual_metadata) and is_json_object(expected_metadata)
        actual_labels = actual_metadata.get("labels")
        expected_labels = expected_metadata.get("labels")
        actual_annotations = actual_metadata.get("annotations")
        expected_annotations = expected_metadata.get("annotations")
        if not is_json_object(actual_labels) or not is_json_object(expected_labels):
            return False
        actual_generation = actual_labels.get(GENERATION_LABEL)
        expected_generation = expected_labels.get(GENERATION_LABEL)
        actual_fingerprint = actual_annotations.get(FENCE_ANNOTATION) if is_json_object(actual_annotations) else None
        expected_fingerprint = (
            expected_annotations.get(FENCE_ANNOTATION) if is_json_object(expected_annotations) else None
        )
        # Early Secrets with the complete cell ID but no generation fence are
        # repairable, but only after any owned Job is deleted below.
        if actual_generation is None and actual_fingerprint is None:
            return True
        if not isinstance(actual_generation, str) or not actual_generation.isdecimal():
            return False
        if not isinstance(expected_generation, str) or not expected_generation.isdecimal():
            return False
        valid_fingerprint = (
            isinstance(actual_fingerprint, str)
            and len(actual_fingerprint) == 32
            and all(character in "0123456789abcdef" for character in actual_fingerprint)
        )
        if not valid_fingerprint:
            return False
        return int(actual_generation) < int(expected_generation) or (
            actual_generation == expected_generation and actual_fingerprint == expected_fingerprint
        )

    @classmethod
    def _job_disposition(cls, observed: JsonObject, desired: JsonObject) -> str:
        if cls._job_matches(observed, desired):
            return "exact"
        if not cls._has_cell_ownership(observed, desired):
            return "collision"
        actual_metadata = observed.get("metadata")
        expected_metadata = desired.get("metadata")
        assert is_json_object(actual_metadata) and is_json_object(expected_metadata)
        actual_labels = actual_metadata.get("labels")
        expected_labels = expected_metadata.get("labels")
        annotations = actual_metadata.get("annotations")
        if not is_json_object(actual_labels) or not is_json_object(expected_labels) or not is_json_object(annotations):
            return "collision"
        actual_generation = actual_labels.get(GENERATION_LABEL)
        expected_generation = expected_labels.get(GENERATION_LABEL)
        fingerprint = annotations.get(FENCE_ANNOTATION)
        if not isinstance(actual_generation, str) or not actual_generation.isdecimal():
            return "collision"
        if not isinstance(expected_generation, str) or not expected_generation.isdecimal():
            return "collision"
        if (
            int(actual_generation) < int(expected_generation)
            and isinstance(fingerprint, str)
            and len(fingerprint) == 32
            and all(character in "0123456789abcdef" for character in fingerprint)
        ):
            return "stale"
        return "collision"

    def _remove_job_before_secret_rotation(
        self,
        path: str,
        desired: JsonObject,
        revalidate: _Revalidate,
    ) -> None:
        existing = self.api.get(path)
        revalidate("after Job rotation preflight")
        if existing is None:
            return
        _metadata, uid, resource_version = self._resource_identity(existing, desired, "Job")
        disposition = self._job_disposition(existing, desired)
        if disposition == "collision":
            raise self._conflict("Job", "a colliding Job prevents safe Secret rotation")
        self._delete_fenced(
            path,
            desired,
            "Job",
            uid=uid,
            resource_version=resource_version,
            revalidate=revalidate,
        )

    def _ensure_service(
        self,
        collection_path: str,
        object_path: str,
        desired: JsonObject,
        revalidate: _Revalidate,
    ) -> None:
        if self._create_revalidated(collection_path, desired, "Service", revalidate):
            return
        existing, _uid, _resource_version = self._read_conflict(object_path, desired, "Service")
        revalidate("after Service conflict observation")
        if not self._service_matches(existing, desired):
            raise self._conflict("Service", "ownership or security-critical spec differs")

    def _ensure_secret(
        self,
        collection_path: str,
        object_path: str,
        job_path: str,
        desired: JsonObject,
        desired_job: JsonObject,
        revalidate: _Revalidate,
    ) -> None:
        if self._create_revalidated(collection_path, desired, "Secret", revalidate):
            return
        existing, uid, resource_version = self._read_conflict(object_path, desired, "Secret")
        revalidate("after Secret conflict observation")
        if self._secret_matches(existing, desired):
            return
        if not self._stale_secret_token(existing, desired):
            raise self._conflict("Secret", "ownership, type, or data shape differs")
        # Never replace a mounted Secret under a running old Job: projected
        # volumes would hand the new credential to the old incarnation.
        self._remove_job_before_secret_rotation(job_path, desired_job, revalidate)
        self._delete_fenced(
            object_path,
            desired,
            "Secret",
            uid=uid,
            resource_version=resource_version,
            revalidate=revalidate,
        )
        if self._create_revalidated(collection_path, desired, "Secret", revalidate):
            return
        replacement, _uid, _resource_version = self._read_conflict(object_path, desired, "Secret")
        revalidate("after Secret replacement observation")
        if not self._secret_matches(replacement, desired):
            raise self._retry_reconcile("Secret", "the fenced replacement did not converge")

    def _ensure_job(
        self,
        collection_path: str,
        object_path: str,
        desired: JsonObject,
        revalidate: _Revalidate,
    ) -> None:
        if self._create_revalidated(collection_path, desired, "Job", revalidate):
            return
        existing, uid, resource_version = self._read_conflict(object_path, desired, "Job")
        revalidate("after Job conflict observation")
        disposition = self._job_disposition(existing, desired)
        if disposition == "exact":
            return
        if disposition != "stale":
            raise self._conflict("Job", "cell, generation, fence, or security-critical spec differs")
        self._delete_fenced(
            object_path,
            desired,
            "Job",
            uid=uid,
            resource_version=resource_version,
            revalidate=revalidate,
        )
        if self._create_revalidated(collection_path, desired, "Job", revalidate):
            return
        replacement, _uid, _resource_version = self._read_conflict(object_path, desired, "Job")
        revalidate("after Job replacement observation")
        if not self._job_matches(replacement, desired):
            raise self._retry_reconcile("Job", "the fenced replacement did not converge")

    def apply_capacity(
        self,
        cell_id: str,
        desired: tp.Mapping[str, JsonValue],
        context: MutationContext,
    ) -> CellObservation:
        """Create this cell's Service, Secret, and Job, in that order.

        Order matters. The Service must exist before the pods so their DNS names
        resolve from the first moment, and the Secret must exist before the Job
        so a pod does not start, fail to mount, and burn the ``backoffLimit: 0``
        budget on a race.

        Deterministic names make a retry find the same objects, but do not
        prove ownership. Every 409 is followed by an exact GET and comparison of
        Eyvan ownership plus the complete security-critical manifest. A stale
        credential is rotated only after its old Job is fenced and gone. The
        driver lock and renewable fleet lease cover the durable write and every
        provider mutation, so release cannot pass an apply that is still capable
        of creating resources.
        """

        accelerator_type, capacity_class, worker_count, options = self._validate_desired(desired)
        try:
            with self._mutation_lock, self.state.exclusive():
                captured: dict[str, object] = {}

                def mutate(record: FleetCellRecord) -> None:
                    if not self._accept_mutation(record, context):
                        # The durable stamp can commit before any Kubernetes POST.
                        # Replays must resume reconciliation without bumping the
                        # provider generation or rotating the worker token again.
                        captured["generation"] = record.provider_generation
                        return
                    record.accelerator_type = accelerator_type
                    record.capacity_class = capacity_class
                    record.desired_state = "up"
                    record.state = CapacityStatus.PROVISIONING
                    # Bumped on every apply that is not a replay, and the Secret is
                    # rewritten below -- ``derive_native_worker_token`` hashes
                    # (management_uid, provider_generation), so a stale Secret would
                    # authenticate workers of an incarnation this one replaced.
                    record.provider_generation = record.provider_generation + 1
                    record.metadata["capacity_backend"] = self.provider_name
                    record.metadata["worker_count"] = worker_count
                    record.metadata["namespace"] = self.namespace
                    # Namespace included so one cell_id in two clusters cannot
                    # collide on a derived token.
                    record.metadata["management_uid"] = stable_id("kubernetes", self.namespace, cell_id)
                    self._stamp(record, context)
                    captured["generation"] = record.provider_generation

                try:
                    record = self.state.mutate_or_create_record(cell_id, mutate, generation=context.generation)
                except StaleFenceError as exc:
                    raise StaleMutationError(str(exc)) from exc
                generation = int(captured.get("generation", record.provider_generation))

                def revalidate(phase: str) -> None:
                    self._revalidate_record_owner(
                        cell_id,
                        generation=context.generation,
                        fencing_token=context.fencing_token,
                        provider_generation=generation,
                        phase=phase,
                        desired_state="up",
                    )

                namespace = self.namespace
                queue_name = self._optional_option(options, "queue_name")
                revalidate("before ClusterQueue validation")
                self._require_safe_queue(queue_name)
                revalidate("after ClusterQueue validation")

                service = headless_service_manifest(cell_id, namespace)
                service_name = object_name("eyvan", cell_id)
                service_collection = f"/api/v1/namespaces/{namespace}/services"
                service_path = f"{service_collection}/{service_name}"

                token = self._worker_token(record, generation)
                secret: JsonObject | None = None
                secret_name: str | None = None
                secret_collection = f"/api/v1/namespaces/{namespace}/secrets"
                secret_path = f"{secret_collection}/{object_name('eyvan-token', cell_id)}"
                if token is not None:
                    secret = secret_manifest(cell_id, namespace, token=token)
                    secret_metadata = secret.get("metadata")
                    assert is_json_object(secret_metadata)
                    labels = secret_metadata.get("labels")
                    assert is_json_object(labels)
                    labels[GENERATION_LABEL] = str(generation)
                    annotations = secret_metadata.get("annotations")
                    assert is_json_object(annotations)
                    annotations[FENCE_ANNOTATION] = fence_fingerprint(context.fencing_token)
                    secret_name = str(secret_metadata["name"])

                job = job_manifest(
                    cell_id=cell_id,
                    namespace=namespace,
                    image=str(options.get("image", self.image)),
                    worker_count=worker_count,
                    provider_generation=generation,
                    fencing_token=context.fencing_token,
                    resources=self._resources(options),
                    secret_name=secret_name,
                    queue_name=queue_name,
                    priority_class=self._optional_option(options, "priority_class"),
                    node_selector=self._mapping_option(options, "node_selector"),
                    tolerations=self._sequence_option(options, "tolerations"),
                )
                job_collection = f"/apis/batch/v1/namespaces/{namespace}/jobs"
                job_path = f"{job_collection}/{service_name}"

                self._ensure_service(service_collection, service_path, service, revalidate)
                if secret is not None:
                    self._ensure_secret(secret_collection, secret_path, job_path, secret, job, revalidate)
                self._ensure_job(job_collection, job_path, job, revalidate)
                revalidate("before final observation")
                observation = self.observe(cell_id)
                revalidate("after final observation")
                return observation
        except StaleFenceError as exc:
            raise StaleMutationError(f"cell {cell_id!r} lost its fleet mutation lease during apply") from exc

    def _require_safe_queue(self, queue_name: str | None) -> None:
        """Refuse a ClusterQueue that could evict Eyvan's work.

        Kueue eviction destroys a pod with no checkpoint hook, bypassing the
        whole PreemptionPlan / CHECKPOINT_AND_STOP / grace_period_seconds
        protocol, and the only trace is a training job that lost hours with no
        ledger entry explaining why.

        The check runs here rather than in ``eyvan doctor`` because this is where
        cluster access exists: the driver runs in-cluster with a ServiceAccount,
        while doctor runs on an operator's laptop and can only warn. The
        precondition functions were written and never called from anywhere, and
        two docstrings claimed doctor refused a configuration it merely warned
        about.

        A queue Eyvan cannot read is not treated as safe -- an unreadable
        ClusterQueue is exactly the case where assuming the best silently loses
        the guarantee.
        """

        if not queue_name:
            return
        response = self.api.request("GET", f"{_KUEUE_API}/clusterqueues/{queue_name}")
        if response.status_code == 404:
            payload = None
        elif response.status_code >= 400:
            raise CapacityProviderError(
                f"could not read ClusterQueue {queue_name!r} to verify it cannot evict Eyvan's work "
                f"(HTTP {response.status_code})",
                code=CapacityProviderFailureCode.CONFIGURATION,
            )
        else:
            decoded = response.json()
            payload = decoded if is_json_object(decoded) else None
        findings = inspect_cluster_queue(payload, queue_name)
        if findings:
            detail = "; ".join(f"{finding.problem} -- {finding.remedy}" for finding in findings)
            raise CapacityProviderError(
                f"ClusterQueue {queue_name!r} is unsafe for Eyvan: {detail}",
                code=CapacityProviderFailureCode.CONFIGURATION,
            )

    def _worker_token(self, record: FleetCellRecord, generation: int) -> str | None:
        """Derive this incarnation's native-worker token, or ``None`` if unconfigured.

        ``None`` is a real answer, not a failure: a cluster with no shared secret
        runs unauthenticated workers, which the agent already supports. Inventing
        a token here would produce one no worker could ever verify.
        """

        from eyvan.worker.runtime.native_bootstrap import derive_native_worker_token

        return derive_native_worker_token(
            self.shared_secret,
            str(record.metadata.get("management_uid", record.name)),
            provider_generation=generation,
        )

    @staticmethod
    def _resources(options: tp.Mapping[str, JsonValue]) -> dict[str, str]:
        raw = options.get("resources")
        if not is_json_object(raw):
            return {}
        return {str(name): str(value) for name, value in raw.items()}

    @staticmethod
    def _optional_option(options: tp.Mapping[str, JsonValue], name: str) -> str | None:
        value = options.get(name)
        return value.strip() if isinstance(value, str) and value.strip() else None

    @staticmethod
    def _mapping_option(options: tp.Mapping[str, JsonValue], name: str) -> dict[str, str]:
        raw = options.get(name)
        if not is_json_object(raw):
            return {}
        return {str(key): str(value) for key, value in raw.items()}

    @staticmethod
    def _sequence_option(options: tp.Mapping[str, JsonValue], name: str) -> tuple[JsonObject, ...]:
        raw = options.get(name)
        if not isinstance(raw, list):
            return ()
        return tuple(item for item in raw if is_json_object(item))

    def reconcile(self, cell_id: str, context: MutationContext) -> CellObservation:
        """Re-assert ownership and re-observe. Creates nothing new."""

        with self._mutation_lock, self.state.exclusive():

            def mutate(record: FleetCellRecord) -> None:
                if not self._accept_mutation(record, context):
                    return
                record.desired_state = "up"
                self._stamp(record, context)

            try:
                self.state.mutate_record(cell_id, mutate, generation=context.generation)
            except FleetCellNotRegisteredError:
                return self.observe(cell_id)
            except StaleFenceError as exc:
                raise StaleMutationError(str(exc)) from exc
        return self.observe(cell_id)

    def drain(self, cell_id: str, context: MutationContext) -> CellObservation:
        """Stop scheduling onto this cell while leaving its Job running.

        Draining does not delete: workloads already leased on this cell keep
        their pods until they finish, which is exactly what distinguishes a drain
        from a release.
        """

        with self._mutation_lock, self.state.exclusive():

            def mutate(record: FleetCellRecord) -> None:
                record.desired_state = "down"
                record.state = CapacityStatus.DRAINING
                self._stamp(record, context)

            try:
                record = self.state.drain_record(cell_id, context.generation, context.fencing_token, mutate)
            except StaleFenceError as exc:
                raise StaleMutationError(str(exc)) from exc
        if record is None:
            return CellObservation(
                cell_id=cell_id,
                state=CapacityStatus.DRAINING,
                generation=context.generation,
                observed_at=datetime.now(UTC),
            )
        return self._observation(record, CapacityStatus.DRAINING, "draining", worker_count=0, ready=0)

    def release(self, cell_id: str, context: MutationContext) -> CellObservation:
        """Fenced-delete the Job, Secret, and Service, then tombstone the cell.

        Every object is exact-GET and ownership-checked before the first DELETE,
        so a stale mutation or same-name collision has no provider side effect.
        The driver lock and fleet lease remain held through foreground Job
        deletion: only after the Job object (and therefore its dependent pods) is
        absent may the mounted Secret and stable-DNS Service be removed.
        """

        namespace = self.namespace
        try:
            with self._mutation_lock, self.state.exclusive():
                if self.state.get(cell_id) is None:
                    # A completed-release replay must never probe/delete a replacement
                    # that later acquired the deterministic Kubernetes names.
                    return CellObservation(
                        cell_id=cell_id,
                        state=CapacityStatus.RELEASED,
                        generation=context.generation,
                        observed_at=datetime.now(UTC),
                    )

                captured: dict[str, object] = {}

                def validate_owner(record: FleetCellRecord) -> None:
                    self._accept_mutation(record, context)  # raises before any Kubernetes call when stale
                    if not isinstance(record.fencing_token, str) or not record.fencing_token:
                        raise StaleMutationError(f"cell {cell_id!r} has no durable Kubernetes ownership fence")
                    captured["generation"] = record.generation
                    captured["provider_generation"] = record.provider_generation
                    captured["fencing_token"] = record.fencing_token

                try:
                    self.state.mutate_record(cell_id, validate_owner, generation=context.generation)
                except FleetCellNotRegisteredError:
                    return CellObservation(
                        cell_id=cell_id,
                        state=CapacityStatus.RELEASED,
                        generation=context.generation,
                        observed_at=datetime.now(UTC),
                    )
                except StaleFenceError as exc:
                    raise StaleMutationError(str(exc)) from exc

                durable_generation = int(captured["generation"])
                provider_generation = int(captured["provider_generation"])
                resource_fencing_token = str(captured["fencing_token"])

                def revalidate(phase: str) -> None:
                    self._revalidate_record_owner(
                        cell_id,
                        generation=durable_generation,
                        fencing_token=resource_fencing_token,
                        provider_generation=provider_generation,
                        phase=phase,
                    )

                resources = (
                    (
                        "Job",
                        f"/apis/batch/v1/namespaces/{namespace}/jobs/{object_name('eyvan', cell_id)}",
                    ),
                    (
                        "Secret",
                        f"/api/v1/namespaces/{namespace}/secrets/{object_name('eyvan-token', cell_id)}",
                    ),
                    (
                        "Service",
                        f"/api/v1/namespaces/{namespace}/services/{object_name('eyvan', cell_id)}",
                    ),
                )
                preflight: list[tuple[str, str, JsonObject, tuple[str, str] | None]] = []
                for kind, path in resources:
                    revalidate(f"before {kind} release preflight")
                    desired = self._release_ownership_manifest(
                        kind,
                        cell_id,
                        namespace,
                        provider_generation=provider_generation,
                        fencing_token=resource_fencing_token,
                    )
                    identity = self._read_release_resource(path, desired, kind)
                    revalidate(f"after {kind} release preflight")
                    preflight.append((kind, path, desired, identity))

                for kind, path, desired, identity in preflight:
                    if identity is None:
                        continue
                    uid, resource_version = identity
                    self._delete_fenced(
                        path,
                        desired,
                        kind,
                        uid=uid,
                        resource_version=resource_version,
                        revalidate=revalidate,
                    )

                revalidate("before durable release tombstone")

                def owned(record: FleetCellRecord) -> bool:
                    self._accept_mutation(record, context)
                    self._stamp(record, context)
                    if record.generation != context.generation or record.fencing_token != context.fencing_token:
                        raise StaleMutationError(f"cell {cell_id!r} changed ownership before kubernetes release")
                    return True

                try:
                    self.state.tombstone_record(cell_id, context.generation, context.fencing_token, owned)
                except StaleFenceError as exc:
                    raise StaleMutationError(str(exc)) from exc
                self.state.require_lease()
                return CellObservation(
                    cell_id=cell_id,
                    state=CapacityStatus.RELEASED,
                    generation=context.generation,
                    observed_at=datetime.now(UTC),
                )
        except StaleFenceError as exc:
            raise StaleMutationError(f"cell {cell_id!r} lost its fleet mutation lease during release") from exc


__all__ = ("MAX_WORKERS", "KubernetesCapacityDriver")
