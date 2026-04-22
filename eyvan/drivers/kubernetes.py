# Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
preemption disabled.** ``commands/doctor.py`` checks that precondition, which is
what turns an invisible correctness hazard into a startup failure.

**Honest cost, stated here and in the README.** Eyvan's budgets and fair share
reason about cells it asked for, not the cluster's real free capacity, so an
Eyvan pool can sit WAITING while the cluster is full of non-Eyvan work.
``CapacityBelief`` will *learn* that -- repeated timeouts raise ``route_priority``
-- but Eyvan will never *plan* around it.
"""

from __future__ import annotations

import typing as tp
from datetime import datetime
from threading import RLock

from eyvan._compat import UTC
from eyvan._json import JsonObject, JsonValue, is_json_object
from eyvan.capacity import CapacityStatus
from eyvan.drivers.base import CellCapabilities, CellObservation, MutationContext, StaleMutationError
from eyvan.identifiers import stable_id
from eyvan.persistence.fleet import (
    FleetCellNotRegisteredError,
    FleetCellRecord,
    FleetStateStore,
    StaleFenceError,
)
from eyvan.providers.kubernetes.api import KubernetesApi, capacity_state, interpret_job
from eyvan.providers.kubernetes.workers import (
    fence_fingerprint,
    headless_service_manifest,
    job_manifest,
    object_name,
    secret_manifest,
    worker_endpoints,
)

#: Most workers one Kubernetes cell may contain. A Job's parallelism is the
#: multislice worker count, and the same ceiling that bounds a TPU multislice
#: bounds this: past it, a single failure unit is too large to reason about.
MAX_WORKERS = 512


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
        image: str = "ghcr.io/erfanzar/eyvan-native-worker:latest",
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
        if status.exists and record.fencing_token is not None:
            expected = fence_fingerprint(record.fencing_token)
            if status.fence_fingerprint and status.fence_fingerprint != expected:
                # A leaked previous incarnation. Not adopted: its workers hold a
                # token derived from a generation this record has moved past, so
                # anything they do is already unauthorized.
                return self._observation(
                    record,
                    CapacityStatus.NEEDS_RECREATE,
                    "a job from a previous incarnation owns this name",
                    worker_count=status.parallelism,
                    ready=0,
                )
        state, detail = capacity_state(status)
        return self._observation(record, state, detail, worker_count=status.parallelism, ready=status.ready)

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

        Creates are idempotent by construction: names are derived from
        ``cell_id``, so a 409 is this cell's own earlier create -- exactly the
        replay an at-least-once outbox produces.
        """

        accelerator_type, capacity_class, worker_count, options = self._validate_desired(desired)
        with self._mutation_lock, self.state.exclusive():
            captured: dict[str, object] = {}

            def mutate(record: FleetCellRecord) -> None:
                if not self._accept_mutation(record, context):
                    captured["skip"] = True
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
                captured["management_uid"] = record.metadata["management_uid"]

            try:
                record = self.state.mutate_or_create_record(cell_id, mutate, generation=context.generation)
            except StaleFenceError as exc:
                raise StaleMutationError(str(exc)) from exc

        if captured.get("skip"):
            return self.observe(cell_id)

        generation = int(captured.get("generation", record.provider_generation))
        namespace = self.namespace
        self.api.create(f"/api/v1/namespaces/{namespace}/services", headless_service_manifest(cell_id, namespace))
        token = self._worker_token(record, generation)
        secret_name: str | None = None
        if token is not None:
            secret = secret_manifest(cell_id, namespace, token=token)
            self.api.create(f"/api/v1/namespaces/{namespace}/secrets", secret)
            secret_name = str(secret["metadata"]["name"])  # type: ignore[index]
        self.api.create(
            f"/apis/batch/v1/namespaces/{namespace}/jobs",
            job_manifest(
                cell_id=cell_id,
                namespace=namespace,
                image=str(options.get("image", self.image)),
                worker_count=worker_count,
                provider_generation=generation,
                fencing_token=context.fencing_token,
                resources=self._resources(options),
                secret_name=secret_name,
                queue_name=self._optional_option(options, "queue_name"),
                priority_class=self._optional_option(options, "priority_class"),
                node_selector=self._mapping_option(options, "node_selector"),
                tolerations=self._sequence_option(options, "tolerations"),
            ),
        )
        return self.observe(cell_id)

    def _worker_token(self, record: FleetCellRecord, generation: int) -> str | None:
        """Derive this incarnation's native-worker token, or ``None`` if unconfigured.

        ``None`` is a real answer, not a failure: a cluster with no shared secret
        runs unauthenticated workers, which the agent already supports. Inventing
        a token here would produce one no worker could ever verify.
        """

        from eyvan.runtime.native_bootstrap import derive_native_worker_token

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
        """Delete the Job, Service, and Secret, then tombstone the record.

        The Job goes first and with foreground propagation, so its pods are gone
        before anything else is removed. Deleting the Secret first would leave
        running pods with a mounted volume whose backing object no longer exists.
        """

        namespace = self.namespace
        self.api.delete(f"/apis/batch/v1/namespaces/{namespace}/jobs/{object_name('eyvan', cell_id)}")
        self.api.delete(f"/api/v1/namespaces/{namespace}/secrets/{object_name('eyvan-token', cell_id)}")
        self.api.delete(f"/api/v1/namespaces/{namespace}/services/{object_name('eyvan', cell_id)}")
        with self._mutation_lock, self.state.exclusive():

            def owned(record: FleetCellRecord) -> bool:
                if self._accept_mutation(record, context):
                    self._stamp(record, context)
                if record.generation != context.generation or record.fencing_token != context.fencing_token:
                    raise StaleMutationError(f"cell {cell_id!r} changed ownership before kubernetes release")
                return True

            try:
                self.state.tombstone_record(cell_id, context.generation, context.fencing_token, owned)
            except StaleFenceError as exc:
                raise StaleMutationError(str(exc)) from exc
        return CellObservation(
            cell_id=cell_id,
            state=CapacityStatus.RELEASED,
            generation=context.generation,
            observed_at=datetime.now(UTC),
        )


__all__ = ("MAX_WORKERS", "KubernetesCapacityDriver")
