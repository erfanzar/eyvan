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
"""Isolated adapter from persisted API records to the pure core scheduler."""

from __future__ import annotations

import typing as tp

from eyvan.core import (
    Budget,
    CapacityCell,
    CapacityClass,
    CapacityLease,
    CheckpointMode,
    CheckpointPolicy,
    IdentityScope,
    Quota,
    ResourceGang,
    ResourceVector,
    Scheduler,
    SchedulingResult,
    WorkerResourceScope,
    WorkloadIdentity,
    WorkloadSpec,
)
from eyvan.core import (
    PriorityBand as CorePriorityBand,
)
from eyvan.core.availability import p95_latency
from eyvan.service.models import (
    WORKER_COUNT_LABEL,
    BudgetRecord,
    CellRecord,
    CellState,
    LeaseRecord,
    PolicyRecord,
    PriorityBand,
    QuotaRecord,
    WorkloadRecord,
    WorkloadState,
)
from eyvan.service.models import (
    CapacityClass as ApiCapacityClass,
)
from eyvan.service.models import (
    ResourceVector as ApiResourceVector,
)
from eyvan.service.state import ACTIVE_LEASE_STATES


class SchedulingService(tp.Protocol):
    """Pure global scheduling boundary injected into reconciliation services."""

    def schedule(
        self,
        workloads: tp.Iterable[WorkloadRecord],
        cells: tp.Iterable[CellRecord],
        *,
        leases: tp.Iterable[LeaseRecord] = (),
        budgets: tp.Iterable[BudgetRecord] = (),
        quotas: tp.Iterable[QuotaRecord] = (),
        policies: tp.Iterable[PolicyRecord] = (),
    ) -> SchedulingResult: ...


def _resources(value: ApiResourceVector) -> ResourceVector:
    items = {
        "cpu": value.cpu,
        "memory_gib": value.memory_gib,
        "gpu": value.gpu,
        "tpu": value.tpu,
        **value.custom,
    }
    return ResourceVector({key: quantity for key, quantity in items.items() if quantity})


def _priority(value: PriorityBand) -> CorePriorityBand:
    return CorePriorityBand[value.name]


def _checkpoint(record: WorkloadRecord) -> CheckpointPolicy:
    spec = record.spec
    return CheckpointPolicy(
        mode=CheckpointMode.ON_PREEMPTION if spec.checkpointable and spec.checkpoint_hook else CheckpointMode.NONE,
        grace_period_seconds=spec.checkpoint_grace_seconds,
        # Spot capacity is interruptible by definition: the provider may reclaim
        # it at any moment with no checkpoint, so Eyvan reclaiming it for a
        # higher-priority workload breaks no promise the user was given.
        #
        # This must be set explicitly. The scheduler requires a victim to be
        # `checkpointable or allow_forced_termination` (core/scheduler.py), so
        # leaving it at its default made every non-checkpointable spot lease
        # revocable-but-never-eligible: the request surfaced as
        # PREEMPTION_UNSAFE and that capacity was never reclaimed at all.
        # On-demand capacity is deliberately excluded -- it is only preemptible
        # when the workload can checkpoint.
        allow_forced_termination=spec.capacity_class is ApiCapacityClass.SPOT,
    )


def to_core_workload(record: WorkloadRecord, policy: PolicyRecord | None = None) -> WorkloadSpec:
    """Convert one persisted workload without sharing mutable API types."""

    spec = record.spec
    required_labels = {
        **spec.required_labels,
        WORKER_COUNT_LABEL: str(spec.worker_count),
        **({"provider": spec.provider} if spec.provider else {}),
        **({"region": spec.region} if spec.region else {}),
        **({"accelerator_type": spec.accelerator_type} if spec.accelerator_type else {}),
    }
    portfolio = tuple(CapacityClass.parse(entry.value) for entry in spec.capacity_class_portfolio)
    allowed_label_values: list[tuple[str, tuple[str, ...]]] = []
    # The workload's own lists come first and are narrower than any policy list
    # could be: a policy says what a *project* may use, a spec says what this run
    # will accept. Both are expressed through the same label mechanism the
    # scheduler already applies, so widening costs no new matching code.
    for label, choices in (
        ("region", spec.allowed_regions),
        ("zone", spec.allowed_zones),
        ("accelerator_type", spec.allowed_accelerator_types),
    ):
        if choices:
            allowed_label_values.append((label, choices))
    if policy is not None:
        if not spec.provider and policy.allowed_providers:
            allowed_label_values.append(("provider", policy.allowed_providers))
        if not spec.region and not spec.allowed_regions and policy.allowed_regions:
            allowed_label_values.append(("region", policy.allowed_regions))
    denied_label_values = (
        (("accelerator_type", policy.denied_accelerator_types),)
        if policy is not None and policy.denied_accelerator_types
        else ()
    )
    return WorkloadSpec(
        workload_id=record.id,
        identity=WorkloadIdentity(record.org_id, record.project_id, record.created_by),
        gangs=(
            ResourceGang(
                gang_id=f"{record.id}-0",
                replicas=spec.gang.replicas,
                resources_per_replica=_resources(spec.gang.per_replica),
                required_topology=tuple(spec.gang.required_topology.items()),
            ),
        ),
        priority=_priority(spec.priority_band),
        attempt=record.attempt,
        generation=record.generation,
        queue_position=1000 - spec.intra_band_priority,
        runtime=spec.required_executor,
        required_labels=tuple(required_labels.items()),
        preferred_labels=tuple(spec.preferred_labels.items()),
        allowed_label_values=tuple(allowed_label_values),
        denied_label_values=denied_label_values,
        preferred_cells=spec.preferred_cells,
        # The portfolio, not the single requested class. The core scheduler has
        # always ranked these -- ``capacity_class_rank`` is the fourth key of the
        # lexicographic PlacementScore -- and this adapter collapsing it to one
        # entry was the only thing making the mechanism unreachable.
        allowed_capacity_classes=frozenset(portfolio),
        capacity_class_preference=portfolio,
        checkpoint_policy=_checkpoint(record),
        estimated_duration_seconds=spec.estimated_duration_seconds,
        # A spread-cell TPU job must keep every member alive until it can be
        # preempted as one unit. The existing preemption protocol is
        # single-cell, so do not expose one member as an individual victim.
        revocable=spec.cell_group_count == 1
        and (spec.capacity_class is ApiCapacityClass.SPOT or spec.checkpointable)
        and (policy is None or policy.preemption_enabled),
        cell_group_count=spec.cell_group_count,
        worker_count=spec.worker_count,
        worker_resource_scope=spec.worker_resource_scope,
    )


def to_core_cell(record: CellRecord) -> CapacityCell:
    """Convert an inventory record to scheduler-visible allocatable capacity."""

    labels = {
        **record.labels,
        WORKER_COUNT_LABEL: str(record.worker_count),
        "provider": record.provider,
        "region": record.region,
        **({"zone": record.zone} if record.zone else {}),
        **({"accelerator_type": record.accelerator_type} if record.accelerator_type else {}),
    }
    evidence = record.availability.success_evidence + record.availability.failure_evidence
    # A cell without availability history has no failure signal; treat it as
    # fully reliable instead of dividing by zero.
    success_probability = record.availability.success_evidence / evidence if evidence else 1.0
    latency_p95 = p95_latency(record.availability.provision_latencies_s) or 0
    interruption_rate = record.availability.interruptions / max(record.availability.successful_hours, 1)
    return CapacityCell(
        cell_id=record.id,
        capacity=_resources(record.allocatable),
        capacity_class=CapacityClass.parse(record.capacity_class.value),
        labels=tuple(labels.items()),
        topology=tuple(record.topology.items()),
        runtimes=frozenset({record.executor}),
        unit_prices=ResourceVector(record.unit_prices),
        route_priority=round((1 - success_probability) * 1000 + interruption_rate * 100),
        startup_latency_seconds=round(latency_p95),
        max_replica_resources=_resources(record.max_replica_resources) if record.max_replica_resources else None,
        max_gang_replicas=(
            record.max_gang_replicas
            if record.max_gang_replicas is not None
            else 1
            if record.executor in {"native-local", "eyvan-native"}
            else None
        ),
        exclusive_workloads=record.executor in {"native-local", "eyvan-native"},
        autoscale_request_workload_id=record.autoscale_request_workload_id,
        enabled=record.state is CellState.READY,
        worker_count=record.worker_count,
    )


def landed_capacity_classes(cells: tp.Iterable[CellRecord]) -> dict[str, ApiCapacityClass]:
    """Map cell id to capacity class, for resolving what a lease actually landed on.

    A named function rather than an inline comprehension because it is the whole
    of the wiring that makes the landed class reachable: ``LeaseRecord`` carries no
    capacity class, so without this join ``to_core_lease`` has nothing to consult
    and silently falls back to the *requested* class -- which is the bug.
    """

    return {cell.id: cell.capacity_class for cell in cells}


def to_core_lease(
    record: LeaseRecord,
    workload: WorkloadRecord,
    policy: PolicyRecord | None = None,
    landed_capacity_class: ApiCapacityClass | None = None,
) -> CapacityLease:
    """Convert a durable lease, preserving exact budget reservation dimensions.

    ``landed_capacity_class`` is the class of the cell the lease actually landed
    on, and it decides ``revocable``. With a portfolio the *requested* class is no
    longer the answer: a workload that lists ``spot`` in its portfolio and lands
    on an **on-demand** cell would otherwise be marked force-terminable -- a
    silent promise violation, since nothing about that cell is preemptible.

    ``None`` falls back to the requested class, which is exactly right for the
    single-class case and is what every pre-portfolio caller means.
    """

    resources = _resources(record.resources.per_replica).scale(record.resources.replicas)
    if workload.spec.worker_resource_scope is WorkerResourceScope.PER_WORKER:
        resources = resources.scale(workload.spec.worker_count)
    return CapacityLease(
        lease_id=record.id,
        workload_id=record.workload_id,
        attempt=workload.attempt,
        identity=WorkloadIdentity(record.org_id, record.project_id, workload.created_by),
        cell_id=record.cell_id,
        resources=resources,
        priority=_priority(workload.spec.priority_band),
        hourly_cost=record.hourly_cost,
        reserved_cost=record.reserved_cost,
        resource_hours_reserved=_resources(record.resource_hours_reserved),
        checkpoint_policy=_checkpoint(workload),
        revocable=workload.spec.cell_group_count == 1
        and (
            (landed_capacity_class or workload.spec.capacity_class) is ApiCapacityClass.SPOT
            or workload.spec.checkpointable
        )
        and (policy is None or policy.preemption_enabled),
        fencing_epoch=record.generation,
    )


def to_core_budget(record: BudgetRecord) -> Budget:
    """Convert monetary and accelerator-hour hard limits."""

    return Budget(
        scope=IdentityScope(record.org_id, record.project_id),
        limit=record.limit,
        spent=record.spent,
        currency=record.currency,
        accelerator_hour_limits=ResourceVector(record.accelerator_hour_limits),
        accelerator_hours_spent=ResourceVector(record.accelerator_hours_spent),
        hard_limit=record.hard_limit,
    )


def to_core_quota(record: QuotaRecord) -> Quota:
    """Convert a hierarchical API quota to the exact core vector contract."""

    quantities = {
        "cpu": record.limits.cpu,
        "memory_gib": record.limits.memory_gib,
        "gpu": record.limits.gpu,
        "tpu": record.limits.tpu,
        **record.limits.custom,
    }
    return Quota(
        scope=IdentityScope(record.org_id, record.project_id, record.principal_id),
        limit=ResourceVector({name: quantities[name] for name in record.constrained_resources}),
        weight=record.weight,
    )


class CoreSchedulingService:
    """Adapter around :class:`eyvan.core.Scheduler`."""

    def __init__(self, scheduler: Scheduler | None = None):
        self.scheduler = scheduler or Scheduler()

    def schedule(
        self,
        workloads: tp.Iterable[WorkloadRecord],
        cells: tp.Iterable[CellRecord],
        *,
        leases: tp.Iterable[LeaseRecord] = (),
        budgets: tp.Iterable[BudgetRecord] = (),
        quotas: tp.Iterable[QuotaRecord] = (),
        policies: tp.Iterable[PolicyRecord] = (),
    ) -> SchedulingResult:
        workload_records = list(workloads)
        workload_by_id = {workload.id: workload for workload in workload_records}
        # `schedulable` excludes coordinator records -- a batch parent owns its
        # children and derives its state from them, it does not run.
        pending = [
            workload for workload in workload_records if workload.state is WorkloadState.PENDING and workload.schedulable
        ]
        active_leases = [lease for lease in leases if lease.state in ACTIVE_LEASE_STATES]
        # ``schedule`` already receives the cells; a lease record carries no
        # capacity class of its own, so this join is the only place the landed
        # class is available at conversion time.
        cell_records = list(cells)
        class_by_cell = landed_capacity_classes(cell_records)
        orphans = [lease.id for lease in active_leases if lease.workload_id not in workload_by_id]
        if orphans:
            raise ValueError(f"active lease {orphans[0]!r} references a missing workload")
        policy_by_scope = {(policy.org_id, policy.project_id): policy for policy in policies}
        return self.scheduler.schedule(
            (
                to_core_workload(workload, policy_by_scope.get((workload.org_id, workload.project_id)))
                for workload in pending
            ),
            (to_core_cell(cell) for cell in cell_records),
            leases=(
                to_core_lease(
                    lease,
                    workload_by_id[lease.workload_id],
                    policy_by_scope.get((lease.org_id, lease.project_id)),
                    class_by_cell.get(lease.cell_id),
                )
                for lease in active_leases
            ),
            quotas=(to_core_quota(quota) for quota in quotas),
            budgets=(to_core_budget(budget) for budget in budgets),
        )
