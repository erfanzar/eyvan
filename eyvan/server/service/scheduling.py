# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Isolated adapter from persisted API records to the pure core scheduler."""

from __future__ import annotations

import typing as tp
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal

from eyvan.domain import (
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
from eyvan.domain import (
    PriorityBand as CorePriorityBand,
)
from eyvan.domain.availability import p95_latency
from eyvan.records import (
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
    resource_quantities,
    utcnow,
)
from eyvan.records import (
    CapacityClass as ApiCapacityClass,
)
from eyvan.records import (
    ResourceVector as ApiResourceVector,
)
from eyvan.server.service.federation import BudgetGrantRecord, QuotaGrantRecord
from eyvan.server.service.state import ACTIVE_LEASE_STATES


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
        grants: tp.Iterable[BudgetGrantRecord] = (),
        quota_grants: tp.Iterable[QuotaGrantRecord] = (),
        plane_id: str = "",
        now: datetime | None = None,
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


def _checkpoint(record: WorkloadRecord, landed_capacity_class: ApiCapacityClass | None = None) -> CheckpointPolicy:
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
        #
        # The class that decides is the one the lease *landed* on -- the same
        # class ``revocable`` keys on. With a portfolio, a workload that
        # requested on-demand can land on a spot cell; keying this off the
        # requested class left that lease revocable but still ineligible, and
        # the scheduler answered PREEMPTION_UNSAFE for capacity that was
        # reclaimable by definition. ``None`` falls back to the requested
        # class, right for a workload that has not landed anywhere yet.
        allow_forced_termination=(landed_capacity_class or spec.capacity_class) is ApiCapacityClass.SPOT,
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
        # A multi-cell gang is preemptible as one unit: the scheduler takes
        # every sibling lease or none (``_preemption_candidate``), and
        # ``NativeGangExecutor.checkpoint`` runs the hook on every cell under one
        # shared checkpoint ID. Exposing a single member as an individual victim
        # was the hazard, not multi-cell membership itself.
        revocable=(spec.capacity_class is ApiCapacityClass.SPOT or spec.checkpointable)
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
    on, and it decides ``revocable`` *and* ``allow_forced_termination``. With a
    portfolio the *requested* class is no longer the answer: a workload that
    lists ``spot`` in its portfolio and lands on an **on-demand** cell would
    otherwise be marked force-terminable -- a silent promise violation, since
    nothing about that cell is preemptible. The reverse matters too: a workload
    that requested on-demand and landed on a **spot** cell is preemptable
    whether or not it can checkpoint, and keying the checkpoint policy off the
    requested class left it revocable but never an eligible victim
    (PREEMPTION_UNSAFE on capacity reclaimable by definition).

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
        checkpoint_policy=_checkpoint(workload, landed_capacity_class),
        # Gangs are preemptible as one unit; see to_core_workload.
        revocable=(
            (landed_capacity_class or workload.spec.capacity_class) is ApiCapacityClass.SPOT
            or workload.spec.checkpointable
        )
        and (policy is None or policy.preemption_enabled),
        fencing_epoch=record.generation,
    )


_PERIOD_SECONDS: tp.Final[dict[str, float]] = {
    "hourly": 3_600.0,
    "daily": 86_400.0,
    "weekly": 604_800.0,
    "monthly": 2_592_000.0,
    "quarterly": 7_776_000.0,
    "yearly": 31_536_000.0,
}


def rolled_budget(record: BudgetRecord, now: datetime) -> BudgetRecord:
    """Reset a budget whose declared period has elapsed.

    Port of the control plane's ``_rolled_budget`` (``_settle_lease`` rolls only
    where money moves): ``spent`` is a monotone total over the declared window,
    so admission reading it raw compares spend from a crossed period against a
    fresh limit -- a hard budget then refuses every placement forever once the
    window closes with no in-flight lease to trigger a settlement. Mirrors the
    control-plane semantics exactly: unknown periods never roll, a missing
    ``period_started_at`` is stamped without a reset, whole elapsed periods
    advance at once so the window stays aligned to its origin, and per-period
    alerts re-arm.
    """

    length = _PERIOD_SECONDS.get(record.period.strip().lower())
    if length is None:
        return record
    started = record.period_started_at
    if started is None:
        return record.model_copy(update={"period_started_at": now})
    if (now - started).total_seconds() < length:
        return record
    elapsed_periods = int((now - started).total_seconds() // length)
    return record.model_copy(
        update={
            "spent": 0.0,
            "accelerator_hours_spent": {},
            "alerted_thresholds": (),
            "period_started_at": started + timedelta(seconds=length * elapsed_periods),
        }
    )


def to_core_budget(record: BudgetRecord, *, now: datetime | None = None) -> Budget:
    """Convert monetary and accelerator-hour hard limits.

    The record's ``spent`` is only meaningful inside its declared period, so the
    window is rolled (:func:`rolled_budget`) before conversion. Callers that
    already hold a clock should pass it as ``now``; defaulting to the current
    time keeps the pure read path honest without inventing a second notion of
    "now" per plane.
    """

    record = rolled_budget(record, now or utcnow())
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


def federated_budgets(
    records: tp.Sequence[BudgetRecord],
    grants: tp.Sequence[BudgetGrantRecord],
    *,
    plane_id: str,
    now: datetime,
) -> list[Budget]:
    """Bound this plane's admission by the grants it holds, not by a local counter.

    ``BudgetRecord.spent`` is one durable counter in one database. A tenant with
    projects on three planes therefore had three independent budgets, each
    enforcing its own limit and each reporting full compliance while the tenant
    spent three times what its operator set. Grants existed to fix that and
    nothing consulted them: ``list_budget_grants`` was called from issuance,
    status and expiry, and never from admission. A grant was recorded and
    licensed nothing.

    Three rules, and the second and third are the ones that matter:

    * A plane holding no grants for a tenant is unchanged. That is the home
      plane, and every non-federated deployment.
    * A grantee's effective limit is the **tighter** of its grant total and any
      local limit. A grant may not raise a ceiling a local operator set, and a
      local record may not exceed the slice the home plane issued.
    * A grant is a **hard** ceiling even when the local budget is soft.
      Otherwise a grantee overspends its slice and the bound the whole mechanism
      provides is void.

    A grantee with grants and no local budget record gets a synthesized one, so
    the gate exists at all. Without it, the plane with the least local
    configuration would be the one with no limit -- exactly backwards.
    """

    granted: dict[tuple[str, str], float] = {}
    # The empty check is a short-circuit, not the safety property. What actually
    # keeps a non-federated plane from matching is ``grantee_plane_id``'s
    # ``min_length=1``: no grant can be addressed to "".
    if plane_id:
        for grant in grants:
            if grant.grantee_plane_id == plane_id and grant.active_at(now):
                key = (grant.org_id, grant.project_id)
                granted[key] = granted.get(key, 0.0) + grant.amount
    budgets: list[Budget] = []
    seen: set[tuple[str, str]] = set()
    for record in records:
        key = (record.org_id, record.project_id)
        seen.add(key)
        allowance = granted.get(key)
        if allowance is None:
            budgets.append(to_core_budget(record, now=now))
            continue
        budgets.append(replace(to_core_budget(record, now=now), limit=min(allowance, record.limit), hard_limit=True))
    for key, allowance in sorted(granted.items()):
        if key in seen:
            continue
        org_id, project_id = key
        budgets.append(
            Budget(
                scope=IdentityScope(org_id, project_id),
                limit=allowance,
                spent=0.0,
                currency="USD",
                hard_limit=True,
            )
        )
    return budgets


def federated_quotas(
    records: tp.Sequence[QuotaRecord],
    grants: tp.Sequence[QuotaGrantRecord],
    *,
    plane_id: str,
    now: datetime,
) -> list[Quota]:
    """Bound this plane's placement by the quota slices it holds.

    The capacity half of :func:`federated_budgets`, and it exists for the same
    reason: ``QuotaRecord.limits`` is one vector in one database, so a tenant with
    projects on three planes could run three times its accelerator ceiling with
    every plane locally compliant. Money and chips are separate limits, and the
    scarce thing in a shared fleet is usually chips.

    The same three rules:

    * A plane holding no quota grants for a tenant is unchanged -- the home plane,
      and every non-federated deployment.
    * The effective limit is the **tighter** of the grant and any local quota,
      per dimension. A grant may not raise a ceiling a local operator set, and a
      local record may not exceed the slice the home plane issued.
    * A grantee with grants and no local quota record gets the grant as its
      limit. Without that, the plane with the least local configuration would be
      the one with no ceiling -- exactly backwards.

    Only project-scoped records are federated. Principal-scoped quotas subdivide
    a project's share *within* one plane, and a cross-plane grant has nothing to
    say about how a tenant splits its own slice between its own people.
    """

    granted: dict[tuple[str, str], dict[str, float]] = {}
    # As with budgets, the ``plane_id`` check is a short-circuit rather than the
    # safety property: ``grantee_plane_id`` has ``min_length=1``, so no grant can
    # be addressed to "".
    if plane_id:
        for grant in grants:
            if grant.grantee_plane_id != plane_id or not grant.active_at(now):
                continue
            key = (grant.org_id, grant.project_id)
            totals = granted.setdefault(key, {})
            for name, quantity in resource_quantities(grant.limits).items():
                if quantity:
                    totals[name] = totals.get(name, 0.0) + quantity
    quotas: list[Quota] = []
    seen: set[tuple[str, str]] = set()
    for record in records:
        if record.principal_id is not None or record.project_id is None:
            quotas.append(to_core_quota(record))
            continue
        key = (record.org_id, record.project_id)
        allowance = granted.get(key)
        if allowance is None:
            quotas.append(to_core_quota(record))
            continue
        seen.add(key)
        local = to_core_quota(record)
        # A dimension the grant does not mention is zero, not unbounded: a slice
        # that silently authorized everything it forgot to name would be the
        # opposite of a ceiling. A dimension the *local* quota does not constrain
        # is not introduced here either -- the grant cannot add a constraint the
        # tenant's own operator did not ask for on a plane that is not the home.
        bounded = {
            name: min(quantity, Decimal(str(allowance.get(name, 0.0))))
            for name, quantity in local.limit.as_dict().items()
        }
        quotas.append(replace(local, limit=ResourceVector(bounded)))
    for key, allowance in sorted(granted.items()):
        if key in seen:
            continue
        org_id, project_id = key
        quotas.append(Quota(scope=IdentityScope(org_id, project_id), limit=ResourceVector(dict(allowance)), weight=1.0))
    return quotas


class CoreSchedulingService:
    """Adapter around :class:`eyvan.domain.Scheduler`."""

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
        grants: tp.Iterable[BudgetGrantRecord] = (),
        quota_grants: tp.Iterable[QuotaGrantRecord] = (),
        plane_id: str = "",
        now: datetime | None = None,
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
            quotas=federated_quotas(list(quotas), list(quota_grants), plane_id=plane_id, now=now or utcnow()),
            budgets=federated_budgets(list(budgets), list(grants), plane_id=plane_id, now=now or utcnow()),
        )
