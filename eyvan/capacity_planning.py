"""Pure worker-unit planning for declarative capacity pools."""

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime

from eyvan.capacity import (
    DEFINITIVE_CAPACITY_LOSS_STATUSES,
    EXECUTION_PENDING_CAPACITY_STATUSES,
    QUEUED_CAPACITY_STATUSES,
)
from eyvan.core import SchedulingResult
from eyvan.identifiers import stable_id
from eyvan.service.models import (
    CapacityPoolRecord,
    CellRecord,
    CellState,
    LeaseRecord,
    WorkloadRecord,
    WorkloadState,
)
from eyvan.service.state import ACTIVE_LEASE_STATES

#: ``CellRecord.id`` is ``max_length=96`` and a cell id becomes a provider
#: resource name, so the minted form must fit whatever the widest counter is.
_CELL_ID_LIMIT = 96
_CELL_EPOCH_CHARS = 6


def pool_cell_prefix(pool: CapacityPoolRecord) -> str:
    """Return the provider-safe prefix for physical cells in ``pool``."""

    raw = re.sub(r"[^a-z0-9-]+", "-", f"{pool.fleet_name}-{pool.node_type}".lower()).strip("-")
    return (raw or "eyvan-cell")[:84].rstrip("-")


def pool_cell_epoch(pool: CapacityPoolRecord) -> str:
    """Return a short token naming this pool record's cell-id namespace.

    **Why cell ids cannot be a plain counter.** The control plane mints ids from
    ``len(managed) + 1`` and de-duplicates only against cells *it* currently
    holds. The agent, meanwhile, keeps a durable **tombstone** per released cell
    and refuses any later mutation at a generation not newer than the tombstone's
    -- permanently, by design, because that is what stops a resurrected cell from
    being adopted.

    Those two facts collide across time. A control plane whose database is empty
    re-creates the pool, begins at ``0001``, and walks straight into the
    tombstones a previous control plane left on the same agent. Every apply is
    rejected with "generation 1 is not newer than fenced cell ... generation 2",
    the cell is marked lost, the autoscaler mints the next id, and the fleet
    churns forever without ever provisioning. Observed live on 2026-08-01: cells
    0001..0006, twelve consecutive fence conflicts, no capacity.

    Derived from ``created_at`` specifically, not ``generation`` or
    ``initialized_at``. It is the only field that is *stable for the life of the
    pool record* and *fresh when the record is new*: ``upsert_capacity_pool``
    carries ``existing.created_at`` forward on every re-apply, so ``eyvan up``
    against a live fleet keeps minting into the same namespace, while a control
    plane starting on an empty database stamps a new one. ``generation`` moves on
    every re-apply (churning ids for no reason) and ``initialized_at`` is cleared
    to ``None`` by ``apply_fleet`` and set later by the controller, so it would
    flip the namespace midway through a pool's life.
    """

    return stable_id("cellns", pool.id, pool.created_at.isoformat()).split("_", 1)[1][:_CELL_EPOCH_CHARS]


def pool_cell_id(pool: CapacityPoolRecord, index: int) -> str:
    """Return the ``index``-th physical cell id for ``pool``.

    Owns the whole format so the length budget is enforced in one place: the
    counter is what gets the remaining room, since truncating *it* would alias
    two distinct cells onto one id.
    """

    epoch = pool_cell_epoch(pool)
    suffix = f"-{epoch}-{index:04d}"
    return f"{pool_cell_prefix(pool)[: _CELL_ID_LIMIT - len(suffix)].rstrip('-')}{suffix}"


def cell_matches_pool(cell: CellRecord, pool: CapacityPoolRecord) -> bool:
    """Return whether a cell matches its pool template and physical shape."""

    desired = pool.make_cell(cell.id, worker_count=cell.worker_count)
    return cell_matches_template(cell, desired)


def cell_matches_template(cell: CellRecord, desired: CellRecord) -> bool:
    """Return whether a cell has the requested provisionable template."""

    return (
        cell.provider == desired.provider
        and cell.capacity_backend == desired.capacity_backend
        and cell.provider_project == desired.provider_project
        and cell.region == desired.region
        and cell.zone == desired.zone
        and cell.capacity == desired.capacity
        and cell.allocatable == desired.allocatable
        and cell.unit_prices == desired.unit_prices
        and cell.accelerator_type == desired.accelerator_type
        and cell.capacity_class == desired.capacity_class
        and cell.executor == desired.executor
        and cell.endpoint == desired.endpoint
        and cell.pool_id == desired.pool_id
        and cell.provider_options == desired.provider_options
        and cell.runtime == desired.runtime
        and cell.labels == desired.labels
        and cell.topology == desired.topology
        and cell.max_replica_resources == desired.max_replica_resources
        and cell.max_gang_replicas == desired.max_gang_replicas
        and cell.worker_count == desired.worker_count
    )


def cell_requires_replacement(cell: CellRecord, pool: CapacityPoolRecord) -> bool:
    """Return whether mutable metadata cannot reconcile a cell to its pool."""

    desired = pool.make_cell(cell.id, worker_count=cell.worker_count)
    return (
        cell.provider != desired.provider
        or cell.capacity_backend != desired.capacity_backend
        or cell.provider_project != desired.provider_project
        or cell.region != desired.region
        or cell.zone != desired.zone
        or cell.capacity != desired.capacity
        or cell.accelerator_type != desired.accelerator_type
        or cell.capacity_class != desired.capacity_class
        or cell.executor != desired.executor
        or cell.endpoint != desired.endpoint
        or cell.pool_id != desired.pool_id
        or cell.provider_options != desired.provider_options
        or cell.runtime != desired.runtime
        or cell.worker_count != desired.worker_count
    )


def cell_provides_pool_supply(cell: CellRecord, pool: CapacityPoolRecord, now: datetime) -> bool:
    """Return whether a real cell supplies or is still acquiring its worker units."""

    if not cell_matches_pool(cell, pool):
        return False
    phase = cell.provider_phase
    if phase in DEFINITIVE_CAPACITY_LOSS_STATUSES:
        return False
    if cell.state is CellState.READY:
        return True
    if cell.state not in {CellState.REGISTERING, CellState.UNAVAILABLE}:
        return False
    phase_started_at = cell.provider_phase_started_at or cell.created_at
    elapsed_s = max(0.0, (now - phase_started_at).total_seconds())
    if phase in QUEUED_CAPACITY_STATUSES:
        return pool.queue_timeout_s is None or elapsed_s < pool.queue_timeout_s
    return elapsed_s < pool.provisioning_timeout_s


def cell_execution_runtime_is_pending(cell: CellRecord) -> bool:
    """Return whether capacity exists but its native execution worker is unready."""

    return cell.provider_phase in EXECUTION_PENDING_CAPACITY_STATUSES


@dataclass(frozen=True, slots=True)
class PoolDemand:
    """Scheduler-selected real capacity and exact virtual cell shapes."""

    real_cell_ids: frozenset[str]
    virtual_worker_counts: tuple[int, ...]
    virtual_workload_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        """Keep each virtual capacity shape attached to its requesting workload."""

        if len(self.virtual_worker_counts) != len(self.virtual_workload_ids):
            raise ValueError("virtual worker counts must align with virtual workload ids")


@dataclass(frozen=True, slots=True)
class CapacitySimulation:
    """Scheduler inventory plus ownership metadata for virtual capacity."""

    cells: tuple[CellRecord, ...]
    pool_by_cell_id: dict[str, str]
    virtual_worker_count_by_cell_id: dict[str, int]

    @classmethod
    def build(
        cls,
        pools: tuple[CapacityPoolRecord, ...],
        cells: tuple[CellRecord, ...],
        workloads: tuple[WorkloadRecord, ...],
        leases: tuple[LeaseRecord, ...],
        now: datetime,
    ) -> CapacitySimulation:
        """Build real supply and shape-exact virtual candidates for pending work."""

        pools_by_id = {pool.id: pool for pool in pools}
        scheduler_cells: list[CellRecord] = []
        pool_by_cell_id: dict[str, str] = {}
        virtual_worker_count_by_cell_id: dict[str, int] = {}
        active_lease_cells = {lease.cell_id for lease in leases if lease.state in ACTIVE_LEASE_STATES}
        free_shared_exact_supply: Counter[tuple[str, int]] = Counter()
        free_owned_exact_supply: Counter[tuple[str, int, str]] = Counter()

        for cell in cells:
            pool = pools_by_id.get(cell.pool_id or "")
            if pool is None:
                scheduler_cells.append(cell)
                continue
            pool_by_cell_id[cell.id] = pool.id
            if not pool.enabled or not cell_provides_pool_supply(cell, pool, now):
                continue
            if cell.state is CellState.READY:
                scheduler_cells.append(cell)
            elif cell.state in {CellState.REGISTERING, CellState.UNAVAILABLE}:
                scheduler_cells.append(cell.model_copy(update={"state": CellState.READY}))
            if cell.id not in active_lease_cells:
                if cell.autoscale_request_workload_id is None:
                    free_shared_exact_supply[(pool.id, cell.worker_count)] += 1
                else:
                    free_owned_exact_supply[(pool.id, cell.worker_count, cell.autoscale_request_workload_id)] += 1

        generic_workloads: list[WorkloadRecord] = []
        for workload in workloads:
            if workload.state is not WorkloadState.PENDING:
                continue
            exact_pool_id = workload.spec.required_labels.get("eyvan_pool")
            exact_pool = pools_by_id.get(exact_pool_id or "")
            if exact_pool is None or not exact_pool.enabled:
                generic_workloads.append(workload)
                continue
            for member in range(workload.spec.cell_group_count):
                supply_key = (exact_pool.id, workload.spec.worker_count)
                owned_supply_key = (*supply_key, workload.id)
                if free_owned_exact_supply[owned_supply_key]:
                    free_owned_exact_supply[owned_supply_key] -= 1
                    continue
                if free_shared_exact_supply[supply_key]:
                    free_shared_exact_supply[supply_key] -= 1
                    continue
                cell_id = stable_id("virtual_cell", exact_pool.id, workload.id, member)
                pool_by_cell_id[cell_id] = exact_pool.id
                virtual_worker_count_by_cell_id[cell_id] = workload.spec.worker_count
                scheduler_cells.append(
                    exact_pool.make_cell(cell_id, worker_count=workload.spec.worker_count).model_copy(
                        update={"state": CellState.READY}
                    )
                )

        for pool in pools:
            if not pool.enabled:
                continue
            for workload in generic_workloads:
                worker_count = workload.spec.worker_count
                if worker_count > 1 and (
                    pool.provider != "gcp" or pool.capacity_backend != "gcp-tpu" or pool.executor != "eyvan-native"
                ):
                    # Multi-worker cells only exist on the GCP TPU backend with the
                    # native executor; other pools can never host this shape.
                    continue
                for member in range(workload.spec.cell_group_count):
                    cell_id = stable_id("virtual_cell", pool.id, "generic", workload.id, member)
                    pool_by_cell_id[cell_id] = pool.id
                    virtual_worker_count_by_cell_id[cell_id] = worker_count
                    scheduler_cells.append(
                        pool.make_cell(cell_id, worker_count=worker_count).model_copy(update={"state": CellState.READY})
                    )

        return cls(
            cells=tuple(scheduler_cells),
            pool_by_cell_id=pool_by_cell_id,
            virtual_worker_count_by_cell_id=virtual_worker_count_by_cell_id,
        )

    def demand(
        self,
        pools: tuple[CapacityPoolRecord, ...],
        leases: tuple[LeaseRecord, ...],
        result: SchedulingResult,
    ) -> dict[str, PoolDemand]:
        """Resolve scheduler placements to physical cell shapes per pool."""

        real_ids: dict[str, set[str]] = {pool.id: set() for pool in pools}
        virtual_counts: dict[str, list[int]] = {pool.id: [] for pool in pools}
        virtual_workload_ids: dict[str, list[str]] = {pool.id: [] for pool in pools}
        for lease in leases:
            if lease.state not in ACTIVE_LEASE_STATES:
                continue
            pool_id = self.pool_by_cell_id.get(lease.cell_id)
            if pool_id in real_ids:
                real_ids[pool_id].add(lease.cell_id)
        for placement in result.placements:
            for lease in placement.all_leases:
                pool_id = self.pool_by_cell_id.get(lease.cell_id)
                if pool_id not in real_ids:
                    continue
                worker_count = self.virtual_worker_count_by_cell_id.get(lease.cell_id)
                if worker_count is None:
                    real_ids[pool_id].add(lease.cell_id)
                else:
                    virtual_counts[pool_id].append(worker_count)
                    virtual_workload_ids[pool_id].append(lease.workload_id)
        return {
            pool.id: PoolDemand(
                real_cell_ids=frozenset(real_ids[pool.id]),
                virtual_worker_counts=tuple(virtual_counts[pool.id]),
                virtual_workload_ids=tuple(virtual_workload_ids[pool.id]),
            )
            for pool in pools
        }


@dataclass(frozen=True, slots=True)
class PoolScaleIntent:
    """One pool's desired worker-unit changes before shared quota allocation."""

    pool: CapacityPoolRecord
    live: tuple[CellRecord, ...]
    matching_supply: tuple[CellRecord, ...]
    demand: PoolDemand
    target_worker_units: int
    requested_worker_counts: tuple[int, ...]
    requested_exact_count: int
    demanded_virtual_worker_counts: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class PoolScalePlan:
    """Atomic launch and retention plan after max-worker and quota limits."""

    pool: CapacityPoolRecord
    live: tuple[CellRecord, ...]
    matching_supply: tuple[CellRecord, ...]
    target_worker_units: int
    launch_worker_counts: tuple[int, ...]
    launch_workload_ids: tuple[str | None, ...]
    protected_cell_ids: frozenset[str]
    forced_drain_cell_ids: frozenset[str]

    def __post_init__(self) -> None:
        """Keep every planned physical cell associated with an optional source workload."""

        if len(self.launch_worker_counts) != len(self.launch_workload_ids):
            raise ValueError("planned worker counts must align with planned workload ids")

    @property
    def supply_worker_units_after_launch(self) -> int:
        return sum(cell.worker_count for cell in self.matching_supply) + sum(self.launch_worker_counts)


@dataclass(frozen=True, slots=True)
class QuotaAllocation:
    """Shared-quota launches and same-pool units that must be reclaimed."""

    launch_worker_counts: dict[str, tuple[int, ...]]
    reclaim_worker_units: dict[str, int]
    blocked_workload_ids: frozenset[str]


class CapacityPlanner:
    """Plan atomic physical cells while pool bounds remain worker-unit bounds."""

    def __init__(
        self,
        pools: tuple[CapacityPoolRecord, ...],
        cells_by_pool: dict[str, list[CellRecord]],
        now: datetime,
    ):
        self._pools = pools
        self._cells_by_pool = cells_by_pool
        self._now = now
        self._quota_blocked_workload_ids: frozenset[str] = frozenset()

    @property
    def quota_blocked_workload_ids(self) -> frozenset[str]:
        """Return exact pending workloads rejected by a finite shared provider quota."""

        return self._quota_blocked_workload_ids

    def plan(self, demand_by_pool: dict[str, PoolDemand]) -> dict[str, PoolScalePlan]:
        """Return quota-safe atomic changes for every pool."""

        intents = {pool.id: self._intent(pool, demand_by_pool[pool.id]) for pool in self._pools}
        quota = self._allocate_shared_quota(intents)
        self._quota_blocked_workload_ids = quota.blocked_workload_ids
        return {
            pool.id: self._finalize(
                intents[pool.id],
                quota.launch_worker_counts[pool.id],
                quota.reclaim_worker_units[pool.id],
            )
            for pool in self._pools
        }

    def _intent(self, pool: CapacityPoolRecord, demand: PoolDemand) -> PoolScaleIntent:
        managed = tuple(sorted(self._cells_by_pool[pool.id], key=lambda cell: cell.id))
        live = tuple(cell for cell in managed if cell.state is not CellState.DELETED)
        matching = tuple(cell for cell in live if cell_provides_pool_supply(cell, pool, self._now))
        floor = pool.initial_target if pool.enabled and pool.initialized_at is None else pool.warm_floor(self._now)
        real_by_id = {cell.id: cell for cell in matching}
        required_real_units = sum(
            real_by_id[cell_id].worker_count for cell_id in demand.real_cell_ids if cell_id in real_by_id
        )
        demanded_virtual_units = sum(demand.virtual_worker_counts)
        target = (
            0
            if not pool.enabled
            else min(
                pool.max_cells,
                max(floor, required_real_units + demanded_virtual_units),
            )
        )
        occupied_units = sum(cell.worker_count for cell in live)
        available_units = max(pool.max_cells - occupied_units, 0)
        matching_units = sum(cell.worker_count for cell in matching)
        rate_limit = (
            pool.launch_batch
            if not matching
            else min(pool.launch_batch, max(1, math.ceil(len(matching) * pool.upscaling_speed)))
        )
        requested: list[int] = []
        for worker_count in demand.virtual_worker_counts:
            if len(requested) >= rate_limit or worker_count > available_units:
                break
            requested.append(worker_count)
            available_units -= worker_count
        requested_exact_count = len(requested)
        floor_deficit = max(target - matching_units - sum(requested), 0)
        while floor_deficit and available_units and len(requested) < rate_limit:
            requested.append(1)
            floor_deficit -= 1
            available_units -= 1
        return PoolScaleIntent(
            pool=pool,
            live=live,
            matching_supply=matching,
            demand=demand,
            target_worker_units=target,
            requested_worker_counts=tuple(requested),
            requested_exact_count=requested_exact_count,
            demanded_virtual_worker_counts=demand.virtual_worker_counts,
        )

    def _allocate_shared_quota(self, intents: dict[str, PoolScaleIntent]) -> QuotaAllocation:
        allowed = {pool_id: intent.requested_worker_counts for pool_id, intent in intents.items()}
        reclaim_worker_units = {pool_id: 0 for pool_id in intents}
        blocked_workload_ids: set[str] = set()
        grouped: dict[str, list[PoolScaleIntent]] = {}
        for intent in intents.values():
            if intent.pool.quota_group is not None:
                grouped.setdefault(intent.pool.quota_group, []).append(intent)

        for members in grouped.values():
            group_limit = min(intent.pool.quota_limit for intent in members)
            if group_limit is None:
                raise ValueError("quota group pools must configure quota_limit")
            live_units = sum(
                cell.worker_count * intent.pool.quota_units_per_cell
                for intent in members
                for cell in intent.live
                if intent.pool.quota_units_per_cell is not None
            )
            remaining_units = max(group_limit - live_units, 0)
            accepted: dict[str, list[int]] = {intent.pool.id: [] for intent in members}
            while remaining_units:
                candidates: list[tuple[PoolScaleIntent, int, int]] = []
                for intent in members:
                    accepted_count = len(accepted[intent.pool.id])
                    if accepted_count >= len(intent.requested_worker_counts):
                        continue
                    units_per_worker = intent.pool.quota_units_per_cell
                    if units_per_worker is None:
                        raise ValueError("quota group pools must configure quota_units_per_cell")
                    worker_count = intent.requested_worker_counts[accepted_count]
                    quota_cost = worker_count * units_per_worker
                    if quota_cost <= remaining_units:
                        candidates.append((intent, worker_count, quota_cost))
                if not candidates:
                    break
                intent, worker_count, quota_cost = min(
                    candidates,
                    key=lambda candidate: (
                        len(accepted[candidate[0].pool.id]) / len(candidate[0].requested_worker_counts),
                        candidate[2],
                        candidate[0].pool.id,
                    ),
                )
                accepted[intent.pool.id].append(worker_count)
                remaining_units -= quota_cost
            for intent in members:
                allowed[intent.pool.id] = tuple(accepted[intent.pool.id])
                accepted_exact_count = min(len(accepted[intent.pool.id]), intent.requested_exact_count)
                blocked_workload_ids.update(
                    intent.demand.virtual_workload_ids[accepted_exact_count : intent.requested_exact_count]
                )
            denied_exact = [intent for intent in members if len(accepted[intent.pool.id]) < intent.requested_exact_count]
            if denied_exact:
                intent = min(
                    denied_exact,
                    key=lambda candidate: (
                        len(accepted[candidate.pool.id]) / len(candidate.requested_worker_counts),
                        candidate.requested_worker_counts[len(accepted[candidate.pool.id])],
                        candidate.pool.id,
                    ),
                )
                accepted_count = len(accepted[intent.pool.id])
                worker_count = intent.requested_worker_counts[accepted_count]
                units_per_worker = intent.pool.quota_units_per_cell
                if units_per_worker is None:
                    raise ValueError("quota group pools must configure quota_units_per_cell")
                quota_shortage = worker_count * units_per_worker - remaining_units
                reclaim_worker_units[intent.pool.id] = math.ceil(quota_shortage / units_per_worker)
        return QuotaAllocation(
            launch_worker_counts=allowed,
            reclaim_worker_units=reclaim_worker_units,
            blocked_workload_ids=frozenset(blocked_workload_ids),
        )

    def _finalize(
        self,
        intent: PoolScaleIntent,
        launch_worker_counts: tuple[int, ...],
        quota_reclaim_worker_units: int,
    ) -> PoolScalePlan:
        if launch_worker_counts != intent.requested_worker_counts[: len(launch_worker_counts)]:
            raise ValueError("quota allocation must preserve the requested launch order")
        matching_by_id = {cell.id: cell for cell in intent.matching_supply}
        protected = {cell_id for cell_id in intent.demand.real_cell_ids if cell_id in matching_by_id}
        protected.update(cell.id for cell in intent.matching_supply if cell.autoscale_request_workload_id is not None)
        occupied_units = sum(cell.worker_count for cell in intent.live)
        available_after_launch = max(
            intent.pool.max_cells - occupied_units - sum(launch_worker_counts),
            0,
        )
        reclaiming_units = sum(
            cell.worker_count for cell in intent.live if cell.state in {CellState.DRAINING, CellState.DELETING}
        )
        available_after_reclaim = available_after_launch + reclaiming_units
        launched_counts = Counter(launch_worker_counts)
        unlaunched_exact: list[int] = []
        for worker_count in intent.demanded_virtual_worker_counts:
            if launched_counts[worker_count]:
                launched_counts[worker_count] -= 1
            else:
                unlaunched_exact.append(worker_count)

        blocked_worker_count = (
            unlaunched_exact[0]
            if unlaunched_exact
            else 1
            if intent.target_worker_units
            > sum(cell.worker_count for cell in intent.matching_supply) + sum(launch_worker_counts)
            else None
        )
        forced_drain: set[str] = set()
        capacity_units_to_free = (
            blocked_worker_count - available_after_reclaim
            if blocked_worker_count is not None and blocked_worker_count > available_after_reclaim
            else 0
        )
        quota_units_to_free = max(quota_reclaim_worker_units - reclaiming_units, 0) if unlaunched_exact else 0
        units_to_free = max(capacity_units_to_free, quota_units_to_free)
        if units_to_free:
            drainable = sorted(
                (
                    cell
                    for cell in intent.live
                    if cell.id not in protected and cell.state not in {CellState.DRAINING, CellState.DELETING}
                ),
                key=lambda cell: (cell.worker_count, cell.id),
            )
            freed = 0
            for cell in drainable:
                forced_drain.add(cell.id)
                freed += cell.worker_count
                if freed >= units_to_free:
                    break

        protected_units = sum(matching_by_id[cell_id].worker_count for cell_id in protected)
        required_existing_units = max(intent.target_worker_units - sum(launch_worker_counts), 0)
        for cell in sorted(intent.matching_supply, key=lambda candidate: (candidate.worker_count, candidate.id)):
            if protected_units >= required_existing_units:
                break
            if cell.id in protected or cell.id in forced_drain:
                continue
            protected.add(cell.id)
            protected_units += cell.worker_count

        launched_exact_count = min(len(launch_worker_counts), intent.requested_exact_count)
        launch_workload_ids = intent.demand.virtual_workload_ids[:launched_exact_count] + (None,) * (
            len(launch_worker_counts) - launched_exact_count
        )

        return PoolScalePlan(
            pool=intent.pool,
            live=intent.live,
            matching_supply=intent.matching_supply,
            target_worker_units=intent.target_worker_units,
            launch_worker_counts=launch_worker_counts,
            launch_workload_ids=launch_workload_ids,
            protected_cell_ids=frozenset(protected),
            forced_drain_cell_ids=frozenset(forced_drain),
        )
