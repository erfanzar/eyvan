# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Behavioral tests for pure capacity planning over simulated pool demand."""

from __future__ import annotations

import re
from datetime import timedelta

import pytest

from eyvan.capacity.planning import (
    CapacityPlanner,
    CapacitySimulation,
    PoolDemand,
    cell_matches_pool,
    cell_requires_replacement,
)
from eyvan.records import (
    CapacityPoolRecord,
    CellRecord,
    CellState,
    CommandExecution,
    LeaseRecord,
    Principal,
    ResourceGang,
    ResourceLimits,
    ResourceVector,
    Role,
    RoleGrant,
    WorkerResourceScope,
    WorkloadDefinition,
    WorkloadRecord,
    WorkloadState,
    utcnow,
)
from eyvan.server.controller import EyvanController
from eyvan.server.persistence import InMemoryRepository
from eyvan.server.service import ControlPlaneService
from eyvan.server.service.scheduling import CoreSchedulingService


def fleet_admin() -> Principal:
    return Principal(subject="admin", grants=(RoleGrant(role=Role.FLEET_ADMIN),))


def operator() -> Principal:
    return Principal(
        subject="operator",
        grants=(RoleGrant(role=Role.PROJECT_OPERATOR, org_id="acme", project_id="research"),),
    )


def cpu_pool(pool_id: str = "research/cpu", *, max_cells: int = 4) -> CapacityPoolRecord:
    return CapacityPoolRecord(
        id=pool_id,
        fleet_name="research",
        node_type=pool_id.rsplit("/", 1)[-1],
        provider="gcp",
        capacity_backend="gcp-compute",
        provider_project="project-a",
        region="us-central1",
        zone="us-central1-a",
        capacity={"cpu": 4, "memory_gib": 16},
        allocatable={"cpu": 4, "memory_gib": 16},
        executor="native-local",
        agent_endpoint="http://agent:8090",
        provider_options={"kind": "cpu", "machine_type": "n2-standard-4"},
        max_cells=max_cells,
    )


def multislice_tpu_pool(pool_id: str = "research/v5p-16", *, max_cells: int = 16) -> CapacityPoolRecord:
    """Return the autoscaler template for one v5p-16 TPU worker."""

    return CapacityPoolRecord(
        id=pool_id,
        fleet_name="research",
        node_type=pool_id.rsplit("/", 1)[-1],
        provider="gcp",
        capacity_backend="gcp-tpu",
        provider_project="project-a",
        region="us-east5",
        zone="us-east5-a",
        capacity={"cpu": 32, "tpu": 8},
        allocatable={"cpu": 32, "tpu": 8},
        accelerator_type="v5p-16",
        executor="eyvan-native",
        agent_endpoint="http://agent:8090",
        max_cells=max_cells,
        launch_batch=8,
    )


def shared_quota_pool(
    pool: CapacityPoolRecord,
    *,
    group: str,
    limit: int | None,
    units_per_cell: int | None,
) -> CapacityPoolRecord:
    """Attach a shared quota declaration without re-running pool validators."""

    return pool.model_copy(update={"quota_group": group, "quota_limit": limit, "quota_units_per_cell": units_per_cell})


def generic_multislice_workload(workload_id: str = "workload-multislice", worker_count: int = 2) -> WorkloadRecord:
    return WorkloadRecord(
        id=workload_id,
        org_id="acme",
        project_id="research",
        created_by="operator",
        spec=WorkloadDefinition(
            name="multislice-train",
            execution=CommandExecution(command="python train.py"),
            gang={"replicas": 1, "per_replica": {"cpu": 32, "memory_gib": 0, "tpu": 8}},
            provider="gcp",
            required_executor="eyvan-native",
            worker_count=worker_count,
            worker_resource_scope=WorkerResourceScope.PER_WORKER,
        ),
    )


def generic_cpu_workload(workload_id: str = "workload-cpu") -> WorkloadRecord:
    return WorkloadRecord(
        id=workload_id,
        org_id="acme",
        project_id="research",
        created_by="operator",
        spec=WorkloadDefinition(
            name="cpu-train",
            execution=CommandExecution(command="python train.py"),
            gang={"replicas": 1, "per_replica": {"cpu": 4, "memory_gib": 16}},
        ),
    )


def demand_for(pool: CapacityPoolRecord, worker_counts: tuple[int, ...], workload_ids: tuple[str, ...]) -> PoolDemand:
    return PoolDemand(
        real_cell_ids=frozenset(),
        virtual_worker_counts=worker_counts,
        virtual_workload_ids=workload_ids,
    )


def test_generic_multislice_workload_gets_shape_exact_virtual_capacity_and_scale_up_intent() -> None:
    pool = multislice_tpu_pool()
    workload = generic_multislice_workload(worker_count=2)
    now = utcnow()

    simulation = CapacitySimulation.build((pool,), (), (workload,), (), now)

    assert len(simulation.cells) == 1
    virtual_cell = simulation.cells[0]
    assert virtual_cell.state is CellState.READY
    assert virtual_cell.worker_count == 2
    assert virtual_cell.allocatable == ResourceLimits(cpu=64, tpu=16)
    assert simulation.pool_by_cell_id == {virtual_cell.id: pool.id}
    assert simulation.virtual_worker_count_by_cell_id == {virtual_cell.id: 2}

    result = CoreSchedulingService().schedule((workload,), simulation.cells)
    assert [placement.workload_id for placement in result.placements] == [workload.id]

    demand = simulation.demand((pool,), (), result)
    assert demand[pool.id].virtual_worker_counts == (2,)
    assert demand[pool.id].virtual_workload_ids == (workload.id,)

    planner = CapacityPlanner((pool,), {pool.id: []}, now)
    plan = planner.plan(demand)[pool.id]
    assert plan.launch_worker_counts == (2,)
    assert plan.launch_workload_ids == (workload.id,)
    assert plan.target_worker_units == 2


def test_generic_multislice_workload_skips_pools_without_a_multiworker_backend() -> None:
    pool = cpu_pool()
    workload = generic_multislice_workload(worker_count=2)

    simulation = CapacitySimulation.build((pool,), (), (workload,), (), utcnow())

    assert simulation.cells == ()
    assert simulation.virtual_worker_count_by_cell_id == {}
    result = CoreSchedulingService().schedule((workload,), simulation.cells)
    assert result.placements == ()
    assert [failure.workload_id for failure in result.infeasibilities] == [workload.id]


def test_generic_single_worker_workload_still_triggers_scale_up() -> None:
    pool = cpu_pool()
    workload = generic_cpu_workload()
    now = utcnow()

    simulation = CapacitySimulation.build((pool,), (), (workload,), (), now)

    assert len(simulation.cells) == 1
    virtual_cell = simulation.cells[0]
    assert virtual_cell.worker_count == 1
    assert virtual_cell.allocatable == ResourceLimits(cpu=4, memory_gib=16)
    assert simulation.virtual_worker_count_by_cell_id == {virtual_cell.id: 1}

    result = CoreSchedulingService().schedule((workload,), simulation.cells)
    assert [placement.workload_id for placement in result.placements] == [workload.id]

    demand = simulation.demand((pool,), (), result)
    assert demand[pool.id].virtual_worker_counts == (1,)
    assert demand[pool.id].virtual_workload_ids == (workload.id,)

    planner = CapacityPlanner((pool,), {pool.id: []}, now)
    plan = planner.plan(demand)[pool.id]
    assert plan.launch_worker_counts == (1,)
    assert plan.launch_workload_ids == (workload.id,)


def test_shared_quota_group_allocates_launches_round_robin() -> None:
    group = "gcp-project-a-us-east5-a-v5p"
    first = shared_quota_pool(multislice_tpu_pool("research/shared-a"), group=group, limit=3, units_per_cell=1)
    second = shared_quota_pool(multislice_tpu_pool("research/shared-b"), group=group, limit=3, units_per_cell=1)
    demand = {
        first.id: demand_for(first, (1, 1), ("workload-a1", "workload-a2")),
        second.id: demand_for(second, (1, 1), ("workload-b1", "workload-b2")),
    }

    planner = CapacityPlanner((first, second), {first.id: [], second.id: []}, utcnow())
    plans = planner.plan(demand)

    assert plans[first.id].launch_worker_counts == (1, 1)
    assert plans[first.id].launch_workload_ids == ("workload-a1", "workload-a2")
    assert plans[second.id].launch_worker_counts == (1,)
    assert plans[second.id].launch_workload_ids == ("workload-b1",)
    assert planner.quota_blocked_workload_ids == frozenset({"workload-b2"})


def test_quota_group_pool_missing_limit_fails_with_value_error() -> None:
    pool = shared_quota_pool(multislice_tpu_pool("research/shared"), group="group", limit=None, units_per_cell=1)
    planner = CapacityPlanner((pool,), {pool.id: []}, utcnow())
    demand = {pool.id: demand_for(pool, (1,), ("workload-1",))}

    with pytest.raises(ValueError, match="quota group pools must configure quota_limit"):
        planner.plan(demand)


def test_quota_group_pool_missing_units_fails_with_value_error_during_allocation() -> None:
    pool = shared_quota_pool(multislice_tpu_pool("research/shared"), group="group", limit=4, units_per_cell=None)
    planner = CapacityPlanner((pool,), {pool.id: []}, utcnow())
    demand = {pool.id: demand_for(pool, (1,), ("workload-1",))}

    with pytest.raises(ValueError, match="quota group pools must configure quota_units_per_cell"):
        planner.plan(demand)


def test_quota_group_pool_missing_units_fails_with_value_error_during_reclaim() -> None:
    pool = shared_quota_pool(multislice_tpu_pool("research/shared"), group="group", limit=0, units_per_cell=None)
    planner = CapacityPlanner((pool,), {pool.id: []}, utcnow())
    demand = {pool.id: demand_for(pool, (1,), ("workload-1",))}

    with pytest.raises(ValueError, match="quota group pools must configure quota_units_per_cell"):
        planner.plan(demand)


@pytest.mark.asyncio
async def test_generic_multislice_workload_scales_tpu_pool_end_to_end() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository, scheduler=CoreSchedulingService())
    controller = EyvanController(repository, service)
    pool = multislice_tpu_pool()
    await service.apply_fleet(fleet_admin(), "research", (pool,), idempotency_key="fleet-v1")
    workload, _ = await service.submit_workload(
        operator(),
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(
            name="generic-multislice",
            execution=CommandExecution(command="python train.py"),
            gang={"replicas": 1, "per_replica": {"cpu": 32, "memory_gib": 0, "tpu": 8}},
            provider="gcp",
            required_executor="eyvan-native",
            worker_count=2,
            worker_resource_scope=WorkerResourceScope.PER_WORKER,
        ),
        idempotency_key="generic-multislice-1",
    )

    assert await controller.reconcile_capacity_pools_once() == {"launched": 1, "drained": 0, "deleted": 0}
    assert await controller.reconcile_capacity_pools_once() == {"launched": 0, "drained": 0, "deleted": 0}
    async with repository.transaction() as transaction:
        cells = await transaction.list_cells()
    assert len(cells) == 1
    assert cells[0].pool_id == pool.id
    assert cells[0].worker_count == 2
    assert cells[0].autoscale_request_workload_id == workload.id
    assert cells[0].allocatable == ResourceLimits(cpu=64, tpu=16)
    assert cells[0].provider_options["node_count"] == 2


@pytest.mark.asyncio
async def test_a_multi_worker_workload_pinned_to_a_non_tpu_pool_cannot_stop_the_leader() -> None:
    """One unplaceable workload used to halt scheduling for the whole plane.

    ``capacity-pools`` is the second leader phase, and when this was written a
    raise there aborted every later phase of the cycle -- observation,
    scheduling, settlement, billing -- with the loop retrying the identical
    failure on every tick. Failures are contained per phase now, but the
    behaviour asserted below is the one that made the containment unnecessary
    here: the phase completes. The pinned branch of
    ``CapacitySimulation.build`` called ``make_cell(worker_count=2)`` with no
    backend guard, and ``make_cell`` refuses a multi-worker cell on a non-TPU
    backend by raising. So a workload pinned to such a pool was a durable poison
    pill: it stayed PENDING, so nothing cleared it, and the plane never scheduled
    anything again.

    A workload that can never be placed is skipped and left pending. That is what
    an unplaceable workload is; it must not also be an outage.
    """

    repository = InMemoryRepository()
    service = ControlPlaneService(repository, scheduler=CoreSchedulingService())
    controller = EyvanController(repository, service)
    # A TPU pool on the compute backend -- what the catalog says a v5p-8 is, and
    # a shape `eyvan-native` legitimately supports at one worker.
    pool = multislice_tpu_pool(pool_id="research/v5p-8").model_copy(
        update={"capacity_backend": "gcp-compute", "accelerator_type": "v5p-8"}
    )
    await service.apply_fleet(fleet_admin(), "research", (pool,), idempotency_key="fleet-v1")
    await service.submit_workload(
        operator(),
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(
            name="pinned-multislice-on-compute",
            execution=CommandExecution(command="python train.py"),
            gang={"replicas": 1, "per_replica": {"cpu": 32, "memory_gib": 0, "tpu": 8}},
            provider="gcp",
            required_executor="eyvan-native",
            required_labels={"eyvan_pool": pool.id},
            worker_count=2,
            worker_resource_scope=WorkerResourceScope.PER_WORKER,
        ),
        idempotency_key="pinned-multislice-1",
    )

    # The phase completes rather than raising, and mints nothing for a shape that
    # cannot exist.
    assert await controller.reconcile_capacity_pools_once() == {"launched": 0, "drained": 0, "deleted": 0}
    # Still true on the next tick: the workload is durable, so a fix that merely
    # survived once would still stop the plane forever.
    assert await controller.reconcile_capacity_pools_once() == {"launched": 0, "drained": 0, "deleted": 0}
    async with repository.transaction() as transaction:
        assert await transaction.list_cells() == []


def test_cell_ids_do_not_repeat_across_control_plane_incarnations() -> None:
    """Found live 2026-08-01: a fresh control plane collided with old tombstones.

    The minter counts from ``len(managed) + 1`` and de-duplicates only against
    cells the control plane currently holds. The agent keeps a durable tombstone
    per released cell and permanently refuses any mutation at a generation not
    newer than it -- by design, because that is what stops a resurrected cell
    from being adopted.

    So a control plane whose database is empty re-creates the pool, starts at
    0001, and walks into the tombstones a previous one left on the same agent.
    Observed: cells 0001..0006, twelve consecutive "generation 1 is not newer
    than fenced cell ... generation 2" rejections, and a fleet that churned
    forever without provisioning.
    """

    from eyvan.capacity.planning import pool_cell_id

    first = multislice_tpu_pool()
    # A control plane on an empty database re-creates the pool, which stamps a
    # fresh created_at. Same id, same prefix, same starting index.
    reborn = first.model_copy(update={"created_at": utcnow()})

    assert pool_cell_id(first, 1) != pool_cell_id(reborn, 1)


def test_re_applying_a_live_fleet_keeps_minting_into_one_namespace() -> None:
    """`eyvan up` bumps generation; that must not orphan the counter."""

    from eyvan.capacity.planning import pool_cell_id

    pool = multislice_tpu_pool()
    # What upsert_capacity_pool does on a re-apply: bump generation, clear
    # initialized_at, carry created_at forward.
    reapplied = pool.model_copy(
        update={"generation": pool.generation + 1, "initialized_at": None, "updated_at": utcnow()}
    )

    assert pool_cell_id(pool, 7) == pool_cell_id(reapplied, 7)


def test_two_pools_sharing_a_prefix_get_different_namespaces() -> None:
    """Otherwise two fleets with the same node type collide on cell ids."""

    from eyvan.capacity.planning import pool_cell_id

    first = multislice_tpu_pool()
    other = first.model_copy(update={"id": f"{first.id}-other"})

    assert pool_cell_id(first, 1) != pool_cell_id(other, 1)


def test_a_minted_cell_id_fits_the_record_and_stays_provider_safe() -> None:
    """Cell ids are `max_length=96` and become GCP resource names."""

    from eyvan.capacity.planning import pool_cell_id

    pool = multislice_tpu_pool().model_copy(update={"fleet_name": "f" * 60, "node_type": "n" * 60})

    for index in (1, 9999, 10_000):
        cell_id = pool_cell_id(pool, index)
        assert len(cell_id) <= 96, cell_id
        assert re.fullmatch(r"[a-z0-9-]+", cell_id), cell_id
        assert not cell_id.startswith("-") and not cell_id.endswith("-")


def test_the_counter_is_never_truncated_away() -> None:
    """Trimming the tail to fit would alias two distinct cells onto one id.

    The interesting index is the one where the id *would* overflow: at a maximal
    prefix, index 10000 needs one character more than index 1000, so a naive
    ``[:96]`` on the assembled string turns "10000" back into "1000" -- a cell
    silently sharing an id, a lease, and a fence with a different one. The prefix
    is the only part it is safe to shorten.
    """

    from eyvan.capacity.planning import pool_cell_id

    pool = multislice_tpu_pool().model_copy(update={"fleet_name": "f" * 60, "node_type": "n" * 60})
    # max_cells is capped at 10_000, so this is the whole reachable range.
    indices = (1, 2, 999, 1000, 1001, 9999, 10_000)

    minted = [pool_cell_id(pool, index) for index in indices]

    assert len(set(minted)) == len(indices), minted
    assert minted[0].endswith("-0001")
    assert minted[-1].endswith("-10000")


def test_a_deleted_cell_never_gives_its_index_back() -> None:
    """Found live on 2026-08-02, after it had burned Spot capacity all night.

    The counter was ``len(live_cells) + 1``, so deleting a cell walked it
    *backwards* and a churning pool handed the same id out twice. The second
    holder then inherited whatever the first had left at the provider under that
    name: a new cell 0007 found a ``v5p-128`` queued resource created hours
    earlier wearing the name it had just been given.

    Eyvan correctly refused to adopt a resource of the wrong shape -- and then
    drained and retried forever against an ACTIVE, billing TPU it could never
    accept. The earlier ``research-*`` v5p-128 orphan was the same mechanism.
    """

    from eyvan.capacity.planning import next_pool_cell_index, pool_cell_id

    pool = multislice_tpu_pool()
    first = CellRecord(
        id=pool_cell_id(pool, 1),
        provider="gcp",
        provider_project="project-a",
        region="us-east5",
        zone="us-east5-a",
        state=CellState.DELETED,
        capacity=pool.capacity,
        allocatable=pool.allocatable,
        pool_id=pool.id,
    )

    assert next_pool_cell_index(pool, [first]) == 2, "a spent index must never be reissued"


def test_the_index_advances_past_the_highest_ever_used() -> None:
    """Gaps are fine; going backwards is not."""

    from eyvan.capacity.planning import next_pool_cell_index, pool_cell_id

    pool = multislice_tpu_pool()
    cells = [
        CellRecord(
            id=pool_cell_id(pool, index),
            provider="gcp",
            provider_project="project-a",
            region="us-east5",
            zone="us-east5-a",
            state=CellState.DELETED if index < 5 else CellState.READY,
            capacity=pool.capacity,
            allocatable=pool.allocatable,
            pool_id=pool.id,
        )
        for index in (1, 5)
    ]

    assert next_pool_cell_index(pool, cells) == 6


def test_a_pool_with_no_history_starts_at_one() -> None:
    from eyvan.capacity.planning import next_pool_cell_index

    assert next_pool_cell_index(multislice_tpu_pool(), []) == 1


def test_another_pools_cells_do_not_advance_this_counter() -> None:
    """Two pools sharing a prefix must not consume each other's indices."""

    from eyvan.capacity.planning import next_pool_cell_index, pool_cell_id

    pool = multislice_tpu_pool()
    other = pool.model_copy(update={"id": f"{pool.id}-other"})
    foreign = CellRecord(
        id=pool_cell_id(other, 9),
        provider="gcp",
        provider_project="project-a",
        region="us-east5",
        zone="us-east5-a",
        state=CellState.READY,
        capacity=pool.capacity,
        allocatable=pool.allocatable,
        pool_id=other.id,
    )

    assert next_pool_cell_index(pool, [foreign]) == 1


def test_a_pool_edit_that_orphans_a_cell_shape_does_not_stop_the_leader() -> None:
    """The poison pill again, reached by editing a pool instead of submitting.

    `cell_matches_pool` and `cell_requires_replacement` both mint a comparison
    cell from the pool, and `make_cell` refuses a multi-worker cell on a non-TPU
    backend by raising. Narrowing a pool that already has a multi-worker cell
    therefore raised inside the `capacity-pools` leader phase, which carries no
    failure isolation at the time -- so scheduling, settlement and billing stopped every
    cycle, over a configuration change.

    A pool that can no longer mint its own cell has answered both questions: the
    cell does not match, and it requires replacement.
    """

    pool = multislice_tpu_pool()
    wide = pool.make_cell("cell-wide", worker_count=2)
    narrowed = pool.model_copy(update={"capacity_backend": "gcp-compute"})

    assert cell_matches_pool(wide, narrowed) is False
    assert cell_requires_replacement(wide, narrowed) is True


# --------------------------------------------------------------------------- a leased cell in a stopped pool


def _live_workload() -> WorkloadRecord:
    """Return the workload whose lease holds the cell below."""

    return WorkloadRecord(
        id="wl-live",
        # Running, as anything holding a live lease is. A pending workload with
        # an active lease is itself a snapshot the scheduler refuses, and for a
        # different reason than the one under test here.
        state=WorkloadState.RUNNING,
        org_id="acme",
        project_id="research",
        created_by="operator",
        spec=WorkloadDefinition(
            name="live",
            execution=CommandExecution(command="python train.py"),
            gang={"replicas": 1, "per_replica": {"cpu": 32, "memory_gib": 0, "tpu": 8}},
            provider="gcp",
            required_executor="eyvan-native",
        ),
    )


def _leased_cell_and_lease(
    pool: CapacityPoolRecord, workload: WorkloadRecord | None = None
) -> tuple[CellRecord, LeaseRecord]:
    """Return one ready cell of ``pool`` and the live lease holding it."""

    holder = workload if workload is not None else _live_workload()
    cell = pool.make_cell(f"{pool.id}-0001", worker_count=1).model_copy(update={"state": CellState.READY})
    lease = LeaseRecord(
        id="lease-live",
        workload_id=holder.id,
        cell_id=cell.id,
        org_id="acme",
        project_id="research",
        # Memory named explicitly at zero: this pool template declares none, and
        # the gang's own default would put a gigabyte on a cell that has no
        # memory to give -- an overcommit the scheduler refuses before it ever
        # reaches the property under test.
        resources=ResourceGang(replicas=1, per_replica=ResourceVector(cpu=1.0, memory_gib=0.0)),
        fencing_token=f"{cell.id}:1",
        hourly_cost=1.0,
        expires_at=utcnow() + timedelta(hours=1),
    )
    return cell, lease


def test_a_disabled_pool_does_not_take_its_leased_cell_out_of_the_snapshot() -> None:
    """The scheduler is handed the cells and the live leases in one call.

    It rejects a snapshot whose lease names a cell the list does not hold, and it
    is right to: there is no capacity to check that lease against. So a cell that
    is no longer supply cannot simply be dropped while something still runs on
    it.

    Observed on a live fleet: `eyvan down` disabled a pool whose v5p-16
    multislice still held a lease, and the plane spent every two seconds raising
    out of this phase -- which carries the phases that observe, settle and
    release -- while the accelerator went on billing.
    """

    pool = multislice_tpu_pool()
    cell, lease = _leased_cell_and_lease(pool)
    disabled = pool.model_copy(update={"enabled": False})

    simulation = CapacitySimulation.build((disabled,), (cell,), (), (lease,), utcnow())

    assert [held.id for held in simulation.cells] == [cell.id]


def test_a_leased_cell_in_a_disabled_pool_attracts_no_new_work() -> None:
    """Keeping it in the snapshot must not put it back into supply.

    The cell is reported as draining, which is what it is: it carries work and
    must accept none. ``to_core_cell`` reads that as ``enabled=False``.
    """

    pool = multislice_tpu_pool()
    cell, lease = _leased_cell_and_lease(pool)
    disabled = pool.model_copy(update={"enabled": False})

    simulation = CapacitySimulation.build((disabled,), (cell,), (), (lease,), utcnow())

    assert simulation.cells[0].state is CellState.DRAINING


def test_an_unleased_cell_in_a_disabled_pool_stays_out_of_the_snapshot() -> None:
    """The fix must not resurrect a disabled pool's free capacity."""

    pool = multislice_tpu_pool()
    cell, _lease = _leased_cell_and_lease(pool)
    disabled = pool.model_copy(update={"enabled": False})

    simulation = CapacitySimulation.build((disabled,), (cell,), (), (), utcnow())

    assert simulation.cells == ()


def test_the_scheduler_accepts_the_snapshot_a_disabled_pool_produces() -> None:
    """The property the crash was: this call must not raise.

    Asserted against the real adapter rather than by inspecting the list, because
    what broke was the pair -- cells and leases -- reaching the scheduler
    together.
    """

    pool = multislice_tpu_pool()
    workload = _live_workload()
    cell, lease = _leased_cell_and_lease(pool, workload)
    disabled = pool.model_copy(update={"enabled": False})

    simulation = CapacitySimulation.build((disabled,), (cell,), (workload,), (lease,), utcnow())

    CoreSchedulingService().schedule([workload], simulation.cells, leases=[lease])


@pytest.mark.asyncio
async def test_disabling_a_pool_under_a_live_lease_does_not_stop_the_leader_cycle() -> None:
    """What `eyvan down` does, with something still running.

    The phase must complete. Everything that releases capacity and settles
    billing runs after it, so a raise here is not one phase lost -- it is an
    accelerator nobody stops paying for.
    """

    repository = InMemoryRepository()
    service = ControlPlaneService(repository, scheduler=CoreSchedulingService())
    controller = EyvanController(repository, service)
    pool = multislice_tpu_pool()
    await service.apply_fleet(fleet_admin(), "research", (pool,), idempotency_key="fleet-v1")
    workload = _live_workload()
    cell, lease = _leased_cell_and_lease(pool, workload)
    async with repository.transaction() as transaction:
        await transaction.put_workload(workload)
        await transaction.put_cell(cell)
        await transaction.put_lease(lease)
    # The route `eyvan down` calls, rather than a hand-disabled pool: it is what
    # sets enabled=False with min and max at zero, and that combination is the
    # one the live plane wedged on.
    await service.disable_fleet(fleet_admin(), "research", idempotency_key="fleet-down")

    assert await controller.reconcile_capacity_pools_once() == {"launched": 0, "drained": 0, "deleted": 0}
