# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""An upgrade recycles the capacity Eyvan provisioned and holds the capacity it does not own.

Every Eyvan commit changes the build digest, so every plane upgrade leaves every
running cell stale. For a cell Eyvan provisioned, the answer pinned in
``test_stale_build_cells_recycle_themselves.py`` is right: drain it, and the
replacement is a new machine on the new build. For a cell that is a claim on an
operator-owned host (``manual``, ``onprem-ssh``) it never converges: the
replacement lands on the same host, with the same stale build and a new cell id
whose token the host does not hold. Observed live: the manual cell was deleted
within five seconds of the agent restarting on another build, and its
replacement stayed ``registering`` for the rest of the run.

These tests pin the policy that replaced that. Under the default ``auto`` a
cloud pool still recycles; an operator-owned cell is held -- kept, marked, and
attracting no placement -- and a dispatch onto it is refused without deleting
anything; and an in-place upgrade that makes every digest match returns the
cell to service, so a hold can never strand a host for good.
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

import pytest
from test_controller import FakeAgent, principal

import eyvan._internal.build_identity as build_identity_module
from eyvan.capacity.build_skew import BuildSkewPolicy, resolve_build_skew_policy
from eyvan.config import EyvanSettings
from eyvan.records import (
    CellRecord,
    CellState,
    CommandExecution,
    OutboxRecord,
    OutboxState,
    ResourceVector,
    Role,
    WorkloadDefinition,
    WorkloadState,
    utcnow,
)
from eyvan.server.controller import EyvanController
from eyvan.server.controller.fleets import BUILD_SKEW_CLEARED_ACTION, BUILD_SKEW_HELD_ACTION
from eyvan.server.persistence import InMemoryRepository
from eyvan.server.service import ControlPlaneService
from eyvan.server.service.scheduling import CoreSchedulingService, to_core_cell
from eyvan.worker.agent.client import AgentBuild

PLANE = "a" * 64
STALE = "b" * 64


def _plane_digest(monkeypatch: pytest.MonkeyPatch, digest: str = PLANE) -> None:
    monkeypatch.setattr(
        build_identity_module,
        "build_identity",
        lambda: SimpleNamespace(source_digest=digest),
    )


class _UpgradeableAgent(FakeAgent):
    """An agent whose own and workers' builds the test upgrades in place."""

    def __init__(self, agent_digest: str = PLANE, worker_digest: str = PLANE):
        super().__init__()
        self.agent_digest = agent_digest
        self.worker_digest = worker_digest

    def build(self):
        return AgentBuild(
            version="0.0.1",
            origin="wheel",
            origin_detail="",
            source_digest=self.agent_digest,
            digest_detail="",
            file_count=1,
            python="3.13",
            platform="linux",
        )

    def worker_builds(self, cell_id):
        return [{"rank": 0, "endpoint": "http://w0:8100", "build": {"source_digest": self.worker_digest}}]


def _cell(cell_id: str, backend: str) -> CellRecord:
    cloud = backend == "gcp-compute"
    return CellRecord(
        id=cell_id,
        provider="gcp" if cloud else "onprem" if backend == "onprem-ssh" else backend,
        capacity_backend=backend,
        provider_project="research-project" if cloud else None,
        region="us-central1" if cloud else "local",
        zone="us-central1-a" if cloud else None,
        state=CellState.READY,
        capacity=ResourceVector(cpu=1, memory_gib=1),
        allocatable=ResourceVector(cpu=1, memory_gib=1),
    )


def _controller(
    agent: FakeAgent, settings: EyvanSettings | None = None
) -> tuple[InMemoryRepository, ControlPlaneService, EyvanController]:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository, scheduler=CoreSchedulingService())
    controller = EyvanController(
        repository,
        service,
        settings=settings or EyvanSettings(),
        agent_factory=lambda _cell: agent,
    )
    return repository, service, controller


async def _put(repository: InMemoryRepository, *cells: CellRecord) -> None:
    async with repository.transaction() as transaction:
        for cell in cells:
            await transaction.put_cell(cell)


async def _get(repository: InMemoryRepository, cell_id: str) -> CellRecord:
    async with repository.transaction() as transaction:
        cell = await transaction.get_cell(cell_id)
    assert cell is not None
    return cell


async def _actions(repository: InMemoryRepository, cell_id: str) -> list[str]:
    async with repository.transaction() as transaction:
        events = await transaction.list_events()
    return [event.action for event in events if event.entity_id == cell_id]


async def _claimed_dispatch(repository: InMemoryRepository, controller: EyvanController, workload_id: str):
    async with repository.transaction() as transaction:
        workload = await transaction.get_workload(workload_id)
        assert workload is not None and workload.lease_id is not None
        commands = await transaction.list_outbox_for_aggregates(
            "workload", {workload.id: workload.generation}, topic="workload.dispatch"
        )
    assert len(commands) == 1
    # `dispatch_outbox_once` claims a command before delivering it; calling the
    # dispatch path directly has to stage the same claim or it reads as stale.
    claimed: OutboxRecord = commands[0].model_copy(
        update={
            "state": OutboxState.PROCESSING,
            "attempts": 1,
            "claimed_by": controller.worker_id,
            "claimed_until": utcnow() + timedelta(seconds=60),
        }
    )
    async with repository.transaction() as transaction:
        await transaction.put_outbox(claimed)
    return claimed


def test_auto_recycles_provisioned_capacity_and_refuses_on_operator_owned_hosts() -> None:
    assert resolve_build_skew_policy("auto", "gcp-tpu") is BuildSkewPolicy.RECYCLE
    assert resolve_build_skew_policy("auto", "gcp-compute") is BuildSkewPolicy.RECYCLE
    assert resolve_build_skew_policy("auto", "kubernetes") is BuildSkewPolicy.RECYCLE
    assert resolve_build_skew_policy("auto", "local") is BuildSkewPolicy.RECYCLE
    assert resolve_build_skew_policy("auto", "onprem-ssh") is BuildSkewPolicy.REFUSE
    assert resolve_build_skew_policy("auto", "manual") is BuildSkewPolicy.REFUSE
    assert resolve_build_skew_policy("auto", "gcp-attach") is BuildSkewPolicy.REFUSE
    # An explicit setting applies to every cell, operator-owned or not.
    assert resolve_build_skew_policy("recycle", "manual") is BuildSkewPolicy.RECYCLE
    assert resolve_build_skew_policy("warn", "gcp-tpu") is BuildSkewPolicy.WARN


def test_an_unknown_policy_is_refused_rather_than_defaulted() -> None:
    assert EyvanSettings().build_skew_policy == "auto"
    with pytest.raises(ValueError, match="build_skew_policy must be one of"):
        EyvanSettings(build_skew_policy="drain")


def test_a_held_cell_attracts_no_placement() -> None:
    held = _cell("cell-held", "manual").model_copy(update={"build_skew": "worker bbbbbbbbbbbb"})
    assert to_core_cell(_cell("cell-free", "manual")).enabled is True
    assert to_core_cell(held).enabled is False


@pytest.mark.asyncio
async def test_a_cloud_cell_on_another_build_is_still_recycled(monkeypatch) -> None:
    _plane_digest(monkeypatch)
    repository, _service, controller = _controller(_UpgradeableAgent(worker_digest=STALE))
    await _put(repository, _cell("cell-cloud", "gcp-compute"))

    assert await controller.recycle_stale_build_cells_once() == 1

    cell = await _get(repository, "cell-cloud")
    assert cell.state is CellState.DRAINING
    assert cell.build_skew is None


@pytest.mark.parametrize("backend", ["manual", "onprem-ssh"])
@pytest.mark.asyncio
async def test_an_operator_owned_cell_on_another_build_is_held_not_deleted(monkeypatch, backend: str) -> None:
    _plane_digest(monkeypatch)
    repository, _service, controller = _controller(_UpgradeableAgent(worker_digest=STALE))
    await _put(repository, _cell("cell-onprem", backend))

    assert await controller.recycle_stale_build_cells_once() == 0
    # A second pass over the same mismatch neither re-announces nor rewrites.
    assert await controller.recycle_stale_build_cells_once() == 0

    cell = await _get(repository, "cell-onprem")
    assert cell.state is CellState.READY, "the host is the operator's; nothing drains or deletes it"
    assert cell.generation == 1
    assert cell.build_skew is not None and STALE[:12] in cell.build_skew
    assert await _actions(repository, "cell-onprem") == [BUILD_SKEW_HELD_ACTION]
    async with repository.transaction() as transaction:
        drains = await transaction.list_outbox_for_aggregates("cell", {"cell-onprem": 1}, topic="capacity.drain")
    assert drains == []


@pytest.mark.asyncio
async def test_a_mismatched_worker_is_refused_then_dispatched_to_once_upgraded_in_place(monkeypatch) -> None:
    """Refused and held, never placed while held, and back in service after an upgrade.

    The whole loop an operator sees: the plane is upgraded past an on-prem host,
    the next dispatch onto it is refused without deleting anything, the workload
    waits, the operator reinstalls the worker on the host, and the same cell --
    same id, same token -- runs the workload.
    """

    _plane_digest(monkeypatch)
    agent = _UpgradeableAgent(worker_digest=STALE)
    repository, service, controller = _controller(agent)
    await _put(repository, _cell("cell-onprem", "onprem-ssh"))
    workload, _ = await service.submit_workload(
        principal(Role.PROJECT_OPERATOR),
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(name="queued", execution=CommandExecution(command="python train.py")),
        idempotency_key="queued",
    )
    result = await controller.reconcile_once()
    assert [placement.workload_id for placement in result.placements] == [workload.id]

    await controller._dispatch_workload(await _claimed_dispatch(repository, controller, workload.id))

    async with repository.transaction() as transaction:
        waiting = await transaction.get_workload(workload.id)
    cell = await _get(repository, "cell-onprem")
    assert agent.submissions == [], "nothing was dispatched onto the stale worker"
    assert waiting is not None and waiting.state is WorkloadState.PENDING
    assert "held until it is upgraded in place" in (waiting.scheduling_reason or "")
    assert cell.state is CellState.READY and cell.build_skew is not None
    assert await _actions(repository, "cell-onprem") == [BUILD_SKEW_HELD_ACTION]

    # Held: the scheduler does not lease the cell straight back, which would
    # spend the workload's attempts on one stale host until it failed.
    result = await controller.reconcile_once()
    assert list(result.placements) == []

    # The operator reinstalls the worker on the host from the plane's wheel.
    agent.worker_digest = PLANE
    assert await controller.recycle_stale_build_cells_once() == 0
    cell = await _get(repository, "cell-onprem")
    assert cell.build_skew is None
    assert await _actions(repository, "cell-onprem") == [BUILD_SKEW_HELD_ACTION, BUILD_SKEW_CLEARED_ACTION]

    result = await controller.reconcile_once()
    assert [placement.workload_id for placement in result.placements] == [workload.id]
    await controller._dispatch_workload(await _claimed_dispatch(repository, controller, workload.id))
    assert len(agent.submissions) == 1, "the same cell runs the workload once its build matches"
    assert (await _get(repository, "cell-onprem")).id == "cell-onprem"


@pytest.mark.asyncio
async def test_warn_dispatches_onto_a_stale_cell_and_drains_nothing(monkeypatch) -> None:
    _plane_digest(monkeypatch)
    agent = _UpgradeableAgent(worker_digest=STALE)
    repository, service, controller = _controller(agent, EyvanSettings(build_skew_policy="warn"))
    await _put(repository, _cell("cell-cloud", "gcp-compute"))

    assert await controller.recycle_stale_build_cells_once() == 0
    cell = await _get(repository, "cell-cloud")
    assert cell.state is CellState.READY and cell.build_skew is None

    workload, _ = await service.submit_workload(
        principal(Role.PROJECT_OPERATOR),
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(name="queued", execution=CommandExecution(command="python train.py")),
        idempotency_key="queued",
    )
    await controller.reconcile_once()
    await controller._dispatch_workload(await _claimed_dispatch(repository, controller, workload.id))
    assert len(agent.submissions) == 1


@pytest.mark.asyncio
async def test_an_explicit_recycle_still_recycles_operator_owned_cells(monkeypatch) -> None:
    _plane_digest(monkeypatch)
    repository, _service, controller = _controller(
        _UpgradeableAgent(agent_digest=STALE), EyvanSettings(build_skew_policy="recycle")
    )
    await _put(repository, _cell("cell-onprem", "manual"))

    assert await controller.recycle_stale_build_cells_once() == 1
    assert (await _get(repository, "cell-onprem")).state is CellState.DRAINING


def test_a_held_pool_cell_is_supply_on_its_way_back_not_a_reason_to_launch() -> None:
    """The autoscaler must not answer a hold by asking the backend for a second cell.

    On operator-owned hosts that second cell is a claim on the very machine the
    held one already holds -- the "claimed by another Eyvan cell" race seen live
    when the stale cell was recycled. Held capacity is counted like capacity
    still registering: present, not yet placeable.
    """

    from test_capacity_planning import _idle_cell, cpu_pool, generic_cpu_workload

    from eyvan.capacity.planning import CapacitySimulation

    pool = cpu_pool()
    held = _idle_cell(pool, 1).model_copy(update={"build_skew": "worker bbbbbbbbbbbb"})
    workload = generic_cpu_workload()
    now = utcnow()

    simulation = CapacitySimulation.build((pool,), (held,), (workload,), (), now)
    result = CoreSchedulingService().schedule((workload,), simulation.cells)
    demand = simulation.demand((pool,), (held,), result)

    assert demand[pool.id].virtual_worker_counts == ()
    assert held.id in simulation.acquiring_cell_ids


class _RestartingAgent(_UpgradeableAgent):
    """An agent that, while restarting, can report neither its own build nor its workers'."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.restarting = False

    def build(self):
        return None if self.restarting else super().build()

    def worker_builds(self, cell_id):
        return None if self.restarting else super().worker_builds(cell_id)


@pytest.mark.asyncio
async def test_a_hold_is_kept_until_every_build_is_proven_current(monkeypatch) -> None:
    _plane_digest(monkeypatch)
    agent = _RestartingAgent(worker_digest=STALE)
    repository, _service, controller = _controller(agent)
    await _put(repository, _cell("cell-onprem", "manual"))
    assert await controller.recycle_stale_build_cells_once() == 0
    assert (await _get(repository, "cell-onprem")).build_skew is not None

    # Nothing readable is not proof of a match: the hold stays.
    agent.restarting = True
    assert await controller.recycle_stale_build_cells_once() == 0
    assert (await _get(repository, "cell-onprem")).build_skew is not None

    # The agent answers on the plane's build but its worker still cannot say.
    agent.restarting = False
    agent.worker_builds = lambda cell_id: [{"rank": 0, "endpoint": "http://w0:8100", "build": None}]
    assert await controller.recycle_stale_build_cells_once() == 0
    assert (await _get(repository, "cell-onprem")).build_skew is not None

    del agent.worker_builds
    agent.worker_digest = PLANE
    assert await controller.recycle_stale_build_cells_once() == 0
    assert (await _get(repository, "cell-onprem")).build_skew is None
    assert await _actions(repository, "cell-onprem") == [BUILD_SKEW_HELD_ACTION, BUILD_SKEW_CLEARED_ACTION]


@pytest.mark.asyncio
async def test_warn_releases_a_hold_made_under_an_earlier_policy(monkeypatch) -> None:
    _plane_digest(monkeypatch)
    agent = _UpgradeableAgent(worker_digest=STALE)
    repository, service, controller = _controller(agent)
    await _put(repository, _cell("cell-onprem", "manual"))
    assert await controller.recycle_stale_build_cells_once() == 0
    assert (await _get(repository, "cell-onprem")).build_skew is not None

    warned = EyvanController(
        repository,
        service,
        settings=EyvanSettings(build_skew_policy="warn"),
        agent_factory=lambda _cell: agent,
    )
    assert await warned.recycle_stale_build_cells_once() == 0
    cell = await _get(repository, "cell-onprem")
    assert cell.state is CellState.READY and cell.build_skew is None
