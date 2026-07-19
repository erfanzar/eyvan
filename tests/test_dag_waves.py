# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""A DAG's entry fan-out is bounded, and its progress survives a restart.

A DAG with five hundred independent roots floods the queue exactly like an array
does, so the same wave bounds it. What waves do *not* touch is a node released
because its predecessors succeeded: that gate is the dependency, not the plan,
and holding such a node back would stall a pipeline mid-flight. This asserts
both -- the bound on the entry, and the deliberate absence of a bound on the
dependency path -- so neither is mistaken for the other later.
"""

from __future__ import annotations

from eyvan.config import EyvanSettings
from eyvan.records import (
    CommandExecution,
    CoordinationKind,
    ExpansionAdmissionState,
    ExpansionMode,
    Principal,
    ResourceGang,
    ResourceVector,
    Role,
    RoleGrant,
    WorkloadDefinition,
    WorkloadState,
)
from eyvan.server.controller import EyvanController
from eyvan.server.persistence import InMemoryRepository
from eyvan.server.service import ControlPlaneService
from eyvan.server.service.dag import DagNode
from eyvan.server.service.expansion import ExpansionLimits


def _principal() -> Principal:
    return Principal(
        subject="alice",
        grants=(
            RoleGrant(role=Role.PROJECT_OPERATOR, org_id="acme", project_id="research"),
            RoleGrant(role=Role.FLEET_ADMIN),
        ),
    )


def _template(name: str = "stage") -> WorkloadDefinition:
    return WorkloadDefinition(
        name=name,
        execution=CommandExecution(command="python stage.py"),
        gang=ResourceGang(replicas=1, per_replica=ResourceVector(cpu=1, memory_gib=1)),
    )


async def _service(*, wave: int = 0, ceiling: int = 1000) -> ControlPlaneService:
    repository = InMemoryRepository()
    await repository.open()
    return ControlPlaneService(
        repository,
        expansion_limits=ExpansionLimits(ceiling=ceiling, wave_size=wave),
    )


def _controller(service: ControlPlaneService) -> EyvanController:
    return EyvanController(service.repository, service, settings=EyvanSettings())


async def _succeed(service: ControlPlaneService, *workload_ids: str) -> None:
    async with service.repository.transaction() as tx:
        for workload_id in workload_ids:
            stored = await tx.get_workload(workload_id)
            assert stored is not None
            await tx.put_workload(stored.model_copy(update={"state": WorkloadState.SUCCEEDED}))


def _by_name(nodes) -> dict[str, object]:
    return {node.spec.name: node for node in nodes}


async def test_a_wide_dags_roots_are_admitted_in_waves() -> None:
    service = await _service(wave=2)

    parent, nodes = await service.submit_dag(
        _principal(),
        org_id="acme",
        project_id="research",
        nodes=[DagNode(name=f"root-{index}", template=_template()) for index in range(5)],
        idempotency_key="dag-1",
        name_prefix="wide",
    )

    assert parent.coordination_kind is CoordinationKind.DAG
    assert [node.schedulable for node in nodes] == [True, True, False, False, False]
    admission = await service.get_expansion_admission(_principal(), parent.id)
    assert admission is not None
    assert (admission.kind, admission.mode, admission.wave_size) == (CoordinationKind.DAG, ExpansionMode.WAVES, 2)
    assert admission.total_children == 5, "the plan governs the roots, which are the entry fan-out"


async def test_a_dependent_node_is_never_released_by_the_wave_phase() -> None:
    """Its gate is its predecessor. Releasing it early would run a stage out of order."""

    service = await _service(wave=1)
    parent, nodes = await service.submit_dag(
        _principal(),
        org_id="acme",
        project_id="research",
        nodes=[
            DagNode(name="prepare", template=_template()),
            DagNode(name="train", template=_template(), depends_on=("prepare",)),
            DagNode(name="evaluate", template=_template(), depends_on=("train",)),
        ],
        idempotency_key="dag-1",
        name_prefix="pipeline",
    )
    controller = _controller(service)
    stages = _by_name(nodes)

    # One root, one wave: nothing is deferred, so no plan is opened at all.
    assert [node.schedulable for node in nodes] == [True, False, False]
    async with service.repository.transaction() as tx:
        assert await tx.get_expansion_admission(parent.id) is None

    assert await controller.release_expansion_waves_once() == 0
    async with service.repository.transaction() as tx:
        stored = await tx.get_workload(stages["pipeline-train"].id)
    assert stored is not None and not stored.schedulable, "a dependent node is not a wave admission"

    await _succeed(service, stages["pipeline-prepare"].id)
    assert await controller.release_ready_dag_nodes_once() == 1
    async with service.repository.transaction() as tx:
        stored = await tx.get_workload(stages["pipeline-train"].id)
    assert stored is not None and stored.schedulable, "the dependency path still releases it"


async def test_a_dependency_release_counts_against_the_next_wave() -> None:
    """A node the DAG phase opened is occupying the queue, wave or no wave.

    The two paths are not independent: if the wave ignored nodes released by
    dependency satisfaction, an expansion would exceed its own bound as soon as
    one stage finished.
    """

    service = await _service(wave=2)
    _parent, nodes = await service.submit_dag(
        _principal(),
        org_id="acme",
        project_id="research",
        nodes=[
            DagNode(name="root-a", template=_template()),
            DagNode(name="root-b", template=_template()),
            DagNode(name="root-c", template=_template()),
            DagNode(name="after-a", template=_template(), depends_on=("root-a",)),
        ],
        idempotency_key="dag-1",
        name_prefix="mixed",
    )
    controller = _controller(service)
    stages = _by_name(nodes)

    await _succeed(service, stages["mixed-root-a"].id)
    assert await controller.release_ready_dag_nodes_once() == 1

    # root-a finished, freeing one place; after-a took it. root-b still occupies
    # the other, so root-c waits.
    assert await controller.release_expansion_waves_once() == 0
    async with service.repository.transaction() as tx:
        stored = await tx.get_workload(stages["mixed-root-c"].id)
    assert stored is not None and not stored.schedulable

    await _succeed(service, stages["mixed-after-a"].id)

    assert await controller.release_expansion_waves_once() == 1
    async with service.repository.transaction() as tx:
        stored = await tx.get_workload(stages["mixed-root-c"].id)
    assert stored is not None and stored.schedulable


async def test_dag_progress_is_derivable_after_a_control_plane_restart() -> None:
    """Nothing about a half-admitted DAG lives in the leader's memory."""

    service = await _service(wave=2)
    parent, nodes = await service.submit_dag(
        _principal(),
        org_id="acme",
        project_id="research",
        nodes=[DagNode(name=f"root-{index}", template=_template()) for index in range(6)],
        idempotency_key="dag-1",
        name_prefix="wide",
    )
    repository = service.repository
    stages = _by_name(nodes)
    await _succeed(service, stages["wide-root-0"].id, stages["wide-root-1"].id)

    # A new service and controller over the same durable rows, holding the plan
    # this process never saw.
    restarted = ControlPlaneService(repository)
    released = await _controller(restarted).release_expansion_waves_once()

    assert released == 2
    async with repository.transaction() as tx:
        stored = [await tx.get_workload(node.id) for node in nodes]
        admission = await tx.get_expansion_admission(parent.id)
    assert [item.schedulable for item in stored if item is not None] == [True, True, True, True, False, False]
    assert admission is not None
    assert admission.released_children == 4
    assert admission.state is ExpansionAdmissionState.ADMITTING


async def test_an_unbounded_dag_keeps_todays_shape_exactly() -> None:
    """With no wave configured, every root is schedulable and no plan is written."""

    service = await _service()

    parent, nodes = await service.submit_dag(
        _principal(),
        org_id="acme",
        project_id="research",
        nodes=[
            DagNode(name="left", template=_template()),
            DagNode(name="right", template=_template()),
            DagNode(name="merge", template=_template(), depends_on=("left", "right")),
        ],
        idempotency_key="dag-1",
        name_prefix="fan",
    )

    stages = _by_name(nodes)
    assert stages["fan-left"].schedulable and stages["fan-right"].schedulable
    assert not stages["fan-merge"].schedulable
    async with service.repository.transaction() as tx:
        assert await tx.get_expansion_admission(parent.id) is None
