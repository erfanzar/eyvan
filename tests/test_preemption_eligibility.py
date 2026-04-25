# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Preemption eligibility for non-checkpointable spot capacity.

The scheduler requires a victim to be ``checkpointable or
allow_forced_termination``. The service adapter never set the second flag, so a
non-checkpointable spot lease was revocable but never *eligible*: a
higher-priority workload got PREEMPTION_UNSAFE and that capacity was never
reclaimed. These tests pin the policy that fixes it, and pin the boundary that
on-demand capacity is still protected.
"""

from __future__ import annotations

from eyvan.core.domain import InfeasibilityCode
from eyvan.service.models import (
    CapacityClass,
    CellRecord,
    CellState,
    CommandExecution,
    PriorityBand,
    ResourceGang,
    ResourceVector,
    WorkloadDefinition,
    WorkloadRecord,
)
from eyvan.service.scheduling import CoreSchedulingService, _checkpoint


def _spec(
    *,
    capacity_class: CapacityClass,
    checkpointable: bool = False,
    band: PriorityBand = PriorityBand.BATCH,
) -> WorkloadDefinition:
    return WorkloadDefinition(
        name="job",
        execution=CommandExecution(command="python train.py"),
        gang=ResourceGang(replicas=1, per_replica=ResourceVector(cpu=8, memory_gib=8)),
        capacity_class=capacity_class,
        priority_band=band,
        checkpointable=checkpointable,
        checkpoint_hook="hook" if checkpointable else None,
        checkpoint_output_uri="gs://bucket/ckpt" if checkpointable else None,
    )


def _record(workload_id: str, spec: WorkloadDefinition, state) -> WorkloadRecord:
    return WorkloadRecord(
        id=workload_id,
        org_id="acme",
        project_id="research",
        spec=spec,
        state=state,
        created_by="alice",
    )


def test_spot_capacity_is_force_terminable() -> None:
    policy = _checkpoint(_record("wl", _spec(capacity_class=CapacityClass.SPOT), None or "pending"))

    assert policy.allow_forced_termination is True


def test_on_demand_capacity_is_not_force_terminable() -> None:
    """On-demand is only preemptible when the workload can checkpoint."""

    policy = _checkpoint(_record("wl", _spec(capacity_class=CapacityClass.ON_DEMAND), "pending"))

    assert policy.allow_forced_termination is False


def test_checkpointable_spot_keeps_its_checkpoint_mode() -> None:
    """Enabling forced termination must not disable checkpoint-on-preemption."""

    policy = _checkpoint(_record("wl", _spec(capacity_class=CapacityClass.SPOT, checkpointable=True), "pending"))

    assert policy.checkpointable is True
    assert policy.allow_forced_termination is True


def test_a_higher_priority_workload_reclaims_non_checkpointable_spot_capacity() -> None:
    """The behaviour that was broken: this used to report PREEMPTION_UNSAFE."""

    from datetime import timedelta

    from eyvan.service.models import LeaseRecord, ResourceHours, WorkloadState, utcnow

    victim_spec = _spec(capacity_class=CapacityClass.SPOT, band=PriorityBand.BATCH)
    victim = _record("wl-victim", victim_spec, WorkloadState.RUNNING)
    claimant = _record(
        "wl-claimant",
        _spec(capacity_class=CapacityClass.SPOT, band=PriorityBand.PRODUCTION),
        WorkloadState.PENDING,
    )
    cell = CellRecord(
        id="cell-a",
        provider="local",
        region="local",
        state=CellState.READY,
        capacity=ResourceVector(cpu=8, memory_gib=8),
        allocatable=ResourceVector(cpu=8, memory_gib=8),
        capacity_class=CapacityClass.SPOT,
    )
    lease = LeaseRecord(
        id="lease-victim",
        workload_id=victim.id,
        cell_id=cell.id,
        cell_generation=1,
        org_id=victim.org_id,
        project_id=victim.project_id,
        resources=victim_spec.gang,
        hourly_cost=1.0,
        reserved_cost=1.0,
        resource_hours_reserved=ResourceHours(cpu=8.0, memory_gib=8.0, gpu=0.0, tpu=0.0),
        generation=1,
        fencing_token="fence",
        expires_at=utcnow() + timedelta(minutes=5),
    )

    result = CoreSchedulingService().schedule([victim, claimant], [cell], leases=[lease])

    preempted = [plan for plan in result.preemptions if plan.workload_id == claimant.id]
    unsafe = [
        item
        for item in result.infeasibilities
        if item.workload_id == claimant.id and item.code is InfeasibilityCode.PREEMPTION_UNSAFE
    ]
    assert preempted, "a higher-priority workload must be able to reclaim spot capacity"
    assert not unsafe
    assert [victim.workload_id for victim in preempted[0].victims] == [victim.id]
