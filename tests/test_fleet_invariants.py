# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The named invariants, and that each one can actually fail.

An invariant checker whose checks never fire is a checker that reports health it
has not established -- which is worse than none, because it is believed. So each
test here breaks one property deliberately and asserts the named violation, and
a companion test asserts a healthy fleet produces no violations at all.

That second half is not decoration. A checker that fires on ordinary states is
one an operator mutes within a week, and a muted invariant is an absent one.
"""

from __future__ import annotations

from datetime import timedelta

from eyvan.records import (
    BudgetRecord,
    CapacityPoolRecord,
    CellRecord,
    CellState,
    CommandExecution,
    LeaseRecord,
    LeaseState,
    OutboxRecord,
    ResourceGang,
    ResourceVector,
    WorkloadDefinition,
    WorkloadRecord,
    WorkloadState,
    utcnow,
)
from eyvan.server.invariants import (
    STUCK_OUTBOX_AFTER_S,
    FleetSnapshot,
    Invariant,
    check_fleet,
    compare_snapshots,
    snapshot_fleet,
)
from eyvan.server.persistence.memory import InMemoryRepository


def _pool(pool_id: str = "pool-a") -> CapacityPoolRecord:
    return CapacityPoolRecord(
        id=pool_id,
        fleet_name="research",
        node_type="cpu",
        provider="local",
        region="local",
        capacity=ResourceVector(cpu=8, memory_gib=64),
        allocatable=ResourceVector(cpu=8, memory_gib=64),
        agent_endpoint="http://agent:8090",
    )


def _cell(cell_id: str = "cell-a", *, pool_id: str | None = "pool-a", state: CellState = CellState.READY):
    return CellRecord(
        id=cell_id,
        provider="local",
        region="local",
        state=state,
        pool_id=pool_id,
        capacity=ResourceVector(cpu=8, memory_gib=64),
        allocatable=ResourceVector(cpu=8, memory_gib=64),
        endpoint="http://agent:8090",
    )


def _workload(workload_id: str = "wl-a", *, state: WorkloadState = WorkloadState.RUNNING) -> WorkloadRecord:
    return WorkloadRecord(
        id=workload_id,
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(name="train", execution=CommandExecution(command="python train.py")),
        state=state,
        created_by="alice",
    )


def _lease(
    lease_id: str = "lease-a",
    *,
    workload_id: str = "wl-a",
    cell_id: str = "cell-a",
    state: LeaseState = LeaseState.ACTIVE,
) -> LeaseRecord:
    return LeaseRecord(
        id=lease_id,
        workload_id=workload_id,
        cell_id=cell_id,
        org_id="acme",
        project_id="research",
        state=state,
        fencing_token=f"{cell_id}:1",
        resources=ResourceGang(replicas=1, per_replica=ResourceVector(cpu=1.0)),
        expires_at=utcnow() + timedelta(hours=1),
    )


def _budget(spent: float = 0.0, *, limit: float = 1000.0, revision: int = 1) -> BudgetRecord:
    return BudgetRecord(
        org_id="acme",
        project_id="research",
        limit=limit,
        spent=spent,
        revision=revision,
        updated_by="root",
    )


async def _fleet(*records) -> InMemoryRepository:
    """Seed a repository with whatever records a scenario needs."""

    repository = InMemoryRepository()
    async with repository.transaction() as tx:
        for record in records:
            if isinstance(record, CapacityPoolRecord):
                await tx.put_capacity_pool(record)
            elif isinstance(record, CellRecord):
                await tx.put_cell(record)
            elif isinstance(record, WorkloadRecord):
                await tx.put_workload(record)
            elif isinstance(record, LeaseRecord):
                await tx.put_lease(record)
            elif isinstance(record, BudgetRecord):
                await tx.put_budget(record)
            elif isinstance(record, OutboxRecord):
                await tx.put_outbox(record)
            else:  # pragma: no cover - a scenario passing an unhandled type
                raise TypeError(f"no seeding path for {type(record).__name__}")
    return repository


def _names(violations) -> set[Invariant]:
    return {violation.invariant for violation in violations}


# --------------------------------------------------------------------------- the healthy case


async def test_a_healthy_fleet_reports_nothing() -> None:
    """A checker that fires on ordinary states is one an operator mutes.

    And a muted invariant is an absent one, so this is the assertion the other
    twelve depend on being true.
    """

    repository = await _fleet(_pool(), _cell(), _workload(), _lease(), _budget())

    assert await check_fleet(repository) == ()


async def test_a_queued_workload_holding_no_capacity_is_not_a_violation() -> None:
    """PENDING and ADMITTED legitimately hold no lease.

    Counting them would make I4 fire on every healthy queue, which is the fastest
    way to make the whole set worthless.
    """

    repository = await _fleet(_pool(), _cell(), _workload(state=WorkloadState.PENDING))

    assert await check_fleet(repository) == ()


async def test_a_settled_lease_on_a_finished_workload_is_not_a_violation() -> None:
    """I2 is about *active* leases. A released one is the correct end state."""

    repository = await _fleet(
        _pool(),
        _cell(),
        _workload(state=WorkloadState.SUCCEEDED),
        _lease(state=LeaseState.RELEASED),
    )

    assert await check_fleet(repository) == ()


# --------------------------------------------------------------------------- each one can fail


async def test_i1_catches_a_cell_whose_pool_is_gone() -> None:
    """A cell consuming provider capacity that nothing explains is money leaking."""

    repository = await _fleet(_cell(pool_id="pool-deleted"), _workload(state=WorkloadState.PENDING))

    violations = await check_fleet(repository)

    assert _names(violations) == {Invariant.NO_STRANDED_CELLS}
    assert "pool-deleted" in violations[0].detail


async def test_i2_catches_a_lease_outliving_its_workload() -> None:
    """The classic leak: the workload finished and the capacity did not."""

    repository = await _fleet(_pool(), _cell(), _workload(state=WorkloadState.SUCCEEDED), _lease())

    violations = await check_fleet(repository)

    assert Invariant.NO_LEASES_OUTLIVING_WORKLOADS in _names(violations)
    assert "wl-a" in str(violations[0])


async def test_i2_catches_a_lease_whose_workload_vanished() -> None:
    """A missing workload is the same leak with less evidence, so it is named."""

    repository = await _fleet(_pool(), _cell(), _lease())

    assert Invariant.NO_LEASES_OUTLIVING_WORKLOADS in _names(await check_fleet(repository))


async def test_i3_catches_a_stuck_outbox_command() -> None:
    """An undelivered command is work the control plane believes it ordered."""

    stale = OutboxRecord(
        id="cmd-1",
        topic="capacity.apply",
        aggregate_type="cell",
        aggregate_id="cell-a",
        idempotency_key="apply-1",
        generation=1,
        created_at=utcnow() - timedelta(seconds=STUCK_OUTBOX_AFTER_S + 60),
    )
    repository = await _fleet(_pool(), _cell(), stale)

    violations = await check_fleet(repository)

    assert Invariant.NO_STUCK_OUTBOX in _names(violations)


async def test_i3_does_not_fire_on_a_command_still_in_flight() -> None:
    """A provider call can legitimately take minutes.

    An invariant that fires during normal slowness trains an operator to ignore
    it, so the window is generous and this asserts the generosity.
    """

    fresh = OutboxRecord(
        id="cmd-1",
        topic="capacity.apply",
        aggregate_type="cell",
        aggregate_id="cell-a",
        idempotency_key="apply-1",
        generation=1,
        created_at=utcnow() - timedelta(seconds=STUCK_OUTBOX_AFTER_S / 2),
    )
    repository = await _fleet(_pool(), _cell(), fresh)

    assert await check_fleet(repository) == ()


async def test_i4_catches_a_running_workload_with_no_lease() -> None:
    """A workload claiming to run on capacity it does not hold.

    Either it is running unaccounted-for, or it is stuck forever in a state
    nothing will settle. Both are invisible without this.
    """

    repository = await _fleet(_pool(), _cell(), _workload(state=WorkloadState.RUNNING))

    violations = await check_fleet(repository)

    assert _names(violations) == {Invariant.NO_RUNNING_WORKLOAD_WITHOUT_A_LEASE}


async def test_i5_catches_a_budget_that_disagrees_with_its_settled_leases() -> None:
    """Double-charging and under-charging look identical from the budget alone."""

    settled = _lease(state=LeaseState.RELEASED).model_copy(
        update={"settled_at": utcnow(), "settled_cost": 10.0, "settlement_reason": "completed"}
    )
    repository = await _fleet(_pool(), _cell(), _workload(state=WorkloadState.SUCCEEDED), settled, _budget(25.0))

    violations = await check_fleet(repository)

    assert Invariant.BUDGET_MATCHES_SETTLED_COST in _names(violations)


async def test_i5_tolerates_floating_point_accumulation() -> None:
    """An invariant that fires on the last bit fires constantly and says nothing."""

    settled = _lease(state=LeaseState.RELEASED).model_copy(
        update={"settled_at": utcnow(), "settled_cost": 10.0, "settlement_reason": "completed"}
    )
    repository = await _fleet(_pool(), _cell(), _workload(state=WorkloadState.SUCCEEDED), settled, _budget(10.000000001))

    assert await check_fleet(repository) == ()


async def test_i8_catches_retention_that_stopped_pruning() -> None:
    """Audit rows are the fastest-growing table Eyvan writes.

    A prune sweep that silently stopped is invisible until the disk is full,
    which on a control plane means it stops scheduling.
    """

    repository = await _fleet(_pool(), _cell())
    async with repository.transaction() as tx:
        from eyvan.records import AuditEvent

        await tx.append_event(
            AuditEvent(
                id="evt-old",
                actor="system",
                org_id="acme",
                project_id="research",
                entity_type="workload",
                entity_id="wl-a",
                action="workload.submitted",
                timestamp=utcnow() - timedelta(days=400),
            )
        )

    violations = await check_fleet(repository, audit_retention_s=86_400 * 90)

    assert Invariant.AUDIT_RETENTION_PRUNES in _names(violations)


async def test_i8_is_silent_when_no_retention_is_configured() -> None:
    """Absence of a configured window is not evidence that pruning failed."""

    repository = await _fleet(_pool(), _cell())
    async with repository.transaction() as tx:
        from eyvan.records import AuditEvent

        await tx.append_event(
            AuditEvent(
                id="evt-old",
                actor="system",
                org_id="acme",
                project_id="research",
                entity_type="workload",
                entity_id="wl-a",
                action="workload.submitted",
                timestamp=utcnow() - timedelta(days=400),
            )
        )

    assert await check_fleet(repository) == ()


async def test_i9_catches_a_lease_on_a_cell_that_no_longer_exists() -> None:
    """A lease pinned to nothing can never be settled by the cell's release."""

    repository = await _fleet(_pool(), _workload(), _lease(cell_id="cell-gone"))

    assert Invariant.NO_LEASE_ON_A_MISSING_CELL in _names(await check_fleet(repository))


# --------------------------------------------------------------------------- over time


async def test_a_snapshot_records_what_the_over_time_checks_need() -> None:
    """And nothing else: a snapshot that grew with the run would defeat I7."""

    repository = await _fleet(_pool(), _cell(), _workload(), _lease(), _budget(12.0))

    snapshot = await snapshot_fleet(repository)

    assert snapshot.budget_spend == {("acme", "research"): (12.0, 1)}
    assert snapshot.workload_count == 1
    assert snapshot.cell_count == 1


def test_i6_catches_spend_that_went_backwards() -> None:
    """Spend falling within a period means something was refunded or lost.

    Neither is a thing Eyvan does, so either is a bug in settlement.
    """

    now = utcnow()
    previous = FleetSnapshot(observed_at=now - timedelta(hours=1), budget_spend={("acme", "research"): (50.0, 1)})
    current = FleetSnapshot(observed_at=now, budget_spend={("acme", "research"): (30.0, 1)})

    violations = compare_snapshots(previous, current)

    assert _names(violations) == {Invariant.BUDGET_SPEND_IS_MONOTONE}


def test_i6_does_not_fire_when_a_budget_period_rolled() -> None:
    """A roll resets ``spent`` deliberately.

    Calling that a violation would fire on correct behaviour once a month, which
    is exactly how an invariant gets muted.
    """

    now = utcnow()
    previous = FleetSnapshot(observed_at=now - timedelta(hours=1), budget_spend={("acme", "research"): (50.0, 1)})
    current = FleetSnapshot(observed_at=now, budget_spend={("acme", "research"): (0.0, 2)})

    assert compare_snapshots(previous, current) == ()


def test_i7_catches_storage_past_its_ceiling_and_reports_the_growth() -> None:
    """The number an operator needs is the rate, not only the total.

    "12 GB" says nothing; "12 GB, up 4 GB since the last check" says whether
    collection is running at all.
    """

    now = utcnow()
    previous = FleetSnapshot(observed_at=now - timedelta(hours=1), artifact_bytes=8_000)
    current = FleetSnapshot(observed_at=now, artifact_bytes=12_000)

    violations = compare_snapshots(previous, current, max_artifact_bytes=10_000)

    assert _names(violations) == {Invariant.ARTIFACT_STORAGE_IS_BOUNDED}
    assert "4000" in violations[0].detail


def test_i7_is_silent_without_a_configured_ceiling() -> None:
    """No ceiling means no claim about boundedness, which is honest.

    Inventing one would make every deployment that never configured a limit
    report a violation it cannot act on.
    """

    now = utcnow()
    previous = FleetSnapshot(observed_at=now - timedelta(hours=1), artifact_bytes=8_000)
    current = FleetSnapshot(observed_at=now, artifact_bytes=12_000)

    assert compare_snapshots(previous, current) == ()


def test_a_new_budget_is_not_compared_against_nothing() -> None:
    """A scope that appears between observations has no prior to fall from."""

    now = utcnow()
    previous = FleetSnapshot(observed_at=now - timedelta(hours=1))
    current = FleetSnapshot(observed_at=now, budget_spend={("acme", "research"): (30.0, 1)})

    assert compare_snapshots(previous, current) == ()


# --------------------------------------------------------------------------- completeness


def test_every_named_invariant_has_a_test_that_makes_it_fire() -> None:
    """An invariant nobody proved can fail is one nobody has checked.

    This is the guard that keeps the set honest as it grows: adding I10 without
    a scenario that trips it fails here rather than quietly reporting health.
    """

    source = __import__("pathlib").Path(__file__).read_text(encoding="utf-8")
    missing = [
        invariant.name
        for invariant in Invariant
        if f"Invariant.{invariant.name}" not in source.split("def test_every_named_invariant", 1)[0]
    ]

    assert not missing, f"these invariants have no failing scenario: {', '.join(missing)}"


# --------------------------------------------------------------------------- reachable


def test_an_operator_can_ask_the_running_plane(tmp_path) -> None:
    """A checker nothing calls reports on nothing.

    This is the shape half this package's defects have taken: real code, real
    tests, and no production caller. The route and the CLI are what make the
    invariants an operational tool rather than a test fixture.
    """

    from fastapi.testclient import TestClient

    from eyvan.config import EyvanSettings
    from eyvan.server.api.app import create_app

    client = TestClient(
        create_app(
            settings=EyvanSettings(
                dev_auth_enabled=True,
                artifact_token_key="k" * 32,
                artifact_store_uri=str(tmp_path / "artifacts"),
            )
        )
    )

    response = client.get(
        "/api/v1/system/invariants",
        headers={"X-Eyvan-User": "root", "X-Eyvan-Roles": "fleet_admin"},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["violations"] == []
    # The names are reported alongside the result so a reader can tell "nothing
    # is broken" from "nothing was checked" -- which an empty list alone cannot.
    assert body["checked"] == [invariant.value for invariant in Invariant]


def test_the_invariant_report_is_not_readable_by_a_tenant(tmp_path) -> None:
    """It names every cell, lease and budget scope in the fleet."""

    from fastapi.testclient import TestClient

    from eyvan.config import EyvanSettings
    from eyvan.server.api.app import create_app

    client = TestClient(
        create_app(
            settings=EyvanSettings(
                dev_auth_enabled=True,
                artifact_token_key="k" * 32,
                artifact_store_uri=str(tmp_path / "artifacts"),
            )
        )
    )

    response = client.get(
        "/api/v1/system/invariants",
        headers={
            "X-Eyvan-User": "alice",
            "X-Eyvan-Roles": "operator",
            "X-Eyvan-Org": "acme",
            "X-Eyvan-Project": "research",
        },
    )

    assert response.status_code == 403, response.text
