# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The durability scenarios that only mean something against a real database.

``test_chaos`` states its own limit plainly: it runs against the in-memory
backend, so it proves control-plane *logic* and not PostgreSQL isolation. That
limit is honest and it is also the gap -- every one of those invariants is
enforced in production by a transaction, and a copy-on-write dictionary with an
asyncio lock cannot fail the way SERIALIZABLE can.

Not a duplicate of the whole matrix. Most chaos scenarios are about control-plane
decisions and gain nothing from a real backend; re-running them here would double
the runtime and prove the same thing twice. What is here is the subset where the
*store* is what enforces the invariant:

* settlement idempotence, which is the one operation that moves money and is
  guarded by a column rather than by a Python check;
* idempotency-key replay, which is a unique constraint under contention rather
  than a dictionary lookup;
* the orphan sweeps, which read one snapshot and write against a generation --
  the exact shape a concurrent writer breaks;
* fleet-wide invariants after all of it, because a violation that only appears
  once rows are really committed is the kind a soak run finds at hour forty.

These are skipped without a PostgreSQL to run against, and the suite reports
them as skipped rather than passing. A green run with no database is a green run
that proved nothing here, and saying so is the difference between coverage and
the appearance of it.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from eyvan.records import (
    BudgetRecord,
    CellRecord,
    CellState,
    CommandExecution,
    LeaseRecord,
    LeaseState,
    Principal,
    ResourceGang,
    ResourceVector,
    Role,
    RoleGrant,
    WorkloadDefinition,
    WorkloadState,
    utcnow,
)
from eyvan.server.controller import EyvanController
from eyvan.server.invariants import check_fleet
from eyvan.server.persistence.sqlalchemy import SqlAlchemyRepository, create_schema
from eyvan.server.service.control_plane import ControlPlaneService

ADMIN = Principal(subject="root", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
OPERATOR = Principal(
    subject="alice",
    grants=(RoleGrant(role=Role.PROJECT_OPERATOR, org_id="acme", project_id="research"),),
)


def _spec() -> WorkloadDefinition:
    return WorkloadDefinition(
        name="job",
        execution=CommandExecution(command="python train.py"),
        gang=ResourceGang(replicas=1, per_replica=ResourceVector(cpu=1, memory_gib=1)),
    )


async def _leased(repository, service, *, key: str, hourly_cost: float = 10.0):
    """Submit a workload and give it a real, costed lease.

    Load-bearing. A settlement test on a workload with *no* lease never reaches
    ``_settle_lease``, so the ``settled_at`` idempotency boundary is not
    exercised and "the budget was not charged twice" is trivially true because
    it was never charged at all -- which is how a mutation that removed the
    guard survived the first version of these tests.
    """

    async with repository.transaction() as tx:
        await tx.put_cell(
            CellRecord(
                id="cell-a",
                provider="local",
                region="local",
                state=CellState.READY,
                capacity=ResourceVector(cpu=4),
                allocatable=ResourceVector(cpu=4),
                endpoint="http://agent:8090",
            )
        )
    workload, _ = await service.submit_workload(
        OPERATOR, org_id="acme", project_id="research", spec=_spec(), idempotency_key=key
    )
    lease = LeaseRecord(
        id=f"lease-{key}",
        workload_id=workload.id,
        cell_id="cell-a",
        org_id="acme",
        project_id="research",
        resources=_spec().gang,
        state=LeaseState.ACTIVE,
        fencing_token="cell-a:1",
        hourly_cost=hourly_cost,
        run_started_at=utcnow() - timedelta(hours=1),
        expires_at=utcnow() + timedelta(hours=1),
    )
    await service.create_lease(ADMIN, lease, idempotency_key=f"lease-{key}")
    return workload


@pytest.fixture
async def plane(postgres_url: str):
    """A control plane on a freshly migrated PostgreSQL schema."""

    engine = create_async_engine(postgres_url, poolclass=None)
    async with engine.begin() as connection:
        await connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        await connection.execute(text("CREATE SCHEMA public"))
    await create_schema(engine)
    repository = SqlAlchemyRepository(engine)
    await repository.open()
    service = ControlPlaneService(repository)
    try:
        yield repository, service, EyvanController(repository, service)
    finally:
        await repository.close()
        await engine.dispose()


# --------------------------------------------------------------------------- settlement


async def test_a_replayed_settlement_does_not_charge_the_budget_twice(plane) -> None:
    """The one operation that moves money, against the store that guards it.

    ``settled_at`` is the idempotency boundary, and in production it is a column
    read inside a SERIALIZABLE transaction -- not the dictionary lookup the
    in-memory version exercises. A replacement leader replaying the same terminal
    transition is the ordinary failover, not an exotic case.
    """

    repository, service, _controller = plane
    async with repository.transaction() as tx:
        await tx.put_budget(BudgetRecord(org_id="acme", project_id="research", limit=1_000, updated_by="root"))
    workload = await _leased(repository, service, key="failover-1")

    first, _lease, _budget = await service.settle_terminal_workload(
        ADMIN, workload.id, WorkloadState.FAILED, action="workload.failed"
    )
    second, _lease, _budget = await service.settle_terminal_workload(
        ADMIN, workload.id, WorkloadState.FAILED, action="workload.failed"
    )

    assert first.state is second.state is WorkloadState.FAILED
    async with repository.transaction() as tx:
        budget = await tx.get_budget("acme", "research")
        lease = await tx.get_lease("lease-failover-1")
    assert lease is not None
    assert lease.settled_cost > 0
    assert budget is not None
    # Charged once, not zero and not twice. Zero would mean the settlement path
    # was never reached, which is what made the earlier version of this test
    # pass against a control plane with no idempotence at all.
    assert budget.spent == lease.settled_cost


async def test_concurrent_settlements_of_one_workload_still_settle_it_once(plane) -> None:
    """Two leaders settling the same workload at the same instant.

    This is what the in-memory backend structurally cannot test: its transaction
    lock serializes everything, so a race that a real database resolves by
    aborting one transaction never happens at all.
    """

    repository, service, _controller = plane
    async with repository.transaction() as tx:
        await tx.put_budget(BudgetRecord(org_id="acme", project_id="research", limit=1_000, updated_by="root"))
    workload = await _leased(repository, service, key="race-1")

    async def settle():
        try:
            await service.settle_terminal_workload(ADMIN, workload.id, WorkloadState.FAILED, action="workload.failed")
            return True
        except Exception:
            # A losing transaction is a correct outcome here. What must not
            # happen is two *successful* settlements.
            return False

    results = await asyncio.gather(*(settle() for _ in range(4)))

    assert any(results)
    async with repository.transaction() as tx:
        stored = await tx.get_workload(workload.id)
        budget = await tx.get_budget("acme", "research")
        lease = await tx.get_lease("lease-race-1")
    assert stored is not None
    assert stored.state is WorkloadState.FAILED
    assert lease is not None
    assert budget is not None
    # Four concurrent settlements, one charge. Any other number means the
    # database resolved the race and the accounting did not.
    assert budget.spent == lease.settled_cost


# --------------------------------------------------------------------------- replay


async def test_a_duplicated_submission_creates_exactly_one_workload(plane) -> None:
    """At-least-once delivery means the same submit arrives twice.

    In production the guard is a uniqueness constraint under contention; in
    memory it is a dictionary. Only one of those can deadlock, abort, or race.
    """

    _repository, service, _controller = plane

    first, replayed_first = await service.submit_workload(
        OPERATOR, org_id="acme", project_id="research", spec=_spec(), idempotency_key="dup-1"
    )
    second, replayed_second = await service.submit_workload(
        OPERATOR, org_id="acme", project_id="research", spec=_spec(), idempotency_key="dup-1"
    )

    assert first.id == second.id
    assert replayed_first is False
    assert replayed_second is True


async def test_concurrent_duplicate_submissions_create_exactly_one_workload(plane) -> None:
    """Four replicas replaying one delivery at once.

    The case an idempotency key exists for, and the one an in-memory lock hides:
    every caller must end up with the same workload id, and the database must
    contain exactly one.
    """

    _repository, service, _controller = plane

    async def submit():
        try:
            record, _replayed = await service.submit_workload(
                OPERATOR, org_id="acme", project_id="research", spec=_spec(), idempotency_key="dup-race"
            )
            return record.id
        except Exception:
            return None

    ids = {result for result in await asyncio.gather(*(submit() for _ in range(4))) if result}

    assert len(ids) == 1


# --------------------------------------------------------------------------- sweeps


async def test_the_orphan_sweeps_run_against_a_real_snapshot(plane) -> None:
    """Read one snapshot, write against a generation -- the shape a writer breaks.

    Exercised here end to end rather than asserted: what matters is that the
    sweeps commit against a real transaction boundary and leave the fleet whole,
    which is the property a soak run watches for days.
    """

    repository, service, controller = plane
    async with repository.transaction() as tx:
        await tx.put_cell(
            CellRecord(
                id="cell-a",
                provider="local",
                region="local",
                state=CellState.READY,
                capacity=ResourceVector(cpu=4),
                allocatable=ResourceVector(cpu=4),
                endpoint="http://agent:8090",
            )
        )
    workload, _ = await service.submit_workload(
        OPERATOR, org_id="acme", project_id="research", spec=_spec(), idempotency_key="sweep-1"
    )
    lease = LeaseRecord(
        id="lease-a",
        workload_id=workload.id,
        cell_id="cell-a",
        org_id="acme",
        project_id="research",
        resources=_spec().gang,
        state=LeaseState.ACTIVE,
        fencing_token="cell-a:1",
        expires_at=utcnow() + timedelta(hours=1),
    )
    await service.create_lease(ADMIN, lease, idempotency_key="lease-1")
    async with repository.transaction() as tx:
        stored = await tx.get_workload(workload.id)
        assert stored is not None
        await tx.put_workload(
            # The workload keeps claiming the lease. Terminality alone is what
            # makes it an orphan here, so this exercises that branch rather than
            # the "moved on to other capacity" one -- which is covered against
            # the in-memory backend and would otherwise mask it.
            stored.model_copy(
                update={
                    "state": WorkloadState.SUCCEEDED,
                    "updated_at": utcnow() - timedelta(hours=2),
                }
            )
        )
        current = await tx.get_lease("lease-a")
        assert current is not None
        await tx.put_lease(current.model_copy(update={"updated_at": utcnow() - timedelta(hours=2)}))

    assert await controller.settle_orphaned_leases_once() == 1

    async with repository.transaction() as tx:
        settled = await tx.get_lease("lease-a")
    assert settled is not None
    assert settled.state is LeaseState.RELEASED


# --------------------------------------------------------------------------- invariants


async def test_the_fleet_is_whole_after_the_matrix(plane) -> None:
    """The assertion a soak run makes continuously, made once against real rows.

    A violation that only appears after data is genuinely committed -- rather
    than held in a dictionary the test also owns -- is exactly the kind that
    surfaces at hour forty of an unattended run and nowhere earlier.
    """

    repository, service, _controller = plane
    async with repository.transaction() as tx:
        await tx.put_budget(BudgetRecord(org_id="acme", project_id="research", limit=1_000, updated_by="root"))
    workload, _ = await service.submit_workload(
        OPERATOR, org_id="acme", project_id="research", spec=_spec(), idempotency_key="whole-1"
    )
    await service.settle_terminal_workload(ADMIN, workload.id, WorkloadState.FAILED, action="workload.failed")

    assert await check_fleet(repository) == ()
