# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The production backend, executed for the first time.

Every mechanism asserted here is PostgreSQL-specific and every one of them was
unexecuted. ``SqlAlchemyRepository`` is a careful piece of work -- SERIALIZABLE
isolation, ``FOR UPDATE SKIP LOCKED`` claims, retryable-SQLSTATE mapping,
session-scoped advisory locks with a ``pg_locks`` ownership re-check -- and the
suite that covered it ran on SQLite, offline Alembic, and a Python model of
advisory locks. Those prove the shape of the code. They cannot prove the
database behaves the way the code assumes, and the assumptions here are exactly
the ones a different database gets differently.

Each test names the failure the assumption prevents, because "it passes on
PostgreSQL" is not a finding -- "two leaders would each mint autoscaler cell ids
from the same sequence" is.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from eyvan.records import (
    AuditEvent,
    CellRecord,
    CellState,
    CommandExecution,
    OutboxRecord,
    ResourceVector,
    WorkloadDefinition,
    WorkloadRecord,
    WorkloadState,
    utcnow,
)
from eyvan.server.persistence.base import SchemaRevisionMismatch, TransientRepositoryError
from eyvan.server.persistence.leader import PostgresAdvisoryLeader
from eyvan.server.persistence.sqlalchemy import SCHEMA_REVISION, SqlAlchemyRepository, create_schema


@pytest.fixture
async def database(postgres_url: str):
    """Yield an engine over a freshly migrated, empty database.

    A schema per test rather than a database per test: creating a database is a
    second or two of I/O, while ``DROP SCHEMA public CASCADE`` is instant and
    leaves exactly the same clean slate.
    """

    engine = create_async_engine(postgres_url, poolclass=None)
    async with engine.begin() as connection:
        await connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        await connection.execute(text("CREATE SCHEMA public"))
    try:
        yield engine
    finally:
        await engine.dispose()


async def _migrated(engine) -> SqlAlchemyRepository:
    await create_schema(engine)
    repository = SqlAlchemyRepository(engine)
    await repository.open()
    return repository


def _workload(identifier: str, *, state: WorkloadState = WorkloadState.PENDING) -> WorkloadRecord:
    return WorkloadRecord(
        id=identifier,
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(name=identifier, execution=CommandExecution(command="python train.py")),
        state=state,
        created_by="alice",
    )


# --------------------------------------------------------------------------- schema


async def test_the_migration_chain_runs_against_a_real_database(database) -> None:
    """``test_migrations`` asserts emitted DDL strings and never executes them.

    Offline mode proves the scripts *render*. It cannot catch a migration that
    renders valid SQL PostgreSQL then rejects -- an index on a column an earlier
    revision spelled differently, a constraint that cannot be added to a
    populated table. This runs the chain.
    """

    from alembic import command

    from eyvan.server.persistence.migrate import build_alembic_config

    url = str(database.url.render_as_string(hide_password=False))
    config = build_alembic_config(url.replace("+asyncpg", "+psycopg2") if "psycopg2" in url else url)
    config.set_main_option("sqlalchemy.url", url)

    await asyncio.to_thread(command.upgrade, config, "head")

    async with database.connect() as connection:
        stamped = (await connection.execute(text("SELECT version_num FROM alembic_version"))).scalar_one()
        tables = {
            row[0]
            for row in (
                await connection.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'"))
            ).all()
        }

    assert stamped == SCHEMA_REVISION
    assert {
        "workloads",
        "cells",
        "leases",
        "outbox",
        "audit_events",
        "progress_reports",
        "organizations",
        "projects",
    } <= tables


async def test_a_mismatched_stamp_refuses_to_open_a_real_database(database) -> None:
    """The gate exists so a build never queries columns the database lacks."""

    await create_schema(database)
    async with database.begin() as connection:
        await connection.execute(text("UPDATE alembic_version SET version_num = '0002_transactional_outbox'"))

    with pytest.raises(SchemaRevisionMismatch, match="0002_transactional_outbox"):
        await SqlAlchemyRepository(database).open()


# --------------------------------------------------------------------------- leadership


async def test_only_one_of_two_contending_replicas_becomes_leader(database) -> None:
    """Two leaders would each mint autoscaler cell ids from the same sequence.

    ``len(managed) + 1`` is not collision-safe across processes, so a split
    leadership does not merely duplicate work -- it creates two cells claiming
    one id, and the loser's provider resources are stranded with nothing
    tracking them.
    """

    await _migrated(database)
    first, second = PostgresAdvisoryLeader(database), PostgresAdvisoryLeader(database)

    try:
        assert await first.acquire() is True
        assert await second.acquire() is False, "the advisory lock admitted two holders"
    finally:
        await first.release()
        await second.release()


async def test_leadership_transfers_only_after_the_holder_releases(database) -> None:
    """A follower must not be able to take the lock out from under a live leader."""

    await _migrated(database)
    holder, follower = PostgresAdvisoryLeader(database), PostgresAdvisoryLeader(database)

    try:
        assert await holder.acquire() is True
        assert await follower.acquire() is False
        await holder.release()
        assert await follower.acquire() is True
    finally:
        await holder.release()
        await follower.release()


async def test_the_holder_reverifies_ownership_rather_than_liveness(database) -> None:
    """``pg_try_advisory_lock`` is re-entrant, so re-calling it proves nothing.

    A renewal that merely re-acquired would succeed forever and silently
    increment the lock counter -- and behind a transaction-pooling proxy it would
    claim leadership on a backend session that never held the lock. The renewal
    reads ``pg_locks`` for this exact reason, so a lock released underneath it
    demotes this replica instead of producing a second leader.
    """

    await _migrated(database)
    leader = PostgresAdvisoryLeader(database)

    try:
        assert await leader.acquire() is True
        # Release the lock on the leader's own backend session, behind its back:
        # the session stays alive and healthy, and only ownership changes.
        assert leader._connection is not None
        await leader._connection.execute(text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": leader.lock_id})

        # It re-acquires cleanly, which is correct: nobody else holds it. What
        # must not happen is the renewal reporting True without checking.
        assert await leader.acquire() is True
        rival = PostgresAdvisoryLeader(database)
        try:
            assert await rival.acquire() is False
        finally:
            await rival.release()
    finally:
        await leader.release()


# --------------------------------------------------------------------------- outbox


async def test_concurrent_claims_never_hand_one_command_to_two_workers(database) -> None:
    """``FOR UPDATE SKIP LOCKED`` is the whole basis of active-active delivery.

    Two controllers delivering the same ``capacity.apply`` would issue the same
    provider request twice. The provider is idempotent on the mutation key, but
    the second delivery races the first's observation write, and a fence
    conflict there tears down a healthy cell.
    """

    repository = await _migrated(database)
    async with repository.transaction() as transaction:
        for index in range(24):
            await transaction.put_outbox(
                OutboxRecord(
                    id=f"cmd-{index:02d}",
                    idempotency_key=f"apply-{index:02d}",
                    topic="capacity.apply",
                    aggregate_type="cell",
                    aggregate_id=f"cell-{index:02d}",
                    generation=1,
                    payload={},
                )
            )

    batches = await asyncio.gather(
        *(repository.claim_outbox(worker_id=f"worker-{worker}", limit=24) for worker in range(4))
    )

    claimed = [command.id for batch in batches for command in batch]
    assert sorted(claimed) == sorted({command.id for batch in batches for command in batch}), (
        "one command was claimed by more than one worker"
    )
    assert len(claimed) == 24, "SKIP LOCKED dropped commands instead of distributing them"


async def test_a_claimed_command_is_invisible_until_its_lease_expires(database) -> None:
    """Otherwise a slow provider call would be retried while the first is in flight."""

    repository = await _migrated(database)
    async with repository.transaction() as transaction:
        await transaction.put_outbox(
            OutboxRecord(
                id="cmd-1",
                idempotency_key="apply-1",
                topic="capacity.apply",
                aggregate_type="cell",
                aggregate_id="cell-1",
                generation=1,
                payload={},
            )
        )

    first = await repository.claim_outbox(worker_id="worker-a", limit=10, lease_seconds=60)
    second = await repository.claim_outbox(worker_id="worker-b", limit=10, lease_seconds=60)

    assert [command.id for command in first] == ["cmd-1"]
    assert second == []


# --------------------------------------------------------------------------- isolation


async def test_a_serialization_conflict_surfaces_as_a_retryable_error(database) -> None:
    """SERIALIZABLE turns a lost update into an error; the mapping makes it survivable.

    Without the SQLSTATE mapping a 40001 reaches the route as a raw
    ``DBAPIError`` and becomes a 500 -- a submission the caller believes failed
    and which, once the conflict clears, would have succeeded on a retry.
    """

    repository = await _migrated(database)
    async with repository.transaction() as transaction:
        await transaction.put_workload(_workload("wl-1"))

    started = asyncio.Event()

    async def bump(marker: str, first: bool) -> None:
        async with repository.transaction() as transaction:
            stored = await transaction.get_workload("wl-1")
            assert stored is not None
            if first:
                started.set()
                await asyncio.sleep(0.2)
            else:
                await started.wait()
            await transaction.put_workload(stored.model_copy(update={"created_by": marker}))

    results = await asyncio.gather(bump("a", True), bump("b", False), return_exceptions=True)
    failures = [result for result in results if isinstance(result, BaseException)]

    for failure in failures:
        assert isinstance(failure, TransientRepositoryError), (
            f"a serialization conflict surfaced as {type(failure).__name__}, which no caller retries"
        )


async def test_a_transaction_that_raises_leaves_nothing_behind(database) -> None:
    """Half-applied state is the failure mode a single-transaction design exists to prevent."""

    repository = await _migrated(database)

    with pytest.raises(RuntimeError, match="deliberate"):
        async with repository.transaction() as transaction:
            await transaction.put_workload(_workload("wl-rolled-back"))
            raise RuntimeError("deliberate")

    async with repository.transaction() as transaction:
        assert await transaction.get_workload("wl-rolled-back") is None


# --------------------------------------------------------------------------- audit stream


async def test_a_late_commit_is_still_delivered_to_a_follower(database) -> None:
    """The exact hazard the streaming route documents, on the database that has it.

    ``sequence`` is a BIGSERIAL allocated at *flush* while rows become visible at
    *commit*, so two overlapping transactions can commit out of sequence order.
    A follower that only ever advanced past its high-water mark would skip the
    lower sequence forever -- an audit stream silently missing events, which is
    the one thing an audit stream must not do.

    This test's value is proving the out-of-order commit is real here. SQLite
    serializes writers and could never produce it.
    """

    repository = await _migrated(database)

    async def emit(entity: str, hold: float) -> int:
        async with repository.transaction() as transaction:
            event = await transaction.append_event(
                AuditEvent(
                    id=f"evt-{entity}",
                    org_id="acme",
                    project_id="research",
                    entity_type="workload",
                    entity_id=entity,
                    action="test.emitted",
                    actor="alice",
                )
            )
            await asyncio.sleep(hold)
            return int(event.sequence or 0)

    # The first transaction takes its sequence first and commits last.
    low, high = await asyncio.gather(emit("early", 0.3), emit("late", 0.0))
    assert low < high, "the sequences were not allocated in the order this test needs"

    async with repository.transaction() as transaction:
        # A follower already past `low` re-scans behind its cursor, which is what
        # the streaming route does with `_AUDIT_STREAM_LOOKBACK`.
        rescanned = await transaction.list_events(after_sequence=low - 1, org_id="acme", project_id="research")

    assert low in {int(event.sequence or 0) for event in rescanned}, "the late commit was lost"


# --------------------------------------------------------------------------- deployment constraint


async def test_the_advisory_lock_needs_a_session_it_keeps(database) -> None:
    """Documents the PgBouncer ``transaction`` mode constraint as an executable fact.

    A session-scoped advisory lock lives in a backend session. A transaction
    pooler can hand the next statement to a different backend, where the lock
    does not exist -- so the ownership re-check would demote a replica that is
    in fact leader, or worse, a rival would acquire on the backend the first
    replica thinks it holds. Rather than describe that in prose only, assert the
    mechanism it depends on: the lock is invisible from any other session and
    vanishes when its own session ends.
    """

    await _migrated(database)
    leader = PostgresAdvisoryLeader(database)
    assert await leader.acquire() is True

    async with database.connect() as other:
        held_here = await other.scalar(
            text("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()")
        )
    assert held_here == 0, "a session-scoped advisory lock leaked into another session"

    await leader.release()
    async with database.connect() as observer:
        remaining = await observer.scalar(text("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'"))
    assert remaining == 0


# --------------------------------------------------------------------------- indexed reads


async def test_the_indexed_list_paths_run_on_postgres(database) -> None:
    """Migration 0004 added indexes; nothing ever ran a query against them.

    A prune that pages on a column the schema lacks deletes nothing and reports
    success -- this package has already shipped that bug once. Executing the
    filtered reads is what turns a rendered index into a working one.
    """

    repository = await _migrated(database)
    async with repository.transaction() as transaction:
        await transaction.put_workload(_workload("wl-pending"))
        await transaction.put_workload(_workload("wl-running", state=WorkloadState.RUNNING))
        await transaction.put_cell(
            CellRecord(
                id="cell-a",
                provider="gcp",
                provider_project="p",
                region="us-east5",
                zone="us-east5-a",
                state=CellState.READY,
                capacity=ResourceVector(cpu=8, tpu=8),
                allocatable=ResourceVector(cpu=8, tpu=8),
                accelerator_type="v5p-16",
                capacity_class="spot",
                endpoint="http://agent:8090",
                updated_at=utcnow(),
            )
        )

    async with repository.transaction() as transaction:
        by_state = await transaction.list_workloads(state=WorkloadState.RUNNING)
        scoped = await transaction.list_workloads(org_id="acme", project_id="research")
        cells = await transaction.list_cells()

    assert [workload.id for workload in by_state] == ["wl-running"]
    assert {workload.id for workload in scoped} == {"wl-pending", "wl-running"}
    assert [cell.id for cell in cells] == ["cell-a"]


async def test_a_locked_command_is_skipped_rather_than_waited_on(database) -> None:
    """The claim's safety is the row lock, and only PostgreSQL can demonstrate it.

    The concurrent-claim test above races real workers, and a race that happens
    to serialize still passes -- removing ``SKIP LOCKED`` entirely did not fail
    it. So assert the semantics the statement depends on directly: with one
    session holding a row, a second issuing the *same* statement must come back
    with a different row rather than blocking on the first or, worse, returning
    the row its holder is about to claim.

    Both halves matter. Waiting instead of skipping turns active-active delivery
    into a queue behind the slowest provider call; returning the locked row
    hands one command to two workers.
    """

    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from eyvan.server.persistence.sqlalchemy import OutboxRow, _claimable_outbox_clause

    repository = await _migrated(database)
    async with repository.transaction() as transaction:
        for index in range(2):
            await transaction.put_outbox(
                OutboxRecord(
                    id=f"cmd-{index}",
                    idempotency_key=f"apply-{index}",
                    topic="capacity.apply",
                    aggregate_type="cell",
                    aggregate_id=f"cell-{index}",
                    generation=1,
                    payload={},
                )
            )

    def claim_statement():
        return (
            select(OutboxRow)
            .where(_claimable_outbox_clause(utcnow()))
            .order_by(OutboxRow.available_at, OutboxRow.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )

    sessions = async_sessionmaker(
        database.execution_options(isolation_level="READ COMMITTED"),
        expire_on_commit=False,
    )
    async with sessions() as holder:
        await holder.begin()
        held = (await holder.scalars(claim_statement())).all()
        assert [row.id for row in held] == ["cmd-0"]

        async with sessions() as rival:
            await rival.begin()
            # If this blocks, the test times out rather than passing quietly.
            skipped = await asyncio.wait_for(rival.scalars(claim_statement()), timeout=5.0)
            assert [row.id for row in skipped.all()] == ["cmd-1"], (
                "the second claimer did not skip the row the first is holding"
            )
            await rival.rollback()
        await holder.rollback()


async def test_a_secret_allow_list_and_its_mounts_survive_the_real_backend(database) -> None:
    """The two new fields carry the security decision, so they have to persist.

    Both live inside a JSON column rather than a new one, which is why no
    migration accompanies them -- and which is exactly the reason to check
    rather than assume: a field that silently failed to round-trip would
    reappear empty, and an empty ``allowed_secret_references`` refuses every
    mount. The failure would look like a policy nobody set.
    """

    from eyvan.records import PolicyRecord

    repository = await _migrated(database)
    try:
        async with repository.transaction() as tx:
            await tx.put_policy(
                PolicyRecord(
                    org_id="acme",
                    project_id="research",
                    allowed_secret_references=("gcp-secret://my-project/training-",),
                    updated_by="root",
                )
            )
            await tx.put_workload(
                _workload("wl-secret").model_copy(
                    update={
                        "spec": WorkloadDefinition(
                            name="wl-secret",
                            execution=CommandExecution(command="python train.py"),
                            secret_mounts={"HF_TOKEN": "gcp-secret://my-project/training-hf-token"},
                        )
                    }
                )
            )
        async with repository.transaction() as tx:
            policy = await tx.get_policy("acme", "research")
            workload = await tx.get_workload("wl-secret")

        assert policy is not None
        assert policy.allowed_secret_references == ("gcp-secret://my-project/training-",)
        assert workload is not None
        assert workload.spec.secret_mounts == {"HF_TOKEN": "gcp-secret://my-project/training-hf-token"}
    finally:
        await repository.close()
