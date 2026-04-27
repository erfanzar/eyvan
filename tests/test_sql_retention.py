# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Retention against a real database, not the in-memory store.

The SQLAlchemy repository is what production runs, and it had no direct test at
all -- every retention test used ``InMemoryRepository``, whose prune is a Python
list comprehension and therefore cannot exhibit the failure this module exists
to pin down.

That failure is a paging bug. ``prune_outbox`` and ``prune_idempotency`` each
applied their cutoff *after* the ``LIMIT`` had chosen a page, and neither paged
on the column it filtered: outbox by ``available_at`` (set at enqueue) while
filtering ``updated_at`` (set at settlement), idempotency by a surrogate
autoincrement ``id`` with no relation to time at all. A full window of
ineligible rows therefore starved the genuinely expired rows behind it, and the
prune returned 0 on every pass -- forever. For idempotency that means an expired
key stays honored, which is precisely the bug retention was added to fix.

SQLite via aiosqlite stands in for PostgreSQL. It executes the same generated
SQL, which is where the bug lived; it is not a substitute for PostgreSQL on
locking or advisory-lock leadership.

Two paths are genuinely PostgreSQL-only and are worked around rather than
tested here, deliberately and visibly:

- ``put_idempotency`` takes ``pg_advisory_xact_lock(hashtextextended(...))``, so
  these tests insert ``IdempotencyRow`` directly. The subject under test is the
  prune query, not the write path.
- ``audit_events.sequence`` is a ``BigInteger`` identity column. SQLite only
  autoincrements a literal ``INTEGER PRIMARY KEY``, so audit appends cannot run
  here at all; audit pruning keeps its in-memory coverage in ``test_retention``,
  where its ordering column (``sequence``) is monotonic with time and therefore
  cannot exhibit the stall this module pins down.
"""

from __future__ import annotations

import typing as tp
from datetime import timedelta

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import create_async_engine

from eyvan._internal.json_types import model_json
from eyvan.records import (
    IdempotencyRecord,
    OutboxRecord,
    OutboxState,
    utcnow,
)
from eyvan.server.persistence.sqlalchemy import IdempotencyRow, SqlAlchemyRepository, create_schema


@pytest.fixture
async def repository(tmp_path) -> tp.AsyncIterator[SqlAlchemyRepository]:
    # A file rather than :memory: so every pooled connection sees one schema.
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'eyvan.db'}")
    await create_schema(engine)
    try:
        yield SqlAlchemyRepository(engine)
    finally:
        await engine.dispose()


def _outbox(record_id: str, *, enqueued_days_ago: int, settled_days_ago: int) -> OutboxRecord:
    """A settled command whose enqueue and settlement times differ.

    The gap is the whole point: a command queued long ago but settled a moment
    ago sorts first by ``available_at`` and is not eligible by ``updated_at``.
    """

    now = utcnow()
    return OutboxRecord(
        id=record_id,
        topic="capacity.apply",
        aggregate_type="cell",
        aggregate_id="cell-a",
        idempotency_key=f"capacity:{record_id}",
        generation=1,
        state=OutboxState.SUCCEEDED,
        payload={},
        available_at=now - timedelta(days=enqueued_days_ago),
        created_at=now - timedelta(days=enqueued_days_ago),
        updated_at=now - timedelta(days=settled_days_ago),
    )


def _idempotency(key: str, *, age_days: int) -> IdempotencyRecord:
    return IdempotencyRecord(
        scope="scope_a",
        operation="workload.submit",
        key=key,
        fingerprint="f" * 64,
        resource_type="workload",
        resource_id="wl-1",
        created_at=utcnow() - timedelta(days=age_days),
    )


async def _insert_key(transaction, record: IdempotencyRecord) -> None:
    """Insert a binding without ``put_idempotency``'s PostgreSQL advisory lock."""

    transaction.session.add(
        IdempotencyRow(
            scope=record.scope,
            operation=record.operation,
            key=record.key,
            created_at=record.created_at,
            data=model_json(record),
        )
    )


async def _surviving_keys(transaction) -> set[str]:
    """Read bindings straight from the table, for the same reason."""

    from sqlalchemy import select

    rows = (await transaction.session.scalars(select(IdempotencyRow))).all()
    return {row.key for row in rows}


async def test_the_sql_backend_prunes_settled_outbox_commands(repository) -> None:
    async with repository.transaction() as transaction:
        await transaction.put_outbox(_outbox("old", enqueued_days_ago=90, settled_days_ago=90))
        await transaction.put_outbox(_outbox("fresh", enqueued_days_ago=90, settled_days_ago=0))

    async with repository.transaction() as transaction:
        removed = await transaction.prune_outbox(before=utcnow() - timedelta(days=30))

    assert removed == 1
    async with repository.transaction() as transaction:
        assert {record.id for record in await transaction.list_outbox(limit=100)} == {"fresh"}


async def test_an_outbox_prune_reaches_old_rows_behind_a_full_page_of_new_ones(repository) -> None:
    """The LIMIT must select only deletable rows.

    Ten commands queued long ago but settled just now sort first by
    ``available_at``. With the cutoff applied after a ``LIMIT 5`` on that
    ordering, every pass returned 0 and the genuinely expired rows behind them
    were never reached.
    """

    async with repository.transaction() as transaction:
        for index in range(10):
            await transaction.put_outbox(_outbox(f"recent-{index}", enqueued_days_ago=90, settled_days_ago=0))
        for index in range(3):
            await transaction.put_outbox(_outbox(f"expired-{index}", enqueued_days_ago=80, settled_days_ago=80))

    async with repository.transaction() as transaction:
        removed = await transaction.prune_outbox(before=utcnow() - timedelta(days=30), limit=5)

    assert removed == 3, "an expired command behind a full page of recent ones must still be reached"
    async with repository.transaction() as transaction:
        surviving = {record.id for record in await transaction.list_outbox(limit=100)}
    assert surviving == {f"recent-{index}" for index in range(10)}


async def test_an_idempotency_prune_reaches_old_keys_regardless_of_insertion_order(repository) -> None:
    """Paged by a surrogate id, an expired key past the LIMIT was never deleted.

    ``id`` is an autoincrement with no relationship to ``created_at``, so
    inserting fresh keys first put them at the front of every page. The expired
    keys behind them stayed honored forever -- a caller reusing one months later
    silently replayed the original response instead of acting.
    """

    async with repository.transaction() as transaction:
        for index in range(10):
            await _insert_key(transaction, _idempotency(f"fresh-{index}", age_days=1))
        for index in range(3):
            await _insert_key(transaction, _idempotency(f"expired-{index}", age_days=90))

    async with repository.transaction() as transaction:
        removed = await transaction.prune_idempotency(before=utcnow() - timedelta(days=30), limit=5)

    assert removed == 3
    async with repository.transaction() as transaction:
        assert await _surviving_keys(transaction) == {f"fresh-{index}" for index in range(10)}


async def test_a_bounded_prune_makes_progress_on_every_call(repository) -> None:
    """The batch limit must bound work per call without ever stalling."""

    async with repository.transaction() as transaction:
        for index in range(10):
            await _insert_key(transaction, _idempotency(f"expired-{index}", age_days=90))

    cutoff = utcnow() - timedelta(days=30)
    counts = []
    for _pass in range(4):
        async with repository.transaction() as transaction:
            counts.append(await transaction.prune_idempotency(before=cutoff, limit=4))

    assert counts == [4, 4, 2, 0]


async def test_only_settled_outbox_commands_are_deletable_on_sql(repository) -> None:
    """PENDING, PROCESSING, and FAILED rows are all still owed delivery."""

    async with repository.transaction() as transaction:
        for state in (OutboxState.PENDING, OutboxState.PROCESSING, OutboxState.FAILED):
            record = _outbox(f"owed-{state.value}", enqueued_days_ago=90, settled_days_ago=90)
            await transaction.put_outbox(record.model_copy(update={"state": state}))
        await transaction.put_outbox(_outbox("settled", enqueued_days_ago=90, settled_days_ago=90))

    async with repository.transaction() as transaction:
        removed = await transaction.prune_outbox(before=utcnow() - timedelta(days=30))

    assert removed == 1
    async with repository.transaction() as transaction:
        assert len(await transaction.list_outbox(limit=100)) == 3


# --------------------------------------------------------------------------- per-row decode


async def test_one_unreadable_row_does_not_hide_every_other_workload(repository, caplog) -> None:
    """A corrupt or forward-versioned row used to halt scheduling for the whole fleet.

    Every ``list_*`` decoded inside a comprehension, so one bad row raised out of the
    entire call — and ``list_workloads()`` is what the leader's reconcile loop calls,
    unfiltered, on its hot path. Rolling back to a build that predates a newly added
    field is enough to produce such a row, which is exactly why this matters more with
    each new persisted field.
    """

    import logging

    from sqlalchemy import update

    from eyvan.records import CommandExecution, WorkloadDefinition, WorkloadRecord
    from eyvan.server.persistence.sqlalchemy import WorkloadRow

    def _workload(workload_id: str) -> WorkloadRecord:
        return WorkloadRecord(
            id=workload_id,
            org_id="acme",
            project_id="research",
            spec=WorkloadDefinition(name=workload_id, execution=CommandExecution(command="python train.py")),
            created_by="alice",
        )

    async with repository.transaction() as transaction:
        for name in ("wl-healthy-a", "wl-corrupt", "wl-healthy-b"):
            await transaction.put_workload(_workload(name))

    # Corrupt one payload the way a rollback or a hand-edit would.
    async with repository.transaction() as transaction:
        await transaction.session.execute(
            update(WorkloadRow).where(WorkloadRow.id == "wl-corrupt").values(data={"id": "wl-corrupt"})
        )

    with caplog.at_level(logging.ERROR):
        async with repository.transaction() as transaction:
            listed = await transaction.list_workloads()

    assert {workload.id for workload in listed} == {"wl-healthy-a", "wl-healthy-b"}
    # Loud, not silent: a shrinking list is its own kind of outage.
    assert "wl-corrupt" in caplog.text


async def test_a_corrupt_row_is_skipped_rather_than_guessed_at(repository) -> None:
    """Skipping is the only safe repair: inventing defaults would persist a lie."""

    from sqlalchemy import update

    from eyvan.records import CommandExecution, WorkloadDefinition, WorkloadRecord
    from eyvan.server.persistence.sqlalchemy import WorkloadRow

    async with repository.transaction() as transaction:
        await transaction.put_workload(
            WorkloadRecord(
                id="wl-only",
                org_id="acme",
                project_id="research",
                spec=WorkloadDefinition(name="only", execution=CommandExecution(command="python train.py")),
                created_by="alice",
            )
        )
    async with repository.transaction() as transaction:
        await transaction.session.execute(
            update(WorkloadRow).where(WorkloadRow.id == "wl-only").values(data={"nonsense": True})
        )

    async with repository.transaction() as transaction:
        assert await transaction.list_workloads() == []
        # get_* is unchanged: a single-record read still surfaces the problem, because
        # there is no healthy remainder to protect and a silent None would be worse.
        with pytest.raises(ValidationError):
            await transaction.get_workload("wl-only")


# --------------------------------------------------------------------------- schema revision gate


async def test_a_matching_schema_revision_opens_cleanly(repository) -> None:
    """``create_schema`` stamps the revision this build expects."""

    await repository.open()


async def test_a_database_behind_this_build_refuses_to_open(tmp_path) -> None:
    """Running new code against an un-migrated database must fail loudly.

    This package has already shipped a bug where a prune silently deleted nothing
    because it paged on a column the schema did not have. Code that indexes and pages
    on columns the database lacks produces wrong answers, not errors, so the mismatch
    has to be caught before the first query.
    """

    from sqlalchemy import text

    from eyvan.server.persistence.base import SchemaRevisionMismatch

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'behind.db'}")
    await create_schema(engine)
    async with engine.begin() as connection:
        await connection.execute(text("UPDATE schema_revisions SET revision = '0002_transactional_outbox'"))

    try:
        with pytest.raises(SchemaRevisionMismatch, match="0002_transactional_outbox"):
            await SqlAlchemyRepository(engine).open()
    finally:
        await engine.dispose()


async def test_a_database_ahead_of_this_build_also_refuses_to_open(tmp_path) -> None:
    """Refusal is symmetric: a rollback is as dangerous as a missing migration.

    A newer database holds rows carrying fields this build cannot parse. The per-row
    list decoder exists to *contain* that, one row at a time -- not to make running
    against the wrong schema routine.
    """

    from sqlalchemy import text

    from eyvan.server.persistence.base import SchemaRevisionMismatch

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'ahead.db'}")
    await create_schema(engine)
    async with engine.begin() as connection:
        await connection.execute(text("UPDATE schema_revisions SET revision = '0099_from_the_future'"))

    try:
        with pytest.raises(SchemaRevisionMismatch, match="0099_from_the_future"):
            await SqlAlchemyRepository(engine).open()
    finally:
        await engine.dispose()


async def test_an_unstamped_database_is_allowed_to_open(tmp_path) -> None:
    """A fresh database has no stamp yet, and initialization is about to add one.

    Refusing here would make ``auto_create_schema`` and ``eyvan-migrate`` impossible
    to bootstrap.
    """

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'fresh.db'}")
    try:
        await SqlAlchemyRepository(engine).open()
    finally:
        await engine.dispose()
