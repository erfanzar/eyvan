# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Safe programmatic Alembic entrypoint shipped inside the Eyvan wheel.

The wheel bundles ``eyvan/server/persistence/migrations/`` so an installed package
(or container image) can inspect and upgrade the control-plane schema without an
external ``alembic.ini`` or a source checkout.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import typing as tp
from dataclasses import dataclass
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from alembic.script.revision import RevisionError
from alembic.util.exc import CommandError
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from eyvan._internal.compat import StrEnum

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
DEFAULT_DATABASE_URL = "postgresql+asyncpg://eyvan:eyvan@127.0.0.1:5432/eyvan"


class UnsafeAutomaticMigrationError(RuntimeError):
    """Raised when the packaged migration policy refuses a database state."""


class MigrationClassification(StrEnum):
    """Relationship between the database revision and the packaged graph."""

    FRESH = "fresh"
    EXACT_HEAD = "exact_head"
    BEHIND = "behind"
    UNKNOWN = "unknown"
    AHEAD = "ahead"
    DIVERGED = "diverged"
    MULTIPLE_CURRENT_REVISIONS = "multiple_current_revisions"
    MULTIPLE_PACKAGED_HEADS = "multiple_packaged_heads"
    INVALID_PACKAGED_GRAPH = "invalid_packaged_graph"


@dataclass(frozen=True, slots=True)
class MigrationDatabaseState:
    """Database facts and their relationship to the packaged Alembic graph."""

    tables: tuple[str, ...]
    current_revisions: tuple[str, ...]
    expected_heads: tuple[str, ...]
    classification: MigrationClassification

    @property
    def is_fresh(self) -> bool:
        """Whether the target schema contains no tables and the graph has one head."""

        return self.classification is MigrationClassification.FRESH

    @property
    def is_at_head(self) -> bool:
        """Whether the database is at the one exact packaged head."""

        return self.classification is MigrationClassification.EXACT_HEAD

    @property
    def requires_maintenance(self) -> bool:
        """Whether a maintenance acknowledgement may authorize this upgrade."""

        return self.classification is MigrationClassification.BEHIND


__all__ = (
    "DEFAULT_DATABASE_URL",
    "MIGRATIONS_DIR",
    "MigrationClassification",
    "MigrationDatabaseState",
    "UnsafeAutomaticMigrationError",
    "build_alembic_config",
    "classify_database_state",
    "inspect_database_state",
    "main",
    "require_safe_migration",
)


def build_alembic_config(database_url: str | None = None) -> Config:
    """Build an Alembic config bound to the packaged migrations.

    Args:
        database_url: SQLAlchemy URL for the control-plane database. Defaults to
            ``EYVAN_DATABASE_URL``, then to the local-development default. When
            migrations actually run, the packaged ``env.py`` re-applies
            ``EYVAN_DATABASE_URL`` if set, matching the source-tree
            ``alembic.ini`` flow.

    Returns:
        Config: Alembic configuration pointing at the packaged migration scripts.
    """

    config = Config()
    config.set_main_option("script_location", str(MIGRATIONS_DIR))
    config.set_main_option("sqlalchemy.url", database_url or os.getenv("EYVAN_DATABASE_URL") or DEFAULT_DATABASE_URL)
    return config


def _resolves_exactly(script_directory: ScriptDirectory, revision: str) -> bool:
    """Return whether ``revision`` is an exact ID in the supplied graph."""

    try:
        resolved = script_directory.get_revision(revision)
    except (CommandError, RevisionError):
        return False
    return resolved is not None and resolved.revision == revision


def _is_ancestor(script_directory: ScriptDirectory, ancestor: str, descendant: str) -> bool:
    """Return whether Alembic can traverse from ``descendant`` to ``ancestor``."""

    if ancestor == descendant:
        return True
    try:
        tuple(script_directory.iterate_revisions(descendant, ancestor))
    except (CommandError, RevisionError):
        return False
    return True


def classify_database_state(
    *,
    tables: tp.Iterable[str],
    current_revisions: tp.Iterable[str],
    script_directory: ScriptDirectory,
) -> MigrationDatabaseState:
    """Classify database facts against an Alembic revision graph.

    Only a single exact packaged head can be an automatic target. A single
    database revision is ``behind`` only when Alembic proves that exact revision
    is an ancestor of that head. Every state that cannot make that proof remains
    fail-closed, including unversioned non-empty schemas.
    """

    normalized_tables = tuple(sorted(str(table) for table in tables))
    normalized_current = tuple(sorted(str(revision) for revision in current_revisions))
    expected_heads = tuple(sorted(str(revision) for revision in script_directory.get_heads()))

    if not expected_heads:
        classification = MigrationClassification.INVALID_PACKAGED_GRAPH
    elif len(expected_heads) > 1:
        classification = MigrationClassification.MULTIPLE_PACKAGED_HEADS
    elif not _resolves_exactly(script_directory, expected_heads[0]):
        classification = MigrationClassification.INVALID_PACKAGED_GRAPH
    elif len(normalized_current) > 1:
        classification = MigrationClassification.MULTIPLE_CURRENT_REVISIONS
    elif not normalized_tables and not normalized_current:
        classification = MigrationClassification.FRESH
    elif not normalized_current:
        classification = MigrationClassification.UNKNOWN
    else:
        current = normalized_current[0]
        expected = expected_heads[0]
        if not _resolves_exactly(script_directory, current):
            classification = MigrationClassification.UNKNOWN
        elif current == expected:
            classification = MigrationClassification.EXACT_HEAD
        elif _is_ancestor(script_directory, current, expected):
            classification = MigrationClassification.BEHIND
        elif _is_ancestor(script_directory, expected, current):
            classification = MigrationClassification.AHEAD
        else:
            classification = MigrationClassification.DIVERGED

    return MigrationDatabaseState(
        tables=normalized_tables,
        current_revisions=normalized_current,
        expected_heads=expected_heads,
        classification=classification,
    )


async def _inspect_database_state(config: Config) -> MigrationDatabaseState:
    """Inspect schema tables and the Alembic revision through the async driver."""

    database_url = config.get_main_option("sqlalchemy.url")
    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:

            def read_state(sync_connection: tp.Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
                inspector = inspect(sync_connection)
                tables = tuple(sorted(inspector.get_table_names()))
                revisions: tuple[str, ...] = ()
                if "alembic_version" in tables:
                    rows = sync_connection.execute(text("SELECT version_num FROM alembic_version"))
                    revisions = tuple(sorted(str(row[0]) for row in rows))
                return tables, revisions

            tables, revisions = await connection.run_sync(read_state)
    finally:
        await engine.dispose()
    return classify_database_state(
        tables=tables,
        current_revisions=revisions,
        script_directory=ScriptDirectory.from_config(config),
    )


def inspect_database_state(config: Config) -> MigrationDatabaseState:
    """Return the database state used by the safe-upgrade gate."""

    return asyncio.run(_inspect_database_state(config))


def require_safe_migration(
    state: MigrationDatabaseState,
    *,
    maintenance_acknowledged: bool,
) -> None:
    """Apply the fail-closed migration policy to one classified state.

    Acknowledgement is deliberately narrow and one-use: it permits only a
    database proven to be behind the single packaged head. Supplying it for a
    fresh or exact-head database is rejected so a Helm value cannot remain set
    and silently authorize a later migration.
    """

    classification = state.classification
    current = ", ".join(state.current_revisions) or "no Alembic revision"
    expected = ", ".join(state.expected_heads) or "no packaged head"

    if classification in {MigrationClassification.FRESH, MigrationClassification.EXACT_HEAD}:
        if maintenance_acknowledged:
            raise UnsafeAutomaticMigrationError(
                "refusing an unnecessary --maintenance-acknowledged flag "
                f"for migration state {classification.value}; remove the one-use acknowledgement"
            )
        return

    if classification is MigrationClassification.BEHIND:
        if maintenance_acknowledged:
            return
        raise UnsafeAutomaticMigrationError(
            "refusing to upgrade a known-behind schema outside acknowledged maintenance "
            f"(database: {current}; packaged head: {expected}). Quiesce every writer, "
            "verify a backup, rehearse restore, then rerun with --maintenance-acknowledged. "
            "A failed rollout is repaired forward; never run an Alembic downgrade as rollback."
        )

    raise UnsafeAutomaticMigrationError(
        "refusing an unsafe Alembic revision relationship "
        f"({classification.value}; database: {current}; packaged head: {expected}). "
        "--maintenance-acknowledged authorizes only one exact, known ancestor of a single packaged head."
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Safely inspect or upgrade the Eyvan database schema")
    parser.add_argument(
        "--check",
        action="store_true",
        help="evaluate the migration policy without changing the database",
    )
    parser.add_argument(
        "--maintenance-acknowledged",
        action="store_true",
        help="acknowledge that writers are quiesced and backup/restore preparation is complete",
    )
    return parser


def main(argv: tp.Sequence[str] | None = None) -> int:
    """Inspect the configured database, then safely upgrade it to ``head``."""

    args = _parser().parse_args(list(argv) if argv is not None else None)
    config = build_alembic_config()
    state = inspect_database_state(config)
    require_safe_migration(state, maintenance_acknowledged=args.maintenance_acknowledged)
    if args.check:
        status = {
            MigrationClassification.FRESH: "fresh",
            MigrationClassification.EXACT_HEAD: "at packaged head",
            MigrationClassification.BEHIND: "known behind; maintenance acknowledged",
        }[state.classification]
        print(f"Eyvan migration check passed: {status}.")
        return 0
    command.upgrade(config, "head")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
