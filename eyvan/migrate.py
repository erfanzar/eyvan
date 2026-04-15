"""Programmatic Alembic entrypoint shipped inside the eyvan wheel.

The wheel bundles ``eyvan/persistence/migrations/`` so an installed package
(or container image) can upgrade the control-plane schema without an
external ``alembic.ini`` or a source checkout.
"""

from __future__ import annotations

import os
from pathlib import Path

from alembic import command
from alembic.config import Config

MIGRATIONS_DIR = Path(__file__).resolve().parent / "persistence" / "migrations"
DEFAULT_DATABASE_URL = "postgresql+asyncpg://eyvan:eyvan@127.0.0.1:5432/eyvan"

__all__ = ("DEFAULT_DATABASE_URL", "MIGRATIONS_DIR", "build_alembic_config", "main")


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


def main() -> None:
    """Upgrade the configured database to ``head`` (``alembic upgrade head``)."""

    command.upgrade(build_alembic_config(), "head")


if __name__ == "__main__":
    main()
