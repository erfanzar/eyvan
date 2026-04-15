"""Packaging invariants: pinned image requirements and the packaged migration entrypoint."""

from __future__ import annotations

import re
import typing as tp
from pathlib import Path

import pytest
import tomllib
from alembic.script import ScriptDirectory

import eyvan.migrate as migrate

EYVAN_ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS_PATH = EYVAN_ROOT / "requirements-docker.txt"


def _normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _pyproject() -> dict[str, tp.Any]:
    with (EYVAN_ROOT / "pyproject.toml").open("rb") as handle:
        return tomllib.load(handle)


def _requirement_lines(path: Path) -> list[str]:
    """Return logical requirement lines with backslash continuations joined."""

    logical_lines: list[str] = []
    buffer = ""
    for raw_line in path.read_text().splitlines():
        line = buffer + " " + raw_line.strip() if buffer else raw_line.strip()
        if line.endswith("\\"):
            buffer = line[:-1].rstrip()
            continue
        buffer = ""
        if line:
            logical_lines.append(line)
    if buffer:
        logical_lines.append(buffer)
    return logical_lines


def _pinned_requirements(path: Path) -> dict[str, str]:
    pinned: dict[str, str] = {}
    for line in _requirement_lines(path):
        if line.startswith("#"):
            continue
        token = line.split()[0]
        name, _, _version = token.partition("==")
        pinned[_normalize(name)] = token
    return pinned


def test_requirements_docker_exists_and_every_line_is_pinned() -> None:
    assert REQUIREMENTS_PATH.is_file(), "run: uv export --package eyvan --no-dev --no-emit-workspace --locked"
    requirement_lines = [line for line in _requirement_lines(REQUIREMENTS_PATH) if not line.startswith("#")]
    assert requirement_lines, "requirements-docker.txt has no requirement lines"
    for line in requirement_lines:
        token = line.split()[0]
        assert "==" in token, f"requirement is not fully pinned: {token!r}"
        assert "--hash=sha256:" in line, f"requirement is missing hashes: {token!r}"


def test_requirements_cover_every_mandatory_pyproject_dependency() -> None:
    dependencies = _pyproject()["project"]["dependencies"]
    mandatory = {_normalize(re.match(r"[A-Za-z0-9_.-]+", dependency).group(0)) for dependency in dependencies}
    assert mandatory, "pyproject declares no mandatory dependencies"
    pinned = _pinned_requirements(REQUIREMENTS_PATH)
    missing = mandatory - pinned.keys()
    assert not missing, f"mandatory dependencies missing from requirements-docker.txt: {sorted(missing)}"


def test_pyproject_registers_migrate_entrypoint() -> None:
    scripts = _pyproject()["project"]["scripts"]
    assert scripts.get("eyvan-migrate") == "eyvan.migrate:main"


def test_alembic_config_points_at_packaged_migrations(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EYVAN_DATABASE_URL", raising=False)
    config = migrate.build_alembic_config("postgresql+asyncpg://eyvan:eyvan@db:5432/eyvan")
    script_location = Path(config.get_main_option("script_location"))
    assert script_location == migrate.MIGRATIONS_DIR
    assert (script_location / "env.py").is_file()
    assert (script_location / "versions").is_dir()
    assert config.get_main_option("sqlalchemy.url") == "postgresql+asyncpg://eyvan:eyvan@db:5432/eyvan"


def test_alembic_config_honors_database_url_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EYVAN_DATABASE_URL", "postgresql+asyncpg://eyvan:eyvan@db.internal:5432/eyvan")
    config = migrate.build_alembic_config()
    assert config.get_main_option("sqlalchemy.url") == "postgresql+asyncpg://eyvan:eyvan@db.internal:5432/eyvan"


def test_packaged_migrations_are_discoverable_by_alembic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EYVAN_DATABASE_URL", raising=False)
    script = ScriptDirectory.from_config(migrate.build_alembic_config())
    discovered = {revision.revision for revision in script.walk_revisions()}
    version_files = {path.stem for path in migrate.MIGRATIONS_DIR.glob("versions/*.py") if path.stem != "__init__"}
    assert version_files, "no migration scripts are packaged"
    assert discovered == version_files


def test_migrate_main_upgrades_head_with_env_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tp.Any, str]] = []

    def fake_upgrade(config: tp.Any, revision: str, **_kwargs: tp.Any) -> None:
        calls.append((config, revision))

    monkeypatch.setattr(migrate.command, "upgrade", fake_upgrade)
    monkeypatch.setenv("EYVAN_DATABASE_URL", "postgresql+asyncpg://eyvan:eyvan@db.internal:5432/eyvan")
    migrate.main()
    assert len(calls) == 1
    config, revision = calls[0]
    assert revision == "head"
    assert config.get_main_option("sqlalchemy.url") == "postgresql+asyncpg://eyvan:eyvan@db.internal:5432/eyvan"
    assert Path(config.get_main_option("script_location")) == migrate.MIGRATIONS_DIR


def test_dockerfiles_install_from_pinned_requirements() -> None:
    for name in ("Dockerfile", "Dockerfile.agent"):
        content = (EYVAN_ROOT / name).read_text()
        assert "uv sync" not in content, f"{name} still re-resolves dependencies at image-build time"
        assert "requirements-docker.txt" in content, f"{name} does not install the pinned requirements"


def test_migration_job_uses_wheel_entrypoint() -> None:
    content = (EYVAN_ROOT / "deploy" / "migration-job.yaml").read_text()
    assert 'command: ["eyvan-migrate"]' in content
    assert "alembic" not in content


def test_lazy_create_app_reexport_matches_the_real_factory_signature() -> None:
    """``eyvan.api.create_app`` must expose every parameter of the real factory.

    The lazy re-export exists so ``import eyvan.api`` does not pull in FastAPI.
    It re-declares the signature by hand, so any parameter added to
    ``eyvan.api.app.create_app`` and not mirrored here becomes silently
    unreachable through the public entry point.
    """

    import inspect

    import eyvan.api as lazy_api
    from eyvan.api.app import create_app as real_create_app

    lazy = inspect.signature(lazy_api.create_app)
    real = inspect.signature(real_create_app)

    assert list(lazy.parameters) == list(real.parameters)
    for name, real_parameter in real.parameters.items():
        assert lazy.parameters[name].kind is real_parameter.kind, f"{name} changed parameter kind"
        assert lazy.parameters[name].default == real_parameter.default, f"{name} default drifted"
