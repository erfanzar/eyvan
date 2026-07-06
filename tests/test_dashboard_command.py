# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Installed-wheel and source-development dashboard command contracts."""

from __future__ import annotations

from click.testing import CliRunner

import eyvan.sdk.commands.dashboard as dashboard_commands
from eyvan.sdk.cli import main


def _env(output: str) -> dict[str, str]:
    return dict(line.split("=", 1) for line in output.strip().splitlines() if "=" in line)


def test_dashboard_opens_the_hosted_control_plane_url(monkeypatch) -> None:
    opened: list[str] = []
    monkeypatch.setattr(dashboard_commands.click, "launch", opened.append)

    result = CliRunner().invoke(main, ["--url", "https://eyvan.example.com", "dashboard"])

    assert result.exit_code == 0, result.output
    assert opened == ["https://eyvan.example.com/"]
    assert "https://eyvan.example.com/" in result.output


def test_dashboard_is_independent_of_source_checkout(monkeypatch) -> None:
    """An isolated wheel has no sibling dashboard/package.json directory."""

    def no_sources() -> None:
        raise AssertionError("the hosted command must not look for dashboard sources")

    monkeypatch.setattr(dashboard_commands, "_dashboard_root", no_sources)
    result = CliRunner().invoke(
        main,
        ["--url", "https://eyvan.example.com/control-plane", "dashboard", "--no-open"],
    )

    assert result.exit_code == 0, result.output
    assert "https://eyvan.example.com/control-plane/" in result.output


def test_dashboard_dev_targets_this_session_not_a_hardcoded_port() -> None:
    """`eyvan runtime` picks a per-profile port; 8080 is frequently someone else."""

    result = CliRunner().invoke(
        main,
        [
            "--url",
            "http://127.0.0.1:8099",
            "--org",
            "acme",
            "--project",
            "research",
            "dashboard-dev",
            "--print-env",
        ],
    )

    assert result.exit_code == 0, result.output
    assert _env(result.output)["EYVAN_DASHBOARD_API"] == "http://127.0.0.1:8099"


def test_development_auth_is_enabled_explicitly() -> None:
    """Without the flag `apiAuthHeaders()` returns {} and every request 401s."""

    result = CliRunner().invoke(main, ["--url", "http://127.0.0.1:8099", "dashboard-dev", "--print-env"])

    assert _env(result.output)["VITE_EYVAN_DEV_AUTH"] == "true"


def test_the_scope_comes_from_the_session() -> None:
    """A dashboard pointed at a different project shows an empty, plausible page."""

    result = CliRunner().invoke(
        main,
        [
            "--url",
            "http://127.0.0.1:8099",
            "--org",
            "acme",
            "--project",
            "research",
            "dashboard-dev",
            "--print-env",
        ],
    )

    environment = _env(result.output)
    assert environment["VITE_EYVAN_ORG"] == "acme"
    assert environment["VITE_EYVAN_PROJECT"] == "research"


def test_the_role_carries_over_so_the_fleet_page_is_populated() -> None:
    """Cells, pools and leases all require manage_fleet; `viewer` sees nothing."""

    result = CliRunner().invoke(
        main,
        ["--url", "http://127.0.0.1:8099", "--role", "fleet_admin", "dashboard-dev", "--print-env"],
    )

    assert _env(result.output)["VITE_EYVAN_DEV_ROLE"] == "fleet_admin"


def test_the_credential_is_never_printed(tmp_path) -> None:
    """`--print-env` output is for pasting into a shell, and shells keep history."""

    credential = tmp_path / "credential"
    credential.write_text("super-secret-value", encoding="utf-8")

    result = CliRunner().invoke(
        main,
        [
            "--url",
            "http://127.0.0.1:8099",
            "dashboard-dev",
            "--print-env",
            "--credential-path",
            str(credential),
        ],
    )

    assert "super-secret-value" not in result.output
    assert _env(result.output)["VITE_EYVAN_LOCAL_CREDENTIAL"] == "<redacted>"


def test_a_missing_credential_is_omitted_rather_than_sent_empty(tmp_path) -> None:
    """An empty header is a request that fails confusingly instead of loudly."""

    result = CliRunner().invoke(
        main,
        [
            "--url",
            "http://127.0.0.1:8099",
            "dashboard-dev",
            "--print-env",
            "--credential-path",
            str(tmp_path / "absent"),
        ],
    )

    assert "VITE_EYVAN_LOCAL_CREDENTIAL" not in _env(result.output)


def test_dashboard_dev_fails_explicitly_without_source_files(monkeypatch) -> None:
    def missing() -> None:
        raise dashboard_commands.click.ClickException(
            "'eyvan dashboard-dev' requires a source checkout; 'eyvan dashboard' opens the hosted production UI"
        )

    monkeypatch.setattr(dashboard_commands, "_dashboard_root", missing)
    result = CliRunner().invoke(main, ["dashboard-dev", "--print-env"])

    assert result.exit_code != 0
    assert "requires a source checkout" in result.output
    assert "opens the hosted production UI" in result.output
