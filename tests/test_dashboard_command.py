"""`eyvan dashboard` derives what a developer used to export by hand.

Six environment variables, each of which fails *quietly* when wrong: a bad API
target renders an empty fleet that reads as "nothing is running", a missing
dev-auth flag drops the credential before any request, and the default `viewer`
role shows an empty Fleet page because cells and pools need `manage_fleet`.
"""

from __future__ import annotations

from click.testing import CliRunner

from eyvan.cli import main


def _env(output: str) -> dict[str, str]:
    return dict(line.split("=", 1) for line in output.strip().splitlines() if "=" in line)


def test_the_dashboard_targets_this_session_not_a_hardcoded_port() -> None:
    """`eyvan runtime` picks a per-profile port; 8080 is frequently someone else."""

    result = CliRunner().invoke(
        main,
        ["--url", "http://127.0.0.1:8099", "--org", "acme", "--project", "research", "dashboard", "--print-env"],
    )

    assert result.exit_code == 0, result.output
    assert _env(result.output)["EYVAN_DASHBOARD_API"] == "http://127.0.0.1:8099"


def test_development_auth_is_enabled_explicitly() -> None:
    """Without the flag `apiAuthHeaders()` returns {} and every request 401s."""

    result = CliRunner().invoke(main, ["--url", "http://127.0.0.1:8099", "dashboard", "--print-env"])

    assert _env(result.output)["VITE_EYVAN_DEV_AUTH"] == "true"


def test_the_scope_comes_from_the_session() -> None:
    """A dashboard pointed at a different project shows an empty, plausible page."""

    result = CliRunner().invoke(
        main,
        ["--url", "http://127.0.0.1:8099", "--org", "acme", "--project", "research", "dashboard", "--print-env"],
    )

    environment = _env(result.output)
    assert environment["VITE_EYVAN_ORG"] == "acme"
    assert environment["VITE_EYVAN_PROJECT"] == "research"


def test_the_role_carries_over_so_the_fleet_page_is_populated() -> None:
    """Cells, pools and leases all require manage_fleet; `viewer` sees nothing."""

    result = CliRunner().invoke(
        main,
        ["--url", "http://127.0.0.1:8099", "--role", "fleet_admin", "dashboard", "--print-env"],
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
            "dashboard",
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
            "dashboard",
            "--print-env",
            "--credential-path",
            str(tmp_path / "absent"),
        ],
    )

    assert "VITE_EYVAN_LOCAL_CREDENTIAL" not in _env(result.output)
