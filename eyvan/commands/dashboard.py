# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Launch the operations dashboard against the session's own control plane.

Every value the Vite dev server needs is already known to the CLI: the endpoint
comes from ``--url`` or the saved context, the org and project come from the same
place, the role is the one this session resolved, and the loopback credential is a
file on disk that the browser cannot read for itself.

Before this command, starting the dashboard meant exporting six variables by
hand, and getting any of them wrong failed quietly rather than loudly:

* a wrong ``EYVAN_DASHBOARD_API`` proxied to whatever owned port 8080 and
  rendered an empty fleet, which reads as "nothing is running";
* a missing ``VITE_EYVAN_DEV_AUTH`` made ``apiAuthHeaders()`` return ``{}``, so
  the credential was dropped before any request and the page showed
  "Authentication is not configured";
* the default role is ``viewer``, and cells, pools and leases all require
  ``manage_fleet``, so a correct setup still showed an empty Fleet page.

Deliberately not a second source of truth: this reads the session and the
existing credential file, and shells out to the package manager. It does not
duplicate the dashboard's own configuration schema.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import click

from eyvan.commands.state import CliSession

#: Written by ``eyvan runtime init`` for a loopback control plane in
#: LOOPBACK_DEFAULT auth mode, which 401s every request without it.
DEFAULT_LOCAL_CREDENTIAL_PATH = Path("~/.eyvan/local-development-credential")


def _dashboard_root() -> Path:
    """Return the checked-in dashboard directory, or fail with what to do."""

    root = Path(__file__).resolve().parents[2] / "dashboard"
    if not (root / "package.json").is_file():
        raise click.ClickException(
            f"the Eyvan dashboard sources are not present at {root}; "
            "this command runs the development server from a source checkout"
        )
    return root


def _local_credential(path: Path) -> str | None:
    expanded = path.expanduser()
    try:
        value = expanded.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return value or None


@click.command("dashboard")
@click.option("--port", default=4173, show_default=True, help="Port for the dashboard dev server.")
@click.option("--role", "role_override", help="Dashboard role; defaults to this session's role.")
@click.option(
    "--credential-path",
    type=click.Path(path_type=Path),
    default=DEFAULT_LOCAL_CREDENTIAL_PATH,
    show_default=True,
    help="Loopback development credential the browser cannot read itself.",
)
@click.option("--open/--no-open", "open_browser", default=True, help="Open the dashboard in a browser.")
@click.option("--print-env", is_flag=True, help="Print the resolved environment and exit without starting.")
@click.pass_obj
def dashboard(
    session: CliSession,
    port: int,
    role_override: str | None,
    credential_path: Path,
    open_browser: bool,
    print_env: bool,
) -> None:
    """Run the operations dashboard against the active control plane."""

    root = _dashboard_root()
    roles = role_override or ",".join(session.identity.dev_roles) or "fleet_admin"
    environment = {
        "EYVAN_DASHBOARD_API": session.base_url,
        "VITE_EYVAN_DEV_AUTH": "true",
        "VITE_EYVAN_DEV_ROLE": roles,
    }
    if session.identity.dev_user:
        environment["VITE_EYVAN_DEV_USER"] = session.identity.dev_user
    if session.org_id:
        environment["VITE_EYVAN_ORG"] = session.org_id
    if session.project_id:
        environment["VITE_EYVAN_PROJECT"] = session.project_id
    credential = _local_credential(credential_path)
    if credential is not None:
        environment["VITE_EYVAN_LOCAL_CREDENTIAL"] = credential

    if print_env:
        for name, value in sorted(environment.items()):
            # The credential is the one value that must not be echoed: this
            # output is for pasting into a shell, and shells keep history.
            shown = "<redacted>" if name == "VITE_EYVAN_LOCAL_CREDENTIAL" else value
            click.echo(f"{name}={shown}")
        return

    if credential is None and "127.0.0.1" in session.base_url:
        click.echo(
            f"No development credential at {credential_path}; a loopback control plane will reject "
            "every dashboard request. Run 'eyvan runtime init' first.",
            err=True,
        )

    npm = shutil.which("npm")
    if npm is None:
        raise click.ClickException("npm is required to run the dashboard development server")

    click.echo(f"Eyvan dashboard -> {session.base_url} as {roles}")
    click.echo(f"  http://localhost:{port}/")
    if open_browser:
        click.launch(f"http://localhost:{port}/")
    try:
        subprocess.run(
            [npm, "run", "dev", "--", "--port", str(port)],
            cwd=root,
            env={**os.environ, **environment},
            check=True,
        )
    except KeyboardInterrupt:  # pragma: no cover - interactive exit
        click.echo("dashboard stopped")
    except subprocess.CalledProcessError as exc:
        raise click.ClickException(f"the dashboard development server exited with {exc.returncode}") from exc


def register_dashboard_commands(group: click.Group) -> None:
    """Attach the dashboard command to the Eyvan CLI."""

    group.add_command(dashboard)


__all__ = ("dashboard", "register_dashboard_commands")
