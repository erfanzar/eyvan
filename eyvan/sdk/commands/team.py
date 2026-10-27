# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Team accounts from the CLI: ``eyvan login``/``logout``, ``eyvan users``, ``eyvan tokens``.

``eyvan login`` exchanges an email and password once for a named personal API
token, stores it owner-only (never in the context file), and every later
command and ``init()`` against that plane sends it. The password is never
stored and never accepted as a flag value, so it cannot land in shell history.
"""

from __future__ import annotations

import socket
import sys
import typing as tp

import click
import httpx

from eyvan._internal.json_types import is_json_object
from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
from eyvan.sdk.commands.common import JsonRenderer
from eyvan.sdk.commands.connect import connect as connect_plane
from eyvan.sdk.commands.connect import resolve_url
from eyvan.sdk.commands.state import CliSession
from eyvan.sdk.team_credentials import TeamCredential, TeamCredentialError, TeamCredentialStore


def _http(url: str) -> httpx.Client:
    from eyvan._internal.plane_tls import control_plane_httpx_options

    endpoint = ControlPlaneEndpoint.parse(url)
    return httpx.Client(base_url=url, timeout=20.0, trust_env=not endpoint.is_loopback, **control_plane_httpx_options())


def _detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text[:300] or f"HTTP {response.status_code}"
    detail = payload.get("detail") if is_json_object(payload) else None
    return str(detail) if detail else f"HTTP {response.status_code}"


def _store() -> TeamCredentialStore:
    return TeamCredentialStore()


def _emit(as_json: bool, payload: tp.Any, text: str) -> None:
    if as_json:
        JsonRenderer().emit(payload)
    else:
        click.echo(text)


# ------------------------------------------------------------------- login


@click.command("login")
@click.argument("url")
@click.option("--email", help="Your account email (prompted when omitted).")
@click.option("--password-stdin", is_flag=True, help="Read the password from stdin instead of prompting.")
@click.option("--fleet", help="Fleet to make active when the plane has more than one.")
@click.option("--json", "as_json", is_flag=True, help="Emit the result as JSON.")
@click.pass_obj
def login(
    session: CliSession, url: str, email: str | None, password_stdin: bool, fleet: str | None, as_json: bool
) -> None:
    """Sign in to a team Eyvan plane and remember a personal token for it.

    URL may be a host, host:port or full URL. Afterwards every command and
    init() aimed at that plane is authenticated as you.
    """

    try:
        base = resolve_url(url, None)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    endpoint = ControlPlaneEndpoint.parse(base)
    if not endpoint.is_loopback and endpoint.url.startswith("http://"):
        click.echo(
            "warning: this plane is plain HTTP; your password and token cross the network unencrypted. "
            "Ask the admin to enable TLS.",
            err=True,
        )
    with _http(base) as client:
        try:
            status_response = client.get("/api/v1/team/status")
        except httpx.HTTPError as exc:
            raise click.ClickException(f"could not reach {base}: {exc}") from exc
        if status_response.status_code != 200 or not status_response.json().get("team_mode"):
            raise click.ClickException(
                f"{base} does not run team accounts (EYVAN_TEAM_MODE is off); use `eyvan connect` instead"
            )
        address = email or click.prompt("Email")
        if password_stdin:
            password = sys.stdin.readline().rstrip("\n")
        else:
            password = click.prompt("Password", hide_input=True)
        token_name = f"cli-{socket.gethostname()}"[:64] or "cli"
        response = client.post(
            "/api/v1/team/login", json={"email": address, "password": password, "token_name": token_name}
        )
    if response.status_code != 200:
        raise click.ClickException(f"sign-in failed: {_detail(response)}")
    body = response.json()
    user = body.get("user") or {}
    credential = TeamCredential(
        url=endpoint.url,
        token=body["token"],
        token_id=body["record"]["id"],
        subject=user.get("subject", ""),
        email=user.get("email", address),
    )
    try:
        _store().put(credential)
    except (OSError, TeamCredentialError) as exc:
        raise click.ClickException(f"signed in, but could not save the token: {exc}") from exc
    outcome = connect_plane(
        session, host=endpoint.url, port=None, fleet=fleet, token=credential.token, org_id=None, project_id=None
    )
    lines = [f"signed in to {endpoint.url} as {credential.subject} ({credential.email})"]
    lines.append(f"  token {credential.token_id} saved in {_store().path} (owner-only)")
    lines.extend(f"  {note}" for note in outcome.notes)
    if outcome.saved_context:
        lines.append(f"  active fleet: {outcome.saved_context}")
    _emit(
        as_json,
        {
            "url": endpoint.url,
            "subject": credential.subject,
            "token_id": credential.token_id,
            "saved_context": outcome.saved_context,
            "notes": outcome.notes,
        },
        "\n".join(lines),
    )


@click.command("logout")
@click.argument("url", required=False)
@click.pass_obj
def logout(session: CliSession, url: str | None) -> None:
    """Revoke this machine's token for a plane (default: the current one) and forget it."""

    base = ControlPlaneEndpoint.parse(resolve_url(url, None) if url else session.base_url).url
    try:
        credential = _store().get(base)
    except TeamCredentialError as exc:
        raise click.ClickException(str(exc)) from exc
    if credential is None:
        raise click.ClickException(f"not signed in to {base}")
    revoked = True
    try:
        with _http(base) as client:
            response = client.delete(
                f"/api/v1/team/tokens/{credential.token_id}",
                headers={"Authorization": f"Bearer {credential.token}"},
            )
        revoked = response.status_code in (200, 401, 404)
    except httpx.HTTPError:
        revoked = False
    _store().remove(base)
    if revoked:
        click.echo(f"signed out of {base}; token {credential.token_id} revoked")
    else:
        click.echo(
            f"forgot the local token for {base}, but could not reach the plane to revoke {credential.token_id}; "
            "revoke it with `eyvan tokens revoke` or from the dashboard",
            err=True,
        )


# ------------------------------------------------------------------- users


@click.group("users")
def users() -> None:
    """Team accounts on this plane (team mode)."""


@users.command("me")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def users_me(session: CliSession, as_json: bool) -> None:
    """Show who this CLI is signed in as."""

    payload = session.client.request("GET", "/api/v1/team/me")
    assert is_json_object(payload)
    roles = ", ".join(str(grant.get("role")) for grant in payload.get("grants", []) if is_json_object(grant))
    _emit(as_json, payload, f"{payload.get('subject')}  roles: {roles or 'none'}")


@users.command("list")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def users_list(session: CliSession, as_json: bool) -> None:
    """List every account (fleet admins)."""

    payload = session.client.request("GET", "/api/v1/team/users")
    rows = payload.get("users", []) if is_json_object(payload) else []
    text = "\n".join(
        f"{row['subject']:<20} {row['email']:<32} {'disabled' if row['disabled'] else 'active'}"
        for row in rows
        if is_json_object(row)
    )
    _emit(as_json, payload, text or "no accounts yet; invite someone with `eyvan users invite`")


@users.command("invite")
@click.argument("email")
@click.option("--subject", help="User name (default: the part of the email before @).")
@click.option(
    "--role",
    type=click.Choice(
        ["fleet_admin", "org_admin", "project_admin", "project_operator", "project_submitter", "project_viewer"]
    ),
    help="Role the account gets on acceptance.",
)
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def users_invite(
    session: CliSession,
    email: str,
    subject: str | None,
    role: str | None,
    org_id: str | None,
    project_id: str | None,
    as_json: bool,
) -> None:
    """Invite someone; prints a one-time link to send them privately."""

    name = (subject or email.split("@", 1)[0]).strip().lower()
    body: dict[str, tp.Any] = {"email": email, "subject": name}
    if role is not None:
        scope = session.workload_scope(org_id=org_id, project_id=project_id)
        body["role"] = role
        if role != "fleet_admin":
            body["org_id"] = scope.org_id
        if role.startswith("project_"):
            body["project_id"] = scope.project_id
    payload = session.client.request("POST", "/api/v1/team/invites", json=body)
    assert is_json_object(payload)
    _emit(
        as_json,
        payload,
        f"invited {email} as {name}\n  link (shown once, send it privately): {payload['url']}",
    )


@users.command("disable")
@click.argument("subject")
@click.pass_obj
def users_disable(session: CliSession, subject: str) -> None:
    """Disable an account and revoke all of its tokens."""

    session.client.request("POST", f"/api/v1/team/users/{subject}/disable")
    click.echo(f"disabled {subject}; all of its tokens are revoked")


@users.command("enable")
@click.argument("subject")
@click.pass_obj
def users_enable(session: CliSession, subject: str) -> None:
    """Re-enable an account (its old tokens stay revoked)."""

    session.client.request("POST", f"/api/v1/team/users/{subject}/enable")
    click.echo(f"enabled {subject}")


@users.command("invites")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def users_invites(session: CliSession, as_json: bool) -> None:
    """List invites (fleet admins)."""

    payload = session.client.request("GET", "/api/v1/team/invites")
    rows = payload.get("invites", []) if is_json_object(payload) else []
    text = "\n".join(
        f"{row['id']}  {row['email']:<32} "
        f"{'accepted' if row['accepted_at'] else 'revoked' if row['revoked'] else 'open until ' + row['expires_at']}"
        for row in rows
        if is_json_object(row)
    )
    _emit(as_json, payload, text or "no invites")


@users.command("revoke-invite")
@click.argument("invite_id")
@click.pass_obj
def users_revoke_invite(session: CliSession, invite_id: str) -> None:
    """Revoke an open invite."""

    session.client.request("DELETE", f"/api/v1/team/invites/{invite_id}")
    click.echo(f"revoked {invite_id}")


# ------------------------------------------------------------------ tokens


@click.group("tokens")
def tokens() -> None:
    """Your personal API tokens (team mode)."""


@tokens.command("list")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def tokens_list(session: CliSession, as_json: bool) -> None:
    payload = session.client.request("GET", "/api/v1/team/tokens")
    rows = payload.get("tokens", []) if is_json_object(payload) else []
    text = "\n".join(
        f"{row['id']}  {row['name']:<24} {row['prefix']}…  "
        f"{'revoked' if row['revoked_at'] else 'last used ' + (row['last_used_at'] or 'never')}"
        for row in rows
        if is_json_object(row)
    )
    _emit(as_json, payload, text or "no personal tokens")


@tokens.command("create")
@click.argument("name")
@click.option("--expires-in-days", type=click.IntRange(1, 3650))
@click.pass_obj
def tokens_create(session: CliSession, name: str, expires_in_days: int | None) -> None:
    """Mint a token for a script or CI; printed once."""

    body: dict[str, tp.Any] = {"name": name}
    if expires_in_days is not None:
        body["expires_in_days"] = expires_in_days
    payload = session.client.request("POST", "/api/v1/team/tokens", json=body)
    assert is_json_object(payload)
    click.echo(f"{payload['token']}\n(shown once; use it as EYVAN_TOKEN)", err=False)


@tokens.command("revoke")
@click.argument("token_id")
@click.pass_obj
def tokens_revoke(session: CliSession, token_id: str) -> None:
    session.client.request("DELETE", f"/api/v1/team/tokens/{token_id}")
    click.echo(f"revoked {token_id}")


def register_team_commands(group: click.Group) -> None:
    """Attach login/logout/users/tokens to the CLI root."""

    for command in (login, logout, users, tokens):
        group.add_command(command)


__all__ = ("register_team_commands",)
