# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""``eyvan mcp``: connect an MCP client such as Xerxes to this plane's tool server.

``eyvan mcp config`` prints the Xerxes ``mcp.json`` entry for the active
plane with every secret redacted. ``eyvan mcp install --xerxes`` mints a
fresh, named, revocable personal API token (team mode) and merges the entry
into Xerxes's ``mcp.json``, written atomically and owner-only. No token is
ever printed.
"""

from __future__ import annotations

import ipaddress
import json
import os
import socket
import stat
import typing as tp
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import click

from eyvan._internal.workspace import create_private_directory
from eyvan.sdk.commands.state import CliSession
from eyvan.server.local.auth import LOCAL_DEVELOPMENT_CREDENTIAL_HEADER

MCP_ROUTE = "/api/v1/mcp"
DEFAULT_XERXES_MCP_PATH = Path("~/.xerxes/mcp.json")
REDACTED = "<redacted>"
_MAX_DOCUMENT_BYTES = 1_048_576


def mcp_url(base_url: str) -> str:
    """The plane's MCP endpoint."""

    return base_url.rstrip("/") + MCP_ROUTE


def needs_private_network(url: str) -> bool:
    """Whether Xerxes would refuse ``url`` without ``allowPrivateNetwork``.

    Xerxes classifies only ``localhost`` and address literals (it never resolves
    names), so a hostname is reported as public here too.
    """

    host = (urlsplit(url).hostname or "").strip("[]").rstrip(".").lower()
    if host in {"localhost", "localhost.localdomain"}:
        return True
    try:
        return not ipaddress.ip_address(host).is_global
    except ValueError:
        return False


def _team_mode(session: CliSession) -> bool:
    try:
        payload = session.client.request("GET", "/api/v1/team/status")
    except click.ClickException as exc:
        raise click.ClickException(f"could not ask {session.base_url} whether it runs team mode: {exc}") from exc
    return isinstance(payload, dict) and payload.get("team_mode") is True


def _scope_headers(session: CliSession) -> dict[str, str]:
    headers: dict[str, str] = {}
    if session.org_id:
        headers["X-Eyvan-Org"] = session.org_id
    if session.project_id:
        headers["X-Eyvan-Project"] = session.project_id
    return headers


def build_entry(session: CliSession, *, team_mode: bool, token: str | None = None) -> dict[str, tp.Any]:
    """The Xerxes server entry.

    ``token`` is the bearer to embed on a team plane; ``None`` renders it
    redacted. On a development-auth plane the entry carries this CLI's identity
    headers, and this machine's local development credential only when
    ``token`` is not ``None`` (``install``); ``config`` passes ``None``.
    """

    url = mcp_url(session.base_url)
    headers: dict[str, str]
    identity = session.identity
    if team_mode or identity.token is not None:
        headers = {"Authorization": f"Bearer {token if token is not None else REDACTED}"}
        headers.update(_scope_headers(session))
    else:
        # A development-auth plane authenticates the same headers the CLI sends.
        user = identity.dev_user
        if not user:
            raise click.ClickException(
                f"{session.base_url} is not a team plane and this CLI has no development identity for it; "
                "sign in with `eyvan login` or pass --user"
            )
        headers = {"X-Eyvan-User": user, "X-Eyvan-Roles": ",".join(identity.dev_roles)}
        headers.update(_scope_headers(session))
        credential = identity.local_development_credential
        if credential:
            headers[LOCAL_DEVELOPMENT_CREDENTIAL_HEADER] = credential if token is not None else REDACTED
    return {
        "transport": "streamable_http",
        "url": url,
        "headers": headers,
        "allowPrivateNetwork": needs_private_network(url),
    }


def redacted(entry: dict[str, tp.Any]) -> dict[str, tp.Any]:
    """A copy of ``entry`` with the bearer and local credential values replaced."""

    headers = dict(entry.get("headers") or {})
    for name in list(headers):
        if name.lower() == "authorization":
            headers[name] = f"Bearer {REDACTED}"
        elif name.lower() == LOCAL_DEVELOPMENT_CREDENTIAL_HEADER.lower():
            headers[name] = REDACTED
    return {**entry, "headers": headers}


# ------------------------------------------------------------------ mcp.json


def _load_document(path: Path) -> tp.Any:
    """Read Xerxes's mcp.json, refusing anything it would not be safe to rewrite."""

    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise click.ClickException(f"could not open {path} (a symlink or unreadable file?): {exc}") from exc
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise click.ClickException(f"{path} is not a regular file")
        raw = stream.read(_MAX_DOCUMENT_BYTES + 1)
    if len(raw) > _MAX_DOCUMENT_BYTES:
        raise click.ClickException(f"{path} is larger than 1 MiB; refusing to rewrite it")
    if not raw.strip():
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        # Never quote the parser error: the file holds credentials.
        raise click.ClickException(f"{path} is not valid JSON; fix or move it first (nothing was changed)") from None


def _servers(document: tp.Any) -> list[tp.Any] | None:
    if isinstance(document, list):
        return document
    if isinstance(document, dict) and isinstance(document.get("servers"), list):
        return tp.cast(list[tp.Any], document["servers"])
    return None


def has_entry(document: tp.Any, name: str) -> bool:
    """Whether ``document`` (any shape Xerxes accepts) already holds a server named ``name``."""

    servers = _servers(document)
    if servers is not None:
        return any(isinstance(item, dict) and str(item.get("name", "")).strip() == name for item in servers)
    return isinstance(document, dict) and name in document


def merged(document: tp.Any, name: str, entry: dict[str, tp.Any]) -> tp.Any:
    """``document`` with ``name`` set to ``entry``, keeping its shape and every other server."""

    if document is None:
        return {name: entry}
    servers = _servers(document)
    if servers is not None:
        kept = [item for item in servers if not (isinstance(item, dict) and str(item.get("name", "")).strip() == name)]
        kept.append({"name": name, **entry})
        return kept if isinstance(document, list) else {**document, "servers": kept}
    if isinstance(document, dict):
        return {**document, name: entry}
    raise click.ClickException("the existing mcp.json is neither a server map nor a list; nothing was changed")


def write_private(path: Path, document: tp.Any) -> None:
    """Atomically replace ``path`` with ``document``, owner-only (0600)."""

    create_private_directory(path.parent)
    temporary = path.with_name(f".{path.name}-{uuid.uuid4().hex}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(document, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


# ------------------------------------------------------------------ commands


@click.group("mcp")
def mcp() -> None:
    """Use this plane as an MCP tool server (for example from Xerxes)."""


_NAME = click.option("--name", default="eyvan", show_default=True, help="Server name in mcp.json.")


@mcp.command("config")
@_NAME
@click.option("--json", "as_json", is_flag=True, help="Print only the JSON entry.")
@click.pass_obj
def mcp_config(session: CliSession, name: str, as_json: bool) -> None:
    """Print the Xerxes mcp.json entry for the active plane (secrets redacted)."""

    team_mode = _team_mode(session)
    entry = redacted(build_entry(session, team_mode=team_mode, token=None))
    click.echo(json.dumps({name: entry}, indent=2))
    if as_json:
        return
    if team_mode:
        click.echo(
            "the bearer is redacted; `eyvan mcp install --xerxes` mints a personal token for it and writes this entry",
            err=True,
        )
    else:
        click.echo(
            "development-auth plane: no team token is needed; the entry carries this CLI's identity headers"
            + (" and this machine's local development credential (redacted here)" if "<redacted>" in str(entry) else ""),
            err=True,
        )


@mcp.command("install")
@click.option("--xerxes", "xerxes", is_flag=True, help="Install into Xerxes's mcp.json (the only target).")
@click.option(
    "--path",
    "path_text",
    type=click.Path(dir_okay=False),
    help=f"mcp.json to update (default {DEFAULT_XERXES_MCP_PATH}).",
)
@_NAME
@click.option("--yes", is_flag=True, help="Replace an existing entry of the same name.")
@click.option("--json", "as_json", is_flag=True, help="Report the result as JSON (secrets redacted).")
@click.pass_obj
def mcp_install(session: CliSession, xerxes: bool, path_text: str | None, name: str, yes: bool, as_json: bool) -> None:
    """Write this plane into Xerxes's mcp.json with a fresh personal token (team mode)."""

    if not xerxes:
        raise click.UsageError("pass --xerxes (Xerxes is the only supported MCP client)")
    if not name.strip() or len(name) > 128:
        raise click.BadParameter("must be 1-128 characters", param_hint="--name")
    path = Path(path_text).expanduser() if path_text else DEFAULT_XERXES_MCP_PATH.expanduser()
    document = _load_document(path)
    if document is not None and _servers(document) is None and not isinstance(document, dict):
        raise click.ClickException(f"{path} is neither a server map nor a list; nothing was changed")
    if has_entry(document, name) and not yes:
        raise click.ClickException(f"{path} already has an MCP server named {name!r}; pass --yes to replace it")
    team_mode = _team_mode(session)
    notes: list[str] = []
    minted: dict[str, tp.Any] | None = None
    secret: str | None
    if team_mode:
        if session.identity.token is None:
            raise click.ClickException(f"{session.base_url} runs team mode; sign in first with `eyvan login`")
        token_name = f"xerxes-mcp-{socket.gethostname()}"[:64]
        payload = session.client.request("POST", "/api/v1/team/tokens", json={"name": token_name})
        raw_token = payload.get("token") if isinstance(payload, dict) else None
        if not isinstance(payload, dict) or not isinstance(raw_token, str):
            raise click.ClickException("the plane returned an unexpected answer when minting the token")
        secret = raw_token
        raw_record = payload.get("record")
        record: dict[str, tp.Any] = raw_record if isinstance(raw_record, dict) else {}
        minted = {"id": record.get("id"), "name": record.get("name", token_name)}
        notes.append(
            f"minted personal token {minted['id']} ({minted['name']}); revoke it with "
            f"`eyvan tokens revoke {minted['id']}`"
        )
    else:
        if session.identity.token is not None:
            raise click.ClickException(
                f"{session.base_url} does not run team mode and this CLI authenticates with a bearer token; "
                "`eyvan mcp install` writes only revocable team tokens, so it will not copy that one. "
                "Use `eyvan mcp config` and add the credential yourself"
            )
        secret = session.identity.local_development_credential or ""
        notes.append("development-auth plane: no team token is needed and none was minted")
    entry = build_entry(session, team_mode=team_mode, token=secret)
    try:
        write_private(path, merged(document, name, entry))
    except OSError as exc:
        if minted is not None and minted.get("id"):
            try:
                session.client.request("DELETE", f"/api/v1/team/tokens/{minted['id']}")
                notes.append(f"revoked {minted['id']} again because the file could not be written")
            except click.ClickException:
                notes.append(f"could not revoke {minted['id']}; revoke it with `eyvan tokens revoke`")
        raise click.ClickException(f"could not write {path}: {exc}; " + "; ".join(notes[1:])) from exc
    finally:
        secret = None
    if as_json:
        click.echo(
            json.dumps({"path": str(path), "name": name, "entry": redacted(entry), "token": minted, "notes": notes})
        )
        return
    lines = [f"wrote MCP server {name!r} -> {entry['url']} into {path} (owner-only)"]
    lines.extend(f"  {note}" for note in notes)
    if entry["allowPrivateNetwork"]:
        lines.append("  allowPrivateNetwork is set because the plane is on a loopback/private address")
    lines.append("  restart Xerxes (or reload its MCP servers) to connect")
    click.echo("\n".join(lines))


def register_mcp_commands(group: click.Group) -> None:
    """Attach ``eyvan mcp`` to the CLI root."""

    group.add_command(mcp)


__all__ = ("register_mcp_commands",)
