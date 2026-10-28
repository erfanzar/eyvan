# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""``eyvan ask``, ``eyvan size`` and ``eyvan requests``: the fleet assistant from the CLI.

The plane runs the assistant with the caller's own permissions and a provider
key that never leaves the server. A drafted workload is shown, and submitted
only when the user confirms it (``--yes`` or the prompt), as an ordinary
submission made by this CLI with the user's own identity.
"""

from __future__ import annotations

import json
import sys
import typing as tp
import uuid
from pathlib import Path

import click

from eyvan._internal.json_types import is_json_object
from eyvan.sdk.commands.common import JsonRenderer
from eyvan.sdk.commands.state import CliSession


def _object(payload: object) -> dict[str, tp.Any]:
    """Narrow one decoded API answer to a JSON object, or fail with a readable error."""

    if not isinstance(payload, dict):
        raise click.ClickException("the plane returned an unexpected answer")
    return payload


def _objects(value: object) -> list[dict[str, tp.Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _scope(session: CliSession, org_id: str | None, project_id: str | None) -> dict[str, str]:
    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    return {"org_id": scope.org_id, "project_id": scope.project_id}


def _submit_draft(session: CliSession, draft: dict[str, tp.Any]) -> tp.Any:
    """Submit one confirmed draft as this CLI's own user."""

    return session.client.request(
        "POST",
        "/api/v1/workloads",
        json=draft,
        headers={"Idempotency-Key": f"assistant-draft-{uuid.uuid4().hex}"},
    )


@click.command("ask")
@click.argument("question", nargs=-1, required=True)
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--provider", "provider", help="Provider name or id (see `eyvan assistant providers list`).")
@click.option("--model", "model", help="One of the chosen provider's models (default: its default model).")
@click.option("--yes", is_flag=True, help="Submit drafted workloads without asking.")
@click.option("--json", "as_json", is_flag=True, help="Emit the raw assistant turn as JSON.")
@click.pass_obj
def ask(
    session: CliSession,
    question: tuple[str, ...],
    org_id: str | None,
    project_id: str | None,
    provider: str | None,
    model: str | None,
    yes: bool,
    as_json: bool,
) -> None:
    """Ask the fleet assistant (sizing, which fleet, quota, why a job waits)."""

    text = " ".join(question).strip()
    status = session.client.request("GET", "/api/v1/assistant/status")
    if not (isinstance(status, dict) and status.get("enabled")):
        raise click.ClickException(
            "this plane has no assistant provider configured; a fleet admin adds one in the dashboard "
            "(Assistant page) or with `eyvan assistant providers add`"
        )
    body: dict[str, tp.Any] = {"messages": [{"role": "user", "content": text}], **_scope(session, org_id, project_id)}
    if provider is not None:
        body["provider_id"] = _provider_id(session, provider)
    if model is not None:
        body["model"] = model
    payload = _object(session.client.request("POST", "/api/v1/assistant/chat", json=body))
    if as_json:
        JsonRenderer().emit(payload)
        return
    click.echo(str(payload.get("reply", "")).strip())
    for request in _objects(payload.get("requests")):
        click.echo(f"\nfiled request {request['id']} ({request['kind']}): {request['summary']}")
    for draft in _objects(payload.get("drafts")):
        spec = draft.get("spec", {})
        click.echo(f"\ndraft workload in {draft['org_id']}/{draft['project_id']}:")
        click.echo(json.dumps(spec, indent=2, sort_keys=True))
        confirmed = yes or (sys.stdin.isatty() and click.confirm("Submit this workload?", default=False))
        if not confirmed:
            click.echo("not submitted")
            continue
        submitted = _submit_draft(session, draft)
        workload_id = submitted.get("id") if is_json_object(submitted) else None
        click.echo(f"submitted {workload_id}")


@click.command("size")
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False), help="HF config.json.")
@click.option("--parameters", type=float, help="Parameter count, e.g. 7e9, when no config is at hand.")
@click.option("--trainer", type=click.Choice(["pretrain", "sft", "dpo", "orpo", "grpo", "inference"]))
@click.option("--dtype", type=click.Choice(["bfloat16", "float16", "float32"]))
@click.option("--optimizer", type=click.Choice(["adamw", "adafactor", "lion", "sgd", "none"]))
@click.option("--seq", "sequence_length", type=click.IntRange(min=1))
@click.option("--batch", "global_batch_size", type=click.IntRange(min=1))
@click.option("--lora-rank", type=click.IntRange(min=1))
@click.option("--generation-tokens", type=click.IntRange(min=0))
@click.option("--num-generations", type=click.IntRange(min=1))
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def size(
    session: CliSession, config_path: str | None, parameters: float | None, as_json: bool, **options: tp.Any
) -> None:
    """Estimate TPU memory for a job and the smallest slice that fits (EasyDeL-style)."""

    body: dict[str, tp.Any] = {key: value for key, value in options.items() if value is not None}
    if config_path is not None:
        body["model_config"] = json.loads(Path(config_path).read_text(encoding="utf-8"))
    if parameters is not None:
        body["parameters"] = parameters
    if "model_config" not in body and "parameters" not in body:
        raise click.UsageError("pass --config CONFIG.json or --parameters N")
    payload = _object(session.client.request("POST", "/api/v1/assistant/size", json=body))
    if as_json:
        JsonRenderer().emit(payload)
        return
    estimate = _object(payload.get("estimate"))
    click.echo(f"{payload['parameters']:,} parameters; total {estimate['total_gib']} GiB")
    for fit in _objects(payload.get("fits")):
        click.echo(f"  {fit['accelerator_type']:<14} {fit['per_chip_gib']} GiB/chip of {fit['hbm_per_chip_gib']}")
    click.echo(f"recommended: {payload.get('recommended') or 'nothing in the candidate list fits'}")


@click.group("requests")
def requests_group() -> None:
    """Capacity and approval requests to the fleet admins (team mode)."""


@requests_group.command("list")
@click.option("--state", type=click.Choice(["open", "approved", "declined", "withdrawn"]))
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def requests_list(session: CliSession, state: str | None, as_json: bool) -> None:
    """Your requests (fleet admins see everyone's)."""

    payload = _object(session.client.request("GET", "/api/v1/team/requests", params={"state": state} if state else None))
    rows = _objects(payload.get("requests"))
    if as_json:
        JsonRenderer().emit(payload)
        return
    lines = [f"{row['id']}  {row['state']:<9} {row['kind']:<9} {row['subject']:<16} {row['summary']}" for row in rows]
    click.echo("\n".join(lines) or "no requests")


@requests_group.command("file")
@click.argument("kind", type=click.Choice(["capacity", "approval"]))
@click.argument("summary")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def requests_file(session: CliSession, kind: str, summary: str, org_id: str | None, project_id: str | None) -> None:
    """File a request to the fleet admins."""

    payload = _object(
        session.client.request(
            "POST",
            "/api/v1/team/requests",
            json={"kind": kind, "summary": summary, **_scope(session, org_id, project_id)},
        )
    )
    click.echo(f"filed {payload['id']}")


_RESOLVER_COMMANDS = {"approved": "approve", "declined": "decline", "withdrawn": "withdraw"}


def _resolver(resolution: str) -> click.Command:
    @click.command(_RESOLVER_COMMANDS[resolution])
    @click.argument("request_id")
    @click.option("--note", default="")
    @click.pass_obj
    def resolve(session: CliSession, request_id: str, note: str) -> None:
        session.client.request(
            "POST",
            f"/api/v1/team/requests/{request_id}/resolve",
            json={"resolution": resolution, "note": note},
        )
        click.echo(f"{request_id} {resolution}")

    resolve.help = {
        "approved": "Approve an open request (fleet admins).",
        "declined": "Decline an open request (fleet admins).",
        "withdrawn": "Withdraw your own open request.",
    }[resolution]
    return resolve


for _resolution in ("approved", "declined", "withdrawn"):
    requests_group.add_command(_resolver(_resolution))


def _providers(session: CliSession) -> dict[str, tp.Any]:
    return _object(session.client.request("GET", "/api/v1/assistant/providers"))


def _provider_id(session: CliSession, name_or_id: str) -> str:
    """Resolve a provider name (case-insensitive) or id to its id."""

    rows = _objects(_providers(session).get("providers"))
    for row in rows:
        if row.get("id") == name_or_id or str(row.get("name", "")).lower() == name_or_id.lower():
            return str(row["id"])
    names = ", ".join(str(row.get("name")) for row in rows) or "none"
    raise click.ClickException(f"no assistant provider named {name_or_id!r}; available: {names}")


def _models(value: str) -> list[str]:
    return [name.strip() for name in value.split(",") if name.strip()]


def _read_key(key_stdin: bool) -> str | None:
    """Read a provider key without putting it on the command line."""

    if key_stdin:
        return sys.stdin.read().strip() or None
    if sys.stdin.isatty():
        entered = click.prompt("API key (empty for none)", hide_input=True, default="", show_default=False)
        return str(entered).strip() or None
    return None


@click.group("assistant")
def assistant_group() -> None:
    """The fleet assistant's LLM providers."""


@assistant_group.group("providers")
def providers_group() -> None:
    """LLM providers fleet admins configure; everyone picks one in chat."""


@providers_group.command("list")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def providers_list(session: CliSession, as_json: bool) -> None:
    """List providers (fleet admins also see URLs, state and whether a key is set)."""

    payload = _providers(session)
    if as_json:
        JsonRenderer().emit(payload)
        return
    rows = _objects(payload.get("providers"))
    lines = []
    for row in rows:
        extra = ""
        if "base_url" in row:
            key = "key set" if row.get("has_api_key") else "no key"
            extra = f"  {row['base_url']}  {'enabled' if row.get('enabled') else 'disabled'}  {key}"
        models = ", ".join(str(item) for item in row.get("models") or [])
        lines.append(f"{row['id']}  {row['name']:<20} models: {models} (default {row.get('default_model')}){extra}")
    click.echo("\n".join(lines) or "no assistant providers configured")


@providers_group.command("add")
@click.argument("name")
@click.option("--url", "base_url", required=True, help="OpenAI-compatible base URL, e.g. https://api.example.com/v1")
@click.option("--models", required=True, help="Comma-separated model names.")
@click.option("--default-model")
@click.option("--max-steps", type=click.IntRange(1, 20), default=6, show_default=True)
@click.option("--key-stdin", is_flag=True, help="Read the API key from stdin instead of a hidden prompt.")
@click.option("--disabled", is_flag=True, help="Add it switched off.")
@click.pass_obj
def providers_add(
    session: CliSession,
    name: str,
    base_url: str,
    models: str,
    default_model: str | None,
    max_steps: int,
    key_stdin: bool,
    disabled: bool,
) -> None:
    """Add a provider (fleet admins). The key is never shown again."""

    body: dict[str, tp.Any] = {
        "name": name,
        "base_url": base_url,
        "models": _models(models),
        "max_steps": max_steps,
        "enabled": not disabled,
    }
    if default_model:
        body["default_model"] = default_model
    key = _read_key(key_stdin)
    if key:
        body["api_key"] = key
    created = _object(session.client.request("POST", "/api/v1/assistant/providers", json=body))
    click.echo(f"added {created['id']} ({created['name']}); key {'set' if created.get('has_api_key') else 'not set'}")


@providers_group.command("update")
@click.argument("provider")
@click.option("--name")
@click.option("--url", "base_url")
@click.option("--models", help="Comma-separated model names; replaces the list.")
@click.option("--default-model")
@click.option("--max-steps", type=click.IntRange(1, 20))
@click.option("--enable/--disable", "enabled", default=None)
@click.option("--set-key", is_flag=True, help="Enter a new API key (hidden prompt, or stdin with --key-stdin).")
@click.option("--key-stdin", is_flag=True)
@click.option("--clear-key", is_flag=True, help="Remove the stored key.")
@click.pass_obj
def providers_update(
    session: CliSession,
    provider: str,
    name: str | None,
    base_url: str | None,
    models: str | None,
    default_model: str | None,
    max_steps: int | None,
    enabled: bool | None,
    set_key: bool,
    key_stdin: bool,
    clear_key: bool,
) -> None:
    """Change a provider (fleet admins); the stored key is kept unless you set or clear it."""

    body: dict[str, tp.Any] = {}
    for field_name, value in (
        ("name", name),
        ("base_url", base_url),
        ("default_model", default_model),
        ("max_steps", max_steps),
        ("enabled", enabled),
    ):
        if value is not None:
            body[field_name] = value
    if models is not None:
        body["models"] = _models(models)
    if clear_key:
        body["clear_api_key"] = True
    elif set_key or key_stdin:
        key = _read_key(key_stdin)
        if not key:
            raise click.UsageError("no key was entered")
        body["api_key"] = key
    provider_id = _provider_id(session, provider)
    updated = _object(session.client.request("PUT", f"/api/v1/assistant/providers/{provider_id}", json=body))
    click.echo(f"updated {updated['id']} ({updated['name']})")


@providers_group.command("remove")
@click.argument("provider")
@click.pass_obj
def providers_remove(session: CliSession, provider: str) -> None:
    """Remove a provider (fleet admins)."""

    provider_id = _provider_id(session, provider)
    session.client.request("DELETE", f"/api/v1/assistant/providers/{provider_id}")
    click.echo(f"removed {provider_id}")


def register_assistant_commands(group: click.Group) -> None:
    """Attach ask/size/requests/assistant to the CLI root."""

    for command in (ask, size, requests_group, assistant_group):
        group.add_command(command)


__all__ = ("register_assistant_commands",)
