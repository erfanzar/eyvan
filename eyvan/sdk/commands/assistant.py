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
@click.option("--yes", is_flag=True, help="Submit drafted workloads without asking.")
@click.option("--json", "as_json", is_flag=True, help="Emit the raw assistant turn as JSON.")
@click.pass_obj
def ask(
    session: CliSession,
    question: tuple[str, ...],
    org_id: str | None,
    project_id: str | None,
    yes: bool,
    as_json: bool,
) -> None:
    """Ask the fleet assistant (sizing, which fleet, quota, why a job waits)."""

    text = " ".join(question).strip()
    status = session.client.request("GET", "/api/v1/assistant/status")
    if not (isinstance(status, dict) and status.get("enabled")):
        raise click.ClickException(
            "this plane has no assistant configured; its operator adds an `assistant:` section "
            "(url, model, api_key_file) to the server's platform file (/etc/eyvan/platform.yaml)"
        )
    payload = _object(
        session.client.request(
            "POST",
            "/api/v1/assistant/chat",
            json={"messages": [{"role": "user", "content": text}], **_scope(session, org_id, project_id)},
        )
    )
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


def register_assistant_commands(group: click.Group) -> None:
    """Attach ask/size/requests to the CLI root."""

    for command in (ask, size, requests_group):
        group.add_command(command)


__all__ = ("register_assistant_commands",)
