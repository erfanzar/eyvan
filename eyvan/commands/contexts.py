# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Persistent context commands for the Eyvan CLI."""

from __future__ import annotations

import click
from pydantic import ValidationError

from eyvan._json import model_json
from eyvan.commands.common import JsonRenderer
from eyvan.commands.state import CliSession
from eyvan.contexts import ContextConfigError

_renderer = JsonRenderer()


@click.command("use")
@click.argument("fleet")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def use_context(session: CliSession, fleet: str, org_id: str | None, project_id: str | None) -> None:
    """Save FLEET and its account scope as the active context."""

    try:
        context = session.select_context(fleet, org_id=org_id, project_id=project_id)
    except (ContextConfigError, ValidationError, ValueError) as exc:
        raise click.ClickException(f"cannot save Eyvan context: {exc}") from exc
    _renderer.emit(
        {
            "active_context": session.context_config.active_context,
            **model_json(context, exclude_none=True),
        }
    )


@click.command("context")
@click.pass_obj
def show_context(session: CliSession) -> None:
    """Show the active persistent context."""

    active = session.active_context
    if active is None:
        raise click.ClickException("no active context; run `eyvan use FLEET --org ORG --project PROJECT`")
    _renderer.emit(
        {
            "active_context": session.context_config.active_context,
            **model_json(active, exclude_none=True),
        }
    )


def register_context_commands(group: click.Group) -> None:
    """Attach context commands to the root command group."""

    group.add_command(use_context)
    group.add_command(show_context)


__all__ = ("register_context_commands", "show_context", "use_context")
