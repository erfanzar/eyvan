# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Eyvan command composition root."""

from __future__ import annotations

import sys

try:  # readline is unavailable on Windows and in some minimal containers.
    import readline  # Enables line editing and history for Rich prompts.
except ImportError:
    readline = None  # type: ignore[assignment]  # Rich prompts degrade gracefully without it.

import click
from click.core import ParameterSource

from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
from eyvan.sdk.client.identity import resolve_control_plane_identity
from eyvan.sdk.commands.autoscale import register_autoscale_commands
from eyvan.sdk.commands.capacity import register_capacity_commands
from eyvan.sdk.commands.contexts import register_context_commands
from eyvan.sdk.commands.dashboard import register_dashboard_commands
from eyvan.sdk.commands.doctor import register_doctor_commands
from eyvan.sdk.commands.fleets import register_fleet_commands
from eyvan.sdk.commands.governance import register_governance_commands
from eyvan.sdk.commands.http import EyvanApiError, EyvanClient
from eyvan.sdk.commands.operations import register_operation_commands
from eyvan.sdk.commands.runtime import register_runtime_commands
from eyvan.sdk.commands.state import CliSession, ContextOverrides
from eyvan.sdk.commands.workloads import register_workload_commands
from eyvan.sdk.contexts import DEFAULT_BASE_URL, ContextConfigError, FileContextStore

__all__ = ("EyvanApiError", "EyvanClient", "main")


@click.group()
@click.option("--url", envvar="EYVAN_URL", show_default=DEFAULT_BASE_URL)
@click.option("--token", envvar="EYVAN_TOKEN")
@click.option("--user", envvar="EYVAN_DEV_USER")
@click.option("--org", envvar="EYVAN_ORG")
@click.option("--project", envvar="EYVAN_PROJECT")
@click.option("--role", envvar="EYVAN_ROLE", default="operator", show_default=True)
@click.pass_context
def main(
    ctx: click.Context,
    url: str | None,
    token: str | None,
    user: str | None,
    org: str | None,
    project: str | None,
    role: str,
) -> None:
    """Operate workloads and fleet governance through Eyvan."""

    context_store = FileContextStore()
    try:
        context_config = context_store.load()
    except ContextConfigError as exc:
        raise click.ClickException(str(exc)) from exc
    active_context = context_config.active
    overrides = ContextOverrides(base_url=url, org_id=org, project_id=project)
    endpoint = ControlPlaneEndpoint.parse(url or (active_context.base_url if active_context else DEFAULT_BASE_URL))
    org_id = org or (active_context.org_id if active_context else None)
    project_id = project or (active_context.project_id if active_context else None)

    if (
        token is None
        and user is None
        and endpoint.is_loopback
        and ctx.get_parameter_source("role") is ParameterSource.DEFAULT
    ):
        role = "fleet_admin"
        if sys.stderr.isatty():
            click.echo(
                "Using role 'fleet_admin' for the loopback development endpoint; pass --role to override it.",
                err=True,
            )
    identity = resolve_control_plane_identity(
        endpoint,
        token=token,
        dev_user=user,
        dev_org=org_id,
        dev_project=project_id,
        dev_roles=role,
    )
    ctx.obj = CliSession(
        client=EyvanClient(endpoint.url, headers=identity.headers()),
        identity=identity,
        context_store=context_store,
        context_config=context_config,
        base_url=endpoint.url,
        org_id=org_id,
        project_id=project_id,
        overrides=overrides,
    )


for register_commands in (
    register_context_commands,
    register_doctor_commands,
    register_fleet_commands,
    register_workload_commands,
    register_operation_commands,
    register_autoscale_commands,
    register_capacity_commands,
    register_governance_commands,
    register_runtime_commands,
    register_dashboard_commands,
):
    register_commands(main)


if __name__ == "__main__":
    main()
