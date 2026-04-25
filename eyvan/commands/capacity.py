# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Read-only capacity inspection for the Eyvan CLI.

Every route these commands call already existed, along with its service method and
its client method -- what was missing was any way to reach them without writing
the HTTP call by hand. ``eyvan capacity`` in particular was reachable only as a
side effect of ``eyvan up --region``, which meant "what can I run on?" required
provisioning something first.

Deliberately no ``pool create``. Pools are created declaratively by ``eyvan up``
from a fleet config, so an imperative creator would be a second source of truth
for the same objects and the two would drift the first time someone used both.
The help text says so, which turns an inconsistency into a stated policy.
"""

from __future__ import annotations

import json

import click

from eyvan.commands.common import JsonRenderer
from eyvan.commands.state import CliSession


def _emit(payload: object, *, as_json: bool, renderer: JsonRenderer) -> None:
    if as_json:
        click.echo(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return
    renderer.emit(payload)


@click.group("capacity")
def capacity() -> None:
    """Inspect available capacity, pools, cells, and leases."""


@capacity.command("orphans")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def capacity_orphans(session: CliSession, as_json: bool) -> None:
    """List cells an agent still holds that this control plane does not claim.

    A fleet-state document outlives the control plane that wrote it, so a
    replaced database leaves its cells behind in the agent -- and any left
    desired-up are re-ensured forever, provisioning capacity nobody asked for.
    Before this, the only way to see them was reading the agent's state document
    out of object storage by hand.

    Reporting only. One agent can legitimately serve more than one control
    plane, so an unrecognised record is not proven unowned; reclaiming it is an
    operator decision, and this is the information that decision needs.
    """

    payload = session.client.request("GET", "/api/v1/capacity/orphans")
    if as_json:
        click.echo(json.dumps(payload, sort_keys=True, indent=2, default=str))
        return
    if not payload:
        click.echo("no orphaned cell records")
        return
    for entry in payload if isinstance(payload, list) else []:
        if not isinstance(entry, dict):
            continue
        click.echo(
            f"{entry.get('name')}  state={entry.get('state')}  desired={entry.get('desired_state')}  "
            f"qr={entry.get('qr_id')}  agent={entry.get('endpoint')}"
        )


@capacity.command("list")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--accelerator-type", help="Only pools offering this accelerator.")
@click.option("--capacity-class", help="Only pools of this capacity class.")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def list_capacity(
    session: CliSession,
    org_id: str | None,
    project_id: str | None,
    accelerator_type: str | None,
    capacity_class: str | None,
    as_json: bool,
) -> None:
    """Show the capacity pools this project may submit to."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    payload = session.client.request(
        "GET",
        "/api/v1/capacity",
        params={"org_id": scope.org_id, "project_id": scope.project_id},
        headers=session.workload_headers(scope),
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("pools"), list):
        raise click.ClickException("control plane returned an unexpected capacity response")
    rows = [
        pool
        for pool in payload["pools"]
        if isinstance(pool, dict)
        and (accelerator_type is None or pool.get("accelerator_type") == accelerator_type)
        and (capacity_class is None or pool.get("capacity_class") == capacity_class)
    ]
    if as_json:
        click.echo(json.dumps({"pools": rows}, sort_keys=True, indent=2, default=str))
        return
    if not rows:
        # Distinguish "your filters matched nothing" from "the fleet is empty":
        # the two have completely different fixes and the same empty table.
        if payload["pools"]:
            click.echo("No capacity pools match those filters.")
        else:
            click.echo("No enabled capacity pools are available to this project.")
        return
    click.echo("Available capacity pools:")
    for pool in rows:
        click.echo(
            f"  {pool.get('fleet', '-'):30} {pool.get('accelerator_type', '-') or '-':12} "
            f"{pool.get('capacity_class', '-'):10} {pool.get('region', '-'):12} "
            f"{pool.get('zone') or '-':14} max={pool.get('max_cells', '?')}"
        )


@capacity.command("warmth")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def warmth(session: CliSession, as_json: bool) -> None:
    """Report each pool's warm floor and whether it is currently held.

    Read alongside ``capacity billing``: warm capacity is deliberately idle, so
    its cost must be subtracted from the fleet's idle cost before that number
    means anything.
    """

    payload = session.client.request("GET", "/api/v1/capacity/warmth")
    _emit(payload, as_json=as_json, renderer=JsonRenderer())


@capacity.command("billing")
@click.option("--pool", "pool_id", help="Restrict the report to one capacity pool.")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def billing(session: CliSession, pool_id: str | None, as_json: bool) -> None:
    """Report accrued and idle cell cost.

    The idle figure is not derivable from budgets: an idle cell holds no lease, so
    nothing settles and nothing is charged to a project. This is the only place a
    fleet's unused-but-paid-for capacity is visible.
    """

    payload = session.client.request(
        "GET",
        "/api/v1/capacity/billing",
        params={} if pool_id is None else {"pool_id": pool_id},
    )
    _emit(payload, as_json=as_json, renderer=JsonRenderer())


@capacity.group("pool")
def pool() -> None:
    """Inspect capacity pools.

    There is no ``pool create``: pools are declared in a fleet config and applied
    by ``eyvan up``, so creating one here would fork the source of truth.
    """


@pool.command("list")
@click.option("--fleet", "fleet_name", help="Only pools in this fleet.")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def list_pools(session: CliSession, fleet_name: str | None, as_json: bool) -> None:
    """List capacity pool records."""

    payload = session.client.request(
        "GET",
        "/api/v1/capacity-pools",
        params={} if fleet_name is None else {"fleet_name": fleet_name},
    )
    _emit(payload, as_json=as_json, renderer=JsonRenderer())


@pool.command("get")
@click.argument("pool_id")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def get_pool(session: CliSession, pool_id: str, as_json: bool) -> None:
    """Show one capacity pool record."""

    payload = session.client.request("GET", f"/api/v1/capacity-pools/{pool_id}")
    _emit(payload, as_json=as_json, renderer=JsonRenderer())


@capacity.group("cell")
def cell() -> None:
    """Inspect one execution cell.

    Listing cells is ``eyvan cells``, which already exists; only the single-cell
    read was missing, so only that is added here.
    """


@cell.command("get")
@click.argument("cell_id")
@click.option("--json", "as_json", is_flag=True)
@click.pass_obj
def get_cell(session: CliSession, cell_id: str, as_json: bool) -> None:
    """Show one execution cell record."""

    payload = session.client.request("GET", f"/api/v1/cells/{cell_id}")
    _emit(payload, as_json=as_json, renderer=JsonRenderer())


def register_capacity_commands(main: click.Group) -> None:
    """Attach the capacity command group to the CLI root."""

    main.add_command(capacity)


__all__ = ("capacity", "register_capacity_commands")
