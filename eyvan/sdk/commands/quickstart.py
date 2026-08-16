# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""``eyvan quickstart``: from nothing to a first job, one step at a time.

Getting a first job to run meant rendering a fleet template, knowing which of
``eyvan up``'s several modes applied, standing up a head, and then discovering
how pools are named. Every one of those is documented somewhere; none of them
is discoverable from the command line, which is where somebody trying Eyvan for
the first time actually is.

This is a *guide*, not an installer. It works out where you are on that path
and either does the safe step or prints the exact command for the one that
spends money. Provisioning a control plane and allocating accelerators are not
things a tool should do because someone typed "quickstart".
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass

import click

from eyvan._internal.json_types import is_json_object
from eyvan.sdk.commands.state import CliSession

#: What the first job runs. Deliberately trivial and deliberately *not* a
#: framework import: the question this answers is "can this fleet run my code
#: at all", and a hello-world that fails because JAX is missing answers a
#: different one badly.
HELLO_COMMAND = "python -c \"import socket; print(f'hello from {socket.gethostname()}')\""


@dataclass(frozen=True, slots=True)
class Step:
    """One step of the path, and how a caller is told about it.

    Attributes:
        done: Whether this step is already satisfied.
        headline: What the step is, in the user's terms.
        detail: What was observed.
        command: The exact command to run next, when the step is not done and
            this tool must not do it itself. Empty when the step is done or
            when quickstart performs it.
    """

    done: bool
    headline: str
    detail: str
    command: str = ""


def _reachable(session: CliSession) -> Step:
    """Decide whether a control plane is answering at all."""

    try:
        session.client.request("GET", "/api/v1/health")
    except Exception as exc:
        return Step(
            done=False,
            headline="a control plane",
            detail=f"nothing is answering at {session.base_url} ({type(exc).__name__})",
            # Two honest options rather than one: a laptop does not need a VM,
            # and telling someone to provision GCP to try a CPU job is how a
            # first impression gets thrown away.
            command="eyvan head up fleet.yaml        # a control plane on GCP\n"
            "  eyvan-server --log-level info  # or one on this machine",
        )
    return Step(done=True, headline="a control plane", detail=f"answering at {session.base_url}")


def _fleets(session: CliSession, *, org_id: str | None, project_id: str | None) -> tuple[Step, tuple[str, ...]]:
    """Decide whether this scope can submit to any fleet."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    payload = session.client.request(
        "GET",
        "/api/v1/fleets",
        params={"org_id": scope.org_id, "project_id": scope.project_id},
        headers=session.workload_headers(scope),
    )
    names = tuple(name for name in payload if isinstance(name, str)) if isinstance(payload, list) else ()
    if not names:
        return (
            Step(
                done=False,
                headline="an applied fleet",
                detail=f"{scope.org_id}/{scope.project_id} can submit to no fleet yet",
                command="eyvan up --region us-east5   # discover Spot TPU quota and apply a fleet\n"
                "  eyvan up fleet.yaml          # or apply one you wrote",
            ),
            (),
        )
    return Step(done=True, headline="an applied fleet", detail=", ".join(names)), names


def _shapes(session: CliSession) -> tuple[Step, tuple[dict, ...]]:
    """Decide what hardware the applied fleets can actually provide.

    The pool documents come back with the step, not just their shape names: the
    cost estimate below has to ask for what a pool actually provides, and a
    request built from a shape name alone matches nothing.
    """

    payload = session.client.request("GET", "/api/v1/capacity-pools")
    pools = [pool for pool in payload if is_json_object(pool)] if isinstance(payload, list) else []
    usable = tuple(pool for pool in pools if pool.get("enabled") is not False)
    if not pools:
        return (
            Step(
                done=False,
                headline="capacity to run on",
                detail="the fleet declares no pools",
                command="eyvan up --region us-east5",
            ),
            (),
        )
    if not usable:
        return (
            Step(
                done=False,
                headline="capacity to run on",
                detail=f"all {len(pools)} pool(s) are disabled",
                command="eyvan capacity pool enable POOL_ID",
            ),
            (),
        )
    shapes = sorted({str(pool["accelerator_type"]) for pool in usable if isinstance(pool.get("accelerator_type"), str)})
    # A CPU-only fleet is a working fleet and the hello-world runs on it; it
    # just has no accelerator to name.
    detail = ", ".join(shapes) if shapes else f"{len(usable)} pool(s), CPU only"
    return Step(done=True, headline="capacity to run on", detail=detail), usable


def _estimate(session: CliSession, *, scope, pool: dict) -> str:
    """Return what the first job would cost on ``pool``, or why that is unknown.

    The request describes what the pool provides rather than a nominal one CPU:
    an estimate whose resource request no pool matches reports no candidates,
    which reads as "this fleet cannot run it" when the truth is that nobody
    asked for the right thing.
    """

    from eyvan._internal.json_types import model_json
    from eyvan.records import CapacityClass, CommandExecution, ResourceGang, ResourceVector, WorkloadDefinition
    from eyvan.server.api.schemas import WorkloadEstimateRequest

    allocatable = pool.get("allocatable")
    try:
        request = WorkloadEstimateRequest(
            org_id=scope.org_id,
            project_id=scope.project_id,
            spec=WorkloadDefinition(
                name="quickstart",
                execution=CommandExecution(kind="command", command=HELLO_COMMAND),
                gang=ResourceGang(
                    replicas=1,
                    per_replica=(
                        ResourceVector.model_validate(allocatable)
                        if is_json_object(allocatable)
                        else ResourceVector(cpu=1, memory_gib=1)
                    ),
                ),
                accelerator_type=pool.get("accelerator_type") if isinstance(pool.get("accelerator_type"), str) else None,
                provider=pool.get("provider") if isinstance(pool.get("provider"), str) else None,
                region=pool.get("region") if isinstance(pool.get("region"), str) else None,
                # The pool's own class, not the record default. A spot-first
                # fleet has no on-demand pools at all, so the default asks about
                # capacity that does not exist and is answered "nothing matches"
                # -- which reads as a broken fleet rather than a wrong question.
                capacity_class=CapacityClass(pool["capacity_class"])
                if isinstance(pool.get("capacity_class"), str)
                else CapacityClass.ON_DEMAND,
                # Minutes, not the default hour: every figure is an estimate
                # over this number, and quoting an hour for a hello-world
                # overstates it by two orders of magnitude.
                estimated_duration_seconds=300,
            ),
        )
        payload = session.client.request(
            "POST",
            "/api/v1/workloads/estimate",
            json=model_json(request),
            headers=session.workload_headers(scope),
        )
    except Exception as exc:
        return f"unknown ({type(exc).__name__})"
    if not is_json_object(payload):
        return "unknown"
    raw = payload.get("candidates")
    candidates = [entry for entry in raw if is_json_object(entry)] if isinstance(raw, list) else []
    if not candidates:
        # An empty candidate list means nothing in the fleet matches the
        # request, which is a different answer from "it costs nothing" -- and
        # the more useful one to see before submitting.
        return "no pool in this fleet matches that request"
    cheapest = candidates[0]
    cost = cheapest.get("estimated_cost")
    if not isinstance(cost, int | float):
        return "unknown"
    # An unpriced pool estimates zero, and a confident "$0.00" is worse than an
    # admission: it is the number someone budgets against.
    if cheapest.get("price_source") is None:
        return f"unknown ({cheapest.get('pool_id')} declares no price)"
    return f"about ${float(cost):.2f} for five minutes"


def _choose(pools: tuple[dict, ...], shape: str | None) -> dict | None:
    """Pick the pool the first job runs on, honouring an explicit shape."""

    if shape is None:
        return pools[0] if pools else None
    for pool in pools:
        if pool.get("accelerator_type") == shape:
            return pool
    return None


def _emit(step: Step, *, write: tp.Callable[[str], None]) -> None:
    write(f"  {'ok  ' if step.done else 'next'}  {step.headline}: {step.detail}")
    if step.command:
        for line in step.command.splitlines():
            write(f"        {line.strip() if line.startswith('  ') else line}")


@click.command("quickstart")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option(
    "--tpu",
    "shape",
    metavar="TYPE",
    help="Run the first job on this shape. Defaults to whatever the fleet provides.",
)
@click.option(
    "--yes",
    "-y",
    "confirmed",
    is_flag=True,
    default=False,
    help="Submit the first job without asking. It costs real money on real hardware.",
)
@click.pass_obj
def quickstart(
    session: CliSession,
    org_id: str | None,
    project_id: str | None,
    shape: str | None,
    confirmed: bool,
) -> None:
    """Work out what is missing between here and a first running job.

    Each step is either already true, or printed as the exact command that
    makes it true. The last step submits a hello-world and streams it, and
    that one asks first: it allocates real hardware and is billed for it.
    """

    write = click.echo
    write("Eyvan quickstart")
    reachable = _reachable(session)
    _emit(reachable, write=write)
    if not reachable.done:
        return

    fleet_step, _fleet_names = _fleets(session, org_id=org_id, project_id=project_id)
    _emit(fleet_step, write=write)
    if not fleet_step.done:
        return

    capacity, pools = _shapes(session)
    _emit(capacity, write=write)
    if not capacity.done:
        return

    chosen = _choose(pools, shape)
    if chosen is None:
        available = ", ".join(
            sorted({str(pool["accelerator_type"]) for pool in pools if isinstance(pool.get("accelerator_type"), str)})
        )
        raise click.ClickException(f"no enabled pool provides {shape!r}; this fleet provides: {available or 'CPU only'}")
    selected = chosen.get("accelerator_type") if isinstance(chosen.get("accelerator_type"), str) else None
    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    target = f"--tpu {selected}" if selected is not None else "--cpu"
    write("")
    write(f"  first job  eyvan run {target} -- {HELLO_COMMAND}")
    write(f"  cost       {_estimate(session, scope=scope, pool=chosen)}")
    if not confirmed:
        write("")
        write("  Run it with --yes. It allocates real hardware and is billed for it.")
        return

    from eyvan.sdk.commands.workloads import WorkloadCommandService

    write("")
    exit_code = WorkloadCommandService(session).run(
        target_value=selected if selected is not None else "cpu",
        org_id=org_id,
        project_id=project_id,
        name="quickstart",
        environment=(),
        max_retries=0,
        idempotency_key=None,
        command=(HELLO_COMMAND,),
    )
    if exit_code:
        raise SystemExit(exit_code)


def register_quickstart_commands(group: click.Group) -> None:
    """Attach the guided first-run command to the root command group."""

    group.add_command(quickstart)


__all__ = ("HELLO_COMMAND", "Step", "quickstart", "register_quickstart_commands")
