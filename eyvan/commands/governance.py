# Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Quota, budget, and policy commands for the Eyvan CLI.

Ten governance routes shipped with no CLI at all, which is a real gap rather than
a cosmetic one: ``eyvan doctor`` and ``eyvan workload explain`` exist to answer
"why is my job stuck", and the top three answers are quota, budget, and policy --
none of which an operator could read, let alone change, without writing the HTTP
call by hand.

Every mutation goes through ``OperationsCommandService.mutate`` rather than
calling the client directly. That is not stylistic: these routes *require* an
``Idempotency-Key``, and routing through the shared service reuses the one
``IdempotencyKeyFactory`` instead of leaving each command to remember.
"""

from __future__ import annotations

import json
import typing as tp

import click
from pydantic import ValidationError

from eyvan._json import model_json
from eyvan.api.schemas import BudgetPutRequest, PolicyPutRequest, QuotaPutRequest
from eyvan.commands.common import JsonRenderer
from eyvan.commands.operations import OperationsCommandService
from eyvan.commands.state import CliSession
from eyvan.service.models import CapacityClass, ResourceLimits


def _service(session: CliSession) -> OperationsCommandService:
    return OperationsCommandService(session)


def _put(
    session: CliSession,
    path: str,
    request: BudgetPutRequest | PolicyPutRequest | QuotaPutRequest,
    *,
    operation: str,
    idempotency_key: str | None,
) -> None:
    """Send one governance PUT through the shared idempotency factory."""

    service = _service(session)
    service.renderer.emit(
        session.client.request(
            "PUT",
            path,
            json=model_json(request),
            headers={"Idempotency-Key": service.keys.create(operation, idempotency_key)},
        )
    )


@click.group("quota")
def quota() -> None:
    """Read and set hierarchical resource quotas."""


@quota.command("list")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def list_quotas(session: CliSession, org_id: str | None, project_id: str | None) -> None:
    """List quota records in a scope."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    params: dict[str, str | int] = {"org_id": scope.org_id}
    if project_id is not None:
        params["project_id"] = project_id
    _service(session).read("/api/v1/quotas", params=params)


@quota.command("get")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def get_quota(session: CliSession, org_id: str | None, project_id: str | None) -> None:
    """Show the quota that currently applies to this scope.

    The *effective* quota, not a stored row: the hierarchy resolves org, project
    and principal scopes, and knowing which one is binding is the whole question.
    """

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    _service(session).read(
        "/api/v1/quotas/current",
        params={"org_id": scope.org_id, "project_id": scope.project_id},
    )


@quota.command("set")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--principal", "principal_id", help="Narrow the quota to one principal.")
@click.option("--cpu", type=float, default=0.0, show_default=True)
@click.option("--memory-gib", type=float, default=0.0, show_default=True)
@click.option("--gpu", type=int, default=0, show_default=True)
@click.option("--tpu", type=int, default=0, show_default=True)
@click.option("--artifact-bytes", type=int, help="Ceiling on stored artifact bytes; omit for unlimited.")
@click.option("--weight", type=float, default=1.0, show_default=True, help="Fair-share weight.")
@click.option("--idempotency-key")
@click.pass_obj
def set_quota(
    session: CliSession,
    org_id: str | None,
    project_id: str | None,
    principal_id: str | None,
    cpu: float,
    memory_gib: float,
    gpu: int,
    tpu: int,
    artifact_bytes: int | None,
    weight: float,
    idempotency_key: str | None,
) -> None:
    """Set the quota for one scope."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    try:
        request = QuotaPutRequest(
            org_id=scope.org_id,
            project_id=project_id,
            principal_id=principal_id,
            limits=ResourceLimits(cpu=cpu, memory_gib=memory_gib, gpu=gpu, tpu=tpu),
            artifact_bytes_limit=artifact_bytes,
            weight=weight,
        )
    except (ValidationError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    _put(session, "/api/v1/quotas", request, operation="quota", idempotency_key=idempotency_key)


@quota.command("delete")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--principal", "principal_id")
@click.pass_obj
def delete_quota(session: CliSession, org_id: str | None, project_id: str | None, principal_id: str | None) -> None:
    """Remove a quota record, restoring the next-broadest scope."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    params: dict[str, str | int] = {"org_id": scope.org_id}
    if project_id is not None:
        params["project_id"] = project_id
    if principal_id is not None:
        params["principal_id"] = principal_id
    session.client.request("DELETE", "/api/v1/quotas", params=params)
    click.echo("deleted")


@click.group("budget")
def budget() -> None:
    """Read and set monetary admission budgets."""


@budget.command("list")
@click.option("--org", "org_id")
@click.pass_obj
def list_budgets(session: CliSession, org_id: str | None) -> None:
    """List budgets in an organization."""

    scope = session.workload_scope(org_id=org_id)
    _service(session).read("/api/v1/budgets", params={"org_id": scope.org_id})


@budget.command("get")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def get_budget(session: CliSession, org_id: str | None, project_id: str | None) -> None:
    """Show one budget record."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    _service(session).read(f"/api/v1/budgets/{scope.org_id}/{scope.project_id}")


@budget.command("status")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def budget_status(session: CliSession, org_id: str | None, project_id: str | None) -> None:
    """Show a budget's period window and projected end-of-period spend.

    The forecast assumes every active lease runs to the end of the window --
    deliberately pessimistic, because the number exists to warn before a hard
    limit starts refusing admission.
    """

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    _service(session).read(f"/api/v1/budgets/{scope.org_id}/{scope.project_id}/status")


@budget.command("roll")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def roll_budget(session: CliSession, org_id: str | None, project_id: str | None) -> None:
    """Close an elapsed budget period without waiting for the next settlement.

    Not a force-reset: a period that has not elapsed is left alone, because
    zeroing it would erase spend the limit is still meant to bind.
    """

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    renderer = JsonRenderer()
    renderer.emit(session.client.request("POST", f"/api/v1/budgets/{scope.org_id}/{scope.project_id}/roll"))


@budget.command("set")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--limit", type=float, required=True)
@click.option("--period", default="monthly", show_default=True)
@click.option(
    "--alert-at",
    "alert_thresholds",
    multiple=True,
    type=float,
    help="Fraction of the limit that raises an alert; repeatable (e.g. --alert-at 0.8).",
)
@click.option("--soft", is_flag=True, help="Report overspend instead of refusing admission.")
@click.option("--idempotency-key")
@click.pass_obj
def set_budget(
    session: CliSession,
    org_id: str | None,
    project_id: str | None,
    limit: float,
    period: str,
    alert_thresholds: tuple[float, ...],
    soft: bool,
    idempotency_key: str | None,
) -> None:
    """Set a project's budget."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    try:
        request = BudgetPutRequest(
            limit=limit,
            period=period,
            alert_thresholds=tuple(sorted(set(alert_thresholds))),
            hard_limit=not soft,
        )
    except (ValidationError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    _put(
        session,
        f"/api/v1/budgets/{scope.org_id}/{scope.project_id}",
        request,
        operation="budget",
        idempotency_key=idempotency_key,
    )


@click.group("policy")
def policy() -> None:
    """Read and set project scheduling policy."""


@policy.command("list")
@click.option("--org", "org_id")
@click.pass_obj
def list_policies(session: CliSession, org_id: str | None) -> None:
    """List policies in an organization."""

    scope = session.workload_scope(org_id=org_id)
    _service(session).read("/api/v1/policies", params={"org_id": scope.org_id})


@policy.command("get")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def get_policy(session: CliSession, org_id: str | None, project_id: str | None) -> None:
    """Show one project policy."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    _service(session).read(f"/api/v1/policies/{scope.org_id}/{scope.project_id}")


@policy.command("set")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--allow-provider", "allowed_providers", multiple=True)
@click.option("--allow-region", "allowed_regions", multiple=True)
@click.option(
    "--allow-capacity-class",
    "allowed_capacity_classes",
    multiple=True,
    type=click.Choice(tuple(item.value for item in CapacityClass)),
)
@click.option("--deny-accelerator-type", "denied_accelerator_types", multiple=True)
@click.option("--max-priority", type=int, default=1000, show_default=True)
@click.option(
    "--no-preemption",
    is_flag=True,
    help="Refuse to preempt this project's workloads for higher-priority work.",
)
@click.option("--idempotency-key")
@click.pass_obj
def set_policy(
    session: CliSession,
    org_id: str | None,
    project_id: str | None,
    allowed_providers: tuple[str, ...],
    allowed_regions: tuple[str, ...],
    allowed_capacity_classes: tuple[str, ...],
    denied_accelerator_types: tuple[str, ...],
    max_priority: int,
    no_preemption: bool,
    idempotency_key: str | None,
) -> None:
    """Set a project's scheduling policy.

    An empty allow-list means "no restriction", not "allow nothing" -- which is
    why these options are additive and omitting one leaves that dimension open.
    """

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    try:
        request = PolicyPutRequest(
            allowed_providers=allowed_providers,
            allowed_regions=allowed_regions,
            allowed_capacity_classes=tp.cast(tp.Any, allowed_capacity_classes),
            denied_accelerator_types=denied_accelerator_types,
            max_priority=max_priority,
            preemption_enabled=not no_preemption,
        )
    except (ValidationError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    _put(
        session,
        f"/api/v1/policies/{scope.org_id}/{scope.project_id}",
        request,
        operation="policy",
        idempotency_key=idempotency_key,
    )


@click.command("system-reconcile")
@click.pass_obj
def system_reconcile(session: CliSession) -> None:
    """Run one scheduling reconcile pass on the leader.

    Returns 409 when this replica does not hold leadership, which is an ordinary
    answer rather than an error: exactly one replica reconciles at a time.
    """

    payload = session.client.request("POST", "/api/v1/system/reconcile")
    click.echo(json.dumps(payload, sort_keys=True, indent=2, default=str))


def register_governance_commands(main: click.Group) -> None:
    """Attach the governance command groups to the CLI root."""

    for command in (quota, budget, policy, system_reconcile):
        main.add_command(command)


__all__ = ("budget", "policy", "quota", "register_governance_commands", "system_reconcile")
