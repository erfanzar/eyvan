# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The properties Eyvan claims always hold, named once and checkable anywhere.

Every one of these is asserted somewhere already -- by a chaos scenario, by a
unit test, or by the code path that maintains it. What was missing is a place
they are stated *together*, under stable names, in a form something other than
a test can evaluate.

That matters for one reason: a soak run. Everything Eyvan has been proven to do
was proven with a human watching, over minutes. The invariants that actually
distinguish a control plane from a script -- nothing stranded, nothing leaked,
nothing double-charged -- are the ones that only fail after days, and a test
suite cannot watch for days. This module is what a long-running observer polls.

**Point-in-time versus over-time.** Most of these can be decided from one
snapshot: a lease either does or does not outlive its workload. Two cannot.
Budget monotonicity and storage boundedness are statements about a *sequence* of
observations, so they live in ``compare_snapshots`` and require a caller that
remembers. Pretending a single read could settle them would be the more
comfortable design and a false one.

**A violation names its subject.** ``I2 violated`` is a page nobody can act on
at 3am; ``I2: lease lease-x is active while workload wl-y is SUCCEEDED`` is one
that points at two records. Every check returns the identifiers.
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from eyvan._internal.compat import StrEnum
from eyvan.records import (
    CellState,
    OutboxState,
    WorkloadState,
    utcnow,
)
from eyvan.server.persistence.base import Repository
from eyvan.server.service.state import ACTIVE_LEASE_STATES, TERMINAL_WORKLOAD_STATES

#: Workload states that mean "this workload holds capacity right now".
#:
#: ``PENDING`` and ``ADMITTED`` are excluded: a workload waiting for capacity
#: legitimately holds none, and calling that a violation would make the check
#: fire on every healthy queue.
LEASE_HOLDING_STATES: tp.Final[frozenset[WorkloadState]] = frozenset(
    {
        WorkloadState.LEASED,
        WorkloadState.DISPATCHING,
        WorkloadState.RUNNING,
        WorkloadState.CHECKPOINTING,
        WorkloadState.CANCELLING,
    }
)

#: Cell states that consume provider capacity and therefore cost money.
LIVE_CELL_STATES: tp.Final[frozenset[CellState]] = frozenset(
    {CellState.REGISTERING, CellState.READY, CellState.DRAINING, CellState.UNAVAILABLE}
)

#: How long an outbox command may stay undelivered before it is stuck rather
#: than in flight. Generous on purpose: a provider call can legitimately take
#: minutes, and an invariant that fires during normal slowness trains an
#: operator to ignore it.
#:
#: Measured from ``created_at``, so it covers a command being retried as well as
#: one that has never been tried. Retry backoff caps at 300s, so a command still
#: failing after this window has had at least several attempts and is not merely
#: unlucky.
STUCK_OUTBOX_AFTER_S = 900.0

#: How far past its retention window a record may sit before pruning is judged
#: to have stopped. One window of slack, because a sweep runs periodically and
#: catching it mid-cycle is not a failure.
RETENTION_SLACK_FACTOR = 2.0


class Invariant(StrEnum):
    """Stable names for the properties a long-running fleet must not violate.

    Named rather than positional so a soak report, a chaos test and an operator
    can all refer to the same thing across releases. Renaming one is a breaking
    change to every one of those readers, which is the point.
    """

    #: No cell consumes provider capacity without a pool that explains it.
    NO_STRANDED_CELLS = "I1"
    #: No lease is active for a workload that has finished or vanished.
    NO_LEASES_OUTLIVING_WORKLOADS = "I2"
    #: No outbox command sits undelivered indefinitely.
    NO_STUCK_OUTBOX = "I3"
    #: No workload claims to be running without capacity to run on.
    NO_RUNNING_WORKLOAD_WITHOUT_A_LEASE = "I4"
    #: A budget's spend equals what its settled leases actually cost.
    BUDGET_MATCHES_SETTLED_COST = "I5"
    #: A budget's spend never decreases within a period.
    BUDGET_SPEND_IS_MONOTONE = "I6"
    #: Managed artifact storage does not grow without bound.
    ARTIFACT_STORAGE_IS_BOUNDED = "I7"
    #: Audit retention actually prunes.
    AUDIT_RETENTION_PRUNES = "I8"
    #: No lease references a cell that no longer exists.
    NO_LEASE_ON_A_MISSING_CELL = "I9"


@dataclass(frozen=True, slots=True)
class Violation:
    """One broken invariant, with enough identity to act on it."""

    invariant: Invariant
    subject: str
    detail: str

    def __str__(self) -> str:
        return f"{self.invariant.value}: {self.detail}"


@dataclass(frozen=True, slots=True)
class FleetSnapshot:
    """What one observation of the fleet recorded, for the over-time checks.

    Deliberately small. A snapshot that carried whole records would make a soak
    run's memory grow with its duration -- and the thing being watched for is
    unbounded growth.
    """

    observed_at: datetime
    #: Spend per ``(org, project)``, and the budget period it was measured in.
    #: The period matters: a roll resets ``spent``, and calling that a
    #: monotonicity violation would fire on correct behaviour once a month.
    budget_spend: dict[tuple[str, str], tuple[float, int]] = field(default_factory=dict)
    #: Total managed-artifact bytes.
    artifact_bytes: int = 0
    workload_count: int = 0
    cell_count: int = 0


async def snapshot_fleet(repository: Repository, *, now: datetime | None = None) -> FleetSnapshot:
    """Record the small set of figures the over-time invariants compare."""

    moment = now or utcnow()
    async with repository.transaction() as tx:
        budgets = await tx.list_budgets()
        artifacts = await tx.list_managed_artifacts()
        workloads = await tx.list_workloads()
        cells = await tx.list_cells()
    return FleetSnapshot(
        observed_at=moment,
        budget_spend={(budget.org_id, budget.project_id): (budget.spent, budget.revision) for budget in budgets},
        artifact_bytes=sum(artifact.size_bytes for artifact in artifacts),
        workload_count=len(workloads),
        cell_count=len([cell for cell in cells if cell.state is not CellState.DELETED]),
    )


async def check_fleet(
    repository: Repository,
    *,
    now: datetime | None = None,
    audit_retention_s: float | None = None,
) -> tuple[Violation, ...]:
    """Evaluate every point-in-time invariant against one read of the fleet.

    One transaction for the whole check, so the answers describe a single
    consistent state. Reading each collection separately would produce
    violations that are artifacts of the read order -- a lease observed before
    the workload that settled it looks exactly like I2.
    """

    moment = now or utcnow()
    async with repository.transaction() as tx:
        workloads = await tx.list_workloads()
        cells = await tx.list_cells()
        leases = await tx.list_leases()
        pools = await tx.list_capacity_pools()
        budgets = await tx.list_budgets()
        pending_outbox = await tx.list_outbox(state=OutboxState.PENDING, limit=1000)
        processing_outbox = await tx.list_outbox(state=OutboxState.PROCESSING, limit=1000)
        # FAILED is where a command that keeps failing actually sits. Between
        # attempts it is settled to FAILED with `available_at` in the future, so
        # a command retrying forever is PENDING or PROCESSING only for the
        # instant it is being tried -- and this check, which looked at those two
        # states alone, reported a healthy fleet while a v5p-16 was pinned by a
        # cancel the worker answered 500 to twenty-four times.
        failed_outbox = await tx.list_outbox(state=OutboxState.FAILED, limit=1000)
        oldest_events = await tx.list_events(limit=1)

    violations: list[Violation] = []
    workload_by_id = {workload.id: workload for workload in workloads}
    cell_ids = {cell.id for cell in cells if cell.state is not CellState.DELETED}
    pool_ids = {pool.id for pool in pools}

    for cell in cells:
        if cell.state in LIVE_CELL_STATES and cell.pool_id and cell.pool_id not in pool_ids:
            violations.append(
                Violation(
                    Invariant.NO_STRANDED_CELLS,
                    cell.id,
                    f"cell {cell.id} is {cell.state.value} against pool {cell.pool_id!r}, which no longer exists",
                )
            )

    for lease in leases:
        if lease.state not in ACTIVE_LEASE_STATES:
            continue
        workload = workload_by_id.get(lease.workload_id)
        if workload is None:
            violations.append(
                Violation(
                    Invariant.NO_LEASES_OUTLIVING_WORKLOADS,
                    lease.id,
                    f"lease {lease.id} is {lease.state.value} for workload {lease.workload_id!r}, which does not exist",
                )
            )
        elif workload.state in TERMINAL_WORKLOAD_STATES:
            violations.append(
                Violation(
                    Invariant.NO_LEASES_OUTLIVING_WORKLOADS,
                    lease.id,
                    f"lease {lease.id} is {lease.state.value} while workload {workload.id} is {workload.state.value}",
                )
            )
        if lease.cell_id not in cell_ids:
            violations.append(
                Violation(
                    Invariant.NO_LEASE_ON_A_MISSING_CELL,
                    lease.id,
                    f"lease {lease.id} is {lease.state.value} on cell {lease.cell_id!r}, which no longer exists",
                )
            )

    stuck_before = moment - timedelta(seconds=STUCK_OUTBOX_AFTER_S)
    for command in (*pending_outbox, *processing_outbox, *failed_outbox):
        if command.created_at <= stuck_before:
            age = int((moment - command.created_at).total_seconds())
            violations.append(
                Violation(
                    Invariant.NO_STUCK_OUTBOX,
                    command.id,
                    f"outbox command {command.id} ({command.topic}) has been {command.state.value} for {age}s",
                )
            )

    leased_workloads = {lease.workload_id for lease in leases if lease.state in ACTIVE_LEASE_STATES}
    for workload in workloads:
        if workload.state in LEASE_HOLDING_STATES and workload.id not in leased_workloads:
            violations.append(
                Violation(
                    Invariant.NO_RUNNING_WORKLOAD_WITHOUT_A_LEASE,
                    workload.id,
                    f"workload {workload.id} is {workload.state.value} with no active lease",
                )
            )

    settled_by_scope: dict[tuple[str, str], float] = {}
    for lease in leases:
        if lease.settled_at is None:
            continue
        key = (lease.org_id, lease.project_id)
        settled_by_scope[key] = settled_by_scope.get(key, 0.0) + lease.settled_cost
    for budget in budgets:
        settled = settled_by_scope.get((budget.org_id, budget.project_id), 0.0)
        # Tolerance rather than equality: costs accumulate through binary
        # floating point, and an invariant that fires on the last bit would fire
        # constantly while saying nothing about correctness.
        if abs(budget.spent - settled) > max(0.01, abs(settled) * 1e-6):
            violations.append(
                Violation(
                    Invariant.BUDGET_MATCHES_SETTLED_COST,
                    f"{budget.org_id}/{budget.project_id}",
                    f"budget {budget.org_id}/{budget.project_id} records {budget.spent} spent "
                    f"while its settled leases total {settled}",
                )
            )

    if audit_retention_s is not None and oldest_events:
        cutoff = moment - timedelta(seconds=audit_retention_s * RETENTION_SLACK_FACTOR)
        oldest = oldest_events[0]
        if oldest.timestamp < cutoff:
            age = int((moment - oldest.timestamp).total_seconds())
            violations.append(
                Violation(
                    Invariant.AUDIT_RETENTION_PRUNES,
                    oldest.id,
                    f"oldest audit event {oldest.id} is {age}s old against a retention of "
                    f"{int(audit_retention_s)}s; the prune sweep is not running",
                )
            )

    return tuple(violations)


def compare_snapshots(
    previous: FleetSnapshot,
    current: FleetSnapshot,
    *,
    max_artifact_bytes: int | None = None,
) -> tuple[Violation, ...]:
    """Evaluate the invariants that need two observations rather than one.

    Neither of these can be decided from a single read, and a module that
    pretended otherwise would be the more comfortable design and a false one.
    """

    violations: list[Violation] = []
    for scope, (spent, revision) in current.budget_spend.items():
        before = previous.budget_spend.get(scope)
        if before is None:
            continue
        previous_spent, previous_revision = before
        if revision != previous_revision:
            # A period roll resets ``spent`` deliberately. Calling that a
            # monotonicity violation would fire on correct behaviour once a
            # month, which is how an invariant gets muted.
            continue
        if spent < previous_spent:
            violations.append(
                Violation(
                    Invariant.BUDGET_SPEND_IS_MONOTONE,
                    f"{scope[0]}/{scope[1]}",
                    f"budget {scope[0]}/{scope[1]} spend fell from {previous_spent} to {spent} without a period roll",
                )
            )
    if max_artifact_bytes is not None and current.artifact_bytes > max_artifact_bytes:
        growth = current.artifact_bytes - previous.artifact_bytes
        violations.append(
            Violation(
                Invariant.ARTIFACT_STORAGE_IS_BOUNDED,
                "managed-artifacts",
                f"managed artifact storage is {current.artifact_bytes} bytes against a ceiling of "
                f"{max_artifact_bytes} (grew {growth} bytes since the previous observation)",
            )
        )
    return tuple(violations)


__all__ = (
    "LEASE_HOLDING_STATES",
    "LIVE_CELL_STATES",
    "RETENTION_SLACK_FACTOR",
    "STUCK_OUTBOX_AFTER_S",
    "FleetSnapshot",
    "Invariant",
    "Violation",
    "check_fleet",
    "compare_snapshots",
    "snapshot_fleet",
)
