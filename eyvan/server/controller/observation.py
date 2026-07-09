# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Reading live cell and workload state back into durable records."""

from __future__ import annotations

import asyncio
import json
import typing as tp
import uuid
from dataclasses import replace
from datetime import datetime, timedelta

from eyvan._internal.json_types import JsonObject
from eyvan.artifacts.managed import log_uri
from eyvan.capacity import (
    DEFINITIVE_CAPACITY_LOSS_STATUSES,
    PENDING_CAPACITY_STATUSES,
    READY_CAPACITY_STATUSES,
    CapacityStatus,
)
from eyvan.capacity.drivers.base import CapacityProviderFailureCode, ExecutionWorkloadStatus, provision_outcome
from eyvan.capacity.planning import (
    cell_execution_runtime_is_pending,
)
from eyvan.domain import (
    ProvisionOutcome,
)
from eyvan.records import (
    AuditEvent,
    CellRecord,
    CellState,
    CoordinationKind,
    LeaseRecord,
    LeaseState,
    WorkloadRecord,
    WorkloadState,
    utcnow,
)
from eyvan.server.controller._support import (
    _LIVE_WORKLOAD_STATES,
    _PREEMPTION_OWNED_TERMINAL_WORKLOAD_STATUSES,
    _RECOVERABLE_WORKLOAD_STATUSES,
    StaleOutboxCommand,
    _provider_phase_start,
    logger,
)
from eyvan.server.service.errors import ConflictError, NotFoundError
from eyvan.worker.agent.client import (
    CellAgentBusyError,
    CellAgentCapacityProviderError,
    CellAgentStaleMutationError,
    CellAgentUnavailableError,
)
from eyvan.worker.agent.models import (
    GangWorkloadMutationRequest,
    MutationModel,
    WorkloadMutationRequest,
)
from eyvan.worker.runtime.log_records import (
    LogPage,
    LogRecord,
    log_page_from_json,
    merge_pages,
)
from eyvan.worker.runtime.log_records import (
    join_log_cursor as _join_log_cursor,
)
from eyvan.worker.runtime.log_records import (
    split_log_cursor as _split_log_cursor,
)

#: How many ranks an archive read probes before concluding the set has ended.
#: Bounded because a miss costs a storage round trip, and a gang wider than this
#: would have failed the multislice ceiling long before it got here.
_ARCHIVED_RANK_SCAN_LIMIT = 64


def _log_page_from_document(payload: JsonObject, *, rank: int) -> LogPage:
    """Rebuild a page from one agent's JSON, stamping the rank it came from.

    The agent does not know its own rank -- it knows a cell -- so attribution is
    applied here, where the gang's ordering is known.

    Decoding is delegated rather than repeated. This function used to build
    ``LogRecord`` itself and omitted ``timestamp`` entirely, so every record read
    over the wire arrived without one. That is worse than a missing column:
    ``merge_pages`` orders by ``(timestamp or _EPOCH, rank)``, so with every
    timestamp ``None`` a multi-host page collapsed to rank order and the
    chronological interleaving that per-rank attribution exists to produce was
    silently gone.
    """

    page = log_page_from_json(payload)
    return LogPage(
        records=tuple(replace(record, rank=rank) for record in page.records),
        next_cursor=page.next_cursor,
        unavailable=page.unavailable,
        detail=page.detail,
        truncated_ranks=page.truncated_ranks,
    )


def _log_page_document(
    page: LogPage,
    *,
    attempt: int,
    attempt_changed: bool = False,
    cursors: tp.Mapping[int, str] | None = None,
) -> JsonObject:
    """Project a merged page onto the wire, carrying the attempt it belongs to.

    ``next_cursor`` encodes every rank's position and the attempt they came
    from. The attempt has to travel with it: without it an attempt boundary is
    undetectable, and a follower resumes a retry from its predecessor's
    position.
    """

    document: JsonObject = {
        "attempt": attempt,
        "attempt_changed": attempt_changed,
        "records": [dict(record.as_json()) for record in page.records],
        "next_cursor": _join_log_cursor(attempt, cursors or {}),
        "unavailable": page.unavailable,
        "detail": page.detail,
        "truncated_ranks": list(page.truncated_ranks),
    }
    return document


class _ControllerObservation:
    """Reading live cell and workload state back into durable records.

    A concern of :class:`~eyvan.server.controller.EyvanController`; not
    instantiated on its own.
    """

    async def observe_workloads_once(self) -> int:
        async with self.repository.transaction() as transaction:
            workloads = [
                workload
                for workload in await transaction.list_workloads()
                if workload.state in _LIVE_WORKLOAD_STATES
                and (workload.remote_id or workload.state is WorkloadState.LEASED)
            ]
            leases = {lease.id: lease for lease in await transaction.list_leases()}
            cells = {cell.id: cell for cell in await transaction.list_cells()}
            dispatch_commands = await transaction.list_outbox_for_aggregates(
                "workload",
                {workload.id: workload.generation for workload in workloads},
                topic="workload.dispatch",
            )
        observed = 0
        for workload in workloads:
            lease_ids = self._workload_lease_ids(workload)
            members = tuple(leases.get(lease_id) for lease_id in lease_ids)
            concrete_leases = tuple(lease for lease in members if lease is not None)
            if not members or len(concrete_leases) != len(members):
                continue
            member_cells = tuple(cells.get(lease.cell_id) for lease in concrete_leases)
            concrete_cells = tuple(cell for cell in member_cells if cell is not None)
            if len(concrete_cells) != len(member_cells):
                continue
            pairs = tuple(zip(concrete_leases, concrete_cells, strict=True))
            lease, cell = pairs[0]
            is_group = len(pairs) > 1

            lost_pair = next(
                (
                    member
                    for member in pairs
                    if member[1].state in {CellState.DELETING, CellState.DELETED}
                    or (
                        member[1].state is CellState.UNAVAILABLE
                        and member[1].provider_phase in DEFINITIVE_CAPACITY_LOSS_STATUSES
                    )
                    or (member[1].state is CellState.REGISTERING and not cell_execution_runtime_is_pending(member[1]))
                ),
                None,
            )
            if lost_pair is not None:
                _lost_lease, lost_cell = lost_pair
                capacity_lost = lost_cell.state in {CellState.DELETING, CellState.DELETED} or (
                    lost_cell.state is CellState.UNAVAILABLE
                    and lost_cell.provider_phase in DEFINITIVE_CAPACITY_LOSS_STATUSES
                )
                reason = (
                    f"execution cell capacity is {lost_cell.unavailable_reason}"
                    if lost_cell.state is CellState.UNAVAILABLE
                    else f"execution cell is {lost_cell.state.value}"
                    if capacity_lost
                    else "execution cell capacity was recreated"
                )
                try:
                    if workload.state is WorkloadState.LEASED and workload.remote_id is None:
                        await self.service.requeue_unstarted_workload(
                            self._principal,
                            workload.id,
                            scheduling_reason=reason,
                            expected_generation=workload.generation,
                            expected_lease_id=lease.id,
                            expected_cell_generation=lost_cell.generation,
                            expected_cell_state=lost_cell.state,
                            expected_unavailable_reason=lost_cell.unavailable_reason,
                            expected_cell_id=lost_cell.id,
                        )
                    else:
                        if is_group:
                            await self._stop_terminal_workload_peers(
                                workload,
                                lease,
                                cell,
                                ExecutionWorkloadStatus.LOST,
                            )
                        await self._recover_lost_workload(workload, lease, reason, lost_cell=lost_cell)
                except Exception as exc:
                    logger.info("workload %s loss reconciliation became stale: %s", workload.id, exc)
                observed += 1
                continue
            changed_pair = next(
                (member for member in pairs if member[0].cell_generation != member[1].generation),
                None,
            )
            if changed_pair is not None and workload.remote_id is None:
                _changed_lease, changed_cell = changed_pair
                try:
                    await self.service.requeue_unstarted_workload(
                        self._principal,
                        workload.id,
                        scheduling_reason="cell generation changed before dispatch",
                        expected_generation=workload.generation,
                        expected_lease_id=lease.id,
                        expected_cell_generation=changed_cell.generation,
                        expected_cell_state=changed_cell.state,
                        expected_unavailable_reason=changed_cell.unavailable_reason,
                        expected_cell_id=changed_cell.id,
                    )
                except Exception as exc:
                    logger.info("workload %s undispatched recovery became stale: %s", workload.id, exc)
                observed += 1
                continue
            if workload.remote_id is None:
                releasable_undispatched_leases = all(
                    member.state in {LeaseState.PENDING, LeaseState.QUARANTINED} for member in concrete_leases
                ) and any(member.state is LeaseState.QUARANTINED for member in concrete_leases)
                if releasable_undispatched_leases and not self._has_live_dispatch_command(
                    workload,
                    lease_ids,
                    dispatch_commands,
                ):
                    try:
                        await self.service.requeue_unstarted_workload(
                            self._principal,
                            workload.id,
                            scheduling_reason="undispatched lease expired after its dispatch command retired",
                            expected_generation=workload.generation,
                            expected_lease_id=lease.id,
                            expected_cell_generation=cell.generation,
                            expected_cell_state=cell.state,
                            expected_unavailable_reason=cell.unavailable_reason,
                            expected_cell_id=cell.id,
                        )
                    except Exception as exc:
                        logger.info("workload %s expired undispatched recovery became stale: %s", workload.id, exc)
                    observed += 1
                continue
            try:
                if is_group:
                    coordinator_lease, coordinator_cell = self._ordered_gang_member_pairs(pairs)[0]
                    remote = await asyncio.to_thread(
                        self._agent_factory(coordinator_cell).gang_workload,
                        coordinator_lease.cell_id,
                        workload.id,
                        workload.remote_id,
                    )
                else:
                    remote = await asyncio.to_thread(self._agent_factory(cell).workload, cell.id, workload.remote_id)
            except Exception as exc:
                logger.warning("workload %s observation failed: %s", workload.id, exc)
                continue
            try:
                if is_group:
                    await self._recover_group_member_leases(pairs, remote.status)
                await self._apply_workload_observation(
                    workload,
                    lease,
                    cell,
                    remote.status,
                    remote.message,
                )
            except Exception as exc:
                logger.info("workload %s observation became stale: %s", workload.id, exc)
            observed += 1
        return observed

    async def _recover_group_member_leases(
        self,
        members: tuple[tuple[LeaseRecord, CellRecord], ...],
        status: ExecutionWorkloadStatus,
    ) -> None:
        """Renew quarantined non-leader group leases after a healthy group observation."""

        if status not in _RECOVERABLE_WORKLOAD_STATUSES:
            return
        for lease, _cell in members[1:]:
            if lease.state is not LeaseState.QUARANTINED or lease.revocation_reason == "preemption":
                continue
            await self.service.transition_lease(
                self._principal,
                lease.id,
                LeaseState.ACTIVE,
                action="lease.recovered",
            )
            async with self.repository.transaction() as transaction:
                current = await transaction.get_lease(lease.id)
                if current is not None and current.state is LeaseState.ACTIVE:
                    now = utcnow()
                    await transaction.put_lease(
                        current.model_copy(
                            update={
                                "expires_at": now + timedelta(seconds=self.settings.lease_ttl_s),
                                "updated_at": now,
                            }
                        )
                    )

    async def _apply_workload_observation(
        self,
        workload: WorkloadRecord,
        lease: LeaseRecord,
        cell: CellRecord,
        status: ExecutionWorkloadStatus,
        message: str | None,
    ) -> None:
        if status is ExecutionWorkloadStatus.INDETERMINATE:
            await self._escalate_unreadable_execution(workload, cell, message)
            return
        self._indeterminate_since.pop((workload.id, workload.generation), None)
        preemption_owned = lease.revocation_reason == "preemption" and lease.state in {
            LeaseState.REVOKING,
            LeaseState.QUARANTINED,
        }
        if preemption_owned and status in _PREEMPTION_OWNED_TERMINAL_WORKLOAD_STATUSES:
            return
        if lease.state is LeaseState.QUARANTINED and not preemption_owned and status in _RECOVERABLE_WORKLOAD_STATUSES:
            await self.service.transition_lease(
                self._principal,
                lease.id,
                LeaseState.ACTIVE,
                action="lease.recovered",
            )
            async with self.repository.transaction() as transaction:
                recovered = await transaction.get_lease(lease.id)
                if recovered is not None:
                    await transaction.put_lease(
                        recovered.model_copy(
                            update={
                                "expires_at": utcnow() + timedelta(seconds=self.settings.lease_ttl_s),
                                "updated_at": utcnow(),
                            }
                        )
                    )
        if status is ExecutionWorkloadStatus.RUNNING and workload.state is WorkloadState.DISPATCHING:
            await self.service.transition_workload(
                self._principal,
                workload.id,
                WorkloadState.RUNNING,
                expected_generation=workload.generation,
                expected_lease_id=lease.id,
                expected_state=workload.state,
            )
        elif status is ExecutionWorkloadStatus.SUCCEEDED:
            current = workload
            if workload.state is WorkloadState.DISPATCHING:
                current = await self.service.transition_workload(
                    self._principal,
                    workload.id,
                    WorkloadState.RUNNING,
                    expected_generation=workload.generation,
                    expected_lease_id=lease.id,
                    expected_state=workload.state,
                )
            await self._release_terminal(current, lease, WorkloadState.SUCCEEDED, "workload.succeeded")
        elif status in {ExecutionWorkloadStatus.FAILED, ExecutionWorkloadStatus.STOPPED}:
            if status is ExecutionWorkloadStatus.FAILED:
                await self._stop_terminal_workload_peers(workload, lease, cell, status)
            if workload.state is WorkloadState.CANCELLING and not preemption_owned:
                await self._release_terminal(workload, lease, WorkloadState.CANCELLED, "workload.cancelled")
            elif (
                status is ExecutionWorkloadStatus.FAILED
                and not preemption_owned
                and workload.failure_count < workload.spec.max_failure_retries
            ):
                if workload.lease_id != lease.id:
                    raise StaleOutboxCommand(f"lease {lease.id!r} no longer owns workload {workload.id!r}")
                await self.service.settle_failed_workload(
                    self._principal,
                    workload.id,
                    checkpoint_uri=workload.checkpoint_uri,
                    scheduling_reason=(
                        f"execution failed; retrying user-code failure "
                        f"({workload.failure_count + 1} of {workload.spec.max_failure_retries})"
                    ),
                    expected_generation=workload.generation,
                    expected_lease_id=lease.id,
                    expected_state=workload.state,
                )
            elif not preemption_owned:
                # The executor's diagnostic is right here and used one branch
                # below for LOST, but the terminal-failure path used to drop it:
                # a failed workload recorded `failure_reason: None`, so the only
                # account of why it died lived in logs on a cell that is about to
                # be released. Observed live 2026-08-01.
                await self._release_terminal(
                    workload,
                    lease,
                    WorkloadState.FAILED,
                    "workload.failed",
                    failure_reason=message or "execution backend reported failure without a diagnostic",
                )
        elif status in {ExecutionWorkloadStatus.LOST, ExecutionWorkloadStatus.NOT_FOUND}:
            await self._stop_terminal_workload_peers(workload, lease, cell, status)
            await self._recover_lost_workload(
                workload,
                lease,
                message or f"execution backend reported {status.value}",
            )

    async def _stop_terminal_workload_peers(
        self,
        workload: WorkloadRecord,
        lease: LeaseRecord,
        cell: CellRecord,
        status: ExecutionWorkloadStatus,
    ) -> None:
        """Fence every reachable rank before settling a failed slice workload.

        A native multi-host execution can report a failed or lost rank while
        peers are still waiting in a JAX collective. The original lease fence
        remains authoritative for this attempt, so best-effort fan-out stop
        happens before the workload is released or requeued. A missing host is
        expected for ``LOST`` and must not prevent recovery of the lease.
        """

        if workload.remote_id is None:
            return
        if len(self._workload_lease_ids(workload)) > 1:
            try:
                members = await self._load_workload_members(workload)
                ordered = self._ordered_gang_member_pairs(members)
                coordinator_lease, coordinator_cell = ordered[0]
                request = GangWorkloadMutationRequest(
                    members=self._gang_members(
                        ordered,
                        idempotency_prefix=f"terminal-stop:{workload.id}:g{workload.generation}",
                    )
                )
                await asyncio.to_thread(
                    self._agent_factory(coordinator_cell).stop_gang,
                    coordinator_lease.cell_id,
                    workload.id,
                    workload.remote_id,
                    request,
                )
            except Exception as exc:
                logger.warning(
                    "workload %s reported %s and group peer stop was incomplete: %s",
                    workload.id,
                    status.value,
                    exc,
                )
            return
        request = WorkloadMutationRequest(
            mutation=MutationModel(
                idempotency_key=f"terminal-stop:{workload.id}:g{workload.generation}",
                generation=workload.generation,
                fencing_token=lease.fencing_token,
            )
        )
        try:
            await asyncio.to_thread(self._agent_factory(cell).stop, cell.id, workload.remote_id, request)
        except Exception as exc:
            logger.warning(
                "workload %s reported %s and peer stop was incomplete: %s",
                workload.id,
                status.value,
                exc,
            )

    async def _escalate_unreadable_execution(
        self,
        workload: WorkloadRecord,
        cell: CellRecord,
        message: str | None,
    ) -> None:
        """Replace the host of an execution nobody has been able to read.

        An INDETERMINATE observation says the runtime could not be read, not
        that the workload finished, so the controller keeps polling. Polling
        forever is the failure this exists to stop: on 2026-08-02 a workload sat
        in CANCELLING for forty minutes holding a v5p Multislice because every
        observation came back unreadable and nothing ever escalated.

        Settling the workload alone would be unsafe -- it might still be running,
        and releasing the lease would let a second workload onto the same
        accelerator. The cell is the only thing that can be reclaimed with
        certainty, so drain it: replacing the host destroys whatever survived,
        and the existing cell-loss path then recovers the workload.
        """

        grace = timedelta(seconds=self.settings.indeterminate_execution_grace_s)
        key = (workload.id, workload.generation)
        first_seen = self._indeterminate_since.setdefault(key, utcnow())
        if utcnow() - first_seen < grace:
            return
        if cell.state is not CellState.READY:
            # Already registering or on its way out; the cell-loss path owns it.
            return
        self._indeterminate_since.pop(key, None)
        logger.warning(
            "draining cell %s: execution of workload %s has been unreadable for %ss (%s)",
            cell.id,
            workload.id,
            self.settings.indeterminate_execution_grace_s,
            message or "no diagnostic",
        )
        await self.drain_cell(
            self._principal,
            cell.id,
            idempotency_key=f"unreadable-execution:{workload.id}:{workload.generation}",
        )

    async def _recover_lost_workload(
        self,
        workload: WorkloadRecord,
        lease: LeaseRecord,
        reason: str,
        *,
        lost_cell: CellRecord | None = None,
    ) -> None:
        if workload.lease_id != lease.id:
            return
        preemption_owned = lease.revocation_reason == "preemption" and lease.state in {
            LeaseState.REVOKING,
            LeaseState.QUARANTINED,
        }
        if workload.state is WorkloadState.CANCELLING and not preemption_owned:
            await self.service.settle_terminal_workload(
                self._principal,
                workload.id,
                WorkloadState.CANCELLED,
                action="workload.cancelled_after_loss",
                expected_generation=workload.generation,
                expected_lease_id=lease.id,
                expected_state=workload.state,
                expected_cell_generation=None if lost_cell is None else lost_cell.generation,
                expected_cell_state=None if lost_cell is None else lost_cell.state,
                expected_unavailable_reason=None if lost_cell is None else lost_cell.unavailable_reason,
                expected_cell_id=None if lost_cell is None else lost_cell.id,
            )
            return
        # The cloud reclaiming capacity, not Eyvan preempting a lower-priority
        # job for it. They are charged to different budgets because they mean
        # opposite things about whether running again can work: a spot
        # reclamation is the expected ending of a spot job, while a capacity
        # loss may be a pool that has nothing left to give.
        provider_preempted = (
            lost_cell is not None and lost_cell.provider_failure_code is CapacityProviderFailureCode.PREEMPTED
        )
        spent, budget = (
            (workload.preemption_count, workload.spec.max_preemption_retries)
            if provider_preempted
            else (workload.capacity_loss_count, workload.spec.max_retries)
        )
        if (workload.checkpoint_uri or preemption_owned) and spent < budget:
            await self.service.settle_preempted_workload(
                self._principal,
                workload.id,
                checkpoint_uri=workload.checkpoint_uri,
                scheduling_reason=(
                    f"cell lost; resuming from {workload.checkpoint_uri} ({spent + 1} of {budget})"
                    if workload.checkpoint_uri
                    else f"cell lost during preemption; restarting without a checkpoint ({spent + 1} of {budget})"
                ),
                action="workload.recovery_queued",
                expected_generation=workload.generation,
                expected_lease_id=lease.id,
                expected_state=workload.state,
                require_preemption_owner=preemption_owned,
                provider_preempted=provider_preempted,
                expected_cell_generation=None if lost_cell is None else lost_cell.generation,
                expected_cell_state=None if lost_cell is None else lost_cell.state,
                expected_unavailable_reason=None if lost_cell is None else lost_cell.unavailable_reason,
                expected_cell_id=None if lost_cell is None else lost_cell.id,
            )
            return
        if lost_cell is not None and spent < budget:
            await self.service.settle_preempted_workload(
                self._principal,
                workload.id,
                scheduling_reason=(
                    f"{reason}; retrying provider preemption ({spent + 1} of {budget})"
                    if provider_preempted
                    else f"{reason}; retrying capacity loss ({spent + 1} of {budget})"
                ),
                action="workload.capacity_recovery_queued",
                expected_generation=workload.generation,
                expected_lease_id=lease.id,
                expected_state=workload.state,
                provider_preempted=provider_preempted,
                expected_cell_generation=lost_cell.generation,
                expected_cell_state=lost_cell.state,
                expected_unavailable_reason=lost_cell.unavailable_reason,
                expected_cell_id=lost_cell.id,
            )
            return
        await self.service.settle_terminal_workload(
            self._principal,
            workload.id,
            WorkloadState.FAILED,
            action="workload.lost",
            failure_reason=reason,
            expected_generation=workload.generation,
            expected_lease_id=lease.id,
            expected_state=workload.state,
            expected_cell_generation=None if lost_cell is None else lost_cell.generation,
            expected_cell_state=None if lost_cell is None else lost_cell.state,
            expected_unavailable_reason=None if lost_cell is None else lost_cell.unavailable_reason,
            expected_cell_id=None if lost_cell is None else lost_cell.id,
        )

    async def observe_cells_once(self) -> int:
        """Advance capacity state while serializing with lifecycle transitions."""

        return await self._observe_cells_once()

    async def _observe_cells_once(self) -> int:
        """Advance provider capacity state and refresh the schedulable inventory."""

        now = utcnow()
        async with self.repository.transaction() as transaction:
            disabled_pool_ids = {pool.id for pool in await transaction.list_capacity_pools() if not pool.enabled}
            candidates = [
                cell
                for cell in await transaction.list_cells()
                if cell.state in {CellState.REGISTERING, CellState.READY, CellState.UNAVAILABLE}
                and (cell.availability.cooldown_until is None or cell.availability.cooldown_until <= now)
                and cell.pool_id not in disabled_pool_ids
            ]
            # Only REGISTERING cells consult the outbox, and only for their own
            # current generation. Query exactly those aggregates instead of
            # scanning the whole table: the previous limit=10_000 scan was
            # O(cells x outbox) per cycle and, past that many rows, could miss
            # an active command entirely and observe a cell mid-provision.
            registering = {cell.id: cell.generation for cell in candidates if cell.state is CellState.REGISTERING}
            active_capacity_applies = {
                (command.aggregate_id, command.generation)
                for command in await transaction.list_outbox_for_aggregates("cell", registering, topic="capacity.apply")
                if self._is_active_capacity_apply(command)
            }
            cells = [
                cell
                for cell in candidates
                if not (
                    cell.state is CellState.REGISTERING
                    and (cell.provider_phase is None or (cell.id, cell.generation) in active_capacity_applies)
                )
            ]
        semaphore = asyncio.Semaphore(max(1, self.settings.cell_reconcile_concurrency))

        async def reconcile(cell: CellRecord) -> bool:
            async with semaphore:
                return await self._observe_cell_once(cell, now)

        return sum(await asyncio.gather(*(reconcile(cell) for cell in cells)))

    async def _observe_cell_once(self, cell: CellRecord, now: datetime) -> bool:
        """Fence one agent observation against its fleet and cell lifecycle changes."""

        async with self._capacity_operation(cell):
            current = await self._current_observable_cell(cell, now)
            if current is None:
                return False
            return await self._observe_cell_once_locked(current, now)

    async def _current_observable_cell(self, cell: CellRecord, now: datetime) -> CellRecord | None:
        """Return the current cell only when its original observation remains safe."""

        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if current is None or current.generation != cell.generation or current.state is not cell.state:
                return None
            if current.state not in {CellState.REGISTERING, CellState.READY, CellState.UNAVAILABLE}:
                return None
            if current.state is CellState.REGISTERING:
                # One aggregate, one generation: an indexed lookup rather than a
                # full-table scan repeated per cell.
                if any(
                    self._is_active_capacity_apply(command)
                    for command in await transaction.list_outbox_for_aggregates(
                        "cell", {current.id: current.generation}, topic="capacity.apply"
                    )
                ):
                    return None
                if current.provider_phase is None:
                    return None
            if current.availability.cooldown_until is not None and current.availability.cooldown_until > now:
                return None
            if current.pool_id is not None:
                pool = await transaction.get_capacity_pool(current.pool_id)
                if pool is not None and not pool.enabled:
                    return None
            return current

    async def _observe_cell_once_locked(self, cell: CellRecord, now: datetime) -> bool:
        """Reconcile and persist one cell without blocking unrelated cells."""

        agent_responded = False
        materialized_assets: tuple[str, ...] = cell.materialized_assets
        provider_detail: str | None = None
        provider_failure_code = cell.provider_failure_code
        unavailable_reason: str | None = (
            cell.unavailable_reason if provider_failure_code is not None else "agent_unreachable"
        )
        observed_tpu_vms: int | None = None
        try:
            mutation = MutationModel(
                idempotency_key=f"reconcile:{cell.id}:{cell.generation}",
                generation=cell.generation,
                fencing_token=f"cell:{cell.id}:{cell.generation}",
            )
            observation = await asyncio.to_thread(
                self._agent_factory(cell).reconcile,
                cell.id,
                WorkloadMutationRequest(mutation=mutation),
            )
        except CellAgentBusyError:
            logger.debug("cell %s capacity reconciliation is already active", cell.id)
            return False
        except CellAgentStaleMutationError as exc:
            logger.debug("cell %s reconciliation was superseded by a newer agent fence", cell.id)
            # Keep the agent's own words. It distinguishes three different causes
            # -- an older generation, a fencing token that does not own the
            # current one, and a tombstoned cell -- and this path used to collapse
            # all three into one sentence that says only "a fence was rejected".
            # That is the difference between an operator reading the cause and
            # inferring it from a GCS document by hand.
            await self._record_cell_agent_fence_conflict(
                cell_id=cell.id,
                generation=cell.generation,
                diagnostic=(
                    f"cell-agent reconciliation rejected the current fence: {exc}"
                    if str(exc)
                    else "cell-agent reconciliation rejected the current fence"
                ),
            )
            return False
        except CellAgentUnavailableError as exc:
            # Nothing was learned, so nothing is written. Recording this as
            # PROVIDER_ERROR would restart the provider phase clock and feed the
            # zone's belief a failure the provider never caused; see
            # CellAgentUnavailableError for the incident that motivated it.
            #
            # Leaving the record untouched is also what bounds the blast radius:
            # the cell keeps its real phase and its real clock, so a permanently
            # dead agent still retires the cell on `queue_timeout_s` /
            # `provisioning_timeout_s` exactly as it would have. Capacity cannot
            # leak because of this branch -- it can only survive a blip.
            logger.warning("cell %s could not be observed: %s", cell.id, exc)
            return False
        except CellAgentCapacityProviderError as exc:
            provider_detail = str(exc)
            provider_failure_code = exc.code
            unavailable_reason = exc.code.value
            state = CellState.UNAVAILABLE
            remote_state = CapacityStatus.PROVIDER_ERROR
            availability = self._observe_capacity_belief(cell, ProvisionOutcome.PROVIDER_ERROR)
        except Exception as exc:
            provider_detail = str(exc) or type(exc).__name__
            state = CellState.UNAVAILABLE
            remote_state = CapacityStatus.PROVIDER_ERROR
            availability = self._observe_capacity_belief(cell, ProvisionOutcome.PROVIDER_ERROR)
        else:
            agent_responded = True
            provider_detail = observation.detail or None
            remote_state = observation.state
            observed_tpu_vms = observation.observed_tpu_vms
            reported_failure_code = observation.failure_code
            materialized_assets = _reported_assets(observation.metadata)
            if observation.generation != cell.generation and remote_state not in DEFINITIVE_CAPACITY_LOSS_STATUSES:
                logger.warning(
                    "cell %s reported generation %s while generation %s is desired",
                    cell.id,
                    observation.generation,
                    cell.generation,
                )
                return False
            if remote_state in READY_CAPACITY_STATUSES:
                state = CellState.READY
                unavailable_reason = None
                provider_failure_code = None
                availability = (
                    self._observe_capacity_belief(
                        cell,
                        ProvisionOutcome.SUCCESS,
                        latency_s=max(0.0, (now - cell.created_at).total_seconds()),
                    )
                    if cell.state is not CellState.READY
                    else cell.availability
                )
            elif remote_state in PENDING_CAPACITY_STATUSES:
                state = CellState.REGISTERING
                unavailable_reason = None
                provider_failure_code = None
                availability = (
                    self._observe_capacity_belief(cell, ProvisionOutcome.INTERRUPTED)
                    if cell.state is CellState.READY
                    else cell.availability
                )
            else:
                # Every non-ready, non-pending observation used to be recorded as
                # a STOCKOUT belief unless the status was explicitly a quota
                # halt. So an ordinary spot reclamation -- the common ending on
                # this fleet -- taught the placement engine that the pool had no
                # capacity, and the next placement routed away from a zone that
                # was in fact fine. A reported reason now decides.
                state = CellState.UNAVAILABLE
                provider_failure_code = reported_failure_code or provider_failure_code
                unavailable_reason = (
                    reported_failure_code.value
                    if reported_failure_code is not None
                    # Unchanged when nothing new was reported: a code carried
                    # over from a previous observation keeps the prose that came
                    # with it, rather than being flattened into its own name.
                    else (cell.unavailable_reason if provider_failure_code is not None else remote_state.value)
                )
                availability = self._observe_capacity_belief(
                    cell,
                    provision_outcome(reported_failure_code, remote_state),
                )
        transition: tuple[CellRecord, CellRecord] | None = None
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if current is None or current.generation != cell.generation or current.state is not cell.state:
                return False
            phase_started_at = _provider_phase_start(current, remote_state, now)
            updated = current.model_copy(
                update={
                    "state": state,
                    "provider_phase": remote_state,
                    "provider_failure_code": provider_failure_code,
                    "observed_tpu_vms": observed_tpu_vms,
                    "materialized_assets": materialized_assets,
                    "provider_phase_started_at": phase_started_at,
                    "unavailable_reason": unavailable_reason,
                    "unavailable_detail": (provider_detail or None) if unavailable_reason else None,
                    "availability": availability,
                    "last_heartbeat_at": now if agent_responded else current.last_heartbeat_at,
                    "updated_at": now,
                }
            )
            await transaction.put_cell(updated)
            if provider_failure_code is CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED:
                await self._fail_autoscale_workload_for_queue_admission_denial(
                    transaction,
                    updated,
                    provider_detail or "",
                )
            transition = current, updated
        if transition is not None:
            self._capacity_lifecycle.transition(*transition, reason=provider_detail)
        return True

    async def release_ready_dag_nodes_once(self) -> int:
        """Place-enable DAG nodes whose predecessors have all succeeded.

        A gated node is stored ``schedulable=False``, which the scheduler already
        filters on, so releasing it is a single flag flip rather than a second
        admission concept.

        A predecessor that FAILED or was CANCELLED cancels its dependents instead of
        stranding them. Leaving them PENDING forever is the same defect the orphan
        sweep exists to clear -- a workload nothing will ever settle, counted against
        quota and fair share while holding nothing -- and CANCELLED states the truth:
        this never ran because what it waited for did not succeed.
        """

        released = 0
        async with self.repository.transaction() as transaction:
            gated = [
                workload
                for workload in await transaction.list_workloads(state=WorkloadState.PENDING)
                if workload.depends_on and not workload.schedulable
            ]
            predecessor_states = {
                workload.id: workload.state
                for workload in await transaction.list_workloads()
                if any(workload.id in node.depends_on for node in gated)
            }

        for node in gated:
            states = [predecessor_states.get(predecessor) for predecessor in node.depends_on]
            if any(state in {WorkloadState.FAILED, WorkloadState.CANCELLED} or state is None for state in states):
                # A missing predecessor counts as failed: it was pruned or lost, and
                # a dependent that can never learn its outcome must not wait forever.
                try:
                    await self.service.cancel_workload(
                        self._principal,
                        node.id,
                        idempotency_key=f"dag:abandon:{node.id}:g{node.generation}",
                    )
                except (ConflictError, NotFoundError) as exc:
                    logger.warning("could not abandon DAG node %s: %s", node.id, exc)
                    continue
                released += 1
                continue
            if not all(state is WorkloadState.SUCCEEDED for state in states):
                continue
            async with self.repository.transaction() as transaction:
                current = await transaction.get_workload(node.id)
                if current is None or current.schedulable or current.state is not WorkloadState.PENDING:
                    continue
                await transaction.put_workload(
                    current.model_copy(
                        update={
                            "schedulable": True,
                            "scheduling_reason": "dependencies satisfied; waiting for scheduling",
                            "updated_at": utcnow(),
                        }
                    )
                )
                await transaction.append_event(
                    AuditEvent(
                        id=f"evt_{uuid.uuid4().hex}",
                        actor=self._principal.subject,
                        org_id=current.org_id,
                        project_id=current.project_id,
                        entity_type="workload",
                        entity_id=current.id,
                        action="workload.dag_released",
                    )
                )
            released += 1
        return released

    async def release_admitted_groups_once(self) -> int:
        """Release a heterogeneous group once every member can be placed.

        A group's members are differently shaped, so they cannot be one gang. Left
        as independent workloads, the members that fit lease immediately and burn
        accelerator hours for as long as the members that do not stay unplaceable.
        Gating them all on the whole group's feasibility is the one thing groups buy
        over siblings.

        The check is a *feasibility* gate, not a reservation, and the difference is
        worth stating where it is implemented: between this answer and the members'
        actual leases another workload can take the capacity, because the core
        scheduler has no reservation primitive. What this prevents is half a group
        holding accelerators while the rest is structurally infeasible -- the common
        failure -- not a race between two placement passes.

        Members are evaluated as if schedulable, since the scheduler filters out the
        flag this method exists to flip; and they are released in one transaction, so
        a leader that dies mid-release cannot leave a group half-open.
        """

        released = 0
        async with self.repository.transaction() as transaction:
            coordinators = [
                workload
                for workload in await transaction.list_workloads(state=WorkloadState.PENDING)
                if workload.coordination_kind is CoordinationKind.GROUP
            ]
            members_by_parent = {
                coordinator.id: await transaction.list_child_workloads(coordinator.id) for coordinator in coordinators
            }

        for coordinator in coordinators:
            members = members_by_parent.get(coordinator.id, [])
            gated = [member for member in members if not member.schedulable]
            if not gated or any(member.state is not WorkloadState.PENDING for member in gated):
                continue
            feasible = await self.service.group_is_placeable(
                self._principal, member_ids=tuple(member.id for member in gated)
            )
            if not feasible:
                continue
            async with self.repository.transaction() as transaction:
                current = [await transaction.get_workload(member.id) for member in gated]
                if any(item is None or item.schedulable or item.state is not WorkloadState.PENDING for item in current):
                    # State moved under us. Re-evaluate next cycle rather than
                    # releasing a partial group, which is the exact outcome the
                    # gate exists to prevent.
                    continue
                for item in current:
                    assert item is not None
                    await transaction.put_workload(
                        item.model_copy(
                            update={
                                "schedulable": True,
                                "scheduling_reason": "group admitted; waiting for scheduling",
                                "updated_at": utcnow(),
                            }
                        )
                    )
                    await transaction.append_event(
                        AuditEvent(
                            id=f"evt_{uuid.uuid4().hex}",
                            actor=self._principal.subject,
                            org_id=item.org_id,
                            project_id=item.project_id,
                            entity_type="workload",
                            entity_id=item.id,
                            action="workload.group_released",
                            payload={"group_id": coordinator.id, "members": len(current)},
                        )
                    )
                released += len(current)
        return released

    async def workload_logs(self, workload_id: str) -> str:
        """Read driver logs for an active or terminal workload attempt."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
        if workload is None:
            raise LookupError(f"workload {workload_id!r} does not exist")
        if workload.remote_id is None:
            return ""
        members = await self._load_workload_members(workload)
        if len(members) > 1:
            coordinator_lease, coordinator_cell = self._ordered_gang_member_pairs(members)[0]
            return await asyncio.to_thread(
                self._agent_factory(coordinator_cell).gang_logs,
                coordinator_lease.cell_id,
                workload.id,
                workload.remote_id,
            )
        _lease, cell = members[0]
        return await asyncio.to_thread(self._agent_factory(cell).logs, cell.id, workload.remote_id)

    async def workload_log_page(
        self,
        workload_id: str,
        *,
        after: str = "",
        limit: int = 200,
    ) -> JsonObject:
        """Return one resumable page of structured logs for a workload attempt.

        The cursor is opaque to the control plane except for one thing it must
        know: which attempt produced it. ``after`` is ``"{attempt}:{cursor}"``,
        and a cursor from a previous attempt is *not* resumed from -- it is
        reported as a change and the read restarts.

        Silently continuing is the failure that matters here. A retry replaces
        the remote execution entirely, so the old cursor means nothing to the
        new one; a follower that kept using it would be shown the new attempt's
        first lines appended to the old attempt's tail, and would conclude the
        job had resumed where it left off.
        """

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
        if workload is None:
            raise LookupError(f"workload {workload_id!r} does not exist")
        if workload.remote_id is None:
            return _log_page_document(LogPage(), attempt=workload.attempt)

        requested_attempt, cursors = _split_log_cursor(after)
        attempt_changed = requested_attempt is not None and requested_attempt != workload.attempt
        if attempt_changed:
            # A retry runs a different execution; the old positions name lines
            # in journals that no longer exist.
            cursors = {}

        try:
            members = await self._load_workload_members(workload)
        except StaleOutboxCommand:
            # No current lease: the cell is released and its journal went with
            # it. This is the ordinary path for any finished workload, not an
            # error -- and falling back to the archive is the entire reason the
            # archive exists.
            members = ()
        if not members:
            return _log_page_document(
                await self._archived_log_page(workload),
                attempt=workload.attempt,
                attempt_changed=attempt_changed,
            )
        # ``_ordered_gang_member_pairs`` validates a *gang* -- it refuses fewer
        # than two cells -- so a single-cell workload uses its one member
        # directly. The gang path gets the deterministic cell ordering that
        # makes rank N the same host on every read.
        ordered = self._ordered_gang_member_pairs(members) if len(members) > 1 else tuple(members)
        pages: dict[int, LogPage] = {}
        for rank, (_lease, cell) in enumerate(ordered):
            try:
                payload = await asyncio.to_thread(
                    self._agent_factory(cell).log_page,
                    cell.id,
                    workload.remote_id,
                    # This rank's own position. Handing it another rank's cursor
                    # would ask journald to resume from an entry it never wrote.
                    after=cursors.get(rank, ""),
                    limit=limit,
                )
            except Exception as exc:
                # One unreachable rank must not blank the ranks that answered.
                # ``merge_pages`` reports it in place, which is the whole reason
                # ``LogPage`` distinguishes "unavailable" from "empty".
                pages[rank] = LogPage(unavailable=True, detail=str(exc))
                continue
            pages[rank] = _log_page_from_document(payload, rank=rank)
        # Captured before the merge, which deliberately drops them: there is no
        # single cursor for a slice, so the per-rank positions are what a
        # follower needs back.
        advanced = {rank: page.next_cursor or cursors.get(rank, "") for rank, page in pages.items()}
        return _log_page_document(
            merge_pages(pages) if pages else LogPage(),
            attempt=workload.attempt,
            attempt_changed=attempt_changed,
            cursors=advanced,
        )

    async def _archived_log_page(self, workload: WorkloadRecord) -> LogPage:
        """Read one attempt's archived logs back out of managed storage.

        Returns an ``unavailable`` page rather than an empty one when the
        archive cannot be read: after a cell is released there is no other
        source, so "nothing here" and "could not look" are the difference
        between a job that printed nothing and one whose record was lost.
        """

        if self.artifact_access is None:
            return LogPage(unavailable=True, detail="managed artifacts are not configured on this replica")
        records: list[LogRecord] = []
        found = False
        for rank in range(_ARCHIVED_RANK_SCAN_LIMIT):
            try:
                payload = await self.service.read_managed_artifact(
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    uri=log_uri(workload.id, workload.attempt, rank),
                )
            except NotFoundError:
                # Ranks are archived contiguously, so a proved absence is the end.
                break
            except Exception:
                return LogPage(unavailable=True, detail="archived logs could not be read conclusively")
            found = True
            for line in payload.decode(errors="replace").splitlines():
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                if isinstance(entry, dict):
                    records.append(
                        LogRecord(
                            message=str(entry.get("message", "")),
                            stream=str(entry.get("stream", "unknown")),  # type: ignore[arg-type]
                            cursor=str(entry.get("cursor", "")),
                            rank=rank,
                            pid=entry.get("pid") if isinstance(entry.get("pid"), int) else None,
                        )
                    )
        if not found:
            return LogPage(unavailable=True, detail="this attempt has no archived logs")
        return LogPage(records=tuple(records), detail="archived")

    async def workload_exec(
        self,
        workload_id: str,
        command: tp.Sequence[str],
        *,
        timeout_s: float = 30.0,
        rank: int | None = None,
    ) -> JsonObject:
        """Run one diagnostic command beside every rank of a workload.

        Fan-out is the default because the question exec answers is almost
        always "which rank is stuck", and asking one rank cannot distinguish a
        hung collective from a slow step. ``rank`` narrows it once the answer is
        known.

        One rank failing does not fail the call: a result set missing the rank
        you most wanted to see is still more useful than an exception, and the
        failure is reported in place beside the ranks that answered.
        """

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
        if workload is None:
            raise LookupError(f"workload {workload_id!r} does not exist")
        if workload.remote_id is None:
            raise ConflictError(f"workload {workload_id!r} is not running on any cell")
        try:
            members = await self._load_workload_members(workload)
        except StaleOutboxCommand as exc:
            # A terminal workload keeps its remote_id but loses its lease, so
            # "has a remote id" is not "is still somewhere". Letting the
            # internal staleness error escape turned an ordinary race -- the job
            # finished between the read and the exec -- into a 500.
            raise ConflictError(f"workload {workload_id!r} no longer holds a cell: {exc}") from exc
        ordered = self._ordered_gang_member_pairs(members) if len(members) > 1 else tuple(members)
        results: list[JsonObject] = []
        for index, (_lease, cell) in enumerate(ordered):
            if rank is not None and index != rank:
                continue
            try:
                payload = await asyncio.to_thread(
                    self._agent_factory(cell).exec_in_workload,
                    cell.id,
                    workload.remote_id,
                    list(command),
                    timeout_s=timeout_s,
                )
            except Exception as exc:
                results.append({"rank": index, "cell_id": cell.id, "error": str(exc)})
                continue
            results.append({**payload, "rank": index, "cell_id": cell.id})
        if rank is not None and not results:
            raise ConflictError(f"workload {workload_id!r} has no rank {rank}")
        document: JsonObject = {
            "workload_id": workload_id,
            "attempt": workload.attempt,
            "results": list(results),
        }
        return document

    async def workload_process_status(self, workload_id: str) -> JsonObject:
        """Read live process detail for one workload attempt.

        For a gang this reports the coordinator cell, whose slice executor
        already fans out across every rank -- which is the view that makes a
        hung collective legible.
        """

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
        if workload is None:
            raise LookupError(f"workload {workload_id!r} does not exist")
        if workload.remote_id is None:
            return {"workload_id": workload_id, "observed": False, "detail": "workload has no dispatched attempt"}
        members = await self._load_workload_members(workload)
        lease, cell = self._ordered_gang_member_pairs(members)[0] if len(members) > 1 else members[0]
        return await asyncio.to_thread(
            self._agent_factory(cell).process_status,
            lease.cell_id,
            workload.remote_id,
        )


def _reported_assets(metadata: tp.Mapping[str, object] | None) -> tuple[str, ...]:
    """Read an agent's materialized-asset report out of its observation metadata.

    Carried in the existing free-form metadata rather than as a new wire field,
    so an agent that never learns to report keeps validating and a control plane
    that predates the convention keeps parsing. Locality is a preference either
    way, and a new required field would be a deployment ordering problem in
    exchange for nothing.

    Anything that is not a list of non-empty strings is ignored rather than
    rejected. This runs on the observation hot path, and refusing an otherwise
    good observation over a malformed hint would trade a cache miss for an
    unobservable cell.
    """

    if not isinstance(metadata, dict):
        return ()
    reported = metadata.get("materialized_assets")
    if not isinstance(reported, list):
        return ()
    names = [value for value in reported if isinstance(value, str) and value.strip()]
    # Deduplicated and ordered, so two agents reporting the same set produce the
    # same index and placement does not depend on report ordering.
    return tuple(sorted(dict.fromkeys(name.strip() for name in names)))
