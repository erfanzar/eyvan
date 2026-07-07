# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Terminal release, billing, retention, and what capacity failures teach."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections import Counter
from datetime import timedelta

from eyvan._internal.json_types import JsonObject
from eyvan.artifacts.managed import ManagedArtifactKind, log_uri
from eyvan.capacity import (
    CapacityStatus,
)
from eyvan.capacity.drivers.base import CapacityProviderFailureCode, provision_outcome
from eyvan.domain import (
    CapacityBelief,
    CapacityPoolKey,
    ProvisionOutcome,
)
from eyvan.records import (
    AuditEvent,
    AutoscaleBlockReason,
    CapacityBeliefState,
    CellRecord,
    CellState,
    CoordinationKind,
    GcpQueueAdmissionDenialTarget,
    LeaseRecord,
    LeaseState,
    OutboxRecord,
    WorkloadRecord,
    WorkloadState,
    utcnow,
)
from eyvan.server.controller._support import (
    _LIVE_WORKLOAD_STATES,
    StaleOutboxCommand,
    _provider_phase_start,
    logger,
)
from eyvan.server.persistence.base import RepositorySession
from eyvan.server.service.errors import AuthorizationError, ConflictError, InputError, NotFoundError
from eyvan.server.service.provider_diagnostics import safe_provider_diagnostic
from eyvan.server.service.services import CHILD_REVISION, ServicePolicy, plan_service, service_policy
from eyvan.server.service.state import ACTIVE_LEASE_STATES, TERMINAL_WORKLOAD_STATES

#: Lines archived per attempt. A ceiling rather than everything, because the
#: archive is written inside the release path: an unbounded read would make
#: settling a chatty job slow enough to matter, and the last lines are what
#: explain a failure. Truncation is visible -- the page reports it.
_LOG_ARCHIVE_LINE_LIMIT = 1000


class _ControllerSettlement:
    """Terminal release, billing, retention, and what capacity failures teach.

    A concern of :class:`~eyvan.server.controller.EyvanController`; not
    instantiated on its own.
    """

    async def _release_terminal(
        self,
        workload: WorkloadRecord,
        lease: LeaseRecord,
        target: WorkloadState,
        action: str,
        *,
        failure_reason: str | None = None,
    ) -> None:
        if workload.lease_id != lease.id:
            raise StaleOutboxCommand(f"lease {lease.id!r} no longer owns workload {workload.id!r}")
        # Archive before settling. Settlement releases the lease, and releasing
        # a cell destroys its journal -- so the output disappears exactly when
        # the capacity bill stops, which is also when someone starts asking what
        # happened. Failures here are swallowed on purpose: a job that ran must
        # still be recorded as finished even if its logs could not be saved.
        await self._archive_terminal_logs(workload)
        await self.service.settle_terminal_workload(
            self._principal,
            workload.id,
            target,
            action=action,
            failure_reason=failure_reason,
            expected_generation=workload.generation,
            expected_lease_id=lease.id,
            expected_state=workload.state,
        )

    async def _archive_terminal_logs(self, workload: WorkloadRecord) -> None:
        """Copy one attempt's structured logs into managed storage.

        Best effort, and deliberately so. The alternative -- letting an archive
        failure abort settlement -- would leave a finished workload holding its
        lease because its logs were unreadable, turning a diagnostic gap into a
        stranded accelerator.

        Skipped entirely when managed artifacts are not configured, which is the
        local development case: there is nowhere to put them and the journal is
        on the same machine anyway.
        """

        if self.artifact_access is None or workload.remote_id is None or workload.spec.secret_mounts:
            # Durable secret-bearing executions intentionally have no archived
            # output: exact streaming redaction cannot be reconstructed after a
            # worker restart without persisting the plaintext values themselves.
            return
        try:
            page = await self.workload_log_page(workload.id, limit=_LOG_ARCHIVE_LINE_LIMIT)
        except Exception:
            logger.warning("could not read logs for %s before release", workload.id, exc_info=True)
            return
        records = page.get("records")
        if not isinstance(records, list) or not records:
            return
        by_rank: dict[int, list[JsonObject]] = {}
        for entry in records:
            if not isinstance(entry, dict):
                continue
            rank = entry.get("rank")
            by_rank.setdefault(rank if isinstance(rank, int) else 0, []).append(entry)
        for rank, lines in sorted(by_rank.items()):
            # One object per rank, JSON lines: the same shape the live page
            # returns, so a reader does not need a second parser for archived
            # logs than for streaming ones.
            payload = "\n".join(json.dumps(line, sort_keys=True) for line in lines).encode()
            uri = log_uri(workload.id, workload.attempt, rank)
            try:
                await asyncio.to_thread(
                    self.artifact_access.store.put,
                    uri,
                    payload,
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                )
            except Exception:
                logger.warning("could not archive logs for %s rank %d", workload.id, rank, exc_info=True)

    async def _record_capacity_stockout(self, cell: CellRecord) -> None:
        """Persist a STOCKOUT belief for a cell reclaimed before it ever became READY.

        Guarded on the cell's generation so a concurrent recreate does not have its
        fresh belief overwritten by this late write.
        """

        availability = self._observe_capacity_belief(cell, ProvisionOutcome.STOCKOUT)
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if current is None or current.generation != cell.generation:
                return
            await transaction.put_cell(current.model_copy(update={"availability": availability, "updated_at": utcnow()}))

    @staticmethod
    def _observe_capacity_belief(
        cell: CellRecord,
        outcome: ProvisionOutcome,
        *,
        latency_s: float | None = None,
    ) -> CapacityBeliefState:
        current = cell.availability
        belief = CapacityBelief(
            key=CapacityPoolKey(
                provider=cell.provider,
                project=cell.provider_project or "",
                zone=cell.zone or cell.region,
                accelerator_type=cell.accelerator_type or "unknown",
                capacity_class=cell.capacity_class.value,
            ),
            success_evidence=current.success_evidence,
            failure_evidence=current.failure_evidence,
            provision_latencies_s=current.provision_latencies_s,
            interruptions=current.interruptions,
            successful_hours=current.successful_hours,
            consecutive_failures=current.consecutive_failures,
            cooldown_until=current.cooldown_until,
            last_observed_at=current.last_observed_at,
        ).observe(outcome, latency_s=latency_s)
        return CapacityBeliefState(
            success_evidence=belief.success_evidence,
            failure_evidence=belief.failure_evidence,
            provision_latencies_s=belief.provision_latencies_s,
            interruptions=belief.interruptions,
            successful_hours=belief.successful_hours,
            consecutive_failures=belief.consecutive_failures,
            cooldown_until=belief.cooldown_until,
            last_observed_at=belief.last_observed_at,
        )

    async def _record_capacity_failure(
        self,
        command: OutboxRecord,
        error: str,
        *,
        failure_code: CapacityProviderFailureCode | None,
    ) -> None:
        transition: tuple[CellRecord, CellRecord] | None = None
        async with self.repository.transaction() as transaction:
            cell = await transaction.get_cell(command.aggregate_id)
            if cell is None or cell.generation != command.generation or cell.state is not CellState.REGISTERING:
                return
            # Was: search ``error`` for the words "quota" and "capacity". That
            # read a provider's arbitrary prose as a classification, so
            # "capacity pool not found" -- a configuration mistake -- became
            # evidence that a zone had run out of accelerators, and fed it to
            # the belief that decides where the next workload is placed. The
            # provider now says which of these it means.
            outcome = provision_outcome(failure_code)
            observed_at = utcnow()
            unavailable_reason = failure_code.value if failure_code is not None else "provider_failure"
            updated = cell.model_copy(
                update={
                    "state": CellState.UNAVAILABLE,
                    "provider_phase": CapacityStatus.PROVIDER_ERROR,
                    "provider_failure_code": failure_code,
                    "provider_phase_started_at": _provider_phase_start(
                        cell,
                        CapacityStatus.PROVIDER_ERROR,
                        observed_at,
                    ),
                    "unavailable_reason": unavailable_reason,
                    "availability": self._observe_capacity_belief(cell, outcome),
                    "updated_at": observed_at,
                }
            )
            await transaction.put_cell(updated)
            if failure_code is CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED:
                await self._fail_autoscale_workload_for_queue_admission_denial(transaction, updated, error)
            transition = cell, updated
        if transition is not None:
            self._capacity_lifecycle.transition(*transition, reason=error)

    @staticmethod
    async def _has_alternative_pool(
        transaction: RepositorySession,
        workload: WorkloadRecord,
        *,
        denied_cell: CellRecord,
    ) -> bool:
        """Return whether another enabled pool could still host this workload.

        Deliberately coarse: it asks whether an *enabled pool of a different
        shape* exists that the workload's declared portfolio admits, not whether
        that pool has capacity right now. Capacity is what the next reconcile
        cycle decides, and predicting it here would duplicate the planner while
        being wrong more often.
        """

        spec = workload.spec
        classes = {entry.value for entry in spec.capacity_class_portfolio}
        accelerators = set(spec.allowed_accelerator_types) | (
            {spec.accelerator_type} if spec.accelerator_type else set()
        )
        regions = set(spec.allowed_regions) | ({spec.region} if spec.region else set())
        for pool in await transaction.list_capacity_pools():
            if not pool.enabled or pool.id == denied_cell.pool_id:
                continue
            if str(pool.capacity_class) not in classes:
                continue
            if accelerators and pool.accelerator_type not in accelerators:
                continue
            if regions and pool.region not in regions:
                continue
            return True
        return False

    async def _fail_autoscale_workload_for_queue_admission_denial(
        self,
        transaction: RepositorySession,
        cell: CellRecord,
        diagnostic: str,
    ) -> None:
        """Block the pending workload that owns a denied queue request.

        **Terminal only when this pool was the last option.** A denial says this
        *shape in this zone* is unavailable; with a portfolio the workload has
        other admissible pools, and failing it outright discards a run the fleet
        could still place. When another pool remains, the workload stays PENDING
        with ``autoscale_block_reason`` set for explainability, and the ordinary
        reconcile cycle tries the next-best pool -- which is machinery that
        already ships, because ``cell_provides_pool_supply`` stops counting a
        failed cell as supply and ``CapacitySimulation.build`` already generates a
        candidate per admissible pool.
        """

        workload_id = cell.autoscale_request_workload_id
        if workload_id is None:
            return
        workload = await transaction.get_workload(workload_id)
        if workload is None or workload.state is not WorkloadState.PENDING:
            return
        failure_reason = safe_provider_diagnostic(diagnostic)
        if await self._has_alternative_pool(transaction, workload, denied_cell=cell):
            # Not terminal. Record why this attempt stalled so ``workload explain``
            # can say "one pool refused, others remain" rather than going quiet.
            await transaction.put_workload(
                workload.model_copy(
                    update={
                        "autoscale_block_reason": AutoscaleBlockReason.QUEUE_ADMISSION_DENIED,
                        "scheduling_reason": (f"{failure_reason}; other admissible pools remain, waiting to retry"),
                        "updated_at": utcnow(),
                    }
                )
            )
            return
        project = cell.provider_project
        zone = cell.zone
        accelerator_type = cell.accelerator_type
        target = (
            None
            if cell.provider != "gcp" or project is None or zone is None or accelerator_type is None
            else GcpQueueAdmissionDenialTarget(
                provider_project=project,
                zone=zone,
                accelerator_type=accelerator_type,
                capacity_class=cell.capacity_class,
            )
        )
        await self.service._transition_workload(
            transaction,
            self._principal,
            workload,
            WorkloadState.FAILED,
            "workload.queue_admission_denied",
            updates={
                "failure_reason": failure_reason,
                "autoscale_block_reason": AutoscaleBlockReason.QUEUE_ADMISSION_DENIED,
                "queue_admission_denial": target,
                "scheduling_reason": None,
            },
        )

    async def _record_cell_agent_fence_conflict(
        self,
        *,
        cell_id: str,
        generation: int,
        diagnostic: str,
    ) -> None:
        """Mark a cell for replacement without treating an agent fence conflict as cloud failure."""

        transition: tuple[CellRecord, CellRecord] | None = None
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell_id)
            if (
                current is None
                or current.generation != generation
                or current.state not in {CellState.REGISTERING, CellState.READY, CellState.UNAVAILABLE}
            ):
                return
            observed_at = utcnow()
            updated = current.model_copy(
                update={
                    "state": CellState.UNAVAILABLE,
                    "provider_phase": CapacityStatus.NEEDS_RECREATE,
                    "provider_phase_started_at": _provider_phase_start(
                        current,
                        CapacityStatus.NEEDS_RECREATE,
                        observed_at,
                    ),
                    "unavailable_reason": "agent_fence_conflict",
                    "unavailable_detail": diagnostic[:2048],
                    "availability": self._observe_capacity_belief(current, ProvisionOutcome.INTERRUPTED),
                    "updated_at": observed_at,
                }
            )
            await transaction.put_cell(updated)
            transition = current, updated
        if transition is not None:
            self._capacity_lifecycle.transition(*transition, reason=diagnostic)
        if transition is None:
            return
        _previous, unavailable = transition
        if unavailable.pool_id is not None:
            return
        try:
            recreated, replayed = await self.service.recreate_cell(
                self._principal,
                unavailable.id,
                expected_generation=unavailable.generation,
                idempotency_key=f"agent-fence-recreate:{unavailable.id}:g{unavailable.generation}",
            )
        except (ConflictError, NotFoundError) as exc:
            logger.info(
                "cell %s will wait for lease-safe recreation after an agent fence conflict: %s", unavailable.id, exc
            )
            return
        if not replayed:
            self._capacity_lifecycle.transition(
                unavailable,
                recreated,
                reason="cell-agent fence conflict requested a fresh capacity generation",
            )

    async def accrue_cell_billing_once(self) -> int:
        """Advance the fleet's cell billing clocks and return the cells charged.

        Delegates the arithmetic to the service so the accrual runs in one
        transaction with the lease read that decides which cells were idle -- a
        lease released between the two reads would otherwise make a busy cell
        look idle for the whole interval.
        """

        accrual = await self.service.accrue_cell_billing()
        if accrual.idle_seconds > 0:
            logger.debug(
                "Accrued %.0fs across %d cells, %.0fs idle (%.4f idle cost)",
                accrual.accrued_seconds,
                accrual.cells,
                accrual.idle_seconds,
                accrual.idle_cost,
            )
        return accrual.cells

    async def settle_orphaned_workloads_once(self) -> int:
        """Settle a live workload whose lease or cell row no longer exists.

        ``observe_workloads_once`` skips such a workload -- it cannot observe what it
        cannot resolve -- so without this it stays RUNNING forever: never polled,
        never settled, and counted against its project's quota, budget and fair share
        while holding no capacity at all. One hand-repaired or lost row therefore
        permanently shrinks a tenant's effective quota.

        Guarded by a grace period rather than firing on the first miss. Nothing should
        write a workload and its lease in separate transactions -- dispatch commits
        both together -- but settling a workload is irreversible, so requiring the
        condition to persist past the lease TTL costs one cycle and removes the whole
        class of "a race made this look orphaned".
        """

        grace = timedelta(seconds=max(self.settings.lease_ttl_s, 1))
        now = utcnow()
        async with self.repository.transaction() as transaction:
            live = [
                workload
                for workload in await transaction.list_workloads()
                if workload.state in _LIVE_WORKLOAD_STATES and now - workload.updated_at > grace
            ]
            lease_ids = {lease.id for lease in await transaction.list_leases()}
            cell_ids = {cell.id for cell in await transaction.list_cells()}
            leases_by_id = {lease.id: lease for lease in await transaction.list_leases()}

        orphans: list[tuple[WorkloadRecord, str]] = []
        for workload in live:
            members = self._workload_lease_ids(workload)
            if not members:
                continue
            missing_leases = sorted(set(members) - lease_ids)
            if missing_leases:
                orphans.append((workload, f"lease {missing_leases[0]!r} no longer exists"))
                continue
            missing_cells = sorted(
                {leases_by_id[lease_id].cell_id for lease_id in members} - cell_ids,
            )
            if missing_cells:
                orphans.append((workload, f"cell {missing_cells[0]!r} no longer exists"))

        settled = 0
        for workload, reason in orphans:
            try:
                await self.service.settle_terminal_workload(
                    self._principal,
                    workload.id,
                    WorkloadState.FAILED,
                    action="workload.orphaned",
                    failure_reason=f"workload is unobservable: {reason}",
                    expected_generation=workload.generation,
                    expected_state=workload.state,
                    settle_without_leases=True,
                )
            except (ConflictError, NotFoundError) as exc:
                # It recovered or moved on between the scan and the write; the next
                # cycle re-evaluates from fresh state. Logged rather than silently
                # skipped: a sweep that exists to clear stuck workloads must not be
                # able to fail quietly at the one step that clears them.
                logger.warning("orphan sweep could not settle workload %s: %s", workload.id, exc)
                continue
            settled += 1
        return settled

    async def settle_orphaned_leases_once(self) -> int:
        """Release leases that no live workload claims.

        ``settle_orphaned_workloads_once`` settles by *workload*, so a lease its
        workload stopped referencing -- because the workload finished, vanished,
        or retried onto different capacity -- is reachable from no path at all. It
        keeps a cell allocated and its cost unsettled forever: capacity the fleet
        pays for and no tenant is charged for, which is the one leak an operator
        cannot see in any existing view.

        The same grace period the workload sweep uses, for the same reason.
        Nothing should write a lease and its workload in separate transactions,
        but releasing a lease that a dispatch is mid-way through attaching would
        kill live work, and one cycle of patience removes the entire class of
        "a race made this look orphaned".
        """

        grace = timedelta(seconds=max(self.settings.lease_ttl_s, 1))
        now = utcnow()
        async with self.repository.transaction() as transaction:
            leases = await transaction.list_leases()
            workloads = {workload.id: workload for workload in await transaction.list_workloads()}
        candidates = [
            lease
            for lease in leases
            if lease.state in ACTIVE_LEASE_STATES
            and now - lease.updated_at > grace
            and self.service._lease_is_orphaned(lease, workloads.get(lease.workload_id))
        ]

        settled = 0
        for lease in candidates:
            owner = workloads.get(lease.workload_id)
            reason = "orphaned: workload no longer exists" if owner is None else "orphaned: workload released it"
            try:
                if await self.service.settle_orphaned_lease(
                    self._principal,
                    lease.id,
                    reason=reason,
                    expected_generation=lease.generation,
                ):
                    settled += 1
            except (ConflictError, NotFoundError) as exc:
                # It was re-attached or settled between the scan and the write;
                # the next cycle re-evaluates. Logged rather than swallowed: a
                # sweep whose whole purpose is reclaiming capacity must not be
                # able to fail quietly at the step that reclaims it.
                logger.warning("orphan sweep could not settle lease %s: %s", lease.id, exc)
        return settled

    async def quarantine_expired_leases_once(self) -> int:
        """Fence expired leases without killing work or reallocating ambiguous capacity."""

        now = utcnow()
        changed = 0
        async with self.repository.transaction() as transaction:
            candidate_ids = [
                lease.id
                for lease in await transaction.list_leases()
                if lease.state in {LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.REVOKING}
                and lease.expires_at <= now
            ]
        for lease_id in candidate_ids:
            async with self.repository.transaction() as transaction:
                lease = await transaction.get_lease(lease_id)
                if lease is None:
                    continue
                if lease.state not in {LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.REVOKING}:
                    continue
                if lease.expires_at > now:
                    continue
                await transaction.put_lease(
                    lease.model_copy(update={"state": LeaseState.QUARANTINED, "updated_at": now})
                )
                await transaction.append_event(
                    AuditEvent(
                        id=f"evt_{uuid.uuid4().hex}",
                        actor=self._principal.subject,
                        org_id=lease.org_id,
                        project_id=lease.project_id,
                        entity_type="lease",
                        entity_id=lease.id,
                        action="lease.quarantined",
                        previous_state=lease.state.value,
                        new_state=LeaseState.QUARANTINED.value,
                    )
                )
                changed += 1
        return changed

    async def settle_coordinator_workloads_once(self) -> int:
        """Derive a batch parent's terminal state from its children.

        A coordinator never holds a lease, so nothing else can ever settle it --
        it would sit PENDING forever while all 500 of its tasks succeeded, and
        every listing would report the array as still queued.

        The mapping is deliberately pessimistic: one failed task fails the
        array, because a partial array is not a result. A cancelled task without
        any failure reports CANCELLED, which is what the cascade produced.
        """

        settled = 0
        async with self.repository.transaction() as transaction:
            # Filtered on the indexed state column rather than scanning every
            # workload the fleet has ever run. This is a leader hot-path phase, and
            # the terminal records it used to read back are the fastest-growing table
            # in the system. Narrowing ``coordination_kind`` itself needs a typed
            # column and is deferred to the coordination-columns migration; PENDING
            # alone already removes the unbounded part.
            candidate_ids = [
                workload.id
                for workload in await transaction.list_workloads(state=WorkloadState.PENDING)
                # A service is deliberately excluded. Its children are
                # periodically all-terminal -- that is what a restart *is* -- so
                # this settler would declare it SUCCEEDED at the first moment
                # every replica happened to be between lives, and it would stop
                # forever. The failure would look like a service that quietly
                # went away, with a terminal state nobody asked for.
                if workload.coordination_kind is not None and workload.coordination_kind is not CoordinationKind.SERVICE
            ]
        for workload_id in candidate_ids:
            async with self.repository.transaction() as transaction:
                parent = await transaction.get_workload(workload_id)
                if (
                    parent is None
                    or parent.coordination_kind is None
                    or parent.coordination_kind is CoordinationKind.SERVICE
                    or parent.state is not WorkloadState.PENDING
                ):
                    continue
                children = await transaction.list_child_workloads(workload_id)
                if not children or any(child.state not in TERMINAL_WORKLOAD_STATES for child in children):
                    continue
                states = {child.state for child in children}
                if WorkloadState.FAILED in states:
                    target = WorkloadState.FAILED
                elif WorkloadState.CANCELLED in states:
                    target = WorkloadState.CANCELLED
                else:
                    target = WorkloadState.SUCCEEDED
                await transaction.put_workload(
                    parent.model_copy(
                        update={
                            "state": target,
                            "scheduling_reason": (
                                f"{parent.coordination_kind.value} of {len(children)} members settled"
                            ),
                            "updated_at": utcnow(),
                        }
                    )
                )
                await transaction.append_event(
                    AuditEvent(
                        id=f"evt_{uuid.uuid4().hex}",
                        actor=self._principal.subject,
                        org_id=parent.org_id,
                        project_id=parent.project_id,
                        entity_type="workload",
                        entity_id=parent.id,
                        action="workload.batch_settled",
                        previous_state=parent.state.value,
                        new_state=target.value,
                    )
                )
                settled += 1
        return settled

    async def export_metrics_once(self) -> int:
        """Publish fleet state, capacity inventory, and spend to Prometheus.

        Four of the seven metric families were declared, exported at ``/metrics``,
        and written by nothing -- so fleet state, capacity inventory and cost,
        the three things an operator actually dashboards, always read zero. A
        gauge that is always zero is worse than an absent one: it looks like an
        answer.

        Runs on its own slow interval rather than inside the scheduler, because
        gauges describe a level rather than a rate and re-reading the snapshot on
        every scheduling pass would buy nothing.
        """

        if self.metrics is None:
            return 0
        async with self.repository.transaction() as transaction:
            workloads = await transaction.list_workloads()
            cells = await transaction.list_cells()
            budgets = await transaction.list_budgets()

        by_state: Counter[str] = Counter(workload.state.value for workload in workloads)
        self.metrics.workloads.clear()
        for state, count in by_state.items():
            self.metrics.workloads.labels(state=state).set(count)

        accelerators: Counter[tuple[str, str, str]] = Counter()
        for cell in cells:
            if cell.state is CellState.DELETED:
                continue
            key = (cell.state.value, str(cell.capacity_class), cell.accelerator_type or "none")
            accelerators[key] += int(cell.capacity.gpu) + int(cell.capacity.tpu)
        self.metrics.capacity.clear()
        for (state, capacity_class, accelerator_type), total in accelerators.items():
            self.metrics.capacity.labels(
                state=state,
                capacity_class=capacity_class,
                accelerator_type=accelerator_type,
            ).set(total)

        self.metrics.project_spend.clear()
        for budget in budgets:
            self.metrics.project_spend.labels(
                org=budget.org_id,
                project=budget.project_id or "",
                kind="spent",
            ).set(float(budget.spent))

        # Progress series are written by the route as reports arrive, not from
        # this snapshot -- reading them would be one query per live workload. So
        # this phase prunes rather than rebuilds: a workload that has reached a
        # terminal state can report nothing further, and its series would
        # otherwise outlive it for the process's lifetime. ``workload`` is the
        # one unbounded label in the whole metric set, which is why the pruning
        # is not optional.
        live = {workload.id for workload in workloads if workload.state not in TERMINAL_WORKLOAD_STATES}
        for workload_id, rank in self._progress_series_labels():
            if workload_id not in live:
                self.metrics.workload_progress.remove(workload_id, rank)
        return len(by_state)

    def _progress_series_labels(self) -> tuple[tuple[str, str], ...]:
        """Return the (workload, rank) label pairs currently held by the gauge."""

        if self.metrics is None:
            return ()
        return tuple(
            (values[0], values[1]) for values in list(self.metrics.workload_progress._metrics) if len(values) == 2
        )

    async def reconcile_services_once(self) -> int:
        """Bring every service to its declared replica count and revision.

        One action per pass per service, deliberately. The planner is pure and
        could hand back a whole batch, but performing it in one transaction
        would make a partially-applied roll indistinguishable from a completed
        one after a crash -- and a service reconciles again in a second anyway.

        Nothing here is a new workload state. A replica is an ordinary workload
        that succeeds or fails and stays terminal; the service simply asks for
        another one.
        """

        reconciled = 0
        async with self.repository.transaction() as transaction:
            services = [
                workload
                for workload in await transaction.list_workloads(state=WorkloadState.PENDING)
                if workload.coordination_kind is CoordinationKind.SERVICE
            ]
        for service in services:
            async with self.repository.transaction() as transaction:
                current = await transaction.get_workload(service.id)
                if current is None or current.coordination_kind is not CoordinationKind.SERVICE:
                    continue
                children = await transaction.list_child_workloads(current.id)
            try:
                policy = service_policy(current.spec.metadata)
            except InputError:
                logger.warning("service %s has a malformed policy; skipping", current.id, exc_info=True)
                continue
            if policy is None:
                continue
            plan = plan_service(policy, children, now=utcnow())
            if plan.cancel:
                await self._cancel_service_replica(current, plan.cancel[0])
                reconciled += 1
                continue
            if plan.submit > 0:
                await self._start_service_replica(current, policy)
                reconciled += 1
        return reconciled

    async def _start_service_replica(self, service: WorkloadRecord, policy: ServicePolicy) -> None:
        """Submit one replica of a service at its current revision."""

        spec = service.spec.model_copy(
            update={
                "metadata": {
                    **{
                        name: value
                        for name, value in service.spec.metadata.items()
                        if not name.startswith("eyvan.service/")
                    },
                    CHILD_REVISION: policy.revision,
                },
                # A replica is single-attempt: the service is the thing that
                # restarts, so a replica retrying inside itself would hide a
                # crash loop from the backoff that exists to bound it.
                "max_retries": 0,
                "max_failure_retries": 0,
            }
        )
        try:
            await self.service.submit_child_workload(
                parent=service.model_copy(
                    update={"spec": service.spec.model_copy(update={"can_submit_children": True})}
                ),
                spec=spec,
                # Deterministic per revision and per slot, so a crash between
                # the submit and the commit does not start two replicas.
                idempotency_key=f"replica:{policy.revision}:{uuid.uuid4().hex[:12]}",
            )
        except (ConflictError, AuthorizationError):
            logger.warning("could not start a replica for service %s", service.id, exc_info=True)

    async def _cancel_service_replica(self, service: WorkloadRecord, replica_id: str) -> None:
        """Retire one replica, leaving the service itself untouched."""

        try:
            await self.service.cancel_workload(self._principal, replica_id, idempotency_key=f"retire:{replica_id}")
        except (ConflictError, NotFoundError):
            # It finished on its own between the plan and the act, which is the
            # ordinary race for a service and not an error.
            logger.debug("replica %s of service %s was already terminal", replica_id, service.id)

    async def collect_artifacts_once(self) -> int:
        """Delete managed artifacts nothing references and nothing holds.

        The service has always been able to do this; nothing ever asked it to.
        Its own docstring recorded that no caller passed ``dry_run=False``, so
        the index, the quota accounting, the legal holds and the reverse
        reference index all worked while storage grew without bound.

        ``artifact_retention_s`` is an age floor on top of the service's
        ``grace_period_s``: the grace period closes a race inside one pass,
        while the retention window is the operator's statement about how long an
        unreferenced artifact is worth keeping.
        """

        retention_s = self.settings.artifact_retention_s
        if retention_s <= 0:
            return 0
        log_retention_s = self.settings.log_retention_s
        collection = await self.service.collect_artifacts_once(
            principal=self._principal,
            grace_period_s=float(retention_s),
            # Logs outlive the general window, and zero means never collect
            # them -- the same "zero disables" convention the other retention
            # windows use.
            retention_by_kind={ManagedArtifactKind.LOGS.value: float(log_retention_s) if log_retention_s > 0 else None},
            dry_run=False,
        )
        deleted = len(collection.collected_uris)
        if deleted:
            logger.info(
                "collected %d unreferenced managed artifact(s), reclaiming %d bytes",
                deleted,
                collection.reclaimed_bytes,
            )
        return deleted

    async def mark_superseded_results_once(self) -> int:
        """Mark results a later attempt replaced, so collection can see them.

        Without this the superseded marker is never written, so a replaced
        result is never a collection candidate and the retention window above
        can never apply to it.
        """

        return await self.service.mark_superseded_results_once()

    async def expire_budget_grants_once(self) -> int:
        """Return headroom from federated budget grants whose expiry has passed.

        A grant's expiry is mandatory at construction, so leaving the sweep
        uncalled meant every grant lived forever: the granting plane never got
        its headroom back and a tenant could outspend its federated budget
        indefinitely.
        """

        expired = await self.service.expire_budget_grants_once()
        if expired:
            logger.info("expired %d federated budget grant(s)", expired)
        return expired

    async def prune_retained_state_once(self) -> dict[str, int]:
        """Delete state past its retention window and report what was removed.

        Runs leader-only and in bounded batches. A window of ``0`` disables
        pruning for that table, preserving the historical behaviour where
        nothing was ever deleted -- which also meant an idempotency key was
        honored forever, so a caller reusing one months later silently replayed
        the original response instead of acting.
        """

        now = utcnow()
        windows = (
            ("outbox", self.settings.outbox_retention_s, "prune_outbox"),
            ("audit_events", self.settings.audit_retention_s, "prune_events"),
            ("idempotency_keys", self.settings.idempotency_retention_s, "prune_idempotency"),
        )
        removed: dict[str, int] = {}
        failures: list[str] = []
        for name, retention_s, method in windows:
            if retention_s <= 0:
                continue
            cutoff = now - timedelta(seconds=retention_s)
            try:
                async with self.repository.transaction() as transaction:
                    prune = getattr(transaction, method)
                    count = await prune(before=cutoff, limit=self.settings.retention_batch_limit)
            except Exception as exc:  # one broken window must not mask the rest
                # These windows are independent tables with independent hazards.
                # Aborting the loop on the first failure is how a backend that
                # could not prune audit events also stopped pruning idempotency
                # keys -- so replays kept being honored forever, silently.
                failures.append(f"{name}: {exc}")
                logger.exception("retention pruning failed for %s", name)
                continue
            if count:
                removed[name] = count
        if removed:
            logger.info("retention pruned %s", ", ".join(f"{name}={count}" for name, count in sorted(removed.items())))
        if failures:
            raise RuntimeError("retention pruning incomplete: " + "; ".join(failures))
        return removed
