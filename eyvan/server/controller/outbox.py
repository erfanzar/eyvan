# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Claiming, delivering, and recovering durable outbox commands."""

from __future__ import annotations

import asyncio
import typing as tp
from datetime import timedelta

from pydantic import ValidationError

from eyvan._internal.observability import trace_span
from eyvan.capacity.audit import CapacityLifecycleEvent
from eyvan.capacity.drivers.base import CapacityProviderFailureCode
from eyvan.records import (
    CellState,
    OutboxRecord,
    OutboxState,
    WorkloadRecord,
    WorkloadState,
    utcnow,
)
from eyvan.secrets import redact_secret_material
from eyvan.server.controller._support import (
    _RETRIABLE_OUTBOX_STATES,
    CapacityApplySuperseded,
    StaleOutboxCommand,
    _DispatchLeaseMembership,
    logger,
)
from eyvan.server.service.errors import ConflictError, NotFoundError
from eyvan.worker.agent.client import (
    CellAgentBusyError,
    CellAgentCapacityProviderError,
    CellAgentInvalidConfigurationError,
    CellAgentStaleMutationError,
    CellAgentUnsupportedCapabilityError,
)

_SECRET_OUTPUT_SUPPRESSED = "output suppressed for secret-bearing execution"


class _ControllerOutbox:
    """Claiming, delivering, and recovering durable outbox commands.

    A concern of :class:`~eyvan.server.controller.EyvanController`; not
    instantiated on its own.
    """

    async def dispatch_outbox_once(self, *, limit: int = 20) -> int:
        if limit <= 0:
            return 0
        command_ids = await self.repository.snapshot_claimable_outbox_ids(limit=limit)
        dispatched = 0
        for command_id in command_ids:
            command = await self.repository.claim_outbox_id(
                worker_id=self.worker_id,
                command_id=command_id,
                lease_seconds=self.settings.outbox_claim_ttl_s,
            )
            if command is None:
                continue
            await self._dispatch_claimed_outbox(command)
            dispatched += 1
        return dispatched

    async def _dispatch_claimed_outbox(self, command: OutboxRecord) -> None:
        """Deliver and settle one command that this controller has just claimed."""

        try:
            # Delivery is the one place a provider or cell agent can block for
            # minutes, and ``topic`` is exactly the "driver operation" that
            # ``eyvan_reconcile_seconds`` was declared to be labeled by. The
            # histogram and the span existed with no call site anywhere, so
            # neither a slow provider nor a slow agent produced any signal.
            with (
                self._measure_reconcile(command.topic),
                trace_span(
                    "eyvan.outbox_dispatch",
                    topic=command.topic,
                    command_id=command.id,
                ),
            ):
                await self._dispatch_with_claim_heartbeat(command)
        except CapacityApplySuperseded as exc:
            logger.info("discarding superseded capacity apply %s: %s", command.id, exc)
            await self._discard_outbox(command, provider_diagnostic=exc.provider_diagnostic)
        except StaleOutboxCommand as exc:
            logger.info("discarding stale outbox command %s: %s", command.id, exc)
            await self._finish_outbox(command)
        except CellAgentStaleMutationError as exc:
            diagnostic = await self._safe_outbox_diagnostic(command, str(exc))
            logger.info("discarding cell-agent stale mutation %s: %s", command.id, diagnostic)
            if command.topic == "capacity.apply":
                await self._record_cell_agent_fence_conflict(
                    cell_id=command.aggregate_id,
                    generation=command.generation,
                    diagnostic=diagnostic,
                )
                await self._discard_outbox(command, provider_diagnostic=diagnostic)
            elif command.topic == "capacity.drain":
                await self._recover_stale_cell_lifecycle_command(
                    command,
                    expected_state=CellState.DRAINING,
                    diagnostic=diagnostic,
                )
            elif command.topic == "capacity.release":
                await self._recover_stale_cell_lifecycle_command(
                    command,
                    expected_state=CellState.DELETING,
                    diagnostic=diagnostic,
                )
            elif command.topic == "workload.dispatch":
                await self._recover_stale_workload_command(
                    command,
                    expected_state=WorkloadState.LEASED,
                    diagnostic=diagnostic,
                )
            elif command.topic == "workload.cancel":
                await self._recover_stale_workload_command(
                    command,
                    expected_state=WorkloadState.CANCELLING,
                    diagnostic=diagnostic,
                )
            elif command.topic == "workload.checkpoint":
                await self._recover_stale_workload_command(
                    command,
                    expected_state=WorkloadState.CHECKPOINTING,
                    diagnostic=diagnostic,
                )
            elif command.topic == "workload.preempt":
                await self._recover_stale_preemption_command(command, diagnostic=diagnostic)
            else:
                await self._discard_outbox(command, provider_diagnostic=diagnostic)
        except CellAgentBusyError as exc:
            await self._finish_outbox(command, error=str(exc), transient=True)
        except CellAgentUnsupportedCapabilityError as exc:
            # The backend does not implement this operation; retrying cannot
            # change that. Secret-bearing diagnostics are suppressed before
            # either logging or durable settlement.
            diagnostic = await self._safe_outbox_diagnostic(command, str(exc))
            logger.warning(
                "cell agent does not support %s for command %s: %s",
                command.topic,
                command.id,
                diagnostic,
            )
            await self._finish_outbox(
                command,
                error=f"unsupported by cell agent backend: {diagnostic}",
                terminal=True,
            )
        except CellAgentInvalidConfigurationError as exc:
            diagnostic = await self._safe_outbox_diagnostic(command, str(exc))
            logger.warning("cell agent rejected the configuration for command %s: %s", command.id, diagnostic)
            await self._finish_outbox(command, error=f"invalid configuration: {diagnostic}", terminal=True)
        except CellAgentCapacityProviderError as exc:
            await self._finish_outbox(command, error=str(exc), failure_code=exc.code)
        except Exception as exc:
            await self._finish_outbox(command, error=str(exc))
        else:
            await self._finish_outbox(command)

    def _outbox_claim_heartbeat_s(self) -> float:
        """Return a bounded renewal cadence for long-running outbox commands."""

        claim_ttl_s = max(self.settings.outbox_claim_ttl_s, 1)
        return min(30.0, max(0.1, claim_ttl_s / 3))

    @staticmethod
    def _consume_background_dispatch_result(task: asyncio.Task[None]) -> None:
        """Consume a detached RPC result after its durable claim was lost."""

        if task.cancelled():
            return
        try:
            task.exception()
        except asyncio.CancelledError:
            return

    async def _dispatch_with_claim_heartbeat(self, command: OutboxRecord) -> None:
        """Deliver one command while keeping its durable ownership lease alive.

        Provider creates can legitimately run for minutes. The command remains
        idempotent, but a process that dies during the RPC must be reclaimable
        soon rather than hiding a pending workload behind a long claim TTL.
        """

        active_command = await self._renew_outbox_claim(command)
        dispatch = asyncio.create_task(self._dispatch(active_command))
        try:
            while not dispatch.done():
                completed, _ = await asyncio.wait((dispatch,), timeout=self._outbox_claim_heartbeat_s())
                if not completed:
                    active_command = await self._renew_outbox_claim(active_command)
            await dispatch
        except asyncio.CancelledError:
            if not dispatch.done():
                dispatch.add_done_callback(self._consume_background_dispatch_result)
            raise
        except Exception:
            if not dispatch.done():
                dispatch.add_done_callback(self._consume_background_dispatch_result)
            raise

    async def _renew_outbox_claim(self, command: OutboxRecord) -> OutboxRecord:
        """Extend this controller's current claim without reviving a stale command."""

        async with self.repository.transaction() as transaction:
            current = await transaction.get_outbox(command.id)
            if current is None:
                raise StaleOutboxCommand(f"outbox command {command.id!r} no longer exists")
            now = utcnow()
            if (
                current.state is not OutboxState.PROCESSING
                or current.claimed_by != self.worker_id
                or current.claimed_until is None
                or current.claimed_until <= now
                or current.topic != command.topic
                or current.aggregate_type != command.aggregate_type
                or current.aggregate_id != command.aggregate_id
                or current.generation != command.generation
            ):
                raise StaleOutboxCommand(f"outbox command {command.id!r} claim expired or moved to another worker")
            renewed = current.model_copy(
                update={
                    "claimed_until": now + timedelta(seconds=max(self.settings.outbox_claim_ttl_s, 1)),
                    "updated_at": now,
                }
            )
            await transaction.put_outbox(renewed)
            return renewed

    async def _dispatch(self, command: OutboxRecord) -> None:
        if command.topic == "capacity.apply":
            await self._dispatch_capacity(command)
        elif command.topic == "capacity.drain":
            await self._dispatch_capacity_drain(command)
        elif command.topic == "capacity.release":
            await self._dispatch_capacity_release(command)
        elif command.topic == "workload.dispatch":
            await self._dispatch_workload(command)
        elif command.topic == "workload.cancel":
            await self._dispatch_cancel(command)
        elif command.topic == "workload.checkpoint":
            await self._dispatch_checkpoint(command)
        elif command.topic == "workload.preempt":
            await self._dispatch_preemption(command)
        else:
            raise ValueError(f"unsupported outbox topic {command.topic!r}")

    async def _recover_stale_cell_lifecycle_command(
        self,
        command: OutboxRecord,
        *,
        expected_state: CellState,
        diagnostic: str,
    ) -> None:
        """Replace an obsolete drain or release mutation before retiring its outbox record."""

        try:
            previous = await self.service.get_cell(self._principal, command.aggregate_id)
            refenced, replayed = await self.service.refence_cell_lifecycle_command(
                self._principal,
                command.aggregate_id,
                expected_generation=command.generation,
                expected_state=expected_state,
                idempotency_key=f"agent-fence-refence:{command.id}",
            )
        except (ConflictError, NotFoundError) as exc:
            logger.info("stale %s command %s no longer owns its cell: %s", command.topic, command.id, exc)
        except Exception as exc:
            logger.warning("could not refence stale %s command %s: %s", command.topic, command.id, exc)
            await self._finish_outbox(command, error=f"stale mutation recovery failed: {exc}", transient=True)
            return
        else:
            if not replayed:
                self._capacity_lifecycle.event(
                    CapacityLifecycleEvent.RETRY,
                    refenced,
                    previous=previous,
                    reason=f"cell-agent rejected {command.topic}; issued generation {refenced.generation}",
                )
        await self._discard_outbox(command, provider_diagnostic=diagnostic)

    async def _recover_stale_workload_command(
        self,
        command: OutboxRecord,
        *,
        expected_state: WorkloadState,
        diagnostic: str,
    ) -> None:
        """Settle a workload whose agent fence makes remote execution ownership uncertain."""

        raw_lease_id = command.payload.get("lease_id")
        if not isinstance(raw_lease_id, str) or not raw_lease_id:
            await self._discard_outbox(command, provider_diagnostic=diagnostic)
            return
        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
        if (
            workload is None
            or workload.generation != command.generation
            or workload.lease_id != raw_lease_id
            or workload.state is not expected_state
        ):
            await self._discard_outbox(command, provider_diagnostic=diagnostic)
            return
        try:
            members = await self._load_workload_members(workload)
        except StaleOutboxCommand as exc:
            logger.info("stale %s command %s has no current workload members: %s", command.topic, command.id, exc)
            await self._discard_outbox(command, provider_diagnostic=diagnostic)
            return
        if any(lease.generation != command.generation for lease, _cell in members):
            await self._discard_outbox(command, provider_diagnostic=diagnostic)
            return
        for lease, cell in members:
            await self._record_cell_agent_fence_conflict(
                cell_id=cell.id,
                generation=lease.cell_generation,
                diagnostic=f"workload {workload.id!r} {command.topic} fence conflict: {diagnostic}",
            )
        failure_reason = f"cell-agent {command.topic.removeprefix('workload.')} fence conflict: {diagnostic}"
        try:
            await self.service.settle_terminal_workload(
                self._principal,
                workload.id,
                WorkloadState.FAILED,
                action="workload.agent_fence_conflict",
                failure_reason=failure_reason,
                expected_generation=workload.generation,
                expected_lease_id=raw_lease_id,
                expected_state=expected_state,
            )
        except (ConflictError, NotFoundError) as exc:
            logger.info("stale %s command %s settlement lost ownership: %s", command.topic, command.id, exc)
        else:
            await self.reconcile_unmanaged_capacity_recoveries_once()
        await self._discard_outbox(command, provider_diagnostic=diagnostic)

    async def _recover_stale_preemption_command(self, command: OutboxRecord, *, diagnostic: str) -> None:
        """Abort a preemption before stopping, or retain it for explicit operator finalization."""

        try:
            aborted = await self._abort_preemption_checkpoint_phase(command, diagnostic)
        except StaleOutboxCommand as exc:
            logger.info("stale preemption command %s already changed ownership: %s", command.id, exc)
            await self._discard_outbox(command, provider_diagnostic=diagnostic)
            return
        if aborted:
            await self._finish_outbox(command)
            return
        logger.error(
            "preemption command %s lost an agent fence after stopping began; explicit force-finalization is required",
            command.id,
        )
        await self._discard_outbox(command, provider_diagnostic=diagnostic)

    @staticmethod
    def _has_live_dispatch_command(
        workload: WorkloadRecord,
        lease_ids: tuple[str, ...],
        commands: tp.Iterable[OutboxRecord],
    ) -> bool:
        """Return whether a retryable command can still submit this exact lease membership.

        A malformed active command is deliberately treated as live. It must be
        retired by normal outbox delivery before releasing its lease, because a
        controller cannot prove that a malformed persisted payload has no
        submission side effect.
        """

        for command in commands:
            if (
                command.topic != "workload.dispatch"
                or command.aggregate_type != "workload"
                or command.aggregate_id != workload.id
                or command.generation != workload.generation
                or command.state not in _RETRIABLE_OUTBOX_STATES
            ):
                continue
            try:
                membership = _DispatchLeaseMembership.model_validate(command.payload)
            except ValidationError:
                return True
            if membership.ids == lease_ids:
                return True
        return False

    async def _safe_outbox_diagnostic(self, command: OutboxRecord, error: str) -> str:
        """Suppress agent/provider output for a declared-secret workload."""

        if command.aggregate_type == "workload":
            async with self.repository.transaction() as transaction:
                workload = await transaction.get_workload(command.aggregate_id)
            if workload is not None and workload.spec.secret_mounts:
                return _SECRET_OUTPUT_SUPPRESSED
        return redact_secret_material(error)

    async def _finish_outbox(
        self,
        command: OutboxRecord,
        *,
        error: str | None = None,
        failure_code: CapacityProviderFailureCode | None = None,
        transient: bool = False,
        terminal: bool = False,
    ) -> bool:
        """Settle an outbox command only while this controller still owns its live claim.

        ``transient`` retries soon without spending an attempt. ``terminal``
        settles the command for good, keeping its diagnostic: it is for answers
        that will never change on a retry, such as a backend reporting that it
        does not implement the operation at all.
        """

        now = utcnow()
        async with self.repository.transaction() as transaction:
            current = await transaction.get_outbox(command.id)
            if (
                current is None
                or current.state is not OutboxState.PROCESSING
                or current.claimed_by != self.worker_id
                or current.claimed_until is None
                or current.claimed_until <= now
            ):
                return False
            if error is not None:
                if current.aggregate_type == "workload":
                    workload = await transaction.get_workload(current.aggregate_id)
                    error = (
                        _SECRET_OUTPUT_SUPPRESSED
                        if workload is not None and workload.spec.secret_mounts
                        else redact_secret_material(error)
                    )
                else:
                    error = redact_secret_material(error)
            if error is None:
                if current.attempts > 1 or current.last_error is not None:
                    # A command that failed and then succeeded is about to lose
                    # every trace of why: `last_error` is cleared here and nothing
                    # logged the failures. That combination produced a workload
                    # that sat "waiting for dispatch" for 21 minutes across 10
                    # attempts and then settled with attempts=10 and last_error
                    # NULL -- an operator, and the author of this comment, had no
                    # way to see the cause. Say it once, on the way past.
                    logger.warning(
                        "outbox %s (%s for %s %s) succeeded after %d attempt(s); last failure was: %s",
                        current.id,
                        current.topic,
                        current.aggregate_type,
                        current.aggregate_id,
                        current.attempts,
                        current.last_error or "not retained",
                    )
                updated = current.model_copy(
                    update={
                        "state": OutboxState.SUCCEEDED,
                        "claimed_by": None,
                        "claimed_until": None,
                        "last_error": None,
                        "failure_code": None,
                        "updated_at": now,
                    }
                )
            elif terminal or failure_code is CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED:
                logger.warning(
                    "outbox %s (%s for %s %s) will not be retried: %s",
                    current.id,
                    current.topic,
                    current.aggregate_type,
                    current.aggregate_id,
                    error,
                )
                updated = current.model_copy(
                    update={
                        "state": OutboxState.DISCARDED,
                        "claimed_by": None,
                        "claimed_until": None,
                        "last_error": error[:2000],
                        "failure_code": failure_code,
                        "updated_at": now,
                    }
                )
            else:
                delay = 1 if transient else min(300, 5 * (2 ** min(current.attempts - 1, 6)))
                # Logged here rather than at each raise site, because the two
                # handlers that matter most -- the capacity-provider error and the
                # bare `except Exception` -- logged nothing at all, so a dispatch
                # could fail indefinitely in complete silence while the client
                # showed a steady "waiting for dispatch".
                logger.warning(
                    "outbox %s (%s for %s %s) failed on attempt %d, retrying in %ds: %s",
                    current.id,
                    current.topic,
                    current.aggregate_type,
                    current.aggregate_id,
                    current.attempts,
                    delay,
                    error,
                )
                updated = current.model_copy(
                    update={
                        "state": OutboxState.FAILED,
                        "attempts": max(0, current.attempts - 1) if transient else current.attempts,
                        "claimed_by": None,
                        "claimed_until": None,
                        "last_error": error[:2000],
                        "failure_code": failure_code,
                        "available_at": now + timedelta(seconds=delay),
                        "updated_at": now,
                    }
                )
            await transaction.put_outbox(updated)
        if error is not None and command.topic == "capacity.apply" and not transient:
            await self._record_capacity_failure(command, error, failure_code=failure_code)
        return True

    async def _discard_outbox(self, command: OutboxRecord, *, provider_diagnostic: str | None = None) -> None:
        """Finish a superseded command without erasing its prior provider diagnostic."""

        now = utcnow()
        async with self.repository.transaction() as transaction:
            current = await transaction.get_outbox(command.id)
            if (
                current is None
                or current.state is not OutboxState.PROCESSING
                or current.claimed_by != self.worker_id
                or current.claimed_until is None
                or current.claimed_until <= now
            ):
                return
            retained_diagnostic = current.last_error if current.last_error is not None else provider_diagnostic
            await transaction.put_outbox(
                current.model_copy(
                    update={
                        "state": OutboxState.DISCARDED,
                        "claimed_by": None,
                        "claimed_until": None,
                        "last_error": None if retained_diagnostic is None else retained_diagnostic[:2000],
                        "updated_at": now,
                    }
                )
            )
