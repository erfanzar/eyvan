# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The leader-elected phase schedule and the long-running loops."""

from __future__ import annotations

import asyncio

from eyvan._internal.observability import trace_span
from eyvan.server.controller._support import (
    LeaderPhase,
    logger,
)


class _ControllerLoops:
    """The leader-elected phase schedule and the long-running loops.

    A concern of :class:`~eyvan.server.controller.EyvanController`; not
    instantiated on its own.
    """

    async def run(self, stop: asyncio.Event) -> None:
        """Run independent leader-scheduling and replica-local outbox loops."""

        await asyncio.gather(
            self._run_scheduler_loop(stop),
            self._run_outbox_loop(stop),
        )

    def _leader_phases(self) -> tuple[LeaderPhase, ...]:
        """Declare the leader cycle in order.

        A list rather than straight-line code because adding a phase used to mean
        editing the runner *and* threading another deadline through a positional
        tuple in its signature -- so any two changes that each added a phase
        conflicted. Order is the only thing this tuple encodes; every policy a phase
        needs travels with it.
        """

        return (
            LeaderPhase("unmanaged-capacity-recovery", self.reconcile_unmanaged_capacity_recoveries_once),
            LeaderPhase("capacity-pools", self.reconcile_capacity_pools_once),
            LeaderPhase(
                "observe-cells",
                self._observe_cells_once,
                interval_s=self.settings.cell_reconcile_interval_s,
            ),
            LeaderPhase("quarantine-leases", self.quarantine_expired_leases_once),
            LeaderPhase("observe-workloads", self.observe_workloads_once),
            # After observation: a workload the observer could resolve this cycle is
            # not an orphan, whatever the previous cycle saw.
            LeaderPhase("settle-orphans", self.settle_orphaned_workloads_once),
            # After observation, so a coordinator settles in the same cycle its last
            # task does rather than a full interval later.
            LeaderPhase("settle-coordinators", self.settle_coordinator_workloads_once),
            # Before scheduling, so a node released this cycle can be placed in the
            # same cycle rather than waiting a full interval.
            LeaderPhase("release-dag-nodes", self.release_ready_dag_nodes_once),
            # Before scheduling, for the same reason: a group that became feasible
            # this cycle should be placed in it, not a full interval later.
            LeaderPhase("release-groups", self.release_admitted_groups_once),
            LeaderPhase("schedule", self.reconcile_once),
            # After scheduling, so a cell leased this cycle is not charged as idle
            # for the interval in which it was actually being placed. Failures are
            # isolated: an accounting gap must never stop placement.
            LeaderPhase(
                "cell-billing",
                self.accrue_cell_billing_once,
                interval_s=self.settings.cell_billing_interval_s,
                zero_interval_disables=True,
                isolate_failures=True,
            ),
            # Retention is housekeeping and must never delay placement, so it runs
            # last, on its own slow interval, and its failures are isolated.
            LeaderPhase(
                "retention",
                self.prune_retained_state_once,
                interval_s=self.settings.retention_interval_s,
                zero_interval_disables=True,
                isolate_failures=True,
            ),
            # Housekeeping, same treatment. Each of the three below was fully
            # implemented and tested and called by nothing, so the behaviour did
            # not exist in production: artifact storage grew without bound, a
            # replaced result was never marked collectable, and federated budget
            # grants never expired.
            #
            # Marking runs before collection so a result superseded this cycle
            # becomes a candidate in it, rather than a full interval later.
            LeaderPhase(
                "mark-superseded",
                self.mark_superseded_results_once,
                interval_s=self.settings.artifact_collection_interval_s,
                zero_interval_disables=True,
                isolate_failures=True,
            ),
            LeaderPhase(
                "collect-artifacts",
                self.collect_artifacts_once,
                interval_s=self.settings.artifact_collection_interval_s,
                zero_interval_disables=True,
                isolate_failures=True,
            ),
            # Gauges describe a level rather than a rate, so they refresh on their
            # own interval. Four families were declared, exported, and written by
            # nothing, so fleet state, capacity and spend always read zero -- a
            # gauge stuck at zero looks like an answer.
            LeaderPhase(
                "export-metrics",
                self.export_metrics_once,
                interval_s=self.settings.metrics_export_interval_s,
                zero_interval_disables=True,
                isolate_failures=True,
            ),
            LeaderPhase(
                "expire-grants",
                self.expire_budget_grants_once,
                interval_s=self.settings.grant_expiry_interval_s,
                zero_interval_disables=True,
                isolate_failures=True,
            ),
        )

    async def _run_leader_phases(self, *, now: float, due: dict[str, float]) -> dict[str, float]:
        """Run one leader cycle, re-checking leadership before every phase.

        A full cycle spans many provider round trips, so this replica can lose the
        advisory lock partway through. Continuing after that would let two leaders
        mint autoscaler cell ids from the same ``len(managed) + 1`` sequence and
        collide, so each phase re-acquires and the cycle stops the moment leadership
        is gone. Losing it mid-cycle is ordinary, not an error.

        ``due`` maps phase name to its next-eligible monotonic time and is returned
        updated. Keyed by name rather than positionally so a new phase adds an entry
        instead of changing this signature.
        """

        for phase in self._leader_phases():
            if phase.interval_s is not None:
                if phase.zero_interval_disables and phase.interval_s <= 0:
                    continue
                if now < due.get(phase.name, 0.0):
                    continue
                # Advance before the call, so a persistently failing phase cannot pin
                # the loop retrying it on every scheduler tick.
                due[phase.name] = now + phase.interval_s
            if not await self.leader.acquire():
                return due
            # One span per phase is where tracing earns its keep: a slow leader
            # cycle is almost always one phase, and the span names it. Without a
            # configured tracer provider this is a null context, so it costs
            # nothing when nobody is collecting.
            with trace_span("eyvan.leader_phase", phase=phase.name):
                if not phase.isolate_failures:
                    await phase.run()
                    continue
                try:
                    await phase.run()
                except Exception:
                    logger.exception("leader phase %s failed; continuing", phase.name)
        return due

    async def _run_scheduler_loop(self, stop: asyncio.Event) -> None:
        """Run leader-only global reconciliation without blocking outbox delivery."""

        due: dict[str, float] = {}
        try:
            while not stop.is_set():
                try:
                    if await self.leader.acquire():
                        async with self._leader_cycle_lock:
                            now = asyncio.get_running_loop().time()
                            due = await self._run_leader_phases(now=now, due=due)
                except Exception:
                    logger.exception("global scheduling reconciliation failed")
                await self._wait_for_next_cycle(stop)
        finally:
            await self.leader.release()

    async def _run_outbox_loop(self, stop: asyncio.Event) -> None:
        """Deliver durable mutations independently of leader reconciliation."""

        while not stop.is_set():
            try:
                await self.dispatch_outbox_once()
            except Exception:
                logger.exception("outbox reconciliation failed")
            await self._wait_for_next_cycle(stop)

    async def _wait_for_next_cycle(self, stop: asyncio.Event) -> None:
        """Sleep until the next controller interval or shutdown."""

        try:
            await asyncio.wait_for(stop.wait(), timeout=self.settings.scheduler_interval_s)
        except asyncio.TimeoutError:
            pass
