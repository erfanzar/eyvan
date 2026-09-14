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

#: The observations a phase must be able to trust before it commits work against
#: them. Named here rather than inline so the two lists cannot drift: a phase
#: that places a workload is deciding where real money goes, and doing that on a
#: cell inventory this cycle failed to refresh is the mistake the old
#: stop-the-whole-cycle behaviour was really protecting against.
_FRESH_VIEW: tuple[str, ...] = ("observe-cells", "observe-workloads")


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
            # Isolated, unlike the phases it precedes. This one plans future
            # capacity; the ones after it observe, settle, and release the
            # capacity that already exists and is already being paid for. A
            # planner that raises has cost nothing yet, and stopping the cycle
            # over it strands a running accelerator -- which is what happened on
            # a live fleet, for eight minutes of v5p-16, before the simulation
            # defect underneath was found. The failure is still logged with its
            # traceback; it just no longer takes settlement down with it.
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
            # After the workload sweep, and separate from it. That one settles by
            # *workload*, so a lease its workload finished with, vanished from, or
            # retried away from is reachable from no other phase -- it would hold a
            # cell and go unbilled forever. Ordering matters: a workload settled
            # above becomes terminal, which is exactly what makes its leftover
            # leases visible here in the same cycle.
            LeaderPhase("settle-orphan-leases", self.settle_orphaned_leases_once),
            # After observation, so a coordinator settles in the same cycle its last
            # task does rather than a full interval later.
            LeaderPhase("settle-coordinators", self.settle_coordinator_workloads_once),
            # Before scheduling, so a node released this cycle can be placed in the
            # same cycle rather than waiting a full interval.
            LeaderPhase("release-dag-nodes", self.release_ready_dag_nodes_once, requires=_FRESH_VIEW),
            # Before the feasibility gate, and before scheduling: a group that can
            # be placed whole is leased here as one transaction, so the gate below
            # never sees it and the ordinary pass never places half of it. Off by
            # default, in which case this phase returns immediately.
            LeaderPhase("place-groups", self.place_admitted_groups_once, requires=_FRESH_VIEW),
            # Before scheduling, for the same reason: a group that became feasible
            # this cycle should be placed in it, not a full interval later.
            LeaderPhase("release-groups", self.release_admitted_groups_once, requires=_FRESH_VIEW),
            # Before scheduling, so a wave released this cycle competes in it. After
            # settlement above, so a child that finished this cycle has already
            # freed its place in the wave rather than holding it for a full interval.
            LeaderPhase("release-expansion-waves", self.release_expansion_waves_once, requires=_FRESH_VIEW),
            # Health first, so a replica probed this cycle is judged in it. The
            # probe is the only phase here that talks to anything outside the
            # database, and it does so strictly between transactions.
            LeaderPhase(
                "probe-service-health",
                self.probe_service_health_once,
                interval_s=self.settings.service_health_interval_s,
                zero_interval_disables=True,
            ),
            # Then lifecycle, which reads the observations the probe just wrote
            # and retires replicas whose claim on a host is provably gone. Before
            # the replica-count reconciler so the successor is started in the
            # same cycle the predecessor is retired.
            LeaderPhase(
                "reconcile-service-replicas",
                self.reconcile_service_replicas_once,
                interval_s=self.settings.service_replica_lifecycle_interval_s,
                zero_interval_disables=True,
            ),
            # Then autoscaling, which changes the declared count the reconciler
            # below is about to converge on. After lifecycle deliberately: a
            # replacement is a disturbance, and sizing a service from a view that
            # includes a replica already known to be doomed decides on a number
            # that was about to change anyway.
            LeaderPhase(
                "autoscale-services",
                self.autoscale_services_once,
                interval_s=self.settings.service_autoscale_interval_s,
                zero_interval_disables=True,
            ),
            # Before scheduling: a replica started this cycle should be placed
            # in it rather than a full interval later, which for a service is
            # the difference between a blip and a visible outage.
            LeaderPhase(
                "reconcile-services",
                self.reconcile_services_once,
                interval_s=self.settings.service_reconcile_interval_s,
                zero_interval_disables=True,
            ),
            LeaderPhase("schedule", self.reconcile_once, requires=_FRESH_VIEW),
            # After scheduling, so a cell leased this cycle is not charged as idle
            # for the interval in which it was actually being placed. Failures are
            # isolated: an accounting gap must never stop placement.
            LeaderPhase(
                "cell-billing",
                self.accrue_cell_billing_once,
                interval_s=self.settings.cell_billing_interval_s,
                zero_interval_disables=True,
            ),
            # Retention is housekeeping and must never delay placement, so it runs
            # last, on its own slow interval, and its failures are isolated.
            LeaderPhase(
                "retention",
                self.prune_retained_state_once,
                interval_s=self.settings.retention_interval_s,
                zero_interval_disables=True,
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
            ),
            LeaderPhase(
                "collect-artifacts",
                self.collect_artifacts_once,
                interval_s=self.settings.artifact_collection_interval_s,
                zero_interval_disables=True,
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
            ),
            LeaderPhase(
                "expire-grants",
                self.expire_budget_grants_once,
                interval_s=self.settings.grant_expiry_interval_s,
                zero_interval_disables=True,
            ),
            # Managed asset staging, and the eviction that bounds it. Both are
            # housekeeping and must never delay placement, so they run last, on
            # their own interval, with failures isolated -- a host that will not
            # answer about a dataset is not a reason to stop scheduling work.
            #
            # Staging first: a stage confirmed this cycle is one eviction can
            # account for in the same cycle, and the alternative order plans
            # against a cell's charge one interval out of date.
            LeaderPhase(
                "stage-assets",
                self.stage_cell_assets_once,
                interval_s=self.settings.cell_stage_interval_s,
                zero_interval_disables=True,
            ),
            LeaderPhase(
                "evict-stages",
                self._evict_cell_stages_phase,
                interval_s=self.settings.cell_stage_interval_s,
                zero_interval_disables=True,
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

        failed: set[str] = set()
        for phase in self._leader_phases():
            if phase.interval_s is not None:
                if phase.zero_interval_disables and phase.interval_s <= 0:
                    continue
                if now < due.get(phase.name, 0.0):
                    continue
                # Advance before the call, so a persistently failing phase cannot pin
                # the loop retrying it on every scheduler tick.
                due[phase.name] = now + phase.interval_s
            blocked = sorted(name for name in phase.requires if name in failed)
            if blocked:
                # Not an error of this phase's own, so it is stated plainly and
                # the cycle continues. The phases that give capacity back depend
                # on nothing and are therefore never reached by this branch.
                logger.warning(
                    "skipping leader phase %s: it commits against %s, which failed this cycle",
                    phase.name,
                    ", ".join(blocked),
                )
                continue
            if not await self.leader.acquire():
                return due
            # One span per phase is where tracing earns its keep: a slow leader
            # cycle is almost always one phase, and the span names it. Without a
            # configured tracer provider this is a null context, so it costs
            # nothing when nobody is collecting.
            with trace_span("eyvan.leader_phase", phase=phase.name):
                try:
                    await phase.run()
                except Exception:
                    # Contained, always. A failure used to propagate out of the
                    # whole cycle, and because the phases that release capacity
                    # and settle billing sit in the middle of this list, one
                    # failure upstream of them left an accelerator running that
                    # nobody was going to stop paying for. Observed on a live
                    # v5p-16 multislice.
                    #
                    # What the old behaviour protected is kept by ``requires``:
                    # a phase that commits work against the plane's view is
                    # skipped above when that view is stale, rather than being
                    # stopped as a side effect of an unrelated failure.
                    failed.add(phase.name)
                    logger.exception("leader phase %s failed; the rest of this cycle continues", phase.name)
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
