# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The unattended run, and the thing that watches it.

Everything Eyvan has been proven to do was proven with a human watching, over
minutes. The properties that distinguish a control plane from a script --
nothing stranded, nothing leaked, nothing double-charged -- fail after days, and
no test suite watches for days.

This is the watcher. It polls the named invariants on an interval, keeps the
snapshot each tick returns so the over-time properties can be decided at all,
and reports at the end. Left running for 48 to 72 hours against a real fleet, it
is the evidence no code change substitutes for.

**A transient violation is not a finding, and treating it as one makes the whole
run useless.** The point-in-time checks race with the sweeps that fix them: a
lease is genuinely orphaned for the cycle between its workload settling and
``settle-orphan-leases`` reclaiming it, and a soak that failed on that would fail
every time while proving nothing. So a violation has to persist across
consecutive observations before it counts, and the threshold is stated rather
than tuned until the run goes green.

**It does not stop on the first problem.** An observer that exits at hour three
answers "did anything break" and not "what breaks over three days", and the
second question is the one nobody has ever answered here.
"""

from __future__ import annotations

import json
import time
import typing as tp
from dataclasses import dataclass, field

import click

from eyvan.sdk.commands.common import JsonRenderer
from eyvan.sdk.commands.state import CliSession

#: How many consecutive observations a violation must survive to be reported.
#:
#: Three, because the sweeps that clear the transient cases run on the leader
#: loop and a violation that outlives three of its cycles is not a race with
#: one. Lower would report the races; higher would hide a real leak behind a
#: slow poll.
PERSISTENCE_THRESHOLD = 3


@dataclass(slots=True)
class SoakState:
    """What one soak run has observed so far.

    Held in memory and deliberately small: the thing being watched for is
    unbounded growth, and an observer whose own footprint grows with the run
    would eventually become the incident.
    """

    ticks: int = 0
    failures: int = 0
    #: Consecutive observations each violation has survived, keyed by invariant
    #: and subject so two different stranded cells are two findings.
    streaks: dict[tuple[str, str], int] = field(default_factory=dict)
    #: Violations that crossed the threshold, with the streak they reached.
    confirmed: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)
    previous_snapshot: dict[str, object] | None = None
    over_time: list[str] = field(default_factory=list)
    #: Total managed-artifact bytes added across the run. The number that says
    #: whether collection is running at all -- a total alone cannot.
    artifact_growth: int = 0

    def observe(self, payload: dict[str, object]) -> None:
        """Fold one observation into the run."""

        self.ticks += 1
        seen: set[tuple[str, str]] = set()
        violations = payload.get("violations")
        for violation in violations if isinstance(violations, list) else []:
            if not isinstance(violation, dict):
                continue
            key = (str(violation.get("invariant")), str(violation.get("subject")))
            seen.add(key)
            self.streaks[key] = self.streaks.get(key, 0) + 1
            if self.streaks[key] >= PERSISTENCE_THRESHOLD:
                self.confirmed[key] = {
                    "invariant": key[0],
                    "subject": key[1],
                    "detail": violation.get("detail"),
                    "consecutive_observations": self.streaks[key],
                }
        # A violation that cleared restarts its streak rather than decaying. A
        # decaying counter would let a condition that flaps just under the
        # threshold accumulate forever and never report.
        for key in [key for key in self.streaks if key not in seen]:
            del self.streaks[key]
        self._compare(payload.get("snapshot"))

    def _compare(self, snapshot: object) -> None:
        """Decide the properties one observation cannot settle."""

        if not isinstance(snapshot, dict):
            return
        previous = self.previous_snapshot
        self.previous_snapshot = snapshot
        if previous is None:
            return
        before = previous.get("budget_spend")
        after = snapshot.get("budget_spend")
        if isinstance(before, dict) and isinstance(after, dict):
            for scope, current in after.items():
                earlier = before.get(scope)
                if not isinstance(earlier, list) or not isinstance(current, list):
                    continue
                # ``[spent, revision]``. A period roll bumps the revision and
                # resets the spend deliberately, and calling that a violation
                # would fire on correct behaviour once a month.
                if earlier[1] == current[1] and current[0] < earlier[0]:
                    self.over_time.append(f"I6: {scope} spend fell from {earlier[0]} to {current[0]}")
        earlier_bytes = previous.get("artifact_bytes")
        current_bytes = snapshot.get("artifact_bytes")
        if isinstance(earlier_bytes, int) and isinstance(current_bytes, int):
            # Recorded, not judged. Growth is only a violation against a ceiling
            # an operator set, and inventing one here would make every run that
            # stored anything report a failure.
            self.artifact_growth += max(0, current_bytes - earlier_bytes)

    def report(self) -> dict[str, object]:
        """Return the run's verdict, in the shape a CI job can act on."""

        return {
            "observations": self.ticks,
            "failed_observations": self.failures,
            "persistence_threshold": PERSISTENCE_THRESHOLD,
            "violations": sorted(self.confirmed.values(), key=lambda item: (item["invariant"], item["subject"])),
            "over_time": sorted(set(self.over_time)),
            "artifact_bytes": (self.previous_snapshot or {}).get("artifact_bytes"),
            "artifact_growth": self.artifact_growth,
            "healthy": not self.confirmed and not self.over_time,
        }


def run_soak(
    observe: tp.Callable[[], dict[str, object]],
    *,
    ticks: int,
    sleep: tp.Callable[[float], None],
    interval_s: float,
    on_tick: tp.Callable[[int, SoakState], None] | None = None,
) -> SoakState:
    """Poll ``observe`` and fold each result into a run.

    ``observe`` and ``sleep`` are injected so the whole loop is testable without
    waiting: a soak runner whose only proof is a 72-hour run is one nobody can
    change safely.

    A failed observation is counted and does not end the run. The control plane
    restarting mid-soak is exactly the kind of thing a soak exists to survive,
    and an observer that exits on the first refusal would report the restart
    instead of what happened around it.
    """

    state = SoakState()
    for tick in range(ticks):
        try:
            state.observe(observe())
        except Exception:
            state.failures += 1
        if on_tick is not None:
            on_tick(tick, state)
        if tick + 1 < ticks:
            sleep(interval_s)
    return state


@click.command("soak")
@click.option("--hours", type=float, default=48.0, show_default=True, help="How long to watch.")
@click.option("--interval-s", type=float, default=60.0, show_default=True, help="Seconds between observations.")
@click.option("--output", type=click.Path(dir_okay=False), help="Write the final report here as JSON.")
@click.pass_obj
def soak(session: CliSession, hours: float, interval_s: float, output: str | None) -> None:
    """Watch the durability invariants for hours or days, then report.

    Exits non-zero if any invariant was violated persistently, so a scheduled
    run is a signal rather than a log nobody reads.

    Everything Eyvan claims about long-running behaviour is unobserved until
    this has run: the invariants it polls are the ones that fail after days, and
    a suite cannot watch for days.
    """

    if interval_s <= 0 or hours <= 0:
        raise click.ClickException("--hours and --interval-s must both be positive")
    ticks = max(1, int((hours * 3600) / interval_s))

    def observe() -> dict[str, object]:
        payload = session.client.request("GET", "/api/v1/system/invariants")
        if not isinstance(payload, dict):
            raise click.ClickException("the invariants route returned an unexpected document")
        return dict(payload)

    def announce(tick: int, state: SoakState) -> None:
        # One line per hour rather than per observation. A soak that prints
        # every minute for three days produces output nobody reads, and the
        # interesting moment is buried in it.
        if state.ticks and (tick + 1) % max(1, int(3600 / interval_s)) == 0:
            click.echo(
                f"[{state.ticks} observations] "
                f"{len(state.confirmed)} persistent violation(s), {state.failures} unreachable",
                err=True,
            )

    state = run_soak(observe, ticks=ticks, sleep=time.sleep, interval_s=interval_s, on_tick=announce)
    report = state.report()
    JsonRenderer().emit(report)
    if output:
        with open(output, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)
    if not report["healthy"]:
        raise SystemExit(1)


def register_soak_commands(main: click.Group) -> None:
    """Attach the soak command to the CLI root."""

    main.add_command(soak)


__all__ = ("PERSISTENCE_THRESHOLD", "SoakState", "register_soak_commands", "run_soak", "soak")
