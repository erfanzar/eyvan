# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The observer for a run nobody has done yet.

The plan calls the 48-to-72-hour unattended soak the single highest-value
remaining item. It needs hardware and days, and neither is available here. What
*is* buildable is the thing that watches it -- so that when the fleet is
approved the run is a command rather than a project.

Every decision below is tested against injected time and injected observations,
because a runner whose only proof is a 72-hour run is one nobody can change
safely. The two that matter most:

* **a transient violation is not a finding**, since the point-in-time checks
  race with the sweeps that fix them, and a soak that failed on that race would
  fail every time while proving nothing; and
* **an unreachable control plane does not end the run**, because surviving a
  restart is among the things a soak exists to observe.
"""

from __future__ import annotations

from eyvan.sdk.commands.soak import PERSISTENCE_THRESHOLD, SoakState, run_soak


def _violation(invariant: str = "I2", subject: str = "lease-a") -> dict:
    return {"invariant": invariant, "subject": subject, "detail": f"{subject} is stranded"}


def _payload(*violations: dict, snapshot: dict | None = None) -> dict:
    return {"violations": list(violations), "snapshot": snapshot or {}}


def _run(payloads: list[dict]) -> SoakState:
    """Fold a fixed sequence of observations, with no waiting."""

    remaining = list(payloads)
    return run_soak(
        lambda: remaining.pop(0),
        ticks=len(payloads),
        sleep=lambda _seconds: None,
        interval_s=0.0,
    )


# --------------------------------------------------------------------------- transients


def test_a_healthy_run_reports_healthy() -> None:
    """The case the whole exercise exists to be able to claim."""

    state = _run([_payload() for _ in range(5)])

    assert state.report()["healthy"] is True
    assert state.report()["observations"] == 5


def test_a_violation_seen_once_is_not_a_finding() -> None:
    """A lease is genuinely orphaned between its workload settling and the sweep.

    A soak that failed on that would fail every run and prove nothing, which is
    how a signal becomes something people mute.
    """

    state = _run([_payload(_violation()), _payload(), _payload()])

    assert state.report()["healthy"] is True
    assert state.report()["violations"] == []


def test_a_violation_that_persists_is_reported() -> None:
    """The finding the run exists to produce.

    Three consecutive observations is longer than the sweeps that clear the
    transient cases take, so what survives it is not a race.
    """

    state = _run([_payload(_violation()) for _ in range(PERSISTENCE_THRESHOLD)])
    report = state.report()

    assert report["healthy"] is False
    assert report["violations"][0]["invariant"] == "I2"
    assert report["violations"][0]["consecutive_observations"] == PERSISTENCE_THRESHOLD


def test_a_flapping_violation_never_accumulates_into_a_finding() -> None:
    """A cleared streak restarts rather than decaying.

    A decaying counter would let a condition that flaps just under the threshold
    accumulate forever and eventually report -- turning "this recovers every
    time" into a page.
    """

    # Two on, one off, repeatedly. Under a decaying counter the streak walks
    # 1, 2, 1, 2, 3 and reports on the fifth violation; under a reset it walks
    # 1, 2, 0, 1, 2 and never does. A simple on/off alternation cannot tell the
    # two apart, which is how the first version of this test let the decay
    # through.
    state = _run([_payload(_violation()), _payload(_violation()), _payload()] * 5)

    assert state.report()["healthy"] is True
    assert state.report()["violations"] == []


def test_two_subjects_are_two_findings() -> None:
    """Keyed by subject, so a second stranded cell is not hidden by the first."""

    payloads = [_payload(_violation(subject="lease-a"), _violation(subject="lease-b"))] * PERSISTENCE_THRESHOLD
    state = _run(payloads)

    assert {item["subject"] for item in state.report()["violations"]} == {"lease-a", "lease-b"}


# --------------------------------------------------------------------------- survival


def test_an_unreachable_control_plane_does_not_end_the_run() -> None:
    """Surviving a restart is among the things a soak exists to observe.

    An observer that exited on the first refusal would report the restart
    instead of what happened around it.
    """

    calls = {"n": 0}

    def observe() -> dict:
        calls["n"] += 1
        if calls["n"] in {2, 3}:
            raise ConnectionError("control plane restarting")
        return _payload()

    state = run_soak(observe, ticks=6, sleep=lambda _seconds: None, interval_s=0.0)
    report = state.report()

    assert report["observations"] == 4
    assert report["failed_observations"] == 2
    assert report["healthy"] is True


def test_a_run_that_could_never_observe_anything_is_not_reported_healthy() -> None:
    """Zero successful observations is not evidence of health.

    A soak whose control plane was down the whole time must not print the same
    verdict as one that watched a working fleet for three days.
    """

    def observe() -> dict:
        raise ConnectionError("down")

    report = run_soak(observe, ticks=5, sleep=lambda _seconds: None, interval_s=0.0).report()

    assert report["observations"] == 0
    assert report["failed_observations"] == 5


# --------------------------------------------------------------------------- over time


def test_spend_going_backwards_is_caught() -> None:
    """One observation cannot see it, which is why the snapshot is carried.

    Spend falling within a period means something was refunded or lost, and
    neither is a thing Eyvan does.
    """

    state = _run(
        [
            _payload(snapshot={"budget_spend": {"acme/research": [50.0, 1]}, "artifact_bytes": 0}),
            _payload(snapshot={"budget_spend": {"acme/research": [30.0, 1]}, "artifact_bytes": 0}),
        ]
    )
    report = state.report()

    assert report["healthy"] is False
    assert any("I6" in entry for entry in report["over_time"])


def test_a_budget_period_roll_is_not_a_violation() -> None:
    """A roll resets ``spent`` on purpose.

    Reporting it would fire on correct behaviour once a month -- and a soak that
    cries wolf monthly is one whose findings get skimmed.
    """

    state = _run(
        [
            _payload(snapshot={"budget_spend": {"acme/research": [50.0, 1]}, "artifact_bytes": 0}),
            _payload(snapshot={"budget_spend": {"acme/research": [0.0, 2]}, "artifact_bytes": 0}),
        ]
    )

    assert state.report()["healthy"] is True


def test_artifact_growth_is_measured_rather_than_judged() -> None:
    """The rate is what says whether collection is running; a total alone is not.

    Not judged here, because growth is only a violation against a ceiling an
    operator set -- inventing one would make every run that stored anything
    report a failure.
    """

    state = _run(
        [
            _payload(snapshot={"artifact_bytes": 1_000, "budget_spend": {}}),
            _payload(snapshot={"artifact_bytes": 4_000, "budget_spend": {}}),
            _payload(snapshot={"artifact_bytes": 9_000, "budget_spend": {}}),
        ]
    )
    report = state.report()

    assert report["artifact_growth"] == 8_000
    assert report["artifact_bytes"] == 9_000
    assert report["healthy"] is True


def test_the_observer_does_not_grow_with_the_run() -> None:
    """The thing being watched for is unbounded growth.

    An observer whose own footprint grew with a three-day run would eventually
    become the incident, so it keeps counters and one snapshot -- never history.
    """

    state = _run([_payload(snapshot={"artifact_bytes": tick, "budget_spend": {}}) for tick in range(500)])

    assert state.ticks == 500
    assert state.previous_snapshot == {"artifact_bytes": 499, "budget_spend": {}}
    assert len(state.streaks) == 0
    assert len(state.over_time) == 0


# --------------------------------------------------------------------------- the command


def test_the_command_is_attached_and_refuses_nonsense_intervals() -> None:
    """A soak with a zero interval is a busy loop against the control plane."""

    from click.testing import CliRunner

    from eyvan.sdk import cli

    assert "soak" in cli.main.commands

    result = CliRunner().invoke(cli.main, ["--user", "root", "--role", "fleet_admin", "soak", "--interval-s", "0"])

    assert result.exit_code != 0
    assert "positive" in result.output
