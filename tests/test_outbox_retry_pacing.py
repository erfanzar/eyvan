# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""How often a durable command retries while it is failing, and when that stops
being routine.

A transient failure gives its attempt back on purpose: an agent restarting
mid-dispatch must not spend a workload's retry budget. Nothing else grew, so a
condition that only *looked* transient retried once a second forever at an
unchanged attempt number, with an identical warning each time.

Watched live on 2026-08-12: a native worker answered 500 to every stop, the
cancel retried that way for twenty minutes, four TPU VMs stayed leased, and
nothing in the control plane ever said this had stopped being normal. The cause
is fixed elsewhere; what is asserted here is that the next condition of that
shape backs off and gets said out loud.
"""

from __future__ import annotations

import pytest

from eyvan.server.controller.outbox import command_is_stuck, transient_retry_delay_s


def test_a_blip_is_retried_immediately() -> None:
    """The case the give-the-attempt-back rule exists for stays fast."""

    assert transient_retry_delay_s(0.0) == 1
    assert transient_retry_delay_s(5.0) == 1


def test_a_command_failing_for_minutes_backs_off() -> None:
    """Once a second forever is a load problem and a log-flood, not patience."""

    assert transient_retry_delay_s(60.0) > transient_retry_delay_s(5.0)
    assert transient_retry_delay_s(600.0) > transient_retry_delay_s(60.0)


def test_the_backoff_is_monotonic() -> None:
    """Age only grows, so the wait may never shrink under it."""

    delays = [transient_retry_delay_s(age) for age in (0, 15, 30, 90, 300, 900, 3600)]

    assert delays == sorted(delays)


def test_the_backoff_is_capped_low_enough_to_notice_recovery() -> None:
    """Something is waiting on these -- a cancel, a dispatch -- so an hour-old
    command still retries about once a minute rather than once an hour."""

    assert transient_retry_delay_s(3_600.0) == transient_retry_delay_s(86_400.0)
    assert transient_retry_delay_s(86_400.0) <= 60


@pytest.mark.parametrize("age", [0.0, 60.0, 299.0])
def test_a_recent_failure_is_routine(age: float) -> None:
    assert command_is_stuck(age) is False


@pytest.mark.parametrize("age", [300.0, 1_200.0])
def test_a_command_failing_this_long_is_not_routine(age: float) -> None:
    """An operator reading warnings at one a second will not notice one more.

    The level is what makes the twentieth minute look different from the first.
    """

    assert command_is_stuck(age) is True


def test_a_negative_age_is_treated_as_new() -> None:
    """Clock skew between replicas must not produce a negative wait."""

    assert transient_retry_delay_s(-10.0) == 1
