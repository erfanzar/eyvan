# Copyright (C) 2026 Erfan Zare Chavoshi

# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Classifying a spot reclamation Eyvan looked at a moment too late.

Both v5p-16 cells of the 2026-08-13 P5 run were reclaimed by GCP the same way,
and only the first was charged to the preemption budget. The difference was
timing. ``PREEMPTED`` is transient -- GCP deletes the node it reclaimed -- and
reconciles ran ~48s apart, so the second cell was already ``DELETING`` when
Eyvan looked::

    18:04:05  event=lost  previous=ready/HEALTHY  capacity_class=spot
              reason="TPU VM is terminal (DELETING)"
    workload -> failed: "execution cell capacity is NEEDS_RECREATE"
              failure_count=0  preemption_count=1 (unchanged)  capacity_loss_count=0

Untyped, the loss is read as a generic capacity loss and charged to
``max_retries``. A spot job sets that to 0 *because* it expects to be reclaimed
-- P5 says so in as many words -- so the job was killed while the preemption
budget it was given went unspent, and neither counter recorded why.

Reaching this classification at all means ``desired_state`` is still "up": Eyvan
wants the cell and did not ask for it to go away. On spot, a node dying anyway
is a reclamation.
"""

from __future__ import annotations

import json

import pytest
from test_gcp_provider import UID, qr_payload, record, result

from eyvan.capacity import CapacityStatus
from eyvan.capacity.drivers.base import CapacityProviderFailureCode
from eyvan.capacity.providers.gcp.queued_resources import GcloudTpuProvider


def _observe(node_state: str, *, capacity_class: str = "spot"):
    def runner(args):
        if "queued-resources" in args:
            return result(stdout=json.dumps(qr_payload(state="ACTIVE")))
        return result(
            stdout=json.dumps(
                {
                    "state": node_state,
                    "acceleratorType": "v6e-8",
                    "metadata": {"eyvan-uid": UID},
                    "networkEndpoints": [{"ipAddress": "10.2.0.4"}],
                }
            )
        )

    cell = record()
    cell.capacity_class = capacity_class
    return GcloudTpuProvider(runner=runner).ensure(cell)


@pytest.mark.parametrize("node_state", ["PREEMPTED", "DELETING"])
def test_a_spot_node_dying_while_eyvan_wants_it_up_is_charged_as_a_preemption(node_state: str) -> None:
    """The two ways one reclamation looks, depending only on when it is observed.

    Asserting both in one parametrize is the point: a run must not get a
    different budget for the same event because a poll landed a few seconds
    later.
    """

    observation = _observe(node_state)

    assert observation.state is CapacityStatus.NEEDS_RECREATE
    assert observation.failure_code is CapacityProviderFailureCode.PREEMPTED


@pytest.mark.parametrize("node_state", ["STOPPED", "TERMINATED", "HIDDEN"])
def test_a_host_that_died_of_something_else_is_not_relabelled_a_reclamation(node_state: str) -> None:
    """Why the spot branch names two states instead of accepting every terminal one.

    These say something happened to the *host*, not to the purchase. Calling
    them reclamations would suppress the failure evidence an interruption is
    defined not to record, and a zone that keeps killing cells would never
    accumulate the cooldown that stops Eyvan asking it for more.
    """

    observation = _observe(node_state)

    assert observation.state is CapacityStatus.NEEDS_RECREATE
    assert observation.failure_code is None


def test_an_on_demand_node_that_is_deleting_was_not_reclaimed() -> None:
    """On-demand capacity is not reclaimed, so its deletion means something else.

    Widening the rule to every class would let a plain deletion consume a
    preemption budget and report a reclamation that never happened.
    """

    observation = _observe("DELETING", capacity_class="on-demand")

    assert observation.failure_code is None


def test_an_on_demand_node_actually_preempted_is_still_reported_as_preempted() -> None:
    """The provider's own word outranks the purchase class either way."""

    observation = _observe("PREEMPTED", capacity_class="on-demand")

    assert observation.failure_code is CapacityProviderFailureCode.PREEMPTED
