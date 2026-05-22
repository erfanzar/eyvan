# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""A service modelled as a coordinator rather than a restart flag.

There was no service abstraction at all: no ``ServiceRecord``, no health checks,
no rolling updates, no endpoint registry. The two obvious shapes are both worse
than the one built here, and the reasons are structural rather than
aesthetic -- a ``restart_policy`` flag needs a ``SUCCEEDED -> PENDING`` edge,
which weakens the terminal contract for every consumer including
``wait_workload`` and ``EyvanJob.result()``.

The sharpest hazard is not in the new code. ``settle_coordinator_workloads_once``
declares a non-schedulable PENDING parent terminal as soon as all its children
are, and a *service's* children are periodically all-terminal by design -- that
moment is exactly when a replacement is due. Without a guard, the batch settler
would mark the service SUCCEEDED and it would stop forever, looking to an
operator like a service that quietly went away.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from eyvan.records import (
    CommandExecution,
    CoordinationKind,
    WorkloadDefinition,
    WorkloadRecord,
    WorkloadState,
)
from eyvan.server.service.errors import InputError
from eyvan.server.service.services import (
    CHILD_REVISION,
    ServicePolicy,
    child_revision,
    plan_service,
    service_metadata,
    service_policy,
)

NOW = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)


def _replica(
    identifier: str,
    *,
    revision: str = "v1",
    state: WorkloadState = WorkloadState.RUNNING,
    updated_at: datetime | None = None,
) -> WorkloadRecord:
    return WorkloadRecord(
        id=identifier,
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(
            name=identifier,
            execution=CommandExecution(command="python serve.py"),
            metadata={CHILD_REVISION: revision},
        ),
        state=state,
        created_by="alice",
        updated_at=updated_at or NOW,
    )


# --------------------------------------------------------------------------- the policy


def test_a_policy_round_trips_through_metadata() -> None:
    """One file owns both directions, so stringly-typing stays in it."""

    policy = ServicePolicy(replicas=3, revision="v7", max_unavailable=2, max_surge=1, restart_backoff_s=2.5)

    assert service_policy(service_metadata(policy)) == policy


def test_metadata_without_a_replica_count_is_not_a_service() -> None:
    """Every coordinator's metadata passes through here and most are not services."""

    assert service_policy({"unrelated": "value"}) is None


def test_a_malformed_policy_is_refused_rather_than_defaulted() -> None:
    """Defaulting a garbled replica count would silently resize someone's service."""

    with pytest.raises(InputError, match="malformed"):
        service_policy({"eyvan.service/replicas": "three", "eyvan.service/revision": "v1"})


def test_a_roll_that_could_never_progress_is_refused() -> None:
    """Neither stopping first nor starting first permitted means no roll is possible.

    Refusing at construction beats a reconciler that silently does nothing
    forever while an operator waits for a deploy.
    """

    with pytest.raises(InputError, match="max_unavailable or max_surge"):
        ServicePolicy(replicas=2, revision="v1", max_unavailable=0, max_surge=0)


def test_a_service_with_no_revision_is_refused() -> None:
    """Without one, a roll cannot tell a new replica from an old one."""

    with pytest.raises(InputError, match="requires a revision"):
        ServicePolicy(replicas=1, revision="")


# --------------------------------------------------------------------------- reconciling


def test_an_empty_service_starts_every_replica() -> None:
    """The first reconcile after a service is created."""

    plan = plan_service(ServicePolicy(replicas=3, revision="v1"), [], now=NOW)

    assert plan.submit == 3
    assert plan.cancel == ()


def test_a_finished_replica_is_replaced() -> None:
    """ "SUCCEEDED means start another one" without touching the state machine.

    The replica really is terminal; the service simply wants a different one.
    """

    children = [
        _replica("a", state=WorkloadState.SUCCEEDED),
        _replica("b"),
        _replica("c"),
    ]

    plan = plan_service(ServicePolicy(replicas=3, revision="v1"), children, now=NOW)

    assert plan.submit == 1


def test_a_steady_service_does_nothing() -> None:
    """A reconciler that always acts is a reconciler that never converges."""

    children = [_replica("a"), _replica("b")]

    plan = plan_service(ServicePolicy(replicas=2, revision="v1"), children, now=NOW)

    assert (plan.submit, plan.cancel) == (0, ())
    assert plan.detail == "steady"


def test_scaling_in_cancels_the_surplus() -> None:
    """Reducing the replica count is as much a desired state as raising it."""

    children = [_replica("a"), _replica("b"), _replica("c")]

    plan = plan_service(ServicePolicy(replicas=2, revision="v1"), children, now=NOW)

    assert len(plan.cancel) == 1


def test_scaling_to_zero_stops_everything_without_rolling() -> None:
    """Zero is a valid desired state, and it is not a deploy."""

    children = [_replica("a"), _replica("b")]

    plan = plan_service(ServicePolicy(replicas=0, revision="v1"), children, now=NOW)

    assert set(plan.cancel) == {"a", "b"}


def test_a_terminal_replica_is_not_counted_as_running() -> None:
    """Otherwise a service whose replicas all crashed would look fully staffed."""

    children = [_replica("a", state=WorkloadState.FAILED, updated_at=NOW - timedelta(hours=1))]

    plan = plan_service(ServicePolicy(replicas=1, revision="v1"), children, now=NOW)

    assert plan.submit == 1


# --------------------------------------------------------------------------- rolling


def test_a_roll_retires_only_as_many_as_the_bound_allows() -> None:
    """``max_unavailable=1`` means one replica down at a time, not all of them."""

    children = [_replica(name, revision="v1") for name in ("a", "b", "c")]

    plan = plan_service(ServicePolicy(replicas=3, revision="v2"), children, now=NOW)

    assert len(plan.cancel) == 1


def test_a_roll_measures_availability_against_every_live_replica() -> None:
    """A stale replica is still serving, and the bound is on availability.

    Measuring against the *new* replicas alone deadlocks at the start of every
    roll: with none started yet it concludes the service is already maximally
    unavailable, never stops the first old one, and the deploy hangs with no
    error anywhere. My first version did exactly that.
    """

    children = [
        _replica("new", revision="v2"),
        _replica("old-1", revision="v1"),
        _replica("old-2", revision="v1"),
    ]

    plan = plan_service(ServicePolicy(replicas=3, revision="v2"), children, now=NOW)

    # Three live replicas against a floor of two: one may stop.
    assert len(plan.cancel) == 1
    assert plan.cancel[0].startswith("old-")


def test_a_roll_stops_when_availability_is_already_at_the_floor() -> None:
    """Below the floor, the roll waits rather than continuing to drain."""

    children = [_replica("new", revision="v2"), _replica("old-1", revision="v1")]

    plan = plan_service(ServicePolicy(replicas=3, revision="v2"), children, now=NOW)

    # Two live against a floor of two: nothing may stop, and with no surge
    # budget the roll waits for the missing replica to be started elsewhere.
    assert plan.cancel == ()


def test_a_surge_starts_before_stopping_when_permitted() -> None:
    """``max_surge`` is the opposite trade: spend capacity to avoid a gap."""

    children = [_replica(name, revision="v1") for name in ("a", "b")]

    plan = plan_service(ServicePolicy(replicas=2, revision="v2", max_unavailable=0, max_surge=1), children, now=NOW)

    assert plan.submit == 1
    assert plan.cancel == ()


def test_a_completed_roll_settles_to_steady() -> None:
    """A roll that never finishes is indistinguishable from one that failed."""

    children = [_replica(name, revision="v2") for name in ("a", "b")]

    plan = plan_service(ServicePolicy(replicas=2, revision="v2"), children, now=NOW)

    assert plan.detail == "steady"


def test_a_replica_carries_the_revision_it_was_started_for() -> None:
    """Without it the reconciler cannot tell a new replica from an old one."""

    assert child_revision(_replica("a", revision="v9")) == "v9"
    # A replica with no revision recorded predates the service or was started
    # by something else; it reads as "not the current revision" and is rolled.
    plain = WorkloadRecord(
        id="plain",
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(name="plain", execution=CommandExecution(command="python serve.py")),
        created_by="alice",
    )
    assert child_revision(plain) == ""


# --------------------------------------------------------------------------- backoff


def test_a_crash_looping_service_backs_off() -> None:
    """Every resubmission can trigger a provider capacity request.

    A service whose image does not exist would otherwise ask for accelerators
    once per reconcile cycle, forever.
    """

    children = [
        _replica(name, state=WorkloadState.FAILED, updated_at=NOW - timedelta(seconds=1)) for name in ("a", "b", "c")
    ]

    plan = plan_service(ServicePolicy(replicas=1, revision="v1"), children, now=NOW)

    assert plan.submit == 0
    assert plan.wait_seconds > 0


def test_backoff_grows_with_consecutive_failures() -> None:
    """One failure is bad luck; five in a minute is a broken deploy."""

    def wait(count: int) -> float:
        children = [_replica(f"r{index}", state=WorkloadState.FAILED, updated_at=NOW) for index in range(count)]
        return plan_service(ServicePolicy(replicas=1, revision="v1"), children, now=NOW).wait_seconds

    assert wait(1) < wait(2) < wait(3)


def test_a_long_running_service_that_loses_one_replica_restarts_immediately() -> None:
    """Only *recent* failures count.

    A naive lifetime failure count would make a service that has been up for a
    week wait minutes to replace its first lost replica -- the opposite of what
    an operator expects, and a real outage extension.
    """

    children = [
        _replica("old-failure", state=WorkloadState.FAILED, updated_at=NOW - timedelta(days=7)),
        _replica("just-lost", state=WorkloadState.FAILED, updated_at=NOW - timedelta(days=1)),
    ]

    plan = plan_service(ServicePolicy(replicas=1, revision="v1"), children, now=NOW)

    assert plan.wait_seconds == 0
    assert plan.submit == 1


def test_backoff_is_bounded() -> None:
    """An unbounded backoff is a service that never comes back."""

    children = [_replica(f"r{index}", state=WorkloadState.FAILED, updated_at=NOW) for index in range(40)]

    plan = plan_service(ServicePolicy(replicas=1, revision="v1"), children, now=NOW)

    assert plan.wait_seconds <= 300.0


# --------------------------------------------------------------------------- the hazard


async def test_the_batch_settler_does_not_kill_a_service() -> None:
    """The sharpest hazard in this phase, and it is in code that predates it.

    A service's children are periodically all-terminal -- that moment is when a
    replacement is due. Without the guard the batch settler declares the service
    SUCCEEDED and it stops forever, which an operator sees as a service that
    quietly went away with a terminal state nobody asked for.
    """

    from eyvan.config import EyvanSettings
    from eyvan.server.controller import EyvanController
    from eyvan.server.persistence.memory import InMemoryRepository
    from eyvan.server.service.control_plane import ControlPlaneService
    from eyvan.server.service.scheduling import CoreSchedulingService

    repository = InMemoryRepository()
    controller = EyvanController(
        repository,
        ControlPlaneService(repository, scheduler=CoreSchedulingService()),
        settings=EyvanSettings(),
    )
    async with repository.transaction() as transaction:
        await transaction.put_workload(
            WorkloadRecord(
                id="svc-1",
                org_id="acme",
                project_id="research",
                spec=WorkloadDefinition(
                    name="api",
                    execution=CommandExecution(command="python serve.py"),
                    metadata=service_metadata(ServicePolicy(replicas=1, revision="v1")),
                ),
                state=WorkloadState.PENDING,
                schedulable=False,
                coordination_kind=CoordinationKind.SERVICE,
                created_by="alice",
            )
        )
        # Every replica is between lives, which for a service is ordinary.
        await transaction.put_workload(
            WorkloadRecord(
                id="replica-1",
                org_id="acme",
                project_id="research",
                spec=WorkloadDefinition(name="replica", execution=CommandExecution(command="python serve.py")),
                state=WorkloadState.SUCCEEDED,
                parent_workload_id="svc-1",
                created_by="alice",
            )
        )

    await controller.settle_coordinator_workloads_once()

    async with repository.transaction() as transaction:
        service = await transaction.get_workload("svc-1")

    assert service is not None
    assert service.state is WorkloadState.PENDING, "the batch settler terminated a live service"


async def test_the_batch_settler_still_settles_a_batch() -> None:
    """The guard must not stop the phase doing the job it exists for.

    A coordinator never holds a lease, so nothing else can settle it -- it would
    sit PENDING forever while all its tasks succeeded.
    """

    from eyvan.config import EyvanSettings
    from eyvan.server.controller import EyvanController
    from eyvan.server.persistence.memory import InMemoryRepository
    from eyvan.server.service.control_plane import ControlPlaneService
    from eyvan.server.service.scheduling import CoreSchedulingService

    repository = InMemoryRepository()
    controller = EyvanController(
        repository,
        ControlPlaneService(repository, scheduler=CoreSchedulingService()),
        settings=EyvanSettings(),
    )
    async with repository.transaction() as transaction:
        await transaction.put_workload(
            WorkloadRecord(
                id="batch-1",
                org_id="acme",
                project_id="research",
                spec=WorkloadDefinition(name="sweep", execution=CommandExecution(command="python task.py")),
                state=WorkloadState.PENDING,
                schedulable=False,
                coordination_kind=CoordinationKind.BATCH,
                created_by="alice",
            )
        )
        await transaction.put_workload(
            WorkloadRecord(
                id="task-1",
                org_id="acme",
                project_id="research",
                spec=WorkloadDefinition(name="task", execution=CommandExecution(command="python task.py")),
                state=WorkloadState.SUCCEEDED,
                parent_workload_id="batch-1",
                created_by="alice",
            )
        )

    await controller.settle_coordinator_workloads_once()

    async with repository.transaction() as transaction:
        batch = await transaction.get_workload("batch-1")

    assert batch is not None
    assert batch.state is WorkloadState.SUCCEEDED


def test_old_failures_do_not_inflate_the_backoff_for_a_new_one() -> None:
    """A year of occasional blips must not make the next one wait five minutes.

    The elapsed-time subtraction hides this in most shapes -- an old failure is
    long past its delay -- so it takes many old failures plus one fresh one to
    separate them. A service that lost a replica once a month for a year, then
    lost one just now, should retry in seconds. Counting lifetime failures makes
    it wait the maximum backoff instead, and the longer it runs the worse it
    gets: exactly backwards.
    """

    children = [
        _replica(f"old-{index}", state=WorkloadState.FAILED, updated_at=NOW - timedelta(days=30 * index))
        for index in range(1, 12)
    ]
    children.append(_replica("just-now", state=WorkloadState.FAILED, updated_at=NOW - timedelta(seconds=1)))

    plan = plan_service(ServicePolicy(replicas=1, revision="v1"), children, now=NOW)

    # One recent failure: the first backoff step, not the ceiling.
    assert 0 < plan.wait_seconds <= 5.0
