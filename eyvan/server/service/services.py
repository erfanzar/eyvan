# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""A service as a coordinator, not a workload kind and not a restart flag.

Two shapes were available and both are worse.

``execution.kind`` selects *how a process is launched*, and a service launches
identically to anything else -- so a ``kind`` would name something that is not a
launching difference.

A ``restart_policy`` flag is worse still. ``WORKLOAD_TRANSITIONS[SUCCEEDED]`` is
an empty frozenset, ``observe_workloads_once`` polls only the five live states,
and ``_release_terminal`` settles lease and budget with ``settled_at`` as the
idempotency boundary. Making one record restart itself requires a
``SUCCEEDED -> PENDING`` edge, which weakens the terminal contract for *every*
consumer -- ``wait_workload`` returns the moment a state is terminal, and
``EyvanJob.result()`` reads the artifact immediately after.

So: a ``schedulable=False`` parent whose children are ordinary, terminal,
single-attempt workloads, with a leader phase maintaining "N live children of
the target revision". "SUCCEEDED means restart me" then never touches the state
machine, and a rolling update never interacts with fencing tokens because each
replica is a different workload.

The desired state lives in the parent's ``spec.metadata`` behind the typed
accessor here, so stringly-typing is confined to one file. A persisted
``ServiceRecord`` costs a protocol, three backends and a migration, and cannot
be undone cheaply -- worth paying once the shape has stopped moving, not before.
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from eyvan.records import WorkloadRecord, WorkloadState
from eyvan.server.service.errors import InputError
from eyvan.server.service.state import TERMINAL_WORKLOAD_STATES

#: Metadata keys the service policy is stored under. Prefixed so a user's own
#: metadata cannot collide with control-plane state.
_REPLICAS = "eyvan.service/replicas"
_REVISION = "eyvan.service/revision"
_MAX_UNAVAILABLE = "eyvan.service/max_unavailable"
_MAX_SURGE = "eyvan.service/max_surge"
_BACKOFF_S = "eyvan.service/restart_backoff_s"

#: Replica identity, written onto each child so the reconciler can read a
#: child's revision back without a second record type.
CHILD_REVISION = "eyvan.service/child_revision"

#: Ceiling on how long a crash-looping service waits between attempts. Derived
#: statelessly from the children the reconciler already reads, because every
#: resubmission can trigger a provider capacity request -- a service whose image
#: is broken should not ask for accelerators every second.
_MAX_BACKOFF_S = 300.0


@dataclass(frozen=True, slots=True)
class ServicePolicy:
    """What a service is supposed to look like."""

    replicas: int
    revision: str
    #: How many replicas may be missing during a roll. Default 1 and
    #: ``max_surge`` default 0, because accelerators are scarce: the honest
    #: default is "stop one, then start one", not "start one, then stop one".
    max_unavailable: int = 1
    max_surge: int = 0
    restart_backoff_s: float = 5.0

    def __post_init__(self) -> None:
        if self.replicas < 0:
            raise InputError("a service cannot have a negative replica count")
        if not self.revision:
            raise InputError("a service requires a revision")
        if self.max_unavailable < 0 or self.max_surge < 0:
            raise InputError("service roll bounds cannot be negative")
        if self.max_unavailable == 0 and self.max_surge == 0 and self.replicas:
            # Neither stopping first nor starting first is permitted, so a roll
            # could never make progress. Refusing here beats a reconciler that
            # silently does nothing forever.
            raise InputError("a service roll requires max_unavailable or max_surge above zero")


@dataclass(frozen=True, slots=True)
class ServicePlan:
    """What the reconciler decided, as data rather than as side effects."""

    submit: int = 0
    cancel: tuple[str, ...] = field(default_factory=tuple)
    wait_seconds: float = 0.0
    detail: str = ""


def service_policy(metadata: tp.Mapping[str, str]) -> ServicePolicy | None:
    """Read a service policy out of a parent's metadata, or ``None``.

    ``None`` rather than a raise for a non-service, because every coordinator's
    metadata passes through here and most of them are not services.
    """

    if _REPLICAS not in metadata:
        return None
    try:
        return ServicePolicy(
            replicas=int(metadata[_REPLICAS]),
            revision=metadata.get(_REVISION, ""),
            max_unavailable=int(metadata.get(_MAX_UNAVAILABLE, 1)),
            max_surge=int(metadata.get(_MAX_SURGE, 0)),
            restart_backoff_s=float(metadata.get(_BACKOFF_S, 5.0)),
        )
    except (TypeError, ValueError) as exc:
        raise InputError(f"service policy is malformed: {exc}") from exc


def service_metadata(policy: ServicePolicy) -> dict[str, str]:
    """Render a policy back into metadata, so one file owns both directions."""

    return {
        _REPLICAS: str(policy.replicas),
        _REVISION: policy.revision,
        _MAX_UNAVAILABLE: str(policy.max_unavailable),
        _MAX_SURGE: str(policy.max_surge),
        _BACKOFF_S: str(policy.restart_backoff_s),
    }


def child_revision(child: WorkloadRecord) -> str:
    """Return the revision a replica was started for."""

    return child.spec.metadata.get(CHILD_REVISION, "")


def plan_service(
    policy: ServicePolicy,
    children: tp.Sequence[WorkloadRecord],
    *,
    now: datetime,
) -> ServicePlan:
    """Decide what one service needs, as a pure function of what it has.

    Pure on purpose: reconciliation that reads and writes in the same breath is
    the kind of code where "what would it do here" can only be answered by
    running it against a database. This takes state and returns intent; a thin
    leader-only wrapper performs one action per transaction.
    """

    live = [child for child in children if child.state not in TERMINAL_WORKLOAD_STATES]
    current = [child for child in live if child_revision(child) == policy.revision]
    stale = [child for child in live if child_revision(child) != policy.revision]

    # Scaling to zero is a valid desired state, and it is not a roll: stop
    # everything and stop.
    if policy.replicas == 0:
        if live:
            return ServicePlan(cancel=tuple(child.id for child in live), detail="scaling to zero")
        return ServicePlan(detail="stopped")

    if stale:
        # A roll. The bound is on *availability*, and a stale replica is still
        # serving -- so what may be stopped is measured against every live
        # replica, not against the new ones alone.
        #
        # Measuring against ``current`` instead deadlocks at the start of every
        # roll: with no new replicas yet, it concludes the service is already
        # maximally unavailable and never stops the first old one, so the deploy
        # hangs with no error anywhere.
        floor = policy.replicas - policy.max_unavailable
        allowed_down = max(0, len(live) - floor)
        if allowed_down:
            return ServicePlan(
                cancel=tuple(child.id for child in stale[:allowed_down]),
                detail=f"rolling to {policy.revision}",
            )
        if len(current) + len(stale) < policy.replicas + policy.max_surge:
            return ServicePlan(submit=1, detail=f"surging to {policy.revision}")
        return ServicePlan(detail=f"waiting for a replica to stop before rolling to {policy.revision}")

    missing = policy.replicas - len(current)
    if missing > 0:
        wait = _restart_backoff(policy, children, now=now)
        if wait > 0:
            return ServicePlan(wait_seconds=wait, detail="backing off after repeated replica failures")
        return ServicePlan(submit=missing, detail=f"starting {missing} replica(s)")
    if missing < 0:
        return ServicePlan(cancel=tuple(child.id for child in current[missing:]), detail="scaling in")
    return ServicePlan(detail="steady")


def _restart_backoff(policy: ServicePolicy, children: tp.Sequence[WorkloadRecord], *, now: datetime) -> float:
    """Return how long to wait before replacing a replica that keeps dying.

    Derived from the children already in hand rather than from stored counters:
    a service whose image does not exist would otherwise ask a provider for
    accelerators once per reconcile cycle, forever.

    Only *consecutive recent* failures count. A service that has been up for a
    week and loses one replica restarts immediately, which is the behaviour an
    operator expects and the one a naive failure count would break.
    """

    window = now - timedelta(seconds=_MAX_BACKOFF_S)
    recent = [child for child in children if child.state is WorkloadState.FAILED and child.updated_at >= window]
    if not recent:
        return 0.0
    last = max(child.updated_at for child in recent)
    delay = min(policy.restart_backoff_s * (2 ** (len(recent) - 1)), _MAX_BACKOFF_S)
    elapsed = (now - last).total_seconds()
    return max(0.0, delay - elapsed)


__all__ = (
    "CHILD_REVISION",
    "ServicePlan",
    "ServicePolicy",
    "child_revision",
    "plan_service",
    "service_metadata",
    "service_policy",
)
