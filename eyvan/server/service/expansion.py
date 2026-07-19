# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Admission control for fan-out: a hard ceiling, and bounded waves under it.

A map or a DAG expansion is the one submission shape whose cost is not bounded
by the size of the request body. ``count=50000`` is eleven characters and fifty
thousand accelerator jobs, and every one of them lands in the same queue every
other tenant is waiting in.

Two separate bounds, deliberately not one:

**The ceiling** is how many children one expansion may create at all. Exceeding
it is refused *at submission*, where the submitter is holding the request and
can fix the number. The alternative -- discovering it at dispatch -- gives the
control plane only a retry loop behind a held lease, which is how a permanent
error becomes an undeletable cell.

**The wave** is how many of an admitted expansion's children may compete at
once. This is not a smaller ceiling: every child is created and durably owed
execution, but only ``wave_size`` of them are visible to the scheduler at a
time, so a ten-thousand-task array does not bury another tenant's single job
behind ten thousand queue entries. The rest are released as earlier children
finish.

Waves are off by default (``wave_size=0``), which is exactly today's behaviour:
every child admitted immediately. An operator who turns them on is choosing to
trade an array's own start latency for the queue's.

Nothing here is silent. An expansion is refused with the number it asked for and
the number it may have, or it is admitted with the plan it was admitted under --
a fan-out that was quietly truncated to fit is worse than either.
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass

from eyvan.records import ExpansionMode
from eyvan.server.service.errors import InputError

#: Default ceiling on children created by one expansion. Matches the historical
#: ``batch.DEFAULT_MAX_FAN_OUT`` so an operator who configures nothing keeps the
#: limit this package has always enforced.
DEFAULT_EXPANSION_CEILING = 1000

#: Default wave size. Zero means "no waves": every child of an admitted
#: expansion is schedulable immediately, which is what shipped before this
#: module existed and therefore what an unconfigured deployment keeps doing.
DEFAULT_EXPANSION_WAVE_SIZE = 0


class ExpansionRefused(InputError, ValueError):
    """Raised when an expansion asks for more children than it may create.

    An ``InputError`` so the API maps it to 400 like every other domain
    precondition, and a ``ValueError`` so this module stays usable as a plain
    validator by callers that have not adopted the service error hierarchy.
    """


@dataclass(frozen=True, slots=True)
class ExpansionLimits:
    """The operator-configured bounds one control plane admits expansions under."""

    #: Hard ceiling on children per expansion. A request may lower it for itself
    #: and may never raise it: how many accelerators one call can claim is a
    #: deployment decision, not a request-body one.
    ceiling: int = DEFAULT_EXPANSION_CEILING
    #: How many children of one expansion may be schedulable at once. Zero
    #: disables waves entirely.
    wave_size: int = DEFAULT_EXPANSION_WAVE_SIZE

    def __post_init__(self) -> None:
        if self.ceiling < 1:
            raise ValueError("expansion ceiling must be positive")
        if self.wave_size < 0:
            raise ValueError("expansion wave size must not be negative; use 0 to disable waves")

    def effective_ceiling(self, requested_ceiling: int | None) -> int:
        """Return the ceiling this request is actually held to."""

        if requested_ceiling is None:
            return self.ceiling
        if requested_ceiling < 1:
            raise ExpansionRefused("a request-scoped fan-out limit must be positive")
        return min(self.ceiling, requested_ceiling)


@dataclass(frozen=True, slots=True)
class AdmissionPlan:
    """What one expansion was admitted as, in the terms the submitter asked in."""

    #: Children the request asked for.
    requested: int
    #: The ceiling the request was held to, after combining server and request.
    ceiling: int
    #: Children that may compete at once. Equals ``requested`` when unbounded.
    wave_size: int
    #: Children made schedulable at submission.
    admitted_now: int
    mode: ExpansionMode

    @property
    def deferred(self) -> int:
        """Children created and owed execution but not yet competing."""

        return self.requested - self.admitted_now

    def describe(self) -> str:
        """Return the one-line, operator-readable summary of this admission."""

        if self.mode is ExpansionMode.IMMEDIATE:
            return f"admitted {self.requested} children immediately (ceiling {self.ceiling})"
        return (
            f"admitted {self.requested} children in waves of {self.wave_size}: "
            f"{self.admitted_now} competing now, {self.deferred} waiting (ceiling {self.ceiling})"
        )


def plan_expansion(
    requested: int,
    *,
    limits: ExpansionLimits,
    requested_ceiling: int | None = None,
    quota_headroom: int | None = None,
    subject: str = "expansion",
) -> AdmissionPlan:
    """Decide whether one expansion is admitted, and under what wave plan.

    Args:
        requested: Children the expansion would create.
        limits: The control plane's configured bounds.
        requested_ceiling: A per-request ceiling, which may only lower the
            server's.
        quota_headroom: How many children fit in the tenant's remaining quota
            right now, or ``None`` when that is unknown -- an unmeasurable quota
            narrows nothing, because a guess would refuse work the tenant is
            entitled to.
        subject: What to call the expansion in messages ("batch", "DAG", ...).

    Returns:
        The plan the caller must both apply and report.

    Raises:
        ExpansionRefused: If ``requested`` exceeds the effective ceiling.
    """

    if requested < 1:
        raise ExpansionRefused(f"a {subject} must create at least one child")
    ceiling = limits.effective_ceiling(requested_ceiling)
    if requested > ceiling:
        raise ExpansionRefused(
            f"{subject} of {requested} children exceeds the {ceiling}-child admission ceiling; "
            f"submit at most {ceiling} children per request, or ask an operator to raise "
            "EYVAN_MAX_EXPANSION_CHILDREN"
        )
    wave = wave_size_for(requested, limits=limits, quota_headroom=quota_headroom)
    if wave >= requested:
        return AdmissionPlan(
            requested=requested,
            ceiling=ceiling,
            wave_size=requested,
            admitted_now=requested,
            mode=ExpansionMode.IMMEDIATE,
        )
    return AdmissionPlan(
        requested=requested,
        ceiling=ceiling,
        wave_size=wave,
        admitted_now=wave,
        mode=ExpansionMode.WAVES,
    )


def wave_size_for(
    requested: int,
    *,
    limits: ExpansionLimits,
    quota_headroom: int | None = None,
) -> int:
    """Return how many children of one expansion may compete at once.

    The tighter of the configured wave and what the tenant's quota can currently
    hold, and never below one: a wave of zero is an expansion that never starts,
    which is a deadlock rather than backpressure. A tenant whose quota is
    momentarily full still gets one child at a time, and that child waits in the
    scheduler like any other -- quota is enforced there, not here.
    """

    if limits.wave_size <= 0 and quota_headroom is None:
        return requested
    candidates = [requested]
    if limits.wave_size > 0:
        candidates.append(limits.wave_size)
    if quota_headroom is not None:
        candidates.append(max(int(quota_headroom), 1))
    return max(min(candidates), 1)


def quota_headroom_children(
    limits: tp.Mapping[str, float],
    usage: tp.Mapping[str, float],
    per_child: tp.Mapping[str, float],
) -> int | None:
    """Return how many more children of this shape the tenant's quota can hold.

    ``None`` when nothing can be concluded -- no quota, or a child that consumes
    none of the constrained dimensions. That is reported as unknown rather than
    inferred to be zero or unlimited: an unmeasurable quota must not silently
    become backpressure, and must not silently become permission either.

    Only dimensions the quota actually constrains are considered, and the
    binding one is the smallest. A child needing 4 TPU under a 16-TPU quota with
    8 already leased leaves room for two, whatever the CPU numbers say.
    """

    if not limits:
        return None
    headroom: int | None = None
    for dimension, limit in limits.items():
        need = float(per_child.get(dimension, 0.0))
        if need <= 0.0:
            continue
        remaining = float(limit) - float(usage.get(dimension, 0.0))
        fits = int(max(remaining, 0.0) // need)
        headroom = fits if headroom is None else min(headroom, fits)
    return headroom


__all__ = (
    "DEFAULT_EXPANSION_CEILING",
    "DEFAULT_EXPANSION_WAVE_SIZE",
    "AdmissionPlan",
    "ExpansionLimits",
    "ExpansionMode",
    "ExpansionRefused",
    "plan_expansion",
    "quota_headroom_children",
    "wave_size_for",
)
