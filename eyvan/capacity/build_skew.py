# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""What the plane does with a cell whose agent or workers run another build.

A cell's build is the exact ``source_digest`` of the ``eyvan`` package it runs,
so every Eyvan commit -- a dashboard string included -- puts every cell already
running out of step with a plane upgraded past it. The controller notices that
in two places, the idle sweep and the dispatch guard, and until this policy
existed it answered both the same way: drain the cell, let the autoscaler delete
it, and provision a replacement from the wheel the plane stages now.

That is the right answer for a cell Eyvan provisioned, because a replacement is
a new machine with the new build. It is the wrong answer for a cell that is a
claim on hardware the operator owns (``manual``, ``onprem-ssh``, an attached
TPU VM): Eyvan neither creates nor reinstalls those hosts, so deleting the cell
releases a machine that keeps running the same build, and the replacement --
a new cell id, a new per-cell token -- lands on that same stale host and is
drained again. Recycling never converges there; only an in-place upgrade does.

Kept in the capacity layer beside :mod:`eyvan.capacity.stage_policy` for the same
reason: process configuration validates the setting, and configuration cannot
reach into the control plane.
"""

from __future__ import annotations

from eyvan._internal.compat import StrEnum
from eyvan.capacity.drivers.base import backend_capabilities


class BuildSkewPolicy(StrEnum):
    """How one cell running a build other than the plane's is answered.

    ``AUTO`` is the setting's value, never a resolved policy: it reads the cell's
    backend and picks ``RECYCLE`` for capacity Eyvan provisions and ``REFUSE``
    for hardware the operator owns, which is the only default under which an
    upgrade leaves neither kind of fleet worse off than it found it.
    """

    #: Resolve per cell from its backend (see :func:`resolve_build_skew_policy`).
    AUTO = "auto"
    #: Drain the cell so the autoscaler deletes it and provisions a replacement
    #: from the wheel staged now. The behaviour before this policy existed, and
    #: still the default wherever a replacement is a new machine.
    RECYCLE = "recycle"
    #: Keep the cell, hold it out of scheduling, and refuse dispatch to it until
    #: its agent and every worker report the plane's build again -- which an
    #: in-place upgrade of the host does, and the next sweep then returns the
    #: cell to service. Nothing is deleted, so nothing has to be re-joined.
    REFUSE = "refuse"
    #: Report the skew and dispatch anyway. For an operator who knows the two
    #: builds are compatible (a plane-only change) and would rather run on stale
    #: workers than wait for an upgrade; the builds report still names both.
    WARN = "warn"


def resolve_build_skew_policy(setting: str, capacity_backend: str | None) -> BuildSkewPolicy:
    """Return the policy one cell is judged under.

    ``capacity_backend`` is the cell's own backend name (or its provider, for a
    hand-registered cell that recorded none). An explicit setting applies to
    every cell; ``auto`` refuses on existing hardware and recycles everywhere
    else, because ``existing_hardware`` is exactly the backends whose cells are
    claims on machines Eyvan never created and cannot replace.
    """

    policy = BuildSkewPolicy(setting)
    if policy is not BuildSkewPolicy.AUTO:
        return policy
    if backend_capabilities(capacity_backend).existing_hardware:
        return BuildSkewPolicy.REFUSE
    return BuildSkewPolicy.RECYCLE


__all__ = ("BuildSkewPolicy", "resolve_build_skew_policy")
