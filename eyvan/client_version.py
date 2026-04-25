# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Client/server version negotiation for the Eyvan API.

The control plane and its SDK ship together, so a client that predates a
required server change otherwise fails deep inside a request with a confusing
error. Every Eyvan SDK request carries :data:`CLIENT_VERSION_HEADER`, and a
deployment that has dropped support for older clients sets
``EYVAN_MIN_CLIENT_VERSION`` so the skew is rejected at the boundary with an
actionable message.

The gate is deliberately one-sided. A request that carries no version header is
accepted: browsers, ``curl``, and any non-SDK caller never send one, and the
gate exists to catch stale Eyvan SDKs rather than to mandate a client identity.
"""

from __future__ import annotations

CLIENT_VERSION_HEADER = "X-Eyvan-Client-Version"

_MAX_VERSION_SEGMENTS = 8


def client_version() -> str:
    """Return the running SDK version.

    Imported lazily so this module stays importable from ``eyvan.client``
    without creating a cycle through the lazily-exported package root.
    """

    from eyvan import __version__

    return __version__


def parse_version(raw: str | None) -> tuple[int, ...] | None:
    """Parse a dotted numeric version into a comparable tuple.

    Returns ``None`` for anything that is not purely dotted integers so an
    unrecognized version is treated as unknown rather than silently ordered
    against the floor.
    """

    if not isinstance(raw, str):
        return None
    candidate = raw.strip()
    if not candidate:
        return None
    segments = candidate.split(".")
    if len(segments) > _MAX_VERSION_SEGMENTS:
        return None
    numbers: list[int] = []
    for segment in segments:
        if not segment.isdigit():
            return None
        numbers.append(int(segment))
    return tuple(numbers)


def validate_minimum_client_version(minimum: str | None) -> None:
    """Raise when a configured client-version floor is not comparable.

    Called from settings construction so a malformed floor fails at startup
    rather than rejecting live traffic with an internal error.
    """

    if minimum is None:
        return
    if parse_version(minimum) is None:
        raise ValueError(f"min_client_version must be dotted integers such as '0.1.0', got {minimum!r}")


def is_client_supported(reported: str | None, minimum: str | None) -> bool:
    """Return whether a reported client version satisfies the configured floor.

    An unset floor accepts everything. A missing or unparseable header is also
    accepted -- see the module docstring for why the gate is one-sided.
    """

    floor = parse_version(minimum) if minimum else None
    if floor is None:
        return True
    candidate = parse_version(reported)
    if candidate is None:
        return True
    width = max(len(candidate), len(floor))
    padded_candidate = candidate + (0,) * (width - len(candidate))
    padded_floor = floor + (0,) * (width - len(floor))
    return padded_candidate >= padded_floor


def stale_client_message(reported: str | None, minimum: str) -> str:
    """Return the operator-facing explanation for a rejected stale client."""

    running = reported or "unknown"
    return (
        f"This Eyvan client is version {running}; the control plane requires {minimum} or newer. "
        "Upgrade the client (for example `uv sync` in the workspace, or reinstall the eyvan package) "
        "and retry."
    )
