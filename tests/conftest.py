# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Fixtures shared across the suite.

Only the PostgreSQL session fixture lives here. Everything else in this suite
builds its own harness in the module that needs it, which keeps each test file
readable on its own; a real database is an exception because starting one per
module would cost seconds per file. The other exception is daemon teardown:
any test file that isolates HOME and can start a real local daemon stops, in
teardown, everything its isolated profile recorded, using the one helper here
so no file reinvents (or skips) it.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from postgres_fixture import postgres_url

__all__ = ("postgres_url", "stop_local_daemons_recorded_under")


def stop_local_daemons_recorded_under(home: Path) -> None:
    """Stop local Eyvan daemons an isolated profile recorded, then name survivors.

    A daemon started under an isolated home outlives the test process, keeps
    its port bound with a credential from a home pytest is about to delete,
    and the next real ``eyvan`` command against that port answers 401 from a
    plane nobody owns. The PID records are owner-only by construction, so
    anything readable here was written during this test run. What the
    supervisors can prove they own, they stop -- control planes and the cell
    agents the native GCP runtime records alike; what they cannot prove (a
    foreign command under a recycled PID) they refuse to signal, and the
    liveness check then fails the test naming the survivor instead of
    silently passing.
    """

    from eyvan.capacity.providers.gcp.runtime.supervisor import LocalAgentSupervisor
    from eyvan.server.local.daemon import (
        LocalDaemonError,
        LocalDaemonSupervisor,
        process_is_alive,
        read_owner_only_record,
    )

    for record_path in sorted(home.glob(".eyvan/*.pid")):
        record = read_owner_only_record(record_path)
        if record is None:
            continue
        try:
            if record_path.name.startswith("cell-agent-"):
                LocalAgentSupervisor(endpoint=record.endpoint).stop()
            else:
                LocalDaemonSupervisor().stop(record.endpoint)
        except LocalDaemonError:
            pass
        if process_is_alive(record.pid):
            pytest.fail(
                f"a local Eyvan daemon (pid {record.pid}, {record.endpoint.url}) outlived the test; "
                "record it for teardown or patch its ensure_* helper out"
            )
