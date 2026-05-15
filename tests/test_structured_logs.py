# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""A 202-line log module whose only importer was a test.

``worker/runtime/log_records.py`` defines ``LogRecord``, ``LogPage``,
``parse_journal_json``, ``merge_pages``, cursors, per-rank attribution and an
``unavailable`` flag -- everything needed to answer "what did rank 3 print, and
is rank 2 silent or unreachable". Nothing in the package imported it.

The live contract was ``logs(remote_id) -> str``. A string cannot carry which
rank a line came from, whether it was stdout or stderr, where to resume, or the
difference between a silent workload and an unreachable host. Multi-host fan-out
was string concatenation with ``===== rank N =====`` banners.

This is the first half of closing that: the driver capability, the agent hop
that makes it reachable, and the distinction between "this backend has no
structured logs" and "this workload printed nothing".
"""

from __future__ import annotations

import json

import pytest

from eyvan.capacity.drivers.base import UnsupportedCapabilityError
from eyvan.worker.runtime.log_records import LogPage, LogRecord, merge_pages, parse_journal_json
from eyvan.worker.runtime.registry import StructuredLogExecutionDriver


def _journal(*entries: dict) -> str:
    return "\n".join(json.dumps(entry) for entry in entries)


# --------------------------------------------------------------------------- the capability


def test_the_systemd_executor_satisfies_the_structured_log_protocol() -> None:
    """A protocol rather than a getattr probe, matching the three beside it.

    ``ray_jobs`` deliberately does not implement it: a backend that cannot
    produce cursors should say so by failing the protocol rather than by
    returning something shaped like a page.
    """

    from eyvan.worker.runtime.ray_jobs import RayJobsExecutor
    from eyvan.worker.runtime.systemd_transient import SystemdTransientServiceExecutor

    assert hasattr(SystemdTransientServiceExecutor, "log_page")
    assert not isinstance(RayJobsExecutor, StructuredLogExecutionDriver)


def test_journal_json_becomes_attributed_records() -> None:
    """Priority, timestamp, PID and cursor -- every field a string discards."""

    page = parse_journal_json(
        _journal(
            {
                "MESSAGE": "step 1",
                "PRIORITY": "6",
                "__REALTIME_TIMESTAMP": "1700000000000000",
                "__CURSOR": "c1",
                "_PID": "42",
            },
            {"MESSAGE": "traceback", "PRIORITY": "3", "__CURSOR": "c2"},
        ),
        rank=3,
    )

    assert [record.message for record in page.records] == ["step 1", "traceback"]
    assert [record.stream for record in page.records] == ["stdout", "stderr"]
    assert [record.rank for record in page.records] == [3, 3]
    assert page.records[0].pid == 42
    assert page.next_cursor == "c2"


def test_an_unrecognized_priority_is_unknown_rather_than_stdout() -> None:
    """Guessing would make someone debugging a crash read stderr as ordinary output."""

    page = parse_journal_json(_journal({"MESSAGE": "x", "PRIORITY": "99", "__CURSOR": "c1"}))

    assert page.records[0].stream == "unknown"


def test_a_truncated_final_line_is_tolerated() -> None:
    """journald is being written to while it is read; a partial last line is normal.

    Failing the whole page for it would make a live read fail at random.
    """

    payload = _journal({"MESSAGE": "complete", "__CURSOR": "c1"}) + '\n{"MESSAGE": "part'

    page = parse_journal_json(payload)

    assert [record.message for record in page.records] == ["complete"]


def test_an_unreachable_rank_is_reported_in_place() -> None:
    """ "Rank 2 is silent" and "rank 2 is unreachable" are different incidents.

    A merge that dropped the unavailable rank would show a job running normally
    on the ranks that answered.
    """

    merged = merge_pages(
        {
            0: LogPage(records=(LogRecord(message="a", rank=0, cursor="c0"),), next_cursor="c0"),
            2: LogPage(unavailable=True, detail="host unreachable"),
        }
    )

    # The lines that were read stay readable -- an operator diagnosing a hang
    # needs rank 0's output even while rank 2 is unreachable.
    assert [record.message for record in merged.records] == ["a"]
    # And the gap is named rather than implied by absence.
    assert merged.truncated_ranks == (2,)
    assert "rank 2" in merged.detail
    # Not `unavailable`: some ranks answered, so this is a partial read rather
    # than a failed one, and a caller that treated it as failed would discard
    # output it actually has.
    assert merged.unavailable is False


def test_a_merge_where_every_rank_failed_is_unavailable() -> None:
    """The other side of the same distinction.

    Nothing was read from anywhere, so reporting a partial page would tell an
    operator the job is silent when in fact nothing could be reached.
    """

    merged = merge_pages({0: LogPage(unavailable=True, detail="a"), 1: LogPage(unavailable=True, detail="b")})

    assert merged.unavailable is True
    assert merged.truncated_ranks == (0, 1)


# --------------------------------------------------------------------------- the agent hop


class _PagingExecutor:
    """An execution driver that produces structured pages."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    def log_page(self, remote_id: str, *, after: str = "", limit: int = 200) -> LogPage:
        self.calls.append((remote_id, after, limit))
        return LogPage(
            records=(LogRecord(message="hello", stream="stdout", cursor="c1", rank=0),),
            next_cursor="c1",
        )


class _PlainExecutor:
    """An execution driver with only the string contract."""

    def logs(self, remote_id: str) -> str:
        return "hello\n"


class _Resolver:
    def __init__(self, executor: object) -> None:
        self._executor = executor

    def resolve(self, cell_id: str, observation: object) -> object:
        del cell_id, observation
        return self._executor


def _service(executor: object):
    from eyvan.config import EyvanSettings
    from eyvan.worker.agent.service import CellAgentService

    class _Capacity:
        def capabilities(self):
            from eyvan.worker.agent.models import AgentCapabilities

            return AgentCapabilities(executor="eyvan-native")

    service = CellAgentService.__new__(CellAgentService)
    service.settings = EyvanSettings()
    service.capacity = _Capacity()
    service.execution_backend = "eyvan-native"
    service.executors = _Resolver(executor)
    return service


def test_the_agent_serves_a_page_from_a_capable_backend(monkeypatch) -> None:
    """The hop that makes the driver capability reachable at all.

    Without it, ``log_page`` would be a method nothing calls -- which is how
    ``log_records`` spent its life.
    """

    executor = _PagingExecutor()
    service = _service(executor)
    monkeypatch.setattr(type(service), "_executor", lambda _self, _cell: executor)

    page = service.log_page("cell-a", "wl-1", after="c0", limit=50)

    assert executor.calls == [("wl-1", "c0", 50)]
    assert page["records"][0]["message"] == "hello"
    assert page["next_cursor"] == "c1"
    assert page["unavailable"] is False


def test_a_backend_without_structured_logs_says_so_rather_than_returning_nothing(monkeypatch) -> None:
    """ "No structured logs here" and "no output yet" must not look the same.

    A follower that cannot tell them apart polls forever against a backend that
    will never answer.
    """

    executor = _PlainExecutor()
    service = _service(executor)
    monkeypatch.setattr(type(service), "_executor", lambda _self, _cell: executor)

    with pytest.raises(UnsupportedCapabilityError, match="structured logs"):
        service.log_page("cell-a", "wl-1")


def test_an_unavailable_page_survives_the_agent_boundary(monkeypatch) -> None:
    """The flag exists so a caller does not report "no output" for an unreachable host."""

    class _Unavailable:
        def log_page(self, remote_id: str, *, after: str = "", limit: int = 200) -> LogPage:
            del remote_id, after, limit
            return LogPage(unavailable=True, detail="journalctl failed")

    executor = _Unavailable()
    service = _service(executor)
    monkeypatch.setattr(type(service), "_executor", lambda _self, _cell: executor)

    page = service.log_page("cell-a", "wl-1")

    assert page["unavailable"] is True
    assert page["detail"] == "journalctl failed"
    assert page["records"] == []


def test_the_structured_page_is_a_separate_route_from_the_plain_text_one() -> None:
    """Every existing consumer reads the log route as bytes.

    A ``?format=json`` parameter on the same path would put a second response
    shape behind a route declared ``PlainTextResponse``; a separate path leaves
    the old contract literally unchanged.
    """

    from eyvan.config import EyvanSettings
    from eyvan.worker.agent.app import create_agent_app

    paths = {
        f"{method} {route.path}"
        for route in create_agent_app(_service(_PagingExecutor()), EyvanSettings(agent_shared_secret="s")).routes
        if getattr(route, "methods", None)
        for method in sorted(route.methods - {"HEAD", "OPTIONS"})
    }

    assert "GET /api/v1/cells/{cell_id}/workloads/{remote_id}/logs" in paths
    assert "GET /api/v1/cells/{cell_id}/workloads/{remote_id}/log-page" in paths


# --------------------------------------------------------------------------- the systemd read


def _systemd_executor(tmp_path, runner):
    from eyvan.worker.runtime.systemd_transient import SystemdTransientServiceExecutor

    return SystemdTransientServiceExecutor(tmp_path, command_runner=runner, boot_id_reader=lambda: "boot-a")


def _record_unit(executor, remote_id: str, unit_name: str) -> None:
    """Write the durable record ``log_page`` reads the unit name out of."""

    import json as _json

    path = executor._record_path(remote_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json.dumps({"unit_name": unit_name}), encoding="utf-8")


def test_the_systemd_read_asks_journalctl_for_json_and_parses_it(tmp_path) -> None:
    """The whole point of the capability, exercised rather than assumed.

    Asserting only that the method exists let an implementation returning an
    empty page pass -- which is what a protocol check alone buys.
    """

    import subprocess

    seen: list[tuple[str, ...]] = []

    def runner(command, **_kwargs):
        seen.append(tuple(command))
        return subprocess.CompletedProcess(
            command,
            0,
            _journal(
                {"MESSAGE": "step 1", "PRIORITY": "6", "__CURSOR": "c1"},
                {"MESSAGE": "boom", "PRIORITY": "3", "__CURSOR": "c2"},
            ),
            "",
        )

    executor = _systemd_executor(tmp_path, runner)
    _record_unit(executor, "wl-1", "eyvan-wl-1.service")

    page = executor.log_page("wl-1", limit=10)

    assert any("--output=json" in " ".join(command) for command in seen), "the read must ask for structured output"
    assert [record.message for record in page.records] == ["step 1", "boom"]
    assert [record.stream for record in page.records] == ["stdout", "stderr"]
    assert page.next_cursor == "c2"


def test_the_systemd_read_resumes_after_a_cursor(tmp_path) -> None:
    """A follower must never re-read the line it already has.

    journald resumes strictly after the cursor, which is why the cursor is
    handed back opaque rather than interpreted here.
    """

    import subprocess

    seen: list[tuple[str, ...]] = []

    def runner(command, **_kwargs):
        seen.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, "", "")

    executor = _systemd_executor(tmp_path, runner)
    _record_unit(executor, "wl-1", "eyvan-wl-1.service")

    executor.log_page("wl-1", after="c1")

    joined = " ".join(seen[0])
    assert "--after-cursor=c1" in joined


def test_an_unreadable_journal_is_a_page_not_an_exception(tmp_path) -> None:
    """``logs()`` raises because a string has nowhere to put "I could not read".

    A page does, and a follower polling every second should not have to catch an
    exception to learn a host is briefly unreachable.
    """

    import subprocess

    def runner(command, **_kwargs):
        return subprocess.CompletedProcess(command, 1, "", "No journal files were found")

    executor = _systemd_executor(tmp_path, runner)
    _record_unit(executor, "wl-1", "eyvan-wl-1.service")

    page = executor.log_page("wl-1")

    assert page.unavailable is True
    assert "No journal files" in page.detail
    assert page.records == ()


def test_a_workload_with_no_durable_record_is_unavailable_rather_than_silent(tmp_path) -> None:
    """ "Never ran here" must not read as "ran and printed nothing"."""

    import subprocess

    executor = _systemd_executor(tmp_path, lambda command, **_k: subprocess.CompletedProcess(command, 0, "", ""))

    page = executor.log_page("wl-missing")

    assert page.unavailable is True
    assert page.records == ()
