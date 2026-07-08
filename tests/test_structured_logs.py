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
from pathlib import Path

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


# --------------------------------------------------------------------------- the control plane


def _cursor_helpers():
    from eyvan.server.controller.observation import _log_page_document, _split_log_cursor

    return _split_log_cursor, _log_page_document


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", (None, {})),
        ("3:0=abc", (3, {0: "abc"})),
        ("3:0=abc|1=def", (3, {0: "abc", 1: "def"})),
        ("3:s=abc|journal=cursor", (3, {0: "s=abc|journal=cursor"})),
        ("abc", (None, {})),
        ("notanint:0=abc", (None, {})),
    ],
    ids=["empty", "one-rank", "two-ranks", "legacy-single-rank", "no-attempt", "unparseable"],
)
def test_a_cursor_carries_its_attempt_and_a_position_per_rank(value: str, expected: tuple) -> None:
    """One cursor cannot resume a slice.

    Each rank has its own journal and its own position in it. ``merge_pages``
    says the same thing by refusing to invent a single cursor, and a caller that
    ignored that would hand rank 1 a position from rank 0's journal.

    A value with no attempt is "no opinion" rather than attempt zero, so a first
    read is not mistaken for a superseded one.
    """

    split, _document = _cursor_helpers()

    assert split(value) == expected


def test_the_returned_cursor_carries_every_rank_and_the_attempt() -> None:
    """Without the attempt, a follower resumes a retry from its predecessor's place."""

    _split, document = _cursor_helpers()

    page = document(LogPage(), attempt=4, cursors={0: "c9", 1: "c7"})

    assert page["next_cursor"].startswith("eyvan-log-v2.")
    assert "|" not in page["next_cursor"]
    assert _split(page["next_cursor"]) == (4, {0: "c9", 1: "c7"})
    assert page["attempt"] == 4


def test_a_rank_that_reported_no_position_is_left_out_rather_than_sent_empty() -> None:
    """An empty cursor means "wherever you were", so writing it down says nothing."""

    _split, document = _cursor_helpers()

    encoded = document(LogPage(), attempt=2, cursors={0: "c1", 1: ""})["next_cursor"]
    assert _split(encoded) == (2, {0: "c1"})


def test_an_empty_page_returns_an_empty_cursor_rather_than_a_bare_attempt() -> None:
    """Resuming from empty means "wherever you were"; a start cursor replays everything."""

    _split, document = _cursor_helpers()

    assert document(LogPage(), attempt=2)["next_cursor"] == ""


class _PagingAgent:
    """A cell agent that serves structured pages and records what it was asked."""

    def __init__(self, *, message: str = "line", fail: bool = False) -> None:
        self.message = message
        self.fail = fail
        self.calls: list[tuple[str, str, str, int]] = []

    def log_page(self, cell_id, remote_id, *, after="", limit=200):
        self.calls.append((cell_id, remote_id, after, limit))
        if self.fail:
            raise RuntimeError("agent unreachable")
        return {
            "records": [{"message": self.message, "stream": "stdout", "cursor": "c1", "pid": 7}],
            "next_cursor": "c1",
            "unavailable": False,
            "detail": "",
            "truncated_ranks": [],
        }


async def _controller_with(agent, *, attempt: int = 1):
    from datetime import timedelta

    from eyvan.config import EyvanSettings
    from eyvan.records import (
        CellRecord,
        CellState,
        CommandExecution,
        LeaseRecord,
        LeaseState,
        ResourceGang,
        ResourceVector,
        WorkloadDefinition,
        WorkloadRecord,
        WorkloadState,
        utcnow,
    )
    from eyvan.server.controller import EyvanController
    from eyvan.server.persistence.memory import InMemoryRepository
    from eyvan.server.service.control_plane import ControlPlaneService
    from eyvan.server.service.scheduling import CoreSchedulingService

    repository = InMemoryRepository()
    service = ControlPlaneService(repository, scheduler=CoreSchedulingService())
    controller = EyvanController(
        repository,
        service,
        settings=EyvanSettings(),
        agent_factory=lambda _cell: agent,
    )
    async with repository.transaction() as transaction:
        await transaction.put_lease(
            LeaseRecord(
                id="lease-1",
                workload_id="wl-1",
                cell_id="cell-a",
                org_id="acme",
                project_id="research",
                state=LeaseState.ACTIVE,
                resources=ResourceGang(replicas=1, per_replica=ResourceVector(cpu=1.0)),
                generation=1,
                fencing_token="token-1",
                expires_at=utcnow() + timedelta(hours=24),
            )
        )
        await transaction.put_cell(
            CellRecord(
                id="cell-a",
                provider="local",
                region="local",
                state=CellState.READY,
                capacity=ResourceVector(cpu=8),
                allocatable=ResourceVector(cpu=8),
                endpoint="http://agent:8090",
            )
        )
        await transaction.put_workload(
            WorkloadRecord(
                id="wl-1",
                org_id="acme",
                project_id="research",
                spec=WorkloadDefinition(name="t", execution=CommandExecution(command="echo hi")),
                state=WorkloadState.RUNNING,
                attempt=attempt,
                lease_id="lease-1",
                remote_id="remote-1",
                created_by="alice",
            )
        )
    return controller


async def test_a_cursor_from_a_superseded_attempt_restarts_rather_than_continues() -> None:
    """The crux of this endpoint.

    A retry replaces the remote execution entirely, so the old cursor means
    nothing to the new one. Continuing quietly would show the new attempt's
    first lines appended to the old attempt's tail, and a follower would
    conclude the job resumed where it left off.
    """

    agent = _PagingAgent()
    controller = await _controller_with(agent, attempt=2)

    page = await controller.workload_log_page("wl-1", after="1:0=old-cursor")

    assert page["attempt_changed"] is True
    assert page["attempt"] == 2
    # The stale cursor is not handed to the backend, because the backend of the
    # new attempt has never heard of it.
    assert agent.calls[0][2] == ""


async def test_a_cursor_from_the_current_attempt_resumes() -> None:
    """The check must not restart a follower that is simply keeping up."""

    agent = _PagingAgent()
    controller = await _controller_with(agent, attempt=2)

    page = await controller.workload_log_page("wl-1", after="2:0=c1")

    assert page["attempt_changed"] is False
    assert agent.calls[0][2] == "c1"


async def test_an_unreachable_rank_does_not_blank_the_ranks_that_answered() -> None:
    """One failing agent must not turn a running job into a silent one."""

    agent = _PagingAgent(fail=True)
    controller = await _controller_with(agent)

    page = await controller.workload_log_page("wl-1")

    assert page["unavailable"] is True
    assert "unreachable" in page["detail"]


async def test_a_workload_with_no_remote_execution_yields_an_empty_page() -> None:
    """Queued work has produced no output, which is not an error."""

    from eyvan.config import EyvanSettings
    from eyvan.records import CommandExecution, WorkloadDefinition, WorkloadRecord
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
                id="wl-pending",
                org_id="acme",
                project_id="research",
                spec=WorkloadDefinition(name="t", execution=CommandExecution(command="echo hi")),
                created_by="alice",
            )
        )

    page = await controller.workload_log_page("wl-pending")

    assert page["records"] == []
    assert page["unavailable"] is False


# --------------------------------------------------------------------------- surviving release


def _artifact_access(tmp_path):
    from eyvan.artifacts.managed import ArtifactTokenSigner, LocalManagedArtifactStore
    from eyvan.artifacts.transfers import ManagedArtifactAccess

    return ManagedArtifactAccess(
        store=LocalManagedArtifactStore(tmp_path / "artifacts"),
        signer=ArtifactTokenSigner(b"k" * 32, default_ttl_seconds=3600),
        public_url="https://eyvan.test",
        token_ttl_seconds=3600,
    )


async def test_secret_bearing_workload_output_is_not_streamed_or_archived(tmp_path) -> None:
    marker = "embedded-control-plane-secret"
    agent = _PagingAgent(message=f"prefix-{marker}-suffix")
    controller = await _controller_with(agent)
    controller.artifact_access = _artifact_access(tmp_path)
    async with controller.repository.transaction() as transaction:
        workload = await transaction.get_workload("wl-1")
        secret_spec = workload.spec.model_copy(update={"secret_mounts": {"HF_TOKEN": "gcp-secret://project/token"}})
        workload = workload.model_copy(update={"spec": secret_spec})
        await transaction.put_workload(workload)

    page = await controller.workload_log_page(workload.id)
    await controller._archive_terminal_logs(workload)

    assert page["records"] == []
    assert page["unavailable"] is True
    assert marker not in json.dumps(page)
    assert agent.calls == []
    assert not any((tmp_path / "artifacts").rglob("*"))


async def test_logs_survive_the_cell_that_produced_them(tmp_path) -> None:
    """The failure this archive exists for.

    journald lives on the cell. Releasing a cell destroys it, so an attempt's
    output disappears exactly when the capacity bill stops -- which is also when
    someone starts asking what happened.
    """

    agent = _PagingAgent(message="the last thing it printed")
    controller = await _controller_with(agent)
    controller.artifact_access = _artifact_access(tmp_path)
    workload_id = "wl-1"

    async with controller.repository.transaction() as transaction:
        workload = await transaction.get_workload(workload_id)
    await controller._archive_terminal_logs(workload)

    # Now the cell is gone: no lease, no journal, no agent.
    async with controller.repository.transaction() as transaction:
        stored = await transaction.get_workload(workload_id)
        await transaction.put_workload(stored.model_copy(update={"lease_id": None}))
        lease = await transaction.get_lease("lease-1")
        await transaction.put_lease(lease.model_copy(update={"state": lease.state.__class__.RELEASED}))

    page = await controller.workload_log_page(workload_id)

    assert [record["message"] for record in page["records"]] == ["the last thing it printed"]
    assert page["detail"] == "archived"


async def test_an_attempt_with_no_archive_says_so_rather_than_looking_silent(tmp_path) -> None:
    """After release there is no other source.

    "Printed nothing" and "the record was lost" are different answers, and only
    one of them means someone should go looking elsewhere.
    """

    agent = _PagingAgent()
    controller = await _controller_with(agent)
    controller.artifact_access = _artifact_access(tmp_path)

    async with controller.repository.transaction() as transaction:
        stored = await transaction.get_workload("wl-1")
        await transaction.put_workload(stored.model_copy(update={"lease_id": None}))
        lease = await transaction.get_lease("lease-1")
        await transaction.put_lease(lease.model_copy(update={"state": lease.state.__class__.RELEASED}))

    page = await controller.workload_log_page("wl-1")

    assert page["unavailable"] is True
    assert page["records"] == []


async def test_each_attempt_archives_separately(tmp_path) -> None:
    """A retry must not overwrite the output that explains why it retried.

    That output is the one an operator is most likely to be looking for.
    """

    from eyvan.artifacts.managed import log_uri

    access = _artifact_access(tmp_path)
    first = await _controller_with(_PagingAgent(message="attempt one"), attempt=1)
    first.artifact_access = access
    async with first.repository.transaction() as transaction:
        workload = await transaction.get_workload("wl-1")
    await first._archive_terminal_logs(workload)

    second = await _controller_with(_PagingAgent(message="attempt two"), attempt=2)
    second.artifact_access = access
    async with second.repository.transaction() as transaction:
        retried = await transaction.get_workload("wl-1")
    await second._archive_terminal_logs(retried)

    stored_first = access.store.get(log_uri("wl-1", 1, 0), org_id="acme", project_id="research")
    stored_second = access.store.get(log_uri("wl-1", 2, 0), org_id="acme", project_id="research")

    assert b"attempt one" in stored_first
    assert b"attempt two" in stored_second


async def test_archive_overflow_is_trimmed_per_rank_and_replays_as_visibly_partial(tmp_path) -> None:
    """The 1,001st line is a sentinel, not a line silently discarded."""

    from eyvan.artifacts.managed import log_uri
    from eyvan.server.controller.settlement import _LOG_ARCHIVE_LINE_LIMIT

    class _ChattyAgent:
        def __init__(self) -> None:
            self.requested_limits: list[int] = []

        def log_page(self, _cell_id, _remote_id, *, after="", limit=200):
            assert after == ""
            self.requested_limits.append(limit)
            return {
                "records": [
                    {
                        "message": f"rank-{rank}-line-{line}",
                        "stream": "stderr" if rank else "stdout",
                        "rank": rank,
                        "pid": rank + 10,
                    }
                    for rank in (0, 1)
                    for line in range(limit)
                ],
                "next_cursor": "archive-tail",
                "unavailable": False,
                "detail": "",
                "truncated_ranks": [],
            }

    agent = _ChattyAgent()
    controller = await _controller_with(agent)
    controller.artifact_access = _artifact_access(tmp_path)
    async with controller.repository.transaction() as transaction:
        cell = await transaction.get_cell("cell-a")
        await transaction.put_cell(cell.model_copy(update={"runtime_process_count": 2}))
        workload = await transaction.get_workload("wl-1")
    await controller._archive_terminal_logs(workload)

    assert agent.requested_limits == [_LOG_ARCHIVE_LINE_LIMIT + 1]
    manifest_line = controller.artifact_access.store.get(
        log_uri("wl-1", 1, 0), org_id="acme", project_id="research"
    ).splitlines()[0]
    manifest = json.loads(manifest_line)[1]
    assert manifest["line_limit_per_rank"] == _LOG_ARCHIVE_LINE_LIMIT
    assert manifest["truncated_ranks"] == [0, 1]

    async with controller.repository.transaction() as transaction:
        stored = await transaction.get_workload("wl-1")
        await transaction.put_workload(stored.model_copy(update={"lease_id": None}))
        lease = await transaction.get_lease("lease-1")
        await transaction.put_lease(lease.model_copy(update={"state": lease.state.__class__.RELEASED}))
    replayed = await controller.workload_log_page("wl-1")

    assert len(replayed["records"]) == 2 * _LOG_ARCHIVE_LINE_LIMIT
    assert replayed["truncated_ranks"] == [0, 1]
    assert replayed["unavailable"] is False
    by_rank = {rank: [record for record in replayed["records"] if record["rank"] == rank] for rank in (0, 1)}
    for rank, records in by_rank.items():
        assert records[0]["message"] == f"rank-{rank}-line-1"
        assert records[-1]["message"] == f"rank-{rank}-line-{_LOG_ARCHIVE_LINE_LIMIT}"
        assert {record["stream"] for record in records} == ({"stdout"} if rank == 0 else {"stderr"})


async def test_sparse_high_rank_archive_writes_are_bounded_and_direct(tmp_path) -> None:
    """A manifest rank of one million must not create one million gap objects."""

    from eyvan.artifacts.managed import log_uri

    class _CountingStore:
        def __init__(self, backing) -> None:
            self.backing = backing
            self.put_uris: list[str] = []

        def put(self, uri, payload, **scope):
            self.put_uris.append(uri)
            return self.backing.put(uri, payload, **scope)

        def get(self, uri, **scope):
            return self.backing.get(uri, **scope)

    controller = await _controller_with(_PagingAgent())
    access = _artifact_access(tmp_path)
    counting = _CountingStore(access.store)
    object.__setattr__(access, "store", counting)
    controller.artifact_access = access
    high_rank = 1_000_000

    async def sparse_page(_workload_id, *, after="", limit=200):
        assert after == ""
        assert limit == 1001
        return {
            "records": [{"message": "survived", "stream": "stdout", "rank": high_rank}],
            "truncated_ranks": [high_rank + 1],
            "unavailable": False,
        }

    controller.workload_log_page = sparse_page
    workload = await _only_workload(controller)
    await controller._archive_terminal_logs(workload)

    assert counting.put_uris == [
        log_uri("wl-1", 1, 0),
        log_uri("wl-1", 1, high_rank),
        log_uri("wl-1", 1, high_rank + 1),
    ]
    replayed = await controller._archived_log_page(workload)
    assert [(record.rank, record.message) for record in replayed.records] == [(high_rank, "survived")]
    assert replayed.truncated_ranks == (high_rank + 1,)


async def test_checked_in_legacy_archive_is_read_by_the_manifest_aware_reader(tmp_path) -> None:
    """Terminal attempts written before manifests remain readable after upgrade."""

    from datetime import UTC, datetime

    from eyvan.artifacts.managed import log_uri

    controller = await _controller_with(_PagingAgent())
    controller.artifact_access = _artifact_access(tmp_path)
    fixtures = Path(__file__).parent / "fixtures" / "log_archives"
    for rank in (0, 1):
        controller.artifact_access.store.put(
            log_uri("wl-1", 1, rank),
            (fixtures / f"legacy-rank-{rank}.jsonl").read_bytes(),
            org_id="acme",
            project_id="research",
        )

    page = await controller._archived_log_page(await _only_workload(controller))

    assert [(record.rank, record.message, record.stream) for record in page.records] == [
        (1, "legacy earlier stderr", "stderr"),
        (0, "legacy later stdout", "stdout"),
    ]
    assert page.records[0].timestamp == datetime(2026, 8, 1, 12, 0, 1, tzinfo=UTC)
    assert page.truncated_ranks == ()


async def test_new_manifest_archive_retains_the_old_contiguous_jsonl_shape(tmp_path) -> None:
    """A rolling old reader ignores the array manifest and reaches sparse low ranks."""

    from eyvan.artifacts.managed import log_uri

    controller = await _controller_with(_PagingAgent())
    controller.artifact_access = _artifact_access(tmp_path)

    async def sparse_page(_workload_id, *, after="", limit=200):
        del after
        assert limit == 1001
        return {
            "records": [
                {"message": "rank zero", "stream": "stdout", "rank": 0},
                {"message": "rank two", "stream": "stderr", "rank": 2},
            ],
            "truncated_ranks": [],
            "unavailable": False,
        }

    controller.workload_log_page = sparse_page
    await controller._archive_terminal_logs(await _only_workload(controller))

    # Frozen pre-manifest reader shape: contiguous scan, stop on first missing
    # object, and render only JSON objects. The new manifest is an array so it is
    # ignored rather than appearing as an empty message.
    old_shape_records: list[tuple[int, str]] = []
    for rank in range(64):
        try:
            payload = controller.artifact_access.store.get(
                log_uri("wl-1", 1, rank), org_id="acme", project_id="research"
            )
        except Exception:
            break
        for line in payload.splitlines():
            entry = json.loads(line)
            if isinstance(entry, dict):
                old_shape_records.append((rank, str(entry.get("message", ""))))

    assert old_shape_records == [(0, "rank zero"), (2, "rank two")]


async def test_an_archive_failure_does_not_block_settlement(tmp_path) -> None:
    """A finished job must be recorded as finished even if its logs could not be saved.

    Aborting settlement here would leave a completed workload holding its lease
    because its logs were unreadable -- turning a diagnostic gap into a stranded
    accelerator.
    """

    class _BrokenStore:
        def put(self, *_args, **_kwargs):
            raise RuntimeError("storage is down")

    access = _artifact_access(tmp_path)
    object.__setattr__(access, "store", _BrokenStore())
    controller = await _controller_with(_PagingAgent())
    controller.artifact_access = access

    async with controller.repository.transaction() as transaction:
        workload = await transaction.get_workload("wl-1")

    # Must not raise.
    await controller._archive_terminal_logs(workload)


async def test_nothing_is_archived_without_managed_artifacts(tmp_path) -> None:
    """Local development has nowhere to put them and the journal is on the same machine."""

    controller = await _controller_with(_PagingAgent())
    assert controller.artifact_access is None

    await controller._archive_terminal_logs(await _only_workload(controller))


async def _only_workload(controller):
    async with controller.repository.transaction() as transaction:
        return await transaction.get_workload("wl-1")


async def test_the_release_path_itself_archives(tmp_path) -> None:
    """Archival has to be wired into release, not merely callable.

    Calling the archive helper directly proves it works and says nothing about
    whether anything invokes it -- which is precisely how this package's
    recurring defect looks. This drives the real terminal-release path and then
    reads the archive back.
    """

    from eyvan.artifacts.managed import log_uri
    from eyvan.records import WorkloadState

    access = _artifact_access(tmp_path)
    controller = await _controller_with(_PagingAgent(message="final line"))
    controller.artifact_access = access

    async with controller.repository.transaction() as transaction:
        workload = await transaction.get_workload("wl-1")
        lease = await transaction.get_lease("lease-1")

    await controller._release_terminal(workload, lease, WorkloadState.SUCCEEDED, "workload.succeeded")

    stored = access.store.get(log_uri("wl-1", 1, 0), org_id="acme", project_id="research")
    assert b"final line" in stored


# --------------------------------------------------------------------------- retention


async def _collection_service(tmp_path):
    from eyvan.artifacts.managed import LocalManagedArtifactStore
    from eyvan.server.persistence.memory import InMemoryRepository
    from eyvan.server.service.control_plane import ControlPlaneService
    from eyvan.server.service.scheduling import CoreSchedulingService

    store = LocalManagedArtifactStore(tmp_path / "artifacts")
    service = ControlPlaneService(
        InMemoryRepository(),
        scheduler=CoreSchedulingService(),
        artifact_store=store,
    )
    return service, store


async def _index(service, uri: str, *, age_days: int) -> None:
    from datetime import timedelta

    from eyvan.records import ManagedArtifactRecord, utcnow

    async with service.repository.transaction() as transaction:
        await transaction.put_managed_artifact(
            ManagedArtifactRecord(
                uri=uri,
                org_id="acme",
                project_id="research",
                kind=uri.split("//")[1].split("/")[0],
                size_bytes=10,
                created_at=utcnow() - timedelta(days=age_days),
            )
        )


async def test_logs_outlive_the_general_artifact_window(tmp_path) -> None:
    """A callable can be rebuilt from source; an attempt's logs cannot.

    Sharing one window means either deleting the evidence at seven days or
    keeping every pickle for a month. Neither is the answer, which is why the
    window is per kind.
    """

    from eyvan.artifacts.managed import callable_uri, log_uri

    service, _store = await _collection_service(tmp_path)
    stale_callable = callable_uri("a" * 64)
    archived_log = log_uri("wl-1", 1, 0)
    await _index(service, stale_callable, age_days=10)
    await _index(service, archived_log, age_days=10)

    collection = await service.collect_artifacts_once(
        org_id="acme",
        project_id="research",
        grace_period_s=7 * 24 * 3600,
        retention_by_kind={"logs": 30 * 24 * 3600},
        dry_run=True,
    )

    assert stale_callable in collection.collected_uris
    assert archived_log not in collection.collected_uris, "logs were collected on the general window"


async def test_logs_are_collected_once_their_own_window_passes(tmp_path) -> None:
    """A longer window is still a window; unbounded log storage is the other failure."""

    from eyvan.artifacts.managed import log_uri

    service, _store = await _collection_service(tmp_path)
    archived_log = log_uri("wl-1", 1, 0)
    await _index(service, archived_log, age_days=45)

    collection = await service.collect_artifacts_once(
        org_id="acme",
        project_id="research",
        grace_period_s=7 * 24 * 3600,
        retention_by_kind={"logs": 30 * 24 * 3600},
        dry_run=True,
    )

    assert archived_log in collection.collected_uris


async def test_a_window_of_none_keeps_logs_forever(tmp_path) -> None:
    """``EYVAN_LOG_RETENTION_S=0`` means keep them, the same "zero disables"
    convention the other retention windows use.

    Spelled as ``None`` rather than an infinite window because
    ``timedelta(seconds=inf)`` overflows -- the clever version does not run.
    """

    from eyvan.artifacts.managed import log_uri

    service, _store = await _collection_service(tmp_path)
    archived_log = log_uri("wl-1", 1, 0)
    await _index(service, archived_log, age_days=4000)

    collection = await service.collect_artifacts_once(
        org_id="acme",
        project_id="research",
        grace_period_s=7 * 24 * 3600,
        retention_by_kind={"logs": None},
        dry_run=True,
    )

    assert archived_log not in collection.collected_uris


def test_the_controller_passes_the_log_window_it_was_configured_with() -> None:
    """A setting nothing reads is the shape this whole plan is about."""

    import inspect

    from eyvan.server.controller import settlement

    source = inspect.getsource(settlement._ControllerSettlement.collect_artifacts_once)

    assert "log_retention_s" in source
    assert "retention_by_kind" in source


async def test_a_follower_can_actually_advance_through_the_controller() -> None:
    """The defect this replaced: a cursor that never came back.

    ``merge_pages`` returns an empty ``next_cursor`` on purpose -- no single
    value resumes N ranks -- so projecting the merged page's cursor onto the
    wire produced an empty one on every read. A follower polling this endpoint
    would re-read the same tail forever and never advance, and every test that
    checked the cursor did so on a page built by hand rather than one that came
    back through here.
    """

    agent = _PagingAgent()
    controller = await _controller_with(agent, attempt=3)

    first = await controller.workload_log_page("wl-1")

    assert first["next_cursor"].startswith("eyvan-log-v2."), "a follower has nowhere to resume from"
    assert _cursor_helpers()[0](first["next_cursor"]) == (3, {0: "c1"})

    second = await controller.workload_log_page("wl-1", after=first["next_cursor"])

    assert second["attempt_changed"] is False
    assert agent.calls[-1][2] == "c1", "the position did not reach the backend"


async def test_each_rank_is_asked_from_its_own_position() -> None:
    """Handing rank 1 rank 0's cursor asks journald to resume from an entry it never wrote.

    The two ranks are different journals on different hosts; their cursors are
    not comparable, let alone interchangeable.
    """

    from eyvan.server.controller.observation import _split_log_cursor

    attempt, cursors = _split_log_cursor("3:0=from-rank-zero|1=from-rank-one")

    assert attempt == 3
    assert cursors == {0: "from-rank-zero", 1: "from-rank-one"}


class _PerRankAgent:
    """Records exactly which cursor each cell was asked from."""

    def __init__(self) -> None:
        self.asked: dict[str, str] = {}

    def log_page(self, cell_id, remote_id, *, after="", limit=200):
        del remote_id, limit
        self.asked[cell_id] = after
        return {
            "records": [{"message": f"from {cell_id}", "stream": "stdout", "cursor": f"{cell_id}-next"}],
            "next_cursor": f"{cell_id}-next",
            "unavailable": False,
            "detail": "",
            "truncated_ranks": [],
        }


async def _gang_controller(agent, *, runtime_process_counts: tuple[int, int] = (1, 1)):
    """A two-cell gang whose durable rank widths come from provider observation."""

    from datetime import timedelta

    from eyvan.config import EyvanSettings
    from eyvan.records import (
        CellRecord,
        CellState,
        CommandExecution,
        LeaseRecord,
        LeaseState,
        ResourceGang,
        ResourceVector,
        WorkloadDefinition,
        WorkloadRecord,
        WorkloadState,
        utcnow,
    )
    from eyvan.server.controller import EyvanController
    from eyvan.server.persistence.memory import InMemoryRepository
    from eyvan.server.service.control_plane import ControlPlaneService
    from eyvan.server.service.scheduling import CoreSchedulingService

    repository = InMemoryRepository()
    controller = EyvanController(
        repository,
        ControlPlaneService(repository, scheduler=CoreSchedulingService()),
        settings=EyvanSettings(),
        agent_factory=lambda _cell: agent,
    )
    async with repository.transaction() as transaction:
        for index, (cell_id, process_count) in enumerate(zip(("cell-a", "cell-b"), runtime_process_counts, strict=True)):
            if process_count == 1:
                cell = CellRecord(
                    id=cell_id,
                    provider="local",
                    region="local",
                    state=CellState.READY,
                    capacity=ResourceVector(cpu=8),
                    allocatable=ResourceVector(cpu=8),
                    endpoint="http://agent:8090",
                    runtime_process_count=1,
                )
            else:
                # One allocated TPU slice, not ``process_count`` slices. A
                # v5p-16 has two native-worker/JAX endpoints and v5p-32 has four.
                accelerator_type = "v5p-16" if process_count == 2 else "v5p-32"
                tpu = 8 if process_count == 2 else 16
                cell = CellRecord(
                    id=cell_id,
                    provider="gcp",
                    capacity_backend="gcp-tpu",
                    provider_project="provider-project",
                    region="us-central1",
                    zone="us-central1-a",
                    state=CellState.READY,
                    capacity=ResourceVector(cpu=32, tpu=tpu),
                    allocatable=ResourceVector(cpu=32, tpu=tpu),
                    accelerator_type=accelerator_type,
                    endpoint="http://agent:8090",
                    worker_count=1,
                    observed_tpu_vms=process_count,
                    runtime_process_count=process_count,
                )
            await transaction.put_cell(cell)
            await transaction.put_lease(
                LeaseRecord(
                    id=f"lease-{index}",
                    workload_id="wl-gang",
                    cell_id=cell_id,
                    org_id="acme",
                    project_id="research",
                    state=LeaseState.ACTIVE,
                    resources=ResourceGang(replicas=1, per_replica=ResourceVector(cpu=1.0)),
                    generation=1,
                    fencing_token=f"token-{index}",
                    expires_at=utcnow() + timedelta(hours=24),
                )
            )
        await transaction.put_workload(
            WorkloadRecord(
                id="wl-gang",
                org_id="acme",
                project_id="research",
                spec=WorkloadDefinition(name="t", execution=CommandExecution(command="echo hi")),
                state=WorkloadState.RUNNING,
                attempt=1,
                lease_id="lease-0",
                lease_ids=["lease-0", "lease-1"],
                remote_id="remote-1",
                created_by="alice",
            )
        )
    return controller


async def test_every_rank_of_a_gang_is_asked_from_its_own_position() -> None:
    """The case a single-rank test cannot distinguish.

    Two ranks are two journals on two hosts. Their cursors are not comparable,
    let alone interchangeable -- handing rank 1 rank 0's cursor asks journald to
    resume from an entry it never wrote, and journald's answer to that is not
    "start from the beginning", it is undefined.
    """

    agent = _PerRankAgent()
    controller = await _gang_controller(agent)

    first = await controller.workload_log_page("wl-gang")
    assert _cursor_helpers()[0](first["next_cursor"]) == (1, {0: "cell-a-next", 1: "cell-b-next"})

    await controller.workload_log_page("wl-gang", after=first["next_cursor"])

    assert agent.asked == {"cell-a": "cell-a-next", "cell-b": "cell-b-next"}


class _JournalHost:
    """A two-line journal with backend cursors that contain legacy delimiters."""

    def __init__(
        self,
        name: str,
        *,
        fail_once_after_first: bool = False,
        always_unreachable: bool = False,
    ) -> None:
        self.name = name
        self.first_cursor = f"s={name}|position=1"
        self.second_cursor = f"s={name}|position=2"
        self.fail_once_after_first = fail_once_after_first
        self.always_unreachable = always_unreachable
        self.failed = False
        self.seen_after: list[str] = []

    def log_page(self, _remote_id: str, *, after: str = "", limit: int = 200) -> LogPage:
        del limit
        self.seen_after.append(after)
        if self.always_unreachable:
            raise RuntimeError("inner host is unreachable")
        if not after:
            return LogPage(
                records=(LogRecord(message=f"{self.name}:0", stream="stdout", cursor=self.first_cursor),),
                next_cursor=self.first_cursor,
            )
        if after == self.first_cursor:
            if self.fail_once_after_first and not self.failed:
                self.failed = True
                raise RuntimeError("inner host is unreachable")
            return LogPage(
                records=(LogRecord(message=f"{self.name}:1", stream="stdout", cursor=self.second_cursor),),
                next_cursor=self.second_cursor,
            )
        if after == self.second_cursor:
            # No new line means no newly issued cursor. The fan-out layers must
            # retain the position they supplied rather than restart this host.
            return LogPage()
        raise AssertionError(f"{self.name} received foreign cursor {after!r}")


class _MultiHostAgent:
    def __init__(self, hosts: dict[str, tuple[_JournalHost, ...]]) -> None:
        from eyvan.worker.runtime.native_slice import NativeSliceExecutor

        self.slices = {}
        self.unreachable_cells: set[str] = set()
        for cell_id, cell_hosts in hosts.items():
            executor = NativeSliceExecutor.__new__(NativeSliceExecutor)
            executor._workers = cell_hosts
            self.slices[cell_id] = executor

    def log_page(self, cell_id, remote_id, *, after="", limit=200):
        if cell_id in self.unreachable_cells:
            raise RuntimeError("whole cell is unreachable")
        page = self.slices[cell_id].log_page(remote_id, after=after, limit=limit)
        return {
            "records": [dict(record.as_json()) for record in page.records],
            "next_cursor": page.next_cursor,
            "unavailable": page.unavailable,
            "detail": page.detail,
            "truncated_ranks": list(page.truncated_ranks),
        }


async def test_multi_host_cells_keep_global_ranks_and_resume_every_inner_journal() -> None:
    """Exercise controller -> cell -> inner-host cursor composition end to end."""

    from eyvan.worker.runtime.log_records import split_log_cursor

    hosts = {
        "cell-a": (_JournalHost("a0"), _JournalHost("a1")),
        "cell-b": (_JournalHost("b0"), _JournalHost("b1", fail_once_after_first=True)),
    }
    controller = await _gang_controller(_MultiHostAgent(hosts), runtime_process_counts=(2, 2))
    async with controller.repository.transaction() as transaction:
        cells = await transaction.list_cells()
    assert [(cell.worker_count, cell.runtime_process_count) for cell in sorted(cells, key=lambda cell: cell.id)] == [
        (1, 2),
        (1, 2),
    ]

    first = await controller.workload_log_page("wl-gang")
    assert [(record["rank"], record["message"]) for record in first["records"]] == [
        (0, "a0:0"),
        (1, "a1:0"),
        (2, "b0:0"),
        (3, "b1:0"),
    ]
    assert first["truncated_ranks"] == []
    attempt, cell_cursors = split_log_cursor(first["next_cursor"])
    assert attempt == 1
    assert set(cell_cursors) == {0, 1}
    _inner_attempt, cell_a_cursors = split_log_cursor(cell_cursors[0])
    _inner_attempt, cell_b_cursors = split_log_cursor(cell_cursors[1])
    assert cell_a_cursors == {0: hosts["cell-a"][0].first_cursor, 1: hosts["cell-a"][1].first_cursor}
    assert cell_b_cursors == {0: hosts["cell-b"][0].first_cursor, 1: hosts["cell-b"][1].first_cursor}
    assert all("|" in cursor for cursor in (*cell_a_cursors.values(), *cell_b_cursors.values()))

    second = await controller.workload_log_page("wl-gang", after=first["next_cursor"])
    assert [(record["rank"], record["message"]) for record in second["records"]] == [
        (0, "a0:1"),
        (1, "a1:1"),
        (2, "b0:1"),
    ]
    assert second["truncated_ranks"] == [3]
    assert second["unavailable"] is False

    # The unreachable host's last good inner cursor survives the outer page.
    _attempt, second_cells = split_log_cursor(second["next_cursor"])
    _attempt, second_cell_b = split_log_cursor(second_cells[1])
    assert second_cell_b[1] == hosts["cell-b"][1].first_cursor

    third = await controller.workload_log_page("wl-gang", after=second["next_cursor"])
    assert [(record["rank"], record["message"]) for record in third["records"]] == [(3, "b1:1")]
    assert third["truncated_ranks"] == []

    all_lines = [record["message"] for page in (first, second, third) for record in page["records"]]
    assert sorted(all_lines) == ["a0:0", "a0:1", "a1:0", "a1:1", "b0:0", "b0:1", "b1:0", "b1:1"]
    assert len(all_lines) == len(set(all_lines)), "a resumed journal duplicated a line"
    assert hosts["cell-b"][1].seen_after == ["", hosts["cell-b"][1].first_cursor, hosts["cell-b"][1].first_cursor]


async def test_unequal_cell_widths_stay_stable_when_a_whole_and_inner_host_are_unreachable() -> None:
    """Lost endpoints cannot collapse offsets or renumber following JAX ranks."""

    from eyvan.worker.runtime.log_records import split_log_cursor

    hosts = {
        "cell-a": (_JournalHost("a0"), _JournalHost("a1")),
        "cell-b": (
            _JournalHost("b0"),
            _JournalHost("b1", always_unreachable=True),
            _JournalHost("b2"),
            _JournalHost("b3"),
        ),
    }
    agent = _MultiHostAgent(hosts)
    controller = await _gang_controller(agent, runtime_process_counts=(2, 4))

    first = await controller.workload_log_page("wl-gang")
    assert [(record["rank"], record["message"]) for record in first["records"]] == [
        (0, "a0:0"),
        (1, "a1:0"),
        (2, "b0:0"),
        (4, "b2:0"),
        (5, "b3:0"),
    ]
    assert first["truncated_ranks"] == [3]
    _attempt, first_cells = split_log_cursor(first["next_cursor"])

    # Losing the whole two-process first cell must not move cell B from offset 2
    # to offset 0, and its last good nested cursor must remain resumable.
    agent.unreachable_cells.add("cell-a")
    second = await controller.workload_log_page("wl-gang", after=first["next_cursor"])
    assert [(record["rank"], record["message"]) for record in second["records"]] == [
        (2, "b0:1"),
        (4, "b2:1"),
        (5, "b3:1"),
    ]
    assert second["truncated_ranks"] == [0, 1, 3]
    _attempt, second_cells = split_log_cursor(second["next_cursor"])
    assert second_cells[0] == first_cells[0], "an unreachable whole cell advanced or lost its nested cursor"


async def test_a_dropped_out_of_range_record_does_not_advance_its_cell_cursor() -> None:
    """A malformed rank cannot be skipped permanently by accepting its cursor."""

    from eyvan.worker.runtime.log_records import split_log_cursor

    class _MalformedRankAgent:
        def __init__(self) -> None:
            self.calls = 0

        def log_page(self, _cell_id, _remote_id, *, after="", limit=200):
            del limit
            self.calls += 1
            if self.calls == 1:
                assert after == ""
                return {
                    "records": [{"message": "kept", "rank": 0}],
                    "next_cursor": "last-good",
                    "unavailable": False,
                    "detail": "",
                    "truncated_ranks": [],
                }
            assert after == "last-good"
            return {
                "records": [{"message": "would be dropped", "rank": 7}],
                "next_cursor": "past-dropped-record",
                "unavailable": False,
                "detail": "",
                "truncated_ranks": [],
            }

    controller = await _controller_with(_MalformedRankAgent())
    first = await controller.workload_log_page("wl-1")
    second = await controller.workload_log_page("wl-1", after=first["next_cursor"])

    assert second["records"] == []
    assert second["truncated_ranks"] == [0]
    assert "durable runtime process width" in second["detail"]
    assert split_log_cursor(second["next_cursor"]) == (1, {0: "last-good"})


async def test_multi_host_archive_replays_global_ranks_and_explicit_truncation(tmp_path) -> None:
    """A released cell must not turn an unreachable inner host into silence."""

    hosts = {
        "cell-a": (_JournalHost("a0"), _JournalHost("a1")),
        "cell-b": (_JournalHost("b0"), _JournalHost("b1", always_unreachable=True)),
    }
    controller = await _gang_controller(_MultiHostAgent(hosts), runtime_process_counts=(2, 2))
    controller.artifact_access = _artifact_access(tmp_path)

    async with controller.repository.transaction() as transaction:
        workload = await transaction.get_workload("wl-gang")
    await controller._archive_terminal_logs(workload)

    # Release every source cell; only the archived JSONL and its manifest remain.
    async with controller.repository.transaction() as transaction:
        stored = await transaction.get_workload("wl-gang")
        await transaction.put_workload(stored.model_copy(update={"lease_id": None, "lease_ids": []}))
        for lease_id in ("lease-0", "lease-1"):
            lease = await transaction.get_lease(lease_id)
            await transaction.put_lease(lease.model_copy(update={"state": lease.state.__class__.RELEASED}))

    replayed = await controller.workload_log_page("wl-gang")

    assert [(record["rank"], record["message"]) for record in replayed["records"]] == [
        (0, "a0:0"),
        (1, "a1:0"),
        (2, "b0:0"),
    ]
    assert replayed["truncated_ranks"] == [3]
    assert replayed["unavailable"] is False
    assert replayed["detail"] == "archived"


# --------------------------------------------------------------------------- following


def test_a_frame_is_terminated_so_a_browser_actually_fires() -> None:
    """The blank line is the frame terminator, and omitting it is invisible locally.

    A stream missing it looks correct in a terminal and never fires an event in
    a browser, because ``EventSource`` waits for the terminator before
    dispatching.
    """

    from eyvan.server.api.sse import sse_frame

    frame = sse_frame(data={"message": "hi"}, event="log.record", identifier="1:0=c1")

    assert frame.endswith("\n\n")
    assert frame.startswith("id: 1:0=c1\nevent: log.record\ndata: ")


def test_a_payload_is_encoded_on_one_line() -> None:
    """A newline inside a ``data:`` field silently starts a second field.

    A multi-line payload would arrive at the client truncated at the first
    newline, with no error anywhere.
    """

    from eyvan.server.api.sse import sse_frame

    frame = sse_frame(data={"message": "line one\nline two"})

    assert frame.count("data: ") == 1
    assert "\\n" in frame


def test_the_stream_response_disables_proxy_buffering() -> None:
    """An ingress buffer turns "live" into "several minutes late", silently.

    Nothing in any log explains it: the response is correct, it is simply being
    held until a buffer somewhere fills.
    """

    async def _empty():
        if False:
            yield ""

    from eyvan.server.api.sse import sse_response

    response = sse_response(_empty())

    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"


def _stream_client(controller_pages: list[dict]):
    """A control plane whose log pages are scripted, so following is deterministic."""

    from fastapi.testclient import TestClient

    from eyvan.config import EyvanSettings
    from eyvan.server.api.app import create_app

    app = create_app(settings=EyvanSettings(dev_auth_enabled=True, artifact_token_key="k" * 32))
    remaining = list(controller_pages)

    async def scripted(workload_id, *, after="", limit=200):
        del workload_id, after, limit
        return remaining.pop(0) if remaining else {"records": [], "next_cursor": "", "attempt": 1}

    app.state.controller.workload_log_page = scripted
    return TestClient(app), app


def _seed_workload(app) -> None:
    import asyncio as _asyncio

    from eyvan.records import CommandExecution, WorkloadDefinition, WorkloadRecord

    async def store():
        async with app.state.service.repository.transaction() as transaction:
            await transaction.put_workload(
                WorkloadRecord(
                    id="wl-1",
                    org_id="acme",
                    project_id="research",
                    spec=WorkloadDefinition(name="t", execution=CommandExecution(command="echo hi")),
                    created_by="alice",
                )
            )

    _asyncio.run(store())


def test_following_emits_one_frame_per_record() -> None:
    """The point of the route: lines arrive as they are produced."""

    client, app = _stream_client(
        [
            {
                "attempt": 1,
                "attempt_changed": False,
                "records": [
                    {"message": "step 1", "stream": "stdout", "rank": 0},
                    {"message": "step 2", "stream": "stdout", "rank": 1},
                ],
                "next_cursor": "1:0=c1|1=c2",
            }
        ]
    )
    _seed_workload(app)

    response = client.get(
        "/api/v1/workloads/wl-1/logs/stream",
        params={"follow": "false"},
        headers={
            "X-Eyvan-User": "alice",
            "X-Eyvan-Roles": "operator",
            "X-Eyvan-Org": "acme",
            "X-Eyvan-Project": "research",
        },
    )

    assert response.status_code == 200
    assert response.text.count("event: log.record") == 2
    assert "step 1" in response.text
    assert "step 2" in response.text


def test_an_attempt_change_is_announced_exactly_once() -> None:
    """Surviving an attempt change is the crux, and silence is the wrong answer.

    Continuing quietly shows the new attempt's first lines appended to the old
    attempt's tail, and the reader concludes the job resumed.
    """

    client, app = _stream_client(
        [
            {
                "attempt": 2,
                "attempt_changed": True,
                "records": [{"message": "restarted", "stream": "stdout", "rank": 0}],
                "next_cursor": "2:0=c1",
            }
        ]
    )
    _seed_workload(app)

    response = client.get(
        "/api/v1/workloads/wl-1/logs/stream",
        params={"follow": "false"},
        headers={
            "X-Eyvan-User": "alice",
            "X-Eyvan-Roles": "operator",
            "X-Eyvan-Org": "acme",
            "X-Eyvan-Project": "research",
        },
    )

    assert response.text.count("event: log.attempt_changed") == 1
    assert "event: log.record" in response.text


def test_an_unauthorized_follower_is_refused_before_the_response_is_committed() -> None:
    """A check inside the generator runs after ``200`` has already been sent.

    The caller then sees a successful, empty stream -- indistinguishable from a
    job that has printed nothing, which is the worst possible way to deny
    access.
    """

    client, app = _stream_client([])
    _seed_workload(app)

    response = client.get(
        "/api/v1/workloads/wl-1/logs/stream",
        params={"follow": "false"},
        headers={
            "X-Eyvan-User": "mallory",
            "X-Eyvan-Roles": "operator",
            "X-Eyvan-Org": "rival",
            "X-Eyvan-Project": "research",
        },
    )

    assert response.status_code == 403
    assert "event: log.record" not in response.text


def test_a_reconnecting_browser_resumes_from_last_event_id() -> None:
    """``EventSource`` reconnects on its own and sends the last id it saw.

    Ignoring the header means every dropped connection replays from the tail --
    duplicating output the reader already has, and silently losing whatever was
    produced between the drop and the reconnect.
    """

    from fastapi.testclient import TestClient

    from eyvan.config import EyvanSettings
    from eyvan.server.api.app import create_app

    app = create_app(settings=EyvanSettings(dev_auth_enabled=True, artifact_token_key="k" * 32))
    seen: list[str] = []

    async def scripted(workload_id, *, after="", limit=200):
        del workload_id, limit
        seen.append(after)
        return {"attempt": 1, "attempt_changed": False, "records": [], "next_cursor": ""}

    app.state.controller.workload_log_page = scripted
    _seed_workload(app)

    TestClient(app).get(
        "/api/v1/workloads/wl-1/logs/stream",
        params={"follow": "false"},
        headers={
            "X-Eyvan-User": "alice",
            "X-Eyvan-Roles": "operator",
            "X-Eyvan-Org": "acme",
            "X-Eyvan-Project": "research",
            "Last-Event-ID": "2:0=c9|1=c8",
        },
    )

    assert seen == ["2:0=c9|1=c8"], "the reconnect replayed from the tail instead of resuming"
