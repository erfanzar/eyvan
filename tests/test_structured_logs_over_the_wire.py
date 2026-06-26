# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The hop that made structured logs absent on every remote fleet.

Both ends already had everything. The worker's systemd runtime has produced
structured pages since Phase 4 -- journald's own JSON, with priority, timestamp,
PID and an opaque cursor -- and ``log_records`` knew how to merge and render
them. There was simply no route between them: the worker exposed only the
plain-text ``logs`` endpoint, so ``eyvan workload log-page`` answered
``unsupported_capability`` for every fleet that runs on a remote worker, which
is every TPU fleet.

Observed running P2: `execution backend 'eyvan-native' does not expose
structured logs`.
"""

from __future__ import annotations

from datetime import UTC, datetime

import httpx

from eyvan.worker.runtime.log_records import (
    LogPage,
    LogRecord,
    join_log_cursor,
    log_page_from_json,
    split_log_cursor,
)


def _record(message: str, *, rank: int = 0, cursor: str = "c1") -> LogRecord:
    return LogRecord(
        message=message,
        stream="stdout",
        timestamp=datetime(2026, 8, 6, 12, 0, tzinfo=UTC),
        cursor=cursor,
        rank=rank,
    )


# --------------------------------------------------------------------------- the wire shape


def test_a_page_survives_a_round_trip() -> None:
    """`as_json` and `log_page_from_json` are one protocol, not two spellings."""

    page = LogPage(records=(_record("hello"),), next_cursor="c1")
    document = {
        "records": [dict(r.as_json()) for r in page.records],
        "next_cursor": page.next_cursor,
        "unavailable": False,
        "detail": "",
        "truncated_ranks": [],
    }

    restored = log_page_from_json(document)

    assert [r.message for r in restored.records] == ["hello"]
    assert restored.records[0].timestamp == page.records[0].timestamp
    assert restored.next_cursor == "c1"


def test_a_malformed_payload_becomes_unavailable_rather_than_raising() -> None:
    """A follower polling every second must not have to catch exceptions.

    "Unavailable" is also the honest description of a host that answered
    nonsense, which an empty page is not.
    """

    assert log_page_from_json("not a document").unavailable is True
    assert log_page_from_json({"records": "not a list"}).records == ()


def test_an_unparseable_timestamp_does_not_lose_the_line() -> None:
    """The message is the point; the timestamp is context."""

    page = log_page_from_json({"records": [{"message": "kept", "timestamp": "not-a-date"}]})

    assert [r.message for r in page.records] == ["kept"]
    assert page.records[0].timestamp is None


# --------------------------------------------------------------------------- per-rank cursors


def test_each_rank_resumes_from_its_own_position() -> None:
    """A cursor is meaningless to a journal that did not issue it."""

    encoded = join_log_cursor(2, {0: "a", 1: "b"})
    attempt, cursors = split_log_cursor(encoded)

    assert attempt == 2
    assert cursors == {0: "a", 1: "b"}


def test_a_rank_that_reported_no_cursor_is_omitted() -> None:
    """Encoding an empty cursor would tell that rank to replay everything."""

    assert join_log_cursor(1, {0: "a", 1: ""}) == "1:0=a"


def test_no_cursors_at_all_encodes_to_empty() -> None:
    """Empty means "wherever you were"; a start cursor means "replay"."""

    assert join_log_cursor(1, {}) == ""


# --------------------------------------------------------------------------- the slice fan-out


class _Worker:
    def __init__(self, page: LogPage | Exception) -> None:
        self._page = page
        self.seen_after: str | None = None

    def log_page(self, _remote_id: str, *, after: str = "", limit: int = 200) -> LogPage:
        self.seen_after = after
        if isinstance(self._page, Exception):
            raise self._page
        return self._page


def _slice(*workers: _Worker):
    from eyvan.worker.runtime.native_slice import NativeSliceExecutor

    executor = NativeSliceExecutor.__new__(NativeSliceExecutor)
    executor._workers = list(workers)
    return executor


def test_every_host_in_the_slice_is_merged() -> None:
    """A v5p-16 is one cell with two hosts; reporting one is reporting half."""

    page = _slice(
        _Worker(LogPage(records=(_record("from host 0"),), next_cursor="c0")),
        _Worker(LogPage(records=(_record("from host 1"),), next_cursor="c1")),
    ).log_page("wl-1")

    assert sorted(r.message for r in page.records) == ["from host 0", "from host 1"]
    assert {r.rank for r in page.records} == {0, 1}


def test_an_unreachable_host_is_reported_in_place() -> None:
    """The failure this aggregation exists to prevent.

    Silently returning the hosts that answered shows a complete-looking log for a
    half-dead slice.
    """

    page = _slice(
        _Worker(LogPage(records=(_record("alive"),), next_cursor="c0")),
        _Worker(httpx.ConnectError("host is gone")),
    ).log_page("wl-1")

    assert [r.message for r in page.records] == ["alive"]
    assert page.truncated_ranks == (1,)


def test_each_host_is_handed_its_own_cursor() -> None:
    """Handing rank 1 rank 0's cursor asks journald to resume from a foreign id."""

    first, second = _Worker(LogPage()), _Worker(LogPage())

    _slice(first, second).log_page("wl-1", after=join_log_cursor(1, {0: "aaa", 1: "bbb"}))

    assert first.seen_after == "aaa"
    assert second.seen_after == "bbb"


def test_the_slice_cursor_carries_every_host_position() -> None:
    """`merge_pages` returns none on purpose; the slice composes the real one."""

    page = _slice(
        _Worker(LogPage(records=(_record("a"),), next_cursor="c0")),
        _Worker(LogPage(records=(_record("b"),), next_cursor="c1")),
    ).log_page("wl-1", after=join_log_cursor(3, {}))

    _attempt, cursors = split_log_cursor(page.next_cursor)
    assert cursors == {0: "c0", 1: "c1"}
