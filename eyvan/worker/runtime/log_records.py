# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Structured log records, shared by every execution backend.

Sibling of ``callable_protocol.py`` and under the same discipline: **this module
imports no Eyvan package modules**, because the OCI runner stages it beside the
worker rather than installing the package.

**Log lines do not go in ``audit_events``.** Three reasons, each independently
sufficient. ``prune_events`` deserializes every row before testing the cutoff, so
at log volume it starves the governance ledger it exists to trim.
``audit_retention_s`` is 90 days -- right for a compliance ledger, catastrophic
for stdout. And interleaving log lines into the audit ``sequence`` space breaks
the 256/4096 lookback-and-dedupe budget ``stream_events`` sizes deliberately.

**journald stays the body store.** It already provides durable ordering, an
opaque ``__CURSOR``, ``--after-cursor``, ``--lines``, ``--since``, and
``--output=json`` with ``PRIORITY`` and ``_PID``. Reimplementing that in Postgres
would be strictly worse; the control plane keeps an *index* plus an archived blob
for terminal attempts, because releasing a cell otherwise destroys the logs.
"""

from __future__ import annotations

import base64
import json
import re
import typing as tp
from dataclasses import dataclass, field
from datetime import datetime, timezone

#: Which stream a line came from. ``"unknown"`` is a real value, not a default:
#: a missing or unexpected ``PRIORITY`` must not be silently reported as stdout,
#: mirroring the ``pids is None`` vs ``[]`` discipline ``systemd_transient``
#: already documents.
LogStream = tp.Literal["stdout", "stderr", "unknown"]

#: journald priorities that mean "this was written to stderr". Anything outside
#: this set and the stdout set is ``unknown`` rather than guessed.
_STDERR_PRIORITIES = frozenset({"0", "1", "2", "3", "4"})
_STDOUT_PRIORITIES = frozenset({"5", "6", "7"})

#: Marker stored as the first JSON line in rank zero's terminal log archive.
#: The manifest makes sparse/truncated ranks replayable after their cells and
#: durable runtime process-count records are gone.
LOG_ARCHIVE_MANIFEST_KEY = "_eyvan_log_archive"
LOG_ARCHIVE_MANIFEST_VERSION = 1
#: Contiguous rank range probed by readers that predate archive manifests.
#: Writers may place compatibility gap objects only inside this fixed window.
LOG_ARCHIVE_LEGACY_RANK_SCAN_LIMIT = 64


@dataclass(frozen=True, slots=True)
class LogRecord:
    """One captured output line, with enough context to place it."""

    message: str
    stream: LogStream = "unknown"
    timestamp: datetime | None = None
    #: Backend-opaque resume position. journald's ``__CURSOR``, a byte offset for
    #: local processes, a timestamp for Docker. Never parsed by the control
    #: plane -- only handed back to the backend that issued it.
    cursor: str = ""
    rank: int = 0
    pid: int | None = None

    def as_json(self) -> dict[str, object]:
        """Return the stable wire shape."""

        return {
            "message": self.message,
            "stream": self.stream,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "cursor": self.cursor,
            "rank": self.rank,
            "pid": self.pid,
        }


@dataclass(frozen=True, slots=True)
class LogPage:
    """One page of log records plus the cursor to continue from."""

    records: tuple[LogRecord, ...] = ()
    #: Where a follower resumes. Empty when nothing was read, which is distinct
    #: from a cursor pointing at the start: resuming from empty means "wherever
    #: you were", and resuming from a start cursor means "replay everything".
    next_cursor: str = ""
    #: True when the backend reported it could not read logs at all, as opposed
    #: to reading zero lines. A caller that cannot tell those apart will report
    #: "no output" for a job whose host is unreachable.
    unavailable: bool = False
    detail: str = ""
    truncated_ranks: tuple[int, ...] = field(default_factory=tuple)


_LOG_CURSOR_PREFIX = "eyvan-log-v2."
_LOG_CURSOR_VERSION = 2
# A cursor is accepted from an API query and recursively embeds one opaque
# backend position per fan-out level. Bound both its serialized size and its
# cardinality before decoding so an attacker cannot turn a tiny log request into
# an unbounded base64/JSON allocation. The entry ceiling matches the largest
# workload cell group accepted by the public model; 16 MiB still accommodates a
# legitimate 10,000-cell outer cursor whose entries are ordinary journald or
# small nested slice cursors.
_LOG_CURSOR_MAX_BYTES = 16 * 1024 * 1024
_LOG_CURSOR_MAX_ENTRIES = 10_000
_LOG_CURSOR_MAX_VALUE_BYTES = 8 * 1024 * 1024
_LEGACY_RANK_PREFIX = re.compile(r"^[0-9]+=")


def split_log_cursor(value: str) -> tuple[int | None, dict[int, str]]:
    """Decode a versioned log cursor, with compatibility for legacy cursors.

    Version 2 is base64url JSON. Backend cursors are opaque strings inside that
    document, so a journald cursor containing ``|`` or ``=`` cannot be confused
    with the outer fan-out structure. Malformed v2 cursors restart the read
    instead of handing a guessed position to the wrong journal.

    The legacy decoder accepts both the former per-rank shape
    ``"{attempt}:{rank}={cursor}|..."`` and the original single-rank shape
    ``"{attempt}:{cursor}"``. The latter matters for journald cursors such as
    ``s=...``: only a leading decimal rank followed by ``=`` identifies the
    delimiter-based per-rank encoding.

    A bare value with no attempt is read as "no opinion" rather than attempt
    zero, so a first read is not mistaken for a superseded one.
    """

    if not value:
        return None, {}
    # Check characters first so a hostile non-ASCII value is rejected without
    # allocating up to four encoded bytes per code point.
    if len(value) > _LOG_CURSOR_MAX_BYTES:
        return None, {}
    try:
        if len(value.encode("utf-8")) > _LOG_CURSOR_MAX_BYTES:
            return None, {}
    except UnicodeEncodeError:
        return None, {}
    if value.startswith(_LOG_CURSOR_PREFIX):
        return _split_v2_log_cursor(value.removeprefix(_LOG_CURSOR_PREFIX))
    return _split_v1_log_cursor(value)


def _split_v2_log_cursor(encoded: str) -> tuple[int | None, dict[int, str]]:
    if len(encoded) > _LOG_CURSOR_MAX_BYTES - len(_LOG_CURSOR_PREFIX):
        return None, {}
    try:
        padding = "=" * (-len(encoded) % 4)
        raw = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        if len(raw) > _LOG_CURSOR_MAX_BYTES:
            return None, {}
        document = json.loads(raw)
    except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
        return None, {}
    if not isinstance(document, dict) or document.get("version") != _LOG_CURSOR_VERSION:
        return None, {}
    attempt = document.get("attempt")
    entries = document.get("cursors")
    if (
        isinstance(attempt, bool)
        or not isinstance(attempt, int)
        or attempt < 0
        or not isinstance(entries, list)
        or len(entries) > _LOG_CURSOR_MAX_ENTRIES
    ):
        return None, {}
    cursors: dict[int, str] = {}
    for entry in entries:
        if not isinstance(entry, list) or len(entry) != 2:
            return None, {}
        rank, cursor = entry
        if (
            isinstance(rank, bool)
            or not isinstance(rank, int)
            or rank < 0
            or not isinstance(cursor, str)
            or not cursor
            or len(cursor) > _LOG_CURSOR_MAX_VALUE_BYTES
            or rank in cursors
        ):
            return None, {}
        try:
            if len(cursor.encode("utf-8")) > _LOG_CURSOR_MAX_VALUE_BYTES:
                return None, {}
        except UnicodeEncodeError:
            return None, {}
        cursors[rank] = cursor
    return attempt, cursors


def _split_v1_log_cursor(value: str) -> tuple[int | None, dict[int, str]]:
    """Decode the unprefixed delimiter-based v1 and its single-rank predecessor."""

    attempt_text, separator, remainder = value.partition(":")
    if not separator:
        return None, {}
    try:
        attempt = int(attempt_text)
    except ValueError:
        return None, {}
    if attempt < 0:
        return None, {}
    if not remainder:
        return attempt, {}
    if not _LEGACY_RANK_PREFIX.match(remainder):
        if len(remainder.encode("utf-8")) > _LOG_CURSOR_MAX_VALUE_BYTES:
            return None, {}
        return attempt, {0: remainder}
    if remainder.count("|") + 1 > _LOG_CURSOR_MAX_ENTRIES:
        return None, {}
    cursors: dict[int, str] = {}
    for entry in remainder.split("|"):
        rank_text, assigned, cursor = entry.partition("=")
        if not assigned or not cursor:
            continue
        try:
            if len(cursor.encode("utf-8")) > _LOG_CURSOR_MAX_VALUE_BYTES:
                return None, {}
            rank = int(rank_text)
        except (UnicodeEncodeError, ValueError):
            continue
        if rank >= 0:
            cursors[rank] = cursor
    return attempt, cursors


def join_log_cursor(attempt: int, cursors: tp.Mapping[int, str]) -> str:
    """Encode every backend position in a deterministic opaque v2 cursor.

    Empty when no rank reported one, which is distinct from a cursor pointing at
    the start: resuming from empty means "wherever you were", and a start cursor
    means "replay everything".
    """

    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0:
        raise ValueError("structured log cursor attempt must be a non-negative integer")
    live: list[tuple[int, str]] = []
    cursor_bytes_total = 0
    for rank, cursor in cursors.items():
        if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
            raise ValueError("structured log cursor ranks must be non-negative integers")
        if not isinstance(cursor, str):
            raise ValueError("structured log backend cursors must be strings")
        if not cursor:
            continue
        try:
            cursor_size = len(cursor.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise ValueError("structured log backend cursors must be valid UTF-8") from exc
        if cursor_size > _LOG_CURSOR_MAX_VALUE_BYTES:
            raise ValueError("structured log backend cursor exceeds the size limit")
        cursor_bytes_total += cursor_size
        if cursor_bytes_total > _LOG_CURSOR_MAX_BYTES:
            raise ValueError("structured log cursor exceeds the size limit")
        live.append((rank, cursor))
    if not live:
        return ""
    if len(live) > _LOG_CURSOR_MAX_ENTRIES:
        raise ValueError("structured log cursor exceeds the entry limit")
    live.sort(key=lambda entry: entry[0])
    document = {"version": _LOG_CURSOR_VERSION, "attempt": attempt, "cursors": live}
    raw = json.dumps(document, separators=(",", ":")).encode()
    encoded = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    result = f"{_LOG_CURSOR_PREFIX}{encoded}"
    if len(result) > _LOG_CURSOR_MAX_BYTES:
        raise ValueError("structured log cursor exceeds the size limit")
    return result


def _stream_from_wire(value: object) -> LogStream:
    """Accept a stream name the sender already decoded.

    Falls back to the priority mapping so a sender that forwards a raw journald
    PRIORITY is still understood; the two are distinguishable because the stream
    names are a closed set.
    """

    if value in ("stdout", "stderr", "unknown"):
        return tp.cast("LogStream", value)
    return stream_for_priority(value if isinstance(value, (str, int)) else None)


def log_page_from_json(payload: object) -> LogPage:
    """Rebuild a page from the wire shape ``LogPage`` writes.

    The inverse of ``as_json`` and the route's own document, kept beside them so
    the two cannot drift: a remote driver that decoded these fields itself would
    be a second spelling of the protocol.

    A malformed payload becomes an ``unavailable`` page rather than an
    exception, for the same reason ``log_page`` returns one: a follower polling
    every second should not have to catch an exception to learn a host answered
    badly, and "unavailable" is the honest description of what it got.
    """

    if not isinstance(payload, dict):
        return LogPage(unavailable=True, detail="native worker returned a non-object log page")
    raw_records = payload.get("records")
    records: list[LogRecord] = []
    for item in raw_records if isinstance(raw_records, list) else []:
        if not isinstance(item, dict):
            continue
        stamp = item.get("timestamp")
        parsed: datetime | None = None
        if isinstance(stamp, str) and stamp:
            try:
                parsed = datetime.fromisoformat(stamp)
            except ValueError:
                parsed = None
        rank = item.get("rank")
        pid = item.get("pid")
        records.append(
            LogRecord(
                message=str(item.get("message", "")),
                # Already decoded by the sender. Feeding it back through
                # ``stream_for_priority`` -- which expects a journald PRIORITY --
                # silently turned every 'stdout' and 'stderr' into 'unknown', so
                # the one field structured logs add over plain text was empty on
                # every remote read.
                stream=_stream_from_wire(item.get("stream")),
                timestamp=parsed,
                cursor=str(item.get("cursor", "")),
                rank=rank if isinstance(rank, int) and not isinstance(rank, bool) else 0,
                pid=pid if isinstance(pid, int) else None,
            )
        )
    truncated = payload.get("truncated_ranks")
    return LogPage(
        records=tuple(records),
        next_cursor=str(payload.get("next_cursor", "")),
        unavailable=bool(payload.get("unavailable")),
        detail=str(payload.get("detail", "")),
        truncated_ranks=tuple(
            r
            for r in (truncated if isinstance(truncated, list) else [])
            if isinstance(r, int) and not isinstance(r, bool) and r >= 0
        ),
    )


def stream_for_priority(priority: str | int | None) -> LogStream:
    """Map a journald PRIORITY to a stream name.

    Returns ``"unknown"`` for anything unrecognized. Guessing ``stdout`` would
    make an operator debugging a crash read stderr lines as ordinary output.
    """

    if priority is None:
        return "unknown"
    text = str(priority).strip()
    if text in _STDERR_PRIORITIES:
        return "stderr"
    if text in _STDOUT_PRIORITIES:
        return "stdout"
    return "unknown"


def parse_journal_json(payload: str, *, rank: int = 0) -> LogPage:
    """Parse ``journalctl --output=json`` output into records.

    Tolerant by line: journald emits one JSON object per line, and a truncated
    final line during a live read is ordinary rather than a failure. Discarding
    the whole page for it would make ``--follow`` drop output at exactly the
    moments a job is producing the most.
    """

    records: list[LogRecord] = []
    cursor = ""
    for line in payload.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            entry = json.loads(stripped)
        except ValueError:
            continue
        if not isinstance(entry, dict):
            continue
        message = entry.get("MESSAGE")
        if isinstance(message, list):
            # journald returns a byte array for messages it considers non-UTF8.
            message = bytes(item for item in message if isinstance(item, int)).decode("utf-8", errors="replace")
        elif not isinstance(message, str):
            continue
        cursor = str(entry.get("__CURSOR", cursor))
        records.append(
            LogRecord(
                message=message,
                stream=stream_for_priority(entry.get("PRIORITY")),
                timestamp=_microseconds_to_datetime(entry.get("__REALTIME_TIMESTAMP")),
                cursor=str(entry.get("__CURSOR", "")),
                rank=rank,
                pid=_optional_int(entry.get("_PID")),
            )
        )
    return LogPage(records=tuple(records), next_cursor=cursor)


def merge_pages(pages: tp.Mapping[int, LogPage]) -> LogPage:
    """Merge per-rank pages into one, keeping unreachable ranks visible.

    An unreachable rank is reported *in place* rather than dropped: a fan-out
    that silently returned the ranks it could reach would show a complete-looking
    log for a partially-failed slice, which is the exact failure the per-rank
    aggregation discipline exists to prevent.
    """

    records: list[LogRecord] = []
    truncated: set[int] = set()
    details: list[str] = []
    for rank in sorted(pages):
        page = pages[rank]
        page_truncated = set(page.truncated_ranks)
        truncated.update(page_truncated)
        if page.unavailable:
            # A nested fan-out (one cell containing several hosts) already names
            # every unavailable inner rank. A single-rank backend has no nested
            # list, so its mapping key remains the rank that failed.
            if not page_truncated:
                truncated.add(rank)
            if page.detail:
                details.append(page.detail if page_truncated else f"rank {rank}: {page.detail}")
            continue
        records.extend(page.records)
        if page.detail and page_truncated:
            details.append(page.detail)
    records.sort(key=lambda record: (record.timestamp or _EPOCH, record.rank))
    return LogPage(
        records=tuple(records),
        next_cursor="",
        unavailable=bool(truncated) and not records,
        detail="; ".join(details),
        truncated_ranks=tuple(sorted(truncated)),
    )


_EPOCH = datetime.fromtimestamp(0, tz=timezone.utc)


def _microseconds_to_datetime(value: object) -> datetime | None:
    try:
        micros = int(str(value))
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc)


def _optional_int(value: object) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


__all__ = (
    "LogPage",
    "LogRecord",
    "LogStream",
    "merge_pages",
    "parse_journal_json",
    "stream_for_priority",
)
