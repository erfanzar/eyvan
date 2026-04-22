# Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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

import json
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
    unavailable: list[int] = []
    details: list[str] = []
    for rank in sorted(pages):
        page = pages[rank]
        if page.unavailable:
            unavailable.append(rank)
            if page.detail:
                details.append(f"rank {rank}: {page.detail}")
            continue
        records.extend(page.records)
    records.sort(key=lambda record: (record.timestamp or _EPOCH, record.rank))
    return LogPage(
        records=tuple(records),
        next_cursor="",
        unavailable=bool(unavailable) and not records,
        detail="; ".join(details),
        truncated_ranks=tuple(unavailable),
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
