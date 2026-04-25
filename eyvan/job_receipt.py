# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Terminal presentation for durable Eyvan workload submissions and lifecycle updates."""

from __future__ import annotations

import sys
import typing as tp
from dataclasses import dataclass

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from eyvan._compat import StrEnum


class JobLifecycleState(StrEnum):
    """User-facing lifecycle categories for one durable workload."""

    REQUESTING = "requesting"
    QUEUED = "queued"
    PROVISIONING = "provisioning"
    ALLOCATED = "allocated"
    CONNECTING = "connecting"
    READY = "ready"
    DISPATCHING = "dispatching"
    RUNNING = "running"
    CHECKPOINTING = "checkpointing"
    CANCELLING = "cancelling"
    PREEMPTED = "preempted"
    BLOCKED = "blocked"
    FAILED = "failed"
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"


_STATE_LABELS: dict[JobLifecycleState, str] = {
    JobLifecycleState.REQUESTING: "REQUESTING",
    JobLifecycleState.QUEUED: "QUEUED",
    JobLifecycleState.PROVISIONING: "PROVISIONING",
    JobLifecycleState.ALLOCATED: "ALLOCATED",
    JobLifecycleState.CONNECTING: "CONNECTING",
    JobLifecycleState.READY: "READY",
    JobLifecycleState.DISPATCHING: "DISPATCHING",
    JobLifecycleState.RUNNING: "RUNNING",
    JobLifecycleState.CHECKPOINTING: "CHECKPOINTING",
    JobLifecycleState.CANCELLING: "CANCELLING",
    JobLifecycleState.PREEMPTED: "PREEMPTED",
    JobLifecycleState.BLOCKED: "BLOCKED",
    JobLifecycleState.FAILED: "FAILED",
    JobLifecycleState.SUCCEEDED: "SUCCEEDED",
    JobLifecycleState.CANCELLED: "CANCELLED",
}

_STATE_STYLES: dict[JobLifecycleState, str] = {
    JobLifecycleState.REQUESTING: "bold cyan",
    JobLifecycleState.QUEUED: "bold yellow",
    JobLifecycleState.PROVISIONING: "bold yellow",
    JobLifecycleState.ALLOCATED: "bold cyan",
    JobLifecycleState.CONNECTING: "bold cyan",
    JobLifecycleState.READY: "bold cyan",
    JobLifecycleState.DISPATCHING: "bold cyan",
    JobLifecycleState.RUNNING: "bold green",
    JobLifecycleState.CHECKPOINTING: "bold yellow",
    JobLifecycleState.CANCELLING: "bold yellow",
    JobLifecycleState.PREEMPTED: "bold yellow",
    JobLifecycleState.BLOCKED: "bold red",
    JobLifecycleState.FAILED: "bold red",
    JobLifecycleState.SUCCEEDED: "bold green",
    JobLifecycleState.CANCELLED: "bold yellow",
}


@dataclass(frozen=True, slots=True)
class JobSubmissionReceipt:
    """Durable job identity and copy-pasteable controls shown after submission."""

    workload_id: str
    fleet: str | None = None
    resource_shape: str | None = None
    atomic: bool = False

    @property
    def plain_lines(self) -> tuple[str, ...]:
        """Return an ANSI-free receipt suitable for redirected stderr."""

        destination = f" to fleet {self.fleet}" if self.fleet else ""
        resource = (f"  request  {self.resource_shape}",) if self.resource_shape else ()
        allocation = ("  allocation  atomic",) if self.atomic else ()
        return (
            f"Eyvan: job {self.workload_id} submitted{destination}.",
            *resource,
            *allocation,
            "  this job keeps running if this client exits.",
            f"  status  eyvan workload status {self.workload_id}",
            f"  logs    eyvan workload logs {self.workload_id}",
            f"  cancel  eyvan workload cancel {self.workload_id}",
        )


@dataclass(frozen=True, slots=True)
class JobLifecycleUpdate:
    """One deduplicated state transition rendered while a caller waits for a job."""

    state: JobLifecycleState
    scope: str | None
    summary: str | None = None
    diagnostic: str | None = None

    @property
    def plain_lines(self) -> tuple[str, ...]:
        """Return a compact ANSI-free lifecycle update."""

        headline = f"Eyvan: {_STATE_LABELS[self.state].lower()}"
        if self.scope:
            headline = f"{headline} | {self.scope}"
        lines = [headline]
        if self.summary:
            lines.append(f"  {self.summary}")
        if self.diagnostic:
            lines.append(f"  {self.diagnostic}")
        return tuple(lines)


class JobTerminalOutput:
    """Render compact Rich job output on a terminal and plain text when redirected."""

    def __init__(
        self,
        *,
        writer: tp.Callable[[str], None] | None = None,
        stream: tp.TextIO | None = None,
        force_terminal: bool | None = None,
    ) -> None:
        if writer is not None and stream is not None:
            raise ValueError("JobTerminalOutput accepts writer or stream, not both")
        self._writer = writer
        self._console = (
            None
            if writer is not None
            else Console(
                file=sys.stderr if stream is None else stream,
                highlight=False,
                soft_wrap=True,
                force_terminal=force_terminal,
                color_system="standard" if force_terminal else None,
            )
        )

    def submission(self, receipt: JobSubmissionReceipt) -> None:
        """Render the one-time submission receipt."""

        writer = self._writer
        if writer is not None:
            self._write_plain(writer, receipt.plain_lines)
            return
        console = self._console
        assert console is not None
        if not console.is_terminal:
            self._write_plain(console.print, receipt.plain_lines)
            return

        table = Table.grid(padding=(0, 1))
        table.add_column(style="dim", no_wrap=True)
        table.add_column()
        table.add_row("State", Text("Submitted", style="bold green"))
        table.add_row("Job", Text(receipt.workload_id, style="bold cyan"))
        if receipt.fleet:
            table.add_row("Fleet", Text(receipt.fleet, style="bold white"))
        if receipt.resource_shape:
            table.add_row("Request", Text(receipt.resource_shape, style="bold white"))
        if receipt.atomic:
            table.add_row("Allocation", Text("atomic", style="bold white"))
        table.add_row("", Text(""))
        table.add_row("Check", Text(f"eyvan workload status {receipt.workload_id}", style="cyan"))
        table.add_row("Logs", Text(f"eyvan workload logs {receipt.workload_id}", style="cyan"))
        table.add_row("Cancel", Text(f"eyvan workload cancel {receipt.workload_id}", style="cyan"))
        console.print(
            Panel(
                table,
                title="[bold cyan]Eyvan job submitted[/]",
                title_align="left",
                border_style="cyan",
                padding=(0, 1),
                expand=False,
            )
        )

    def lifecycle(self, update: JobLifecycleUpdate) -> None:
        """Render one changed lifecycle observation without repeating the job receipt."""

        writer = self._writer
        if writer is not None:
            self._write_plain(writer, update.plain_lines)
            return
        console = self._console
        assert console is not None
        if not console.is_terminal:
            self._write_plain(console.print, update.plain_lines)
            return

        heading = Text("Eyvan", style="bold cyan")
        heading.append("  ")
        heading.append(_STATE_LABELS[update.state], style=_STATE_STYLES[update.state])
        if update.scope:
            heading.append("  ")
            heading.append(update.scope, style="dim")
        console.print(heading)
        if update.summary:
            console.print(Text(f"  {update.summary}", style="white"))
        if update.diagnostic:
            style = "bold red" if update.state in {JobLifecycleState.BLOCKED, JobLifecycleState.FAILED} else "yellow"
            console.print(Text(f"  {update.diagnostic}", style=style))

    @staticmethod
    def _write_plain(writer: tp.Callable[[str], None], lines: tuple[str, ...]) -> None:
        """Write the exact plain fallback lines to a terminal-independent sink."""

        for line in lines:
            writer(line)


__all__ = (
    "JobLifecycleState",
    "JobLifecycleUpdate",
    "JobSubmissionReceipt",
    "JobTerminalOutput",
)
