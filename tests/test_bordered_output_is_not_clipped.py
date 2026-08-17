# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Every box Eyvan draws has to close.

Eyvan's console is built with ``soft_wrap=True``, which is right for prose: a
sentence keeps its own newlines and the terminal decides where it breaks. It is
wrong for anything with a border. Soft wrapping renders at unbounded width, so a
panel or table wider than the terminal is *clipped* by the terminal rather than
wrapped by Rich -- and what reaches the screen is a half-drawn box with a
sentence cut mid-word.

``print_block`` exists for that, and the defect has now recurred twice by way of
a new call site that used ``console.print`` directly -- once in a generation
summary, once in a restart warning that read "this control plane trusts calla".
So the rule is checked here rather than remembered: no bordered renderable goes
out on the soft-wrapping path.
"""

from __future__ import annotations

import io
import re
from pathlib import Path

from rich.panel import Panel
from rich.table import Table

from eyvan.sdk.terminal import print_block, terminal_console

#: Every Rich box style draws its right edge with one of these.
BORDER_EDGES = ("│", "┃", "|", "║")

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "eyvan"

#: A bordered renderable handed straight to `console.print`. Matches the two
#: forms that have actually shipped broken.
UNGUARDED_PRINT = re.compile(r"\.print\(\s*(Panel\(|Table\(|table\b|panel\b)")


def _rendered(renderable: object, width: int) -> list[str]:
    stream = io.StringIO()
    console = terminal_console(stream, force_terminal=False, environment={"COLUMNS": str(width)})
    console.width = width
    print_block(console, renderable)
    return [line for line in stream.getvalue().splitlines() if line.strip()]


def test_a_panel_wider_than_the_terminal_wraps_instead_of_being_cut() -> None:
    """The exact shape of the shipped defect: a long warning inside a panel."""

    warning = (
        "EYVAN_TRUSTED_CALLABLE_PROJECTS is unset, so this control plane trusts callables only in the "
        "default loopback project; submissions from any other org/project will be refused with 403."
    )

    lines = _rendered(Panel(warning, title="Callable trust narrowed"), width=80)

    assert len(lines) > 3, "a warning this long has to occupy more than one body line"
    for line in lines:
        assert len(line) <= 80, f"escaped the terminal: {line!r}"
    # The last body line still carries a right edge, which is what a clipped box
    # loses first.
    assert any(line.rstrip().endswith(BORDER_EDGES) for line in lines)
    assert "403." in "".join(lines), "the sentence must survive wrapping, not be truncated"


def test_a_table_wider_than_the_terminal_stays_inside_it() -> None:
    table = Table(title="Skipped candidates")
    table.add_column("Reason", overflow="fold")
    table.add_column("Summary")
    table.add_row("Eyvan cannot derive a typed queued-resource worker shape for this zone", "597 types across 31 zones")

    for line in _rendered(table, width=72):
        assert len(line) <= 72, f"escaped the terminal: {line!r}"


def test_no_bordered_renderable_is_printed_on_the_soft_wrapping_path() -> None:
    """The regression guard.

    A new call site is how this came back both times, so the check is on the
    source rather than on any one command's output.
    """

    offenders = [
        f"{path.relative_to(SOURCE_ROOT.parent)}:{number}: {line.strip()}"
        for path in SOURCE_ROOT.rglob("*.py")
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if UNGUARDED_PRINT.search(line) and "print_block" not in line
    ]

    assert offenders == [], "print these through eyvan.sdk.terminal.print_block:\n" + "\n".join(offenders)
