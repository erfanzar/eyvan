# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Deterministic terminal detection and Rich console construction for the Eyvan SDK."""

from __future__ import annotations

import os
import typing as tp
from collections.abc import Mapping
from dataclasses import dataclass

from rich.console import Console


def stream_is_terminal(stream: tp.TextIO) -> bool:
    """Return the stream's physical TTY state without consulting color variables.

    Commands use this physical check when choosing human output versus JSON so
    ``FORCE_COLOR`` can never turn redirected machine output into a table.
    """

    isatty = getattr(stream, "isatty", None)
    if not callable(isatty):
        return False
    try:
        return bool(isatty())
    except (OSError, ValueError):
        return False


def _enabled(environment: Mapping[str, str], name: str) -> bool:
    """Match Rich's established non-empty environment-variable semantics."""

    return environment.get(name, "") != ""


@dataclass(frozen=True, slots=True)
class TerminalRendering:
    """Resolved layout and ANSI-color capabilities for one output stream.

    Layout precedence is explicit ``force_terminal``, then ``NO_COLOR``
    suppressing an implicit ``FORCE_COLOR`` promotion, then ``FORCE_COLOR``,
    then the stream's physical TTY state. ``NO_COLOR`` always disables ANSI
    styling, including when explicit terminal layout is requested.
    """

    is_terminal: bool
    color_enabled: bool

    @classmethod
    def resolve(
        cls,
        stream: tp.TextIO,
        *,
        force_terminal: bool | None = None,
        environment: Mapping[str, str] | None = None,
    ) -> TerminalRendering:
        """Resolve terminal layout and color without delegating precedence to Rich."""

        resolved_environment = os.environ if environment is None else environment
        no_color = _enabled(resolved_environment, "NO_COLOR")
        force_color = _enabled(resolved_environment, "FORCE_COLOR")
        if force_terminal is not None:
            is_terminal = force_terminal
        elif force_color and not no_color:
            is_terminal = True
        else:
            is_terminal = stream_is_terminal(stream)
        return cls(is_terminal=is_terminal, color_enabled=is_terminal and not no_color)


def terminal_console(
    stream: tp.TextIO,
    *,
    force_terminal: bool | None = None,
    environment: Mapping[str, str] | None = None,
) -> Console:
    """Create an Eyvan Rich console with resolved, immutable rendering semantics."""

    resolved_environment = dict(os.environ if environment is None else environment)
    rendering = TerminalRendering.resolve(
        stream,
        force_terminal=force_terminal,
        environment=resolved_environment,
    )
    return Console(
        file=stream,
        highlight=False,
        soft_wrap=True,
        force_terminal=rendering.is_terminal,
        color_system="auto" if rendering.color_enabled else None,
        no_color=not rendering.color_enabled,
        _environ=resolved_environment,
    )


__all__ = ("TerminalRendering", "stream_is_terminal", "terminal_console")
