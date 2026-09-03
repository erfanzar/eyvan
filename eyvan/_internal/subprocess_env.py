# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Subprocess environments for provider CLIs that embed their own Python.

Every GCP operation Eyvan performs goes through ``gcloud``, which is itself a
Python program. When the invoking process steered its own interpreter with
variables like ``PYTHONPATH``, a naive ``subprocess.run`` passes them straight
through, and the CLI's interpreter then resolves imports through paths chosen
for an entirely different program.

This was a live outage, not a hypothetical: on 2026-08-22 a client process set
a checkout-relative ``PYTHONPATH`` to run from a source tree, the cell agent
inherited it, and every ``gcloud`` it spawned crashed at import time with
``PermissionError: Operation not permitted`` -- macOS TCC refused the CLI's
embedded Python a directory listing inside ``~/Documents`` that the invoking
terminal was allowed to read. The fleet-state write path fell over, cells that
were ACTIVE and billing could never reach READY, and nothing ran for hours.

Provider CLIs must start with none of the caller's interpreter steering. These
variables are the whole set that changes where a Python process looks for code
or user configuration; nothing else the caller set can redirect an import.
"""

from __future__ import annotations

import os
import typing as tp

#: Interpreter-steering environment variables stripped from provider CLI
#: subprocesses. ``CLOUDSDK_*`` is deliberately absent: those configure gcloud
#: itself, and an operator who set one meant it.
INTERPRETER_STEERING_ENV_NAMES: tuple[str, ...] = (
    "PYTHONPATH",
    "PYTHONHOME",
    "PYTHONSTARTUP",
    "PYTHONNOUSERSITE",
    "PYTHONUSERBASE",
    "PYTHONEXECUTABLE",
    "__PYVENV_LAUNCHER__",
)


def provider_cli_env(base: tp.Mapping[str, str] | None = None) -> dict[str, str]:
    """Return ``base`` (default ``os.environ``) minus interpreter-steering variables.

    The result is a fresh dict every call, so a caller may further adjust it
    without mutating the process environment.
    """

    environment = dict(os.environ if base is None else base)
    for name in INTERPRETER_STEERING_ENV_NAMES:
        environment.pop(name, None)
    return environment


__all__ = (
    "INTERPRETER_STEERING_ENV_NAMES",
    "provider_cli_env",
)
