# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Fixtures shared across the suite.

Only the PostgreSQL session fixture lives here. Everything else in this suite
builds its own harness in the module that needs it, which keeps each test file
readable on its own; a real database is the exception because starting one per
module would cost seconds per file.
"""

from __future__ import annotations

from postgres_fixture import postgres_url

__all__ = ("postgres_url",)
