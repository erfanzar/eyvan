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
"""Safe, compact diagnostics for external capacity-provider failures."""

from __future__ import annotations

import json
import re

_GCLOUD_MESSAGE = re.compile(r'"message"\s*:\s*"(?P<message>(?:\\.|[^"\\])*)"', re.DOTALL)
_ANSI_CONTROL_SEQUENCE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_SAFE_DIAGNOSTIC_LIMIT = 500


def safe_provider_diagnostic(raw: str | None) -> str | None:
    """Return one bounded operator-safe provider diagnostic.

    Raw command output remains in the durable outbox record. This value is for
    workload status, CLI progress, and SDK errors.
    """

    if raw is None:
        return None
    match = _GCLOUD_MESSAGE.search(raw)
    if match is not None:
        try:
            message = json.loads(f'"{match.group("message")}"')
        except json.JSONDecodeError:
            message = None
        if isinstance(message, str):
            compact = _compact_diagnostic(message)
            if compact is not None:
                return compact

    for line in reversed(raw.splitlines()):
        compact = _compact_diagnostic(line)
        if compact is not None and _is_safe_provider_diagnostic(compact):
            return compact
    return "Capacity provider rejected the request; inspect Eyvan controller logs for details."


def _compact_diagnostic(value: str) -> str | None:
    """Collapse one provider value to a bounded single-line diagnostic."""

    without_ansi = _ANSI_CONTROL_SEQUENCE.sub(" ", value)
    sanitized = "".join(" " if ord(character) < 32 or ord(character) == 127 else character for character in without_ansi)
    compact = " ".join(sanitized.split())
    return compact[:_SAFE_DIAGNOSTIC_LIMIT] if compact else None


def _is_safe_provider_diagnostic(value: str) -> bool:
    """Return whether one line is an operator diagnostic rather than command noise."""

    normalized = value.casefold()
    return (
        normalized.startswith("error:")
        or "queue admission" in normalized
        or "admission denied" in normalized
        or any(
            marker in value
            for marker in (
                "PERMISSION_DENIED",
                "RESOURCE_EXHAUSTED",
                "FAILED_PRECONDITION",
                "INVALID_ARGUMENT",
                "UNAUTHENTICATED",
            )
        )
    )


__all__ = ("safe_provider_diagnostic",)
