"""Small runtime compatibility shims for provider-managed worker images."""

from __future__ import annotations

from datetime import timezone
from enum import Enum

try:
    from datetime import UTC
except ImportError:  # pragma: no cover - exercised on Python 3.10 worker images.
    UTC = timezone.utc

try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - exercised on Python 3.10 worker images.

    class StrEnum(str, Enum):
        """Backport of the explicit-value subset of :class:`enum.StrEnum`."""

        def __str__(self) -> str:
            return str(self.value)


__all__ = ("UTC", "StrEnum")
