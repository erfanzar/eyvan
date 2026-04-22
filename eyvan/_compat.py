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
