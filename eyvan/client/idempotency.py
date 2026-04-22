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
"""Idempotency-key generation shared by Eyvan clients."""

from __future__ import annotations

import uuid
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class IdempotencyKeyFactory:
    """Generate stable retry keys while preserving caller-supplied values."""

    prefix: str = "sdk"

    def create(self, operation: str | None = None, supplied: str | None = None) -> str:
        """Return ``supplied`` or create a fresh key scoped by ``operation``."""

        if supplied is not None:
            if not supplied:
                raise ValueError("idempotency_key must be a non-empty string")
            return supplied
        operation_part = f"-{operation}" if operation else ""
        return f"{self.prefix}{operation_part}-{uuid.uuid4().hex}"
