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
"""Serializable in-memory repository for development and focused tests."""

from __future__ import annotations

from eyvan.persistence.state import StateRepository, StateSession


class InMemorySession(StateSession):
    """Transaction-local in-memory state snapshot."""


class InMemoryRepository(StateRepository):
    """Copy-on-write in-memory repository with serializable transactions and event waits."""

    session_type = InMemorySession


__all__ = ("InMemoryRepository", "InMemorySession")
