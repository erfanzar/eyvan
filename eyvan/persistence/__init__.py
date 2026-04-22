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
"""Persistence contracts and repository implementations."""

from eyvan.persistence.base import Repository, RepositorySession
from eyvan.persistence.fleet import FleetCellRecord, FleetStateStore, GcsBackend, LocalBackend
from eyvan.persistence.leader import PostgresAdvisoryLeader
from eyvan.persistence.local import (
    LocalControlPlaneError,
    LocalControlPlaneInUseError,
    LocalControlPlaneSchemaError,
    LocalJsonRepository,
    LocalJsonSession,
)
from eyvan.persistence.memory import InMemoryRepository, InMemorySession
from eyvan.persistence.sqlalchemy import SCHEMA_REVISION, Base, SqlAlchemyRepository, SqlAlchemySession, create_schema

__all__ = (
    "SCHEMA_REVISION",
    "Base",
    "FleetCellRecord",
    "FleetStateStore",
    "GcsBackend",
    "InMemoryRepository",
    "InMemorySession",
    "LocalBackend",
    "LocalControlPlaneError",
    "LocalControlPlaneInUseError",
    "LocalControlPlaneSchemaError",
    "LocalJsonRepository",
    "LocalJsonSession",
    "PostgresAdvisoryLeader",
    "Repository",
    "RepositorySession",
    "SqlAlchemyRepository",
    "SqlAlchemySession",
    "create_schema",
)
