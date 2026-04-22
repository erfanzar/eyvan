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
"""Eyvan application services and public lifecycle contracts."""

from __future__ import annotations

import typing as tp

from eyvan.service.authz import Authorizer
from eyvan.service.callable_trust import CallableProjectTrustPolicy
from eyvan.service.models import (
    AuditEvent,
    BudgetRecord,
    CallableExecution,
    CapacityClass,
    CellRecord,
    CellState,
    CommandExecution,
    LeaseRecord,
    LeaseState,
    NativeContainerConfig,
    NativeContainerMount,
    NativeRuntimeArtifact,
    NativeRuntimeConfig,
    Permission,
    PolicyRecord,
    Principal,
    PriorityBand,
    QuotaRecord,
    ResourceGang,
    ResourceVector,
    Role,
    RoleGrant,
    WorkloadDefinition,
    WorkloadRecord,
    WorkloadState,
)
from eyvan.service.scheduling import CoreSchedulingService, SchedulingService

if tp.TYPE_CHECKING:
    from eyvan.service.control_plane import ControlPlaneService
from eyvan.service.state import (
    LEASE_TRANSITIONS,
    TERMINAL_WORKLOAD_STATES,
    WORKLOAD_TRANSITIONS,
    InvalidTransition,
    ensure_lease_transition,
    ensure_workload_transition,
)

__all__ = (
    "LEASE_TRANSITIONS",
    "TERMINAL_WORKLOAD_STATES",
    "WORKLOAD_TRANSITIONS",
    "AuditEvent",
    "Authorizer",
    "BudgetRecord",
    "CallableExecution",
    "CallableProjectTrustPolicy",
    "CapacityClass",
    "CellRecord",
    "CellState",
    "CommandExecution",
    "ControlPlaneService",
    "CoreSchedulingService",
    "InvalidTransition",
    "LeaseRecord",
    "LeaseState",
    "NativeContainerConfig",
    "NativeContainerMount",
    "NativeRuntimeArtifact",
    "NativeRuntimeConfig",
    "Permission",
    "PolicyRecord",
    "Principal",
    "PriorityBand",
    "QuotaRecord",
    "ResourceGang",
    "ResourceVector",
    "Role",
    "RoleGrant",
    "SchedulingService",
    "WorkloadDefinition",
    "WorkloadRecord",
    "WorkloadState",
    "ensure_lease_transition",
    "ensure_workload_transition",
)


def __getattr__(name: str) -> type[ControlPlaneService]:
    """Load the persistence-dependent application service without import cycles."""

    if name == "ControlPlaneService":
        from eyvan.service.control_plane import ControlPlaneService

        return ControlPlaneService
    raise AttributeError(name)
