# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Pydantic v2 request and response schemas for the Eyvan HTTP API."""

from __future__ import annotations

import typing as tp
from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from eyvan.server.api.server_startup import ServerControllerMode
from eyvan.server.service.batch import DEFAULT_MAX_FAN_OUT
from eyvan.server.service.errors import ServiceErrorCode
from eyvan.server.service.models import (
    DEFAULT_MAX_RETRIES,
    BudgetRecord,
    CallableExecution,
    CanonicalCapacityClass,
    CapacityClass,
    CapacityPoolRecord,
    CellRecord,
    CellState,
    JsonDocument,
    NativeRuntimeConfig,
    PolicyRecord,
    QuotaRecord,
    ResourceLimits,
    WorkloadDefinition,
    WorkloadRecord,
    WorkspaceRef,
)


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class PlatformAgentNotConfiguredError(ValueError):
    """Raised when a pool omits routing and the platform has no default agent."""


class WorkloadSubmitRequest(ApiModel):
    org_id: str = Field(min_length=1, max_length=128)
    project_id: str = Field(min_length=1, max_length=128)
    spec: WorkloadDefinition
    #: Optional parent in a job tree. The child inherits the parent's scope and
    #: is cancelled with it; it does not become a fair-share level of its own.
    parent_workload_id: str | None = Field(default=None, max_length=96)


class BatchSubmitRequest(ApiModel):
    """Fan out one template into ``count`` ordinary durable workloads."""

    org_id: str = Field(min_length=1, max_length=128)
    project_id: str = Field(min_length=1, max_length=128)
    template: WorkloadDefinition
    count: int = Field(ge=1)
    #: Task names become ``{name_prefix}-{index}``. Defaults to the template's
    #: own name.
    name_prefix: str = Field(default="", max_length=180)
    #: Per-request ceiling, never above the server's. A caller can only lower
    #: it -- raising the fan-out limit is a deployment decision, not a
    #: request-body one.
    max_fan_out: int = Field(default=DEFAULT_MAX_FAN_OUT, ge=1, le=DEFAULT_MAX_FAN_OUT)


class GroupRoleRequest(ApiModel):
    """One named shape within a group, and how many of it the group needs."""

    name: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9-]*$")
    template: WorkloadDefinition
    count: int = Field(default=1, ge=1, le=DEFAULT_MAX_FAN_OUT)


class GroupSubmitRequest(ApiModel):
    """Submit differently-shaped workloads that must admit together."""

    org_id: str = Field(min_length=1, max_length=128)
    project_id: str = Field(min_length=1, max_length=128)
    #: At least two, because a single shape is a batch and batch admits its tasks
    #: independently with no gate to evaluate each cycle.
    roles: tuple[GroupRoleRequest, ...] = Field(min_length=2)
    name_prefix: str = Field(default="", max_length=180)
    max_members: int = Field(default=DEFAULT_MAX_FAN_OUT, ge=2, le=DEFAULT_MAX_FAN_OUT)


class GroupSubmitResponse(ApiModel):
    """The coordinator plus its gated members."""

    parent: WorkloadRecord
    members: list[WorkloadRecord]


class ProgressReportRequest(ApiModel):
    """One rank's self-reported progress.

    Authenticated by the artifact token, not a principal: the caller is a running
    attempt, so there is no ``Principal`` to attribute -- the same reason the
    result-upload route authenticates the way it does.
    """

    rank: int = Field(ge=0)
    attempt: int = Field(ge=1)
    step: int | None = Field(default=None, ge=0)
    total: int | None = Field(default=None, ge=0)
    message: str = Field(default="", max_length=512)
    metrics: dict[str, float] = Field(default_factory=dict)


class ArtifactHoldRequest(ApiModel):
    """Place an explicit human claim on one indexed artifact."""

    uri: str = Field(min_length=1, max_length=512)
    reason: str = Field(min_length=1, max_length=512)
    #: ``None`` is an indefinite hold, which is permitted: the honest lifetime of
    #: "cited in a paper" is indefinite, and an invented expiry would delete the
    #: object on a date nobody chose.
    expires_at: datetime | None = None


class WorkloadEstimateRequest(ApiModel):
    """Ask what one workload definition would cost, without submitting it."""

    org_id: str = Field(min_length=1, max_length=128)
    project_id: str = Field(min_length=1, max_length=128)
    spec: WorkloadDefinition


class DagNodeRequest(ApiModel):
    """One named stage and the sibling stages it waits for."""

    name: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9-]*$")
    template: WorkloadDefinition
    #: Names of sibling nodes in this same request. Not workload ids: that
    #: restriction is what makes cycles detectable at submission and keeps every
    #: edge inside one scope.
    depends_on: tuple[str, ...] = ()


class DagSubmitRequest(ApiModel):
    """Submit a small pipeline of dependent workloads in one transaction."""

    org_id: str = Field(min_length=1, max_length=128)
    project_id: str = Field(min_length=1, max_length=128)
    nodes: tuple[DagNodeRequest, ...] = Field(min_length=1)
    name_prefix: str = Field(default="", max_length=180)
    max_fan_out: int = Field(default=DEFAULT_MAX_FAN_OUT, ge=1, le=DEFAULT_MAX_FAN_OUT)


class BatchSubmitResponse(ApiModel):
    """The coordinator plus its tasks, in task order."""

    #: Not a task: it holds the array and is never scheduled. Cancel it to
    #: cancel every task.
    parent: WorkloadRecord
    tasks: list[WorkloadRecord]


class DagSubmitResponse(ApiModel):
    """The coordinator plus its nodes, in topological order."""

    parent: WorkloadRecord
    nodes: list[WorkloadRecord]


class PoolWorkloadSubmitRequest(ApiModel):
    """Command intent whose infrastructure contract is derived from a fleet node type."""

    org_id: str = Field(min_length=1, max_length=128)
    project_id: str = Field(min_length=1, max_length=128)
    command: str = Field(min_length=1)
    name: str | None = Field(default=None, min_length=1, max_length=200)
    environment: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)
    max_retries: int = Field(default=DEFAULT_MAX_RETRIES, ge=0)
    max_failure_retries: int = Field(default=0, ge=0)
    #: Working tree to materialize on the host before the command runs. Uploaded
    #: separately to the content-addressed workspace route, then referenced here.
    workspace: WorkspaceRef | None = None


class PoolCallableSubmitRequest(ApiModel):
    """Callable intent whose TPU placement is derived from an enabled Spot fleet pool."""

    org_id: str = Field(min_length=1, max_length=128)
    project_id: str = Field(min_length=1, max_length=128)
    accelerator_type: str = Field(min_length=1, max_length=128)
    pods: int = Field(default=1, ge=1)
    capacity_class: CanonicalCapacityClass = CapacityClass.SPOT
    execution: CallableExecution
    name: str | None = Field(default=None, min_length=1, max_length=200)
    environment: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, str] = Field(default_factory=dict)
    max_retries: int = Field(default=DEFAULT_MAX_RETRIES, ge=0)
    max_failure_retries: int = Field(default=0, ge=0)


class PreemptionForceFinalizeRequest(ApiModel):
    reason: str = Field(min_length=1, max_length=1000)


class CellUpsertRequest(ApiModel):
    id: str = Field(min_length=1, max_length=96)
    provider: str = Field(min_length=1)
    capacity_backend: str | None = None
    provider_project: str | None = None
    region: str = Field(min_length=1)
    zone: str | None = None
    state: CellState = CellState.REGISTERING
    capacity: ResourceLimits
    allocatable: ResourceLimits
    unit_prices: dict[str, float] = Field(default_factory=dict)
    accelerator_type: str | None = None
    capacity_class: CanonicalCapacityClass = CapacityClass.ON_DEMAND
    executor: str = "eyvan-native"
    endpoint: str | None = None
    pool_id: str | None = None
    provider_options: JsonDocument = Field(default_factory=dict)
    runtime: NativeRuntimeConfig = Field(default_factory=NativeRuntimeConfig)
    labels: dict[str, str] = Field(default_factory=dict)
    topology: dict[str, str] = Field(default_factory=dict)
    max_replica_resources: ResourceLimits | None = None
    max_gang_replicas: int | None = Field(default=None, ge=1)

    def to_record(self) -> CellRecord:
        return CellRecord(**self.model_dump())


class CapacityPoolUpsertRequest(ApiModel):
    id: str = Field(min_length=1, max_length=192)
    fleet_name: str = Field(min_length=1, max_length=96)
    node_type: str = Field(min_length=1, max_length=96)
    provider: str = Field(min_length=1)
    capacity_backend: str | None = None
    provider_project: str | None = None
    region: str = Field(min_length=1)
    zone: str | None = None
    capacity: ResourceLimits
    allocatable: ResourceLimits
    unit_prices: dict[str, float] = Field(default_factory=dict)
    accelerator_type: str | None = None
    capacity_class: CanonicalCapacityClass = CapacityClass.ON_DEMAND
    executor: str = "eyvan-native"
    agent_endpoint: str | None = Field(default=None, min_length=1)
    provider_options: JsonDocument = Field(default_factory=dict)
    runtime: NativeRuntimeConfig = Field(default_factory=NativeRuntimeConfig)
    labels: dict[str, str] = Field(default_factory=dict)
    topology: dict[str, str] = Field(default_factory=dict)
    max_replica_resources: ResourceLimits | None = None
    max_gang_replicas: int | None = Field(default=None, ge=1)
    quota_group: str | None = Field(default=None, max_length=192)
    quota_limit: int | None = Field(default=None, ge=0)
    quota_units_per_cell: int | None = Field(default=None, ge=1)
    min_cells: int = Field(default=0, ge=0)
    initial_cells: int | None = Field(default=None, ge=0)
    max_cells: int = Field(default=1, ge=0, le=10_000)
    idle_timeout_s: float = Field(default=300, ge=0)
    queue_timeout_s: float | None = Field(default=None, gt=0)
    provisioning_timeout_s: float = Field(default=1800, gt=0)
    upscaling_speed: float = Field(default=1.0, gt=0)
    launch_batch: int = Field(default=5, ge=1)
    enabled: bool = True

    def to_record(self, *, default_agent_endpoint: str | None = None) -> CapacityPoolRecord:
        """Resolve the platform default agent before persisting a pool."""

        agent_endpoint = self.agent_endpoint or default_agent_endpoint
        if agent_endpoint is None:
            raise PlatformAgentNotConfiguredError("no default cell agent is configured by the Eyvan platform")
        return CapacityPoolRecord(**self.model_dump(exclude={"agent_endpoint"}), agent_endpoint=agent_endpoint)


class FleetApplyRequest(ApiModel):
    pools: tuple[CapacityPoolUpsertRequest, ...] = Field(min_length=1)

    def to_records(self, *, default_agent_endpoint: str | None = None) -> tuple[CapacityPoolRecord, ...]:
        """Resolve the platform default agent for every fleet pool."""

        return tuple(pool.to_record(default_agent_endpoint=default_agent_endpoint) for pool in self.pools)


class FleetStatusResponse(ApiModel):
    """Server-side pool and cell state for one applied fleet."""

    name: str
    pools: list[CapacityPoolRecord]
    cells: list[CellRecord]


class CapacityPoolResponse(ApiModel):
    """One available pool advertised by the control plane."""

    fleet: str
    pool: str
    accelerator_type: str
    capacity_class: CanonicalCapacityClass
    region: str
    zone: str | None
    min_cells: int
    max_cells: int
    enabled: bool


class CapacityResponse(ApiModel):
    """Advertised capacity inventory across all applied fleets."""

    pools: list[CapacityPoolResponse]


class PolicyPutRequest(ApiModel):
    allowed_providers: tuple[str, ...] = ()
    allowed_regions: tuple[str, ...] = ()
    allowed_capacity_classes: tuple[CanonicalCapacityClass, ...] = ()
    denied_accelerator_types: tuple[str, ...] = ()
    max_priority: int = Field(default=1000, ge=0, le=1000)
    preemption_enabled: bool = True

    def to_record(self, org_id: str, project_id: str) -> PolicyRecord:
        return PolicyRecord(org_id=org_id, project_id=project_id, updated_by="api", **self.model_dump())


class BudgetPutRequest(ApiModel):
    limit: float = Field(ge=0)
    accelerator_hour_limits: dict[str, float] = Field(default_factory=dict)
    currency: tp.Literal["USD"] = "USD"
    period: str = "monthly"
    #: Fractions of ``limit`` that emit an event the first time spend crosses them
    #: in a period. Settable here because a warning nobody can configure is a
    #: warning nobody gets. Bounded to ``(0, 1]`` at the request boundary as well
    #: as on the record: the record's check would reject this anyway, but as a 500
    #: from deep inside a service rather than a 422 naming the field.
    alert_thresholds: tuple[Annotated[float, Field(gt=0, le=1)], ...] = ()
    hard_limit: bool = True

    def to_record(self, org_id: str, project_id: str) -> BudgetRecord:
        return BudgetRecord(org_id=org_id, project_id=project_id, updated_by="api", **self.model_dump())


class QuotaPutRequest(ApiModel):
    org_id: str = Field(min_length=1, max_length=128)
    project_id: str | None = Field(default=None, max_length=128)
    principal_id: str | None = Field(default=None, max_length=128)
    limits: ResourceLimits
    #: Ceiling on indexed managed-artifact bytes in this scope. ``None`` is
    #: unlimited, which is the historical behaviour and stays the default.
    artifact_bytes_limit: int | None = Field(default=None, ge=0)
    weight: float = Field(default=1.0, gt=0)

    def to_record(self) -> QuotaRecord:
        return QuotaRecord(
            updated_by="api",
            limits=self.limits,
            **self.model_dump(exclude={"limits"}),
        )


class HealthResponse(ApiModel):
    status: str = "ok"
    service: str = "eyvan"
    controller: ServerControllerMode


class ErrorResponse(ApiModel):
    detail: str


class NoMatchingCapacityPoolErrorContext(ApiModel):
    """Fleet inventory returned when callable placement has no exact pool."""

    model_config = ConfigDict(extra="ignore", allow_inf_nan=False)

    fleet: str
    requested_accelerator_type: str
    available_enabled_accelerator_types: tuple[str, ...]


class NoMatchingCapacityPoolErrorResponse(ErrorResponse):
    """Structured API response for an unavailable accelerator in one fleet."""

    model_config = ConfigDict(extra="ignore", allow_inf_nan=False)

    code: tp.Literal[ServiceErrorCode.NO_MATCHING_CAPACITY_POOL] = ServiceErrorCode.NO_MATCHING_CAPACITY_POOL
    context: NoMatchingCapacityPoolErrorContext
