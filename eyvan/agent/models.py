"""Versioned wire models for control-plane to cell-agent operations."""

from __future__ import annotations

import typing as tp
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic import JsonValue as PydanticJsonValue

from eyvan._compat import StrEnum
from eyvan.capacity import CapacityStatus
from eyvan.drivers.base import CapacityProviderFailureCode, ExecutionWorkloadStatus

WireJsonObject: tp.TypeAlias = dict[str, PydanticJsonValue]


class AgentModel(BaseModel):
    """Strict base for every cell-agent request and response."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class MutationConflictCode(StrEnum):
    """Machine-readable mutation conflicts returned by the cell agent."""

    DRIVER_BUSY = "driver_busy"
    STALE_MUTATION = "stale_mutation"


class MutationConflict(AgentModel):
    """Validated HTTP 409 response from a cell agent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    detail: str = Field(min_length=1)
    code: MutationConflictCode


class CapacityProviderFailure(AgentModel):
    """Validated provider failure response from a cell agent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    detail: str = Field(min_length=1)
    code: CapacityProviderFailureCode


class AgentCapabilities(AgentModel):
    """Versioned execution and capacity capabilities exposed by one agent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: str
    executor: str
    accelerator_types: tuple[str, ...]
    capacity_classes: tuple[str, ...]
    #: Backends this agent can apply capacity for. Empty means the agent predates
    #: the field and must be skipped, not rejected.
    capacity_backends: tuple[str, ...] = ()
    supports_checkpoint: bool
    supports_callables: bool
    supports_exec: bool
    max_gang_replicas: int | None
    resource_enforcement: str


class NativeWorkerHealth(AgentModel):
    """Authenticated health response returned by an Eyvan native worker."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    status: tp.Literal["ok"]
    runtime: tp.Literal["native-worker"]


class MutationModel(AgentModel):
    idempotency_key: str = Field(min_length=1, max_length=256)
    generation: int = Field(ge=0)
    fencing_token: str = Field(min_length=1, max_length=512)


class CapacityRequest(AgentModel):
    mutation: MutationModel
    desired: WireJsonObject


class WorkloadRequest(AgentModel):
    mutation: MutationModel
    spec: WireJsonObject


class GangWorkloadMember(AgentModel):
    """One independently fenced cell participating in a native workload gang."""

    model_config = ConfigDict(extra="forbid")

    cell_id: str = Field(min_length=1, max_length=96)
    agent_endpoint: str = Field(min_length=1, max_length=2048)
    mutation: MutationModel

    @model_validator(mode="after")
    def validate_values(self) -> GangWorkloadMember:
        if self.cell_id != self.cell_id.strip():
            raise ValueError("gang member cell_id must be trimmed")
        if self.agent_endpoint != self.agent_endpoint.strip():
            raise ValueError("gang member agent_endpoint must be trimmed")
        try:
            endpoint = urlsplit(self.agent_endpoint)
        except ValueError as exc:
            raise ValueError("gang member agent_endpoint must be a valid URL") from exc
        if not endpoint.scheme or not endpoint.netloc:
            raise ValueError("gang member agent_endpoint must be an absolute URL with a scheme and host")
        return self


class GangWorkloadRequest(AgentModel):
    """One workload submission coordinated across multiple native execution cells."""

    model_config = ConfigDict(extra="forbid")

    members: tuple[GangWorkloadMember, ...] = Field(min_length=2)
    spec: WireJsonObject

    @model_validator(mode="after")
    def validate_members(self) -> GangWorkloadRequest:
        cell_ids = tuple(member.cell_id for member in self.members)
        if len(cell_ids) != len(set(cell_ids)):
            raise ValueError("gang member cell IDs must be unique")
        endpoints = {member.agent_endpoint for member in self.members}
        if len(endpoints) != 1:
            raise ValueError("native workload gangs require every member to use the same agent_endpoint")
        generations = {member.mutation.generation for member in self.members}
        if len(generations) != 1:
            raise ValueError("native workload gang member generations must match")
        return self


class WorkloadMutationRequest(AgentModel):
    mutation: MutationModel


class GangWorkloadMutationRequest(AgentModel):
    """Fenced stop or retirement request for an existing native workload gang."""

    model_config = ConfigDict(extra="forbid")

    members: tuple[GangWorkloadMember, ...] = Field(min_length=2)

    @model_validator(mode="after")
    def validate_members(self) -> GangWorkloadMutationRequest:
        cell_ids = tuple(member.cell_id for member in self.members)
        if len(cell_ids) != len(set(cell_ids)):
            raise ValueError("gang member cell IDs must be unique")
        endpoints = {member.agent_endpoint for member in self.members}
        if len(endpoints) != 1:
            raise ValueError("native workload gangs require every member to use the same agent_endpoint")
        generations = {member.mutation.generation for member in self.members}
        if len(generations) != 1:
            raise ValueError("native workload gang member generations must match")
        return self


class CheckpointRequest(WorkloadMutationRequest):
    hook: str = Field(min_length=1)
    output_uri: str | None = None


class AgentObservation(AgentModel):
    cell_id: str
    state: CapacityStatus
    generation: int
    execution_endpoint: str | None = None
    detail: str = ""
    metadata: WireJsonObject = Field(default_factory=dict)
    observed_tpu_vms: int | None = Field(default=None, ge=1)


class WorkloadObservation(AgentModel):
    remote_id: str
    status: ExecutionWorkloadStatus
    message: str = ""
    checkpoint_uri: str | None = None


class WorkloadReference(AgentModel):
    """Minimal response returned after submission or checkpoint creation."""

    remote_id: str = Field(min_length=1)
