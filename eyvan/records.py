# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Stable control-plane records shared by services and persistence adapters."""

from __future__ import annotations

import hashlib
import json
import re
import typing as tp
from datetime import datetime, timedelta
from pathlib import PurePosixPath
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator
from pydantic import JsonValue as PydanticJsonValue

from eyvan._internal.compat import UTC, StrEnum
from eyvan._internal.json_types import is_json_object
from eyvan.capacity import CapacityStatus
from eyvan.capacity.drivers.base import CapacityProviderFailureCode, canonical_execution_backend
from eyvan.domain import costing
from eyvan.domain.models import InfeasibilityCode, WorkerResourceScope
from eyvan.worker.distributed_environment import (
    DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES,
    is_distributed_topology_environment_name,
)

JsonDocument: tp.TypeAlias = dict[str, PydanticJsonValue]
"""A recursively JSON-compatible document accepted by Pydantic models."""

_SAFE_FLEET_COMPONENT = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$")
_ENVIRONMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_MAX_RETRIES = 3
MAX_GCP_TPU_MULTISLICE_WORKERS = 256
WORKER_COUNT_LABEL = "eyvan_worker_count"
CALLABLE_SIGNING_KEY_ENV = "EYVAN_CALLABLE_SIGNING_KEY"
REMOTE_CALLABLE_SIGNATURE_ERROR = (
    "Eyvan remote callable signatures are not supported because no controller-managed verifier is available"
)
REMOTE_CALLABLE_SIGNING_KEY_ERROR = (
    "Eyvan remote callable signing keys are not supported because Eyvan never forwards caller secrets to workers. "
    "Remove signing_key or EYVAN_CALLABLE_SIGNING_KEY; controller-managed callable signing is not available yet."
)

# These values are injected by the controller or native slice executor.  A
# fleet runtime cannot own them, otherwise a submitted program could break its
# fencing or distributed JAX rendezvous contract.
_RESERVED_RUNTIME_ENVIRONMENT_NAMES = frozenset(
    {
        "HOME",
        "LOGNAME",
        "PATH",
        "USER",
        "VIRTUAL_ENV",
        "XDG_CACHE_HOME",
        "EYVAN_WORKLOAD_ID",
        "EYVAN_ATTEMPT",
        "EYVAN_RESUME_CHECKPOINT_URI",
    }
    | DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES
)
_PROTECTED_CONTAINER_MOUNT_SOURCES = (
    PurePosixPath("/etc/eyvan-native"),
    PurePosixPath("/run/eyvan-native"),
)
_AUTOMATIC_CONTAINER_MOUNT_TARGETS = frozenset(
    {
        "/opt/eyvan-native/oci-callable-runner",
        "/opt/eyvan-native/multihost",
        "/var/lib/eyvan/workload",
    }
)


def native_container_mount_targets_overlap(left: str, right: str) -> bool:
    """Return whether two absolute container mount targets overlap.

    Docker permits nested bind mounts, but that lets a later mount shadow files
    from an earlier one.  Eyvan uses bind mounts for its workload directory,
    multi-host rendezvous data, and the OCI callable bridge, so target paths
    must be disjoint rather than merely distinct strings.
    """

    left_path = PurePosixPath(left)
    right_path = PurePosixPath(right)
    return left_path == right_path or left_path in right_path.parents or right_path in left_path.parents


def _first_overlapping_mount_target(targets: tp.Sequence[str]) -> str | None:
    """Return the later target in the first overlapping pair, if any."""

    for index, target in enumerate(targets):
        if any(native_container_mount_targets_overlap(target, prior) for prior in targets[:index]):
            return target
    return None


def utcnow() -> datetime:
    """Return an aware UTC timestamp."""

    return datetime.now(UTC)


def _tpu_node_prefix(cell_id: str) -> str:
    """Return a deterministic GCP-safe node prefix for one physical cell."""

    normalized = re.sub(r"[^a-z0-9-]+", "-", cell_id.lower()).strip("-") or "eyvan-cell"
    direct = f"{normalized}-node"
    if len(direct) <= 63:
        return direct
    digest = hashlib.sha256(cell_id.encode()).hexdigest()[:8]
    return f"{normalized[:49].rstrip('-')}-{digest}-node"


def _validate_string_map(
    values: tp.Mapping[str, PydanticJsonValue],
    *,
    field_name: str,
    forbidden_keys: frozenset[str] = frozenset(),
    validate_string_values: bool = True,
) -> None:
    for key, value in values.items():
        if not key or key != key.strip():
            raise ValueError(f"{field_name} keys must be non-empty and trimmed")
        if key.casefold() in {candidate.casefold() for candidate in forbidden_keys}:
            raise ValueError(f"{field_name} key {key!r} collides with a built-in field")
        if validate_string_values and isinstance(value, str) and (not value or value != value.strip()):
            raise ValueError(f"{field_name} values must be non-empty and trimmed")


def _validate_strings(values: tp.Iterable[str], *, field_name: str) -> None:
    if any(not value or value != value.strip() for value in values):
        raise ValueError(f"{field_name} values must be non-empty and trimmed")


def _validate_string(value: str | None, *, field_name: str, optional: bool = False) -> None:
    if value is None and optional:
        return
    if value is None or not value or value != value.strip():
        raise ValueError(f"{field_name} must be non-empty and trimmed")


def _resource_quantities(resources: ResourceVector) -> dict[str, float]:
    return {
        "cpu": resources.cpu,
        "memory_gib": resources.memory_gib,
        "gpu": resources.gpu,
        "tpu": resources.tpu,
        **resources.custom,
    }


class EyvanModel(BaseModel):
    """Base model for persisted and API-facing control-plane data."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class NativeRuntimeArtifact(EyvanModel):
    """A GCS artifact staged on every native capacity host before work starts."""

    source: str
    destination: str
    recursive: bool = False

    @model_validator(mode="after")
    def validate_artifact(self) -> NativeRuntimeArtifact:
        if not self.source.startswith("gs://") or not self.source[5:].split("/", 1)[0]:
            raise ValueError("native runtime artifact source must be a gs:// URI")
        if "\x00" in self.source:
            raise ValueError("native runtime artifact source must not contain NUL bytes")
        destination = PurePosixPath(self.destination)
        if not destination.is_absolute():
            raise ValueError("native runtime artifact destination must be an absolute path")
        if "\x00" in self.destination or ".." in destination.parts:
            raise ValueError("native runtime artifact destination must not contain NUL bytes or '..'")
        if str(destination) == "/" or str(destination).startswith("//"):
            raise ValueError("native runtime artifact destination must not be root or start with '//'")
        return self


class NativeContainerMount(EyvanModel):
    """One explicit host path made available to an Eyvan-native OCI workload."""

    source: str
    target: str
    read_only: bool = True

    @model_validator(mode="after")
    def validate_mount(self) -> NativeContainerMount:
        for field_name, value in (("source", self.source), ("target", self.target)):
            if value != value.strip() or "\x00" in value:
                raise ValueError(f"native container mount {field_name} must be trimmed and NUL-free")
            path = PurePosixPath(value)
            if not path.is_absolute():
                raise ValueError(f"native container mount {field_name} must be an absolute path")
            if ".." in path.parts or str(path) == "/" or str(path).startswith("//"):
                raise ValueError(
                    f"native container mount {field_name} must not be root, start with '//', or contain '..'"
                )
            if field_name == "source" and any(
                path == protected or protected in path.parents for protected in _PROTECTED_CONTAINER_MOUNT_SOURCES
            ):
                raise ValueError(f"native container mount source {value!r} exposes Eyvan-native credentials")
        return self


class NativeContainerConfig(EyvanModel):
    """Structured OCI execution settings for an Eyvan-native workload.

    This intentionally exposes only stable runtime properties. Arbitrary Docker
    command-line flags are not part of Eyvan's durable fleet API.
    """

    image: str
    pull_policy: Literal["always", "if-not-present", "never"] = "if-not-present"
    working_dir: str | None = None
    privileged: bool = False
    mounts: tuple[NativeContainerMount, ...] = ()

    @model_validator(mode="after")
    def validate_container(self) -> NativeContainerConfig:
        image = self.image
        if (
            image != image.strip()
            or "\x00" in image
            or any(character.isspace() for character in image)
            or image.startswith(("-", "/", "."))
            or "://" in image
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/@:+-]*", image)
        ):
            raise ValueError("native container image must be an OCI image reference")
        if self.working_dir is not None:
            working_dir = self.working_dir
            if working_dir != working_dir.strip() or "\x00" in working_dir:
                raise ValueError("native container working_dir must be trimmed and NUL-free")
            path = PurePosixPath(working_dir)
            if not path.is_absolute() or ".." in path.parts or str(path) == "/" or str(path).startswith("//"):
                raise ValueError("native container working_dir must be an absolute non-root path without '..'")
        targets = [str(PurePosixPath(mount.target)) for mount in self.mounts]
        if len(targets) != len(set(targets)):
            raise ValueError("native container mounts must not share a target")
        conflicting_target = _first_overlapping_mount_target(targets)
        if conflicting_target is not None:
            raise ValueError(
                f"native container mount target {conflicting_target!r} overlaps another configured mount target"
            )
        return self


class NativeRuntimeConfig(EyvanModel):
    """Declarative host setup and workload defaults for Eyvan-native cells.

    The configuration is deliberately host-oriented. ``vm_image`` selects a
    GCP Compute OS image, while ``container`` describes OCI execution handled
    by the Eyvan-native runtime.
    """

    env: dict[str, str] = Field(default_factory=dict)
    init_commands: tuple[str, ...] = ()
    artifacts: tuple[NativeRuntimeArtifact, ...] = ()
    vm_image: str | None = None
    container: NativeContainerConfig | None = None

    @property
    def is_empty(self) -> bool:
        """Return whether this runtime changes neither bootstrap nor workloads."""

        return not (self.env or self.init_commands or self.artifacts or self.vm_image or self.container is not None)

    def merged(self, override: NativeRuntimeConfig | None) -> NativeRuntimeConfig:
        """Apply a node-type runtime over fleet defaults deterministically."""

        if override is None:
            return self
        return NativeRuntimeConfig(
            env={**self.env, **override.env},
            init_commands=(*self.init_commands, *override.init_commands),
            artifacts=(*self.artifacts, *override.artifacts),
            vm_image=override.vm_image if override.vm_image is not None else self.vm_image,
            container=override.container if override.container is not None else self.container,
        )

    @model_validator(mode="after")
    def validate_runtime(self) -> NativeRuntimeConfig:
        for name, value in self.env.items():
            if not _ENVIRONMENT_NAME.fullmatch(name):
                raise ValueError("native runtime environment names must be valid shell identifiers")
            if name in _RESERVED_RUNTIME_ENVIRONMENT_NAMES or name.startswith("EYVAN_"):
                raise ValueError(f"native runtime environment variable {name!r} is reserved by Eyvan")
            if "\x00" in value:
                raise ValueError(f"native runtime environment variable {name!r} must not contain NUL bytes")
        for command in self.init_commands:
            if not command.strip():
                raise ValueError("native runtime init command must be non-empty")
            if "\x00" in command:
                raise ValueError("native runtime init command must not contain NUL bytes")
        destinations = [artifact.destination for artifact in self.artifacts]
        conflicting_destination = _first_overlapping_mount_target(destinations)
        if conflicting_destination is not None:
            raise ValueError(
                f"native runtime artifact destination {conflicting_destination!r} overlaps another artifact destination"
            )
        if self.container is not None:
            automatic_artifact_target = next(
                (
                    destination
                    for destination in destinations
                    if any(
                        native_container_mount_targets_overlap(destination, automatic_target)
                        for automatic_target in _AUTOMATIC_CONTAINER_MOUNT_TARGETS
                    )
                ),
                None,
            )
            if automatic_artifact_target is not None:
                raise ValueError(
                    "native runtime artifact destination "
                    f"{automatic_artifact_target!r} conflicts with an Eyvan automatic mount"
                )
            reserved_targets = (*_AUTOMATIC_CONTAINER_MOUNT_TARGETS, *destinations)
            conflicting_target = next(
                (
                    mount.target
                    for mount in self.container.mounts
                    if any(
                        native_container_mount_targets_overlap(mount.target, reserved_target)
                        for reserved_target in reserved_targets
                    )
                ),
                None,
            )
            if conflicting_target is not None:
                raise ValueError(
                    f"native container mount target {conflicting_target!r} conflicts with an Eyvan automatic mount"
                )
        if self.vm_image is not None:
            if not self.vm_image.strip():
                raise ValueError("native runtime vm_image must be non-empty when set")
            if "\x00" in self.vm_image:
                raise ValueError("native runtime vm_image must not contain NUL bytes")
        return self


class WarmWindow(EyvanModel):
    """A recurring window during which a pool keeps a raised cell floor.

    Scale-to-zero saves real money overnight and costs it at 09:00, when every user
    pays the full provisioning latency simultaneously. A window is the smallest thing
    that fixes that without predicting anything.

    Time is a **fixed UTC offset**, not a named zone. Eyvan ships no timezone
    database, and a DST-aware window it cannot actually honour would be worse than
    one whose limits are stated: a fleet in a DST region shifts by an hour twice a
    year, which is visible and adjustable, rather than silently wrong.
    """

    min_cells: int = Field(ge=0, le=10_000)
    #: Half-open ``[start_hour, end_hour)`` so adjacent windows do not overlap by one
    #: hour. ``end_hour`` may be less than ``start_hour`` to wrap past midnight.
    start_hour: int = Field(ge=0, le=23)
    end_hour: int = Field(ge=0, le=23)
    #: ISO weekdays (Monday is 1). Empty means every day.
    days: tuple[int, ...] = ()
    utc_offset_minutes: int = Field(default=0, ge=-1440, le=1440)

    @model_validator(mode="after")
    def validate_window(self) -> WarmWindow:
        if self.start_hour == self.end_hour:
            raise ValueError("warm window start_hour and end_hour must differ; a zero-length window never applies")
        if any(day < 1 or day > 7 for day in self.days):
            raise ValueError("warm window days must be ISO weekdays from 1 (Monday) through 7 (Sunday)")
        if len(set(self.days)) != len(self.days):
            raise ValueError("warm window days must not repeat")
        return self

    def covers(self, moment: datetime) -> bool:
        """Return whether ``moment`` falls inside this window."""

        local = moment + timedelta(minutes=self.utc_offset_minutes)
        if self.days and local.isoweekday() not in self.days:
            return False
        if self.start_hour < self.end_hour:
            return self.start_hour <= local.hour < self.end_hour
        # Wraps past midnight: the window is the union of both ends of the day. The
        # weekday test above uses the *start* side, so an overnight window is named
        # by the day it begins on.
        return local.hour >= self.start_hour or local.hour < self.end_hour


class CoordinationKind(StrEnum):
    """What a coordinator record coordinates, and therefore how it settles.

    A coordinator owns children and has no lease of its own, so nothing but a
    dedicated controller phase can ever settle it. Naming the kind explicitly --
    rather than inferring "coordinator" from ``schedulable is False`` -- keeps that
    phase from capturing unrelated unschedulable records as they are introduced.
    """

    BATCH = "batch"
    DAG = "dag"
    #: A long-running service: a coordinator whose children are *replicas*.
    #: Settled by nobody. A batch parent finishes when its children do; a
    #: service's children are periodically all-terminal by design, which is the
    #: moment a replacement is due -- so the batch settler must not see it.
    SERVICE = "service"
    #: Differently-shaped members that admit together. Distinct from BATCH because
    #: the settler must not declare a group finished from a subset of its members,
    #: and distinct from DAG because there are no edges between them -- only the
    #: shared admission gate.
    GROUP = "group"


class WorkloadState(StrEnum):
    """Durable lifecycle states for a whole workload."""

    PENDING = "pending"
    ADMITTED = "admitted"
    LEASED = "leased"
    DISPATCHING = "dispatching"
    RUNNING = "running"
    CHECKPOINTING = "checkpointing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"


class AutoscaleBlockReason(StrEnum):
    """Durable reason an autoscaler cannot add a workload's required capacity."""

    SHARED_TPU_QUOTA = "shared_tpu_quota"
    QUEUE_ADMISSION_DENIED = "queue_admission_denied"


class CellState(StrEnum):
    """Observed availability of an execution cell."""

    REGISTERING = "registering"
    READY = "ready"
    DRAINING = "draining"
    DELETING = "deleting"
    UNAVAILABLE = "unavailable"
    DELETED = "deleted"


class LeaseState(StrEnum):
    """Fenced ownership states for capacity leases."""

    PENDING = "pending"
    ACTIVE = "active"
    REVOKING = "revoking"
    RELEASED = "released"
    EXPIRED = "expired"
    QUARANTINED = "quarantined"


class OutboxState(StrEnum):
    """Delivery state for a transactional backend command."""

    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DISCARDED = "discarded"


class Role(StrEnum):
    """Organization and project roles understood by the control plane."""

    FLEET_ADMIN = "fleet_admin"
    ORG_ADMIN = "org_admin"
    PROJECT_ADMIN = "project_admin"
    PROJECT_OPERATOR = "project_operator"
    PROJECT_VIEWER = "project_viewer"


class Permission(StrEnum):
    """Actions enforced by the service authorization policy."""

    READ = "read"
    SUBMIT = "submit"
    OPERATE = "operate"
    MANAGE_PROJECT = "manage_project"
    #: Administer capacity an organization owns. Distinct from MANAGE_FLEET,
    #: which reaches every tenant's capacity and the provider configuration
    #: underneath it: an org admin needs to size its own pools without being
    #: handed the ability to delete another tenant's cells.
    MANAGE_OWN_CAPACITY = "manage_own_capacity"
    MANAGE_FLEET = "manage_fleet"


class CapacityClass(StrEnum):
    """Canonical capacity purchase classes used at the API boundary."""

    RESERVED = "reserved"
    ON_DEMAND = "on_demand"
    SPOT = "spot"


class PriorityBand(StrEnum):
    """Strict scheduler priority bands."""

    BATCH = "batch"
    INTERACTIVE = "interactive"
    PRODUCTION = "production"


def normalize_capacity_class(value: tp.Any) -> tp.Any:
    """Normalize accepted API aliases to one durable capacity-class spelling."""

    if isinstance(value, str):
        value = value.strip().lower().replace("-", "_")
    return value


CanonicalCapacityClass = Annotated[CapacityClass, BeforeValidator(normalize_capacity_class)]


class GcpQueueAdmissionDenialTarget(EyvanModel):
    """Exact GCP TPU target rejected before it entered the capacity queue."""

    provider_project: str
    zone: str
    accelerator_type: str
    capacity_class: CanonicalCapacityClass

    @model_validator(mode="after")
    def validate_identity(self) -> GcpQueueAdmissionDenialTarget:
        """Require the immutable provider identity needed for local suppression."""

        _validate_string(self.provider_project, field_name="GCP queue-denial project")
        _validate_string(self.zone, field_name="GCP queue-denial zone")
        _validate_string(self.accelerator_type, field_name="GCP queue-denial accelerator type")
        return self


class RoleGrant(EyvanModel):
    """A role scoped to a fleet, organization, or project."""

    role: Role
    org_id: str | None = Field(default=None, max_length=128)
    project_id: str | None = Field(default=None, max_length=128)

    @model_validator(mode="after")
    def validate_scope(self) -> RoleGrant:
        if self.role == Role.FLEET_ADMIN:
            # Fleet administration is global; drop any org/project scope so a
            # stored grant can never appear narrower than the access it gives.
            if self.org_id is not None or self.project_id is not None:
                object.__setattr__(self, "org_id", None)
                object.__setattr__(self, "project_id", None)
            return self
        if not self.org_id:
            raise ValueError(f"{self.role.value} requires an organization scope")
        if self.role == Role.ORG_ADMIN:
            # Same reason as fleet admin above, one level down. Organization
            # administration covers every project in the organization -- the
            # authorizer never consults ``project_id`` for this role -- so a
            # stored grant reading ``org_admin @ acme/research`` would suggest a
            # boundary that does not exist, in an audit log, an admin UI, or a
            # reviewer's head.
            if self.project_id is not None:
                object.__setattr__(self, "project_id", None)
            return self
        if self.role in {Role.PROJECT_ADMIN, Role.PROJECT_OPERATOR, Role.PROJECT_VIEWER} and not self.project_id:
            raise ValueError(f"{self.role.value} requires a project scope")
        return self


class RoleGrantRecord(EyvanModel):
    """A durable role grant this control plane issued and can revoke.

    Until this existed, ``eyvan_grants`` in an OIDC token was the *only* source
    of authority, so onboarding a tenant meant configuring an identity provider
    to mint a bespoke claim that deserializes into ``tuple[RoleGrant, ...]`` --
    a claim mentioned nowhere in the README. Anyone whose IdP could not be
    taught a custom claim could not use Eyvan's authorization at all, and
    revoking access meant an IdP change rather than an API call.

    The claim is still honored. This is a second source, not a replacement:
    a deployment that already mints the claim keeps working, and one that cannot
    now has somewhere to put grants. They are unioned, because a grant from
    either source is a grant the operator made.

    Keyed by ``(subject, role, org_id, project_id)`` so re-issuing the same
    grant overwrites rather than accumulating -- there is no meaningful
    difference between holding one copy of a role and holding two.
    """

    subject: str = Field(min_length=1, max_length=256)
    grant: RoleGrant
    #: Time-boxed access, for the case that would otherwise be permanent: a
    #: temporary elevation during an incident. ``None`` is indefinite, which is
    #: the ordinary case and should not be dressed up as an expiry nobody chose.
    expires_at: datetime | None = None
    reason: str = Field(default="", max_length=512)
    granted_by: str = Field(min_length=1, max_length=256)
    granted_at: datetime = Field(default_factory=utcnow)

    @property
    def key(self) -> tuple[str, str, str, str]:
        """Return the identity this grant is stored under."""

        return (self.subject, self.grant.role.value, self.grant.org_id or "", self.grant.project_id or "")

    def active_at(self, moment: datetime) -> bool:
        """Return whether this grant confers authority at one instant."""

        return self.expires_at is None or self.expires_at > moment

    @model_validator(mode="after")
    def validate_grant_record(self) -> RoleGrantRecord:
        _validate_string(self.subject, field_name="grant subject")
        _validate_string(self.granted_by, field_name="grant issuer")
        if self.expires_at is not None and self.expires_at <= self.granted_at:
            raise ValueError("grant expiry must be after the moment it was granted")
        return self


class Principal(EyvanModel):
    """Verified caller identity and its explicit role grants."""

    subject: str = Field(min_length=1)
    display_name: str | None = None
    grants: tuple[RoleGrant, ...] = ()


class ResourceVector(EyvanModel):
    """Scalar resources requested by one workload replica or offered by a cell."""

    cpu: float = Field(default=1.0, ge=0)
    memory_gib: float = Field(default=1.0, ge=0)
    gpu: int = Field(default=0, ge=0)
    tpu: int = Field(default=0, ge=0)
    custom: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_custom(self) -> ResourceVector:
        if any(value < 0 for value in self.custom.values()):
            raise ValueError("custom resource quantities must be non-negative")
        _validate_string_map(
            self.custom,
            field_name="custom resources",
            forbidden_keys=frozenset({"cpu", "memory", "memory_gib", "gpu", "tpu"}),
        )
        return self


class ResourceLimits(ResourceVector):
    """Resource limit vector whose omitted scalar dimensions mean zero."""

    cpu: float = Field(default=0, ge=0)
    memory_gib: float = Field(default=0, ge=0)


ResourceVectorT = tp.TypeVar("ResourceVectorT", bound=ResourceVector)


def scale_resources(resources: ResourceVectorT, multiplier: int) -> ResourceVectorT:
    """Return a resource vector scaled by a positive worker count."""

    if isinstance(multiplier, bool) or not isinstance(multiplier, int) or multiplier < 1:
        raise ValueError("resource multiplier must be a positive integer")
    return resources.model_copy(
        update={
            "cpu": resources.cpu * multiplier,
            "memory_gib": resources.memory_gib * multiplier,
            "gpu": resources.gpu * multiplier,
            "tpu": resources.tpu * multiplier,
            "custom": {name: amount * multiplier for name, amount in resources.custom.items()},
        }
    )


class ResourceHours(ResourceLimits):
    """Fractional resource-time quantities used only for accounting."""

    gpu: float = Field(default=0, ge=0)
    tpu: float = Field(default=0, ge=0)


def normalize_resource_hours(value: tp.Any) -> tp.Any:
    if isinstance(value, ResourceVector) and not isinstance(value, ResourceHours):
        return value.model_dump()
    return value


CanonicalResourceHours = Annotated[ResourceHours, BeforeValidator(normalize_resource_hours)]


def normalize_resource_limits(value: tp.Any) -> tp.Any:
    """Convert legacy resource vectors without carrying their workload defaults."""

    if isinstance(value, ResourceVector) and not isinstance(value, ResourceLimits):
        return value.model_dump(exclude_unset=True)
    return value


CanonicalResourceLimits = Annotated[ResourceLimits, BeforeValidator(normalize_resource_limits)]
CanonicalExecutor = Annotated[str, BeforeValidator(canonical_execution_backend)]
CanonicalOptionalExecutor = Annotated[
    str | None,
    BeforeValidator(lambda value: None if value is None else canonical_execution_backend(value)),
]


class ResourceGang(EyvanModel):
    """Atomic group of equal resource bundles."""

    replicas: int = Field(default=1, ge=1)
    per_replica: ResourceVector = Field(default_factory=ResourceVector)
    required_topology: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_topology(self) -> ResourceGang:
        if not any(quantity > 0 for quantity in _resource_quantities(self.per_replica).values()):
            raise ValueError("gang per-replica resources must contain a positive quantity")
        _validate_string_map(self.required_topology, field_name="required topology")
        return self


class CommandExecution(EyvanModel):
    """A shell command submitted to a cell execution backend."""

    kind: Literal["command"] = "command"
    command: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_command(self) -> CommandExecution:
        _validate_string(self.command, field_name="command")
        return self


class CallableExecution(EyvanModel):
    """Reference to a callable artifact in trusted object storage."""

    kind: Literal["callable"] = "callable"
    artifact_uri: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    python: str = Field(pattern=r"^\d+\.\d+$")
    format_version: int = Field(default=1, ge=1)
    signature: str | None = None
    result_uri: str | None = None
    workspace_uri: str | None = None
    workspace_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    #: HMAC of the workspace payload under the same key as ``signature``. A
    #: workspace lands at ``sys.path[0]`` and becomes cwd *before* the callable
    #: is unpickled, so an unsigned bundle can shadow any module the pickle
    #: imports by reference -- which would make ``signature`` decorative.
    workspace_signature: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_uris(self) -> CallableExecution:
        _validate_string(self.artifact_uri, field_name="callable artifact URI")
        _validate_string(self.result_uri, field_name="callable result URI", optional=True)
        _validate_string(self.workspace_uri, field_name="callable workspace URI", optional=True)
        if (self.workspace_uri is None) != (self.workspace_sha256 is None):
            raise ValueError("callable workspace URI and digest must be provided together")
        if self.workspace_signature is not None and self.workspace_uri is None:
            raise ValueError("callable workspace signature requires a workspace URI")
        if self.signature is not None and self.workspace_uri is not None and self.workspace_signature is None:
            # Signing the callable but not its workspace buys nothing: the
            # workspace is on sys.path first and can replace what the signed
            # pickle resolves.
            raise ValueError("a signed callable with a workspace must also sign the workspace bundle")
        return self


WorkloadExecution = Annotated[CommandExecution | CallableExecution, Field(discriminator="kind")]


class WorkspaceRef(EyvanModel):
    """A packaged working tree to materialize on the host before the workload runs.

    Deliberately a property of the *workload*, not of one execution kind. A
    workspace answers "what source is on this host", which is independent of
    whether the process is launched as a shell command or as a pickled callable.

    Modelling it on :class:`CallableExecution` -- where it first appeared,
    because callables needed it first -- made that independence untrue in
    practice: ``CommandExecution`` has only a command string, so ``eyvan run``
    could not ship a working tree at all, and the only way to get your own code
    onto an accelerator was to bake an image or use the Python decorator. That
    was a modelling accident, not a decision.

    The three fields travel together and are validated together: a URI without a
    digest cannot be verified, and a signature without a URI signs nothing.
    """

    uri: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    #: HMAC of the bundle payload. A workspace is extracted and placed ahead of
    #: the workload's own imports, so an unsigned bundle can shadow any module
    #: the workload resolves by name.
    signature: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_workspace_ref(self) -> WorkspaceRef:
        """Reject a reference that cannot be verified on arrival."""

        _validate_string(self.uri, field_name="workspace URI")
        return self


class WorkloadDefinition(EyvanModel):
    """Execution and placement intent accepted by the public API."""

    name: str = Field(min_length=1, max_length=200)
    execution: WorkloadExecution
    #: Source to materialize on the host before the entrypoint runs, for every
    #: execution kind. See :class:`WorkspaceRef`.
    workspace: WorkspaceRef | None = None

    def resolved_workspace(self) -> WorkspaceRef | None:
        """Return the workspace to materialize, from either supported location.

        ``CallableExecution`` carried these fields before they were hoisted onto
        the workload, and specs written against that shape are still accepted --
        a submission is a durable record, so an older client must keep working.
        The spec-level field wins when both are set, because it is the one a
        newer client fills in deliberately.
        """

        if self.workspace is not None:
            return self.workspace
        execution = self.execution
        if execution.kind != "callable" or execution.workspace_uri is None:
            return None
        return WorkspaceRef(
            uri=execution.workspace_uri,
            sha256=str(execution.workspace_sha256),
            signature=execution.workspace_signature,
        )

    gang: ResourceGang = Field(default_factory=ResourceGang)
    environment: dict[str, str] = Field(default_factory=dict)
    backend_options: JsonDocument = Field(default_factory=dict)
    required_executor: CanonicalOptionalExecutor = None
    metadata: dict[str, str] = Field(default_factory=dict)
    provider: str | None = None
    region: str | None = None
    accelerator_type: str | None = None
    capacity_class: CanonicalCapacityClass = CapacityClass.ON_DEMAND
    priority_band: PriorityBand = PriorityBand.INTERACTIVE
    intra_band_priority: int = Field(default=0, ge=0, le=1000)
    required_labels: dict[str, str] = Field(default_factory=dict)
    preferred_labels: dict[str, str] = Field(default_factory=dict)
    preferred_cells: tuple[str, ...] = ()
    #: Logical names of large inputs this workload pulls -- a dataset URI, a model
    #: checkpoint, a container image. Used only to *prefer* cells that recently ran
    #: something naming the same asset, so a wrong or stale guess costs one pull
    #: and can never make the workload unplaceable. Opaque to Eyvan: two workloads
    #: naming the same string are treated as wanting the same bytes, and nothing
    #: here fetches, validates or resolves the name.
    named_assets: tuple[str, ...] = ()
    #: Additional capacity classes this workload will accept, in preference order
    #: after ``capacity_class``. Additive rather than a replacement list so every
    #: existing spec keeps its exact behaviour: a spec that names none is still a
    #: single-class request, which is what the scheduler saw before portfolios.
    additional_capacity_classes: tuple[CanonicalCapacityClass, ...] = ()
    #: Regions this workload will accept, when it does not pin one. Mutually
    #: exclusive with ``region``: a pin is a statement about *the* region, and a
    #: list beside it would leave "which wins" to whichever check ran first.
    allowed_regions: tuple[str, ...] = ()
    #: Zones this workload will accept. Same rule and same reason as regions.
    allowed_zones: tuple[str, ...] = ()
    #: Accelerator types this workload will accept, when it does not pin one.
    #:
    #: Plumbing only, and the limit is worth stating where it is declared:
    #: ``_static_cell_failure`` requires ``worker_count`` equality and
    #: ``fits_within``, so one ``ResourceGang`` matches a v5p-16 *and* a v5p-32
    #: only if the request is smaller than both -- which for a whole-slice TPU job
    #: it is not. Widening this does not make a fixed-shape job portable across
    #: generations; that needs shape-adaptive gang rewriting, which is not built.
    allowed_accelerator_types: tuple[str, ...] = ()
    estimated_duration_seconds: int = Field(default=3600, ge=1)
    checkpointable: bool = False
    checkpoint_hook: str | None = None
    checkpoint_output_uri: str | None = None
    checkpoint_grace_seconds: int = Field(default=0, ge=0)
    max_retries: int = Field(default=0, ge=0, le=100)
    max_failure_retries: int = Field(default=0, ge=0, le=100)
    #: Whether this workload receives a capability to submit children of its own.
    #: Off by default: a job that does not fan out should not be handed a
    #: credential it cannot use, and a credential nobody asked for is one nobody
    #: is watching. The gang path issues one token for the attempt, so every rank
    #: receives the same one -- deciding which rank may use it is the SDK's job,
    #: not the token's.
    can_submit_children: bool = False
    cell_group_count: int = Field(default=1, ge=1, le=10_000)
    worker_count: int = Field(default=1, ge=1, le=10_000, strict=True)
    worker_resource_scope: WorkerResourceScope = WorkerResourceScope.ATOMIC_CELL

    @model_validator(mode="after")
    def validate_checkpoint(self) -> WorkloadDefinition:
        _validate_string(self.name, field_name="workload name")
        _validate_string(self.provider, field_name="provider", optional=True)
        _validate_string(self.region, field_name="region", optional=True)
        _validate_string(self.accelerator_type, field_name="accelerator type", optional=True)
        _validate_string(self.required_executor, field_name="required executor", optional=True)
        if not self.checkpointable and any(
            value is not None for value in (self.checkpoint_hook, self.checkpoint_output_uri)
        ):
            raise ValueError("checkpoint hook and output URI require checkpointable=true")
        if self.checkpointable and not (self.checkpoint_hook and self.checkpoint_output_uri):
            raise ValueError("checkpointable workloads require a checkpoint hook and durable output URI")
        _validate_string_map(self.required_labels, field_name="required labels")
        _validate_string_map(self.preferred_labels, field_name="preferred labels")
        _validate_string_map(self.metadata, field_name="metadata")
        _validate_string_map(self.environment, field_name="environment")
        for name, value in self.environment.items():
            if not _ENVIRONMENT_NAME.fullmatch(name):
                raise ValueError("workload environment names must be valid shell identifiers")
            if name in _RESERVED_RUNTIME_ENVIRONMENT_NAMES or is_distributed_topology_environment_name(name):
                raise ValueError(f"workload environment variable {name!r} is reserved by Eyvan")
            if "\x00" in value:
                raise ValueError(f"workload environment variable {name!r} must not contain NUL bytes")
        _validate_strings(self.preferred_cells, field_name="preferred cells")
        if len(self.preferred_cells) != len(set(self.preferred_cells)):
            raise ValueError("preferred cells must not contain duplicates")
        _validate_strings(self.named_assets, field_name="named assets")
        if len(self.named_assets) != len(set(self.named_assets)):
            raise ValueError("named assets must not contain duplicates")
        portfolio = self.capacity_class_portfolio
        if len(set(portfolio)) != len(portfolio):
            raise ValueError("additional capacity classes must not repeat the primary class or each other")
        for pin, choices, label in (
            (self.region, self.allowed_regions, "region"),
            (self.accelerator_type, self.allowed_accelerator_types, "accelerator type"),
        ):
            if pin and choices:
                raise ValueError(f"a workload pins one {label} or lists several, never both")
        # No zone-pin conflict to check: a spec has no ``zone`` field. Zones are
        # expressed only as a list, because a single zone is already sayable as a
        # one-element list and a second spelling would be a second thing to keep
        # consistent.
        for name, values in (
            ("allowed regions", self.allowed_regions),
            ("allowed zones", self.allowed_zones),
            ("allowed accelerator types", self.allowed_accelerator_types),
        ):
            _validate_strings(values, field_name=name)
            if len(set(values)) != len(values):
                raise ValueError(f"{name} must not contain duplicates")
        declared_worker_count = self.required_labels.get(WORKER_COUNT_LABEL)
        if declared_worker_count is not None and declared_worker_count != str(self.worker_count):
            raise ValueError(f"required label {WORKER_COUNT_LABEL!r} must match worker_count")
        if self.worker_count > 1:
            if self.cell_group_count != 1:
                raise ValueError("multi-worker capacity must use exactly one physical cell")
            if self.provider != "gcp":
                raise ValueError("multi-worker capacity requires the GCP provider")
            if self.required_executor != "eyvan-native":
                raise ValueError("multi-worker capacity requires the eyvan-native executor")
            if self.gang.per_replica.tpu <= 0 or self.gang.per_replica.gpu > 0:
                raise ValueError("multi-worker capacity requires a TPU resource request")
            if self.worker_count > MAX_GCP_TPU_MULTISLICE_WORKERS:
                raise ValueError(f"GCP TPU multislice worker_count must not exceed {MAX_GCP_TPU_MULTISLICE_WORKERS}")
        return self

    @property
    def capacity_class_portfolio(self) -> tuple[CanonicalCapacityClass, ...]:
        """Return every acceptable capacity class, most preferred first.

        ``capacity_class`` stays primary and stays the default, so a spec that
        declares no additional classes is the single-class request the scheduler
        already understood. The core scheduler has always ranked a portfolio --
        ``capacity_class_rank`` is the fourth key of ``PlacementScore`` -- and the
        only thing collapsing it to one entry was the adapter.
        """

        return (self.capacity_class, *self.additional_capacity_classes)

    @property
    def resources_per_cell(self) -> ResourceVector:
        """Return the aggregate resource reservation for one physical cell."""

        resources = scale_resources(self.gang.per_replica, self.gang.replicas)
        if self.worker_resource_scope is WorkerResourceScope.PER_WORKER:
            return scale_resources(resources, self.worker_count)
        return resources


class CellRejection(EyvanModel):
    """One cell-specific reason a workload could not be placed there."""

    cell_id: str = Field(max_length=128)
    code: InfeasibilityCode
    message: str = Field(max_length=512)


class WorkloadExplanation(EyvanModel):
    """Why one workload is or is not currently placeable.

    Computed on demand by re-running the pure scheduler against current state,
    so it always reflects the fleet as it is now rather than a verdict cached
    when the workload was last considered. The scheduler already produces the
    per-cell detail; persisting it would add a second field to keep in sync at
    every ``scheduling_reason`` write site, and would go stale the moment a
    cell changed.

    ``cells`` is bounded for display; ``considered_cell_count`` is the true
    number of rejected cells so a truncated list is never mistaken for the
    whole fleet.
    """

    workload_id: str = Field(max_length=96)
    state: WorkloadState
    placeable: bool
    placement_cell_id: str | None = None
    code: InfeasibilityCode | None = None
    message: str | None = Field(default=None, max_length=512)
    cells: tuple[CellRejection, ...] = ()
    considered_cell_count: int = Field(default=0, ge=0)
    #: Cells the autoscaler is currently building for this workload. The
    #: scheduler only ever sees cells that already exist, so without this a
    #: workload waiting on capacity is indistinguishable from one that can
    #: never be placed -- both report "no cell satisfies all hard workload
    #: constraints", and only one of them is a problem.
    provisioning_cell_ids: tuple[str, ...] = ()
    scheduling_reason: str | None = None
    autoscale_block_reason: AutoscaleBlockReason | None = None
    queue_admission_denial: GcpQueueAdmissionDenialTarget | None = None
    observed_at: datetime = Field(default_factory=utcnow)

    @property
    def truncated_cell_count(self) -> int:
        """Return how many rejected cells were omitted from ``cells``."""

        return max(0, self.considered_cell_count - len(self.cells))


class WorkloadRecord(EyvanModel):
    """Persisted workload aggregate."""

    id: str = Field(max_length=96)
    org_id: str = Field(max_length=128)
    project_id: str = Field(max_length=128)
    spec: WorkloadDefinition
    state: WorkloadState = WorkloadState.PENDING
    attempt: int = 1
    failure_count: int = Field(default=0, ge=0)
    capacity_loss_count: int = Field(default=0, ge=0)
    generation: int = 1
    lease_id: str | None = None
    lease_ids: tuple[str, ...] = ()
    remote_id: str | None = None
    #: Where this replica says it can be reached, once it says so.
    #:
    #: On the workload rather than in a registry table, which is what makes the
    #: liveness rule free: an endpoint is served only while the record carrying
    #: it is RUNNING at the current generation, and a replica that dies takes
    #: its endpoint with it. A separate table would need a TTL or a heartbeat to
    #: reach the same conclusion, and both are a second source of truth about
    #: something the control plane already knows exactly.
    #:
    #: Set by the workload itself after it can serve. That is real readiness --
    #: an authenticated statement by the process that would answer -- rather
    #: than an inference from a state machine that only knows the process
    #: started.
    service_endpoint: str | None = Field(default=None, max_length=512)
    checkpoint_uri: str | None = None
    failure_reason: str | None = None
    scheduling_reason: str | None = None
    autoscale_block_reason: AutoscaleBlockReason | None = None
    queue_admission_denial: GcpQueueAdmissionDenialTarget | None = None
    preemption_id: str | None = Field(default=None, max_length=96)
    #: Parent in a job tree. A child is an ordinary workload that competes as a
    #: normal sibling under the same (org, project, principal) fair-share leaf --
    #: job structure is a relation and a cancellation cascade, not a fourth
    #: scheduling level. See eyvan.server.service.subjobs for why.
    parent_workload_id: str | None = Field(default=None, max_length=96)
    #: False for a record that must never be placed. The scheduler considers every
    #: PENDING workload, so without this a batch of N tasks placed N+1 jobs and the
    #: extra one ran the template command with no task index -- on a full accelerator
    #: slice. Deliberately *not* the test for "is this a coordinator": several planned
    #: shapes are unschedulable without being coordinators (a DAG member waiting on a
    #: predecessor, a workload delegated to a peer control plane), and identifying a
    #: coordinator by the absence of a flag would silently capture all of them.
    schedulable: bool = True
    #: Set only on a record that owns children and derives its state from them.
    #: ``None`` means "not a coordinator" -- a positive, self-describing marker, so a
    #: new unschedulable shape cannot accidentally be settled from a sibling's state.
    coordination_kind: CoordinationKind | None = None
    #: Sibling workloads that must succeed before this one may be placed. Non-empty
    #: implies ``schedulable=False`` until the controller releases it. Dependencies
    #: are between whole workloads and carry no data: results already travel as
    #: ``eyvan://`` result URIs, and adding a value channel here would make the
    #: control plane a data plane.
    depends_on: tuple[str, ...] = ()
    created_by: str = Field(max_length=256)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    @model_validator(mode="after")
    def validate_identity(self) -> WorkloadRecord:
        _validate_string(self.id, field_name="workload id")
        _validate_string(self.org_id, field_name="organization id")
        _validate_string(self.project_id, field_name="project id")
        _validate_string(self.created_by, field_name="creator id")
        _validate_strings(self.lease_ids, field_name="workload lease ids")
        if len(self.lease_ids) != len(set(self.lease_ids)):
            raise ValueError("workload lease_ids must not contain duplicates")
        if self.lease_ids and self.lease_id != self.lease_ids[0]:
            raise ValueError("workload lease_id must be the first workload lease_ids entry")
        if self.lease_id is None and self.lease_ids:
            raise ValueError("workload lease_ids require lease_id")
        if (
            self.queue_admission_denial is not None
            and self.autoscale_block_reason is not AutoscaleBlockReason.QUEUE_ADMISSION_DENIED
        ):
            raise ValueError("queue_admission_denial requires autoscale_block_reason queue_admission_denied")
        return self


class CapacityBeliefState(EyvanModel):
    """Persisted empirical availability evidence for one execution cell."""

    success_evidence: float = Field(default=1.0, gt=0)
    failure_evidence: float = Field(default=1.0, gt=0)
    provision_latencies_s: tuple[float, ...] = ()
    interruptions: float = Field(default=0, ge=0)
    successful_hours: float = Field(default=0, ge=0)
    consecutive_failures: int = Field(default=0, ge=0)
    cooldown_until: datetime | None = None
    last_observed_at: datetime | None = None


class CellRecord(EyvanModel):
    """Persisted execution-cell inventory record."""

    id: str = Field(max_length=96)
    provider: str
    capacity_backend: str | None = None
    provider_project: str | None = None
    region: str
    zone: str | None = None
    state: CellState = CellState.REGISTERING
    capacity: CanonicalResourceLimits
    allocatable: CanonicalResourceLimits
    unit_prices: dict[str, float] = Field(default_factory=dict)
    accelerator_type: str | None = None
    capacity_class: CanonicalCapacityClass = CapacityClass.ON_DEMAND
    executor: CanonicalExecutor = "eyvan-native"
    endpoint: str | None = None
    pool_id: str | None = None
    autoscale_request_workload_id: str | None = Field(default=None, max_length=96)
    provider_options: JsonDocument = Field(default_factory=dict)
    runtime: NativeRuntimeConfig = Field(default_factory=NativeRuntimeConfig)
    labels: dict[str, str] = Field(default_factory=dict)
    topology: dict[str, str] = Field(default_factory=dict)
    max_replica_resources: CanonicalResourceLimits | None = None
    max_gang_replicas: int | None = Field(default=None, ge=1)
    worker_count: int = Field(default=1, ge=1, le=10_000, strict=True)
    provider_phase: CapacityStatus | None = None
    provider_failure_code: CapacityProviderFailureCode | None = None
    observed_tpu_vms: int | None = Field(default=None, ge=1)
    provider_phase_started_at: datetime | None = None
    unavailable_reason: str | None = None
    #: Free-text cause behind ``unavailable_reason``, which is a closed code that
    #: other paths compare. A fence conflict has three distinct causes and a
    #: provider error has many; collapsing them all to one code means an operator
    #: has to reconstruct the cause from an agent's storage document by hand.
    unavailable_detail: str | None = Field(default=None, max_length=2048)
    availability: CapacityBeliefState = Field(default_factory=CapacityBeliefState)
    generation: int = 1
    idle_since: datetime | None = None
    last_heartbeat_at: datetime | None = None
    #: When the provider started charging for this cell. Set once the provider
    #: resource exists, cleared when it is released. Distinct from ``created_at``:
    #: a queued TPU request is a record Eyvan holds and a bill nobody is paying.
    billing_started_at: datetime | None = None
    #: When the billing clock was last advanced. The accrual loop is at-least-once
    #: and may run late or twice; charging ``now - last_tick`` rather than one
    #: fixed interval makes a missed or repeated tick cost the right amount.
    billing_ticked_at: datetime | None = None
    #: Wall-clock seconds this cell has existed at the provider's expense.
    billed_seconds: float = Field(default=0.0, ge=0)
    #: The subset of ``billed_seconds`` during which no lease held the cell. This
    #: is capacity the fleet paid for and nobody used -- the number an operator
    #: needs to size a pool, and one that no lease-derived ledger can produce,
    #: because an idle cell has no lease to settle.
    idle_seconds: float = Field(default=0.0, ge=0)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    def hourly_cost(self) -> float:
        """Return what this cell costs per hour at its declared unit prices.

        Zero when the pool carries no prices -- which is exactly defect D7's
        shape, and why an unpriced pool is a ``doctor`` finding rather than
        something this method can compensate for. A silent zero here would make
        an idle-cost report say the fleet is free.
        """

        capacity = self.capacity
        quantities = {
            "cpu": capacity.cpu,
            "memory_gib": capacity.memory_gib,
            "gpu": float(capacity.gpu),
            "tpu": float(capacity.tpu),
            **capacity.custom,
        }
        return costing.total(*(price * quantities.get(resource, 0.0) for resource, price in self.unit_prices.items()))

    @model_validator(mode="before")
    @classmethod
    def upgrade_legacy_worker_count(cls, value: tp.Any) -> tp.Any:
        """Restore the worker count omitted by records written before that field existed."""

        if not is_json_object(value) or "worker_count" in value:
            return value
        provider_options = value.get("provider_options")
        if not is_json_object(provider_options) or "node_count" not in provider_options:
            return value
        node_count = provider_options["node_count"]
        if isinstance(node_count, bool) or not isinstance(node_count, int) or node_count < 1:
            raise ValueError("GCP TPU provider_options.node_count must be a positive integer")
        upgraded = dict(value)
        upgraded["worker_count"] = node_count
        return upgraded

    @model_validator(mode="after")
    def validate_provider_identity(self) -> CellRecord:
        _validate_string(self.id, field_name="cell id")
        _validate_string(self.provider, field_name="cell provider")
        _validate_string(self.capacity_backend, field_name="capacity backend", optional=True)
        _validate_string(self.provider_project, field_name="provider project", optional=True)
        _validate_string(self.region, field_name="cell region")
        _validate_string(self.zone, field_name="cell zone", optional=True)
        _validate_string(self.accelerator_type, field_name="cell accelerator type", optional=True)
        _validate_string(self.executor, field_name="cell executor")
        _validate_string(self.endpoint, field_name="cell endpoint", optional=True)
        _validate_string(self.pool_id, field_name="capacity pool id", optional=True)
        _validate_string(
            self.autoscale_request_workload_id,
            field_name="autoscale request workload id",
            optional=True,
        )
        if self.autoscale_request_workload_id is not None and self.pool_id is None:
            raise ValueError("autoscale request workload id requires a capacity pool")
        if self.executor in {"native-local", "eyvan-native"} and self.max_gang_replicas not in {None, 1}:
            raise ValueError(f"{self.executor} cells support exactly one gang replica")
        if not self.runtime.is_empty and self.executor != "eyvan-native":
            raise ValueError("native runtime configuration requires executor eyvan-native")
        if self.provider == "gcp" and not (self.provider_project and self.zone):
            raise ValueError("GCP cells require provider_project and zone")
        if self.capacity_backend == "gcp-tpu" and not self.accelerator_type:
            raise ValueError("GCP TPU cells require accelerator_type")
        node_count = self.provider_options.get("node_count", 1)
        if isinstance(node_count, bool) or not isinstance(node_count, int) or node_count < 1:
            raise ValueError("GCP TPU provider_options.node_count must be a positive integer")
        if node_count != self.worker_count:
            raise ValueError("cell worker_count must match provider_options.node_count")
        node_prefix = self.provider_options.get("node_prefix")
        if self.worker_count == 1 and node_prefix is not None:
            raise ValueError("provider_options.node_prefix requires worker_count greater than one")
        if self.worker_count > 1:
            if self.provider != "gcp" or self.capacity_backend != "gcp-tpu":
                raise ValueError("multi-worker cells require the GCP TPU capacity backend")
            if self.executor != "eyvan-native":
                raise ValueError("multi-worker cells require the eyvan-native executor")
            if self.worker_count > MAX_GCP_TPU_MULTISLICE_WORKERS:
                raise ValueError(f"GCP TPU multislice worker_count must not exceed {MAX_GCP_TPU_MULTISLICE_WORKERS}")
            if not isinstance(node_prefix, str) or not node_prefix.strip() or node_prefix != node_prefix.strip():
                raise ValueError("multi-worker GCP TPU cells require provider_options.node_prefix")
        declared_worker_count = self.labels.get(WORKER_COUNT_LABEL)
        if declared_worker_count is not None and declared_worker_count != str(self.worker_count):
            raise ValueError(f"cell label {WORKER_COUNT_LABEL!r} must match worker_count")
        if any(price < 0 for price in self.unit_prices.values()):
            raise ValueError("cell unit prices must be non-negative")
        _validate_string_map(self.unit_prices, field_name="unit prices")
        _validate_string_map(self.provider_options, field_name="provider options", validate_string_values=False)
        if not self.runtime.is_empty and self.provider_options.get("startup_script") is not None:
            raise ValueError("use runtime.init_commands instead of provider_options.startup_script")
        _validate_string_map(self.labels, field_name="cell labels")
        _validate_string_map(self.topology, field_name="cell topology")
        capacity = _resource_quantities(self.capacity)
        allocatable = _resource_quantities(self.allocatable)
        if not any(quantity > 0 for quantity in capacity.values()):
            raise ValueError("cell capacity must contain a positive resource quantity")
        if not any(quantity > 0 for quantity in allocatable.values()):
            raise ValueError("cell allocatable capacity must contain a positive resource quantity")
        for resource in capacity.keys() | allocatable.keys():
            if allocatable.get(resource, 0) > capacity.get(resource, 0):
                raise ValueError(f"allocatable {resource} must fit within cell capacity")
        if self.max_replica_resources is not None:
            maximum = _resource_quantities(self.max_replica_resources)
            if not any(quantity > 0 for quantity in maximum.values()):
                raise ValueError("max per-replica resources must contain a positive quantity")
            for resource in maximum.keys() | allocatable.keys():
                if maximum.get(resource, 0) > allocatable.get(resource, 0):
                    raise ValueError(f"max per-replica {resource} must fit within allocatable capacity")
        return self


class CapacityPoolRecord(EyvanModel):
    """Declarative node type used by the capacity autoscaler."""

    id: str = Field(max_length=192)
    fleet_name: str = Field(max_length=96)
    node_type: str = Field(max_length=96)
    #: Organization accountable for this pool, or ``None`` for shared capacity.
    #: Without it "an org admin may manage its own pools" had no referent: a
    #: pool belonged to nobody, so the only workable rule was fleet-admin-only.
    #: ``None`` keeps every existing pool shared and fleet-administered, so
    #: ownership is something an operator opts a pool into.
    owner_org_id: str | None = Field(default=None, max_length=128)
    provider: str
    capacity_backend: str | None = None
    provider_project: str | None = None
    region: str
    zone: str | None = None
    capacity: CanonicalResourceLimits
    allocatable: CanonicalResourceLimits
    unit_prices: dict[str, float] = Field(default_factory=dict)
    accelerator_type: str | None = None
    capacity_class: CanonicalCapacityClass = CapacityClass.ON_DEMAND
    executor: CanonicalExecutor = "eyvan-native"
    agent_endpoint: str
    provider_options: JsonDocument = Field(default_factory=dict)
    runtime: NativeRuntimeConfig = Field(default_factory=NativeRuntimeConfig)
    labels: dict[str, str] = Field(default_factory=dict)
    topology: dict[str, str] = Field(default_factory=dict)
    max_replica_resources: CanonicalResourceLimits | None = None
    max_gang_replicas: int | None = Field(default=None, ge=1)
    quota_group: str | None = Field(default=None, max_length=192)
    quota_limit: int | None = Field(default=None, ge=0)
    quota_units_per_cell: int | None = Field(default=None, ge=1)
    min_cells: int = Field(default=0, ge=0)
    #: Recurring windows that raise the floor above ``min_cells``. Scale-to-zero is
    #: correct overnight and wrong at the start of a working day, when every user pays
    #: the full provisioning latency at once.
    warm_windows: tuple[WarmWindow, ...] = ()
    initial_cells: int | None = Field(default=None, ge=0)
    max_cells: int = Field(default=1, ge=0, le=10_000)
    idle_timeout_s: float = Field(default=300, ge=0)
    queue_timeout_s: float | None = Field(default=None, gt=0)
    provisioning_timeout_s: float = Field(default=1800, gt=0)
    upscaling_speed: float = Field(default=1.0, gt=0)
    launch_batch: int = Field(default=5, ge=1)
    enabled: bool = True
    initialized_at: datetime | None = None
    generation: int = Field(default=1, ge=1)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    @model_validator(mode="after")
    def validate_pool(self) -> CapacityPoolRecord:
        for field_name, value in (
            ("capacity pool id", self.id),
            ("fleet name", self.fleet_name),
            ("node type", self.node_type),
            ("capacity provider", self.provider),
            ("capacity region", self.region),
            ("cell executor", self.executor),
            ("agent endpoint", self.agent_endpoint),
        ):
            _validate_string(value, field_name=field_name)
        if not _SAFE_FLEET_COMPONENT.fullmatch(self.fleet_name):
            raise ValueError("fleet name must contain only lowercase letters, digits, and hyphens")
        if not _SAFE_FLEET_COMPONENT.fullmatch(self.node_type):
            raise ValueError("node type must contain only lowercase letters, digits, and hyphens")
        _validate_string(self.provider_project, field_name="provider project", optional=True)
        _validate_string(self.capacity_backend, field_name="capacity backend", optional=True)
        _validate_string(self.zone, field_name="capacity zone", optional=True)
        _validate_string(self.accelerator_type, field_name="accelerator type", optional=True)
        if self.min_cells > self.max_cells:
            raise ValueError("capacity pool min_cells must not exceed max_cells")
        if self.initial_cells is not None and not (self.min_cells <= self.initial_cells <= self.max_cells):
            raise ValueError("capacity pool initial_cells must be between min_cells and max_cells")
        if self.capacity_backend == "gcp-tpu":
            node_count = self.provider_options.get("node_count", 1)
            if isinstance(node_count, bool) or not isinstance(node_count, int) or node_count != 1:
                raise ValueError("GCP TPU capacity pool provider_options.node_count must be exactly one when set")
            if self.provider_options.get("node_prefix") is not None:
                raise ValueError("GCP TPU capacity pool provider_options.node_prefix is autoscaler-owned")
        quota_fields = (self.quota_group, self.quota_limit, self.quota_units_per_cell)
        if any(value is not None for value in quota_fields):
            if self.quota_group is None or self.quota_limit is None or self.quota_units_per_cell is None:
                raise ValueError("quota_group, quota_limit, and quota_units_per_cell must be configured together")
            _validate_string(self.quota_group, field_name="GCP TPU quota group")
            if self.provider != "gcp" or self.capacity_backend != "gcp-tpu":
                raise ValueError("shared TPU quota budgets require provider gcp with capacity_backend gcp-tpu")
            if self.initial_target * self.quota_units_per_cell > self.quota_limit:
                raise ValueError("capacity pool initial quota demand exceeds quota_limit")
        CellRecord(
            id="capacity-pool-template",
            provider=self.provider,
            capacity_backend=self.capacity_backend,
            provider_project=self.provider_project,
            region=self.region,
            zone=self.zone,
            capacity=self.capacity,
            allocatable=self.allocatable,
            unit_prices=self.unit_prices,
            accelerator_type=self.accelerator_type,
            capacity_class=self.capacity_class,
            executor=self.executor,
            endpoint=self.agent_endpoint,
            pool_id=self.id,
            provider_options=self.provider_options,
            runtime=self.runtime,
            labels=self.labels,
            topology=self.topology,
            max_replica_resources=self.max_replica_resources,
            max_gang_replicas=self.max_gang_replicas,
        )
        return self

    @property
    def initial_target(self) -> int:
        return self.min_cells if self.initial_cells is None else self.initial_cells

    def warm_floor(self, now: datetime) -> int:
        """Return the cell floor in force at ``now``.

        The highest matching window wins rather than the first, so overlapping
        windows compose predictably however they are ordered -- an operator adding a
        "conference week" window on top of "business hours" gets the larger of the
        two, not whichever happens to be listed first.
        """

        # ``min_cells`` participates in the max rather than acting as a fallback: a
        # window only ever *raises* the floor, so one declaring fewer cells than the
        # pool's own baseline must not lower it.
        return max((self.min_cells, *(window.min_cells for window in self.warm_windows if window.covers(now))))

    @property
    def template_fingerprint(self) -> str:
        """Return the immutable capacity identity used to pin new workloads during rollouts."""

        payload = self.model_dump(
            mode="json",
            include={
                "provider",
                "capacity_backend",
                "provider_project",
                "region",
                "zone",
                "capacity",
                "accelerator_type",
                "capacity_class",
                "executor",
                "agent_endpoint",
                "provider_options",
                "runtime",
            },
        )
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        return hashlib.sha256(encoded).hexdigest()

    def make_cell(
        self,
        cell_id: str,
        *,
        worker_count: int = 1,
        autoscale_request_workload_id: str | None = None,
    ) -> CellRecord:
        """Materialize one managed cell containing ``worker_count`` complete TPU workers."""

        if isinstance(worker_count, bool) or not isinstance(worker_count, int) or worker_count < 1:
            raise ValueError("capacity cell worker_count must be a positive integer")
        provider_options = dict(self.provider_options)
        provider_options.pop("node_count", None)
        if worker_count > 1:
            if self.provider != "gcp" or self.capacity_backend != "gcp-tpu":
                raise ValueError("multi-worker cells require a GCP TPU capacity pool")
            if self.executor != "eyvan-native":
                raise ValueError("multi-worker cells require the eyvan-native executor")
            if worker_count > MAX_GCP_TPU_MULTISLICE_WORKERS:
                raise ValueError(f"GCP TPU multislice worker_count must not exceed {MAX_GCP_TPU_MULTISLICE_WORKERS}")
            provider_options["node_count"] = worker_count
            provider_options["node_prefix"] = _tpu_node_prefix(cell_id)

        return CellRecord(
            id=cell_id,
            provider=self.provider,
            capacity_backend=self.capacity_backend,
            provider_project=self.provider_project,
            region=self.region,
            zone=self.zone,
            capacity=scale_resources(self.capacity, worker_count),
            allocatable=scale_resources(self.allocatable, worker_count),
            unit_prices=self.unit_prices,
            accelerator_type=self.accelerator_type,
            capacity_class=self.capacity_class,
            executor=self.executor,
            endpoint=self.agent_endpoint,
            pool_id=self.id,
            autoscale_request_workload_id=autoscale_request_workload_id,
            provider_options=provider_options,
            runtime=self.runtime,
            labels={
                **self.labels,
                "eyvan_fleet": self.fleet_name,
                "eyvan_pool": self.id,
                "eyvan_pool_template": self.template_fingerprint,
                WORKER_COUNT_LABEL: str(worker_count),
            },
            topology=self.topology,
            max_replica_resources=(
                scale_resources(self.max_replica_resources, worker_count)
                if self.max_replica_resources is not None
                else None
            ),
            max_gang_replicas=self.max_gang_replicas,
            worker_count=worker_count,
        )


def validate_capacity_pool_quota_groups(pools: tp.Iterable[CapacityPoolRecord]) -> None:
    """Validate finite GCP TPU budgets shared by multiple autoscaling pools.

    A group represents one provider quota dimension, normally a GCP project,
    zone, generation, and purchase class. Different TPU shapes may consume a
    different number of units from that shared ceiling.
    """

    grouped: dict[str, list[CapacityPoolRecord]] = {}
    for pool in pools:
        if pool.quota_group is not None:
            grouped.setdefault(pool.quota_group, []).append(pool)

    for group, members in grouped.items():
        limits = {pool.quota_limit for pool in members}
        if len(limits) != 1:
            raise ValueError(f"quota group {group!r} must use one quota_limit across every pool")
        scopes = {
            (
                pool.provider,
                pool.capacity_backend,
                pool.provider_project,
                pool.region,
                pool.zone,
                pool.capacity_class.value,
            )
            for pool in members
        }
        if len(scopes) != 1:
            raise ValueError(f"quota group {group!r} cannot span distinct GCP TPU quota scopes")
        quota_limit = next(iter(limits))
        assert quota_limit is not None
        initial_units = 0
        for pool in members:
            quota_units_per_cell = pool.quota_units_per_cell
            assert quota_units_per_cell is not None
            initial_units += pool.initial_target * quota_units_per_cell
        if initial_units > quota_limit:
            raise ValueError(
                f"quota group {group!r} initial capacity requires {initial_units} quota units, "
                f"but quota_limit is {quota_limit}"
            )


class LeaseRecord(EyvanModel):
    """Fenced assignment of one workload to one execution cell."""

    id: str = Field(max_length=96)
    workload_id: str = Field(max_length=96)
    cell_id: str = Field(max_length=96)
    cell_generation: int = Field(default=1, ge=1)
    org_id: str = Field(max_length=128)
    project_id: str = Field(max_length=128)
    state: LeaseState = LeaseState.PENDING
    resources: ResourceGang
    hourly_cost: float = Field(default=0, ge=0)
    reserved_cost: float = Field(default=0, ge=0)
    resource_hours_reserved: CanonicalResourceHours = Field(default_factory=ResourceHours)
    settled_cost: float = Field(default=0, ge=0)
    accelerator_hours_settled: dict[str, float] = Field(default_factory=dict)
    settlement_reason: str | None = None
    settled_at: datetime | None = None
    #: Write-once, stamped the first time this lease enters ACTIVE -- which the
    #: dispatch transaction already commits atomically with the workload's move to
    #: DISPATCHING, so ACTIVE *is* this system's run boundary and no new plumbing is
    #: needed to observe it. `created_at -> run_started_at` is queued time (capacity
    #: reserved, payload not started) and `run_started_at -> settled_at` is running
    #: time. None at settlement means the lease never ran and is pure queue.
    run_started_at: datetime | None = None
    #: Settled clock, recorded so cost is auditable without re-deriving it from
    #: timestamps that later records may not carry.
    queued_seconds: float = Field(default=0, ge=0)
    running_seconds: float = Field(default=0, ge=0)
    generation: int = 1
    fencing_token: str
    group_id: str | None = Field(default=None, max_length=128)
    group_index: int = Field(default=0, ge=0)
    revocation_reason: Literal["preemption"] | None = None
    expires_at: datetime
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    @model_validator(mode="after")
    def validate_settlement(self) -> LeaseRecord:
        _validate_string(self.id, field_name="lease id")
        _validate_string(self.workload_id, field_name="lease workload id")
        _validate_string(self.cell_id, field_name="lease cell id")
        _validate_string(self.org_id, field_name="lease organization id")
        _validate_string(self.project_id, field_name="lease project id")
        _validate_string(self.fencing_token, field_name="lease fencing token")
        _validate_string(self.group_id, field_name="lease group id", optional=True)
        if any(value < 0 for value in self.accelerator_hours_settled.values()):
            raise ValueError("settled accelerator hours must be non-negative")
        if self.settled_at is None and (
            self.settled_cost or self.accelerator_hours_settled or self.settlement_reason is not None
        ):
            raise ValueError("lease settlement values require settled_at")
        if self.settled_at is not None and self.settlement_reason is None:
            raise ValueError("settled leases require a settlement reason")
        return self


class PolicyRecord(EyvanModel):
    """Placement policy for an organization project."""

    org_id: str = Field(max_length=128)
    project_id: str = Field(max_length=128)
    allowed_providers: tuple[str, ...] = ()
    allowed_regions: tuple[str, ...] = ()
    allowed_capacity_classes: tuple[CanonicalCapacityClass, ...] = ()
    denied_accelerator_types: tuple[str, ...] = ()
    max_priority: int = Field(default=1000, ge=0, le=1000)
    preemption_enabled: bool = True
    revision: int = 1
    updated_by: str
    updated_at: datetime = Field(default_factory=utcnow)

    @model_validator(mode="after")
    def validate_policy_values(self) -> PolicyRecord:
        _validate_string(self.org_id, field_name="policy organization id")
        _validate_string(self.project_id, field_name="policy project id")
        _validate_string(self.updated_by, field_name="policy updater id")
        _validate_strings(self.allowed_providers, field_name="allowed providers")
        _validate_strings(self.allowed_regions, field_name="allowed regions")
        _validate_strings(self.denied_accelerator_types, field_name="denied accelerator types")
        return self


class QuotaRecord(EyvanModel):
    """Hierarchical resource quota and DRF weight."""

    org_id: str = Field(max_length=128)
    project_id: str | None = Field(default=None, max_length=128)
    principal_id: str | None = Field(default=None, max_length=128)
    limits: CanonicalResourceLimits
    constrained_resources: tuple[str, ...] = ()
    #: Ceiling on indexed managed-artifact bytes in this scope. Separate from
    #: ``limits`` because that vector is *schedulable* capacity resolved against
    #: cells, while artifact bytes are storage the control plane holds for the
    #: project whether or not anything is running. ``None`` means unlimited,
    #: which is the historical behaviour and stays the default.
    artifact_bytes_limit: int | None = Field(default=None, ge=0)
    weight: float = Field(default=1.0, gt=0)
    revision: int = 1
    updated_by: str
    updated_at: datetime = Field(default_factory=utcnow)

    @model_validator(mode="before")
    @classmethod
    def infer_constrained_resources(cls, value: tp.Any) -> tp.Any:
        if not isinstance(value, tp.Mapping) or "constrained_resources" in value:
            return value
        data = dict(value)
        limits = data.get("limits")
        names: set[str] = set()
        if isinstance(limits, ResourceVector):
            names.update(limits.model_fields_set - {"custom"})
            if "custom" in limits.model_fields_set:
                names.update(limits.custom)
        elif isinstance(limits, tp.Mapping):
            names.update(str(name) for name in limits if name != "custom")
            custom = limits.get("custom")
            if isinstance(custom, tp.Mapping):
                names.update(str(name) for name in custom)
        data["constrained_resources"] = tuple(sorted(names))
        return data

    @model_validator(mode="after")
    def validate_scope(self) -> QuotaRecord:
        _validate_string(self.org_id, field_name="quota organization id")
        _validate_string(self.project_id, field_name="quota project id", optional=True)
        _validate_string(self.principal_id, field_name="quota principal id", optional=True)
        _validate_string(self.updated_by, field_name="quota updater id")
        _validate_strings(self.constrained_resources, field_name="quota constrained resources")
        available = {"cpu", "memory_gib", "gpu", "tpu", *self.limits.custom}
        unknown = sorted(set(self.constrained_resources) - available)
        if unknown:
            raise ValueError(f"quota constrained resource {unknown[0]!r} is missing from limits")
        if self.principal_id is not None and self.project_id is None:
            raise ValueError("principal quota scope requires project_id")
        return self


class OrganizationRecord(EyvanModel):
    """A tenant this control plane knows about.

    Until this existed, ``org_id`` was a free-form string on every request and a
    tenant was whatever anyone typed. Nothing could answer "which organizations
    exist", so nothing could list them, bill them, or refuse a typo -- a
    ``PUT /quotas`` naming ``acmee`` created a quota for a tenant that would
    never submit anything, and it looked exactly like success.

    Registration is deliberately thin. It is a name and an owner, not a
    settings object: quotas, budgets and policies are already their own records
    scoped by ``org_id``, and duplicating any of them here would create two
    places to look and one of them wrong.
    """

    id: str = Field(max_length=128)
    display_name: str = Field(default="", max_length=200)
    #: Subject of the principal accountable for this tenant. Not a grant --
    #: authority still comes from :class:`RoleGrant` -- but the answer to "who
    #: do we talk to", which an audit trail cannot reconstruct after the fact.
    owner: str = Field(min_length=1, max_length=256)
    labels: dict[str, str] = Field(default_factory=dict)
    #: Registered-but-inactive rather than deleted. Deleting an organization
    #: whose workloads, leases and audit events still reference it would orphan
    #: all of them; disabling refuses new work while leaving history readable.
    enabled: bool = True
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    created_by: str = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def validate_organization(self) -> OrganizationRecord:
        _validate_string(self.id, field_name="organization id")
        _validate_string(self.owner, field_name="organization owner")
        _validate_string(self.created_by, field_name="organization creator")
        _validate_string_map(self.labels, field_name="organization labels")
        return self


class ProjectRecord(EyvanModel):
    """A project inside one registered organization.

    Scoped by ``(org_id, id)`` because project names are only unique within a
    tenant: two organizations both having a ``research`` project is the ordinary
    case, and a globally unique project id would make the second one impossible
    to create for reasons no user could see.
    """

    org_id: str = Field(max_length=128)
    id: str = Field(max_length=128)
    display_name: str = Field(default="", max_length=200)
    owner: str = Field(min_length=1, max_length=256)
    labels: dict[str, str] = Field(default_factory=dict)
    enabled: bool = True
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    created_by: str = Field(min_length=1, max_length=256)

    @property
    def key(self) -> tuple[str, str]:
        """Return the scoped identity this record is stored under."""

        return self.org_id, self.id

    @model_validator(mode="after")
    def validate_project(self) -> ProjectRecord:
        _validate_string(self.org_id, field_name="project organization id")
        _validate_string(self.id, field_name="project id")
        _validate_string(self.owner, field_name="project owner")
        _validate_string(self.created_by, field_name="project creator")
        _validate_string_map(self.labels, field_name="project labels")
        return self


class BudgetRecord(EyvanModel):
    """Monetary admission budget for an organization project."""

    org_id: str = Field(max_length=128)
    project_id: str = Field(max_length=128)
    limit: float = Field(ge=0)
    spent: float = Field(default=0, ge=0)
    accelerator_hour_limits: dict[str, float] = Field(default_factory=dict)
    accelerator_hours_spent: dict[str, float] = Field(default_factory=dict)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    period: str = "monthly"
    #: Start of the window ``spent`` accumulates over. Nothing used to reset ``spent``,
    #: which was harmless only while it held a duration *estimate* that ran roughly as
    #: often high as low. Once settlement became actual elapsed cost, ``spent`` became a
    #: true monotone total and ``period`` a lie: every alert threshold would fire once
    #: in the fleet's lifetime and never again. None means "not yet rolled"; the first
    #: settlement stamps it.
    period_started_at: datetime | None = None
    #: Fractions of ``limit`` that emit an audit event the first time spend crosses
    #: them in a period. Warning before the wall is the point: a hard budget that
    #: speaks only by refusing admission tells an operator at the exact moment when
    #: the only remaining option is to raise the limit.
    alert_thresholds: tuple[float, ...] = ()
    #: Thresholds already announced in the current period, cleared when it rolls.
    #: Without this, every settlement past 80% re-announces 80%, and an alert that
    #: repeats on every job is an alert nobody reads.
    alerted_thresholds: tuple[float, ...] = ()
    hard_limit: bool = True
    revision: int = 1
    updated_by: str
    updated_at: datetime = Field(default_factory=utcnow)

    @model_validator(mode="after")
    def validate_spend(self) -> BudgetRecord:
        _validate_string(self.org_id, field_name="budget organization id")
        _validate_string(self.project_id, field_name="budget project id")
        _validate_string(self.currency, field_name="budget currency")
        if self.currency != "USD":
            raise ValueError("Eyvan v0 budgets support only USD")
        _validate_string(self.period, field_name="budget period")
        _validate_string(self.updated_by, field_name="budget updater id")
        if any(value < 0 for value in self.accelerator_hour_limits.values()):
            raise ValueError("accelerator-hour limits must be non-negative")
        if any(value < 0 for value in self.accelerator_hours_spent.values()):
            raise ValueError("accelerator-hour spend must be non-negative")
        _validate_string_map(self.accelerator_hour_limits, field_name="accelerator-hour limits")
        _validate_string_map(self.accelerator_hours_spent, field_name="accelerator-hour spend")
        if any(not 0 < threshold <= 1 for threshold in self.alert_thresholds):
            raise ValueError("budget alert thresholds must be fractions in (0, 1]")
        if len(set(self.alert_thresholds)) != len(self.alert_thresholds):
            raise ValueError("budget alert thresholds must not repeat")
        unknown = sorted(set(self.alerted_thresholds) - set(self.alert_thresholds))
        if unknown:
            raise ValueError(f"budget records an alert for undeclared threshold {unknown[0]}")
        return self

    @property
    def utilization(self) -> float:
        """Return spend as a fraction of the limit, or zero for a zero limit.

        Zero rather than infinity: a zero-limit budget refuses everything, so its
        utilization is a number nobody acts on, and dividing by zero here would
        break every report that lists budgets alongside one another.
        """

        return 0.0 if self.limit <= 0 else self.spent / self.limit

    def thresholds_crossed_by(self, spent: float) -> tuple[float, ...]:
        """Return declared thresholds ``spent`` reaches that have not yet fired."""

        if self.limit <= 0:
            return ()
        fraction = spent / self.limit
        already = set(self.alerted_thresholds)
        return tuple(
            sorted(
                threshold for threshold in self.alert_thresholds if threshold <= fraction and threshold not in already
            )
        )


class PoolEstimate(EyvanModel):
    """What one pool would charge for one workload, and where the price came from."""

    pool_id: str
    accelerator_type: str | None = None
    capacity_class: str
    region: str
    zone: str | None = None
    #: Cells this workload needs in this pool, after ``worker_count`` scaling.
    cells: int = Field(ge=1)
    hourly_cost: float = Field(ge=0)
    estimated_cost: float = Field(ge=0)
    #: Where the price was resolved from, or ``None`` when the pool declares none.
    #: Reported rather than defaulted to zero: an unpriced pool estimating "free"
    #: is defect D7 wearing a friendly face.
    price_source: str | None = None


class WorkloadEstimate(EyvanModel):
    """Pre-submission cost estimate for one workload definition.

    Answers "what will this cost, and will my budget take it" *before* a record
    exists -- the question every operator asks and that previously required
    submitting and reading the resulting lease's ``reserved_cost``.

    Every figure is an estimate over ``estimated_duration_seconds``, which is a
    user-supplied number. The estimate is only as honest as that input, and the
    field is named to say so rather than presenting a projection as a quote.
    """

    org_id: str
    project_id: str
    estimated_duration_seconds: int = Field(ge=1)
    #: Candidate pools, cheapest first. Empty means nothing in the fleet matches
    #: the request -- a different answer from "it costs zero".
    candidates: tuple[PoolEstimate, ...] = ()
    #: Current spend and limit at estimate time, when a budget exists.
    budget_limit: float | None = None
    budget_spent: float | None = None
    budget_hard_limit: bool = True
    #: Pools whose estimate would not fit the remaining hard budget. Named rather
    #: than filtered out: "your budget refuses the cheap option too" is the useful
    #: message, and silently dropping candidates would look like no capacity.
    budget_blocked_pool_ids: tuple[str, ...] = ()
    #: Pools matched but declaring no price. Their estimate is zero and meaningless.
    unpriced_pool_ids: tuple[str, ...] = ()

    @property
    def cheapest(self) -> PoolEstimate | None:
        """Return the cheapest candidate, or ``None`` when nothing matched."""

        return self.candidates[0] if self.candidates else None


class QueueTrendPoint(EyvanModel):
    """One time bucket of the submitted-vs-admitted trend."""

    started_at: datetime
    submitted: int = Field(ge=0)
    admitted: int = Field(ge=0)


class QueueTrend(EyvanModel):
    """Queue pressure over recent time, derived from the audit ledger.

    Bucketed by wall clock, not by event count: the question is "is the queue
    growing", and count-based buckets compress exactly the quiet periods that
    answer it.
    """

    observed_at: datetime
    bucket_seconds: float = Field(gt=0)
    points: tuple[QueueTrendPoint, ...] = ()


class FederatedBudgetStatus(EyvanModel):
    """One tenant's spend across every plane, as this plane understands it.

    The peer figures are reported, not observed: each is as fresh as that peer's
    last report. That is why this number informs and alerts and never admits --
    admission stays local, against a grant already bounded when it was issued, so
    a stale report can make this report wrong without making spend unbounded.
    """

    org_id: str
    project_id: str
    home_plane_id: str | None = None
    limit: float = Field(ge=0)
    #: Spend on this plane, from its own settled leases.
    local_spent: float = Field(ge=0)
    #: Currently authorized on peers, whether or not they have used it.
    granted_away: float = Field(ge=0)
    reported_peer_spend: float = Field(ge=0)
    total_spent: float = Field(ge=0)
    active_grants: int = Field(ge=0)
    #: Active grants whose holder has never reported. Named because it is the
    #: difference between "the peers are idle" and "the peers are not talking",
    #: and a total that silently mixes the two is worse than no total.
    stale_reports: int = Field(ge=0)


class BudgetStatus(EyvanModel):
    """A budget's spend, its period window, and where it is heading.

    Two of these fields revive dashboard surfaces that could never populate:
    ``forecast`` was typed and never set, so "Current forecast" always read
    Unavailable and the alert block behind it could not fire; ``period_ends_at``
    is what makes ``spent`` interpretable at all, since a number with no window
    attached cannot be compared to a limit that has one.
    """

    org_id: str
    project_id: str
    limit: float = Field(ge=0)
    spent: float = Field(ge=0)
    currency: str = "USD"
    period: str = "monthly"
    period_started_at: datetime | None = None
    period_ends_at: datetime | None = None
    hard_limit: bool = True
    #: Projected end-of-period spend: current spend plus what every active lease
    #: will cost for the rest of the window. Derived, never stored -- a stored
    #: forecast is a number that goes stale the moment a lease starts or ends.
    forecast: float | None = None
    alert_thresholds: tuple[float, ...] = ()
    alerted_thresholds: tuple[float, ...] = ()

    @property
    def utilization(self) -> float:
        """Return spend as a fraction of the limit, or zero for a zero limit."""

        return 0.0 if self.limit <= 0 else self.spent / self.limit

    @property
    def forecast_exceeds_limit(self) -> bool:
        """Return whether this budget is projected to run out before its period does."""

        return self.forecast is not None and self.limit > 0 and self.forecast > self.limit


class AuditEvent(EyvanModel):
    """Immutable, ordered audit event emitted by a committed transaction."""

    id: str = Field(max_length=96)
    sequence: int = 0
    timestamp: datetime = Field(default_factory=utcnow)
    actor: str
    org_id: str | None = Field(default=None, max_length=128)
    project_id: str | None = Field(default=None, max_length=128)
    entity_type: str = Field(max_length=64)
    entity_id: str = Field(max_length=256)
    action: str
    previous_state: str | None = None
    new_state: str | None = None
    payload: JsonDocument = Field(default_factory=dict)


#: Cell states in which the provider resource exists and is being charged for.
#: REGISTERING is deliberately excluded: a queued TPU request is a record Eyvan
#: holds and a bill nobody is paying, and charging it would make queue time --
#: the one genuinely free interval -- look like spend. DELETING is included
#: because the resource still exists until the delete completes.
BILLABLE_CELL_STATES = frozenset({CellState.READY, CellState.DRAINING, CellState.UNAVAILABLE, CellState.DELETING})


class PoolWarmth(EyvanModel):
    """Whether one pool's warm floor is currently held, and what it costs.

    A warm window is a promise an operator wrote and nothing reported on. Two
    things go wrong invisibly: the floor is not met, so the 09:00 latency the
    window exists to prevent happens anyway; or the floor is met and the cells sit
    unused, which is *correct* and looks identical to waste in every other view.

    That second case is why this joins the billing clock. Warm capacity is
    deliberately idle. Without a way to name it, an operator reading idle cost
    cannot tell the capacity they asked to keep hot from the capacity their
    packing is wasting, and the only safe reaction to an unexplained idle number
    is to delete the warm floor -- undoing the fix.
    """

    pool_id: str
    #: The floor in force at the reported moment: ``min_cells`` raised by any
    #: window covering it. Zero means scale-to-zero is permitted right now.
    floor: int = Field(ge=0)
    #: Whether a *window* is what raised the floor, as opposed to the pool's
    #: standing ``min_cells``. An operator debugging "why is this pool not empty
    #: at 03:00" needs these distinguished.
    window_active: bool = False
    #: Cells that currently count toward the floor.
    warm_cells: int = Field(ge=0)
    #: Warm cells holding no lease. Not waste: this is the capacity the floor
    #: exists to reserve, and it is the number that must be subtracted from a
    #: fleet's idle cost before that cost means anything.
    warm_idle_cells: int = Field(ge=0)
    hourly_cost: float = Field(default=0.0, ge=0)

    @property
    def satisfied(self) -> bool:
        """Return whether the pool currently holds its floor."""

        return self.warm_cells >= self.floor


class WarmthReport(EyvanModel):
    """Warm-floor status across every pool, at one moment."""

    observed_at: datetime = Field(default_factory=utcnow)
    pools: tuple[PoolWarmth, ...] = ()

    @property
    def unsatisfied(self) -> tuple[PoolWarmth, ...]:
        """Return pools whose floor is declared and not currently held."""

        return tuple(pool for pool in self.pools if not pool.satisfied)

    @property
    def warm_hourly_cost(self) -> float:
        """Return what holding every floor costs per hour."""

        return costing.total(*(pool.hourly_cost for pool in self.pools))


class CellBillingAccrual(EyvanModel):
    """What one accrual pass added to the fleet's cell clocks."""

    cells: int = Field(ge=0)
    accrued_seconds: float = Field(ge=0)
    idle_seconds: float = Field(ge=0)
    accrued_cost: float = Field(ge=0)
    idle_cost: float = Field(ge=0)


class CellBillingReport(EyvanModel):
    """Accrued and idle cell cost over a fleet or one pool."""

    pool_id: str | None = None
    cells: int = Field(ge=0)
    billed_seconds: float = Field(ge=0)
    idle_seconds: float = Field(ge=0)
    accrued_cost: float = Field(ge=0)
    idle_cost: float = Field(ge=0)
    #: Cells contributing zero cost because their pool declares no unit prices.
    #: Reported rather than hidden: without it, defect D7's fleet reads as a
    #: fleet that genuinely costs nothing.
    unpriced_cells: int = Field(ge=0)


class ArtifactCollection(EyvanModel):
    """What one collection pass reclaimed, or would reclaim in a dry run."""

    #: URIs whose bytes were removed, or would be. Named rather than counted: an
    #: operator approving a first real run needs to see *which* objects, and a
    #: count cannot be checked against expectations.
    collected_uris: tuple[str, ...] = ()
    reclaimed_bytes: int = Field(default=0, ge=0)
    #: Indexed artifacts examined and kept, by reason. The kept count is what makes
    #: a surprisingly small collection diagnosable without re-deriving the index.
    held: int = Field(default=0, ge=0)
    referenced: int = Field(default=0, ge=0)
    within_grace: int = Field(default=0, ge=0)
    #: True when nothing was deleted and the pass only reported.
    dry_run: bool = True


class ArtifactUsage(EyvanModel):
    """Indexed artifact bytes in one scope, and the ceiling they are measured against."""

    org_id: str
    project_id: str | None = None
    artifact_count: int = Field(ge=0)
    stored_bytes: int = Field(ge=0)
    #: ``None`` when no quota applies, which is not the same as a quota of zero.
    quota_bytes: int | None = Field(default=None, ge=0)


class ArtifactHold(EyvanModel):
    """An explicit claim that keeps one artifact out of any future collection.

    References answer "is something still going to run this". A hold answers a
    question references cannot: "a human still needs this" -- a callable under
    investigation, a result cited outside Eyvan, a workspace pinned for a repro.
    Without it the only way to protect such an object is to keep a workload alive
    around it, which spends capacity to express a bookkeeping fact.

    One hold per artifact, deliberately. A hold *set* would need its own identity,
    ownership and expiry semantics, and the failure it prevents -- two people each
    believing the other's hold protects the object -- is not one this package has
    seen. The single hold names its owner and reason, so a second person finds a
    stated claim to argue with rather than an anonymous flag.
    """

    reason: str = Field(min_length=1, max_length=512)
    held_by: str = Field(min_length=1, max_length=128)
    created_at: datetime = Field(default_factory=utcnow)
    #: ``None`` is an indefinite hold. Permitted because the honest lifetime of
    #: "cited in a paper" is indefinite, and an expiry invented to avoid saying so
    #: would delete the object on a date nobody chose.
    expires_at: datetime | None = None

    def active_at(self, moment: datetime) -> bool:
        """Return whether this hold is in force at ``moment``."""

        return self.expires_at is None or moment < self.expires_at


class ManagedArtifactRecord(EyvanModel):
    """One stored object in the managed artifact store, and what it costs.

    The control plane has always been able to *serve* artifacts and has never
    known how many there are or how many bytes they occupy. This record is that
    ledger, and it deliberately holds no bytes: ``uri`` addresses the object in
    the store, which stays the only copy.

    Content-addressed kinds are shared by construction -- two projects uploading
    the same callable produce the same digest and therefore the same URI -- so a
    re-upload *updates* this row rather than adding one. ``size_bytes`` is
    recorded at store time because it cannot be recovered later without reading
    the object back, and byte accounting that requires reading every object is
    accounting nobody runs.
    """

    uri: str = Field(max_length=512)
    kind: str = Field(max_length=32)
    org_id: str = Field(max_length=128)
    project_id: str = Field(max_length=128)
    size_bytes: int = Field(ge=0)
    #: Digest of the stored bytes for content-addressed kinds, else ``None``.
    #: Recorded so a future collector can prove it is deleting the object it
    #: indexed rather than one that replaced it at the same URI.
    sha256: str | None = Field(default=None, max_length=64)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    #: Last capability-authenticated read. Recency, not liveness -- an artifact
    #: no attempt has read in a month may still be referenced by a queued
    #: workload, so this informs a human, never a deletion.
    last_accessed_at: datetime | None = None
    #: An explicit human claim on this object, independent of any workload.
    hold: ArtifactHold | None = None
    #: Set when a later attempt of the same workload minted a newer result URI at
    #: a different address, making this one the output of a run that was retried.
    #: Superseded is a *fact*, not a verdict: an operator debugging why attempt 2
    #: differs from attempt 1 needs attempt 1's output, which is exactly why this
    #: is recorded rather than acted on.
    superseded_at: datetime | None = None
    superseded_by_uri: str | None = Field(default=None, max_length=512)

    def collectable_at(self, moment: datetime, *, has_live_reference: bool) -> bool:
        """Return whether nothing currently protects this artifact.

        Three independent protections, all of which must be absent. A live
        reference means something may still run against it. A hold means a human
        said not to. Both are checked here rather than at each future call site,
        so a collector cannot be written that consults one and forgets the other.
        """

        if has_live_reference:
            return False
        return self.hold is None or not self.hold.active_at(moment)

    @model_validator(mode="after")
    def validate_artifact(self) -> ManagedArtifactRecord:
        _validate_string(self.uri, field_name="managed artifact uri")
        _validate_string(self.kind, field_name="managed artifact kind")
        _validate_string(self.org_id, field_name="managed artifact organization id")
        _validate_string(self.project_id, field_name="managed artifact project id")
        _validate_string(self.sha256, field_name="managed artifact digest", optional=True)
        return self


class ArtifactReferenceRecord(EyvanModel):
    """One workload's claim on one artifact: the reverse index.

    Nothing could previously answer "which workloads use this callable?", so
    "unreferenced" was not a computable property and no collector could be
    written safely. A reference is created when a workload that names an
    artifact is stored, and released when that workload reaches a terminal
    state -- but the row is *kept*, because the history of who referenced what is
    what makes a later deletion auditable.
    """

    #: Deterministic: one workload referencing one URI is one row, so a replayed
    #: submission cannot inflate the reference set.
    id: str = Field(max_length=200)
    uri: str = Field(max_length=512)
    workload_id: str = Field(max_length=96)
    org_id: str = Field(max_length=128)
    project_id: str = Field(max_length=128)
    created_at: datetime = Field(default_factory=utcnow)
    #: Set when the referencing workload became terminal. A URI with no
    #: unreleased reference is a *candidate* for collection, never a target:
    #: collection is out of scope here and this package deletes nothing.
    released_at: datetime | None = None

    @model_validator(mode="after")
    def validate_reference(self) -> ArtifactReferenceRecord:
        _validate_string(self.id, field_name="artifact reference id")
        _validate_string(self.uri, field_name="artifact reference uri")
        _validate_string(self.workload_id, field_name="artifact reference workload id")
        _validate_string(self.org_id, field_name="artifact reference organization id")
        _validate_string(self.project_id, field_name="artifact reference project id")
        return self


def artifact_reference_id(uri: str, workload_id: str) -> str:
    """Return the deterministic reference id for one (uri, workload) pair.

    Derived rather than generated so that a replayed submission produces the
    same row: the reference set must count *workloads*, not submissions.
    """

    digest = hashlib.sha256(f"{uri}\n{workload_id}".encode()).hexdigest()
    return f"ref-{digest[:40]}"


class IdempotencyRecord(EyvanModel):
    """Durable response pointer for one scoped mutating request."""

    scope: str = Field(max_length=256)
    operation: str = Field(max_length=96)
    key: str = Field(max_length=200)
    fingerprint: str
    resource_type: str
    resource_id: str
    created_at: datetime = Field(default_factory=utcnow)


class OutboxRecord(EyvanModel):
    """A fenced, at-least-once backend command committed with domain state."""

    id: str = Field(max_length=96)
    topic: str
    aggregate_type: str
    aggregate_id: str
    idempotency_key: str
    generation: int = Field(ge=0)
    payload: JsonDocument = Field(default_factory=dict)
    state: OutboxState = OutboxState.PENDING
    attempts: int = Field(default=0, ge=0)
    available_at: datetime = Field(default_factory=utcnow)
    claimed_by: str | None = None
    claimed_until: datetime | None = None
    last_error: str | None = None
    failure_code: CapacityProviderFailureCode | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class WorkloadProgressPhase(StrEnum):
    """Operator-facing phase for one workload while capacity is acquired and used."""

    WAITING_FOR_AUTOSCALER = "waiting_for_autoscaler"
    REQUESTING_CAPACITY = "requesting_capacity"
    WAITING_FOR_CAPACITY = "waiting_for_capacity"
    PROVISIONING = "provisioning"
    CAPACITY_ALLOCATED = "capacity_allocated"
    NATIVE_WORKERS_CONNECTING = "native_workers_connecting"
    WAITING_FOR_SCHEDULER = "waiting_for_scheduler"
    WAITING_FOR_DISPATCH = "waiting_for_dispatch"
    READY_FOR_DISPATCH = "ready_for_dispatch"
    PROVIDER_FAILURE = "provider_failure"
    QUEUE_ADMISSION_DENIED = "queue_admission_denied"
    QUOTA_BLOCKED = "quota_blocked"
    PREEMPTED = "preempted"
    DISPATCHING = "dispatching"
    RUNNING = "running"
    CHECKPOINTING = "checkpointing"
    CANCELLING = "cancelling"
    TERMINAL = "terminal"


class WorkloadCapacityRequest(EyvanModel):
    """Exact capacity request after gang-replica and physical-cell expansion."""

    cells: int = Field(ge=1)
    workers_per_cell: int = Field(ge=1)
    resources_per_cell: ResourceVector
    total_resources: ResourceVector


class WorkloadPoolProgress(EyvanModel):
    """Stable identity of the exact capacity pool pinned by a workload."""

    id: str
    fleet_name: str
    node_type: str
    provider: str
    provider_project: str | None = None
    region: str
    zone: str | None
    accelerator_type: str | None
    capacity_class: CanonicalCapacityClass


class WorkloadCapacityPhaseProgress(EyvanModel):
    """Current cells grouped by their durable Eyvan and provider phases."""

    state: CellState
    provider_phase: CapacityStatus | None
    cells: int = Field(ge=0)
    workers: int = Field(ge=0)


class WorkloadCapacitySummary(EyvanModel):
    """Exact-pool capacity counts for a workload's requested physical shape."""

    active_cells: int = Field(ge=0)
    active_workers: int = Field(ge=0)
    ready_cells: int = Field(ge=0)
    ready_workers: int = Field(ge=0)
    requesting_cells: int = Field(default=0, ge=0)
    requesting_workers: int = Field(default=0, ge=0)
    waiting_cells: int = Field(ge=0)
    waiting_workers: int = Field(ge=0)
    provisioning_cells: int = Field(ge=0)
    provisioning_workers: int = Field(ge=0)
    lost_cells: int = Field(ge=0)
    lost_workers: int = Field(ge=0)
    observed_tpu_vms: int | None = Field(default=None, ge=1)
    phases: tuple[WorkloadCapacityPhaseProgress, ...] = ()


class CapacityRetryProgress(EyvanModel):
    """Durable state of the newest relevant capacity-apply command."""

    state: OutboxState
    attempts: int = Field(ge=0)
    next_attempt_at: datetime | None
    diagnostic: str | None = Field(default=None, max_length=2000)
    failure_code: CapacityProviderFailureCode | None = None


class UserProgressReport(EyvanModel):
    """One rank's latest self-reported progress.

    **Latest per rank, no history.** Upserted on ``(workload_id, attempt, rank)``,
    so it is idempotent by overwrite, needs no idempotency key, and cannot grow
    without bound. Unbounded user-metric history in the control-plane database is
    the thing to refuse now rather than after it has filled a disk.
    """

    rank: int = Field(ge=0)
    attempt: int = Field(ge=1)
    step: int | None = Field(default=None, ge=0)
    total: int | None = Field(default=None, ge=0)
    message: str = Field(default="", max_length=512)
    #: Free-form user metrics. Reported and displayed; deliberately **not**
    #: exported as Prometheus series, because unbounded label cardinality coming
    #: from user code is a self-inflicted outage.
    metrics: dict[str, float] = Field(default_factory=dict)
    reported_at: datetime = Field(default_factory=utcnow)

    @model_validator(mode="after")
    def validate_report(self) -> UserProgressReport:
        if self.total is not None and self.step is not None and self.step > self.total:
            raise ValueError("progress step must not exceed total")
        _validate_string_map({name: str(value) for name, value in self.metrics.items()}, field_name="metrics")
        return self

    @property
    def fraction(self) -> float | None:
        """Return completion as a fraction, or ``None`` when it is not known.

        ``None`` rather than zero: a job that reported a step but no total knows
        how far it has come and not how far it has to go, and rendering that as
        0% is worse than rendering nothing.
        """

        if self.step is None or not self.total:
            return None
        return min(1.0, self.step / self.total)


class WorkloadProgress(EyvanModel):
    """Read-only autoscaling progress for one submitted workload."""

    workload_id: str
    workload_state: WorkloadState
    phase: WorkloadProgressPhase
    scheduling_reason: str | None
    failure_reason: str | None
    autoscale_block_reason: AutoscaleBlockReason | None = None
    queue_admission_denial: GcpQueueAdmissionDenialTarget | None = None
    request: WorkloadCapacityRequest
    pool: WorkloadPoolProgress | None
    capacity: WorkloadCapacitySummary
    retry: CapacityRetryProgress | None
    #: What the workload itself reported, newest per rank. Exposed on the route
    #: the dashboard and ``client/callables.py`` already poll, so a training loop
    #: reporting its step needs no new polling loop anywhere.
    user_progress: tuple[UserProgressReport, ...] = ()
