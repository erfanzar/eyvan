# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Backend-neutral contracts for capacity and execution cells."""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass, field
from datetime import datetime

from typing_extensions import TypedDict

from eyvan._internal.compat import StrEnum
from eyvan._internal.json_types import JsonObject, JsonValue
from eyvan.capacity import CapacityStatus


def canonical_execution_backend(name: str) -> str:
    """Return the stable public name for a built-in execution backend."""

    normalized = name.strip().lower().replace("_", "-")
    aliases = {
        "local": "native-local",
        "local-process": "native-local",
        "native": "eyvan-native",
        "native-remote": "eyvan-native",
        "native-worker": "eyvan-native",
        "eyvan": "eyvan-native",
        "ray": "ray-jobs",
        "rayjobs": "ray-jobs",
    }
    return aliases.get(normalized, normalized)


def capacity_backend_agent_family(name: str) -> str:
    """Return the group of capacity backends one cell agent can serve at once.

    ``CellAgentService`` reads ``settings.capacity_backend`` once at construction and
    builds a single driver, so an agent serves exactly one *driver* -- but several backend
    names can share one. ``gcp``, ``gcp-tpu`` and ``gcp-compute`` are all modes of the same
    factory (``drivers/registry.py``), which is why a mixed TPU + Compute fleet behind one
    agent endpoint is legitimate and must keep validating.

    Expressed as data here rather than read from the registry on purpose: the registry's
    stated invariant is that selecting one backend never imports another's dependencies, so
    a config-time validator must not import it just to ask this question.
    """

    normalized = name.strip().lower().replace("_", "-")
    families = {
        "gcp": "gcp",
        "gcp-tpu": "gcp",
        "tpu": "gcp",
        "gcp-compute": "gcp",
        "compute": "gcp",
        "kubernetes": "kubernetes",
        "k8s": "kubernetes",
    }
    return families.get(normalized, normalized)


class StaleMutationError(RuntimeError):
    """Reject a mutation that does not own the persisted generation fence."""


class DriverBusyError(RuntimeError):
    """A shared driver mutation lease is currently owned elsewhere."""


class CapacityProviderFailureCode(StrEnum):
    """Stable machine-readable reason for a capacity-provider failure."""

    PROVIDER_ERROR = "provider_error"
    QUEUE_ADMISSION_DENIED = "queue_admission_denied"
    #: The provider's control API could not be reached or was overloaded. Distinct
    #: from PROVIDER_ERROR because it is the ordinary state of a cluster during an
    #: upgrade, and discarding the command would drop capacity the control plane
    #: believes it asked for.
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    #: The request will fail identically however many times it is retried --
    #: missing credentials, a namespace that does not exist, a rejected manifest.
    CONFIGURATION = "configuration"


class CapacityProviderError(RuntimeError):
    """An operator-safe diagnostic returned by an external capacity provider."""

    def __init__(
        self,
        diagnostic: str,
        *,
        code: CapacityProviderFailureCode = CapacityProviderFailureCode.PROVIDER_ERROR,
        retryable: bool | None = None,
    ) -> None:
        normalized = diagnostic.strip()
        self.diagnostic = normalized or "capacity provider operation failed"
        self.code = code
        # Derived from the code unless stated, so existing raisers keep their
        # behaviour and a new one cannot forget to classify itself.
        self.retryable = code is CapacityProviderFailureCode.PROVIDER_UNAVAILABLE if retryable is None else retryable
        super().__init__(self.diagnostic)


class UnsupportedCapabilityError(RuntimeError):
    """A selected backend cannot satisfy a requested capability."""


class ExecutionWorkloadStatus(StrEnum):
    """Backend-neutral state reported for one remote execution.

    ``INDETERMINATE`` means a backend explicitly attempted an observation but
    could not establish a definitive state. It is never a missing-value or
    default fallback.
    """

    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    CHECKPOINTING = "CHECKPOINTING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    LOST = "LOST"
    NOT_FOUND = "NOT_FOUND"
    INDETERMINATE = "INDETERMINATE"


class ExecutionWorkloadResult(TypedDict):
    """Stable JSON-compatible workload status payload returned by an executor."""

    remote_id: str
    status: ExecutionWorkloadStatus
    message: str


@dataclass(frozen=True, slots=True)
class ExecutionWorkloadObservation(tp.Mapping[str, JsonValue]):
    """Typed executor observation with the existing mapping wire shape."""

    remote_id: str
    status: ExecutionWorkloadStatus
    message: str = ""

    def as_result(self) -> ExecutionWorkloadResult:
        """Return the stable payload consumed by the agent wire model."""

        return {"remote_id": self.remote_id, "status": self.status, "message": self.message}

    def __getitem__(self, key: str) -> JsonValue:
        """Read a status field through the legacy mapping interface."""

        if key == "remote_id":
            return self.remote_id
        if key == "status":
            return self.status
        if key == "message":
            return self.message
        raise KeyError(key)

    def __iter__(self) -> tp.Iterator[str]:
        """Iterate the stable workload-status field names."""

        return iter(("remote_id", "status", "message"))

    def __len__(self) -> int:
        """Return the fixed number of workload-status fields."""

        return 3


ExecutionWorkloadReport: tp.TypeAlias = ExecutionWorkloadObservation | ExecutionWorkloadResult
"""Typed status result returned by every execution driver.

Most drivers return :class:`ExecutionWorkloadObservation`. Coordinating
drivers may return a typed dictionary that extends ``ExecutionWorkloadResult``
with per-worker details.
"""


@dataclass(frozen=True, slots=True)
class MutationContext:
    """Fencing metadata required for every mutating driver operation."""

    idempotency_key: str
    generation: int
    fencing_token: str


@dataclass(frozen=True, slots=True)
class CellCapabilities:
    """Features and resource shapes supported by a cell driver."""

    provider: str
    executor: str
    accelerator_types: tuple[str, ...]
    capacity_classes: tuple[str, ...]
    #: Capacity backend names this agent's single driver can serve. **Plural**,
    #: because the GCP driver legitimately serves ``gcp``, ``gcp-tpu`` and
    #: ``gcp-compute`` from one process, and Kubernetes serves ``kubernetes`` and
    #: ``k8s``. Empty means "does not declare", so an older agent that predates
    #: this field is skipped rather than treated as serving nothing.
    capacity_backends: tuple[str, ...] = ()
    supports_checkpoint: bool = True
    supports_callables: bool = False
    #: Whether an operator has *permitted* diagnostic exec on this cell.
    #:
    #: Defaulted to **False**, changed from True. A default-on capability that no
    #: production code read is precisely how it became dead: nothing could have
    #: noticed it was wrong. Every declaration site now says so explicitly, and
    #: this is the operator-facing admission gate -- whether a backend *can*
    #: exec is a separate question, answered by ``ExecCapableExecutionDriver``.
    supports_exec: bool = False
    max_gang_replicas: int | None = None
    resource_enforcement: str = "backend"


@dataclass(frozen=True, slots=True)
class CellObservation:
    """Observed state returned during reconciliation."""

    cell_id: str
    state: CapacityStatus
    generation: int
    execution_endpoint: str | None = None
    detail: str = ""
    observed_at: datetime | None = None
    metadata: JsonObject = field(default_factory=dict)
    observed_tpu_vms: int | None = None


class CapacityDriver(tp.Protocol):
    """Provider lifecycle used by the global capacity reconciler."""

    def capabilities(self) -> CellCapabilities: ...

    def observe(self, cell_id: str) -> CellObservation: ...

    def reconcile(self, cell_id: str, context: MutationContext) -> CellObservation: ...

    def apply_capacity(
        self,
        cell_id: str,
        desired: tp.Mapping[str, JsonValue],
        context: MutationContext,
    ) -> CellObservation: ...

    def drain(self, cell_id: str, context: MutationContext) -> CellObservation: ...

    def release(self, cell_id: str, context: MutationContext) -> CellObservation: ...


class ExecutionDriver(tp.Protocol):
    """Whole-workload operations delegated to an execution cell."""

    def submit(self, spec: tp.Mapping[str, JsonValue], context: MutationContext) -> str: ...

    def status(self, remote_id: str) -> ExecutionWorkloadReport: ...

    def stop(self, remote_id: str, context: MutationContext) -> None: ...

    def retire(self, remote_id: str, context: MutationContext) -> None: ...

    def checkpoint(
        self,
        remote_id: str,
        *,
        hook: str,
        output_uri: str | None,
        context: MutationContext,
    ) -> str: ...

    def logs(self, remote_id: str) -> str: ...


class ExecutionDriverResolver(tp.Protocol):
    """Resolve the configured execution backend for a particular cell."""

    def resolve(self, cell_id: str, observation: CellObservation) -> ExecutionDriver: ...
