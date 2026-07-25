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
from eyvan.domain.availability import ProvisionOutcome


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
        # Retired: kept as aliases so a persisted workload naming Ray still
        # normalises to one refused backend instead of an unknown-typo error.
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
    #: The provider has no capacity of this shape right now. Distinct from
    #: PROVIDER_ERROR because it says something about the *pool* rather than
    #: about the request, and the placement engine routes on that difference.
    STOCKOUT = "stockout"
    #: The project's quota for this shape is exhausted. Retrying in the same
    #: pool cannot succeed until a human raises the limit, so it must not look
    #: like a stockout that another zone might satisfy.
    QUOTA_EXCEEDED = "quota_exceeded"
    #: The provider reclaimed capacity Eyvan already held. Expected on spot, and
    #: the cheapest failure in this enum to retry -- which is precisely why it
    #: must not be charged to the same budget as a stockout or a bad request.
    PREEMPTED = "preempted"


#: How one provider reason updates a pool's availability belief.
#:
#: Explicit and total rather than derived from the failure text. The two places
#: that needed this both reconstructed it by searching an arbitrary provider
#: string for the words "quota" and "capacity" -- which meant a message like
#: "capacity pool not found" (a configuration mistake) was recorded as evidence
#: that a zone had run out of accelerators, and every reason nobody had written
#: a substring for became a stockout by default.
#:
#: ``ProvisionOutcome.HOST_FAULT`` is deliberately unreachable from this map and
#: has no ``CapacityProviderFailureCode``. A host fault is observed from an
#: *execution* -- an attempt's own output naming an accelerator that never came
#: up -- and never from a provider API, which reports a machine it created
#: successfully. Adding a provider code for it would put a value on the agent
#: wire and in ``CellRecord.provider_failure_code`` that no provider raises, that
#: a rolled-back replica could not decode, and that would make
#: :func:`provision_outcome` claim a provider said something it did not.
_PROVISION_OUTCOMES: dict[CapacityProviderFailureCode, ProvisionOutcome] = {
    CapacityProviderFailureCode.STOCKOUT: ProvisionOutcome.STOCKOUT,
    CapacityProviderFailureCode.QUOTA_EXCEEDED: ProvisionOutcome.QUOTA_ERROR,
    # Not a stockout. The provider had the capacity and took it back, which says
    # nothing about whether the pool can satisfy the next request -- and saying
    # otherwise steers placement away from a zone that is working.
    CapacityProviderFailureCode.PREEMPTED: ProvisionOutcome.INTERRUPTED,
    CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED: ProvisionOutcome.PROVIDER_ERROR,
    CapacityProviderFailureCode.PROVIDER_UNAVAILABLE: ProvisionOutcome.PROVIDER_ERROR,
    CapacityProviderFailureCode.CONFIGURATION: ProvisionOutcome.PROVIDER_ERROR,
    CapacityProviderFailureCode.PROVIDER_ERROR: ProvisionOutcome.PROVIDER_ERROR,
}


def provision_outcome(
    failure_code: CapacityProviderFailureCode | None,
    status: CapacityStatus | None = None,
) -> ProvisionOutcome:
    """Return how one provider failure should move a pool's availability belief.

    ``status`` is the fallback for an observation that reported no reason --
    an older agent, or a provider that simply did not say. ``HALTED_QUOTA`` is
    self-describing; anything else unnamed is a provider error, **not** a
    stockout. Defaulting the unknown to "this pool is empty" is how a belief
    that drives placement gets poisoned by causes that have nothing to do with
    capacity.
    """

    if failure_code is not None:
        return _PROVISION_OUTCOMES[failure_code]
    if status is CapacityStatus.HALTED_QUOTA:
        return ProvisionOutcome.QUOTA_ERROR
    return ProvisionOutcome.PROVIDER_ERROR


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
    #: Stable number of runtime/JAX processes exposed by this cell. This is not
    #: ``worker_count``: the latter counts allocated slices, while one TPU slice
    #: can expose several native-worker endpoints.
    runtime_process_count: int | None = None
    #: Why the provider is in a non-ready state, when it said something specific.
    #: A code that arrives only on a *raised* provider error is a code that never
    #: describes a successful observation -- and a spot preemption is a
    #: successful observation of a reclaimed machine, not an error.
    failure_code: CapacityProviderFailureCode | None = None


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


class CapacityProviderPlugin(tp.Protocol):
    """The object an out-of-tree capacity provider publishes at its entry point.

    This is the whole ABI surface an external distribution has to satisfy. It is
    deliberately three members and no base class: a provider must be able to
    declare which ABI it was built against **without importing Eyvan**, because
    the version check exists precisely for the case where importing this Eyvan
    would fail or would mean something different than the provider expects. A
    module object satisfies this protocol, so the smallest legal plugin is a
    module with two module-level constants and one function.

    Attributes:
        abi_version: The published ABI this provider was written against, as
            ``"MAJOR.MINOR"``. A plain string rather than a shared type for the
            reason above. Mismatches are refused by
            :mod:`eyvan.capacity.drivers.registry`, never adapted.
        backend_names: The ``capacity_backend`` names this plugin serves, in the
            same normalized form the registry uses. Plural because one factory
            legitimately serves several modes, exactly as ``gcp``, ``gcp-tpu``
            and ``gcp-compute`` share one in-tree factory.
    """

    abi_version: str
    backend_names: tp.Sequence[str]

    def create(self, name: str, *, state: tp.Any, settings: tp.Any) -> CapacityDriver:
        """Build the driver for one of ``backend_names``.

        Args:
            name: The normalized backend name being created, so one plugin can
                serve several modes.
            state: The durable, fenced :class:`~eyvan.capacity.fleet_state.FleetStateStore`
                this agent owns. Typed as ``Any`` here only to keep this module
                import-free of the store it describes; the passed object is
                always a ``FleetStateStore``.
            settings: The agent's :class:`~eyvan.config.EyvanSettings`.

        Returns:
            A driver satisfying :class:`CapacityDriver`.
        """
        ...
