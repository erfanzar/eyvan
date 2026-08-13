# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Durable callable-job value types shared by execution and SDK adapters."""

from __future__ import annotations

import re
import typing as tp
from dataclasses import dataclass, field

from eyvan.artifacts import StoredCallableArtifact
from eyvan.domain.accelerators import sort_accelerator_types
from eyvan.records import (
    CALLABLE_SIGNING_KEY_ENV,
    DEFAULT_MAX_RETRIES,
    REMOTE_CALLABLE_SIGNATURE_ERROR,
    REMOTE_CALLABLE_SIGNING_KEY_ERROR,
    CapacityClass,
    WorkloadRecord,
    WorkloadState,
)

R_co = tp.TypeVar("R_co", covariant=True)

_ACCELERATOR_TYPE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def reject_remote_callable_signing_key(signing_key: bytes | None) -> None:
    """Reject caller-owned signing keys that cannot safely reach remote workers."""

    if signing_key:
        raise ValueError(REMOTE_CALLABLE_SIGNING_KEY_ERROR)


def reject_remote_callable_signature(signature: str | None) -> None:
    """Reject artifact signatures that no remote worker can verify safely."""

    if signature is not None:
        raise ValueError(REMOTE_CALLABLE_SIGNATURE_ERROR)


def reject_remote_callable_signing_environment(environment: tp.Mapping[str, str] | None) -> None:
    """Reject the worker-only signing secret from remote callable environments."""

    if environment is not None and CALLABLE_SIGNING_KEY_ENV in environment:
        raise ValueError(REMOTE_CALLABLE_SIGNING_KEY_ERROR)


@tp.runtime_checkable
class AcceleratorConfig(tp.Protocol):
    """What the submission path needs from any hardware request.

    ``TpuConfig``, ``GpuConfig`` and ``CpuConfig`` differ only in which shapes they
    accept, so every consumer -- fleet resolution, pool selection, the callable
    submitter -- works against this instead of one concrete class.
    """

    type: str
    pods: int
    max_retries: int
    max_failure_retries: int
    max_preemption_retries: int
    capacity_class: CapacityClass
    secret_mounts: tp.Mapping[str, str]
    checkpointable: bool
    checkpoint_hook: str | None
    checkpoint_output_uri: str | None
    can_submit_children: bool


def _validate_accelerator_request(config: AcceleratorConfig, *, label: str, multi_worker: bool) -> str:
    """Validate the shared fields and return the normalized accelerator type.

    ``multi_worker`` is false for GPU and CPU because ``WorkloadDefinition`` requires
    that ``worker_count > 1`` imply a TPU resource request -- Eyvan's multi-worker
    unit is one atomic Multislice queued resource, which has no GPU or CPU analogue.
    Rejecting ``pods > 1`` here turns that into a clear SDK-level error instead of a
    validation failure deep in the submit path.
    """

    accelerator_type = config.type.strip() if isinstance(config.type, str) else ""
    if not accelerator_type or not _ACCELERATOR_TYPE.fullmatch(accelerator_type):
        raise ValueError(f"{label} type must contain only letters, digits, dots, underscores, or hyphens")
    if isinstance(config.pods, bool) or not isinstance(config.pods, int) or config.pods < 1:
        raise ValueError(f"{label} pods must be a positive integer")
    if not multi_worker and config.pods != 1:
        raise ValueError(f"{label} does not support multi-worker capacity; only TPU pods may exceed 1")
    if not isinstance(config.capacity_class, CapacityClass):
        raise ValueError(f"{label} capacity_class must be a CapacityClass")
    if (
        isinstance(config.max_retries, bool)
        or not isinstance(config.max_retries, int)
        or config.max_retries < 0
        or config.max_retries > 100
    ):
        raise ValueError(f"{label} max_retries must be an integer from 0 through 100")
    # A separate budget, not a synonym. ``max_retries`` pays for capacity the
    # fleet lost -- a preemption, a stockout, a reclaimed spot cell -- which says
    # nothing about the job. ``max_failure_retries`` pays for the job's own
    # non-zero exit, which usually will repeat, which is why it defaults to zero
    # and why merging the two would let a deterministically broken job burn a
    # capacity budget sized for interruptions.
    if (
        isinstance(config.max_failure_retries, bool)
        or not isinstance(config.max_failure_retries, int)
        or config.max_failure_retries < 0
        or config.max_failure_retries > 100
    ):
        raise ValueError(f"{label} max_failure_retries must be an integer from 0 through 100")
    return accelerator_type


@dataclass(frozen=True, slots=True)
class TpuConfig:
    """TPU shape and purchase class requested by an Eyvan callable."""

    type: str
    pods: int = 1
    max_retries: int = DEFAULT_MAX_RETRIES
    #: Retries the job's *own* failure, not lost capacity. Zero by default: a
    #: non-zero exit usually repeats, and re-running it costs an accelerator.
    max_failure_retries: int = 0
    #: Restarts after the provider reclaims the capacity. Larger than
    #: ``max_retries`` on purpose: this is the cheap failure to retry, and a spot
    #: job that cannot outlast a handful of reclamations cannot use spot at all.
    max_preemption_retries: int = 10
    capacity_class: CapacityClass = CapacityClass.SPOT
    #: Secret references, resolved at dispatch and injected as environment
    #: variables on the cell. Unlike a value passed through ``environment``, a
    #: reference is what gets persisted -- which is the whole point.
    secret_mounts: tp.Mapping[str, str] = field(default_factory=dict)
    #: Whether this job can be checkpointed, and the hook that does it. The
    #: scheduler grants ``ON_PREEMPTION`` only when ``checkpointable`` is set and
    #: a hook is named, so a job that declares neither restarts from zero on
    #: every reclamation -- ``max_preemption_retries`` buys it more attempts, not
    #: a resumption. Off by default because a hook that does not exist is worse
    #: than none: the job would be stopped for a checkpoint it never wrote.
    checkpointable: bool = False
    checkpoint_hook: str | None = None
    checkpoint_output_uri: str | None = None
    #: Whether this job receives a capability to submit children of its own,
    #: under its owner's identity. Off by default: a credential nobody asked
    #: for is one nobody is watching.
    can_submit_children: bool = False

    def __post_init__(self) -> None:
        normalized = _validate_accelerator_request(self, label="TPU", multi_worker=True)
        object.__setattr__(self, "type", normalized)


@dataclass(frozen=True, slots=True)
class GpuConfig:
    """GPU shape and purchase class requested by an Eyvan callable.

    Names a pool's accelerator type (``nvidia-tesla-a100``, ``nvidia-h100-80gb``),
    not a GPU count: the fleet YAML owns shape, provider and machine type, and the
    job names a logical pool. That separation is the same one ``TpuConfig`` keeps,
    and it is why this class carries no ``machine_type`` or ``gpu_count``.

    Defaults to on-demand rather than spot, unlike TPU: GPU spot capacity is
    reclaimed far more aggressively, and a silently preemptible default would
    surprise a caller who wrote one line.
    """

    type: str
    max_retries: int = DEFAULT_MAX_RETRIES
    #: Retries the job's *own* failure, not lost capacity. See ``TpuConfig``.
    max_failure_retries: int = 0
    #: Restarts after the provider reclaims the capacity. See ``TpuConfig``.
    max_preemption_retries: int = 10
    capacity_class: CapacityClass = CapacityClass.ON_DEMAND
    #: Fixed at one. Present so the submission path is shape-agnostic.
    pods: int = 1
    #: Secret references, resolved at dispatch and injected as environment
    #: variables on the cell. Unlike a value passed through ``environment``, a
    #: reference is what gets persisted -- which is the whole point.
    secret_mounts: tp.Mapping[str, str] = field(default_factory=dict)
    #: Whether this job can be checkpointed, and the hook that does it. The
    #: scheduler grants ``ON_PREEMPTION`` only when ``checkpointable`` is set and
    #: a hook is named, so a job that declares neither restarts from zero on
    #: every reclamation -- ``max_preemption_retries`` buys it more attempts, not
    #: a resumption. Off by default because a hook that does not exist is worse
    #: than none: the job would be stopped for a checkpoint it never wrote.
    checkpointable: bool = False
    checkpoint_hook: str | None = None
    checkpoint_output_uri: str | None = None
    #: Whether this job receives a capability to submit children of its own,
    #: under its owner's identity. Off by default: a credential nobody asked
    #: for is one nobody is watching.
    can_submit_children: bool = False

    def __post_init__(self) -> None:
        normalized = _validate_accelerator_request(self, label="GPU", multi_worker=False)
        object.__setattr__(self, "type", normalized)


@dataclass(frozen=True, slots=True)
class CpuConfig:
    """CPU shape and purchase class requested by an Eyvan callable."""

    type: str = "cpu"
    max_retries: int = DEFAULT_MAX_RETRIES
    #: Retries the job's *own* failure, not lost capacity. See ``TpuConfig``.
    max_failure_retries: int = 0
    #: Restarts after the provider reclaims the capacity. See ``TpuConfig``.
    max_preemption_retries: int = 10
    capacity_class: CapacityClass = CapacityClass.ON_DEMAND
    #: Fixed at one. Present so the submission path is shape-agnostic.
    pods: int = 1
    #: Secret references, resolved at dispatch and injected as environment
    #: variables on the cell. Unlike a value passed through ``environment``, a
    #: reference is what gets persisted -- which is the whole point.
    secret_mounts: tp.Mapping[str, str] = field(default_factory=dict)
    #: Whether this job can be checkpointed, and the hook that does it. The
    #: scheduler grants ``ON_PREEMPTION`` only when ``checkpointable`` is set and
    #: a hook is named, so a job that declares neither restarts from zero on
    #: every reclamation -- ``max_preemption_retries`` buys it more attempts, not
    #: a resumption. Off by default because a hook that does not exist is worse
    #: than none: the job would be stopped for a checkpoint it never wrote.
    checkpointable: bool = False
    checkpoint_hook: str | None = None
    checkpoint_output_uri: str | None = None
    #: Whether this job receives a capability to submit children of its own,
    #: under its owner's identity. Off by default: a credential nobody asked
    #: for is one nobody is watching.
    can_submit_children: bool = False

    def __post_init__(self) -> None:
        normalized = _validate_accelerator_request(self, label="CPU", multi_worker=False)
        object.__setattr__(self, "type", normalized)


@dataclass(frozen=True, slots=True)
class CallableSubmission:
    """Durable callable intent passed from the decorator to an SDK adapter."""

    invocation_id: str
    fleet: str
    org_id: str
    project_id: str
    callable_name: str
    #: The hardware request. Named ``tpu`` for wire compatibility; any
    #: ``AcceleratorConfig`` -- TPU, GPU or CPU -- is accepted.
    tpu: AcceleratorConfig
    artifact: StoredCallableArtifact
    result_uri: str
    #: Content address of an uploaded workspace bundle, when the caller opted in
    #: with ``@execute(..., workspace=...)``. ``None`` keeps the historical
    #: behaviour of shipping only the bound function.
    workspace_uri: str | None = None
    workspace_sha256: str | None = None
    #: HMAC of the bundle under the project signing key. Required whenever the
    #: callable itself is signed: the workspace precedes it on ``sys.path``.
    workspace_signature: str | None = None


@dataclass(frozen=True, slots=True)
class JobCompletion:
    """Terminal state returned by an Eyvan job backend."""

    state: WorkloadState
    succeeded: bool
    message: str | None = None


JobProgressWaiter: tp.TypeAlias = tp.Callable[[str, float | None], JobCompletion]


class JobBackend(tp.Protocol):
    """Operations an SDK adapter supplies to a durable ``EyvanJob``."""

    def wait(self, job_id: str, timeout: float | None = None) -> JobCompletion:
        """Wait for terminal state and describe the outcome."""

        ...

    def status(self, job_id: str) -> WorkloadState:
        """Return the current control-plane state."""

        ...

    def logs(self, job_id: str) -> str:
        """Return currently available workload logs."""

        ...

    def cancel(self, job_id: str) -> WorkloadRecord:
        """Request cancellation and return the updated workload."""

        ...


class EyvanJobError(RuntimeError):
    """Raised when a durable callable job does not succeed."""

    def __init__(self, job_id: str, completion: JobCompletion):
        detail = f": {completion.message}" if completion.message else ""
        super().__init__(f"Eyvan job {job_id!r} finished in state {completion.state!r}{detail}")
        self.job_id = job_id
        self.completion = completion


class EyvanPlacementError(RuntimeError):
    """Raised when no enabled capacity pool can satisfy a callable's TPU request.

    ``fleet`` names the fleet that was already selected. It is ``None`` when the
    request failed while auto-resolving a fleet from a :class:`TpuConfig`, in
    which case ``scope`` names the ``org/project`` that was searched.
    """

    def __init__(
        self,
        *,
        requested_accelerator_type: str,
        available_accelerator_types: tp.Iterable[str],
        fleet: str | None = None,
        scope: str | None = None,
    ) -> None:
        self.fleet = fleet
        self.scope = scope
        self.requested_accelerator_type = requested_accelerator_type
        self.available_accelerator_types = sort_accelerator_types(available_accelerator_types)
        if fleet is not None:
            location = f"in selected fleet {fleet!r}"
            empty_detail = "That fleet has no compatible enabled TPU pools for this request."
        else:
            location = "in any enabled fleet" if scope is None else f"in any enabled fleet for {scope}"
            empty_detail = "No enabled fleet exposes a compatible TPU pool for this request."
        message = f"Eyvan cannot run TPU {requested_accelerator_type!r} {location}."
        if self.available_accelerator_types:
            available = ", ".join(self.available_accelerator_types)
            message = (
                f"{message} Compatible TPU types for this request: {available}. "
                "Change TpuConfig(type=...) to one of those, or select/apply a fleet containing the requested type "
                "with `eyvan use FLEET` or `eyvan up CONFIG`."
            )
        else:
            message = (
                f"{message} {empty_detail} "
                "Select another fleet with `eyvan use FLEET`, or apply one with `eyvan up CONFIG`."
            )
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class EyvanJob(tp.Generic[R_co]):
    """Durable handle returned by an Eyvan callable submission."""

    id: str
    result_uri: str
    backend: JobBackend = field(repr=False, compare=False)
    result_reader: tp.Callable[[str], R_co] = field(repr=False, compare=False)
    progress_waiter: JobProgressWaiter | None = field(default=None, repr=False, compare=False)

    def wait(self, timeout: float | None = None, *, progress: bool = False) -> JobCompletion:
        """Wait until the workload reaches a terminal state.

        Args:
            timeout: Maximum number of seconds to wait, or ``None`` to wait indefinitely.
            progress: Render control-plane lifecycle updates while waiting when the backend supports them.
        """

        if progress and self.progress_waiter is not None:
            return self.progress_waiter(self.id, timeout)
        return self.backend.wait(self.id, timeout)

    def status(self) -> WorkloadState:
        """Return the current workload state."""

        return self.backend.status(self.id)

    def logs(self) -> str:
        """Return currently available workload logs."""

        return self.backend.logs(self.id)

    def cancel(self) -> WorkloadRecord:
        """Request cancellation through the control plane."""

        return self.backend.cancel(self.id)

    def result(self, timeout: float | None = None) -> R_co:
        """Wait for success with lifecycle updates, then decode the durable result artifact."""

        completion = self.wait(timeout, progress=True)
        if not completion.succeeded:
            raise EyvanJobError(self.id, completion)
        return self.result_reader(self.result_uri)


class CallableSubmitter(tp.Protocol[R_co]):
    """SDK integration boundary for native callable workloads."""

    def submit_callable(self, request: CallableSubmission) -> EyvanJob[R_co]:
        """Create one durable workload from ``request``."""

        ...


__all__ = (
    "AcceleratorConfig",
    "CallableSubmission",
    "CallableSubmitter",
    "CpuConfig",
    "EyvanJob",
    "EyvanJobError",
    "EyvanPlacementError",
    "GpuConfig",
    "JobBackend",
    "JobCompletion",
    "JobProgressWaiter",
    "TpuConfig",
    "reject_remote_callable_signature",
    "reject_remote_callable_signing_environment",
    "reject_remote_callable_signing_key",
)
