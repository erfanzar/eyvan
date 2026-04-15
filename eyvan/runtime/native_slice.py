"""Coordinated native execution across every host in one TPU slice group."""

from __future__ import annotations

import copy
import logging
import typing as tp
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx
from pydantic import ValidationError

from eyvan._compat import StrEnum
from eyvan._json import JsonObject, JsonValue
from eyvan.distributed_environment import without_distributed_topology_overrides
from eyvan.drivers.base import (
    ExecutionWorkloadObservation,
    ExecutionWorkloadResult,
    ExecutionWorkloadStatus,
    MutationContext,
)
from eyvan.runtime.native_remote import NativeRemoteExecutor

_MULTIHOST_SITE_PATH = "/opt/eyvan-native/multihost"
_COORDINATOR_PORT = 12345
_RETIRE_SUMMARY_LIMIT = 4
_FAILED_STATUSES = frozenset({ExecutionWorkloadStatus.FAILED})
_LOST_STATUSES = frozenset({ExecutionWorkloadStatus.LOST, ExecutionWorkloadStatus.NOT_FOUND})
_RUNNING_STATUSES = frozenset(
    {
        ExecutionWorkloadStatus.PENDING,
        ExecutionWorkloadStatus.SUBMITTED,
        ExecutionWorkloadStatus.RUNNING,
        ExecutionWorkloadStatus.STOPPING,
        ExecutionWorkloadStatus.CHECKPOINTING,
    }
)
_MutationAction: tp.TypeAlias = tp.Literal["stop", "retire"]

logger = logging.getLogger(__name__)


class JaxClusterDetectionMode(StrEnum):
    """Supported JAX discovery contracts for one native TPU allocation."""

    EXPLICIT = "explicit"
    GCE_TPU = "gcetpu"


class NativeSliceWorkerResult(tp.TypedDict):
    """Wire-compatible observation for one host in a native TPU slice."""

    rank: int
    endpoint: str
    remote_id: str
    status: ExecutionWorkloadStatus
    message: str


class NativeSliceWorkloadResult(ExecutionWorkloadResult):
    """Aggregate slice status plus the typed observation from every rank."""

    workers: list[NativeSliceWorkerResult]


@dataclass(frozen=True, slots=True)
class _SliceWorkerObservation:
    """One typed worker observation together with its stable rank metadata."""

    rank: int
    endpoint: str
    observation: ExecutionWorkloadObservation

    def as_result(self) -> NativeSliceWorkerResult:
        """Return the worker detail in the established mapping wire shape."""

        return {
            "rank": self.rank,
            "endpoint": self.endpoint,
            "remote_id": self.observation.remote_id,
            "status": self.observation.status,
            "message": self.observation.message,
        }


def _indeterminate_observation(remote_id: str, reason: str) -> ExecutionWorkloadObservation:
    """Represent an explicit failure to obtain a definitive worker observation."""

    return ExecutionWorkloadObservation(
        remote_id=remote_id,
        status=ExecutionWorkloadStatus.INDETERMINATE,
        message=reason,
    )


def _lost_observation(remote_id: str, reason: str) -> ExecutionWorkloadObservation:
    """Represent a worker proven unreachable by a transport failure."""

    return ExecutionWorkloadObservation(
        remote_id=remote_id,
        status=ExecutionWorkloadStatus.LOST,
        message=reason,
    )


def _invalid_observation_reason(
    error: Exception,
    *,
    endpoint: str,
) -> str:
    """Keep a validated worker message when Pydantic rejects its observation."""

    if isinstance(error, ValidationError):
        for detail in error.errors():
            if "input" not in detail:
                continue
            payload = detail["input"]
            if not isinstance(payload, dict) or "message" not in payload:
                continue
            message = payload["message"]
            if isinstance(message, str) and message.strip():
                return f"native worker {endpoint} returned an invalid observation: {message}"
    return f"native worker {endpoint} returned an invalid observation: {type(error).__name__}: {error}"


def _normalize_worker_observation(
    observation: ExecutionWorkloadObservation,
    *,
    expected_remote_id: str,
    endpoint: str,
) -> ExecutionWorkloadObservation:
    """Require one typed observation to describe the requested remote workload."""

    if observation.remote_id != expected_remote_id:
        return _indeterminate_observation(
            expected_remote_id,
            f"native worker {endpoint} returned a mismatched remote_id",
        )
    if observation.status is ExecutionWorkloadStatus.INDETERMINATE and not observation.message:
        return _indeterminate_observation(
            expected_remote_id,
            f"native worker {endpoint} reported an indeterminate status without a reason",
        )
    return observation


def _aggregate_status(workers: tp.Sequence[_SliceWorkerObservation]) -> ExecutionWorkloadStatus:
    """Reduce typed rank observations without manufacturing a default status."""

    states = tuple(worker.observation.status for worker in workers)
    if not states:
        raise RuntimeError("native slice has no workers to observe")
    if all(state is ExecutionWorkloadStatus.SUCCEEDED for state in states):
        return ExecutionWorkloadStatus.SUCCEEDED
    if all(state is ExecutionWorkloadStatus.NOT_FOUND for state in states):
        return ExecutionWorkloadStatus.NOT_FOUND
    if all(state is ExecutionWorkloadStatus.STOPPED for state in states):
        return ExecutionWorkloadStatus.STOPPED
    if any(state in _FAILED_STATUSES for state in states):
        return ExecutionWorkloadStatus.FAILED
    if any(state is ExecutionWorkloadStatus.INDETERMINATE for state in states):
        return ExecutionWorkloadStatus.INDETERMINATE
    if any(state in _RUNNING_STATUSES for state in states) and all(
        state in _RUNNING_STATUSES | {ExecutionWorkloadStatus.SUCCEEDED} for state in states
    ):
        # Hosts can reach a terminal systemd state a few seconds apart. A
        # completed rank alongside otherwise active ranks is still converging.
        return ExecutionWorkloadStatus.RUNNING
    if any(
        state in _LOST_STATUSES | {ExecutionWorkloadStatus.STOPPED, ExecutionWorkloadStatus.SUCCEEDED}
        for state in states
    ):
        # A vanished or stopped rank prevents distributed JAX progress even
        # while peers remain active.
        return ExecutionWorkloadStatus.LOST
    if any(state in _RUNNING_STATUSES for state in states):
        return ExecutionWorkloadStatus.RUNNING
    raise RuntimeError(f"native slice has no aggregate rule for worker states {states!r}")


def _aggregate_message(workers: tp.Sequence[_SliceWorkerObservation]) -> str:
    """Render each explicit rank observation, preserving failure reasons."""

    return "; ".join(
        f"rank {worker.rank}={worker.observation.status.value}"
        + (f": {worker.observation.message}" if worker.observation.message else "")
        for worker in workers
    )


def _bounded_retire_summary(failures: tp.Sequence[str]) -> str:
    """Render a bounded per-rank retire failure summary."""

    summary = "; ".join(failures[:_RETIRE_SUMMARY_LIMIT])
    remaining = len(failures) - _RETIRE_SUMMARY_LIMIT
    if remaining > 0:
        summary += f"; +{remaining} more"
    return summary


def _augment_error_detail(error: BaseException, detail: str) -> None:
    """Append cleanup detail to an error without changing its type."""

    if error.args and isinstance(error.args[0], str):
        error.args = (f"{error.args[0]}; {detail}", *error.args[1:])
    else:
        error.args = (*error.args, detail)


class NativeSliceExecutor:
    """Fan one fenced workload out to every host in an atomic TPU slice group.

    Each remote worker receives the same logical submission ID, but rank-local
    JAX environment variables. The configured ``sitecustomize`` hook performs
    ``jax.distributed.initialize`` before the workload imports JAX.
    """

    def __init__(
        self,
        endpoints: tp.Sequence[str],
        *,
        secret: str,
        clients: tp.Sequence[httpx.Client] | None = None,
        jax_cluster_detection: JaxClusterDetectionMode = JaxClusterDetectionMode.EXPLICIT,
    ) -> None:
        normalized = tuple(endpoint.rstrip("/") for endpoint in endpoints)
        if not normalized or any(not endpoint or endpoint != endpoint.strip() for endpoint in normalized):
            raise ValueError("native slice execution requires non-empty worker endpoints")
        if len(normalized) != len(set(normalized)):
            raise ValueError("native slice execution worker endpoints must be unique")
        coordinator = urlparse(normalized[0])
        if coordinator.scheme not in {"http", "https"} or not coordinator.hostname:
            raise ValueError("native slice coordinator endpoint must be an HTTP URL with a host")
        if clients is not None and len(clients) != len(normalized):
            raise ValueError("native slice clients must match the number of worker endpoints")
        if not isinstance(jax_cluster_detection, JaxClusterDetectionMode):
            raise TypeError("native slice JAX cluster detection must be a JaxClusterDetectionMode")
        self.endpoints = normalized
        self.jax_cluster_detection = jax_cluster_detection
        self._coordinator_address = f"{coordinator.hostname}:{_COORDINATOR_PORT}"
        self._workers = tuple(
            NativeRemoteExecutor(endpoint, secret=secret, client=client)
            for endpoint, client in zip(normalized, clients or (None,) * len(normalized), strict=True)
        )

    def health(self) -> bool:
        """Return whether every host accepts authenticated native-worker requests."""

        return all(worker.health() for worker in self._workers)

    def _ranked_spec(self, spec: tp.Mapping[str, JsonValue], rank: int) -> JsonObject:
        """Copy one JSON workload spec and add its rank-local environment."""

        payload: JsonObject = copy.deepcopy(dict(spec))
        raw_environment = payload.get("environment")
        if raw_environment is None:
            source_environment: JsonObject = {}
        elif isinstance(raw_environment, dict):
            source_environment = raw_environment
        else:
            raise ValueError("native slice workload environment must be a mapping")
        environment: dict[str, str] = {}
        for key, value in source_environment.items():
            if not isinstance(value, str):
                raise ValueError("native slice workload environment must be a string mapping")
            environment[key] = value
        environment = without_distributed_topology_overrides(environment)
        python_path = environment.get("PYTHONPATH", "")
        environment.update(
            {
                "EYVAN_DISTRIBUTED_PROCESS_COUNT": str(len(self._workers)),
                "EYVAN_DISTRIBUTED_PROCESS_ID": str(rank),
                "EYVAN_JAX_CLUSTER_DETECTION": self.jax_cluster_detection.value,
                "EYVAN_JAX_MULTIHOST": "1",
                "EYVAN_REPLICA_COUNT": str(len(self._workers)),
                "EYVAN_REPLICA_INDEX": str(rank),
                "JAX_COORDINATOR_PORT": str(_COORDINATOR_PORT),
                "PYTHONPATH": f"{_MULTIHOST_SITE_PATH}:{python_path}" if python_path else _MULTIHOST_SITE_PATH,
            }
        )
        if self.jax_cluster_detection is JaxClusterDetectionMode.EXPLICIT:
            environment.update(
                {
                    "JAX_COORDINATOR_ADDRESS": self._coordinator_address,
                    "JAX_PROCESS_COUNT": str(len(self._workers)),
                    "JAX_PROCESS_ID": str(rank),
                    "JAX_PROCESS_INDEX": str(rank),
                }
            )
        payload["environment"] = environment
        return payload

    def submit(self, spec: tp.Mapping[str, JsonValue], context: MutationContext) -> str:
        """Submit one identical logical workload to every slice host.

        A submission is only considered accepted when every worker returns the
        same logical ID. Any partial launch is fenced and retired before the
        original error is raised. A retire failure during that cleanup leaves a
        rank running, so it is logged at warning level and appended to the
        raised error to keep the orphaned rank observable.
        """

        raw_submission_id = spec.get("submission_id")
        if raw_submission_id is None:
            expected_remote_id = context.idempotency_key
        elif isinstance(raw_submission_id, str) and raw_submission_id.strip():
            expected_remote_id = raw_submission_id.strip()
        else:
            raise ValueError("native slice submission requires a non-empty submission ID")
        attempted: list[tuple[int, NativeRemoteExecutor, set[str]]] = []
        try:
            for rank, worker in enumerate(self._workers):
                remote_ids = {expected_remote_id}
                attempted.append((rank, worker, remote_ids))
                remote_id = worker.submit(self._ranked_spec(spec, rank), context)
                remote_ids.add(remote_id)
                if remote_id != expected_remote_id:
                    raise RuntimeError(
                        f"native slice worker returned unexpected remote ID {remote_id!r}; "
                        f"expected {expected_remote_id!r}"
                    )
            return expected_remote_id
        except BaseException as launch_error:
            retire_failures: list[str] = []
            for rank, worker, remote_ids in reversed(attempted):
                for attempted_remote_id in sorted(remote_ids):
                    try:
                        worker.retire(attempted_remote_id, context)
                    except Exception as exc:
                        retire_failures.append(f"rank {rank} ({worker.endpoint}): {type(exc).__name__}")
            if retire_failures:
                detail = f"partial-launch cleanup could not retire {_bounded_retire_summary(retire_failures)}"
                logger.warning("native slice submission failed and %s", detail)
                _augment_error_detail(launch_error, detail)
            raise

    def status(self, remote_id: str) -> NativeSliceWorkloadResult:
        """Aggregate typed rank observations into one durable slice state."""

        workers: list[_SliceWorkerObservation] = []
        for rank, (endpoint, worker) in enumerate(zip(self.endpoints, self._workers, strict=True)):
            try:
                observation = _normalize_worker_observation(
                    worker.status(remote_id),
                    expected_remote_id=remote_id,
                    endpoint=endpoint,
                )
            except httpx.TransportError as exc:
                observation = _lost_observation(
                    remote_id,
                    f"native worker {endpoint} is unreachable: {type(exc).__name__}: {exc}",
                )
            except (httpx.HTTPStatusError, RuntimeError, ValueError) as exc:
                observation = _indeterminate_observation(
                    remote_id,
                    _invalid_observation_reason(exc, endpoint=endpoint),
                )
            workers.append(
                _SliceWorkerObservation(
                    rank=rank,
                    endpoint=endpoint,
                    observation=observation,
                )
            )
        aggregate = ExecutionWorkloadObservation(
            remote_id=remote_id,
            status=_aggregate_status(workers),
            message=_aggregate_message(workers),
        )
        return {
            "remote_id": aggregate.remote_id,
            "status": aggregate.status,
            "message": aggregate.message,
            "workers": [worker.as_result() for worker in workers],
        }

    def stop(self, remote_id: str, context: MutationContext) -> None:
        """Stop the workload on every host, preserving the first failure."""

        self._fanout_mutation("stop", remote_id, context)

    def retire(self, remote_id: str, context: MutationContext) -> None:
        """Retire the workload on every host, preserving the first failure."""

        self._fanout_mutation("retire", remote_id, context)

    def _fanout_mutation(self, action: _MutationAction, remote_id: str, context: MutationContext) -> None:
        """Apply one closed mutation action to every worker."""

        first_error: BaseException | None = None
        for worker in self._workers:
            try:
                if action == "stop":
                    worker.stop(remote_id, context)
                elif action == "retire":
                    worker.retire(remote_id, context)
                else:
                    raise ValueError(f"unsupported native slice mutation action {action!r}")
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    def checkpoint(
        self,
        remote_id: str,
        *,
        hook: str,
        output_uri: str | None,
        context: MutationContext,
    ) -> str:
        """Launch one rank-aware checkpoint hook across every host in the slice.

        The hook runs on all hosts under a single shared logical ID, and each
        host's worker inherits *that host's* source environment -- so every rank
        keeps its JAX coordinator address, process id, and process count and the
        hook forms the same cluster the workload itself did. Without that, ranks
        would each come up as a single-process job and race on one output URI.

        Launch is all-or-nothing, mirroring ``submit``: a partial launch is
        fenced and retired before the original error is raised, because a
        surviving rank would hold accelerators and could still write to the
        checkpoint destination. Completion is *not* asserted here -- callers
        poll ``status``, which only reports success once every rank has
        succeeded.
        """

        if not isinstance(hook, str) or not hook.strip():
            raise ValueError("native slice checkpoint hook must be non-empty")
        expected_remote_id = context.idempotency_key
        attempted: list[tuple[int, NativeRemoteExecutor, set[str]]] = []
        try:
            for rank, worker in enumerate(self._workers):
                remote_ids = {expected_remote_id}
                attempted.append((rank, worker, remote_ids))
                checkpoint_id = worker.checkpoint(
                    remote_id,
                    hook=hook,
                    output_uri=output_uri,
                    context=context,
                )
                remote_ids.add(checkpoint_id)
                if checkpoint_id != expected_remote_id:
                    raise RuntimeError(
                        f"native slice worker returned unexpected checkpoint ID {checkpoint_id!r}; "
                        f"expected {expected_remote_id!r}"
                    )
            return expected_remote_id
        except BaseException as launch_error:
            retire_failures: list[str] = []
            for rank, worker, remote_ids in reversed(attempted):
                for attempted_remote_id in sorted(remote_ids):
                    try:
                        worker.retire(attempted_remote_id, context)
                    except Exception as exc:
                        retire_failures.append(f"rank {rank} ({worker.endpoint}): {type(exc).__name__}")
            if retire_failures:
                detail = f"partial checkpoint cleanup could not retire {_bounded_retire_summary(retire_failures)}"
                logger.warning("native slice checkpoint failed and %s", detail)
                _augment_error_detail(launch_error, detail)
            raise

    def logs(self, remote_id: str) -> str:
        """Return rank-prefixed logs from every TPU VM host when available."""

        sections: list[str] = []
        for rank, (endpoint, worker) in enumerate(zip(self.endpoints, self._workers, strict=True)):
            try:
                logs = worker.logs(remote_id)
            except httpx.TransportError as exc:
                logs = f"native worker is unreachable: {type(exc).__name__}: {exc}"
            except (httpx.HTTPStatusError, RuntimeError, ValueError) as exc:
                logs = f"native worker log retrieval failed: {type(exc).__name__}: {exc}"
            sections.append(f"===== rank {rank} {endpoint} =====\n{logs}")
        return "\n".join(sections)

    def process_status(self, remote_id: str) -> JsonObject:
        """Return per-rank process detail across every host in the slice.

        A hung multi-host job is exactly the case where one rank differs from
        the rest, so an unreachable host is reported in place rather than
        failing the whole probe.
        """

        ranks: list[JsonValue] = []
        for rank, (endpoint, worker) in enumerate(zip(self.endpoints, self._workers, strict=True)):
            try:
                detail: JsonValue = worker.process_status(remote_id)
            except httpx.TransportError as exc:
                detail = {"observed": False, "detail": f"native worker is unreachable: {type(exc).__name__}: {exc}"}
            except (httpx.HTTPStatusError, RuntimeError, ValueError) as exc:
                detail = {"observed": False, "detail": f"process status failed: {type(exc).__name__}: {exc}"}
            ranks.append({"rank": rank, "endpoint": endpoint, "status": detail})
        return {"remote_id": remote_id, "ranks": ranks}

    def close(self) -> None:
        """Close internally owned native-worker clients."""

        for worker in self._workers:
            worker.close()


__all__ = ("NativeSliceExecutor",)
