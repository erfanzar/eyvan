# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Coordinated native execution across several independently managed cells."""

from __future__ import annotations

import copy
import logging
import typing as tp
from dataclasses import dataclass, replace
from urllib.parse import urlparse

import httpx
from pydantic import ValidationError

from eyvan._internal.json_types import JsonObject, JsonValue
from eyvan.capacity.drivers.base import (
    ExecutionWorkloadObservation,
    ExecutionWorkloadResult,
    ExecutionWorkloadStatus,
    MutationContext,
)
from eyvan.worker.distributed_environment import without_distributed_topology_overrides
from eyvan.worker.runtime.native_remote import NativeRemoteExecutor

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


class NativeGangWorkerResult(tp.TypedDict):
    """Wire-compatible observation for one worker in a native cell gang."""

    rank: int
    cell_id: str
    endpoint: str
    remote_id: str
    status: ExecutionWorkloadStatus
    message: str
    checkpoint_uri: str | None


class NativeGangWorkloadResult(ExecutionWorkloadResult):
    """Aggregate gang status plus the typed observation from every rank."""

    workers: list[NativeGangWorkerResult]


@dataclass(frozen=True, slots=True)
class NativeGangMember:
    """Native-worker endpoints and fence owned by one execution cell."""

    cell_id: str
    endpoints: tuple[str, ...]
    secret: str
    context: MutationContext

    def __post_init__(self) -> None:
        if not isinstance(self.cell_id, str) or not self.cell_id.strip():
            raise ValueError("native gang member cell ID must be non-empty and trimmed")
        if not self.endpoints or any(
            not isinstance(endpoint, str) or not endpoint.strip() for endpoint in self.endpoints
        ):
            raise ValueError("native gang member requires non-empty worker endpoints")
        if len(self.endpoints) != len(set(self.endpoints)):
            raise ValueError("native gang member worker endpoints must be unique")
        if not isinstance(self.secret, str) or not self.secret.strip():
            raise ValueError("native gang member requires a non-empty worker bearer secret")


@dataclass(frozen=True, slots=True)
class _GangWorker:
    """One rank-addressable native worker with its cell-scoped fence."""

    cell_id: str
    cell_index: int
    endpoint: str
    context: MutationContext
    executor: NativeRemoteExecutor


@dataclass(frozen=True, slots=True)
class _GangWorkerObservation:
    """One typed worker observation together with stable gang rank metadata."""

    rank: int
    worker: _GangWorker
    observation: ExecutionWorkloadObservation
    checkpoint_uri: str | None = None

    def as_result(self) -> NativeGangWorkerResult:
        """Return the worker detail in the established mapping wire shape."""

        return {
            "rank": self.rank,
            "cell_id": self.worker.cell_id,
            "endpoint": self.worker.endpoint,
            "remote_id": self.observation.remote_id,
            "status": self.observation.status,
            "message": self.observation.message,
            "checkpoint_uri": self.checkpoint_uri,
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


def _aggregate_status(workers: tp.Sequence[_GangWorkerObservation]) -> ExecutionWorkloadStatus:
    """Reduce typed rank observations without manufacturing a default status."""

    states = tuple(worker.observation.status for worker in workers)
    if not states:
        raise RuntimeError("native gang has no workers to observe")
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
        return ExecutionWorkloadStatus.RUNNING
    if any(
        state in _LOST_STATUSES | {ExecutionWorkloadStatus.STOPPED, ExecutionWorkloadStatus.SUCCEEDED}
        for state in states
    ):
        return ExecutionWorkloadStatus.LOST
    if any(state in _RUNNING_STATUSES for state in states):
        return ExecutionWorkloadStatus.RUNNING
    raise RuntimeError(f"native gang has no aggregate rule for worker states {states!r}")


def _aggregate_message(workers: tp.Sequence[_GangWorkerObservation]) -> str:
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


class NativeGangExecutor:
    """Fan one fenced JAX workload across independently allocated Eyvan cells.

    Every worker uses the cell-scoped native-worker bearer token and mutation
    context that created its TPU slice. This differs from
    :class:`NativeSliceExecutor`, whose endpoints all belong to one cell and
    therefore share one token and fence.
    """

    def __init__(
        self,
        members: tp.Sequence[NativeGangMember],
        *,
        clients: tp.Sequence[httpx.Client | None] | None = None,
    ) -> None:
        ordered_members = tuple(sorted(members, key=lambda member: member.cell_id))
        if len(ordered_members) < 2:
            raise ValueError("native gang execution requires at least two execution cells")
        if len({member.cell_id for member in ordered_members}) != len(ordered_members):
            raise ValueError("native gang execution cell IDs must be unique")

        flattened = [
            (member, cell_index, endpoint.rstrip("/"))
            for cell_index, member in enumerate(ordered_members)
            for endpoint in member.endpoints
        ]
        endpoints = tuple(endpoint for _, _, endpoint in flattened)
        if len(endpoints) != len(set(endpoints)):
            raise ValueError("native gang execution worker endpoints must be unique across cells")
        if clients is not None and len(clients) != len(flattened):
            raise ValueError("native gang clients must match the number of worker endpoints")
        coordinator = urlparse(endpoints[0])
        if coordinator.scheme not in {"http", "https"} or not coordinator.hostname:
            raise ValueError("native gang coordinator endpoint must be an HTTP URL with a host")

        self.members = ordered_members
        self.endpoints = endpoints
        self._coordinator_address = f"{coordinator.hostname}:{_COORDINATOR_PORT}"
        supplied_clients = clients or (None,) * len(flattened)
        self._workers = tuple(
            _GangWorker(
                cell_id=member.cell_id,
                cell_index=cell_index,
                endpoint=endpoint,
                context=member.context,
                executor=NativeRemoteExecutor(endpoint, secret=member.secret, client=client),
            )
            for (member, cell_index, endpoint), client in zip(flattened, supplied_clients, strict=True)
        )

    def health(self) -> bool:
        """Return whether every native worker accepts its cell-scoped credential."""

        return all(worker.executor.health() for worker in self._workers)

    def _ranked_spec(self, spec: tp.Mapping[str, JsonValue], rank: int, worker: _GangWorker) -> JsonObject:
        """Copy one JSON workload spec and add its rank-local environment."""

        payload: JsonObject = copy.deepcopy(dict(spec))
        raw_environment = payload.get("environment")
        if raw_environment is None:
            source_environment: JsonObject = {}
        elif isinstance(raw_environment, dict):
            source_environment = raw_environment
        else:
            raise ValueError("native gang workload environment must be a mapping")
        environment: dict[str, str] = {}
        for key, value in source_environment.items():
            if not isinstance(value, str):
                raise ValueError("native gang workload environment must be a string mapping")
            environment[key] = value
        environment = without_distributed_topology_overrides(environment)
        python_path = environment.get("PYTHONPATH", "")
        environment.update(
            {
                "EYVAN_DISTRIBUTED_PROCESS_COUNT": str(len(self._workers)),
                "EYVAN_DISTRIBUTED_PROCESS_ID": str(rank),
                "EYVAN_GANG_CELL_ID": worker.cell_id,
                "EYVAN_GANG_CELL_INDEX": str(worker.cell_index),
                "EYVAN_JAX_MULTIHOST": "1",
                "EYVAN_REPLICA_COUNT": str(len(self._workers)),
                "EYVAN_REPLICA_INDEX": str(rank),
                "JAX_COORDINATOR_ADDRESS": self._coordinator_address,
                "JAX_COORDINATOR_PORT": str(_COORDINATOR_PORT),
                "JAX_PARTITION_INDEX": str(worker.cell_index),
                "JAX_PROCESS_COUNT": str(len(self._workers)),
                "JAX_PROCESS_ID": str(rank),
                "JAX_PROCESS_INDEX": str(rank),
                "PYTHONPATH": f"{_MULTIHOST_SITE_PATH}:{python_path}" if python_path else _MULTIHOST_SITE_PATH,
            }
        )
        payload["environment"] = environment
        return payload

    def submit(self, spec: tp.Mapping[str, JsonValue]) -> str:
        """Submit one logical workload to every gang rank or retire a partial launch.

        A retire failure during partial-launch cleanup leaves a rank running,
        so it is logged at warning level and appended to the raised error to
        keep the orphaned rank observable.
        """

        raw_submission_id = spec.get("submission_id")
        if not isinstance(raw_submission_id, str) or not raw_submission_id.strip():
            raise ValueError("native gang submission requires a non-empty submission ID")
        expected_remote_id = raw_submission_id.strip()
        attempted: list[tuple[int, _GangWorker, set[str]]] = []
        try:
            for rank, worker in enumerate(self._workers):
                remote_ids = {expected_remote_id}
                attempted.append((rank, worker, remote_ids))
                remote_id = worker.executor.submit(self._ranked_spec(spec, rank, worker), worker.context)
                remote_ids.add(remote_id)
                if remote_id != expected_remote_id:
                    raise RuntimeError(
                        f"native gang worker returned unexpected remote ID {remote_id!r}; "
                        f"expected {expected_remote_id!r}"
                    )
            return expected_remote_id
        except BaseException as launch_error:
            retire_failures: list[str] = []
            for rank, worker, remote_ids in reversed(attempted):
                for attempted_remote_id in sorted(remote_ids):
                    try:
                        worker.executor.retire(attempted_remote_id, worker.context)
                    except Exception as exc:
                        retire_failures.append(f"rank {rank} ({worker.endpoint}): {type(exc).__name__}")
            if retire_failures:
                detail = f"partial-launch cleanup could not retire {_bounded_retire_summary(retire_failures)}"
                logger.warning("native gang submission failed and %s", detail)
                _augment_error_detail(launch_error, detail)
            raise

    def status(self, remote_id: str) -> NativeGangWorkloadResult:
        """Aggregate typed rank observations into one durable gang state."""

        workers: list[_GangWorkerObservation] = []
        for rank, worker in enumerate(self._workers):
            try:
                observation = _normalize_worker_observation(
                    worker.executor.status(remote_id),
                    expected_remote_id=remote_id,
                    endpoint=worker.endpoint,
                )
            except httpx.TransportError as exc:
                observation = _lost_observation(
                    remote_id,
                    f"native worker {worker.endpoint} is unreachable: {type(exc).__name__}: {exc}",
                )
            except (httpx.HTTPStatusError, RuntimeError, ValueError) as exc:
                observation = _indeterminate_observation(
                    remote_id,
                    _invalid_observation_reason(exc, endpoint=worker.endpoint),
                )
            workers.append(
                _GangWorkerObservation(
                    rank=rank,
                    worker=worker,
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

    def stop(self, remote_id: str) -> None:
        """Stop every gang rank using its original cell-scoped fence."""

        self._fanout_mutation("stop", remote_id)

    def retire(self, remote_id: str) -> None:
        """Retire every gang rank using its original cell-scoped fence."""

        self._fanout_mutation("retire", remote_id)

    def _fanout_mutation(self, action: _MutationAction, remote_id: str) -> None:
        """Apply one closed mutation action to every worker."""

        first_error: BaseException | None = None
        for worker in self._workers:
            try:
                if action == "stop":
                    worker.executor.stop(remote_id, worker.context)
                elif action == "retire":
                    worker.executor.retire(remote_id, worker.context)
                else:
                    raise ValueError(f"unsupported native gang mutation action {action!r}")
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
        """Launch one checkpoint hook across every cell under a single shared ID.

        Two things differ from a single-cell slice and both are load-bearing.

        A gang holds one lease per cell, so every worker must be addressed with
        *its own* fence -- the shared ``context`` would be rejected by every
        worker but one. And each cell's context carries a different idempotency
        key, so passing them through unchanged would mint N different checkpoint
        IDs and leave nothing for the caller to poll: ``status`` asks each rank
        about one logical checkpoint. Each worker is therefore addressed with
        its own fence carrying the *shared* checkpoint ID, which is exactly the
        asymmetry ``submit`` already has between per-cell fences and one logical
        submission ID.

        Launch is all-or-nothing, mirroring ``submit``: a partial launch is
        retired -- each worker with its own context -- before the original error
        is raised, because a surviving hook would hold accelerators and could
        still write to the checkpoint destination. Completion is not asserted
        here; callers poll ``status``, which reports success only once every
        rank has succeeded.
        """

        if not isinstance(hook, str) or not hook.strip():
            raise ValueError("native gang checkpoint hook must be non-empty")
        checkpoint_id = context.idempotency_key
        if not isinstance(checkpoint_id, str) or not checkpoint_id.strip():
            raise ValueError("native gang checkpoint requires a shared idempotency key")
        attempted: list[tuple[int, _GangWorker, set[str]]] = []
        try:
            for rank, worker in enumerate(self._workers):
                remote_ids = {checkpoint_id}
                attempted.append((rank, worker, remote_ids))
                worker_context = replace(worker.context, idempotency_key=checkpoint_id)
                launched = worker.executor.checkpoint(
                    remote_id,
                    hook=hook,
                    output_uri=output_uri,
                    context=worker_context,
                )
                remote_ids.add(launched)
                if launched != checkpoint_id:
                    raise RuntimeError(
                        f"native gang worker returned unexpected checkpoint ID {launched!r}; expected {checkpoint_id!r}"
                    )
            return checkpoint_id
        except BaseException as launch_error:
            retire_failures: list[str] = []
            for rank, worker, remote_ids in reversed(attempted):
                for attempted_remote_id in sorted(remote_ids):
                    try:
                        # Each worker is retired under its own cell fence; the
                        # shared checkpoint ID names *what* to retire, never who
                        # is allowed to retire it.
                        worker.executor.retire(
                            attempted_remote_id,
                            replace(worker.context, idempotency_key=checkpoint_id),
                        )
                    except Exception as exc:
                        retire_failures.append(
                            f"rank {rank} cell {worker.cell_id} ({worker.endpoint}): {type(exc).__name__}"
                        )
            if retire_failures:
                detail = f"partial checkpoint cleanup could not retire {_bounded_retire_summary(retire_failures)}"
                logger.warning("native gang checkpoint failed and %s", detail)
                _augment_error_detail(launch_error, detail)
            raise

    def logs(self, remote_id: str) -> str:
        """Return rank-prefixed logs from every gang worker when available."""

        sections: list[str] = []
        for rank, worker in enumerate(self._workers):
            try:
                logs = worker.executor.logs(remote_id)
            except httpx.TransportError as exc:
                logs = f"native worker is unreachable: {type(exc).__name__}: {exc}"
            except (httpx.HTTPStatusError, RuntimeError, ValueError) as exc:
                logs = f"native worker log retrieval failed: {type(exc).__name__}: {exc}"
            sections.append(f"===== rank {rank} cell {worker.cell_id} {worker.endpoint} =====\n{logs}")
        return "\n".join(sections)

    def process_status(self, remote_id: str) -> JsonObject:
        """Return per-cell process detail across every gang member."""

        ranks: list[JsonValue] = []
        for rank, worker in enumerate(self._workers):
            try:
                detail: JsonValue = worker.executor.process_status(remote_id)
            except httpx.TransportError as exc:
                detail = {"observed": False, "detail": f"native worker is unreachable: {type(exc).__name__}: {exc}"}
            except (httpx.HTTPStatusError, RuntimeError, ValueError) as exc:
                detail = {"observed": False, "detail": f"process status failed: {type(exc).__name__}: {exc}"}
            ranks.append({"rank": rank, "cell_id": worker.cell_id, "endpoint": worker.endpoint, "status": detail})
        return {"remote_id": remote_id, "ranks": ranks}

    def close(self) -> None:
        """Close internally owned native-worker clients."""

        for worker in self._workers:
            worker.executor.close()


__all__ = ("NativeGangExecutor", "NativeGangMember")
