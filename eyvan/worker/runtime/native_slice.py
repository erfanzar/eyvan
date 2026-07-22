# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Coordinated native execution across every host in one TPU slice group."""

from __future__ import annotations

import copy
import logging
import typing as tp
from dataclasses import dataclass, replace
from urllib.parse import urlparse

import httpx
from pydantic import ValidationError

from eyvan._internal.compat import StrEnum
from eyvan._internal.json_types import JsonObject, JsonValue
from eyvan.capacity.drivers.base import (
    ExecutionWorkloadObservation,
    ExecutionWorkloadResult,
    ExecutionWorkloadStatus,
    MutationContext,
)
from eyvan.worker.distributed_environment import without_distributed_topology_overrides
from eyvan.worker.runtime._multihost_site import (
    DEFAULT_GANG_INITIALIZATION,
    GangInitializationPolicy,
    gang_formation_failure_reason,
    gang_initialization_outcome,
)
from eyvan.worker.runtime.log_records import (
    CURSOR_KIND_WORKLOAD_LOG,
    CURSOR_MODE_LIVE,
    CursorContext,
    CursorFresh,
    CursorResume,
    LogPage,
    decode_log_cursor,
    join_log_cursor,
    merge_pages,
)
from eyvan.worker.runtime.native_remote import NativeRemoteExecutor
from eyvan.worker.runtime.systemd_transient import _rank_checkpoint_uri

#: Where a resuming workload is told to read its checkpoint from. Must be
#: derived per rank exactly as the hook derives its write destination.
_RESUME_CHECKPOINT_ENV = "EYVAN_RESUME_CHECKPOINT_URI"

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


def _reported_exit_status(detail: JsonObject) -> int | None:
    """Return one host's reported process exit status, or ``None`` if unknown.

    ``None`` covers three different things -- the host was not observed, the
    unit is gone, the payload predates the field -- and all three mean the same
    here: this rank offers no evidence either way, which is not the same as
    evidence that it exited cleanly.
    """

    if detail.get("observed") is not True:
        return None
    status = detail.get("exec_main_status")
    return status if isinstance(status, int) and not isinstance(status, bool) else None


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
        initialization: GangInitializationPolicy = DEFAULT_GANG_INITIALIZATION,
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
        if not isinstance(initialization, GangInitializationPolicy):
            raise TypeError("native slice initialization must be a GangInitializationPolicy")
        self.endpoints = normalized
        self.jax_cluster_detection = jax_cluster_detection
        self.initialization = initialization
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
        # The checkpoint hook writes to ``{base}/rank-{n}``, so a resume handed
        # the bare base URI reads an object that was never written. It does not
        # fail: the workload sees "no checkpoint" and restarts from scratch,
        # which on a live v5p-16 silently discarded 252 completed training
        # steps after a successful preemption and checkpoint. The two paths
        # have to agree, so derive the resume URI the same way.
        resume_uri = environment.get(_RESUME_CHECKPOINT_ENV, "")
        if resume_uri:
            environment[_RESUME_CHECKPOINT_ENV] = _rank_checkpoint_uri(resume_uri, rank)
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
        # Last, and after the topology scrub above, so the deadline a cold slice
        # gets is the one this control plane configured. A caller who submits
        # its own copy has already had it stripped -- the whole ``EYVAN_JAX_``
        # prefix is Eyvan's -- and could otherwise hand every rank a one-second
        # barrier and blame the fleet.
        environment.update(self.initialization.environment())
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
        status = _aggregate_status(workers)
        message = _aggregate_message(workers)
        if status is ExecutionWorkloadStatus.FAILED:
            formation = self._gang_formation_failure(remote_id)
            if formation is not None:
                message = f"{formation} {message}"
        aggregate = ExecutionWorkloadObservation(
            remote_id=remote_id,
            status=status,
            message=message,
        )
        return {
            "remote_id": aggregate.remote_id,
            "status": aggregate.status,
            "message": aggregate.message,
            "workers": [worker.as_result() for worker in workers],
        }

    def _gang_formation_failure(self, remote_id: str) -> str | None:
        """Return the terminal reason when this slice's gang never formed.

        A slice is one cell, but its hosts join the same JAX registration
        barrier a multi-cell gang does, so a cold start loses it the same way
        and the operator needs the same distinction: lengthen the deadline and
        resubmit, or go and read the user's traceback. The evidence is the exit
        status Eyvan's own initialization hook gives a rank that never joined,
        which each host keeps in its durable execution record -- so this is
        re-derived on every poll from remote state and reads identically after a
        controller restart.

        ``None`` means no rank offered that evidence, which is deliberately not
        "the gang formed": an unreachable host, a reaped unit, and an ordinary
        user-code failure all land here.
        """

        if not self.initialization.fatal:
            # Nothing can carry the evidence: without the fatal hook a rank that
            # never joined is left to run on, and its exit status is user code's.
            return None
        deadline_ranks: list[int] = []
        error_ranks: list[int] = []
        for rank, worker in enumerate(self._workers):
            try:
                detail = worker.process_status(remote_id)
            except (httpx.HTTPError, RuntimeError, ValueError):
                continue
            outcome = gang_initialization_outcome(_reported_exit_status(detail))
            if outcome == "deadline":
                deadline_ranks.append(rank)
            elif outcome == "error":
                error_ranks.append(rank)
        if not deadline_ranks and not error_ranks:
            return None
        return gang_formation_failure_reason(
            deadline_s=self.initialization.initialization_timeout_s,
            deadline_ranks=deadline_ranks,
            error_ranks=error_ranks,
        )

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

    def log_page(self, remote_id: str, *, after: str = "", limit: int = 200) -> LogPage:
        """Return one structured page covering every host in this slice.

        The control plane fans out across *cells*, while this layer exposes
        every host's local rank inside one cell. The controller offsets those
        local ranks into the workload's deterministic global rank space; without
        this merge a v5p-16 would report only whichever host answered first.

        Each host resumes from its own position. A single cursor is meaningless
        to a journal that did not issue it, which is why `merge_pages` refuses to
        invent one, and the same per-rank encoding the control plane uses is
        reused rather than respelled.

        An unreachable host is reported *in place* via `truncated_ranks` rather
        than dropped: a fan-out that silently returned the hosts it could reach
        would show a complete-looking log for a half-dead slice, which is the
        failure this whole aggregation exists to prevent.

        A cursor this slice cannot resolve is answered as an unavailable page
        naming the codec's refusal, not by silently restarting every host. The
        two are indistinguishable to the control plane, and the silent one
        replays the whole slice's journal while reporting a clean read.
        """

        expected = CursorContext(
            kind=CURSOR_KIND_WORKLOAD_LOG,
            mode=CURSOR_MODE_LIVE,
            ranks=tuple(range(len(self._workers))),
        )
        outcome = decode_log_cursor(after, expected=expected)
        if not isinstance(outcome, (CursorFresh, CursorResume)):
            return LogPage(
                unavailable=True,
                detail=f"{outcome.reason}: {outcome.detail}",
                truncated_ranks=tuple(range(len(self._workers))),
            )
        _attempt = outcome.attempt if isinstance(outcome, CursorResume) else None
        cursors = outcome.cursors if isinstance(outcome, CursorResume) else {}
        pages: dict[int, LogPage] = {}
        next_cursors: dict[int, str] = {}
        for rank, worker in enumerate(self._workers):
            try:
                page = worker.log_page(remote_id, after=cursors.get(rank, ""), limit=limit)
            except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                page = LogPage(unavailable=True, detail=f"{type(exc).__name__}: {exc}")
            pages[rank] = LogPage(
                records=tuple(replace(record, rank=rank) for record in page.records),
                next_cursor=page.next_cursor,
                unavailable=page.unavailable,
                detail=page.detail,
                truncated_ranks=(rank,) if page.truncated_ranks else (),
            )
            # A backend legitimately returns no cursor when this poll produced
            # no new lines. Keep the position it was asked from; dropping it
            # would restart that host on the next outer poll and duplicate its
            # tail. The same applies while a host is temporarily unreachable.
            advanced = cursors.get(rank, "") if page.unavailable else (page.next_cursor or cursors.get(rank, ""))
            if advanced:
                next_cursors[rank] = advanced
        merged = merge_pages(pages)
        # ``merge_pages`` deliberately returns no cursor; the slice-level one is
        # the per-host positions joined, which is the only thing that can resume
        # every host.
        return LogPage(
            records=merged.records,
            next_cursor=join_log_cursor(_attempt or 0, next_cursors),
            unavailable=merged.unavailable,
            detail=merged.detail,
            truncated_ranks=merged.truncated_ranks,
        )

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
