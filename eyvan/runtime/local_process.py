# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Persistent single-host execution backend for Eyvan development cells."""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import typing as tp
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import RLock

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic import JsonValue as PydanticJsonValue

from eyvan._compat import UTC
from eyvan._json import JsonObject, JsonValue, is_json_object, model_json
from eyvan.drivers.base import (
    ExecutionWorkloadObservation,
    ExecutionWorkloadStatus,
    MutationContext,
    StaleMutationError,
)

_TERMINAL_STATES = frozenset(
    {
        ExecutionWorkloadStatus.SUCCEEDED,
        ExecutionWorkloadStatus.FAILED,
        ExecutionWorkloadStatus.STOPPED,
        ExecutionWorkloadStatus.LOST,
    }
)
_STARTUP_GRACE_S = 10.0
_OWNER_ABSENCE_GRACE_S = 1.0


class _LocalExecutionRecord(BaseModel):
    """Complete durable record written for a newly submitted local workload."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    remote_id: str
    status: ExecutionWorkloadStatus
    message: str
    generation: int = Field(ge=0, strict=True)
    fencing_token: str
    idempotency_key: str
    entrypoint: str
    environment: dict[str, str]
    metadata: dict[str, PydanticJsonValue]
    worker_pid: int | None
    worker_start_marker: str | None
    worker_token: str
    stop_requested: bool
    exit_code: int | None
    created_at: str
    updated_at: str
    started_at: str | None
    finished_at: str | None
    log_path: str
    owner_path: str


class _LogicalWorkloadFence(BaseModel):
    """Strict durable high watermark for one logical workload."""

    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)

    generation: int = Field(ge=0)
    fencing_token: str
    remote_id: str


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


def _default_state_dir() -> Path:
    configured = os.getenv("EYVAN_LOCAL_RUNTIME_DIR")
    if configured:
        return Path(configured).expanduser()
    agent_state = os.getenv("EYVAN_AGENT_STATE_DIR")
    if agent_state:
        return Path(agent_state).expanduser() / "runtime"
    return Path.home() / ".local" / "state" / "eyvan" / "runtime"


def _environment_float(name: str, *, default: float) -> float:
    """Parse a numeric environment override, failing fast on a malformed value."""

    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a finite numeric value, got {raw!r}") from None
    if not math.isfinite(value):
        raise ValueError(f"{name} must be a finite numeric value, got {raw!r}")
    return value


def _record_name(remote_id: str) -> str:
    return hashlib.sha256(remote_id.encode()).hexdigest()


def _atomic_write_json(path: Path, value: tp.Mapping[str, JsonValue]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _load_json(path: Path) -> JsonObject | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    if not is_json_object(payload):
        raise RuntimeError(f"local execution record {path} is not a strict JSON object")
    return payload


@contextmanager
def _file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _record_lock_path(record_path: Path) -> Path:
    return record_path.with_suffix(".lock")


def _read_record(record_path: Path) -> JsonObject | None:
    with _file_lock(_record_lock_path(record_path)):
        return _load_json(record_path)


def _mutate_record(
    record_path: Path,
    mutation: tp.Callable[[JsonObject], None],
) -> JsonObject | None:
    with _file_lock(_record_lock_path(record_path)):
        record = _load_json(record_path)
        if record is None:
            return None
        mutation(record)
        record["updated_at"] = _utcnow()
        _atomic_write_json(record_path, record)
        return record


def _persisted_status(
    remote_id: str,
    record: tp.Mapping[str, JsonValue],
) -> ExecutionWorkloadStatus:
    """Parse one required durable status without inventing a fallback."""

    try:
        raw_status = record["status"]
    except KeyError as exc:
        raise RuntimeError(f"local execution {remote_id!r} has no persisted status") from exc
    if not isinstance(raw_status, str):
        raise RuntimeError(f"local execution {remote_id!r} has a non-string persisted status")
    try:
        return ExecutionWorkloadStatus(raw_status)
    except ValueError as exc:
        raise RuntimeError(f"local execution {remote_id!r} has unsupported persisted status {raw_status!r}") from exc


def _persisted_message(remote_id: str, record: tp.Mapping[str, JsonValue]) -> str:
    """Read an optional durable status message without coercing its type."""

    try:
        message = record["message"]
    except KeyError:
        return ""
    if not isinstance(message, str):
        raise RuntimeError(f"local execution {remote_id!r} has a non-string persisted message")
    return message


def _owner_active(owner_path: Path) -> bool:
    owner_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(owner_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


def _process_start_marker(pid: int) -> str | None:
    """Return a stable best-effort marker for a live process identity.

    The durable worker record can outlive the process that created it. A raw
    PID is therefore never sufficient authority to signal a process group.
    Linux exposes a monotonic start tick in ``/proc``; other POSIX hosts use
    the portable ``ps`` start timestamp as a conservative fallback.
    """

    if pid <= 0:
        return None
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        payload = stat_path.read_text(encoding="utf-8")
        _prefix, suffix = payload.rsplit(")", 1)
        fields = suffix.split()
        # ``suffix`` starts at /proc stat field 3; field 22 is starttime.
        return f"proc:{fields[19]}"
    except (FileNotFoundError, IndexError, OSError, ValueError):
        pass
    try:
        result = subprocess.run(
            ("ps", "-o", "lstart=", "-p", str(pid)),
            check=False,
            capture_output=True,
            text=True,
            timeout=1,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    marker = result.stdout.strip()
    return f"ps:{marker}" if result.returncode == 0 and marker else None


def _process_has_worker_token(pid: int, worker_token: str) -> bool:
    """Check that a live process still carries the worker launch nonce."""

    if pid <= 0 or not worker_token:
        return False
    expected = worker_token.encode()
    try:
        arguments = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        return expected in arguments
    except OSError:
        pass
    try:
        result = subprocess.run(
            ("ps", "-ww", "-o", "command=", "-p", str(pid)),
            check=False,
            capture_output=True,
            text=True,
            timeout=1,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0 and worker_token in result.stdout


def _unreaped_process_is_running(pid: int) -> bool:
    """Return whether a direct child is still live without reaping it.

    The live executor needs an unreaped child to prove that its process-group
    ID cannot have been recycled. A transiently unavailable owner lock is not
    enough evidence to terminate that child or its descendants.
    """

    if pid <= 0:
        return False
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        payload = stat_path.read_text(encoding="utf-8")
        _prefix, suffix = payload.rsplit(")", 1)
        state = suffix.split(maxsplit=1)[0]
        return state != "Z"
    except (FileNotFoundError, IndexError, OSError, ValueError):
        pass
    try:
        result = subprocess.run(
            ("ps", "-o", "stat=", "-p", str(pid)),
            check=False,
            capture_output=True,
            text=True,
            timeout=1,
        )
    except (OSError, subprocess.SubprocessError):
        # Fail open: the caller still owns this unreaped child, and uncertain
        # liveness must not cause a process-group signal.
        return True
    state = result.stdout.strip()
    return result.returncode == 0 and bool(state) and not state.startswith("Z")


def _tracked_worker_process_group(record: tp.Mapping[str, JsonValue]) -> int | None:
    """Return the worker process group only when its persisted identity matches."""

    worker_pid = record.get("worker_pid")
    marker = record.get("worker_start_marker")
    worker_token = record.get("worker_token")
    if (
        not isinstance(worker_pid, int)
        or worker_pid <= 0
        or not isinstance(marker, str)
        or not marker
        or not isinstance(worker_token, str)
        or not worker_token
    ):
        return None
    if _process_start_marker(worker_pid) != marker or not _process_has_worker_token(worker_pid, worker_token):
        return None
    try:
        return worker_pid if os.getpgid(worker_pid) == worker_pid and os.getsid(worker_pid) == worker_pid else None
    except (PermissionError, ProcessLookupError):
        return None


def _terminate_process_group(process_group: int, *, grace_s: float = 0.25) -> None:
    """Best-effort cleanup for commands orphaned by a crashed worker."""

    if process_group <= 0:
        return
    try:
        os.killpg(process_group, signal.SIGTERM)
    except (PermissionError, ProcessLookupError):
        return
    deadline = time.monotonic() + grace_s
    while time.monotonic() < deadline:
        try:
            os.killpg(process_group, 0)
        except (PermissionError, ProcessLookupError):
            return
        time.sleep(0.01)
    try:
        os.killpg(process_group, signal.SIGKILL)
    except (PermissionError, ProcessLookupError):
        pass


def _terminate_tracked_worker(record: tp.Mapping[str, JsonValue], *, grace_s: float = 0.25) -> bool:
    """Terminate a worker group only after proving its PID was not recycled."""

    process_group = _tracked_worker_process_group(record)
    if process_group is None:
        return False
    _terminate_process_group(process_group, grace_s=grace_s)
    return True


def _signal_tracked_worker(record: tp.Mapping[str, JsonValue], signal_number: int) -> bool:
    """Send one signal to the verified detached worker process group."""

    process_group = _tracked_worker_process_group(record)
    if process_group is None:
        return False
    try:
        os.killpg(process_group, signal_number)
    except (PermissionError, ProcessLookupError):
        return False
    return True


def _environment(spec: tp.Mapping[str, JsonValue]) -> dict[str, str]:
    runtime_env = spec.get("runtime_env") or {}
    if not isinstance(runtime_env, tp.Mapping):
        raise ValueError("native-local runtime_env must be a mapping")
    unsupported = sorted(str(key) for key in runtime_env if key != "env_vars")
    if unsupported:
        raise ValueError(f"native-local execution does not support runtime_env field {unsupported[0]!r}")
    legacy = runtime_env.get("env_vars") or {}
    neutral = spec.get("environment") or {}
    if not isinstance(legacy, tp.Mapping) or not isinstance(neutral, tp.Mapping):
        raise ValueError("native-local environment variables must be a mapping")
    result: dict[str, str] = {}
    for raw_name, raw_value in (*legacy.items(), *neutral.items()):
        if not isinstance(raw_name, str) or not raw_name or raw_name != raw_name.strip():
            raise ValueError("native-local environment variable names must be non-empty strings")
        if not isinstance(raw_value, str):
            raise ValueError(f"native-local environment variable {raw_name!r} must have a string value")
        result[raw_name] = raw_value
    result.setdefault("EYVAN_REPLICA_INDEX", "0")
    result.setdefault("EYVAN_REPLICA_COUNT", "1")
    return result


def _validate_gang(spec: tp.Mapping[str, JsonValue]) -> None:
    gang = spec.get("gang")
    if not isinstance(gang, tp.Mapping):
        raise ValueError("native-local workloads require a gang resource contract")
    replicas = gang.get("replicas")
    if isinstance(replicas, bool) or not isinstance(replicas, int):
        raise ValueError("native-local gang replicas must be an integer")
    if replicas != 1:
        raise ValueError("native-local execution supports exactly one replica")
    if not isinstance(gang.get("per_replica"), tp.Mapping):
        raise ValueError("native-local gang per_replica resources must be a mapping")
    topology = gang.get("required_topology") or {}
    if not isinstance(topology, tp.Mapping):
        raise ValueError("native-local required topology must be a mapping")
    if topology:
        raise ValueError("native-local execution does not support topology constraints")


class LocalProcessExecutor:
    """Run fenced workloads as durable, detached processes on one host.

    This backend is intended for development and control-plane integration. It
    does not enforce CPU or memory limits and deliberately supports only a
    single replica.
    """

    def __init__(self, state_dir: str | os.PathLike[str] | None = None) -> None:
        self.state_dir = Path(state_dir).expanduser() if state_dir is not None else _default_state_dir()
        self.jobs_dir = self.state_dir / "jobs"
        self.logs_dir = self.state_dir / "logs"
        self.owners_dir = self.state_dir / "owners"
        self.workloads_dir = self.state_dir / "workloads"
        for directory in (self.state_dir, self.jobs_dir, self.logs_dir, self.owners_dir, self.workloads_dir):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            directory.chmod(0o700)
        self._lock = RLock()
        # Retaining direct children until terminal state lets this live worker
        # safely clean a crashed child's process group: an unreaped child PID
        # cannot be recycled. Reconstructed executors fail closed because they
        # do not have that ownership proof.
        self._worker_processes: dict[str, subprocess.Popen[bytes]] = {}

    def _paths(self, remote_id: str) -> tuple[Path, Path, Path]:
        name = _record_name(remote_id)
        return self.jobs_dir / f"{name}.json", self.logs_dir / f"{name}.log", self.owners_dir / f"{name}.lock"

    def _workload_fence_path(self, workload_id: str) -> Path:
        return self.workloads_dir / f"{_record_name(workload_id)}.json"

    def _release_worker_process(self, remote_id: str) -> None:
        process = self._worker_processes.pop(remote_id, None)
        if process is not None:
            process.poll()

    def _owned_unreaped_worker(
        self,
        remote_id: str,
        record: tp.Mapping[str, JsonValue],
    ) -> subprocess.Popen[bytes] | None:
        process = self._worker_processes.get(remote_id)
        worker_pid = record.get("worker_pid")
        if process is None or not isinstance(worker_pid, int) or process.pid != worker_pid:
            return None
        return process

    @staticmethod
    def _logical_workload_id(metadata: tp.Mapping[str, JsonValue]) -> str | None:
        value = metadata.get("eyvan_workload_id")
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("native-local eyvan_workload_id must be a non-empty string")
        return value

    def _claim_logical_workload(
        self,
        workload_id: str,
        remote_id: str,
        context: MutationContext,
    ) -> None:
        """Fence one logical workload before its attempt process is launched.

        The cell-agent normally prevents an older attempt from reaching this
        worker. Persisting the same high watermark at the execution boundary
        closes the gap for delayed or directly replayed HTTP submissions.
        """

        path = self._workload_fence_path(workload_id)
        with _file_lock(_record_lock_path(path)):
            current = _load_json(path)
            predecessor: _LogicalWorkloadFence | None = None
            if current is not None:
                try:
                    persisted = _LogicalWorkloadFence.model_validate(current)
                except ValidationError as exc:
                    raise StaleMutationError(f"logical workload {workload_id!r} has an invalid persisted fence") from exc
                if context.generation < persisted.generation:
                    raise StaleMutationError(
                        f"generation {context.generation} is older than logical workload {workload_id!r} "
                        f"generation {persisted.generation}"
                    )
                if context.generation == persisted.generation:
                    if persisted.fencing_token != context.fencing_token:
                        raise StaleMutationError(
                            f"fencing token does not own logical workload {workload_id!r} "
                            f"generation {persisted.generation}"
                        )
                    if persisted.remote_id != remote_id:
                        raise StaleMutationError(
                            f"submission {remote_id!r} does not own logical workload {workload_id!r} "
                            f"generation {persisted.generation}"
                        )
                    return
                predecessor = persisted

            now = _utcnow()
            if predecessor is not None:
                self.retire(
                    predecessor.remote_id,
                    MutationContext(
                        idempotency_key=f"supersede:{workload_id}:{context.generation}",
                        generation=predecessor.generation,
                        fencing_token=predecessor.fencing_token,
                    ),
                )
            _atomic_write_json(
                path,
                {
                    "workload_id": workload_id,
                    "generation": context.generation,
                    "fencing_token": context.fencing_token,
                    "remote_id": remote_id,
                    **({"predecessor": model_json(predecessor)} if predecessor is not None else {}),
                    "updated_at": now,
                },
            )

    @staticmethod
    def _validate_fence(
        remote_id: str,
        record: tp.Mapping[str, JsonValue],
        context: MutationContext,
    ) -> None:
        generation = record.get("generation")
        if isinstance(generation, bool) or not isinstance(generation, int):
            raise StaleMutationError(f"local execution {remote_id!r} has an invalid persisted generation")
        token = record.get("fencing_token")
        if generation != context.generation:
            raise StaleMutationError(
                f"generation {context.generation} does not own local execution {remote_id!r} generation {generation}"
            )
        if not isinstance(token, str) or token != context.fencing_token:
            raise StaleMutationError(f"fencing token does not own local execution {remote_id!r} generation {generation}")

    def submit(self, spec: tp.Mapping[str, JsonValue], context: MutationContext) -> str:
        """Launch one detached process, or replay an owned submission."""

        entrypoint = spec.get("entrypoint")
        if not isinstance(entrypoint, str) or not entrypoint.strip():
            raise ValueError("native-local workloads require a non-empty entrypoint")
        _validate_gang(spec)
        environment = _environment(spec)
        submitted_remote_id = spec.get("submission_id")
        if submitted_remote_id is None:
            remote_id = context.idempotency_key.strip()
        elif isinstance(submitted_remote_id, str):
            remote_id = submitted_remote_id.strip()
        else:
            raise ValueError("native-local submission ID must be a string")
        if not remote_id:
            raise ValueError("native-local submission ID must be non-empty")
        metadata = spec.get("metadata") or {}
        if not is_json_object(metadata):
            raise ValueError("native-local workload metadata must be a JSON object")
        workload_id = self._logical_workload_id(metadata)

        record_path, log_path, owner_path = self._paths(remote_id)
        with self._lock:
            # Reject a generation that reuses an existing attempt ID before
            # advancing the logical high watermark. Otherwise a rejected
            # request could fence out the valid running attempt.
            with _file_lock(_record_lock_path(record_path)):
                existing = _load_json(record_path)
                if existing is not None:
                    self._validate_fence(remote_id, existing, context)
            if workload_id is not None:
                self._claim_logical_workload(workload_id, remote_id, context)
            with _file_lock(_record_lock_path(record_path)):
                existing = _load_json(record_path)
                if existing is not None:
                    self._validate_fence(remote_id, existing, context)
                    return remote_id
                now = _utcnow()
                new_record = _LocalExecutionRecord(
                    remote_id=remote_id,
                    status=ExecutionWorkloadStatus.PENDING,
                    message="waiting for native-local worker",
                    generation=context.generation,
                    fencing_token=context.fencing_token,
                    idempotency_key=context.idempotency_key,
                    entrypoint=entrypoint,
                    environment=environment,
                    metadata=metadata,
                    worker_pid=None,
                    worker_start_marker=None,
                    worker_token=secrets.token_urlsafe(24),
                    stop_requested=False,
                    exit_code=None,
                    created_at=now,
                    updated_at=now,
                    started_at=None,
                    finished_at=None,
                    log_path=str(log_path),
                    owner_path=str(owner_path),
                )
                record = model_json(new_record)
                _atomic_write_json(record_path, record)
                # The detached worker only needs the persisted record. Keep
                # agent and native-worker credentials out of its process tree.
                worker_env = {
                    **{key: value for key, value in os.environ.items() if not key.startswith("EYVAN_")},
                    "PYTHONUNBUFFERED": "1",
                }
                try:
                    process = subprocess.Popen(
                        (
                            sys.executable,
                            "-m",
                            "eyvan.runtime.local_worker",
                            "--record",
                            str(record_path),
                            "--log",
                            str(log_path),
                            "--owner",
                            str(owner_path),
                            f"--worker-token={new_record.worker_token}",
                        ),
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        close_fds=True,
                        start_new_session=True,
                        env=worker_env,
                    )
                except BaseException as exc:
                    record.update(
                        {
                            "status": ExecutionWorkloadStatus.FAILED.value,
                            "message": f"failed to launch native-local worker: {exc}",
                            "finished_at": _utcnow(),
                            "updated_at": _utcnow(),
                        }
                    )
                    _atomic_write_json(record_path, record)
                    raise
                record["worker_pid"] = process.pid
                record["worker_start_marker"] = _process_start_marker(process.pid)
                record["updated_at"] = _utcnow()
                _atomic_write_json(record_path, record)
                self._worker_processes[remote_id] = process

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if _owner_active(owner_path):
                break
            current = _read_record(record_path)
            if current is not None and _persisted_status(remote_id, current) in _TERMINAL_STATES:
                break
            time.sleep(0.01)
        return remote_id

    def _normalize_abandoned(
        self,
        remote_id: str,
        record_path: Path,
        owner_path: Path,
    ) -> JsonObject | None:
        record = _read_record(record_path)
        if record is None:
            return None
        status = _persisted_status(remote_id, record)
        if status in _TERMINAL_STATES:
            self._release_worker_process(remote_id)
            return record
        if _owner_active(owner_path):
            return record
        owned_worker = self._owned_unreaped_worker(remote_id, record)
        # ``local_worker`` records RUNNING before it starts the workload child.
        # A direct worker that has already exited while the durable record is
        # still PENDING therefore cannot have left a command behind. Report
        # that as a launch failure immediately instead of waiting for the
        # absence grace and describing an unstarted workload as LOST.
        if status is ExecutionWorkloadStatus.PENDING and owned_worker is not None:
            exit_code = owned_worker.poll()
            if exit_code is not None:

                def launch_failed(current: JsonObject) -> None:
                    if (
                        _persisted_status(remote_id, current) is not ExecutionWorkloadStatus.PENDING
                        or current.get("started_at") is not None
                    ):
                        return
                    current["status"] = ExecutionWorkloadStatus.FAILED.value
                    current["message"] = f"native-local worker exited before recording readiness (exit code {exit_code})"
                    current["exit_code"] = exit_code
                    current["finished_at"] = _utcnow()

                finalized = _mutate_record(record_path, launch_failed) or record
                if _persisted_status(remote_id, finalized) in _TERMINAL_STATES:
                    self._worker_processes.pop(remote_id, None)
                return finalized
        try:
            owner_absent_at = datetime.fromisoformat(str(record.get("owner_absent_at")))
            owner_absence_s = (datetime.now(UTC) - owner_absent_at).total_seconds()
        except (TypeError, ValueError):
            owner_absence_s = 0
        if owner_absence_s < _OWNER_ABSENCE_GRACE_S:

            def mark_owner_absent(current: JsonObject) -> None:
                current.setdefault("owner_absent_at", _utcnow())

            return _mutate_record(record_path, mark_owner_absent) or record
        try:
            created = datetime.fromisoformat(str(record["created_at"]))
            age = (datetime.now(UTC) - created).total_seconds()
        except (KeyError, TypeError, ValueError):
            age = _STARTUP_GRACE_S
        if status is ExecutionWorkloadStatus.PENDING and age < _STARTUP_GRACE_S:
            return record
        if owned_worker is not None and _unreaped_process_is_running(owned_worker.pid):
            # The direct worker remains live. Its advisory ownership lock can
            # briefly disappear around startup or process scheduling, but that
            # is not authority to kill an active process group.
            return record
        if owned_worker is not None:
            # This executor still owns an unreaped direct child, so its PID
            # cannot have been recycled. It is safe to clean descendants.
            _terminate_process_group(owned_worker.pid)
            try:
                owned_worker.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
            self._worker_processes.pop(remote_id, None)

        def abandon(current: JsonObject) -> None:
            if _persisted_status(remote_id, current) in _TERMINAL_STATES:
                return
            if owned_worker is not None:
                current["status"] = (
                    ExecutionWorkloadStatus.STOPPED.value
                    if bool(current.get("stop_requested"))
                    else ExecutionWorkloadStatus.LOST.value
                )
                current["message"] = (
                    "native-local worker stopped"
                    if bool(current.get("stop_requested"))
                    else "native-local worker crashed"
                )
            else:
                current["status"] = ExecutionWorkloadStatus.INDETERMINATE.value
                current["message"] = "native-local worker ownership is ambiguous; workload was not signalled"
            current["finished_at"] = _utcnow()

        return _mutate_record(record_path, abandon) or record

    def status(self, remote_id: str) -> ExecutionWorkloadObservation:
        """Return a canonical status derived from durable worker state."""

        record_path, _log_path, owner_path = self._paths(remote_id)
        record = self._normalize_abandoned(remote_id, record_path, owner_path)
        if record is None:
            return ExecutionWorkloadObservation(
                remote_id=remote_id,
                status=ExecutionWorkloadStatus.NOT_FOUND,
                message="local execution is absent",
            )
        status = _persisted_status(remote_id, record)
        if status in _TERMINAL_STATES:
            self._release_worker_process(remote_id)
        return ExecutionWorkloadObservation(
            remote_id=remote_id,
            status=status,
            message=_persisted_message(remote_id, record),
        )

    def stop(self, remote_id: str, context: MutationContext) -> None:
        """Request termination of the worker's detached process group."""

        record_path, _log_path, owner_path = self._paths(remote_id)
        with self._lock:
            record = _read_record(record_path)
            if record is None:
                return
            self._validate_fence(remote_id, record, context)
            if _persisted_status(remote_id, record) in _TERMINAL_STATES:
                return

            def request_stop(current: JsonObject) -> None:
                current["stop_requested"] = True
                current["message"] = "native-local stop requested"

            record = _mutate_record(record_path, request_stop) or record
            if _owner_active(owner_path):
                _signal_tracked_worker(record, signal.SIGTERM)

    def retire(self, remote_id: str, context: MutationContext) -> None:
        """Stop a superseded execution and wait until its process group exits."""

        record_path, _log_path, owner_path = self._paths(remote_id)
        record = _read_record(record_path)
        if record is None:
            return
        self._validate_fence(remote_id, record, context)
        self.stop(remote_id, context)
        timeout_s = max(_environment_float("EYVAN_SUPERSEDE_WAIT_TIMEOUT_S", default=300.0), 0.1)
        poll_s = max(_environment_float("EYVAN_SUPERSEDE_POLL_S", default=0.1), 0.01)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            status = self.status(remote_id).status
            if status in _TERMINAL_STATES:
                return
            if status is ExecutionWorkloadStatus.INDETERMINATE:
                raise RuntimeError(f"cannot safely retire ambiguous native-local execution {remote_id!r}")
            time.sleep(poll_s)
        current = _read_record(record_path)
        if current is not None and _owner_active(owner_path):
            _signal_tracked_worker(current, signal.SIGKILL)

        def ambiguous(value: JsonObject) -> None:
            if _persisted_status(remote_id, value) in _TERMINAL_STATES:
                return
            value["stop_requested"] = True
            value["status"] = ExecutionWorkloadStatus.INDETERMINATE.value
            value["message"] = "native-local retirement timed out; workload ownership is ambiguous"
            value["finished_at"] = _utcnow()

        _mutate_record(record_path, ambiguous)
        raise RuntimeError(f"cannot safely retire native-local execution {remote_id!r} before its timeout")

    def checkpoint(
        self,
        remote_id: str,
        *,
        hook: str,
        output_uri: str | None,
        context: MutationContext,
    ) -> str:
        """Launch an external checkpoint hook after validating source ownership."""

        if not hook or not hook.strip():
            raise ValueError("native-local checkpoint hook must be non-empty")
        source_path, _log_path, _owner_path = self._paths(remote_id)
        source = _read_record(source_path)
        if source is None:
            raise RuntimeError(f"local execution {remote_id!r} is absent")
        self._validate_fence(remote_id, source, context)
        return self.submit(
            {
                "entrypoint": hook,
                "submission_id": context.idempotency_key,
                "environment": {
                    "EYVAN_SOURCE_JOB_ID": remote_id,
                    "EYVAN_CHECKPOINT_URI": output_uri or "",
                },
                "gang": {"replicas": 1, "per_replica": {"cpu": 1}},
                "metadata": {"eyvan_operation": "checkpoint", "eyvan_source_job_id": remote_id},
            },
            context,
        )

    def logs(self, remote_id: str) -> str:
        """Return the durable combined stdout/stderr stream."""

        _record_path, log_path, _owner_path = self._paths(remote_id)
        try:
            return log_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return ""


__all__ = ("LocalProcessExecutor",)
