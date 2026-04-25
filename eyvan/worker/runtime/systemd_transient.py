# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Native systemd transient-service executor for single-host Eyvan cells.

Each Eyvan submission receives its own deterministic transient service unit.
Systemd owns the process cgroup, so stopping a workload also stops background
children created by its shell entrypoint. Durable records retain the fencing
contract and prevent a missing unit from being mistaken for a successful run.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
import typing as tp
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from eyvan._internal.compat import UTC
from eyvan._internal.json_types import JsonObject, JsonValue, is_json_object
from eyvan.capacity.drivers.base import (
    ExecutionWorkloadObservation,
    ExecutionWorkloadStatus,
    MutationContext,
    StaleMutationError,
)
from eyvan.worker.distributed_environment import without_distributed_topology_overrides

_TERMINAL_STATES = frozenset(
    {
        ExecutionWorkloadStatus.SUCCEEDED,
        ExecutionWorkloadStatus.FAILED,
        ExecutionWorkloadStatus.STOPPED,
        ExecutionWorkloadStatus.LOST,
    }
)
_FINAL_STATES = _TERMINAL_STATES | {ExecutionWorkloadStatus.INDETERMINATE}
_COLLECTABLE_STATES = frozenset(
    {
        ExecutionWorkloadStatus.SUCCEEDED,
        ExecutionWorkloadStatus.FAILED,
        ExecutionWorkloadStatus.STOPPED,
    }
)
_DURABLE_STATES = _FINAL_STATES | {ExecutionWorkloadStatus.PENDING, ExecutionWorkloadStatus.RUNNING}
_RECOVERABLE_CGROUP_INDETERMINATE = "cannot observe systemd workload cgroup before completion"

# Indeterminacy that a later observation can genuinely resolve. INDETERMINATE is
# otherwise latched as final, which is right when the record itself is unusable
# but wrong when the runtime merely could not be reached at one instant: a stop
# that failed leaves a workload whose unit is still observable, and latching then
# makes the first transient failure permanent and strands the accelerator.
_RECOVERABLE_INDETERMINATE_FIELD = "indeterminate_recoverable"


def _is_recoverable_indeterminate(record: tp.Mapping[str, JsonValue]) -> bool:
    """Return whether a later observation may still resolve this indeterminacy.

    Recorded as an explicit field rather than inferred from the message, because
    ``stop`` rewrites ``message`` while leaving the status in place -- a text
    comparison silently stops matching at exactly the moment it is needed.
    """

    if record.get(_RECOVERABLE_INDETERMINATE_FIELD) is True:
        return True
    return record.get("message") == _RECOVERABLE_CGROUP_INDETERMINATE


_UNIT_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_SYSTEMD_IDENTITY = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*\Z")


@dataclass(frozen=True, slots=True)
class SystemdCommandResult:
    """Result returned by the small systemd command-runner seam."""

    returncode: int
    stdout: str = ""
    stderr: str = ""


SystemdCommandRunner = tp.Callable[[tuple[str, ...]], SystemdCommandResult]
CgroupProcessesReader = tp.Callable[[str], bool | None]
CgroupPidsReader = tp.Callable[[str], tuple[int, ...] | None]


class SystemdLogsUnavailableError(RuntimeError):
    """journalctl could not read a workload's journal output.

    A genuinely silent workload returns an empty string from ``logs()``; this
    error keeps a journal transport failure distinguishable from that case.
    """

    def __init__(self, unit_name: str, stderr: str) -> None:
        self.unit_name = unit_name
        self.stderr = stderr
        detail = stderr or "journalctl failed"
        super().__init__(f"cannot read systemd journal for unit {unit_name!r}: {detail}")


class _InvalidDurableRecord(ValueError):
    """A persisted systemd record cannot safely describe a workload."""


@dataclass(frozen=True, slots=True)
class _LogicalWorkloadPredecessor:
    """Validated owner of the prior logical-workload generation."""

    generation: int
    fencing_token: str
    remote_id: str


def _utcnow() -> str:
    return datetime.now(UTC).isoformat()


def _default_state_dir() -> Path:
    configured = os.getenv("EYVAN_SYSTEMD_RUNTIME_DIR")
    if configured:
        return Path(configured).expanduser()
    agent_state = os.getenv("EYVAN_AGENT_STATE_DIR")
    if agent_state:
        return Path(agent_state).expanduser() / "systemd-runtime"
    return Path.home() / ".local" / "state" / "eyvan" / "systemd-runtime"


def _record_name(remote_id: str) -> str:
    return hashlib.sha256(remote_id.encode("utf-8")).hexdigest()


def systemd_unit_name(remote_id: str) -> str:
    """Return a deterministic, systemd-safe transient service unit name."""

    if not isinstance(remote_id, str) or not remote_id.strip():
        raise ValueError("systemd submission ID must be a non-empty string")
    normalized = _UNIT_SAFE.sub("-", remote_id.strip()).strip(".-") or "workload"
    normalized = normalized[:48].rstrip(".-") or "workload"
    digest = hashlib.sha256(remote_id.encode("utf-8")).hexdigest()[:16]
    return f"eyvan-{normalized}-{digest}.service"


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


def _write_environment_file(path: Path, environment: tp.Mapping[str, str]) -> None:
    """Atomically write one owner-only environment file for systemd or Docker.

    Both systemd ``EnvironmentFile=`` units and ``docker run --env-file``
    parse one ``NAME=value`` assignment per line, so values cannot span
    lines. The file keeps the workload environment out of unit argv, which
    ``systemctl show``, ``ps``, and the journal expose to every host user.
    """

    lines: list[str] = []
    for name, value in sorted(environment.items()):
        if "\n" in value or "\r" in value:
            raise ValueError(f"environment variable {name!r} must not contain a newline")
        lines.append(f"{name}={value}")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write("".join(f"{line}\n" for line in lines))
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
        raise RuntimeError(f"systemd execution record {path} is not a JSON object")
    return payload


def _persisted_status(remote_id: str, record: tp.Mapping[str, JsonValue]) -> ExecutionWorkloadStatus:
    """Parse one required durable status without inventing a fallback."""

    try:
        raw_status = record["status"]
    except KeyError as exc:
        raise _InvalidDurableRecord(f"systemd execution {remote_id!r} has no persisted status") from exc
    if not isinstance(raw_status, str):
        raise _InvalidDurableRecord(f"systemd execution {remote_id!r} has a non-string persisted status")
    try:
        status = ExecutionWorkloadStatus(raw_status)
    except ValueError as exc:
        raise _InvalidDurableRecord(
            f"systemd execution {remote_id!r} has unsupported persisted status {raw_status!r}"
        ) from exc
    if status not in _DURABLE_STATES:
        raise _InvalidDurableRecord(f"systemd execution {remote_id!r} has unsupported durable status {status.value!r}")
    return status


def _persisted_message(remote_id: str, record: tp.Mapping[str, JsonValue]) -> str:
    """Read the required durable message without coercing its type."""

    try:
        message = record["message"]
    except KeyError as exc:
        raise _InvalidDurableRecord(f"systemd execution {remote_id!r} has no persisted message") from exc
    if not isinstance(message, str):
        raise _InvalidDurableRecord(f"systemd execution {remote_id!r} has a non-string persisted message")
    return message


def _persisted_remote_id(expected_remote_id: str, record: tp.Mapping[str, JsonValue]) -> None:
    """Verify that a hashed record still belongs to the requested execution."""

    try:
        remote_id = record["remote_id"]
    except KeyError as exc:
        raise _InvalidDurableRecord(f"systemd execution {expected_remote_id!r} has no persisted remote ID") from exc
    if not isinstance(remote_id, str) or not remote_id:
        raise _InvalidDurableRecord(f"systemd execution {expected_remote_id!r} has an invalid persisted remote ID")
    if remote_id != expected_remote_id:
        raise _InvalidDurableRecord(f"systemd execution record belongs to {remote_id!r}, not {expected_remote_id!r}")


def _durable_observation(remote_id: str, record: tp.Mapping[str, JsonValue]) -> ExecutionWorkloadObservation:
    """Return a complete observation from a validated durable record."""

    _persisted_remote_id(remote_id, record)
    status = _persisted_status(remote_id, record)
    message = _persisted_message(remote_id, record)
    if status is ExecutionWorkloadStatus.INDETERMINATE and not message:
        raise _InvalidDurableRecord(f"systemd execution {remote_id!r} has an indeterminate status without a reason")
    return ExecutionWorkloadObservation(
        remote_id=remote_id,
        status=status,
        message=message,
    )


def _known_persisted_status(record: tp.Mapping[str, JsonValue]) -> ExecutionWorkloadStatus | None:
    """Return an existing valid durable status without treating invalid data as a state."""

    try:
        return _persisted_status("<record>", record)
    except _InvalidDurableRecord:
        return None


def _required_record_integer(record: tp.Mapping[str, JsonValue], field: str) -> int:
    """Read a JSON integer field without accepting booleans or coercions."""

    try:
        value = record[field]
    except KeyError as exc:
        raise ValueError(f"record has no {field!r} field") from exc
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"record has a non-integer {field!r} field")
    return value


def _required_record_string(record: tp.Mapping[str, JsonValue], field: str) -> str:
    """Read a JSON string field without coercing an arbitrary value."""

    try:
        value = record[field]
    except KeyError as exc:
        raise ValueError(f"record has no {field!r} field") from exc
    if not isinstance(value, str):
        raise ValueError(f"record has a non-string {field!r} field")
    return value


@contextmanager
def _file_lock(path: Path) -> tp.Iterator[None]:
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


def _read_boot_id() -> str | None:
    """Return the Linux boot ID when available.

    A transient systemd unit cannot survive a reboot. Persisting this marker
    lets callers record a definitive ``LOST`` result instead of treating the
    post-reboot missing unit as a completed job. Non-Linux hosts simply omit
    the marker.
    """

    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return value or None


def _cgroup_pids(control_group: str) -> tuple[int, ...] | None:
    """Return the PIDs in a systemd cgroup, or ``None`` when it is unreadable.

    ``None`` is deliberately distinct from an empty tuple: an unreadable,
    missing, or malformed cgroup is ambiguous, not proof that nothing is
    running. Collapsing the two would let a background child be reported as a
    completed workload.
    """

    if not isinstance(control_group, str) or not control_group.startswith("/"):
        return None
    parts = tuple(part for part in control_group.split("/") if part)
    if not parts or any(part in {".", ".."} for part in parts):
        return None
    try:
        contents = (Path("/sys/fs/cgroup").joinpath(*parts) / "cgroup.procs").read_text(encoding="utf-8")
    except OSError:
        # None, not (): an unreadable cgroup is ambiguous, not proof that
        # nothing is running. Returning an empty tuple makes
        # _cgroup_has_processes report False, which _status_from_properties maps
        # to SUCCEEDED -- so a permission-denied or namespace-hidden cgroup on a
        # hung collective would be reported as a completed workload.
        return None
    pids: list[int] = []
    for line in contents.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        try:
            pids.append(int(candidate))
        except ValueError:
            # A non-numeric line means the file is not what we think it is;
            # treat the whole reading as ambiguous rather than partially trust it.
            return None
    return tuple(pids)


def _cgroup_has_processes(control_group: str) -> bool | None:
    """Return whether a systemd cgroup still has processes."""

    pids = _cgroup_pids(control_group)
    return None if pids is None else bool(pids)


def _environment(spec: tp.Mapping[str, JsonValue]) -> dict[str, str]:
    runtime_env = spec.get("runtime_env")
    if runtime_env is None:
        runtime_env = {}
    if not isinstance(runtime_env, dict):
        raise ValueError("native-systemd runtime_env must be a mapping")
    unsupported = sorted(key for key in runtime_env if key != "env_vars")
    if unsupported:
        raise ValueError(f"native-systemd execution does not support runtime_env field {unsupported[0]!r}")
    legacy = runtime_env.get("env_vars")
    neutral = spec.get("environment")
    if legacy is None:
        legacy = {}
    if neutral is None:
        neutral = {}
    if not isinstance(legacy, dict) or not isinstance(neutral, dict):
        raise ValueError("native-systemd environment variables must be a mapping")

    result: dict[str, str] = {}
    for raw_name, raw_value in (*legacy.items(), *neutral.items()):
        if not isinstance(raw_name, str) or not _ENVIRONMENT_NAME.fullmatch(raw_name):
            raise ValueError("native-systemd environment variable names must be valid shell identifiers")
        if not isinstance(raw_value, str):
            raise ValueError(f"native-systemd environment variable {raw_name!r} must have a string value")
        if "\n" in raw_value or "\r" in raw_value:
            raise ValueError(f"native-systemd environment variable {raw_name!r} must not contain a newline")
        result[raw_name] = raw_value
    result.setdefault("EYVAN_REPLICA_INDEX", "0")
    result.setdefault("EYVAN_REPLICA_COUNT", "1")
    return result


def _validate_gang(spec: tp.Mapping[str, JsonValue]) -> None:
    gang = spec.get("gang")
    if not isinstance(gang, dict):
        raise ValueError("native-systemd workloads require a gang resource contract")
    replicas = gang.get("replicas")
    if isinstance(replicas, bool) or not isinstance(replicas, int):
        raise ValueError("native-systemd gang replicas must be an integer")
    if replicas != 1:
        raise ValueError("native-systemd execution supports exactly one replica")
    if not isinstance(gang.get("per_replica"), dict):
        raise ValueError("native-systemd gang per_replica resources must be a mapping")
    topology = gang.get("required_topology")
    if topology is None:
        topology = {}
    if not isinstance(topology, dict):
        raise ValueError("native-systemd required topology must be a mapping")
    if topology:
        raise ValueError("native-systemd execution does not support topology constraints")


def _parse_properties(payload: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in payload.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key] = value
    return result


def _systemd_runner(command: tuple[str, ...], *, timeout_s: float) -> SystemdCommandResult:
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=timeout_s)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return SystemdCommandResult(returncode=127, stderr=str(exc))
    return SystemdCommandResult(
        returncode=result.returncode,
        stdout=result.stdout or "",
        stderr=result.stderr or "",
    )


class SystemdTransientServiceExecutor:
    """Execute fenced native workloads as durable systemd transient services.

    The executor supports one local replica. On systemd 250 and later,
    ``ExitType=cgroup`` keeps a unit nonterminal until every workload process
    has exited, including background children created by its shell entrypoint.
    On older managers, Eyvan queries the unit's ``ControlGroup`` before
    accepting an exited main process as complete. ``RemainAfterExit=yes``
    keeps the completed unit observable until Eyvan persists its terminal
    state. ``KillMode=control-group`` makes `systemctl stop` terminate that
    whole cgroup.
    """

    def __init__(
        self,
        state_dir: str | os.PathLike[str] | None = None,
        *,
        command_runner: SystemdCommandRunner | None = None,
        boot_id_reader: tp.Callable[[], str | None] | None = None,
        cgroup_processes_reader: CgroupProcessesReader | None = None,
        cgroup_pids_reader: CgroupPidsReader | None = None,
        use_exit_type_cgroup: bool = False,
        workload_user: str | None = None,
        workload_group: str | None = None,
        workload_directory: str | os.PathLike[str] | None = None,
        base_environment: tp.Mapping[str, str] | None = None,
        command_timeout_s: float = 315.0,
    ) -> None:
        if command_timeout_s <= 0:
            raise ValueError("systemd command timeout must be positive")
        if not isinstance(use_exit_type_cgroup, bool):
            raise ValueError("systemd ExitType cgroup setting must be a boolean")
        for label, identity in (("workload user", workload_user), ("workload group", workload_group)):
            if identity is not None and (not isinstance(identity, str) or not _SYSTEMD_IDENTITY.fullmatch(identity)):
                raise ValueError(f"systemd {label} must be a valid local identity")
        if workload_group is not None and workload_user is None:
            raise ValueError("systemd workload group requires a workload user")
        if workload_directory is not None:
            if not isinstance(workload_directory, (str, os.PathLike)) or not Path(workload_directory).is_absolute():
                raise ValueError("systemd workload directory must be absolute")
        if base_environment is not None:
            for name, value in base_environment.items():
                if not isinstance(name, str) or not _ENVIRONMENT_NAME.fullmatch(name):
                    raise ValueError("systemd base environment variable names must be valid shell identifiers")
                if not isinstance(value, str) or "\x00" in value:
                    raise ValueError(f"systemd base environment variable {name!r} must be a NUL-free string")
        self.state_dir = Path(state_dir).expanduser() if state_dir is not None else _default_state_dir()
        self.jobs_dir = self.state_dir / "jobs"
        self.workloads_dir = self.state_dir / "workloads"
        self.env_dir = self.state_dir / "env"
        for directory in (self.state_dir, self.jobs_dir, self.workloads_dir, self.env_dir):
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            directory.chmod(0o700)
        self._command_timeout_s = command_timeout_s
        self._command_runner = command_runner or (
            lambda command: _systemd_runner(command, timeout_s=self._command_timeout_s)
        )
        self._boot_id_reader = boot_id_reader or _read_boot_id
        self._cgroup_pids_reader = cgroup_pids_reader or _cgroup_pids
        # Derive the boolean view from the PID reader unless one is injected, so
        # a test that stubs only one of the two cannot see them disagree.
        self._cgroup_processes_reader = cgroup_processes_reader or (
            _cgroup_has_processes
            if cgroup_pids_reader is None
            else (lambda group: (lambda pids: None if pids is None else bool(pids))(cgroup_pids_reader(group)))
        )
        self._use_exit_type_cgroup = use_exit_type_cgroup
        self._workload_user = workload_user
        self._workload_group = workload_group
        self._workload_directory = str(workload_directory) if workload_directory is not None else None
        self._base_environment = dict(base_environment or {})

    def _record_path(self, remote_id: str) -> Path:
        return self.jobs_dir / f"{_record_name(remote_id)}.json"

    def _workload_fence_path(self, workload_id: str) -> Path:
        return self.workloads_dir / f"{_record_name(workload_id)}.json"

    def _environment_file_path(self, remote_id: str) -> Path:
        return self.env_dir / f"{_record_name(remote_id)}.env"

    @staticmethod
    def _validate_fence(remote_id: str, record: tp.Mapping[str, JsonValue], context: MutationContext) -> None:
        try:
            generation = _required_record_integer(record, "generation")
            token = _required_record_string(record, "fencing_token")
        except ValueError as exc:
            raise StaleMutationError(f"systemd execution {remote_id!r} has an invalid persisted generation") from exc
        if generation != context.generation:
            raise StaleMutationError(
                f"generation {context.generation} does not own systemd execution {remote_id!r} generation {generation}"
            )
        if not isinstance(token, str) or token != context.fencing_token:
            raise StaleMutationError(
                f"fencing token does not own systemd execution {remote_id!r} generation {generation}"
            )

    @staticmethod
    def _logical_workload_id(metadata: tp.Mapping[str, JsonValue]) -> str | None:
        value = metadata.get("eyvan_workload_id")
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("native-systemd eyvan_workload_id must be a non-empty string")
        return value

    def _run(self, command: tuple[str, ...]) -> SystemdCommandResult:
        try:
            return self._command_runner(command)
        except Exception as exc:
            return SystemdCommandResult(returncode=127, stderr=str(exc))

    def _show_unit(self, unit_name: str) -> tuple[str, dict[str, str] | None, str]:
        """Return ``(outcome, properties, detail)`` for one unit.

        ``outcome`` is ``present``, ``missing``, or ``unavailable``. The last
        category deliberately does not collapse to a finished workload.
        """

        result = self._run(
            (
                "systemctl",
                "show",
                "--no-pager",
                "--property=LoadState",
                "--property=ActiveState",
                "--property=SubState",
                "--property=Result",
                "--property=ExecMainCode",
                "--property=ExecMainStatus",
                "--property=ControlGroup",
                "--property=MainPID",
                unit_name,
            )
        )
        properties = _parse_properties(result.stdout)
        detail = result.stderr.strip()
        if properties.get("LoadState") == "not-found" or result.returncode == 4:
            return "missing", None, detail or "systemd unit is absent"
        if result.returncode != 0 or properties.get("LoadState") != "loaded":
            return "unavailable", None, detail or "systemctl show did not return a loaded unit"
        return "present", properties, detail

    def _status_from_properties(
        self, record: tp.Mapping[str, JsonValue], properties: tp.Mapping[str, str]
    ) -> tuple[ExecutionWorkloadStatus, str]:
        active = properties.get("ActiveState", "").lower()
        substate = properties.get("SubState", "").lower()
        result = properties.get("Result", "").lower()
        main_status = properties.get("ExecMainStatus", "")
        stop_requested = self._record_boolean(record, "stop_requested")

        if active == "activating":
            return ExecutionWorkloadStatus.PENDING, "systemd service is starting"
        if active in {"active", "reloading"}:
            if substate == "exited":
                # Persisting this mode is necessary because a worker restart or
                # upgrade can change the current systemd capability while an
                # older v249-compatible unit remains active.
                if not self._record_boolean(record, "exit_type_cgroup"):
                    control_group = properties.get("ControlGroup", "")
                    # On systemd 249 an exited transient unit retains
                    # ActiveState=active because of RemainAfterExit=yes, but
                    # its ControlGroup is cleared once every process is gone.
                    # A background child keeps a non-empty cgroup, so an
                    # empty value is positive completion evidence rather than
                    # an unreadable cgroup.
                    if not control_group:
                        return ExecutionWorkloadStatus.SUCCEEDED, "systemd service completed successfully"
                    try:
                        cgroup_has_processes = self._cgroup_processes_reader(control_group)
                    except Exception:
                        cgroup_has_processes = None
                    if cgroup_has_processes is True:
                        return ExecutionWorkloadStatus.RUNNING, "systemd service still has workload cgroup processes"
                    if cgroup_has_processes is not False:
                        return ExecutionWorkloadStatus.INDETERMINATE, _RECOVERABLE_CGROUP_INDETERMINATE
                return ExecutionWorkloadStatus.SUCCEEDED, "systemd service completed successfully"
            return ExecutionWorkloadStatus.RUNNING, "systemd service is running"
        if active == "deactivating":
            return ExecutionWorkloadStatus.RUNNING, "systemd service is stopping"
        if active == "inactive":
            if stop_requested:
                return ExecutionWorkloadStatus.STOPPED, "systemd service stopped by Eyvan"
            if result in {"", "success"} and main_status in {"", "0"}:
                return ExecutionWorkloadStatus.FAILED, "systemd service became inactive before Eyvan observed completion"
            return ExecutionWorkloadStatus.FAILED, f"systemd service exited with result {result or 'unknown'}"
        if active == "failed":
            if stop_requested:
                return ExecutionWorkloadStatus.STOPPED, "systemd service stopped by Eyvan"
            return ExecutionWorkloadStatus.FAILED, f"systemd service failed with result {result or 'unknown'}"
        return ExecutionWorkloadStatus.INDETERMINATE, (
            f"systemd service reported unexpected ActiveState={active or 'missing'}"
        )

    @staticmethod
    def _record_boolean(record: tp.Mapping[str, JsonValue], field: str) -> bool:
        """Read one required lifecycle flag without truthiness coercion."""

        try:
            value = record[field]
        except KeyError as exc:
            raise _InvalidDurableRecord(f"systemd execution record has no {field!r} flag") from exc
        if not isinstance(value, bool):
            raise _InvalidDurableRecord(f"systemd execution record has a non-boolean {field!r} flag")
        return value

    @staticmethod
    def _is_final(status: ExecutionWorkloadStatus) -> bool:
        return status in _FINAL_STATES

    @staticmethod
    def _set_status(record: JsonObject, status: ExecutionWorkloadStatus, message: str) -> None:
        previous = _known_persisted_status(record)
        record["status"] = status.value
        record["message"] = message
        if status is not ExecutionWorkloadStatus.INDETERMINATE:
            record.pop(_RECOVERABLE_INDETERMINATE_FIELD, None)
        if status is ExecutionWorkloadStatus.RUNNING and record.get("started_at") is None:
            record["started_at"] = _utcnow()
        if status in _FINAL_STATES and previous not in _FINAL_STATES:
            record["finished_at"] = _utcnow()
        record["updated_at"] = _utcnow()

    def _mark_indeterminate(self, record: JsonObject, message: str, *, recoverable: bool = False) -> None:
        current_status = _known_persisted_status(record)
        if current_status not in _FINAL_STATES or (
            current_status is ExecutionWorkloadStatus.INDETERMINATE and _is_recoverable_indeterminate(record)
        ):
            self._set_status(record, ExecutionWorkloadStatus.INDETERMINATE, message)
            if recoverable:
                record[_RECOVERABLE_INDETERMINATE_FIELD] = True

    def _collect_terminal_unit(self, record: JsonObject) -> None:
        """Ask systemd to release a unit after its terminal record is durable."""

        if _known_persisted_status(record) not in _COLLECTABLE_STATES or record.get("unit_collected") is True:
            return
        unit_name = record.get("unit_name")
        if not isinstance(unit_name, str) or not unit_name:
            return
        stop = self._run(("systemctl", "stop", "--no-pager", unit_name))
        reset = self._run(("systemctl", "reset-failed", "--no-pager", unit_name))
        # A missing unit is already collected. Other errors remain retryable on
        # the next observation while the durable status remains authoritative.
        if stop.returncode in {0, 4} and reset.returncode in {0, 4}:
            record["unit_collected"] = True
            record.pop("unit_collection_error", None)
            return
        detail = stop.stderr.strip() or reset.stderr.strip() or "systemd did not collect terminal unit"
        record["unit_collection_error"] = detail

    def _boot_changed(self, record: tp.Mapping[str, JsonValue]) -> bool:
        recorded = record.get("boot_id")
        if not isinstance(recorded, str) or not recorded:
            return False
        try:
            current = self._boot_id_reader()
        except OSError:
            return False
        return isinstance(current, str) and bool(current) and current != recorded

    def _claim_logical_workload_locked(
        self,
        path: Path,
        workload_id: str,
        remote_id: str,
        context: MutationContext,
    ) -> None:
        """Persist one workload fence while its caller holds ``path``'s lock."""

        current = _load_json(path)
        predecessor: _LogicalWorkloadPredecessor | None = None
        if current is not None:
            try:
                generation = _required_record_integer(current, "generation")
                current_remote_id = _required_record_string(current, "remote_id")
                token = _required_record_string(current, "fencing_token")
            except ValueError as exc:
                raise StaleMutationError(f"logical workload {workload_id!r} has an invalid persisted fence") from exc
            if not current_remote_id or not token:
                raise StaleMutationError(f"logical workload {workload_id!r} has an invalid persisted fence")
            if context.generation < generation:
                raise StaleMutationError(
                    f"generation {context.generation} is older than logical workload {workload_id!r} "
                    f"generation {generation}"
                )
            if context.generation == generation:
                if token != context.fencing_token:
                    raise StaleMutationError(
                        f"fencing token does not own logical workload {workload_id!r} generation {generation}"
                    )
                if current_remote_id != remote_id:
                    raise StaleMutationError(
                        f"submission {remote_id!r} does not own logical workload {workload_id!r} generation {generation}"
                    )
                return
            predecessor = _LogicalWorkloadPredecessor(
                generation=generation,
                fencing_token=token,
                remote_id=current_remote_id,
            )

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
                **(
                    {
                        "predecessor": {
                            "generation": predecessor.generation,
                            "fencing_token": predecessor.fencing_token,
                            "remote_id": predecessor.remote_id,
                        }
                    }
                    if predecessor is not None
                    else {}
                ),
                "updated_at": _utcnow(),
            },
        )

    def _record_fields(
        self,
        spec: tp.Mapping[str, JsonValue],
        remote_id: str,
        environment: tp.Mapping[str, str],
    ) -> tp.Mapping[str, JsonValue]:
        """Return runtime-specific durable fields for a newly submitted record.

        Subclasses use this narrow hook to persist external runtime ownership
        identifiers before the transient service is launched.  The base
        executor deliberately owns the fencing and lifecycle fields.
        """

        del spec, remote_id, environment
        return {}

    def _prepare_launch(self, record: tp.Mapping[str, JsonValue], spec: tp.Mapping[str, JsonValue]) -> None:
        """Write the workload environment file before its unit is queued.

        The merged launch environment travels as one owner-only 0600 file so
        unit argv stays free of values. ``EnvironmentFile=`` without
        systemd's ``-`` ignore prefix makes a missing file fail the launch
        loudly instead of silently starting without an environment.
        """

        remote_id = record.get("remote_id")
        if not isinstance(remote_id, str) or not remote_id:
            raise RuntimeError("systemd execution record has no remote ID")
        launch_environment = {**self._base_environment, **_environment(spec)}
        _write_environment_file(self._environment_file_path(remote_id), launch_environment)

    def _launch_command(
        self,
        unit_name: str,
        entrypoint: str,
        environment: tp.Mapping[str, str],
        *,
        record: tp.Mapping[str, JsonValue],
        spec: tp.Mapping[str, JsonValue],
    ) -> tuple[str, ...]:
        """Return the systemd launch command for one durable record."""

        del environment, spec
        remote_id = record.get("remote_id")
        if not isinstance(remote_id, str) or not remote_id:
            raise RuntimeError("systemd execution record has no remote ID")
        return (
            "systemd-run",
            "--quiet",
            "--no-block",
            f"--unit={unit_name}",
            "--service-type=exec",
            *(("--property=ExitType=cgroup",) if self._use_exit_type_cgroup else ()),
            "--property=RemainAfterExit=yes",
            "--property=KillMode=control-group",
            "--property=StandardOutput=journal",
            "--property=StandardError=journal",
            "--property=LimitMEMLOCK=infinity",
            "--property=LimitNOFILE=100000",
            "--property=NoNewPrivileges=yes",
            f"--property=EnvironmentFile={self._environment_file_path(remote_id)}",
            *((f"--property=User={self._workload_user}",) if self._workload_user is not None else ()),
            *((f"--property=Group={self._workload_group}",) if self._workload_group is not None else ()),
            *(
                (f"--property=WorkingDirectory={self._workload_directory}",)
                if self._workload_directory is not None
                else ()
            ),
            "/bin/sh",
            "-c",
            entrypoint,
        )

    def _runtime_status(self, record: JsonObject) -> tuple[ExecutionWorkloadStatus, str] | None:
        """Optionally observe an external runtime before inspecting systemd.

        ``None`` delegates status ownership to systemd.  A tuple is treated as
        authoritative for the running invocation and is persisted through the
        same state-transition rules as native systemd observations.
        """

        del record
        return None

    def _stop_runtime(self, record: tp.Mapping[str, JsonValue]) -> None:
        """Stop an external runtime before stopping its transient service."""

        del record

    def _after_systemd_stop(self, record: tp.Mapping[str, JsonValue]) -> None:
        """Finish external-runtime cleanup after the transient unit stops."""

        del record

    def _retire_terminal_runtime(self, record: tp.Mapping[str, JsonValue]) -> None:
        """Release external-runtime state retained after terminal observation."""

        del record

    def _runtime_logs(self, record: tp.Mapping[str, JsonValue]) -> str | None:
        """Return external-runtime logs, or ``None`` to fall back to journald."""

        del record
        return None

    def submit(self, spec: tp.Mapping[str, JsonValue], context: MutationContext) -> str:
        """Launch one transient unit or return an owned durable replay."""

        entrypoint = spec.get("entrypoint")
        if not isinstance(entrypoint, str) or not entrypoint.strip():
            raise ValueError("native-systemd workloads require a non-empty entrypoint")
        _validate_gang(spec)
        environment = _environment(spec)
        submitted_remote_id = spec.get("submission_id")
        if submitted_remote_id is None:
            remote_id = context.idempotency_key.strip()
        elif isinstance(submitted_remote_id, str):
            remote_id = submitted_remote_id.strip()
        else:
            raise ValueError("native-systemd submission ID must be a string")
        if not remote_id:
            raise ValueError("native-systemd submission ID must be non-empty")
        metadata = spec.get("metadata")
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict):
            raise ValueError("native-systemd workload metadata must be a mapping")
        workload_id = self._logical_workload_id(metadata)

        record_path = self._record_path(remote_id)
        # Reject a reused attempt ID before touching the logical workload
        # fence. Otherwise a rejected generation could supersede the current
        # workload high-watermark without ever owning this record.
        with _file_lock(_record_lock_path(record_path)):
            existing = _load_json(record_path)
            if existing is not None:
                self._validate_fence(remote_id, existing, context)
                return remote_id

        def reserve_and_launch() -> str:
            """Reserve and launch while any logical workload lock remains held."""

            with _file_lock(_record_lock_path(record_path)):
                existing = _load_json(record_path)
                if existing is not None:
                    self._validate_fence(remote_id, existing, context)
                    return remote_id
                now = _utcnow()
                try:
                    boot_id = self._boot_id_reader()
                except OSError:
                    boot_id = None
                record_fields = dict(self._record_fields(spec, remote_id, environment))
                unit_name = systemd_unit_name(remote_id)
                protected_fields = {
                    "remote_id",
                    "unit_name",
                    "status",
                    "message",
                    "generation",
                    "fencing_token",
                    "idempotency_key",
                    "entrypoint",
                    "environment",
                    "metadata",
                    "exit_type_cgroup",
                    "boot_id",
                    "stop_requested",
                    "created_at",
                    "updated_at",
                    "started_at",
                    "finished_at",
                }
                conflicting_fields = sorted(protected_fields & set(record_fields))
                if conflicting_fields:
                    raise ValueError(
                        f"systemd runtime record fields must not replace lifecycle field {conflicting_fields[0]!r}"
                    )
                record: JsonObject = {
                    "remote_id": remote_id,
                    "unit_name": unit_name,
                    "status": ExecutionWorkloadStatus.PENDING.value,
                    "message": "waiting for systemd transient service",
                    "generation": context.generation,
                    "fencing_token": context.fencing_token,
                    "idempotency_key": context.idempotency_key,
                    "entrypoint": entrypoint,
                    "environment": environment,
                    "metadata": dict(metadata),
                    "exit_type_cgroup": self._use_exit_type_cgroup,
                    "boot_id": boot_id if isinstance(boot_id, str) and boot_id else None,
                    "stop_requested": False,
                    "created_at": now,
                    "updated_at": now,
                    "started_at": None,
                    "finished_at": None,
                    **record_fields,
                }
                _atomic_write_json(record_path, record)
                try:
                    self._prepare_launch(record, spec)
                except Exception as exc:
                    self._set_status(record, ExecutionWorkloadStatus.FAILED, f"failed to prepare native runtime: {exc}")
                    _atomic_write_json(record_path, record)
                    raise RuntimeError(f"failed to launch systemd execution {remote_id!r}: {exc}") from exc
                result = self._run(
                    self._launch_command(
                        unit_name,
                        entrypoint,
                        environment,
                        record=record,
                        spec=spec,
                    )
                )
                if result.returncode == 0:
                    return remote_id

                outcome, _properties, detail = self._show_unit(unit_name)
                launch_detail = result.stderr.strip() or detail or "systemd-run failed"
                if outcome == "missing":
                    self._set_status(
                        record,
                        ExecutionWorkloadStatus.FAILED,
                        f"failed to launch systemd service: {launch_detail}",
                    )
                elif outcome == "unavailable":
                    self._mark_indeterminate(
                        record,
                        f"cannot determine whether systemd service launched: {launch_detail}",
                    )
                else:
                    # A non-zero client result can race a successfully queued unit.
                    # Keep its record pending and let normal observation resolve it.
                    record["updated_at"] = _utcnow()
                _atomic_write_json(record_path, record)
                if outcome == "present":
                    return remote_id
            raise RuntimeError(f"failed to launch systemd execution {remote_id!r}: {launch_detail}")

        if workload_id is None:
            return reserve_and_launch()
        workload_path = self._workload_fence_path(workload_id)
        # Do not release this lock between fence advancement and the durable
        # record reservation/systemd-run call. A newer generation must observe
        # and retire an actual prior attempt, never a fence with a launch gap.
        with _file_lock(_record_lock_path(workload_path)):
            self._claim_logical_workload_locked(workload_path, workload_id, remote_id, context)
            return reserve_and_launch()

    def status(self, remote_id: str) -> ExecutionWorkloadObservation:
        """Return durable status, failing closed when systemd ownership is lost."""

        record_path = self._record_path(remote_id)
        with _file_lock(_record_lock_path(record_path)):
            record = _load_json(record_path)
            if record is None:
                return ExecutionWorkloadObservation(
                    remote_id=remote_id,
                    status=ExecutionWorkloadStatus.NOT_FOUND,
                    message="systemd execution is absent",
                )
            try:
                durable = _durable_observation(remote_id, record)
            except _InvalidDurableRecord as exc:
                message = f"invalid durable record: {exc}"
                self._set_status(record, ExecutionWorkloadStatus.INDETERMINATE, message)
                _atomic_write_json(record_path, record)
                return ExecutionWorkloadObservation(
                    remote_id=remote_id,
                    status=ExecutionWorkloadStatus.INDETERMINATE,
                    message=message,
                )

            # A cgroup probe can be transiently unavailable on systemd 249.
            # Recheck precisely that ambiguity: a later empty ControlGroup is
            # positive evidence that every workload process exited. Other
            # indeterminate outcomes remain terminal and fail closed.
            recoverable_cgroup_indeterminate = (
                durable.status is ExecutionWorkloadStatus.INDETERMINATE and _is_recoverable_indeterminate(record)
            )
            if self._is_final(durable.status) and not recoverable_cgroup_indeterminate:
                self._collect_terminal_unit(record)
                _atomic_write_json(record_path, record)
                return durable
            if self._boot_changed(record):
                self._set_status(
                    record,
                    ExecutionWorkloadStatus.LOST,
                    "host boot ID changed; transient systemd service was lost",
                )
                _atomic_write_json(record_path, record)
                return _durable_observation(remote_id, record)

            try:
                runtime_observation = self._runtime_status(record)
                if runtime_observation is not None:
                    status, message = runtime_observation
                    if status is ExecutionWorkloadStatus.INDETERMINATE:
                        self._mark_indeterminate(record, message)
                    else:
                        self._set_status(record, status, message)
                    _atomic_write_json(record_path, record)
                    self._collect_terminal_unit(record)
                    _atomic_write_json(record_path, record)
                    return _durable_observation(remote_id, record)

                unit_name = record.get("unit_name")
                if not isinstance(unit_name, str) or not unit_name:
                    self._mark_indeterminate(record, "systemd execution record has no unit name")
                    _atomic_write_json(record_path, record)
                    return _durable_observation(remote_id, record)
                outcome, properties, detail = self._show_unit(unit_name)
                if outcome == "missing":
                    self._mark_indeterminate(record, "systemd service disappeared before a terminal state was recorded")
                elif outcome == "unavailable" or properties is None:
                    self._mark_indeterminate(record, f"cannot observe systemd service: {detail}")
                else:
                    status, message = self._status_from_properties(record, properties)
                    if status is ExecutionWorkloadStatus.INDETERMINATE:
                        self._mark_indeterminate(record, message)
                    else:
                        self._set_status(record, status, message)
            except _InvalidDurableRecord as exc:
                self._mark_indeterminate(record, f"invalid durable record: {exc}")
            _atomic_write_json(record_path, record)
            self._collect_terminal_unit(record)
            _atomic_write_json(record_path, record)
            return _durable_observation(remote_id, record)

    def stop(self, remote_id: str, context: MutationContext) -> None:
        """Stop the workload's systemd cgroup through ``systemctl stop``."""

        record_path = self._record_path(remote_id)
        with _file_lock(_record_lock_path(record_path)):
            record = _load_json(record_path)
            if record is None:
                return
            self._validate_fence(remote_id, record, context)
            try:
                durable = _durable_observation(remote_id, record)
            except _InvalidDurableRecord as exc:
                self._set_status(record, ExecutionWorkloadStatus.INDETERMINATE, f"invalid durable record: {exc}")
                _atomic_write_json(record_path, record)
                raise RuntimeError(f"cannot safely stop ambiguous systemd execution {remote_id!r}") from exc
            if durable.status in _TERMINAL_STATES:
                return
            # What makes a stop unsafe is not knowing *what* to stop, so the unit
            # name is the gate -- not the status.  An INDETERMINATE record with a
            # known unit is the case where stopping is most necessary: the status
            # is INDETERMINATE precisely because an earlier stop failed, and
            # refusing here made that first failure permanent.  The workload could
            # then never be cancelled and its accelerator was held until an
            # operator intervened, which is how one v5p Multislice stayed leased
            # to a container that had already exited.
            unit_name = record.get("unit_name")
            if not isinstance(unit_name, str) or not unit_name:
                self._mark_indeterminate(record, "systemd execution record has no unit name")
                _atomic_write_json(record_path, record)
                raise RuntimeError(f"cannot safely stop ambiguous systemd execution {remote_id!r}")
            record["stop_requested"] = True
            record["message"] = "systemd stop requested by Eyvan"
            record["updated_at"] = _utcnow()
            _atomic_write_json(record_path, record)

        try:
            self._stop_runtime(record)
        except Exception as exc:
            with _file_lock(_record_lock_path(record_path)):
                current = _load_json(record_path)
                if current is not None and _known_persisted_status(current) not in _FINAL_STATES:
                    self._mark_indeterminate(current, f"failed to stop native runtime: {exc}", recoverable=True)
                    _atomic_write_json(record_path, current)
            raise RuntimeError(f"failed to stop native runtime for {remote_id!r}: {exc}") from exc

        result = self._run(("systemctl", "stop", "--no-pager", unit_name))
        try:
            self._after_systemd_stop(record)
        except Exception as exc:
            with _file_lock(_record_lock_path(record_path)):
                current = _load_json(record_path)
                if current is not None and _known_persisted_status(current) not in _FINAL_STATES:
                    self._mark_indeterminate(current, f"failed to finish native runtime stop: {exc}", recoverable=True)
                    _atomic_write_json(record_path, current)
            raise RuntimeError(f"failed to finish native runtime stop for {remote_id!r}: {exc}") from exc

        # Both stop stages have now succeeded, which is direct positive evidence
        # about a workload whose record may still carry an INDETERMINATE latched
        # by an *earlier* attempt -- including one written by a build that
        # predates the recoverable flag. Let the verification below re-derive
        # from systemd and Docker instead of being vetoed by that stale unknown,
        # which is what kept a Multislice leased to an exited container.
        with _file_lock(_record_lock_path(record_path)):
            current = _load_json(record_path)
            if current is not None and _known_persisted_status(current) is ExecutionWorkloadStatus.INDETERMINATE:
                current[_RECOVERABLE_INDETERMINATE_FIELD] = True
                _atomic_write_json(record_path, current)

        observed = self.status(remote_id)
        if observed.status is ExecutionWorkloadStatus.INDETERMINATE:
            raise RuntimeError(f"cannot safely verify that systemd execution {remote_id!r} stopped")
        if result.returncode != 0 and observed.status not in _TERMINAL_STATES:
            raise RuntimeError(
                f"systemctl stop failed for {remote_id!r}: {result.stderr.strip() or 'unknown systemd error'}"
            )

    def retire(self, remote_id: str, context: MutationContext) -> None:
        """Stop a superseded cgroup and wait for a verified terminal record."""

        record_path = self._record_path(remote_id)
        with _file_lock(_record_lock_path(record_path)):
            record = _load_json(record_path)
            if record is None:
                return
            self._validate_fence(remote_id, record, context)

        try:
            timeout_s = max(float(os.getenv("EYVAN_SUPERSEDE_WAIT_TIMEOUT_S", "300")), 0.1)
        except ValueError:
            timeout_s = 300.0
        try:
            poll_s = max(float(os.getenv("EYVAN_SUPERSEDE_POLL_S", "0.1")), 0.01)
        except ValueError:
            poll_s = 0.1
        deadline = time.monotonic() + timeout_s
        while True:
            observed = self.status(remote_id)
            if observed.status in _TERMINAL_STATES:
                with _file_lock(_record_lock_path(record_path)):
                    record = _load_json(record_path)
                    if record is not None:
                        self._validate_fence(remote_id, record, context)
                        self._retire_terminal_runtime(record)
                return
            if observed.status is ExecutionWorkloadStatus.INDETERMINATE:
                raise RuntimeError(f"cannot safely retire ambiguous systemd execution {remote_id!r}")
            self.stop(remote_id, context)
            if time.monotonic() >= deadline:
                break
            time.sleep(poll_s)

        with _file_lock(_record_lock_path(record_path)):
            record = _load_json(record_path)
            if record is not None and _known_persisted_status(record) not in _FINAL_STATES:
                self._mark_indeterminate(record, "systemd retirement timed out; workload ownership is ambiguous")
                _atomic_write_json(record_path, record)
        raise RuntimeError(f"cannot safely retire systemd execution {remote_id!r} before its timeout")

    def checkpoint(
        self,
        remote_id: str,
        *,
        hook: str,
        output_uri: str | None,
        context: MutationContext,
    ) -> str:
        """Run a separately fenced checkpoint hook after source validation."""

        if not isinstance(hook, str) or not hook.strip():
            raise ValueError("native-systemd checkpoint hook must be non-empty")
        record_path = self._record_path(remote_id)
        with _file_lock(_record_lock_path(record_path)):
            source = _load_json(record_path)
            if source is None:
                raise RuntimeError(f"systemd execution {remote_id!r} is absent")
            self._validate_fence(remote_id, source, context)
            try:
                source_status = _durable_observation(remote_id, source).status
            except _InvalidDurableRecord as exc:
                self._set_status(source, ExecutionWorkloadStatus.INDETERMINATE, f"invalid durable record: {exc}")
                _atomic_write_json(record_path, source)
                raise RuntimeError(f"cannot checkpoint ambiguous systemd execution {remote_id!r}") from exc
            if source_status is ExecutionWorkloadStatus.INDETERMINATE:
                raise RuntimeError(f"cannot checkpoint ambiguous systemd execution {remote_id!r}")
        inherited = source.get("environment")
        source_environment = (
            {name: value for name, value in (inherited or {}).items() if isinstance(value, str)}
            if isinstance(inherited, dict)
            else {}
        )

        # Keep the hook's rank IDENTITY, drop the cluster-forming topology. The
        # source workload is still RUNNING while its hook executes, so
        # inheriting JAX_COORDINATOR_* would make rank 0's hook bind the port the
        # live training coordinator already holds. `without_distributed_topology_overrides`
        # is the same filter the slice applies before injecting its own values.
        environment = without_distributed_topology_overrides(source_environment)
        rank = _checkpoint_rank(source_environment)
        if rank is not None:
            environment["EYVAN_CHECKPOINT_RANK"] = str(rank)
        environment.update(
            {
                "EYVAN_SOURCE_JOB_ID": remote_id,
                # Per-rank destination. JAX distributed membership does not
                # serialize object-store writes, so a single shared URI would let
                # every rank race on one destination. The unsuffixed value stays
                # available as the base so a hook can still address the set.
                "EYVAN_CHECKPOINT_URI": _rank_checkpoint_uri(output_uri, rank),
                "EYVAN_CHECKPOINT_BASE_URI": output_uri or "",
                # Per-rank profile destination, on the same plumbing and for the
                # same reason. On a TPU host the only useful profile is
                # jax.profiler/libtpu tracing, which must start *inside* the
                # training process: it needs the JAX runtime's device handles,
                # there is no ptrace equivalent for TPU device state, and libtpu
                # holds an exclusive per-host lock. So Eyvan hands the callable a
                # place to write and gets out of the way -- a platform-driven
                # ``profile_task`` is a recorded non-goal, not an omission.
                "EYVAN_PROFILE_URI": _rank_profile_uri(output_uri, rank),
            }
        )
        return self.submit(
            {
                "entrypoint": hook,
                "submission_id": context.idempotency_key,
                "environment": environment,
                "gang": {"replicas": 1, "per_replica": {"cpu": 1}},
                "metadata": {"eyvan_operation": "checkpoint", "eyvan_source_job_id": remote_id},
            },
            context,
        )

    def logs(self, remote_id: str) -> str:
        """Return the unit's journal output without treating log absence as success.

        A missing durable record, or one that never recorded a unit, yields an
        empty string because no workload output can exist. A journalctl
        failure is not an empty log: it raises ``SystemdLogsUnavailableError``
        carrying the captured stderr so callers can tell a log transport
        failure apart from a genuinely silent workload.

        Raises:
            SystemdLogsUnavailableError: If journalctl could not read the
                unit's journal.
        """

        record_path = self._record_path(remote_id)
        with _file_lock(_record_lock_path(record_path)):
            record = _load_json(record_path)
            if record is None:
                return ""
            unit_name = record.get("unit_name")
            if not isinstance(unit_name, str) or not unit_name:
                return ""
            runtime_logs = self._runtime_logs(record)
            if runtime_logs is not None:
                return runtime_logs
        result = self._run(("journalctl", "--no-pager", "--output=cat", f"--unit={unit_name}"))
        if result.returncode != 0:
            raise SystemdLogsUnavailableError(unit_name, result.stderr.strip())
        return result.stdout

    def process_status(self, remote_id: str) -> JsonObject:
        """Return live process detail for one workload attempt.

        Built for the case this stack actually hits: a multi-host job that has
        stopped making progress. ``status`` alone reports RUNNING for both a
        healthy step loop and a hung collective, so this exposes the unit's main
        PID and the cgroup's process list to tell them apart.

        ``pids`` is ``None`` -- never ``[]`` -- when the cgroup could not be
        read, preserving the same ambiguity distinction the status path relies
        on.
        """

        record_path = self._record_path(remote_id)
        with _file_lock(_record_lock_path(record_path)):
            record = _load_json(record_path)
        if record is None:
            return {"remote_id": remote_id, "observed": False, "detail": "no durable execution record"}
        unit_name = record.get("unit_name")
        if not isinstance(unit_name, str) or not unit_name:
            return {"remote_id": remote_id, "observed": False, "detail": "execution record has no unit"}

        outcome, properties, detail = self._show_unit(unit_name)
        if outcome != "present" or properties is None:
            return {
                "remote_id": remote_id,
                "unit_name": unit_name,
                "observed": False,
                "detail": detail or f"systemd unit is {outcome}",
            }
        control_group = properties.get("ControlGroup", "")
        pids = self._cgroup_pids_reader(control_group) if control_group else None
        return {
            "remote_id": remote_id,
            "unit_name": unit_name,
            "observed": True,
            "active_state": properties.get("ActiveState", ""),
            "sub_state": properties.get("SubState", ""),
            "result": properties.get("Result", ""),
            "main_pid": _optional_int(properties.get("MainPID")),
            "exec_main_status": _optional_int(properties.get("ExecMainStatus")),
            "control_group": control_group,
            "pids": None if pids is None else list(pids),
            "process_count": None if pids is None else len(pids),
            "detail": detail,
        }


def _checkpoint_rank(environment: tp.Mapping[str, str]) -> int | None:
    """Return this host's rank from a source workload environment, if it has one.

    A single-host workload has no rank; ``None`` means "not part of a slice"
    rather than "rank 0", so its checkpoint destination stays unsuffixed.
    """

    # Only the slice executor injects these. EYVAN_REPLICA_INDEX is deliberately
    # excluded: `submit` sets it from the gang's replica count on every workload,
    # so it is 0 for an ordinary single-host job and would shadow the real rank.
    for name in ("EYVAN_DISTRIBUTED_PROCESS_ID", "JAX_PROCESS_ID"):
        raw = environment.get(name)
        if raw is None:
            continue
        try:
            rank = int(raw)
        except ValueError:
            continue
        if rank >= 0:
            return rank
    return None


def _rank_checkpoint_uri(output_uri: str | None, rank: int | None) -> str:
    """Return a per-rank checkpoint destination derived from a shared URI."""

    if not output_uri or rank is None:
        return output_uri or ""
    return f"{output_uri.rstrip('/')}/rank-{rank}"


def _rank_profile_uri(output_uri: str | None, rank: int | None) -> str:
    """Return a per-rank profile destination beside the checkpoint destination.

    Separate prefix, same base: a profile is not a checkpoint, and writing both
    under one path would make a resume read trace files as state.
    """

    if not output_uri:
        return ""
    base = f"{output_uri.rstrip('/')}/profiles"
    return base if rank is None else f"{base}/rank-{rank}"


def _optional_int(value: str | None) -> int | None:
    """Parse a systemd integer property without inventing a value."""

    if value is None or not value.strip():
        return None
    try:
        return int(value.strip())
    except ValueError:
        return None


__all__ = (
    "SystemdCommandResult",
    "SystemdLogsUnavailableError",
    "SystemdTransientServiceExecutor",
    "systemd_unit_name",
)
