# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Private local supervision for Eyvan's native GCP API-and-agent runtime."""

from __future__ import annotations

import os
import shlex
import signal
import subprocess
import sys
import time
import typing as tp
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from pydantic import ValidationError

from eyvan._internal.compat import StrEnum
from eyvan.capacity.providers.gcp.runtime.profile import (
    GcpNativeRuntimeProfile,
    GcpNativeRuntimeProfileError,
    GcpNativeRuntimeProfileStore,
)
from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
from eyvan.server.local.daemon import (
    DEFAULT_LOCAL_CELL_AGENT_URL,
    Clock,
    LocalDaemonConfigurationError,
    LocalDaemonError,
    LocalDaemonPaths,
    LocalDaemonProcess,
    LocalDaemonRecord,
    LocalDaemonResult,
    LocalDaemonState,
    LocalDaemonSupervisor,
    LocalDaemonTarget,
    ProcessKiller,
    Sleeper,
    VerifiedProcessStopOutcome,
    clear_owner_only_record,
    force_kill_process,
    open_owner_only_log,
    owner_only_startup_lock,
    process_is_alive,
    read_owner_only_record,
    stop_owned_process,
    stop_verified_process,
    write_owner_only_record,
)
from eyvan.worker.agent.client import CellAgentClient, CellAgentUnavailableError
from eyvan.worker.agent.models import AgentCapabilities

if tp.TYPE_CHECKING:
    from eyvan.capacity.fleet_config import EyvanFleetConfig

# Compatibility export for callers that imported the initial native-runtime name.
DEFAULT_LOCAL_GCP_AGENT_URL = DEFAULT_LOCAL_CELL_AGENT_URL


class GcpRuntimeError(LocalDaemonError):
    """Base error for a local native GCP runtime that cannot safely be used."""


class GcpRuntimeProfileNotPreparedError(GcpRuntimeError):
    """The owner has not staged the native worker package required by GCP workers."""


class LocalAgentConfigurationError(GcpRuntimeError):
    """A live local cell agent does not match the selected native GCP runtime profile."""


class LocalAgentStartupError(GcpRuntimeError):
    """Eyvan could not start a usable authenticated local cell agent."""

    def __init__(self, endpoint: ControlPlaneEndpoint, log_path: Path, reason: str) -> None:
        self.endpoint = endpoint
        self.log_path = log_path
        self.reason = reason
        super().__init__(f"Eyvan could not start its local GCP cell agent at {endpoint.url}: {reason}. See {log_path}.")


class LocalAgentState(StrEnum):
    """The result of one authenticated local cell-agent readiness decision."""

    REUSED = "reused"
    STARTED = "started"


@dataclass(frozen=True, slots=True)
class LocalAgentCommand:
    """One exact subprocess invocation for the private local GCP cell agent."""

    target: LocalDaemonTarget
    log_path: Path
    runtime_profile_path: Path

    @property
    def argv(self) -> tuple[str, ...]:
        """Return an invocation carrying a profile path but never its secrets."""

        return (
            sys.executable,
            "-m",
            "eyvan.worker.agent.cli",
            "--host",
            self.target.host,
            "--port",
            str(self.target.port),
            "--runtime-profile",
            str(self.runtime_profile_path),
            "--log-level",
            "info",
        )


class LocalAgentLauncher(tp.Protocol):
    """Starts one locally supervised cell-agent process."""

    def start(self, command: LocalAgentCommand) -> LocalDaemonProcess:
        """Launch a cell agent without waiting for its authenticated capability response."""

        ...


class LocalControlPlaneSupervisor(tp.Protocol):
    """Profile-aware local API supervision used by the native GCP runtime."""

    def ensure(
        self,
        endpoint: str | ControlPlaneEndpoint,
        *,
        runtime_profile_path: Path | None = None,
    ) -> LocalDaemonResult:
        """Start or reuse the private control plane for one selected profile."""

        ...

    def stop_started(self, result: LocalDaemonResult, *, runtime_profile_path: Path) -> None:
        """Stop only a daemon started by this supervision attempt."""

        ...


AgentCapabilitiesProbe: tp.TypeAlias = tp.Callable[[ControlPlaneEndpoint, str], AgentCapabilities | None]
ProcessArgumentReader: tp.TypeAlias = tp.Callable[[int], tuple[str, ...] | None]
ProcessLiveness: tp.TypeAlias = tp.Callable[[int], bool]
ProcessTerminator: tp.TypeAlias = tp.Callable[[int], None]


@dataclass(frozen=True, slots=True)
class LocalAgentResult:
    """A ready local cell-agent process and its authenticated capabilities."""

    endpoint: ControlPlaneEndpoint
    state: LocalAgentState
    pid: int
    log_path: Path
    capabilities: AgentCapabilities


class NativeGcpCellAgentSupervisor(tp.Protocol):
    """Private cell-agent supervision required by the native GCP runtime."""

    def ensure(self, profile_path: Path, profile: GcpNativeRuntimeProfile) -> LocalAgentResult:
        """Start or reuse the authenticated agent for one selected profile."""

        ...


class NativeGcpRuntimePreparer(tp.Protocol):
    """Prepare the local private runtime needed before applying native GCP pools."""

    def ensure(self, endpoint: str | ControlPlaneEndpoint) -> GcpRuntimeResult:
        """Start or reuse the local API-and-agent pair for native GCP execution."""

        ...


@dataclass(frozen=True, slots=True)
class SubprocessLocalAgentLauncher:
    """Start the current Eyvan installation as a detached local cell agent."""

    def start(self, command: LocalAgentCommand) -> LocalDaemonProcess:
        """Start the process with owner-only log output and no inherited terminal input."""

        with open_owner_only_log(command.log_path) as log:
            return subprocess.Popen(
                command.argv,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )


@dataclass(frozen=True, slots=True)
class HttpCellAgentCapabilitiesProbe:
    """Probe the authenticated capability contract without proxying loopback traffic."""

    timeout_s: float = 0.25

    def __call__(self, endpoint: ControlPlaneEndpoint, secret: str) -> AgentCapabilities | None:
        """Return capabilities only when the selected agent accepts the selected shared secret."""

        try:
            with httpx.Client(
                base_url=endpoint.url,
                headers={"Authorization": f"Bearer {secret}"},
                timeout=self.timeout_s,
                trust_env=False,
            ) as transport:
                return CellAgentClient(endpoint.url, client=transport).capabilities()
        except (CellAgentUnavailableError, httpx.HTTPError, ValidationError, ValueError):
            # "The agent did not answer" is the expected case here -- this probe
            # runs *while* an agent is being replaced. It became a typed error
            # rather than a bare httpx one when the client grew a single
            # transport seam, and omitting it aborted `eyvan runtime restart`
            # with the very condition the probe exists to tolerate.
            return None


def _default_clock() -> Clock:
    """Return the monotonic clock used by the default local-agent supervisor."""

    return time.monotonic


def _default_sleeper() -> Sleeper:
    """Return the blocking sleep function used by the default local-agent supervisor."""

    return time.sleep


def _read_local_agent_process_arguments(pid: int) -> tuple[str, ...] | None:
    """Read one process command without invoking a shell or exposing it to users."""

    try:
        completed = subprocess.run(
            ("ps", "-ww", "-p", str(pid), "-o", "command="),
            check=False,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    command = completed.stdout.strip()
    if not command:
        return None
    try:
        return tuple(shlex.split(command))
    except ValueError:
        return None


def _terminate_local_agent_process(pid: int) -> None:
    """Request graceful shutdown after the caller verified the process identity."""

    os.kill(pid, signal.SIGTERM)


def _single_option_value(arguments: tuple[str, ...], option: str) -> str | None:
    """Return an unambiguous command-line option value, if it is present exactly once."""

    values = tuple(arguments[index + 1] for index, value in enumerate(arguments[:-1]) if value == option)
    if len(values) != 1:
        return None
    return values[0]


def _matches_local_agent_process(
    arguments: tuple[str, ...] | None,
    target: LocalDaemonTarget,
    *,
    runtime_profile_path: str,
) -> bool:
    """Return whether a recorded PID still runs the exact expected Eyvan cell agent."""

    if arguments is None:
        return False
    has_module_invocation = any(
        arguments[index : index + 2] == ("-m", "eyvan.worker.agent.cli") for index in range(len(arguments) - 1)
    )
    return (
        has_module_invocation
        and _single_option_value(arguments, "--host") == target.host
        and _single_option_value(arguments, "--port") == str(target.port)
        and _single_option_value(arguments, "--runtime-profile") == runtime_profile_path
    )


@dataclass(frozen=True, slots=True)
class LocalAgentSupervisor:
    """Start or reuse one authenticated local GCP cell agent for one private profile."""

    paths: LocalDaemonPaths = field(default_factory=LocalDaemonPaths)
    launcher: LocalAgentLauncher = field(default_factory=SubprocessLocalAgentLauncher)
    capability_probe: AgentCapabilitiesProbe = field(default_factory=HttpCellAgentCapabilitiesProbe)
    endpoint: ControlPlaneEndpoint = field(
        default_factory=lambda: ControlPlaneEndpoint.parse(DEFAULT_LOCAL_GCP_AGENT_URL)
    )
    startup_timeout_s: float = 20.0
    poll_interval_s: float = 0.1
    clock: Clock = field(default_factory=_default_clock)
    sleeper: Sleeper = field(default_factory=_default_sleeper)
    process_argument_reader: ProcessArgumentReader = _read_local_agent_process_arguments
    process_liveness: ProcessLiveness = process_is_alive
    process_terminator: ProcessTerminator = _terminate_local_agent_process
    process_killer: ProcessKiller = force_kill_process

    def __post_init__(self) -> None:
        if self.startup_timeout_s <= 0:
            raise ValueError("startup_timeout_s must be positive")
        if self.poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be positive")
        if LocalDaemonTarget.from_endpoint(self.endpoint) is None:
            raise ValueError("local GCP cell agent endpoint must be literal HTTP loopback")

    def ensure(self, profile_path: Path, profile: GcpNativeRuntimeProfile) -> LocalAgentResult:
        """Start the profile's local agent or reuse its authenticated capability contract."""

        expanded_profile_path, profile_id = self._prepared_profile(profile_path, profile)
        target = self._target()
        record_path = self.paths.agent_record_path(target)
        log_path = self.paths.agent_log_path(target)
        existing = self._existing_result(profile, profile_id, record_path, log_path)
        if existing is not None:
            return existing

        with owner_only_startup_lock(self.paths.agent_lock_path(target)):
            existing = self._existing_result(profile, profile_id, record_path, log_path)
            if existing is not None:
                return existing
            return self._start_locked(
                target=target,
                expanded_profile_path=expanded_profile_path,
                profile=profile,
                profile_id=profile_id,
                record_path=record_path,
                log_path=log_path,
            )

    def restart(self, profile_path: Path, profile: GcpNativeRuntimeProfile) -> LocalAgentResult:
        """Replace only a verified profile-owned cell agent with a freshly started one.

        This is intentionally separate from :meth:`ensure`: regular commands
        must retain an already healthy agent. A restart validates the
        owner-only PID record, its selected runtime profile, and the recorded
        process command before requesting termination.
        """

        expanded_profile_path, profile_id = self._prepared_profile(profile_path, profile)
        target = self._target()
        record_path = self.paths.agent_record_path(target)
        log_path = self.paths.agent_log_path(target)
        with owner_only_startup_lock(self.paths.agent_lock_path(target)):
            record = read_owner_only_record(record_path)
            if record is None:
                capabilities = self.capability_probe(self.endpoint, profile.agent_shared_secret)
                if capabilities is not None:
                    self._validate_capabilities(capabilities)
                    raise LocalAgentConfigurationError(
                        f"a cell agent is already listening at {self.endpoint.url}, but it is not owned by the "
                        "selected native GCP runtime profile; refusing to stop it"
                    )
            elif record.endpoint != self.endpoint:
                raise LocalAgentConfigurationError(
                    f"the recorded Eyvan cell-agent endpoint does not match {self.endpoint.url}; refusing to stop it"
                )
            elif self.process_liveness(record.pid):
                if record.runtime_profile_path != profile_id:
                    raise LocalAgentConfigurationError(
                        f"the local Eyvan cell agent at {self.endpoint.url} belongs to a different runtime profile; "
                        "refusing to stop it"
                    )
                self._stop_recorded_agent(
                    record,
                    target=target,
                    log_path=log_path,
                    expected_runtime_profile_path=profile_id,
                )
                clear_owner_only_record(record_path, pid=record.pid)
            else:
                capabilities = self.capability_probe(self.endpoint, profile.agent_shared_secret)
                if capabilities is not None:
                    self._validate_capabilities(capabilities)
                    raise LocalAgentConfigurationError(
                        f"a cell agent is already listening at {self.endpoint.url}, but its owner-only PID record "
                        "is stale; refusing to stop it"
                    )
                clear_owner_only_record(record_path, pid=record.pid)
            return self._start_locked(
                target=target,
                expanded_profile_path=expanded_profile_path,
                profile=profile,
                profile_id=profile_id,
                record_path=record_path,
                log_path=log_path,
            )

    @staticmethod
    def _prepared_profile(
        profile_path: Path,
        profile: GcpNativeRuntimeProfile,
    ) -> tuple[Path, str]:
        """Validate the staged-worker requirement and derive the immutable profile identity."""

        if profile.native_worker_wheel_uri is None:
            raise GcpRuntimeProfileNotPreparedError(
                "the native GCP runtime has no staged Eyvan worker package; run `eyvan runtime init`"
            )
        expanded_profile_path = profile_path.expanduser().resolve()
        return expanded_profile_path, str(expanded_profile_path)

    def _start_locked(
        self,
        *,
        target: LocalDaemonTarget,
        expanded_profile_path: Path,
        profile: GcpNativeRuntimeProfile,
        profile_id: str,
        record_path: Path,
        log_path: Path,
    ) -> LocalAgentResult:
        """Launch and authenticate a fresh agent while holding its owner-only startup lock."""

        command = LocalAgentCommand(
            target=target,
            log_path=log_path,
            runtime_profile_path=expanded_profile_path,
        )
        try:
            process = self.launcher.start(command)
        except OSError as exc:
            raise LocalAgentStartupError(self.endpoint, log_path, str(exc)) from exc
        try:
            write_owner_only_record(
                record_path,
                LocalDaemonRecord(
                    endpoint=self.endpoint,
                    pid=process.pid,
                    runtime_profile_path=profile_id,
                ),
            )
        except BaseException:
            stop_owned_process(process)
            raise
        return self._wait_for_ready(process, profile, record_path, log_path)

    def _existing_result(
        self,
        profile: GcpNativeRuntimeProfile,
        profile_id: str,
        record_path: Path,
        log_path: Path,
    ) -> LocalAgentResult | None:
        """Return a compatible authenticated agent or reject a conflicting live process."""

        capabilities = self.capability_probe(self.endpoint, profile.agent_shared_secret)
        record = read_owner_only_record(record_path)
        if capabilities is None:
            if record is not None and record.endpoint == self.endpoint and self.process_liveness(record.pid):
                if record.runtime_profile_path != profile_id:
                    raise LocalAgentConfigurationError(
                        f"the local Eyvan cell agent at {self.endpoint.url} is running with a different runtime profile"
                    )
                return self._wait_for_existing_agent(profile, record, log_path)
            return None
        self._validate_capabilities(capabilities)
        if record is None or record.endpoint != self.endpoint or not self.process_liveness(record.pid):
            raise LocalAgentConfigurationError(
                f"a cell agent is already listening at {self.endpoint.url}, but it is not owned by the selected "
                "native GCP runtime profile"
            )
        if record.runtime_profile_path != profile_id:
            raise LocalAgentConfigurationError(
                f"the local Eyvan cell agent at {self.endpoint.url} is running with a different runtime profile"
            )
        return LocalAgentResult(
            endpoint=self.endpoint,
            state=LocalAgentState.REUSED,
            pid=record.pid,
            log_path=log_path,
            capabilities=capabilities,
        )

    def _wait_for_existing_agent(
        self,
        profile: GcpNativeRuntimeProfile,
        record: LocalDaemonRecord,
        log_path: Path,
    ) -> LocalAgentResult:
        """Wait for another supervisor's owned matching agent instead of racing a second bind."""

        deadline = self.clock() + self.startup_timeout_s
        while True:
            capabilities = self.capability_probe(self.endpoint, profile.agent_shared_secret)
            if capabilities is not None:
                self._validate_capabilities(capabilities)
                return LocalAgentResult(
                    endpoint=self.endpoint,
                    state=LocalAgentState.REUSED,
                    pid=record.pid,
                    log_path=log_path,
                    capabilities=capabilities,
                )
            if not self.process_liveness(record.pid):
                clear_owner_only_record(self.paths.agent_record_path(self._target()), pid=record.pid)
                raise LocalAgentStartupError(self.endpoint, log_path, "recorded agent process exited before readiness")
            remaining = deadline - self.clock()
            if remaining <= 0:
                raise LocalAgentStartupError(
                    self.endpoint,
                    log_path,
                    "recorded agent did not become ready before timeout",
                )
            self.sleeper(min(self.poll_interval_s, remaining))

    def _wait_for_ready(
        self,
        process: LocalDaemonProcess,
        profile: GcpNativeRuntimeProfile,
        record_path: Path,
        log_path: Path,
    ) -> LocalAgentResult:
        """Wait for the started process to pass its authenticated native GCP capability probe."""

        deadline = self.clock() + self.startup_timeout_s
        while True:
            capabilities = self.capability_probe(self.endpoint, profile.agent_shared_secret)
            if capabilities is not None:
                self._validate_capabilities(capabilities)
                return LocalAgentResult(
                    endpoint=self.endpoint,
                    state=LocalAgentState.STARTED,
                    pid=process.pid,
                    log_path=log_path,
                    capabilities=capabilities,
                )
            exit_code = process.poll()
            if exit_code is not None:
                clear_owner_only_record(record_path, pid=process.pid)
                raise LocalAgentStartupError(self.endpoint, log_path, f"agent process exited with status {exit_code}")
            remaining = deadline - self.clock()
            if remaining <= 0:
                stop_owned_process(process)
                clear_owner_only_record(record_path, pid=process.pid)
                raise LocalAgentStartupError(self.endpoint, log_path, "agent did not become ready before timeout")
            self.sleeper(min(self.poll_interval_s, remaining))

    def _stop_recorded_agent(
        self,
        record: LocalDaemonRecord,
        *,
        target: LocalDaemonTarget,
        log_path: Path,
        expected_runtime_profile_path: str,
    ) -> None:
        """Terminate a PID only after its command proves it is the recorded Eyvan cell agent."""

        if not self.process_liveness(record.pid):
            return
        if not _matches_local_agent_process(
            self.process_argument_reader(record.pid),
            target,
            runtime_profile_path=expected_runtime_profile_path,
        ):
            raise LocalAgentConfigurationError(
                f"the recorded PID {record.pid} for {target.endpoint.url} is not the expected Eyvan cell-agent "
                "process; refusing to stop it"
            )
        try:
            outcome = stop_verified_process(
                record.pid,
                process_liveness=self.process_liveness,
                process_verifier=lambda pid: _matches_local_agent_process(
                    self.process_argument_reader(pid),
                    target,
                    runtime_profile_path=expected_runtime_profile_path,
                ),
                process_terminator=self.process_terminator,
                process_killer=self.process_killer,
                graceful_timeout_s=min(self.startup_timeout_s, 10.0),
                forced_timeout_s=min(self.startup_timeout_s, 5.0),
                poll_interval_s=self.poll_interval_s,
                clock=self.clock,
                sleeper=self.sleeper,
            )
        except ProcessLookupError:
            return
        except PermissionError as exc:
            raise LocalAgentConfigurationError(
                f"could not stop the recorded Eyvan cell-agent PID {record.pid}: permission denied"
            ) from exc
        except OSError as exc:
            raise LocalAgentConfigurationError(
                f"could not stop the recorded Eyvan cell-agent PID {record.pid}: {exc}"
            ) from exc
        if outcome is VerifiedProcessStopOutcome.TIMED_OUT:
            raise LocalAgentConfigurationError(
                f"the recorded Eyvan cell-agent PID {record.pid} did not exit after forced restart. See {log_path}."
            )

    def _target(self) -> LocalDaemonTarget:
        """Return the validated local target used by every agent record path."""

        target = LocalDaemonTarget.from_endpoint(self.endpoint)
        assert target is not None
        return target

    @staticmethod
    def _validate_capabilities(capabilities: AgentCapabilities) -> None:
        """Reject a reachable agent that is not the native GCP executor requested by the profile."""

        if capabilities.provider != "gcp" or capabilities.executor != "eyvan-native":
            raise LocalAgentConfigurationError(
                "the local Eyvan cell agent must expose provider 'gcp' and executor 'eyvan-native' for native GCP runs"
            )


@dataclass(frozen=True, slots=True)
class GcpRuntimeResult:
    """A fully prepared local API-and-agent pair for one native GCP profile."""

    profile: GcpNativeRuntimeProfile
    control_plane: LocalDaemonResult
    agent: LocalAgentResult


@dataclass(frozen=True, slots=True)
class GcpNativeRuntimeSupervisor:
    """Own the private local control-plane and cell-agent processes for native GCP fleets."""

    profile_store: GcpNativeRuntimeProfileStore = field(default_factory=GcpNativeRuntimeProfileStore)
    control_plane: LocalControlPlaneSupervisor = field(default_factory=LocalDaemonSupervisor)
    agent: NativeGcpCellAgentSupervisor = field(default_factory=LocalAgentSupervisor)

    def ensure(self, endpoint: str | ControlPlaneEndpoint) -> GcpRuntimeResult:
        """Load one profile, then start/reuse its API and authenticated cell agent."""

        configured_endpoint = ControlPlaneEndpoint.parse(endpoint)
        if LocalDaemonTarget.from_endpoint(configured_endpoint) is None:
            raise GcpRuntimeError(
                "the native GCP runtime requires a literal HTTP loopback control-plane URL such as http://127.0.0.1:8080"
            )
        try:
            profile = self.profile_store.load()
        except FileNotFoundError as exc:
            raise GcpRuntimeProfileNotPreparedError(
                "the native GCP runtime is not initialized; run `eyvan runtime init`"
            ) from exc
        except GcpNativeRuntimeProfileError as exc:
            raise GcpRuntimeError(f"the private native GCP runtime profile is invalid: {exc}") from exc
        if profile.native_worker_wheel_uri is None:
            raise GcpRuntimeProfileNotPreparedError(
                "the native GCP runtime has no staged Eyvan worker package; run `eyvan runtime init`"
            )
        try:
            control_plane = self.control_plane.ensure(
                configured_endpoint,
                runtime_profile_path=self.profile_store.path,
            )
        except LocalDaemonConfigurationError as exc:
            raise GcpRuntimeError(
                f"{exc}. Restart the locally supervised Eyvan control plane, then retry the native GCP command"
            ) from exc
        try:
            agent = self.agent.ensure(self.profile_store.path, profile)
        except Exception as agent_error:
            if control_plane.state is LocalDaemonState.STARTED:
                try:
                    self.control_plane.stop_started(control_plane, runtime_profile_path=self.profile_store.path)
                except LocalDaemonError as cleanup_error:
                    raise GcpRuntimeError(
                        "Eyvan could not start its local GCP cell agent and could not stop the newly started "
                        f"control plane: {cleanup_error}"
                    ) from agent_error
            raise
        return GcpRuntimeResult(profile=profile, control_plane=control_plane, agent=agent)


def ensure_gcp_native_runtime(endpoint: str | ControlPlaneEndpoint) -> GcpRuntimeResult:
    """Ensure the default private native GCP runtime for a literal loopback control plane."""

    return GcpNativeRuntimeSupervisor().ensure(endpoint)


def has_gcp_native_runtime_profile() -> bool:
    """Return whether the current user has a valid private native GCP runtime profile."""

    try:
        GcpNativeRuntimeProfileStore().load()
    except FileNotFoundError:
        return False
    except GcpNativeRuntimeProfileError as exc:
        raise GcpRuntimeError(f"the private native GCP runtime profile is invalid: {exc}") from exc
    return True


def requires_native_gcp_runtime(config: EyvanFleetConfig) -> bool:
    """Return whether an Eyvan fleet needs the private local native GCP runtime pair."""

    if config.provider.type != "gcp":
        return False
    from eyvan.capacity.drivers.base import canonical_execution_backend

    for node_type in config.pools.values():
        if node_type.agent_endpoint is not None or config.agent_endpoint is not None:
            continue
        executor = canonical_execution_backend(node_type.executor or config.executor)
        capacity_backend = (node_type.capacity_backend or node_type.default_capacity_backend).strip().lower()
        capacity_backend = capacity_backend.replace("_", "-")
        if executor == "eyvan-native" and capacity_backend in {"gcp", "gcp-compute", "gcp-tpu", "compute", "tpu"}:
            return True
    return False


__all__ = (
    "DEFAULT_LOCAL_GCP_AGENT_URL",
    "AgentCapabilitiesProbe",
    "GcpNativeRuntimeSupervisor",
    "GcpRuntimeError",
    "GcpRuntimeProfileNotPreparedError",
    "GcpRuntimeResult",
    "HttpCellAgentCapabilitiesProbe",
    "LocalAgentCommand",
    "LocalAgentConfigurationError",
    "LocalAgentLauncher",
    "LocalAgentResult",
    "LocalAgentStartupError",
    "LocalAgentState",
    "LocalAgentSupervisor",
    "LocalControlPlaneSupervisor",
    "NativeGcpCellAgentSupervisor",
    "NativeGcpRuntimePreparer",
    "SubprocessLocalAgentLauncher",
    "ensure_gcp_native_runtime",
    "has_gcp_native_runtime_profile",
    "requires_native_gcp_runtime",
)
