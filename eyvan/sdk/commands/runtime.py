# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""One-time local setup for Eyvan's native GCP runtime."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import typing as tp
from dataclasses import dataclass
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from eyvan._internal.json_types import JsonObject
from eyvan.capacity.autoscale import AutoscaleConfigCache, validate_gcp_project_id
from eyvan.capacity.providers.gcp.runtime.bootstrap import (
    GcpRuntimeBootstrap,
    GcpRuntimeBootstrapError,
    GcpRuntimePreflight,
)
from eyvan.capacity.providers.gcp.runtime.profile import (
    DEFAULT_GCP_NATIVE_RUNTIME_PROFILE_PATH,
    GcpNativeRuntimeProfile,
    GcpNativeRuntimeProfileError,
    GcpNativeRuntimeProfileStore,
)
from eyvan.capacity.providers.gcp.runtime.supervisor import LocalAgentState, LocalAgentSupervisor
from eyvan.sdk.client.endpoint import DEFAULT_CONTROL_PLANE_URL, ControlPlaneEndpoint
from eyvan.sdk.commands.autoscale import autoscale_console, gcloud_default_project, is_interactive
from eyvan.sdk.commands.state import CliSession
from eyvan.server.local.daemon import LocalDaemonError, LocalDaemonResult, LocalDaemonSupervisor

_DEFAULT_BUILD_TIMEOUT_S = 300.0
_VALIDATION_AGENT_SECRET = "a" * 32
_VALIDATION_NATIVE_WORKER_SECRET = "b" * 32


class NativeWorkerWheelBuildError(OSError):
    """The local Eyvan wheel build required for native workers did not complete."""


class GcpRuntimeInitializationError(RuntimeError):
    """The bootstrapper did not return the complete profile contract required by the CLI."""


class GcpRuntimeRestartError(RuntimeError):
    """A private native GCP runtime cannot be safely restarted."""


class UvBuildRunner(tp.Protocol):
    """Typed boundary for the local ``uv build`` invocation."""

    def __call__(
        self,
        args: tp.Sequence[str],
        *,
        cwd: Path,
        check: bool,
        capture_output: bool,
        timeout: float,
    ) -> subprocess.CompletedProcess[bytes]:
        """Run one local build command."""

        ...


class RuntimeProfileStore(tp.Protocol):
    """Private profile persistence required by native runtime initialization."""

    def load_or_create(
        self,
        *,
        project_id: str,
        artifact_root_uri: str,
        native_worker_wheel_uri: str | None = None,
    ) -> GcpNativeRuntimeProfile:
        """Load the stable profile or create its owner-only credentials once."""

        ...

    def save(self, profile: GcpNativeRuntimeProfile) -> Path:
        """Persist the staged-wheel profile atomically."""

        ...


class RuntimeBootstrapper(tp.Protocol):
    """Typed preparation contract used by the runtime setup service."""

    def preflight(self, profile: GcpNativeRuntimeProfile) -> GcpRuntimePreflight:
        """Check GCP project, storage, and TPU access without creating capacity."""

        ...

    def stage_native_worker_wheel(self, profile: GcpNativeRuntimeProfile) -> GcpNativeRuntimeProfile:
        """Stage the content-addressed Eyvan native-worker wheel."""

        ...


def _run_uv_build(
    args: tp.Sequence[str],
    *,
    cwd: Path,
    check: bool,
    capture_output: bool,
    timeout: float,
) -> subprocess.CompletedProcess[bytes]:
    """Run a bounded local wheel build without streaming its output into the CLI."""

    return subprocess.run(args, cwd=cwd, check=check, capture_output=capture_output, timeout=timeout)


class UvNativeWorkerWheelBuilder:
    """Build one temporary Eyvan wheel from the installed source checkout.

    The wheel only lives long enough for ``GcpRuntimeBootstrap`` to hash and
    stage it under an immutable GCS object name. It is never placed in the
    runtime profile or passed to a daemon command line.
    """

    def __init__(
        self,
        *,
        package_root: Path | None = None,
        runner: UvBuildRunner = _run_uv_build,
        timeout_s: float = _DEFAULT_BUILD_TIMEOUT_S,
    ):
        if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)) or timeout_s <= 0:
            raise ValueError("timeout_s must be a positive number")
        self.package_root = (package_root or Path(__file__).resolve().parents[2]).resolve()
        self._runner = runner
        self.timeout_s = float(timeout_s)
        self._output_directory: Path | None = None

    def build(self) -> Path:
        """Build and return the single wheel produced in a private temporary directory."""

        self.close()
        if not self.package_root.is_dir():
            raise NativeWorkerWheelBuildError(f"Eyvan package root {self.package_root} does not exist")
        output_directory = Path(tempfile.mkdtemp(prefix="eyvan-native-worker-"))
        self._output_directory = output_directory
        command = ("uv", "build", "--wheel", "--out-dir", str(output_directory), str(self.package_root))
        try:
            completed = self._runner(
                command,
                cwd=self.package_root,
                check=False,
                capture_output=True,
                timeout=self.timeout_s,
            )
        except FileNotFoundError as exc:
            raise NativeWorkerWheelBuildError("uv is not installed or is not on PATH") from exc
        except subprocess.TimeoutExpired as exc:
            raise NativeWorkerWheelBuildError(f"uv build exceeded {self.timeout_s:g}s") from exc
        except OSError as exc:
            raise NativeWorkerWheelBuildError(f"could not start uv build: {exc}") from exc
        if completed.returncode != 0:
            detail = _process_output(completed)
            message = "uv build failed"
            if detail:
                message = f"{message}: {detail[:800]}"
            raise NativeWorkerWheelBuildError(message)
        wheels = tuple(sorted(output_directory.glob("*.whl")))
        if len(wheels) != 1:
            raise NativeWorkerWheelBuildError(
                f"uv build must produce exactly one Eyvan wheel, found {len(wheels)} in {output_directory}"
            )
        return wheels[0]

    def close(self) -> None:
        """Remove the temporary local wheel directory once staging is complete."""

        output_directory = self._output_directory
        self._output_directory = None
        if output_directory is not None:
            shutil.rmtree(output_directory, ignore_errors=True)


@dataclass(frozen=True, slots=True)
class RuntimeInitializationResult:
    """Non-secret result of preparing one local native GCP runtime."""

    profile_path: Path
    profile: GcpNativeRuntimeProfile
    preflight: GcpRuntimePreflight

    def as_document(self) -> JsonObject:
        """Return automation-safe setup output without local credentials."""

        native_worker_wheel_uri = self.profile.native_worker_wheel_uri
        if native_worker_wheel_uri is None:
            raise AssertionError("a staged native runtime profile must include a worker wheel URI")
        return {
            "artifact_root_uri": self.profile.artifact_root_uri,
            "checks": [step.value for step in self.preflight.checks],
            "native_worker_wheel_uri": native_worker_wheel_uri,
            "profile_path": str(self.profile_path),
            "project_id": self.profile.project_id,
        }


def replacement_trusted_callable_projects() -> tuple[str, ...]:
    """Return the callable trust the replacement control plane will start with.

    The supervisor launches the new process with the *invoking* environment, so
    this is read here rather than from the outgoing process -- a POSIX process
    cannot recover another's environment, and inventing one would be worse than
    reporting the one that will actually apply.
    """

    raw = os.getenv("EYVAN_TRUSTED_CALLABLE_PROJECTS", "")
    return tuple(part.strip() for part in raw.split(",") if part.strip())


@dataclass(frozen=True, slots=True)
class RuntimeRestartResult:
    """Non-secret result of restarting the local native GCP API-and-agent runtime."""

    endpoint: ControlPlaneEndpoint
    control_plane: LocalDaemonResult
    agent_endpoint: ControlPlaneEndpoint
    agent_state: LocalAgentState
    trusted_callable_projects: tuple[str, ...] = ()

    @property
    def callable_trust_warning(self) -> str | None:
        """Warn when the replacement will refuse the callables the operator submits.

        ``EYVAN_TRUSTED_CALLABLE_PROJECTS`` is read once at control-plane start,
        so restarting from a shell that does not export it silently narrows the
        runtime: the restart reports success and the loss only surfaces later as
        a 403 on the next callable submission, which names the workload rather
        than the restart that caused it.
        """

        if self.trusted_callable_projects:
            return None
        return (
            "EYVAN_TRUSTED_CALLABLE_PROJECTS is unset, so this control plane trusts callables only in the "
            "default loopback project; submissions from any other org/project will be refused with 403. "
            "Export it before restarting to keep the previous trust."
        )

    def as_document(self) -> JsonObject:
        """Return automation-safe restart output without profile credentials."""

        return {
            "agent": {
                "endpoint": self.agent_endpoint.url,
                "state": self.agent_state.value,
            },
            "callable_trust_warning": self.callable_trust_warning,
            "control_plane": {
                "endpoint": self.control_plane.endpoint.url,
                "pid": self.control_plane.pid,
                "state": self.control_plane.state.value,
            },
            "endpoint": self.endpoint.url,
            "trusted_callable_projects": list(self.trusted_callable_projects),
        }


class GcpRuntimeInitializer:
    """Create private runtime credentials once, preflight GCP, and stage Eyvan's worker wheel."""

    def __init__(self, profile_store: RuntimeProfileStore, bootstrap: RuntimeBootstrapper):
        self._profile_store = profile_store
        self._bootstrap = bootstrap

    def initialize(self, *, project_id: str, artifact_root_uri: str) -> RuntimeInitializationResult:
        """Prepare a reusable native runtime without provisioning a TPU."""

        profile = self._profile_store.load_or_create(
            project_id=project_id,
            artifact_root_uri=artifact_root_uri,
        )
        preflight = self._bootstrap.preflight(profile)
        staged_profile = self._bootstrap.stage_native_worker_wheel(profile)
        if staged_profile.native_worker_wheel_uri is None:
            raise GcpRuntimeInitializationError("native runtime wheel staging completed without a worker wheel URI")
        profile_path = self._profile_store.save(staged_profile)
        return RuntimeInitializationResult(profile_path=profile_path, profile=staged_profile, preflight=preflight)


def suggested_runtime_project(*, defaults_path: Path | None, cache_dir: Path | None) -> str | None:
    """Return a valid autoscale project first, then the active gcloud project when available."""

    try:
        defaults = AutoscaleConfigCache(cache_dir).load_defaults(defaults_path)
    except ValueError:
        defaults = None
    if defaults is not None and defaults.project is not None:
        try:
            return validate_gcp_project_id(defaults.project)
        except ValueError:
            pass
    candidate = gcloud_default_project()
    if candidate is None:
        return None
    try:
        return validate_gcp_project_id(candidate)
    except ValueError:
        return None


def normalize_runtime_artifact_root(project_id: str, artifact_root_uri: str) -> str:
    """Validate public artifact input through the authoritative runtime-profile schema.

    The transient profile is never persisted. Its placeholder secrets exist only
    because the profile schema deliberately validates all fields together.
    """

    return GcpNativeRuntimeProfile(
        project_id=project_id,
        artifact_root_uri=artifact_root_uri,
        agent_shared_secret=_VALIDATION_AGENT_SECRET,
        native_worker_root_secret=_VALIDATION_NATIVE_WORKER_SECRET,
    ).artifact_root_uri


def _process_output(result: subprocess.CompletedProcess[bytes]) -> str:
    """Decode bounded local build diagnostics consistently."""

    return b"\n".join((result.stderr or b"", result.stdout or b"")).decode("utf-8", "replace").strip()


def _prompt_project(console: Console, default: str | None) -> str:
    """Prompt until the GCP project is a canonical project ID."""

    while True:
        candidate = Prompt.ask("GCP project ID", default=default, console=console).strip()
        try:
            return validate_gcp_project_id(candidate)
        except ValueError as exc:
            console.print(f"[bold red]Invalid GCP project[/]: {exc}")


def _prompt_artifact_root(console: Console, project_id: str, default: str | None) -> str:
    """Prompt until the GCS root is accepted by the shared profile schema."""

    while True:
        candidate = Prompt.ask("GCS artifact root", default=default, console=console).strip()
        try:
            return normalize_runtime_artifact_root(project_id, candidate)
        except GcpNativeRuntimeProfileError as exc:
            console.print(f"[bold red]Invalid GCS artifact root[/]: {exc}")


def _render_runtime_review(
    console: Console,
    *,
    project_id: str,
    artifact_root_uri: str,
    profile_path: Path,
) -> None:
    """Render only public setup choices before any cloud-facing work begins."""

    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_column("Setting", style="bold cyan", min_width=22)
    table.add_column("Value", style="white", overflow="fold")
    table.add_row("GCP project", project_id)
    table.add_row("GCS artifact root", artifact_root_uri)
    table.add_row("Private profile", str(profile_path))
    console.print(Panel(table, title="[bold cyan]Review native GCP runtime[/]", border_style="cyan"))


def _render_runtime_ready(console: Console, result: RuntimeInitializationResult) -> None:
    """Render the initialized runtime without showing local credential material."""

    payload = result.as_document()
    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_column("Setting", style="bold cyan", min_width=22)
    table.add_column("Value", style="white", overflow="fold")
    table.add_row("GCP project", result.profile.project_id)
    table.add_row("GCS artifact root", result.profile.artifact_root_uri)
    table.add_row("Native worker package", str(payload["native_worker_wheel_uri"]))
    table.add_row("Private profile", str(result.profile_path))
    console.print(Panel(table, title="[bold green]Native GCP runtime ready[/]", border_style="green"))


def _render_runtime_restart_review(
    console: Console,
    *,
    endpoint: ControlPlaneEndpoint,
    profile_path: Path,
) -> None:
    """Render the public target of one explicit local daemon replacement."""

    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_column("Setting", style="bold cyan", min_width=22)
    table.add_column("Value", style="white", overflow="fold")
    table.add_row("Control plane", endpoint.url)
    table.add_row("Private profile", str(profile_path))
    console.print(Panel(table, title="[bold yellow]Restart native GCP runtime[/]", border_style="yellow"))


def _render_runtime_restarted(console: Console, result: RuntimeRestartResult) -> None:
    """Render a completed runtime restart without displaying profile credentials."""

    table = Table(show_header=False, box=None, pad_edge=False)
    table.add_column("Service", style="bold cyan", min_width=22)
    table.add_column("State", style="white", overflow="fold")
    table.add_row("Control plane", result.control_plane.state.value)
    table.add_row("Cell agent", result.agent_state.value)
    if result.trusted_callable_projects:
        table.add_row("Trusted callables", ", ".join(result.trusted_callable_projects))
    console.print(Panel(table, title="[bold green]Native GCP runtime restarted[/]", border_style="green"))
    warning = result.callable_trust_warning
    if warning is not None:
        console.print(Panel(warning, title="[bold yellow]Callable trust narrowed[/]", border_style="yellow"))


def _initialize_native_runtime(
    *,
    project_id: str,
    artifact_root_uri: str,
    profile_path: Path,
) -> RuntimeInitializationResult:
    """Build, preflight, stage, and persist one native GCP runtime profile."""

    wheel_builder = UvNativeWorkerWheelBuilder()
    try:
        initializer = GcpRuntimeInitializer(
            GcpNativeRuntimeProfileStore(profile_path),
            GcpRuntimeBootstrap(wheel_builder),
        )
        return initializer.initialize(project_id=project_id, artifact_root_uri=artifact_root_uri)
    finally:
        wheel_builder.close()


def _restart_native_runtime(
    *,
    endpoint: ControlPlaneEndpoint,
    profile_path: Path,
) -> RuntimeRestartResult:
    """Replace a verified local API and then prepare its matching native GCP agent."""

    profile_store = GcpNativeRuntimeProfileStore(profile_path)
    try:
        profile = profile_store.load()
    except FileNotFoundError as exc:
        raise GcpRuntimeRestartError("the native GCP runtime is not initialized; run `eyvan runtime init`") from exc
    except GcpNativeRuntimeProfileError as exc:
        raise GcpRuntimeRestartError(f"the private native GCP runtime profile is invalid: {exc}") from exc
    if profile.native_worker_wheel_uri is None:
        raise GcpRuntimeRestartError(
            "the native GCP runtime has no staged Eyvan worker package; run `eyvan runtime init`"
        )
    control_plane_supervisor = LocalDaemonSupervisor()
    control_plane = control_plane_supervisor.restart(endpoint, runtime_profile_path=profile_store.path)
    try:
        agent = LocalAgentSupervisor().restart(profile_store.path, profile)
    except Exception as agent_error:
        try:
            control_plane_supervisor.stop_started(control_plane, runtime_profile_path=profile_store.path)
        except LocalDaemonError as cleanup_error:
            raise GcpRuntimeRestartError(
                "Eyvan could not restart its local GCP cell agent and could not stop the newly restarted control "
                f"plane: {cleanup_error}"
            ) from agent_error
        raise
    return RuntimeRestartResult(
        trusted_callable_projects=replacement_trusted_callable_projects(),
        endpoint=endpoint,
        control_plane=control_plane,
        agent_endpoint=agent.endpoint,
        agent_state=agent.state,
    )


def _runtime_restart_endpoint(context: click.Context) -> ControlPlaneEndpoint:
    """Resolve the root CLI endpoint while keeping direct command tests self-contained."""

    root_session = context.find_root().obj
    if isinstance(root_session, CliSession):
        return ControlPlaneEndpoint.parse(root_session.base_url)
    return ControlPlaneEndpoint.parse(DEFAULT_CONTROL_PLANE_URL)


@click.group("runtime")
def runtime_group() -> None:
    """Prepare the local native GCP runtime used by Eyvan fleets."""


@runtime_group.command("init")
@click.option("--project", "project_id", help="GCP project ID. Defaults to autoscale defaults or active gcloud project.")
@click.option("--artifact-root", help="GCS bucket or prefix used for Eyvan artifacts and native worker staging.")
@click.option(
    "--profile",
    "profile_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Override the private ~/.eyvan/gcp-native-runtime.json profile path.",
)
@click.option(
    "--defaults",
    "defaults_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Read the project suggestion from this autoscale defaults YAML file.",
)
@click.option("--cache-dir", type=click.Path(path_type=Path, file_okay=False), help="Override ~/.eyvan/autoscale.")
@click.option("--yes", is_flag=True, help="Skip the interactive review confirmation.")
@click.option("--json", "as_json", is_flag=True, help="Emit the non-secret initialized runtime summary as JSON.")
def initialize_runtime(
    project_id: str | None,
    artifact_root: str | None,
    profile_path: Path | None,
    defaults_path: Path | None,
    cache_dir: Path | None,
    yes: bool,
    as_json: bool,
) -> None:
    """Create private local credentials and stage Eyvan's native GCP worker wheel."""

    console = autoscale_console(err=as_json)
    interactive = is_interactive()
    if interactive:
        console.print(
            Panel(
                "Choose the GCP project and GCS root once. Eyvan creates local private credentials automatically.",
                title="[bold cyan]Eyvan Native GCP Runtime[/]",
                border_style="cyan",
            )
        )
    if project_id is None:
        project_id = suggested_runtime_project(defaults_path=defaults_path, cache_dir=cache_dir)
    if project_id is None:
        if not interactive:
            raise click.ClickException(
                "could not resolve a GCP project; pass --project or run `eyvan autoscale init` in a terminal"
            )
        project_id = _prompt_project(console, default=None)
    else:
        try:
            project_id = validate_gcp_project_id(project_id)
        except ValueError as exc:
            raise click.ClickException(f"invalid GCP project: {exc}") from exc

    if artifact_root is None:
        if not interactive:
            raise click.ClickException("runtime init requires --artifact-root outside a terminal")
        artifact_root = _prompt_artifact_root(console, project_id, default=None)
    else:
        try:
            artifact_root = normalize_runtime_artifact_root(project_id, artifact_root)
        except GcpNativeRuntimeProfileError as exc:
            raise click.ClickException(f"invalid GCS artifact root: {exc}") from exc

    resolved_profile_path = GcpNativeRuntimeProfileStore(
        profile_path if profile_path is not None else DEFAULT_GCP_NATIVE_RUNTIME_PROFILE_PATH
    ).path
    if not yes:
        if not interactive:
            raise click.ClickException("runtime init requires --yes outside a terminal")
        _render_runtime_review(
            console,
            project_id=project_id,
            artifact_root_uri=artifact_root,
            profile_path=resolved_profile_path,
        )
        if not Confirm.ask("Initialize this runtime", default=True, console=console):
            raise click.Abort()

    try:
        result = _initialize_native_runtime(
            project_id=project_id,
            artifact_root_uri=artifact_root,
            profile_path=resolved_profile_path,
        )
    except (
        GcpNativeRuntimeProfileError,
        GcpRuntimeBootstrapError,
        NativeWorkerWheelBuildError,
        GcpRuntimeInitializationError,
    ) as exc:
        raise click.ClickException(f"could not initialize native GCP runtime: {exc}") from exc
    if as_json:
        click.echo(json.dumps(result.as_document(), indent=2, sort_keys=True))
    else:
        _render_runtime_ready(console, result)


@runtime_group.command("restart")
@click.option(
    "--profile",
    "profile_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Override the private ~/.eyvan/gcp-native-runtime.json profile path.",
)
@click.option("--yes", is_flag=True, help="Confirm replacement of a verified local Eyvan control plane.")
@click.option("--json", "as_json", is_flag=True, help="Emit the non-secret restart summary as JSON.")
@click.pass_context
def restart_runtime(
    context: click.Context,
    profile_path: Path | None,
    yes: bool,
    as_json: bool,
) -> None:
    """Replace the locally supervised API with the selected native GCP runtime profile."""

    console = autoscale_console(err=as_json)
    endpoint = _runtime_restart_endpoint(context)
    resolved_profile_path = GcpNativeRuntimeProfileStore(
        profile_path if profile_path is not None else DEFAULT_GCP_NATIVE_RUNTIME_PROFILE_PATH
    ).path
    if not yes:
        if not is_interactive():
            raise click.ClickException("runtime restart requires --yes outside a terminal")
        _render_runtime_restart_review(
            console,
            endpoint=endpoint,
            profile_path=resolved_profile_path,
        )
        if not Confirm.ask("Restart this local runtime", default=False, console=console):
            raise click.Abort()
    try:
        result = _restart_native_runtime(endpoint=endpoint, profile_path=resolved_profile_path)
    except (GcpRuntimeRestartError, LocalDaemonError) as exc:
        raise click.ClickException(f"could not restart native GCP runtime: {exc}") from exc
    if as_json:
        click.echo(json.dumps(result.as_document(), indent=2, sort_keys=True))
    else:
        _render_runtime_restarted(console, result)


def register_runtime_commands(group: click.Group) -> None:
    """Attach native GCP runtime setup commands to the root CLI."""

    group.add_command(runtime_group)


__all__ = (
    "GcpRuntimeInitializationError",
    "GcpRuntimeInitializer",
    "GcpRuntimeRestartError",
    "NativeWorkerWheelBuildError",
    "RuntimeInitializationResult",
    "RuntimeRestartResult",
    "UvNativeWorkerWheelBuilder",
    "initialize_runtime",
    "normalize_runtime_artifact_root",
    "register_runtime_commands",
    "restart_runtime",
    "runtime_group",
    "suggested_runtime_project",
)
