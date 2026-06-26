# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Authenticated HTTP transport for one Eyvan native execution worker."""

from __future__ import annotations

import argparse
import hmac
import json
import logging
import os
import typing as tp
from pathlib import Path, PurePosixPath

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from eyvan._internal.json_types import JsonObject, JsonValue, is_json_object
from eyvan.capacity.drivers.base import ExecutionDriver, ExecutionWorkloadReport, MutationContext, StaleMutationError
from eyvan.records import NativeContainerConfig, NativeRuntimeConfig
from eyvan.worker.agent.models import (
    CheckpointRequest,
    MutationConflictCode,
    NativeWorkerHealth,
    WorkloadMutationRequest,
    WorkloadReference,
    WorkloadRequest,
)
from eyvan.worker.runtime.local_process import LocalProcessExecutor
from eyvan.worker.runtime.registry import IntrospectableExecutionDriver
from eyvan.worker.runtime.systemd_transient import SystemdLogsUnavailableError, SystemdTransientServiceExecutor

logger = logging.getLogger(__name__)


def _context(request: WorkloadRequest | WorkloadMutationRequest | CheckpointRequest) -> MutationContext:
    return MutationContext(**request.mutation.model_dump())


def _environment_bool(name: str, *, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


def _runtime_environment(value: tp.Mapping[str, str] | None) -> dict[str, str]:
    """Validate an operator-owned default environment for native workloads."""

    if value is None:
        return {}
    if not isinstance(value, tp.Mapping):
        raise ValueError("native worker runtime environment must be a mapping")
    try:
        return NativeRuntimeConfig(env=dict(value)).env
    except Exception as exc:
        raise ValueError(f"native worker runtime environment is invalid: {exc}") from exc


def _load_runtime_environment(path: Path) -> dict[str, str]:
    """Read the JSON environment emitted by the native bootstrap script."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read native worker runtime environment file: {exc}") from exc
    return _runtime_environment(value)


def _container_artifact_mounts(value: JsonValue) -> tuple[str, ...]:
    """Validate the staged host paths handed to the container executor."""

    if not isinstance(value, (list, tuple)):
        raise ValueError("native worker container artifact mounts must be a list")
    mounts: list[str] = []
    for item in value:
        if not isinstance(item, str) or item != item.strip() or "\x00" in item:
            raise ValueError("native worker container artifact mounts must be trimmed NUL-free paths")
        path = PurePosixPath(item)
        if not path.is_absolute() or ".." in path.parts or str(path) == "/" or str(path).startswith("//"):
            raise ValueError("native worker container artifact mounts must be absolute non-root paths without '..'")
        mounts.append(str(path))
    if len(mounts) != len(set(mounts)):
        raise ValueError("native worker container artifact mounts must be unique")
    return tuple(mounts)


def _load_container_runtime(path: Path) -> tuple[NativeContainerConfig, tuple[str, ...]]:
    """Load the typed OCI configuration emitted by native capacity bootstrap."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"could not read native worker container runtime file: {exc}") from exc
    if not is_json_object(value) or set(value) != {"artifact_mounts", "container"}:
        raise ValueError("native worker container runtime file must contain container and artifact_mounts")
    try:
        container = NativeContainerConfig.model_validate(value["container"])
    except Exception as exc:
        raise ValueError(f"native worker container runtime is invalid: {exc}") from exc
    return container, _container_artifact_mounts(value["artifact_mounts"])


def _base_environment(runtime_env: tp.Mapping[str, str]) -> dict[str, str]:
    """Return workload defaults injected below job-specific environment values."""

    workload_directory = os.getenv("EYVAN_NATIVE_WORKLOAD_DIRECTORY", "/var/lib/eyvan/workload")
    workload_user = os.getenv("EYVAN_NATIVE_WORKLOAD_USER", "eyvan-workload")
    return {
        **runtime_env,
        "HOME": workload_directory,
        "LOGNAME": workload_user,
        "PATH": os.getenv(
            "PATH",
            "/opt/eyvan-native/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        ),
        "USER": workload_user,
        "VIRTUAL_ENV": os.getenv("VIRTUAL_ENV", "/opt/eyvan-native/venv"),
        "XDG_CACHE_HOME": os.getenv("XDG_CACHE_HOME", f"{workload_directory}/.cache"),
    }


def create_native_worker_app(
    executor: ExecutionDriver | None = None,
    *,
    state_dir: str | os.PathLike[str] | None = None,
    secret: str | None = None,
    systemd_transient: bool = False,
    systemd_exit_type_cgroup: bool = False,
    runtime_env: tp.Mapping[str, str] | None = None,
    runtime_container: NativeContainerConfig | None = None,
    container_artifact_mounts: tp.Sequence[str] = (),
) -> FastAPI:
    """Create one native execution-worker application.

    Args:
        executor: Worker-side execution driver. When omitted, a durable local
            process executor is constructed from ``state_dir`` unless
            ``systemd_transient`` selects the production systemd executor.
        state_dir: Durable execution state directory used by the default
            executor. It cannot be combined with an explicit ``executor``.
        secret: Bearer credential required by every execution API. The
            liveness endpoint remains unauthenticated.
        systemd_transient: Run workloads as cgroup-contained transient systemd
            services. Compute VM bootstrap always enables this mode; the local
            executor remains available for development and in-process tests.
        systemd_exit_type_cgroup: Use systemd 250's ``ExitType=cgroup``
            behavior. The v5p Ubuntu 22.04 image uses cgroup observation
            instead, so it does not need this newer systemd feature.
        runtime_env: Fleet-level workload environment merged below each job's
            explicit environment. It is supported by the production systemd
            executor only.
        runtime_container: Structured OCI runtime settings selected from the
            fleet YAML. It requires the production systemd worker path.
        container_artifact_mounts: Staged host artifact paths automatically
            made read-only at the same paths inside the container.

    Returns:
        The configured FastAPI application.

    Raises:
        ValueError: If explicit executor selection conflicts with default
            executor options.
    """

    if executor is not None and (
        state_dir is not None
        or systemd_transient
        or runtime_env is not None
        or runtime_container is not None
        or container_artifact_mounts
    ):
        raise ValueError("default native worker options cannot be combined with an explicit executor")
    if not isinstance(secret, str) or not secret.strip():
        raise ValueError("native worker requires a non-empty bearer secret")
    if not isinstance(systemd_exit_type_cgroup, bool):
        raise ValueError("systemd ExitType cgroup setting must be a boolean")
    runtime_env = _runtime_environment(runtime_env)
    container_artifact_mounts = _container_artifact_mounts(list(container_artifact_mounts))
    if runtime_container is not None and not isinstance(runtime_container, NativeContainerConfig):
        raise ValueError("native worker container runtime must be a NativeContainerConfig")
    if runtime_env and not systemd_transient:
        raise ValueError("native worker runtime environment requires systemd_transient")
    if runtime_container is not None and not systemd_transient:
        raise ValueError("native worker container runtime requires systemd_transient")
    if runtime_container is None and container_artifact_mounts:
        raise ValueError("native worker container artifact mounts require a container runtime")
    if executor is None:
        base_environment = _base_environment(runtime_env)
        if runtime_container is not None:
            from eyvan.worker.runtime.container import DockerContainerExecutor

            executor = DockerContainerExecutor(
                state_dir=state_dir,
                container=runtime_container,
                artifact_mounts=container_artifact_mounts,
                use_exit_type_cgroup=systemd_exit_type_cgroup,
                workload_directory=os.getenv("EYVAN_NATIVE_WORKLOAD_DIRECTORY", "/var/lib/eyvan/workload"),
                base_environment=base_environment,
            )
        elif systemd_transient:
            executor = SystemdTransientServiceExecutor(
                state_dir=state_dir,
                use_exit_type_cgroup=systemd_exit_type_cgroup,
                workload_user=os.getenv("EYVAN_NATIVE_WORKLOAD_USER", "eyvan-workload"),
                workload_group=os.getenv("EYVAN_NATIVE_WORKLOAD_GROUP", "eyvan-workload"),
                workload_directory=os.getenv("EYVAN_NATIVE_WORKLOAD_DIRECTORY", "/var/lib/eyvan/workload"),
                base_environment=base_environment,
            )
        else:
            executor = LocalProcessExecutor(state_dir=state_dir)
    app = FastAPI(title="Eyvan Native Worker", version="1.0.0", docs_url=None, redoc_url=None)
    app.state.executor = executor

    def authenticate(authorization: str | None = Header(default=None)) -> None:
        expected = f"Bearer {secret}"
        if authorization is None or not hmac.compare_digest(authorization, expected):
            raise HTTPException(status_code=401, detail="invalid native worker credential")

    @app.exception_handler(StaleMutationError)
    async def stale_mutation_handler(_request: Request, exc: StaleMutationError) -> JSONResponse:
        return JSONResponse(
            status_code=409,
            content={"detail": str(exc), "code": MutationConflictCode.STALE_MUTATION.value},
        )

    @app.exception_handler(SystemdLogsUnavailableError)
    async def logs_unavailable_handler(_request: Request, exc: SystemdLogsUnavailableError) -> JSONResponse:
        """Report a journal transport failure as a typed 503, not a 500.

        A log read that cannot reach journalctl is a transient dependency
        failure, and it must stay distinguishable from a genuinely silent
        workload -- returning an empty body would look like success.
        """

        return JSONResponse(status_code=503, content={"detail": str(exc), "code": "logs_unavailable"})

    @app.get("/healthz")
    def health() -> NativeWorkerHealth:
        return NativeWorkerHealth(status="ok", runtime="native-worker")

    @app.get("/api/v1/health", dependencies=[Depends(authenticate)])
    def authenticated_health() -> NativeWorkerHealth:
        return NativeWorkerHealth(status="ok", runtime="native-worker")

    @app.post("/api/v1/workloads", dependencies=[Depends(authenticate)])
    def submit(body: WorkloadRequest) -> WorkloadReference:
        metadata = body.spec.get("metadata")
        workload_id = metadata.get("eyvan_workload_id") if isinstance(metadata, dict) else None
        if not isinstance(workload_id, str) or not workload_id.strip():
            raise HTTPException(status_code=422, detail="native worker workload metadata requires eyvan_workload_id")
        remote_id = executor.submit(body.spec, _context(body))
        return WorkloadReference(remote_id=remote_id)

    @app.get("/api/v1/workloads/{remote_id}", dependencies=[Depends(authenticate)])
    def status(remote_id: str) -> ExecutionWorkloadReport:
        return executor.status(remote_id)

    @app.post("/api/v1/workloads/{remote_id}/stop", dependencies=[Depends(authenticate)])
    def stop(remote_id: str, body: WorkloadMutationRequest) -> ExecutionWorkloadReport:
        executor.stop(remote_id, _context(body))
        return executor.status(remote_id)

    @app.post("/api/v1/workloads/{remote_id}/retire", dependencies=[Depends(authenticate)])
    def retire(remote_id: str, body: WorkloadMutationRequest) -> ExecutionWorkloadReport:
        executor.retire(remote_id, _context(body))
        return executor.status(remote_id)

    @app.post("/api/v1/workloads/{remote_id}/checkpoint", dependencies=[Depends(authenticate)])
    def checkpoint(remote_id: str, body: CheckpointRequest) -> WorkloadReference:
        checkpoint_id = executor.checkpoint(
            remote_id,
            hook=body.hook,
            output_uri=body.output_uri,
            context=_context(body),
        )
        return WorkloadReference(remote_id=checkpoint_id)

    @app.get(
        "/api/v1/workloads/{remote_id}/logs",
        response_class=PlainTextResponse,
        dependencies=[Depends(authenticate)],
    )
    def logs(remote_id: str) -> str:
        return executor.logs(remote_id)

    @app.get(
        "/api/v1/workloads/{remote_id}/log-page",
        dependencies=[Depends(authenticate)],
        response_model=None,
    )
    def log_page(remote_id: str, after: str = "", limit: int = 200) -> JsonObject:
        """Return one resumable page of structured records.

        The executor on this host has produced these since Phase 4 -- the
        systemd runtime parses journald's own JSON, with priority, timestamp,
        PID and an opaque cursor. Nothing exposed them over the wire, so the
        control plane could only fetch `logs()`, which is those records with the
        fields thrown away, and `eyvan workload log-page` answered
        `unsupported_capability` for every fleet that runs on a remote worker.

        A 501 when the executor genuinely cannot produce pages, never an empty
        one: "this backend has no structured logs" and "this workload produced
        no output" are different answers, and a follower that cannot tell them
        apart polls forever against a backend that will never answer.
        """

        page = getattr(executor, "log_page", None)
        if page is None:
            raise HTTPException(
                status_code=501,
                detail=f"execution backend {type(executor).__name__} does not expose structured logs",
            )
        result = page(remote_id, after=after, limit=limit)
        return {
            "records": [dict(record.as_json()) for record in result.records],
            "next_cursor": result.next_cursor,
            "unavailable": result.unavailable,
            "detail": result.detail,
            "truncated_ranks": list(result.truncated_ranks),
        }

    @app.get(
        "/api/v1/workloads/{remote_id}/process-status",
        dependencies=[Depends(authenticate)],
        response_model=None,
    )
    def process_status(remote_id: str) -> JsonObject:
        """Return live process detail for debugging a stalled workload."""

        # ``isinstance`` against the declared protocol, not a getattr probe:
        # ``runtime/registry`` already establishes that seam for close and health,
        # and a third ad-hoc probe would make the pattern look optional.
        if not isinstance(executor, IntrospectableExecutionDriver):
            raise HTTPException(
                status_code=501,
                detail="this native execution backend does not expose process introspection",
            )
        return executor.process_status(remote_id)

    return app


class NativeWorkerArguments(argparse.Namespace):
    """Typed command-line arguments accepted by the native worker process."""

    host: str
    port: int
    state_dir: str | None
    secret: str | None
    secret_file: Path | None
    runtime_env_file: Path | None
    container_runtime_file: Path | None
    systemd_transient: bool


def main(argv: tp.Sequence[str] | None = None) -> int:
    """Run a native worker HTTP server."""

    parser = argparse.ArgumentParser(description="Run an Eyvan native execution worker")
    parser.add_argument("--host", default=os.getenv("EYVAN_NATIVE_WORKER_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("EYVAN_NATIVE_WORKER_PORT", "8100")))
    parser.add_argument("--state-dir", default=os.getenv("EYVAN_NATIVE_WORKER_STATE_DIR"))
    parser.add_argument("--secret", default=os.getenv("EYVAN_NATIVE_WORKER_SECRET"))
    parser.add_argument("--secret-file", type=Path)
    parser.add_argument("--runtime-env-file", type=Path)
    parser.add_argument("--container-runtime-file", type=Path)
    parser.add_argument(
        "--systemd-transient",
        action="store_true",
        help="execute workloads as cgroup-contained transient systemd services",
    )
    args = parser.parse_args(list(argv) if argv is not None else None, namespace=NativeWorkerArguments())
    if args.port <= 0 or args.port > 65535:
        parser.error("--port must be between 1 and 65535")
    if args.secret is not None and args.secret_file is not None:
        parser.error("set only one of --secret or --secret-file")
    try:
        systemd_exit_type_cgroup = _environment_bool("EYVAN_SYSTEMD_EXIT_TYPE_CGROUP", default=False)
    except ValueError as exc:
        parser.error(str(exc))
    secret = args.secret
    # The secret must not be inherited by workload child processes.
    os.environ.pop("EYVAN_NATIVE_WORKER_SECRET", None)
    runtime_env: dict[str, str] = {}
    if args.runtime_env_file is not None:
        try:
            runtime_env = _load_runtime_environment(args.runtime_env_file)
        except ValueError as exc:
            parser.error(str(exc))
    runtime_container: NativeContainerConfig | None = None
    container_artifact_mounts: tuple[str, ...] = ()
    if args.container_runtime_file is not None:
        try:
            runtime_container, container_artifact_mounts = _load_container_runtime(args.container_runtime_file)
        except ValueError as exc:
            parser.error(str(exc))
    if args.secret_file is not None:
        try:
            secret = args.secret_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            parser.error(f"could not read --secret-file: {exc}")
        try:
            args.secret_file.unlink()
        except OSError as exc:
            logger.warning("could not remove native worker secret file %s: %s", args.secret_file, exc)

    import uvicorn

    uvicorn.run(
        create_native_worker_app(
            state_dir=args.state_dir,
            secret=secret,
            systemd_transient=args.systemd_transient,
            systemd_exit_type_cgroup=systemd_exit_type_cgroup,
            runtime_env=runtime_env if args.runtime_env_file is not None else None,
            runtime_container=runtime_container,
            container_artifact_mounts=container_artifact_mounts,
        ),
        host=args.host,
        port=args.port,
    )
    return 0


__all__ = ("create_native_worker_app", "main")


if __name__ == "__main__":
    raise SystemExit(main())
