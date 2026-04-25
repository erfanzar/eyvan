# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Ray-like callable submission primitives backed only by native Eyvan APIs."""

from __future__ import annotations

import functools
import hashlib
import hmac
import inspect
import os
import threading
import typing as tp
import uuid
from dataclasses import dataclass, field

from eyvan._internal.workspace import build_workspace
from eyvan.artifacts import (
    ArtifactStore,
    EyvanManagedArtifactStore,
    GCSArtifactStore,
    artifact_store_from_uri,
)
from eyvan.capacity.providers.gcp.runtime.profile import (
    GcpNativeRuntimeProfile,
    GcpNativeRuntimeProfileError,
    GcpNativeRuntimeProfileStore,
)
from eyvan.capacity.providers.gcp.runtime.supervisor import ensure_gcp_native_runtime
from eyvan.sdk.client.callables import EyvanCallableSubmitter
from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
from eyvan.sdk.client.identity import resolve_control_plane_identity
from eyvan.sdk.client.jobs import (
    AcceleratorConfig,
    CallableSubmission,
    CallableSubmitter,
    EyvanJob,
    EyvanJobError,
    EyvanPlacementError,
    JobBackend,
    JobCompletion,
    TpuConfig,
    reject_remote_callable_signing_key,
)
from eyvan.sdk.client.transport import EyvanUnavailableError
from eyvan.sdk.contexts import (
    DEFAULT_BASE_URL,
    DEFAULT_ORG_ID,
    DEFAULT_PROJECT_ID,
    context_config_path,
    get_active_context,
)
from eyvan.server.local.daemon import ensure_loopback_control_plane
from eyvan.worker.runtime.callables import dump_callable

P = tp.ParamSpec("P")
R = tp.TypeVar("R")
R_co = tp.TypeVar("R_co", covariant=True)


@dataclass(frozen=True, slots=True)
class ExecutionContext(tp.Generic[R_co]):
    """Process-wide defaults used by decorated Eyvan callables."""

    fleet: str | None
    org_id: str
    project_id: str
    artifact_store: ArtifactStore
    submitter: CallableSubmitter[R_co] | None = None
    signing_key: bytes | None = field(default=None, repr=False)


_context: ExecutionContext[tp.NoReturn] | None = None
_context_lock = threading.RLock()


def _required(value: str | None, *, argument: str, env_name: str) -> str:
    normalized = value.strip() if value else ""
    if not normalized:
        raise ValueError(f"Eyvan init requires {argument} or {env_name}")
    return normalized


def _optional(value: str | None) -> str | None:
    """Return a trimmed string or None when empty."""

    normalized = value.strip() if value else ""
    return normalized if normalized else None


def _configured_gcp_runtime_profile(endpoint: ControlPlaneEndpoint) -> GcpNativeRuntimeProfile | None:
    """Load the private GCP profile selected by a loopback execution context, if any."""

    if not endpoint.is_loopback:
        return None
    try:
        return GcpNativeRuntimeProfileStore().load()
    except FileNotFoundError:
        return None
    except GcpNativeRuntimeProfileError as exc:
        raise RuntimeError(f"Eyvan native GCP runtime profile is invalid: {exc}") from exc


def _prepared_gcp_runtime_profile(
    endpoint: ControlPlaneEndpoint,
    profile: GcpNativeRuntimeProfile | None,
) -> GcpNativeRuntimeProfile | None:
    """Start the private GCP runtime only after its owner has completed one-time setup."""

    if profile is None:
        return None
    return ensure_gcp_native_runtime(endpoint).profile


def _validate_native_gcp_artifact_override(
    artifact_uri: str | None,
    artifact_store: ArtifactStore | None,
) -> None:
    """Require callable inputs and results to remain reachable from native GCP workers."""

    if artifact_uri is not None and not artifact_uri.startswith("gs://"):
        raise ValueError(
            "the local native GCP runtime requires GCS-backed callable artifacts; omit artifact_uri to use its "
            "configured GCS root, or pass a gs:// URI"
        )
    if artifact_store is not None and not isinstance(artifact_store, GCSArtifactStore):
        raise ValueError(
            "the local native GCP runtime requires a GCSArtifactStore; omit artifact_store to use its configured "
            "GCS root, or pass a GCSArtifactStore"
        )


def init(
    *,
    fleet: str | None = None,
    org_id: str | None = None,
    project_id: str | None = None,
    artifact_uri: str | None = None,
    artifact_store: ArtifactStore | None = None,
    submitter: CallableSubmitter[tp.NoReturn] | None = None,
    signing_key: bytes | str | None = None,
    base_url: str | None = None,
    token: str | None = None,
    dev_user: str | None = None,
    environ: tp.Mapping[str, str] | None = None,
) -> ExecutionContext[tp.NoReturn]:
    """Configure callable execution, using ``EYVAN_*`` environment fallbacks."""

    environment = os.environ if environ is None else environ
    active_context = get_active_context(context_config_path(environment))
    configured_fleet = _optional(
        fleet or environment.get("EYVAN_FLEET") or (active_context.fleet if active_context else None)
    )
    configured_org = (
        _optional(org_id or environment.get("EYVAN_ORG") or (active_context.org_id if active_context else None))
        or DEFAULT_ORG_ID
    )
    configured_project = (
        _optional(
            project_id or environment.get("EYVAN_PROJECT") or (active_context.project_id if active_context else None)
        )
        or DEFAULT_PROJECT_ID
    )
    if artifact_uri is not None and artifact_store is not None:
        raise ValueError("Eyvan init accepts artifact_uri or artifact_store, not both")
    configured_key = signing_key if signing_key is not None else environment.get("EYVAN_CALLABLE_SIGNING_KEY")
    if isinstance(configured_key, str):
        configured_key = configured_key.encode()
    if submitter is None:
        reject_remote_callable_signing_key(configured_key)
    configured_base_url = (
        base_url
        or environment.get("EYVAN_URL")
        or (active_context.base_url if active_context else None)
        or DEFAULT_BASE_URL
    )
    endpoint = ControlPlaneEndpoint.parse(configured_base_url)
    configured_artifact_uri = artifact_uri or environment.get("EYVAN_ARTIFACT_URI")
    requires_default_client = submitter is None or (artifact_store is None and configured_artifact_uri is None)
    runtime_profile = _configured_gcp_runtime_profile(endpoint)
    if runtime_profile is not None:
        _validate_native_gcp_artifact_override(configured_artifact_uri, artifact_store)
    if requires_default_client and endpoint.is_loopback:
        runtime_profile = _prepared_gcp_runtime_profile(endpoint, runtime_profile)
        if runtime_profile is None:
            ensure_loopback_control_plane(endpoint)
    client = None
    if requires_default_client:
        from eyvan.sdk.client.sync import EyvanClient

        configured_role = environment.get("EYVAN_ROLE", "operator")
        identity = resolve_control_plane_identity(
            endpoint,
            token=token or environment.get("EYVAN_TOKEN"),
            dev_user=dev_user or environment.get("EYVAN_DEV_USER"),
            dev_org=configured_org,
            dev_project=configured_project,
            dev_roles=configured_role,
        )
        client = EyvanClient(
            base_url=endpoint.url,
            identity=identity,
        )
    if artifact_store is None:
        if configured_artifact_uri is not None:
            artifact_store = artifact_store_from_uri(configured_artifact_uri)
        elif runtime_profile is not None:
            artifact_store = artifact_store_from_uri(runtime_profile.artifact_root_uri)
        else:
            if client is None:
                raise RuntimeError("Eyvan managed artifacts require the default SDK client")
            artifact_store = EyvanManagedArtifactStore(
                client,
                org_id=configured_org,
                project_id=configured_project,
            )
    if submitter is None:
        from eyvan.capacity.autoscale import AutoscaleConfigCache
        from eyvan.sdk.client.callables import EyvanCallableSubmitter

        if client is None:
            raise RuntimeError("Eyvan callable submission requires the default SDK client")
        submitter = EyvanCallableSubmitter[tp.NoReturn](
            client,
            artifact_store,
            queue_admission_cache=AutoscaleConfigCache(
                active_context.autoscale_cache_dir if active_context is not None else None
            ),
        )
    context = ExecutionContext(
        fleet=configured_fleet,
        org_id=configured_org,
        project_id=configured_project,
        artifact_store=artifact_store,
        submitter=submitter,
        signing_key=configured_key or None,
    )
    global _context
    with _context_lock:
        _context = context
    return context


def get_context() -> ExecutionContext[tp.NoReturn]:
    """Return the configured context, lazily loading the active CLI context."""

    with _context_lock:
        context = _context
        if context is None:
            context = init()
        return context


def _zero_arg_invocation(fn: tp.Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> tp.Callable[[], R]:
    """Capture one fully bound invocation without retaining its wrapper context."""

    def invoke() -> R:
        return fn(*args, **kwargs)

    return invoke


def _resolve_fleet_from_tpu(context: ExecutionContext[tp.Any], tpu: AcceleratorConfig) -> str:
    """Ask the control plane which fleet has an enabled pool matching the TpuConfig shape."""

    if not isinstance(context.submitter, EyvanCallableSubmitter):
        raise RuntimeError(
            "Auto-resolving a fleet from a TpuConfig requires the default Eyvan callable submitter; "
            "pass fleet= or configure a submitter that supports capacity lookup"
        )
    pools = context.submitter.client.list_capacity(
        org_id=context.org_id,
        project_id=context.project_id,
        accelerator_type=tpu.type,
        capacity_class=tpu.capacity_class,
    )
    if not pools:
        raise EyvanPlacementError(
            requested_accelerator_type=tpu.type,
            available_accelerator_types=(),
            scope=f"{context.org_id}/{context.project_id} with capacity class {tpu.capacity_class.value!r}",
        )
    for pool in pools:
        if pool.max_cells >= tpu.pods:
            return pool.fleet
    return pools[0].fleet


class Executable(tp.Generic[P, R]):
    """Side-effect-free callable wrapper produced by ``@execute``."""

    def __init__(
        self,
        fn: tp.Callable[P, R],
        config: AcceleratorConfig,
        context: ExecutionContext[R] | None,
        workspace: str | os.PathLike[str] | None = None,
    ):
        self._fn = fn
        self._config = config
        self._context = context
        self._workspace = workspace
        functools.update_wrapper(self, fn)

    @property
    def config(self) -> AcceleratorConfig:
        """Return this callable's immutable TPU request."""

        return self._config

    def submit(self, *args: P.args, **kwargs: P.kwargs) -> EyvanJob[R]:
        """Serialize and submit one invocation, returning its durable handle."""

        context = self._context or get_context()
        if context.submitter is None:
            raise RuntimeError("Eyvan callable execution requires a configured CallableSubmitter")
        inspect.signature(self._fn).bind(*args, **kwargs)
        bound = _zero_arg_invocation(self._fn, *args, **kwargs)
        artifact = context.artifact_store.put(dump_callable(bound, signing_key=context.signing_key))
        invocation_id = f"call-{uuid.uuid4().hex}"
        fleet = context.fleet
        if fleet is None:
            fleet = _resolve_fleet_from_tpu(context, self._config)
        workspace_uri: str | None = None
        workspace_sha256: str | None = None
        workspace_signature: str | None = None
        if self._workspace is not None:
            bundle = build_workspace(self._workspace)
            workspace_uri = context.artifact_store.put_workspace(bundle.payload, bundle.sha256)
            workspace_sha256 = bundle.sha256
            # Signed under the same key as the callable. The bundle is entered
            # and put on sys.path before the pickle is loaded, so an unsigned
            # workspace can shadow any module the signed callable resolves by
            # reference -- and the callable's signature would prove nothing.
            if context.signing_key is not None:
                workspace_signature = hmac.new(context.signing_key, bundle.payload, hashlib.sha256).hexdigest()
        request = CallableSubmission(
            invocation_id=invocation_id,
            fleet=fleet,
            org_id=context.org_id,
            project_id=context.project_id,
            callable_name=f"{self._fn.__module__}.{self._fn.__qualname__}",
            tpu=self._config,
            artifact=artifact,
            result_uri=context.artifact_store.allocate_result_uri(invocation_id),
            workspace_uri=workspace_uri,
            workspace_sha256=workspace_sha256,
            workspace_signature=workspace_signature,
        )
        return context.submitter.submit_callable(request)

    def remote(self, *args: P.args, **kwargs: P.kwargs) -> EyvanJob[R]:
        """Alias for ``submit``, matching familiar remote-execution APIs."""

        return self.submit(*args, **kwargs)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Submit one invocation and block for its durable result."""

        return self.submit(*args, **kwargs).result()


def execute(
    config: TpuConfig,
    *,
    context: ExecutionContext[R] | None = None,
    workspace: str | os.PathLike[str] | None = None,
) -> tp.Callable[[tp.Callable[P, R]], Executable[P, R]]:
    """Decorate a function for durable, scale-from-zero Eyvan execution.

    ``workspace`` opts into shipping a working directory alongside the callable,
    so application modules travel with the job instead of having to be baked
    into the image or staged as fleet artifacts. It is off by default: packaging
    a directory is a visible cost and a visible trust decision, not something to
    do behind the caller's back.
    """

    if not isinstance(config, TpuConfig):
        raise TypeError("execute requires a TpuConfig")

    def decorate(fn: tp.Callable[P, R]) -> Executable[P, R]:
        if not callable(fn):
            raise TypeError("execute can only decorate callable objects")
        return Executable(fn, config, context, workspace)

    return decorate


__all__ = (
    "CallableSubmission",
    "CallableSubmitter",
    "Executable",
    "ExecutionContext",
    "EyvanJob",
    "EyvanJobError",
    "EyvanPlacementError",
    "EyvanUnavailableError",
    "JobBackend",
    "JobCompletion",
    "TpuConfig",
    "execute",
    "get_context",
    "init",
)
