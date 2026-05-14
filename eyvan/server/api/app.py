# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""FastAPI transport for the Eyvan control-plane service."""

from __future__ import annotations

import argparse
import asyncio
import collections
import hmac
import ipaddress
import json
import logging
import os
import secrets
import subprocess
import typing as tp
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlsplit

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from eyvan._internal.client_version import CLIENT_VERSION_HEADER, is_client_supported, stale_client_message
from eyvan._internal.json_types import JsonObject, model_json
from eyvan._internal.observability import EyvanMetrics
from eyvan._internal.platform import EyvanPlatformConfig, load_platform_config
from eyvan.artifacts.managed import (
    ArtifactAction,
    ArtifactTokenSigner,
    InvalidArtifactToken,
    ManagedArtifactConflictError,
    callable_uri,
    managed_artifact_store_from_uri,
    result_uri,
    workspace_uri,
)
from eyvan.artifacts.transfers import ManagedArtifactAccess
from eyvan.capacity.providers.gcp.runtime.profile import GcpNativeRuntimeProfileStore
from eyvan.config import EyvanSettings, RepositoryBackend
from eyvan.records import (
    ArtifactReferenceRecord,
    ArtifactUsage,
    AuditEvent,
    BudgetRecord,
    BudgetStatus,
    CallableExecution,
    CapacityClass,
    CapacityPoolRecord,
    CellBillingReport,
    CellRecord,
    LeaseRecord,
    ManagedArtifactRecord,
    OrganizationRecord,
    Permission,
    PolicyRecord,
    Principal,
    ProjectRecord,
    QueueTrend,
    QuotaRecord,
    RoleGrantRecord,
    UserProgressReport,
    WarmthReport,
    WorkloadEstimate,
    WorkloadExplanation,
    WorkloadProgress,
    WorkloadRecord,
    WorkloadState,
)
from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
from eyvan.sdk.contexts import DEFAULT_ORG_ID, DEFAULT_PROJECT_ID
from eyvan.server.api.auth import OIDCTokenVerifier, RequestAuthenticator, TokenVerifier, authenticated_principal
from eyvan.server.api.schemas import (
    ArtifactHoldRequest,
    BatchSubmitRequest,
    BatchSubmitResponse,
    BudgetPutRequest,
    CapacityPoolResponse,
    CapacityPoolUpsertRequest,
    CapacityResponse,
    CellUpsertRequest,
    DagSubmitRequest,
    DagSubmitResponse,
    FleetApplyRequest,
    FleetStatusResponse,
    GroupSubmitRequest,
    GroupSubmitResponse,
    HealthResponse,
    NoMatchingCapacityPoolErrorContext,
    NoMatchingCapacityPoolErrorResponse,
    OrganizationRegisterRequest,
    PlatformAgentNotConfiguredError,
    PolicyPutRequest,
    PoolCallableSubmitRequest,
    PoolWorkloadSubmitRequest,
    PreemptionForceFinalizeRequest,
    ProgressReportRequest,
    ProjectRegisterRequest,
    QuotaPutRequest,
    RoleGrantIssueRequest,
    WorkloadEstimateRequest,
    WorkloadSubmitRequest,
)
from eyvan.server.api.server_bind import ServerBindPolicy
from eyvan.server.api.server_logging import ServerLogLevel, server_log_config
from eyvan.server.api.server_startup import DevelopmentAuthMode, ServerStartupSettings
from eyvan.server.controller import EyvanController
from eyvan.server.local.auth import LocalDevelopmentCredentialStore
from eyvan.server.local.daemon import DEFAULT_LOCAL_CELL_AGENT_URL
from eyvan.server.local.profile import LocalArtifactTokenKeyStore
from eyvan.server.persistence.base import Repository, TransientRepositoryError
from eyvan.server.persistence.leader import PostgresAdvisoryLeader
from eyvan.server.persistence.local import LocalJsonRepository
from eyvan.server.persistence.memory import InMemoryRepository
from eyvan.server.persistence.sqlalchemy import SqlAlchemyRepository, create_schema
from eyvan.server.service.callable_trust import CallableProjectTrustPolicy
from eyvan.server.service.control_plane import ControlPlaneService
from eyvan.server.service.dag import DagNode
from eyvan.server.service.errors import (
    AuthorizationError,
    ConflictError,
    EyvanServiceError,
    InputError,
    NoMatchingCapacityPoolError,
    NotFoundError,
)
from eyvan.server.service.groups import GroupRole
from eyvan.server.service.scheduling import CoreSchedulingService
from eyvan.server.service.workload_progress import WorkloadProgressResolver

logger = logging.getLogger(__name__)

UNMATCHED_ROUTE_LABEL = "<unmatched>"

#: How far behind its cursor an audit follower re-scans, so an event whose
#: transaction committed after a higher-sequence one is still delivered.
_AUDIT_STREAM_LOOKBACK = 256

#: Bound on remembered event ids used to suppress the duplicates that lookback
#: necessarily re-reads. Comfortably larger than the lookback window.
_AUDIT_STREAM_DEDUPE_LIMIT = 4096


def _route_label(request: Request) -> str:
    """Return a bounded Prometheus ``operation`` label for one request.

    Only a matched route template is safe to use as a label value. An unmatched
    path is caller-controlled, so labelling with it lets any scanner mint an
    unbounded number of Prometheus time series and grow the registry without
    limit -- reachable before authentication.
    """

    path = getattr(request.scope.get("route"), "path", None)
    return path if isinstance(path, str) else UNMATCHED_ROUTE_LABEL


def _artifact_token_key(settings: EyvanSettings) -> bytes:
    """Resolve an explicit key, a stable local key, or an ephemeral in-memory development key."""

    if settings.artifact_token_key is not None:
        return settings.artifact_token_key.encode()
    if settings.repository_backend is RepositoryBackend.LOCAL:
        return LocalArtifactTokenKeyStore(settings.local_artifact_token_key_path).load_or_create()
    if settings.repository_backend is RepositoryBackend.MEMORY:
        logger.warning(
            "EYVAN_ARTIFACT_TOKEN_KEY is not set; managed-artifact tokens minted by this in-memory "
            "development process stop verifying as soon as it exits"
        )
        return secrets.token_bytes(32)
    raise ValueError(
        "EYVAN_ARTIFACT_TOKEN_KEY must be set when the Eyvan repository backend is not local; an ephemeral "
        "per-process signing key would break managed-artifact token validation across replicas and restarts"
    )


def create_app(
    *,
    settings: EyvanSettings | None = None,
    repository: Repository | None = None,
    service: ControlPlaneService | None = None,
    verifier: TokenVerifier | None = None,
    metrics: EyvanMetrics | None = None,
    metrics_registry: CollectorRegistry | None = None,
    artifact_access: ManagedArtifactAccess | None = None,
    server_bind: ServerBindPolicy | None = None,
    automatic_loopback_auth: bool = False,
    platform_config: EyvanPlatformConfig | None = None,
) -> FastAPI:
    """Build an application with injectable auth, service, and persistence adapters.

    Programmatic builds default to a loopback bind; network exposure requires an explicit
    ``server_bind`` (or the ``create_network_app`` factory) and a production configuration.
    """

    server_bind = server_bind or ServerBindPolicy.from_host("127.0.0.1")
    if automatic_loopback_auth:
        if settings is None:
            raise ValueError("automatic loopback authentication requires explicit local daemon settings")
        if not server_bind.is_loopback or not settings.dev_auth_enabled:
            raise ValueError("automatic loopback authentication requires a development-authenticated loopback bind")
        startup = ServerStartupSettings(
            bind=server_bind,
            settings=settings,
            development_auth_mode=DevelopmentAuthMode.LOOPBACK_DEFAULT,
        )
    elif settings is not None:
        startup = ServerStartupSettings.from_settings(server_bind, settings)
    else:
        startup = ServerStartupSettings.from_environment(server_bind)
    settings = startup.settings
    local_development_credential = (
        LocalDevelopmentCredentialStore().load_or_create()
        if startup.development_auth_mode is DevelopmentAuthMode.LOOPBACK_DEFAULT
        else None
    )
    platform = platform_config or load_platform_config(settings.platform_config_path)
    loopback_development = settings.dev_auth_enabled and server_bind.is_loopback
    if platform.default_agent_endpoint is None and loopback_development:
        platform = EyvanPlatformConfig(default_agent_endpoint=DEFAULT_LOCAL_CELL_AGENT_URL)
    engine: AsyncEngine | None = None
    if repository is None:
        if settings.repository_backend is RepositoryBackend.MEMORY:
            repository = InMemoryRepository()
        elif settings.repository_backend is RepositoryBackend.LOCAL:
            repository = LocalJsonRepository(settings.local_state_path)
        elif settings.repository_backend is RepositoryBackend.POSTGRES:
            engine = create_async_engine(settings.database_url, pool_pre_ping=True)
            repository = SqlAlchemyRepository(engine)
        else:
            raise ValueError("unsupported Eyvan repository backend")
    if service is None:
        trusted_callable_projects = set(settings.trusted_callable_projects)
        if loopback_development:
            trusted_callable_projects.add(f"{DEFAULT_ORG_ID}/{DEFAULT_PROJECT_ID}")
        service = ControlPlaneService(
            repository,
            callable_trust=CallableProjectTrustPolicy.from_projects(trusted_callable_projects),
            scheduler=CoreSchedulingService(),
        )
    if verifier is None and settings.oidc_issuer:
        verifier = OIDCTokenVerifier(
            issuer=settings.oidc_issuer,
            audience=settings.oidc_audience,
            jwks_url=settings.oidc_jwks_url,
        )
    registry = metrics_registry or CollectorRegistry(auto_describe=True)
    metrics = metrics or EyvanMetrics(registry=registry)
    if artifact_access is None:
        if settings.artifact_token_ttl_s <= 0:
            raise ValueError("EYVAN_ARTIFACT_TOKEN_TTL_S must be positive")
        token_key = _artifact_token_key(settings)
        artifact_access = ManagedArtifactAccess(
            store=managed_artifact_store_from_uri(settings.artifact_store_uri),
            signer=ArtifactTokenSigner(
                token_key,
                default_ttl_seconds=settings.artifact_token_ttl_s,
                max_ttl_seconds=settings.artifact_token_ttl_s,
                clock_skew_seconds=30,
            ),
            public_url=settings.public_url,
            token_ttl_seconds=settings.artifact_token_ttl_s,
        )

    if engine is not None:
        leader = PostgresAdvisoryLeader(engine)
    else:
        leader = None
    controller = EyvanController(
        repository,
        service,
        settings=settings,
        leader=leader,
        metrics=metrics,
        artifact_access=artifact_access,
    )
    workload_progress = WorkloadProgressResolver(service, repository)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if engine is not None and settings.auto_create_schema:
            await create_schema(engine)
        await repository.open()
        stop = asyncio.Event()
        task = (
            asyncio.create_task(controller.run(stop), name="eyvan-controller") if settings.controller_enabled else None
        )
        if startup.development_auth_mode is DevelopmentAuthMode.LOOPBACK_DEFAULT:
            logger.info(
                "Eyvan loopback development auth enabled; owner credential and local identity headers are required"
            )
        elif startup.development_auth_mode is DevelopmentAuthMode.DISABLED and server_bind.is_loopback:
            logger.warning(
                "Eyvan loopback development auth is disabled; local CLI and SDK identity headers will be rejected"
            )
        if task is None:
            logger.warning(
                "Eyvan controller mode=api_only; this process will serve the API without reconciling workloads "
                "or capacity"
            )
        else:
            logger.info("Eyvan controller mode=active; workload scheduling and capacity reconciliation are active")
        try:
            yield
        finally:
            stop.set()
            if task is not None:
                await task
            await controller.close()
            await repository.close()
            if engine is not None:
                await engine.dispose()

    app = FastAPI(title="Eyvan Fleet API", version="1.0.0", lifespan=lifespan)
    app.state.service = service
    app.state.authenticator = RequestAuthenticator(
        settings,
        verifier=verifier,
        local_development_credential=local_development_credential,
    )
    # The seam that makes durable grants real. Without it the records exist, the
    # routes issue them, and no request is ever authorized by one.
    app.state.grant_resolver = service.active_grants_for
    app.state.metrics = metrics
    app.state.metrics_registry = registry
    app.state.controller = controller
    app.state.artifact_access = artifact_access
    app.state.platform = platform
    app.state.server_startup = startup

    @app.middleware("http")
    async def observe_request(request: Request, call_next):
        if request.scope["path"].startswith("/v1/"):
            request.scope["path"] = "/api" + request.scope["path"]
        try:
            response = await call_next(request)
        except Exception:
            metrics.api_requests.labels(operation=_route_label(request), result="error").inc()
            raise
        metrics.api_requests.labels(operation=_route_label(request), result=str(response.status_code)).inc()
        return response

    @app.middleware("http")
    async def enforce_client_version(request: Request, call_next):
        """Reject an Eyvan SDK older than this deployment supports.

        Both path spellings are checked because Starlette runs a
        later-registered middleware outermost, so this may see a request before
        ``observe_request`` rewrites the ``/v1/`` compatibility alias.
        """

        minimum = settings.min_client_version
        if minimum and request.scope["path"].startswith(("/api/v1/", "/v1/")):
            reported = request.headers.get(CLIENT_VERSION_HEADER)
            if not is_client_supported(reported, minimum):
                return JSONResponse(
                    status_code=status.HTTP_426_UPGRADE_REQUIRED,
                    content={
                        "detail": stale_client_message(reported, minimum),
                        "code": "client_too_old",
                        "context": {"client_version": reported, "minimum_client_version": minimum},
                    },
                )
        return await call_next(request)

    @app.exception_handler(EyvanServiceError)
    async def handle_service_error(request: Request, exc: EyvanServiceError) -> JSONResponse:
        del request
        if isinstance(exc, AuthorizationError):
            status_code = status.HTTP_403_FORBIDDEN
        elif isinstance(exc, NotFoundError):
            status_code = status.HTTP_404_NOT_FOUND
        elif isinstance(exc, ConflictError):
            status_code = status.HTTP_409_CONFLICT
        else:
            status_code = status.HTTP_400_BAD_REQUEST
        if isinstance(exc, (AuthorizationError, NotFoundError)):
            logger.info("Eyvan API request rejected (%s): %s", type(exc).__name__, exc)
        else:
            logger.warning("Eyvan API service error (%s): %s", type(exc).__name__, exc)
        if isinstance(exc, NoMatchingCapacityPoolError):
            response = NoMatchingCapacityPoolErrorResponse(
                detail=str(exc),
                context=NoMatchingCapacityPoolErrorContext(
                    fleet=exc.fleet,
                    requested_accelerator_type=exc.requested_accelerator_type,
                    available_enabled_accelerator_types=exc.available_enabled_accelerator_types,
                ),
            )
            return JSONResponse(status_code=status_code, content=model_json(response))
        return JSONResponse(status_code=status_code, content={"detail": str(exc)})

    @app.exception_handler(RequestValidationError)
    async def handle_request_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        del request
        errors = [{key: value for key, value in error.items() if key not in {"input", "ctx"}} for error in exc.errors()]
        return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, content={"detail": errors})

    @app.exception_handler(ValidationError)
    async def handle_model_validation_error(request: Request, exc: ValidationError) -> JSONResponse:
        del request
        errors = [{key: value for key, value in error.items() if key not in {"input", "ctx"}} for error in exc.errors()]
        return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, content={"detail": errors})

    @app.exception_handler(TransientRepositoryError)
    async def handle_transient_repository_error(request: Request, exc: TransientRepositoryError) -> JSONResponse:
        del request
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            headers={"Retry-After": "1"},
            content={"detail": str(exc)},
        )

    @app.get("/healthz", response_model=HealthResponse, tags=["system"])
    @app.get("/api/v1/health", response_model=HealthResponse, tags=["system"])
    async def health() -> HealthResponse:
        return HealthResponse(controller=startup.controller_mode)

    async def artifact_body(request: Request, *, limit: int) -> bytes:
        if limit <= 0:
            raise ValueError("managed artifact size limits must be positive")
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                declared_length = int(content_length)
                if declared_length < 0:
                    raise ValueError
                if declared_length > limit:
                    raise InputError(f"managed artifact exceeds the {limit}-byte limit")
            except ValueError as exc:
                raise InputError("Content-Length must be a non-negative integer") from exc
        payload = bytearray()
        async for chunk in request.stream():
            if len(payload) + len(chunk) > limit:
                raise InputError(f"managed artifact exceeds the {limit}-byte limit")
            payload.extend(chunk)
        return bytes(payload)

    async def managed_get(uri: str, *, org_id: str, project_id: str) -> bytes:
        try:
            return await asyncio.to_thread(artifact_access.store.get, uri, org_id=org_id, project_id=project_id)
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            raise NotFoundError(f"managed artifact {uri!r} was not found") from exc

    async def managed_put(
        uri: str,
        payload: bytes,
        *,
        org_id: str,
        project_id: str,
        sha256: str | None = None,
    ) -> None:
        """Index one object's bytes, then store it.

        Index first: the quota check lives in ``index_managed_artifact`` and has
        to be able to refuse the write. Ordering it after the store would leave a
        rejected upload sitting in the object store with no index row -- bytes
        that are charged to nobody and that no collector would ever find.

        The reverse failure is the safe one. If the store write fails after the
        index row lands, the row over-reports usage for one URI until the next
        successful write to it, which costs the project quota headroom rather
        than losing an object.
        """

        await service.index_managed_artifact(
            org_id=org_id,
            project_id=project_id,
            uri=uri,
            size_bytes=len(payload),
            sha256=sha256,
        )
        try:
            await asyncio.to_thread(artifact_access.store.put, uri, payload, org_id=org_id, project_id=project_id)
        except ManagedArtifactConflictError as exc:
            raise ConflictError(str(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise InputError(str(exc)) from exc

    def verify_artifact_token(
        token: str,
        action: ArtifactAction,
        uri: str,
        *,
        org_id: str,
        project_id: str,
        workload_id: str,
    ) -> None:
        try:
            artifact_access.signer.verify(
                token,
                action,
                uri,
                org_id=org_id,
                project_id=project_id,
                workload_id=workload_id,
            )
        except InvalidArtifactToken as exc:
            raise AuthorizationError("invalid managed artifact transfer authorization") from exc

    async def verify_and_audit_artifact_read(
        token: str,
        uri: str,
        *,
        org_id: str,
        project_id: str,
        workload_id: str,
        kind: str,
    ) -> None:
        """Verify a read capability and record the attempt, refused or granted.

        Both outcomes are audited. A refusal is the more interesting of the two: a
        rejected token is the signal that one leaked or outlived its attempt, and it
        is invisible if only successes are recorded.
        """

        try:
            verify_artifact_token(
                token,
                ArtifactAction.READ,
                uri,
                org_id=org_id,
                project_id=project_id,
                workload_id=workload_id,
            )
        except AuthorizationError:
            await service.record_artifact_access(
                org_id=org_id,
                project_id=project_id,
                workload_id=workload_id,
                kind=kind,
                object_id=uri,
                action="artifact.read_denied",
            )
            raise
        await service.record_artifact_access(
            org_id=org_id,
            project_id=project_id,
            workload_id=workload_id,
            kind=kind,
            object_id=uri,
            action="artifact.read",
        )

    async def current_result_uri(workload_id: str) -> str:
        """Return the managed result URI this workload's current attempt owns.

        Read live rather than passed in by the caller: ``result_spec_for_generation``
        mints a new URI per attempt, so a caller-supplied one would let a retried
        attempt keep reporting against the address its predecessor held.

        Raises:
            AuthorizationError: If the workload has no managed result, which is
                the case for every command workload -- those have no artifact
                token either, so there is nothing this route could verify.
        """

        async with repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
        execution = workload.spec.execution if workload is not None else None
        uri = getattr(execution, "result_uri", None)
        if not isinstance(uri, str) or not uri:
            raise AuthorizationError("this workload has no managed result to authenticate against")
        return uri

    async def authorize_current_result_target(
        uri: str,
        *,
        org_id: str,
        project_id: str,
        workload_id: str,
    ) -> None:
        # The audit event is written in the *same* transaction as the authorization
        # read, so a granted upload cannot be committed unaudited.
        async with repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
            execution = workload.spec.execution if workload is not None else None
            granted = (
                workload is not None
                and workload.org_id == org_id
                and workload.project_id == project_id
                and workload.state
                in {
                    WorkloadState.LEASED,
                    WorkloadState.DISPATCHING,
                    WorkloadState.RUNNING,
                    WorkloadState.CHECKPOINTING,
                    WorkloadState.CANCELLING,
                }
                and isinstance(execution, CallableExecution)
                and execution.result_uri == uri
            )
            await service.record_artifact_access(
                org_id=org_id,
                project_id=project_id,
                workload_id=workload_id,
                kind="results",
                object_id=uri,
                action="artifact.write" if granted else "artifact.write_denied",
                tx=transaction,
            )
        if not granted:
            raise AuthorizationError("managed result destination is not current for this workload attempt")

    @app.get("/api/v1/capacity/queue-trend", response_model=QueueTrend, tags=["capacity"])
    async def queue_trend(
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        window_s: float = 86_400.0,
        buckets: int = 24,
    ) -> QueueTrend:
        """Report submitted-vs-admitted counts over recent time buckets.

        Derived from the audit ledger, which already records both with timestamps
        and a monotonic sequence -- so this is a query over data that exists rather
        than a second time-series store to prune, back up, and keep consistent.
        """

        return await service.queue_trend(
            principal, org_id=org_id, project_id=project_id, window_s=window_s, buckets=buckets
        )

    @app.get("/api/v1/capacity/warmth", response_model=WarmthReport, tags=["capacity"])
    async def warmth_report(
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> WarmthReport:
        """Report each pool's warm floor and whether it is currently held.

        Pair this with ``/capacity/billing``: warm capacity is deliberately idle,
        so its cost must be subtracted from the fleet's idle cost before that
        number means anything. Without the pairing, the only safe reaction to an
        unexplained idle figure is to delete the warm floor -- undoing the fix.
        """

        return await service.warmth_report(principal)

    @app.get("/api/v1/capacity/billing", response_model=CellBillingReport, tags=["capacity"])
    async def cell_billing_report(
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        pool_id: str | None = None,
    ) -> CellBillingReport:
        """Report accrued and idle cell cost across the fleet or one pool.

        The idle figure is not derivable from budgets: an idle cell holds no lease,
        so nothing settles and nothing is charged to a project. This is the only
        place a fleet's unused-but-paid-for capacity is visible.
        """

        return await service.cell_billing_report(principal, pool_id=pool_id)

    @app.get("/api/v1/artifacts", response_model=list[ManagedArtifactRecord], tags=["artifacts"])
    async def list_managed_artifacts(
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        kind: str | None = None,
        limit: int = 1000,
    ) -> list[ManagedArtifactRecord]:
        """List indexed artifacts in one project."""

        return await service.list_managed_artifacts(
            principal, org_id=org_id, project_id=project_id, kind=kind, limit=limit
        )

    @app.get(
        "/api/v1/artifacts/collectable",
        response_model=list[ManagedArtifactRecord],
        tags=["artifacts"],
    )
    async def collectable_artifacts(
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> list[ManagedArtifactRecord]:
        """List artifacts nothing currently protects.

        A candidate list, not a deletion queue. Nothing in this build deletes an
        artifact: being able to compare what a collector *would* take against what
        you expected is how the index earns the trust a collector needs.
        """

        return await service.collectable_artifacts(principal, org_id=org_id, project_id=project_id)

    @app.post("/api/v1/artifacts/holds", response_model=ManagedArtifactRecord, tags=["artifacts"])
    async def hold_artifact(
        request: ArtifactHoldRequest,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> ManagedArtifactRecord:
        """Claim one artifact so no future collection may reclaim it.

        Attributed to the calling principal: "who said to keep this" is the only
        question ever asked of a hold, and a typed-in owner cannot be asked.
        """

        return await service.hold_artifact(
            principal, uri=request.uri, reason=request.reason, expires_at=request.expires_at
        )

    @app.delete("/api/v1/artifacts/holds", response_model=ManagedArtifactRecord, tags=["artifacts"])
    async def release_artifact_hold(
        uri: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> ManagedArtifactRecord:
        """Release the hold on one artifact, whoever placed it."""

        return await service.release_artifact_hold(principal, uri=uri)

    @app.get("/api/v1/artifacts/usage", response_model=ArtifactUsage, tags=["artifacts"])
    async def artifact_usage(
        org_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        project_id: str | None = None,
    ) -> ArtifactUsage:
        """Report indexed artifact bytes and the quota they are measured against."""

        return await service.artifact_usage(principal, org_id=org_id, project_id=project_id)

    @app.get(
        "/api/v1/artifacts/references",
        response_model=list[ArtifactReferenceRecord],
        tags=["artifacts"],
    )
    async def artifact_references(
        uri: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        active_only: bool = False,
    ) -> list[ArtifactReferenceRecord]:
        """List the workloads that claim one artifact."""

        return await service.artifact_references(principal, uri=uri, active_only=active_only)

    @app.put("/api/v1/artifacts/callables/{digest}", tags=["artifacts"])
    async def upload_callable_artifact(
        digest: str,
        raw_request: Request,
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> dict[str, str]:
        service.authorize_callable_artifact(principal, org_id=org_id, project_id=project_id)
        try:
            uri = callable_uri(digest)
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        payload = await artifact_body(raw_request, limit=settings.artifact_max_callable_bytes)
        await managed_put(uri, payload, org_id=org_id, project_id=project_id, sha256=digest)
        return {"uri": uri, "sha256": digest}

    @app.put("/api/v1/artifacts/workspaces/{digest}", tags=["artifacts"])
    async def upload_workspace_artifact(
        digest: str,
        raw_request: Request,
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> dict[str, str]:
        """Store one content-addressed workspace bundle.

        A bundle carries executable application code, so it is gated by the same
        callable-trust check as the callable itself rather than a weaker one.
        """

        service.authorize_callable_artifact(principal, org_id=org_id, project_id=project_id)
        try:
            uri = workspace_uri(digest)
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        payload = await artifact_body(raw_request, limit=settings.artifact_max_workspace_bytes)
        await managed_put(uri, payload, org_id=org_id, project_id=project_id, sha256=digest)
        return {"uri": uri, "sha256": digest}

    @app.get(
        "/api/v1/artifacts/workspaces/{digest}/content",
        response_class=Response,
        include_in_schema=False,
    )
    async def transfer_workspace_artifact(
        digest: str,
        org_id: str,
        project_id: str,
        workload_id: str,
        token: tp.Annotated[str, Header(alias="X-Eyvan-Artifact-Token", min_length=1)],
    ) -> Response:
        try:
            uri = workspace_uri(digest)
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        await verify_and_audit_artifact_read(
            token,
            uri,
            org_id=org_id,
            project_id=project_id,
            workload_id=workload_id,
            kind="workspaces",
        )
        return Response(
            await managed_get(uri, org_id=org_id, project_id=project_id),
            media_type="application/gzip",
            headers={"Cache-Control": "private, no-store"},
        )

    @app.get("/api/v1/artifacts/results/{result_id}", response_class=Response, tags=["artifacts"])
    async def download_result_artifact(
        result_id: str,
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> Response:
        service.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        try:
            uri = result_uri(result_id)
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        return Response(
            await managed_get(uri, org_id=org_id, project_id=project_id),
            media_type="application/octet-stream",
            headers={"Cache-Control": "private, no-store"},
        )

    @app.get(
        "/api/v1/artifacts/callables/{digest}/content",
        response_class=Response,
        include_in_schema=False,
    )
    async def transfer_callable_artifact(
        digest: str,
        org_id: str,
        project_id: str,
        workload_id: str,
        token: tp.Annotated[str, Header(alias="X-Eyvan-Artifact-Token", min_length=1)],
    ) -> Response:
        try:
            uri = callable_uri(digest)
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        await verify_and_audit_artifact_read(
            token,
            uri,
            org_id=org_id,
            project_id=project_id,
            workload_id=workload_id,
            kind="callables",
        )
        return Response(
            await managed_get(uri, org_id=org_id, project_id=project_id),
            media_type="application/octet-stream",
            headers={"Cache-Control": "private, no-store"},
        )

    @app.put("/api/v1/artifacts/results/{result_id}/content", include_in_schema=False)
    async def transfer_result_artifact(
        result_id: str,
        raw_request: Request,
        org_id: str,
        project_id: str,
        workload_id: str,
        token: tp.Annotated[str, Header(alias="X-Eyvan-Artifact-Token", min_length=1)],
    ) -> dict[str, str]:
        try:
            uri = result_uri(result_id)
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        verify_artifact_token(
            token,
            ArtifactAction.WRITE,
            uri,
            org_id=org_id,
            project_id=project_id,
            workload_id=workload_id,
        )
        await authorize_current_result_target(
            uri,
            org_id=org_id,
            project_id=project_id,
            workload_id=workload_id,
        )
        payload = await artifact_body(raw_request, limit=settings.artifact_max_result_bytes)
        await managed_put(uri, payload, org_id=org_id, project_id=project_id)
        return {"uri": uri}

    @app.get("/metrics", include_in_schema=False)
    async def prometheus_metrics(raw_request: Request) -> Response:
        """Serve the Prometheus exposition to an authenticated scraper.

        This endpoint had no ``Depends`` at all while exporting
        ``eyvan_project_spend_usd`` labeled by (org, project) -- every tenant's
        spend, readable by anyone who could reach the port.

        A bearer token rather than an OIDC identity because a scraper has no
        identity to present, and the realistic alternative to a token is leaving
        it open. When no token is configured the endpoint stays open, which is
        only reachable on a loopback bind: ``ServerStartupSettings`` refuses to
        start a network-bound server without one.
        """

        expected = settings.metrics_token
        if expected:
            authorization = raw_request.headers.get("Authorization", "")
            scheme, _, presented = authorization.partition(" ")
            if scheme.lower() != "bearer" or not hmac.compare_digest(presented, expected):
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="metrics token required")
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.post("/api/v1/system/reconcile", tags=["system"])
    async def reconcile_once(
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> dict[str, int]:
        service.authorizer.require(principal, Permission.MANAGE_FLEET)
        result = await controller.reconcile_if_leader()
        if result is None:
            raise ConflictError("this replica does not hold scheduler leadership")
        return {
            "placements": len(result.placements),
            "preemptions": len(result.preemptions),
            "infeasibilities": len(result.infeasibilities),
        }

    @app.post(
        "/api/v1/workloads", response_model=WorkloadRecord, status_code=status.HTTP_201_CREATED, tags=["workloads"]
    )
    async def submit_workload(
        request: WorkloadSubmitRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> WorkloadRecord:
        workload, replayed = await service.submit_workload(
            principal,
            org_id=request.org_id,
            project_id=request.project_id,
            spec=request.spec,
            idempotency_key=idempotency_key,
            parent_workload_id=request.parent_workload_id,
        )
        if replayed:
            response.status_code = status.HTTP_200_OK
            response.headers["Idempotent-Replay"] = "true"
        return workload

    @app.post(
        "/api/v1/batches",
        response_model=BatchSubmitResponse,
        status_code=status.HTTP_201_CREATED,
        tags=["workloads"],
    )
    async def submit_batch(
        request: BatchSubmitRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> BatchSubmitResponse:
        """Fan a template out into ``count`` ordinary durable workloads.

        The whole array commits in one transaction, so a partially created batch
        cannot exist. ``parent`` is a coordinator that is never scheduled;
        cancelling it cancels every task.
        """

        parent, tasks = await service.submit_batch(
            principal,
            org_id=request.org_id,
            project_id=request.project_id,
            template=request.template,
            count=request.count,
            idempotency_key=idempotency_key,
            name_prefix=request.name_prefix,
            max_fan_out=request.max_fan_out,
        )
        # No Idempotent-Replay header: submit_batch is replay-transparent by
        # construction, returning the same parent and the same tasks in the same
        # order, so a caller cannot act differently on the two cases.
        return BatchSubmitResponse(parent=parent, tasks=tasks)

    @app.post("/api/v1/workloads/estimate", response_model=WorkloadEstimate, tags=["workloads"])
    async def estimate_workload(
        request: WorkloadEstimateRequest,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> WorkloadEstimate:
        """Estimate what a workload would cost, before any record exists.

        Not a scheduling dry run: it consults declared pool attributes and prices,
        never live cell availability, because an estimate that claimed to predict
        placement would be wrong the moment the fleet moved. No ``Idempotency-Key``
        -- it writes nothing.
        """

        return await service.estimate_workload(
            principal,
            org_id=request.org_id,
            project_id=request.project_id,
            spec=request.spec,
        )

    @app.post(
        "/api/v1/groups", response_model=GroupSubmitResponse, status_code=status.HTTP_201_CREATED, tags=["workloads"]
    )
    async def submit_group(
        request: GroupSubmitRequest,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> GroupSubmitResponse:
        """Submit differently-shaped workloads that must admit together.

        Every member is created unschedulable and all are released in one
        transaction once each is independently placeable in the same pass. This is
        a feasibility gate, not a reservation: capacity can still be taken between
        that answer and the members' leases. What it prevents is half a group
        holding accelerators while the rest is structurally infeasible.
        """

        parent, members = await service.submit_group(
            principal,
            org_id=request.org_id,
            project_id=request.project_id,
            roles=[GroupRole(name=role.name, template=role.template, count=role.count) for role in request.roles],
            idempotency_key=idempotency_key,
            name_prefix=request.name_prefix,
            max_members=request.max_members,
        )
        return GroupSubmitResponse(parent=parent, members=members)

    @app.post(
        "/api/v1/dags",
        response_model=DagSubmitResponse,
        status_code=status.HTTP_201_CREATED,
        tags=["workloads"],
    )
    async def submit_dag(
        request: DagSubmitRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> DagSubmitResponse:
        """Submit a small pipeline of dependent workloads in one transaction.

        Each node is an ordinary durable workload. A node with predecessors is
        created unschedulable and released once every predecessor has succeeded; if
        one fails or is cancelled, its dependents are cancelled rather than left
        waiting forever. Dependencies order execution only -- results travel as
        ``eyvan://`` result URIs, not through the control plane.
        """

        parent, nodes = await service.submit_dag(
            principal,
            org_id=request.org_id,
            project_id=request.project_id,
            nodes=[
                DagNode(name=node.name, template=node.template, depends_on=tuple(node.depends_on))
                for node in request.nodes
            ],
            idempotency_key=idempotency_key,
            name_prefix=request.name_prefix,
            max_fan_out=request.max_fan_out,
        )
        return DagSubmitResponse(parent=parent, nodes=nodes)

    @app.post(
        "/api/v1/fleets/{fleet_name}/node-types/{node_type}/workloads",
        response_model=WorkloadRecord,
        status_code=status.HTTP_201_CREATED,
        tags=["workloads"],
    )
    async def submit_pool_workload(
        fleet_name: str,
        node_type: str,
        request: PoolWorkloadSubmitRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> WorkloadRecord:
        workload, replayed = await service.submit_pool_workload(
            principal,
            fleet_name=fleet_name,
            node_type=node_type,
            org_id=request.org_id,
            project_id=request.project_id,
            command=request.command,
            name=request.name,
            environment=request.environment,
            metadata=request.metadata,
            max_retries=request.max_retries,
            max_failure_retries=request.max_failure_retries,
            workspace=request.workspace,
            idempotency_key=idempotency_key,
        )
        if replayed:
            response.status_code = status.HTTP_200_OK
            response.headers["Idempotent-Replay"] = "true"
        return workload

    @app.post(
        "/api/v1/fleets/{fleet_name}/callables",
        response_model=WorkloadRecord,
        responses={
            status.HTTP_404_NOT_FOUND: {
                "model": NoMatchingCapacityPoolErrorResponse,
                "description": "The selected fleet has no compatible pool for the requested accelerator type.",
            }
        },
        status_code=status.HTTP_201_CREATED,
        tags=["workloads"],
    )
    async def submit_pool_callable(
        fleet_name: str,
        request: PoolCallableSubmitRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> WorkloadRecord:
        workload, replayed = await service.submit_pool_callable(
            principal,
            fleet_name=fleet_name,
            org_id=request.org_id,
            project_id=request.project_id,
            accelerator_type=request.accelerator_type,
            pods=request.pods,
            capacity_class=request.capacity_class,
            execution=request.execution,
            name=request.name,
            environment=request.environment,
            metadata=request.metadata,
            max_retries=request.max_retries,
            max_failure_retries=request.max_failure_retries,
            idempotency_key=idempotency_key,
        )
        if replayed:
            response.status_code = status.HTTP_200_OK
            response.headers["Idempotent-Replay"] = "true"
        return workload

    @app.get("/api/v1/workloads", response_model=list[WorkloadRecord], tags=["workloads"])
    async def list_workloads(
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        state_filter: tp.Annotated[WorkloadState | None, Query(alias="state")] = None,
    ) -> list[WorkloadRecord]:
        return await service.list_workloads(principal, org_id=org_id, project_id=project_id, state=state_filter)

    @app.get("/api/v1/workloads/{workload_id}", response_model=WorkloadRecord, tags=["workloads"])
    async def get_workload(
        workload_id: str, principal: tp.Annotated[Principal, Depends(authenticated_principal)]
    ) -> WorkloadRecord:
        return await service.get_workload(principal, workload_id)

    @app.get("/api/v1/workloads/{workload_id}/children", response_model=list[WorkloadRecord], tags=["workloads"])
    async def list_child_workloads(
        workload_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> list[WorkloadRecord]:
        """Return the direct children of one workload in a job tree."""

        return await service.list_child_workloads(principal, workload_id)

    @app.get("/api/v1/workloads/{workload_id}/progress", response_model=WorkloadProgress, tags=["workloads"])
    async def get_workload_progress(
        workload_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> WorkloadProgress:
        """Return autoscaler and provider lifecycle progress for one workload."""

        return await workload_progress.resolve(principal, workload_id)

    @app.get(
        "/api/v1/workloads/{workload_id}/explain",
        response_model=WorkloadExplanation,
        tags=["workloads"],
    )
    async def explain_workload(
        workload_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> WorkloadExplanation:
        """Return why one workload is or is not currently placeable.

        Recomputed on demand from the pure scheduler, so it reflects the fleet
        as it is now rather than the verdict from the last scheduling pass.
        """

        return await service.explain_workload(principal, workload_id)

    @app.post(
        "/api/v1/workloads/{workload_id}/progress-reports",
        response_model=UserProgressReport,
        include_in_schema=False,
    )
    async def report_progress(
        workload_id: str,
        request: ProgressReportRequest,
        org_id: str,
        project_id: str,
        token: tp.Annotated[str, Header(alias="X-Eyvan-Artifact-Token", min_length=1)],
    ) -> UserProgressReport:
        """Record one rank's progress, authenticated as the running attempt.

        The token is the attempt's *result-write* capability, reused rather than
        minted separately: it is already scoped to this workload, already revoked
        by a generation bump, and already delivered to every rank. A second token
        kind would be a second thing to rotate and a second way to get it wrong.

        No ``Idempotency-Key``: the write is an upsert keyed on ``(workload, rank)``,
        so it is idempotent by overwrite.
        """

        verify_artifact_token(
            token,
            ArtifactAction.WRITE,
            await current_result_uri(workload_id),
            org_id=org_id,
            project_id=project_id,
            workload_id=workload_id,
        )
        report = await service.record_progress_report(
            workload_id=workload_id,
            rank=request.rank,
            attempt=request.attempt,
            step=request.step,
            total=request.total,
            message=request.message,
            metrics=request.metrics,
        )
        # Only the fraction becomes a series, and only when the job declared a
        # total. The metrics dict is displayed and never exported: its keys come
        # from user code, and one mistyped name in a training loop would mint an
        # unbounded label set. Series for finished workloads are dropped by the
        # export phase, which is what keeps ``workload`` from growing forever.
        if report.fraction is not None:
            metrics.workload_progress.labels(workload=workload_id, rank=str(report.rank)).set(report.fraction)
        return report

    # response_model=None: agent inventory entries are backend-shaped JSON.
    @app.get("/api/v1/capacity/orphans", tags=["capacity"], response_model=None)
    async def capacity_orphans(
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> list[dict[str, str | int | None]]:
        """List cells an agent still holds that no control-plane record claims."""

        service.authorizer.require(principal, Permission.MANAGE_FLEET)
        return await controller.capacity_orphans()

    @app.get(
        "/api/v1/workloads/{workload_id}/logs",
        response_class=PlainTextResponse,
        tags=["workloads"],
    )
    async def workload_logs(
        workload_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> str:
        await service.get_workload(principal, workload_id)
        return await controller.workload_logs(workload_id)

    # response_model=None: the payload is backend-shaped arbitrary JSON, and the
    # recursive JsonObject alias cannot be turned into a Pydantic response model.
    @app.get("/api/v1/workloads/{workload_id}/process-status", tags=["workloads"], response_model=None)
    async def workload_process_status(
        workload_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> JsonObject:
        """Return live process detail for one workload attempt.

        Reading process state is an observation, not a mutation, so it needs
        only the READ that ``get_workload`` already enforces for this scope.
        """

        await service.get_workload(principal, workload_id)
        return await controller.workload_process_status(workload_id)

    @app.post("/api/v1/workloads/{workload_id}/cancel", response_model=WorkloadRecord, tags=["workloads"])
    async def cancel_workload(
        workload_id: str,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        cascade: tp.Annotated[bool, Query()] = True,
    ) -> WorkloadRecord:
        """Cancel one workload, and by default every descendant under it.

        ``cascade=false`` cancels exactly this workload. That is the escape hatch
        for a stored tree deeper than the traversal limit: without it such a
        parent could never be cancelled at all, because every attempt failed on
        the walk before reaching the workload the caller actually named.
        """

        workload, replayed = await service.cancel_workload(
            principal,
            workload_id,
            idempotency_key=idempotency_key,
            cascade=cascade,
        )
        if replayed:
            response.status_code = status.HTTP_200_OK
            response.headers["Idempotent-Replay"] = "true"
        return workload

    @app.post("/api/v1/workloads/{workload_id}/checkpoint", response_model=WorkloadRecord, tags=["workloads"])
    async def checkpoint_workload(
        workload_id: str,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> WorkloadRecord:
        workload, replayed = await service.checkpoint_workload(principal, workload_id, idempotency_key=idempotency_key)
        if replayed:
            response.status_code = status.HTTP_200_OK
            response.headers["Idempotent-Replay"] = "true"
        return workload

    @app.post("/api/v1/workloads/{workload_id}/retry", response_model=WorkloadRecord, tags=["workloads"])
    async def retry_workload(
        workload_id: str,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> WorkloadRecord:
        workload, replayed = await service.retry_workload(principal, workload_id, idempotency_key=idempotency_key)
        if replayed:
            response.status_code = status.HTTP_200_OK
            response.headers["Idempotent-Replay"] = "true"
        return workload

    @app.post(
        "/api/v1/preemptions/{command_id}/force-finalize",
        response_model=WorkloadRecord,
        tags=["workloads"],
    )
    async def force_finalize_preemption(
        command_id: str,
        request: PreemptionForceFinalizeRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> WorkloadRecord:
        workload, replayed = await service.force_finalize_preemption(
            principal,
            command_id,
            reason=request.reason,
            idempotency_key=idempotency_key,
        )
        if replayed:
            response.status_code = status.HTTP_200_OK
            response.headers["Idempotent-Replay"] = "true"
        return workload

    @app.post("/api/v1/cells", response_model=CellRecord, tags=["cells"])
    async def upsert_cell(
        request: CellUpsertRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> CellRecord:
        cell, replayed = await service.upsert_cell(principal, request.to_record(), idempotency_key=idempotency_key)
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return cell

    @app.post("/api/v1/capacity-pools", response_model=CapacityPoolRecord, tags=["fleets"])
    async def upsert_capacity_pool(
        request: CapacityPoolUpsertRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> CapacityPoolRecord:
        try:
            record = request.to_record(default_agent_endpoint=platform.default_agent_endpoint)
        except PlatformAgentNotConfiguredError as exc:
            raise InputError(str(exc)) from exc
        pool, replayed = await controller.upsert_capacity_pool(
            principal,
            record,
            idempotency_key=idempotency_key,
        )
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return pool

    @app.get("/api/v1/capacity-pools", response_model=list[CapacityPoolRecord], tags=["fleets"])
    async def list_capacity_pools(
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        fleet_name: str | None = None,
    ) -> list[CapacityPoolRecord]:
        return await service.list_capacity_pools(principal, fleet_name=fleet_name)

    @app.get("/api/v1/capacity-pools/{pool_id}", response_model=CapacityPoolRecord, tags=["fleets"])
    async def get_capacity_pool(
        pool_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> CapacityPoolRecord:
        """Show one capacity pool record."""

        return await service.get_capacity_pool(principal, pool_id=pool_id)

    @app.delete("/api/v1/capacity-pools/{pool_id}", response_model=CapacityPoolRecord, tags=["fleets"])
    async def disable_capacity_pool(
        pool_id: str,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> CapacityPoolRecord:
        pool, replayed = await controller.disable_capacity_pool(
            principal,
            pool_id,
            idempotency_key=idempotency_key,
        )
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return pool

    @app.put("/api/v1/fleets/{fleet_name}", response_model=list[CapacityPoolRecord], tags=["fleets"])
    async def apply_fleet(
        fleet_name: str,
        request: FleetApplyRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> list[CapacityPoolRecord]:
        try:
            records = request.to_records(default_agent_endpoint=platform.default_agent_endpoint)
        except PlatformAgentNotConfiguredError as exc:
            raise InputError(str(exc)) from exc
        pools, replayed = await controller.apply_fleet(
            principal,
            fleet_name,
            records,
            idempotency_key=idempotency_key,
        )
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return pools

    @app.get("/api/v1/capacity", response_model=CapacityResponse, tags=["capacity"])
    async def list_capacity(
        org_id: tp.Annotated[str, Query(min_length=1, max_length=128)],
        project_id: tp.Annotated[str, Query(min_length=1, max_length=128)],
        accelerator_type: tp.Annotated[str | None, Query(min_length=1, max_length=128)] = None,
        capacity_class: tp.Annotated[CapacityClass | None, Query()] = None,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)] = None,  # type: ignore[assignment]
    ) -> CapacityResponse:
        """List enabled pools available to a workload scope, optionally filtered by TPU shape."""

        pools = await service.list_capacity(
            principal,
            org_id=org_id,
            project_id=project_id,
            accelerator_type=accelerator_type,
            capacity_class=capacity_class,
        )
        return CapacityResponse(
            pools=[
                CapacityPoolResponse(
                    fleet=pool.fleet_name,
                    pool=pool.id,
                    accelerator_type=pool.accelerator_type or pool.node_type,
                    capacity_class=pool.capacity_class,
                    region=pool.region,
                    zone=pool.zone,
                    min_cells=pool.min_cells,
                    max_cells=pool.max_cells,
                    enabled=pool.enabled,
                )
                for pool in pools
            ]
        )

    @app.get("/api/v1/fleets", response_model=list[str], tags=["fleets"])
    async def list_submit_fleets(
        org_id: tp.Annotated[str, Query(min_length=1, max_length=128)],
        project_id: tp.Annotated[str, Query(min_length=1, max_length=128)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> list[str]:
        """List runnable fleet names for one authorized workload scope."""

        return await service.list_submit_fleets(principal, org_id=org_id, project_id=project_id)

    @app.get("/api/v1/fleets/{fleet_name}", response_model=FleetStatusResponse, tags=["fleets"])
    async def fleet_status(
        fleet_name: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> FleetStatusResponse:
        pools = await service.list_capacity_pools(principal, fleet_name=fleet_name)
        if not pools:
            raise NotFoundError(f"fleet {fleet_name!r} was not found")
        pool_ids = {pool.id for pool in pools}
        cells = [cell for cell in await service.list_cells(principal) if cell.pool_id in pool_ids]
        return FleetStatusResponse(name=fleet_name, pools=pools, cells=cells)

    @app.delete("/api/v1/fleets/{fleet_name}", response_model=list[CapacityPoolRecord], tags=["fleets"])
    async def disable_fleet(
        fleet_name: str,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> list[CapacityPoolRecord]:
        pools, replayed = await controller.disable_fleet(
            principal,
            fleet_name,
            idempotency_key=idempotency_key,
        )
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return pools

    @app.get("/api/v1/cells", response_model=list[CellRecord], tags=["cells"])
    async def list_cells(principal: tp.Annotated[Principal, Depends(authenticated_principal)]) -> list[CellRecord]:
        return await service.list_cells(principal)

    @app.get("/api/v1/cells/{cell_id}", response_model=CellRecord, tags=["cells"])
    async def get_cell(cell_id: str, principal: tp.Annotated[Principal, Depends(authenticated_principal)]) -> CellRecord:
        return await service.get_cell(principal, cell_id)

    @app.post("/api/v1/cells/{cell_id}/drain", response_model=CellRecord, tags=["cells"])
    async def drain_cell(
        cell_id: str,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> CellRecord:
        cell, replayed = await controller.drain_cell(principal, cell_id, idempotency_key=idempotency_key)
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return cell

    @app.delete("/api/v1/cells/{cell_id}", response_model=CellRecord, tags=["cells"])
    async def delete_cell(
        cell_id: str,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> CellRecord:
        cell, replayed = await controller.delete_cell(principal, cell_id, idempotency_key=idempotency_key)
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return cell

    @app.get("/api/v1/leases", response_model=list[LeaseRecord], tags=["leases"])
    async def list_leases(
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        workload_id: str | None = None,
    ) -> list[LeaseRecord]:
        return await service.list_leases(principal, org_id=org_id, project_id=project_id, workload_id=workload_id)

    @app.get("/api/v1/leases/{lease_id}", response_model=LeaseRecord, tags=["leases"])
    async def get_lease(
        lease_id: str, principal: tp.Annotated[Principal, Depends(authenticated_principal)]
    ) -> LeaseRecord:
        return await service.get_lease(principal, lease_id)

    @app.put("/api/v1/policies/{org_id}/{project_id}", response_model=PolicyRecord, tags=["governance"])
    async def put_policy(
        org_id: str,
        project_id: str,
        request: PolicyPutRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> PolicyRecord:
        policy, replayed = await service.put_policy(
            principal, request.to_record(org_id, project_id), idempotency_key=idempotency_key
        )
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return policy

    @app.get("/api/v1/policies", response_model=list[PolicyRecord], tags=["governance"])
    async def list_policies(
        org_id: str, principal: tp.Annotated[Principal, Depends(authenticated_principal)]
    ) -> list[PolicyRecord]:
        return await service.list_policies(principal, org_id=org_id)

    @app.get("/api/v1/policies/{org_id}/{project_id}", response_model=PolicyRecord, tags=["governance"])
    async def get_policy(
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> PolicyRecord:
        return await service.get_policy(principal, org_id=org_id, project_id=project_id)

    @app.post("/api/v1/grants", response_model=RoleGrantRecord, status_code=201, tags=["tenancy"])
    async def issue_role_grant(
        request: RoleGrantIssueRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> RoleGrantRecord:
        """Grant a role durably, without an identity-provider change."""

        grant, replayed = await service.issue_role_grant(principal, request.to_record(), idempotency_key=idempotency_key)
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
            response.status_code = 200
        return grant

    @app.get("/api/v1/grants", response_model=list[RoleGrantRecord], tags=["tenancy"])
    async def list_role_grants(
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        subject: str | None = None,
        org_id: str | None = None,
    ) -> list[RoleGrantRecord]:
        """List durable grants the caller is entitled to see."""

        return await service.list_role_grants(principal, subject=subject, org_id=org_id)

    @app.delete("/api/v1/grants", status_code=204, tags=["tenancy"])
    async def revoke_role_grant(
        subject: str,
        role: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        org_id: str = "",
        project_id: str = "",
    ) -> Response:
        """Withdraw a durable grant, effective on the subject's next request."""

        removed = await service.revoke_role_grant(principal, (subject, role, org_id, project_id))
        if not removed:
            raise NotFoundError(f"no grant of {role!r} to {subject!r} in scope {org_id}/{project_id}")
        return Response(status_code=204)

    @app.put("/api/v1/organizations/{org_id}", response_model=OrganizationRecord, tags=["tenancy"])
    async def register_organization(
        org_id: str,
        request: OrganizationRegisterRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> OrganizationRecord:
        """Register a tenant this control plane serves."""

        organization, replayed = await service.register_organization(
            principal,
            request.to_record(org_id, created_by=principal.subject),
            idempotency_key=idempotency_key,
        )
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return organization

    @app.get("/api/v1/organizations", response_model=list[OrganizationRecord], tags=["tenancy"])
    async def list_organizations(
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> list[OrganizationRecord]:
        """List tenants the caller has any grant for; everything, for a fleet admin."""

        return await service.list_organizations(principal)

    @app.get("/api/v1/organizations/{org_id}", response_model=OrganizationRecord, tags=["tenancy"])
    async def get_organization(
        org_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> OrganizationRecord:
        return await service.get_organization(principal, org_id)

    @app.put("/api/v1/organizations/{org_id}/projects/{project_id}", response_model=ProjectRecord, tags=["tenancy"])
    async def register_project(
        org_id: str,
        project_id: str,
        request: ProjectRegisterRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> ProjectRecord:
        project, replayed = await service.register_project(
            principal,
            request.to_record(org_id, project_id, created_by=principal.subject),
            idempotency_key=idempotency_key,
        )
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return project

    @app.get("/api/v1/organizations/{org_id}/projects", response_model=list[ProjectRecord], tags=["tenancy"])
    async def list_projects(
        org_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> list[ProjectRecord]:
        return await service.list_projects(principal, org_id=org_id)

    @app.put("/api/v1/budgets/{org_id}/{project_id}", response_model=BudgetRecord, tags=["governance"])
    async def put_budget(
        org_id: str,
        project_id: str,
        request: BudgetPutRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> BudgetRecord:
        budget, replayed = await service.put_budget(
            principal, request.to_record(org_id, project_id), idempotency_key=idempotency_key
        )
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return budget

    @app.get("/api/v1/budgets", response_model=list[BudgetRecord], tags=["governance"])
    async def list_budgets(
        org_id: str, principal: tp.Annotated[Principal, Depends(authenticated_principal)]
    ) -> list[BudgetRecord]:
        return await service.list_budgets(principal, org_id=org_id)

    @app.get("/api/v1/budgets/{org_id}/{project_id}", response_model=BudgetRecord, tags=["governance"])
    async def get_budget(
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> BudgetRecord:
        return await service.get_budget(principal, org_id=org_id, project_id=project_id)

    @app.get(
        "/api/v1/budgets/{org_id}/{project_id}/status",
        response_model=BudgetStatus,
        tags=["governance"],
    )
    async def budget_status(
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> BudgetStatus:
        """Report a budget's period window and its projected end-of-period spend.

        The forecast assumes every active lease runs to the end of the window --
        the pessimistic reading, deliberately, because this number exists to warn
        before a hard limit starts refusing admission.
        """

        return await service.budget_status(principal, org_id=org_id, project_id=project_id)

    @app.post(
        "/api/v1/budgets/{org_id}/{project_id}/roll",
        response_model=BudgetRecord,
        tags=["governance"],
    )
    async def roll_budget_period(
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> BudgetRecord:
        """Close an elapsed budget period without waiting for the next settlement.

        Not a force-reset: a period that has not elapsed is left alone, because
        zeroing it would erase spend the limit is still meant to bind. Idempotent
        -- rolling twice in one period is the second call returning the record
        unchanged.
        """

        return await service.roll_budget_period(principal, org_id=org_id, project_id=project_id)

    @app.put("/api/v1/quotas", response_model=QuotaRecord, tags=["governance"])
    async def put_quota(
        request: QuotaPutRequest,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
    ) -> QuotaRecord:
        quota, replayed = await service.put_quota(principal, request.to_record(), idempotency_key=idempotency_key)
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        return quota

    @app.get("/api/v1/quotas", response_model=list[QuotaRecord], tags=["governance"])
    async def list_quotas(
        org_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        project_id: str | None = None,
        principal_id: str | None = None,
    ) -> list[QuotaRecord]:
        return await service.list_quotas(principal, org_id=org_id, project_id=project_id, principal_id=principal_id)

    @app.get("/api/v1/quotas/current", response_model=QuotaRecord, tags=["governance"])
    async def get_quota(
        org_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        project_id: str | None = None,
        principal_id: str | None = None,
    ) -> QuotaRecord:
        return await service.get_quota(principal, org_id=org_id, project_id=project_id, principal_id=principal_id)

    @app.delete("/api/v1/quotas", status_code=status.HTTP_204_NO_CONTENT, tags=["governance"])
    async def delete_quota(
        org_id: str,
        response: Response,
        idempotency_key: tp.Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)],
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        project_id: str | None = None,
        principal_id: str | None = None,
    ) -> Response:
        replayed = await service.delete_quota(
            principal,
            org_id=org_id,
            project_id=project_id,
            principal_id=principal_id,
            idempotency_key=idempotency_key,
        )
        if replayed:
            response.headers["Idempotent-Replay"] = "true"
        response.status_code = status.HTTP_204_NO_CONTENT
        return response

    @app.get("/api/v1/events", response_model=list[AuditEvent], tags=["events"])
    async def list_events(
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        after_sequence: int = Query(default=0, ge=0),
        entity_type: str | None = None,
        entity_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[AuditEvent]:
        return await service.list_events(
            principal,
            org_id=org_id,
            project_id=project_id,
            after_sequence=after_sequence,
            entity_type=entity_type,
            entity_id=entity_id,
            limit=limit,
        )

    @app.get("/api/v1/fleet/events", response_model=list[AuditEvent], tags=["events"])
    async def list_fleet_events(
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        after_sequence: int = Query(default=0, ge=0),
        entity_type: str | None = None,
        entity_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> list[AuditEvent]:
        return await service.list_fleet_events(
            principal,
            after_sequence=after_sequence,
            entity_type=entity_type,
            entity_id=entity_id,
            limit=limit,
        )

    @app.get("/api/v1/events/stream", tags=["events"])
    async def stream_events(
        raw_request: Request,
        org_id: str,
        project_id: str,
        principal: tp.Annotated[Principal, Depends(authenticated_principal)],
        after_sequence: int = Query(default=0, ge=0),
        last_event_id: tp.Annotated[str | None, Header(alias="Last-Event-ID")] = None,
        follow: bool = True,
    ) -> StreamingResponse:
        if last_event_id is not None:
            try:
                after_sequence = max(after_sequence, int(last_event_id))
            except ValueError as exc:
                raise InputError("Last-Event-ID must be an integer") from exc
        service.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)

        async def event_source() -> tp.AsyncIterator[str]:
            # An audit `sequence` is a BIGSERIAL allocated at flush, but rows
            # become visible at commit. Two overlapping transactions can
            # therefore commit out of sequence order, and a follower that only
            # ever advances `sequence > cursor` would skip the lower one
            # forever. Re-scan a bounded window behind the cursor and suppress
            # ids already delivered, so a late commit is still emitted exactly
            # once. The window is bounded so memory stays flat on a long stream.
            cursor = after_sequence
            delivered: collections.OrderedDict[str, None] = collections.OrderedDict()

            def remember(event_id: str) -> bool:
                """Record an id and report whether it is newly delivered."""

                if event_id in delivered:
                    return False
                delivered[event_id] = None
                while len(delivered) > _AUDIT_STREAM_DEDUPE_LIMIT:
                    delivered.popitem(last=False)
                return True

            while True:
                events = await service.list_events(
                    principal,
                    org_id=org_id,
                    project_id=project_id,
                    after_sequence=max(0, cursor - _AUDIT_STREAM_LOOKBACK),
                    limit=1000,
                )
                for event in events:
                    cursor = max(cursor, event.sequence)
                    if not remember(event.id):
                        continue
                    data = json.dumps(event.model_dump(mode="json"), separators=(",", ":"))
                    yield f"id: {event.sequence}\nevent: {event.action}\ndata: {data}\n\n"
                if not follow or await raw_request.is_disconnected():
                    return
                new_events = await service.wait_for_events(
                    principal,
                    org_id=org_id,
                    project_id=project_id,
                    after_sequence=cursor,
                    timeout=15.0,
                )
                if not new_events:
                    yield ": keep-alive\n\n"
                else:
                    await asyncio.sleep(0)

        return StreamingResponse(
            event_source(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    dashboard_path = os.environ.get("EYVAN_DASHBOARD_DIR")
    dashboard_dir = Path(dashboard_path) if dashboard_path else None
    if dashboard_dir is not None and dashboard_dir.is_dir():
        app.mount("/", StaticFiles(directory=dashboard_dir, html=True), name="dashboard")
    return app


def create_loopback_app() -> FastAPI:
    """Build the zero-configuration development app for a loopback server bind."""

    return create_app(server_bind=ServerBindPolicy.from_host("127.0.0.1"))


def create_network_app() -> FastAPI:
    """Build a network-exposed app without implicit callable trust."""

    return create_app(server_bind=ServerBindPolicy.from_host("0.0.0.0"))


def _local_daemon_settings(
    settings: EyvanSettings,
    *,
    server_bind: ServerBindPolicy,
    port: int,
    public_url: str,
    local_state_path: Path,
    local_artifact_token_key_path: Path,
    artifact_store_uri: str,
) -> EyvanSettings:
    """Build the isolated, local-only settings used by an internally supervised daemon."""

    endpoint = ControlPlaneEndpoint.parse(public_url)
    parsed = urlsplit(endpoint.url)
    if parsed.scheme != "http" or parsed.path:
        raise ValueError("a local Eyvan daemon public URL must be a path-free HTTP URL")
    if parsed.hostname is None:
        raise ValueError("a local Eyvan daemon public URL must include a literal loopback host")
    try:
        public_address = ipaddress.ip_address(parsed.hostname)
        bind_address = ipaddress.ip_address(server_bind.host)
    except ValueError as exc:
        raise ValueError("a local Eyvan daemon requires literal loopback bind and public addresses") from exc
    if not server_bind.is_loopback or not public_address.is_loopback or public_address != bind_address:
        raise ValueError("a local Eyvan daemon public URL must exactly match its literal loopback bind address")
    public_port = parsed.port or 80
    if public_port != port:
        raise ValueError("a local Eyvan daemon public URL port must match its server port")
    return replace(
        settings,
        repository_backend=RepositoryBackend.LOCAL,
        local_state_path=local_state_path,
        public_url=endpoint.url,
        dev_auth_enabled=True,
        artifact_store_uri=artifact_store_uri,
        artifact_token_key=None,
        local_artifact_token_key_path=local_artifact_token_key_path,
    )


def create_local_daemon_app(
    *,
    server_bind: ServerBindPolicy,
    port: int,
    public_url: str,
    local_state_path: Path,
    local_artifact_token_key_path: Path,
    artifact_store_uri: str,
    runtime_profile_path: Path | None = None,
    settings: EyvanSettings | None = None,
) -> FastAPI:
    """Build one local daemon with endpoint-matched transfer URLs and isolated persistence."""

    if runtime_profile_path is not None:
        if settings is not None:
            raise ValueError("a local Eyvan daemon accepts runtime_profile_path or settings, not both")
        runtime_profile = GcpNativeRuntimeProfileStore(runtime_profile_path).load()
        configured_settings = EyvanSettings.from_gcp_native_runtime_profile(runtime_profile)
        configured_artifact_store_uri = runtime_profile.artifact_root_uri
    else:
        configured_settings = EyvanSettings.from_env() if settings is None else settings
        configured_artifact_store_uri = artifact_store_uri
    daemon_settings = _local_daemon_settings(
        configured_settings,
        server_bind=server_bind,
        port=port,
        public_url=public_url,
        local_state_path=local_state_path,
        local_artifact_token_key_path=local_artifact_token_key_path,
        artifact_store_uri=configured_artifact_store_uri,
    )
    platform_config = (
        EyvanPlatformConfig(default_agent_endpoint=DEFAULT_LOCAL_CELL_AGENT_URL)
        if runtime_profile_path is not None
        else None
    )
    return create_app(
        settings=daemon_settings,
        server_bind=server_bind,
        automatic_loopback_auth=True,
        platform_config=platform_config,
    )


def main() -> None:
    """Run the development API server."""

    parser = argparse.ArgumentParser(description="Run the Eyvan control plane")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--reload", action="store_true")
    parser.add_argument("--local-daemon", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--public-url", help=argparse.SUPPRESS)
    parser.add_argument("--local-state-path", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--local-artifact-token-key-path", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--artifact-store-uri", help=argparse.SUPPRESS)
    parser.add_argument("--runtime-profile", type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        "--log-level",
        type=ServerLogLevel,
        choices=tuple(ServerLogLevel),
        default=ServerLogLevel.INFO,
    )
    args = parser.parse_args()
    server_bind = ServerBindPolicy.from_host(args.host)
    required_local_options = (
        args.public_url,
        args.local_state_path,
        args.local_artifact_token_key_path,
        args.artifact_store_uri,
    )
    if not args.local_daemon:
        if any(option is not None for option in (*required_local_options, args.runtime_profile)):
            parser.error("local daemon options require --local-daemon")
        uvicorn.run(
            server_bind.application_factory,
            factory=True,
            host=server_bind.host,
            port=args.port,
            reload=args.reload,
            log_config=server_log_config(args.log_level),
            log_level=args.log_level.value,
        )
        return
    if args.reload:
        parser.error("--local-daemon does not support --reload")
    if any(option is None for option in required_local_options):
        parser.error("--local-daemon requires its internal public URL and local profile paths")
    assert args.public_url is not None
    assert args.local_state_path is not None
    assert args.local_artifact_token_key_path is not None
    assert args.artifact_store_uri is not None
    try:
        application = create_local_daemon_app(
            server_bind=server_bind,
            port=args.port,
            public_url=args.public_url,
            local_state_path=args.local_state_path,
            local_artifact_token_key_path=args.local_artifact_token_key_path,
            artifact_store_uri=args.artifact_store_uri,
            runtime_profile_path=args.runtime_profile,
        )
    except ValueError as exc:
        parser.error(str(exc))
    uvicorn.run(
        application,
        host=server_bind.host,
        port=args.port,
        log_config=server_log_config(args.log_level),
        log_level=args.log_level.value,
    )


if __name__ == "__main__":
    main()
