# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Process configuration loaded from environment variables."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path

from eyvan._internal.client_version import validate_minimum_client_version
from eyvan._internal.compat import StrEnum
from eyvan.capacity.providers.gcp.runtime.profile import GcpNativeRuntimeProfile
from eyvan.capacity.stage_policy import CellStagePolicy, StageEvictionPolicy

DEFAULT_LOCAL_CONTROL_PLANE_PATH = Path("~/.eyvan/control-plane.json")
"""Where a loopback control plane keeps its durable document by default."""

DEFAULT_LOCAL_ARTIFACT_TOKEN_KEY_PATH = Path("~/.eyvan/artifact-token.key")
"""Where a loopback control plane keeps its artifact-token signing key."""


def _csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


class RepositoryBackend(StrEnum):
    """Supported control-plane persistence backends."""

    MEMORY = "memory"
    LOCAL = "local"
    POSTGRES = "postgres"


def _repository_backend(value: str) -> RepositoryBackend:
    try:
        return RepositoryBackend(value)
    except ValueError as exc:
        choices = ", ".join(backend.value for backend in RepositoryBackend)
        raise ValueError(f"EYVAN_REPOSITORY_BACKEND must be one of: {choices}") from exc


@dataclass(frozen=True, slots=True)
class EyvanSettings:
    """Runtime settings shared by the API, scheduler, and cell agent."""

    database_url: str = "postgresql+asyncpg://eyvan:eyvan@127.0.0.1:5432/eyvan"
    # The in-memory backend is a loopback development default; eyvan.server.api.server_startup refuses to
    # start a network-bound server unless EYVAN_REPOSITORY_BACKEND=postgres is set explicitly.
    repository_backend: RepositoryBackend = RepositoryBackend.MEMORY
    local_state_path: Path = DEFAULT_LOCAL_CONTROL_PLANE_PATH
    auto_create_schema: bool = False
    controller_enabled: bool = True
    public_url: str = "http://127.0.0.1:8080"
    oidc_issuer: str | None = None
    oidc_audience: str = "eyvan"
    oidc_jwks_url: str | None = None
    #: The public OAuth client the *browser* uses. Distinct from the API's
    #: audience: the dashboard is a public client doing PKCE, and the API
    #: validates tokens minted for it. Baking this into the image at build time
    #: meant one image could serve exactly one identity provider.
    oidc_dashboard_client_id: str | None = None
    dev_auth_enabled: bool = False
    # Unset accepts every client. See eyvan._internal.client_version for why the gate is one-sided.
    min_client_version: str | None = None
    trusted_callable_projects: tuple[str, ...] = ()
    artifact_store_uri: str = "~/.eyvan/managed-artifacts"
    artifact_token_key: str | None = None
    local_artifact_token_key_path: Path = DEFAULT_LOCAL_ARTIFACT_TOKEN_KEY_PATH
    artifact_token_ttl_s: int = 604800
    artifact_max_callable_bytes: int = 64 * 1024 * 1024
    artifact_max_result_bytes: int = 256 * 1024 * 1024
    artifact_max_workspace_bytes: int = 64 * 1024 * 1024
    #: How long a secret-read capability stays valid. Short next to the artifact
    #: TTL because a secret capability hands back a *value*: once read, no later
    #: revocation can un-read it.
    #:
    #: Not shorter than an hour, though. The token is minted at dispatch and
    #: presented when the cell launches the unit, and between those two moments
    #: sit provisioning, an image pull and a workspace download. A TTL tuned to
    #: how long the fetch takes would expire during a slow bootstrap and fail
    #: the workload for a reason that has nothing to do with its secrets. The
    #: binding that actually revokes is the attempt, not this.
    secret_token_ttl_s: int = 3600
    scheduler_interval_s: float = 2.0
    cell_reconcile_interval_s: float = 15.0
    cell_reconcile_concurrency: int = 8
    lease_ttl_s: int = 120
    indeterminate_execution_grace_s: int = 900
    """How long an execution status may stay unreadable before its host is replaced.

    An INDETERMINATE observation is not a verdict about the workload, so the
    controller keeps polling -- but polling forever means a lease and an
    accelerator are held by something nobody can read. After this window the
    cell is drained: the host is the only thing that can be reclaimed with
    certainty, and replacing it destroys any process that did survive.
    """

    checkpoint_grace_s: int = 300
    preemption_stop_grace_s: int = 300
    outbox_claim_ttl_s: int = 90
    # Retention windows. 0 disables pruning for that table, which is the
    # historical behaviour: nothing was ever deleted and idempotency keys were
    # therefore honored forever.
    outbox_retention_s: int = 7 * 24 * 3600
    audit_retention_s: int = 90 * 24 * 3600
    idempotency_retention_s: int = 30 * 24 * 3600
    #: This control plane's identity among its peers. Empty means "not federated",
    #: which is the default and the only mode most fleets ever need: one plane per
    #: region with projects pinned to one region already works by construction.
    plane_id: str = ""
    #: Namespace the Kubernetes capacity backend creates cells in. One namespace
    #: per agent: the Job names are derived from cell ids, so two agents sharing a
    #: namespace would collide on names as surely as they would on fleet state.
    kubernetes_namespace: str = "eyvan"
    kubernetes_worker_image: str = ""
    retention_interval_s: float = 3600.0
    artifact_collection_interval_s: float = 3600.0
    artifact_destructive_collection_enabled: bool = False
    artifact_recovery_grace_s: float = 900.0
    """How often artifact lifecycle recovery and dry-run collection execute.

    Collection was implemented, tested, and called by nothing, so managed
    artifact storage grew without bound in production while the index, quota
    accounting, holds and reverse references all worked.
    """

    artifact_retention_s: int = 7 * 24 * 3600
    #: How long an attempt's archived logs are kept. Longer than the general
    #: artifact window on purpose: a callable can be rebuilt from its source,
    #: while logs are the only record of what a run did, and the question they
    #: answer is usually asked well after the run. Zero keeps them forever.
    log_retention_s: int = 30 * 24 * 3600
    """How long an unreferenced artifact is kept before it may be collected.

    Distinct from ``grace_period_s``, which is an anti-race window around a
    single collection pass. 0 disables age-based collection entirely.
    """

    metrics_export_interval_s: float = 60.0
    #: How often services are brought back to their declared replica count.
    #: Faster than the housekeeping phases because this is the loop that
    #: replaces a dead replica -- its interval is the floor on how long a
    #: service runs short. Zero disables it.
    service_reconcile_interval_s: float = 5.0
    """How often fleet-state, capacity and spend gauges are refreshed. 0 disables.

    Gauges describe a level, not a rate, so they are refreshed on their own slow
    interval rather than on every scheduling pass.
    """

    grant_expiry_interval_s: float = 300.0
    """How often expired federated budget grants are swept. 0 disables it.

    A grant's expiry is mandatory, so without the sweep headroom is never
    returned to the granting plane and a tenant can outspend its federated
    budget indefinitely.
    """
    #: How often the leader advances cell billing clocks. Slower than reconcile
    #: because the accrual charges elapsed time rather than a fixed tick, so a
    #: longer interval costs resolution, never accuracy. Zero disables it.
    cell_billing_interval_s: float = 300.0
    #: How often managed asset staging and stage eviction run. Zero disables
    #: both, which is the default and today's behaviour: locality stays the soft
    #: preference it has always been until an operator sizes a cache.
    cell_stage_interval_s: float = 0.0
    #: Hard ceiling on the bytes Eyvan may own on one cell's disk. Zero is the
    #: off switch, and it is off by construction: nothing is staged, so nothing
    #: has to be reclaimed and no budget can be exceeded.
    cell_stage_budget_bytes: int = 0
    #: Largest single asset staging may fetch. Separate from the budget because
    #: it answers a different question -- the budget is how much of a host Eyvan
    #: may use, this is how large one asset may be before staging it is somebody's
    #: explicit decision rather than a cache's. Must not exceed the budget.
    cell_stage_max_bytes: int = 0
    #: Which staged asset is given up first. Named rather than defaulted at the
    #: policy layer; the setting carries a value only so the loader has one, and
    #: an unknown name is refused rather than silently replaced.
    cell_stage_eviction_policy: str = "least_recently_used"
    #: Whether the eviction phase may actually delete. Off by default, so the
    #: phase computes and logs exactly the plan it would execute and removes
    #: nothing -- a cache that starts deleting the moment it is switched on gives
    #: an operator no way to see what it was about to do first.
    cell_stage_eviction_enabled: bool = False
    #: Staging attempts that may end without an answer before a stage is blocked.
    #: Bounds a fetch that can never succeed without treating one timeout as a
    #: verdict about the asset.
    cell_stage_max_attempts: int = 3
    retention_batch_limit: int = 1000
    agent_shared_secret: str | None = field(default=None, repr=False)
    #: Bearer token a Prometheus scraper presents at ``/metrics``. A separate
    #: credential rather than an OIDC identity because a scraper has none, and
    #: because the alternative in practice is leaving the endpoint open: it
    #: exports ``eyvan_project_spend_usd`` labeled by (org, project), so anyone
    #: who can reach the port reads every tenant's spend. Required for a
    #: network-bound server, which is what makes the exposure impossible rather
    #: than merely discouraged.
    metrics_token: str | None = field(default=None, repr=False)
    native_worker_root_secret: str | None = field(default=None, repr=False)
    fleet_state_uri: str | None = None
    capacity_backend: str = "gcp"
    execution_backend: str = "eyvan-native"
    agent_state_dir: str = "~/.eyvan/agent"
    ray_jobs_port: int = 8265
    native_worker_port: int = 8100
    native_worker_package: str | None = None
    native_worker_package_sha256: str | None = None
    native_worker_package_generation: str | None = None
    native_worker_artifact_root_uri: str | None = None
    native_worker_public_ingress_tag: str | None = None
    native_worker_wheelhouse: str | None = None
    native_worker_wheelhouse_sha256: str | None = None
    native_worker_wheelhouse_generation: str | None = None
    native_worker_state_dir: str = "/var/lib/eyvan/native-worker"
    platform_config_path: str = "/etc/eyvan/platform.yaml"

    def __post_init__(self) -> None:
        """Validate settings that participate in durable controller ownership."""

        if self.outbox_claim_ttl_s <= 0:
            raise ValueError("outbox_claim_ttl_s must be positive")
        for name in ("outbox_retention_s", "audit_retention_s", "idempotency_retention_s"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative; use 0 to disable pruning")
        if self.retention_batch_limit <= 0:
            raise ValueError("retention_batch_limit must be positive")
        if not math.isfinite(self.artifact_collection_interval_s) or self.artifact_collection_interval_s <= 0:
            raise ValueError("artifact_collection_interval_s must be finite and positive")
        if not math.isfinite(self.artifact_recovery_grace_s) or self.artifact_recovery_grace_s < 0:
            raise ValueError("artifact_recovery_grace_s must be finite and non-negative")
        for name in ("artifact_retention_s", "log_retention_s"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative; use 0 to disable collection")
        self._validate_cell_stage_settings()
        validate_minimum_client_version(self.min_client_version)

    def _validate_cell_stage_settings(self) -> None:
        """Refuse stage bounds that would let a cache bound nothing.

        Every check here is the difference between a budget and a number. A
        negative ceiling, a per-action limit above the whole budget, or an
        eviction policy nobody recognises all produce a configuration that looks
        like it constrains a disk and does not.
        """

        if not math.isfinite(self.cell_stage_interval_s) or self.cell_stage_interval_s < 0:
            raise ValueError("cell_stage_interval_s must be finite and non-negative; use 0 to disable staging")
        for name in ("cell_stage_budget_bytes", "cell_stage_max_bytes"):
            value = getattr(self, name)
            if value < 0:
                raise ValueError(f"{name} must not be negative; use 0 to disable staging")
        if self.cell_stage_max_bytes > self.cell_stage_budget_bytes:
            raise ValueError("cell_stage_max_bytes must not exceed cell_stage_budget_bytes")
        if self.cell_stage_max_attempts < 1:
            raise ValueError("cell_stage_max_attempts must be positive")
        if self.cell_stage_eviction_policy not in {policy.value for policy in StageEvictionPolicy}:
            choices = ", ".join(sorted(policy.value for policy in StageEvictionPolicy))
            raise ValueError(f"cell_stage_eviction_policy must be one of: {choices}")

    def cell_stage_policy(self) -> CellStagePolicy:
        """Return the validated staging bounds this deployment declared.

        A method rather than a field so the policy is constructed from settings
        that have already been validated together, and so a deployment that never
        enables staging carries no policy object it does not use.
        """

        return CellStagePolicy(
            budget_bytes=self.cell_stage_budget_bytes,
            max_stage_bytes=self.cell_stage_max_bytes,
            eviction=StageEvictionPolicy(self.cell_stage_eviction_policy),
            max_attempts=self.cell_stage_max_attempts,
        )

    @classmethod
    def from_gcp_native_runtime_profile(cls, profile: GcpNativeRuntimeProfile) -> EyvanSettings:
        """Build local API and agent settings from one owner-only GCP runtime profile."""

        if not isinstance(profile, GcpNativeRuntimeProfile):
            raise TypeError("GCP native runtime settings require a GcpNativeRuntimeProfile")
        return cls(
            agent_shared_secret=profile.agent_shared_secret,
            native_worker_root_secret=profile.native_worker_root_secret,
            fleet_state_uri=os.getenv("EYVAN_FLEET_STATE_URI", profile.fleet_state_uri),
            # Operator-owned and *not* something a runtime profile can know, so it
            # is read from the environment here for the same reason
            # ``fleet_state_uri`` is. Dropping it made the documented
            # ``EYVAN_TRUSTED_CALLABLE_PROJECTS`` silently inert on the native GCP
            # path -- every callable submission outside the loopback development
            # project was refused with no way to grant it. Found live 2026-08-01.
            trusted_callable_projects=_csv(os.getenv("EYVAN_TRUSTED_CALLABLE_PROJECTS", "")),
            capacity_backend="gcp",
            execution_backend="eyvan-native",
            artifact_store_uri=profile.artifact_root_uri,
            native_worker_package=profile.native_worker_wheel_uri,
            native_worker_artifact_root_uri=profile.artifact_root_uri,
            native_worker_public_ingress_tag=profile.native_worker_public_ingress_tag,
        )

    @classmethod
    def from_env(cls) -> EyvanSettings:
        """Load settings from ``EYVAN_*`` environment variables."""

        defaults = cls()

        def truthy(name: str, default: bool = False) -> bool:
            raw = os.getenv(name)
            return default if raw is None else raw.lower() in {"1", "true", "yes", "on"}

        return cls(
            database_url=os.getenv("EYVAN_DATABASE_URL", defaults.database_url),
            repository_backend=_repository_backend(
                os.getenv("EYVAN_REPOSITORY_BACKEND", defaults.repository_backend.value)
            ),
            auto_create_schema=truthy("EYVAN_AUTO_CREATE_SCHEMA"),
            controller_enabled=truthy("EYVAN_CONTROLLER_ENABLED", defaults.controller_enabled),
            public_url=os.getenv("EYVAN_PUBLIC_URL", defaults.public_url),
            oidc_issuer=os.getenv("EYVAN_OIDC_ISSUER"),
            oidc_dashboard_client_id=os.getenv("EYVAN_OIDC_DASHBOARD_CLIENT_ID"),
            oidc_audience=os.getenv("EYVAN_OIDC_AUDIENCE", defaults.oidc_audience),
            oidc_jwks_url=os.getenv("EYVAN_OIDC_JWKS_URL"),
            dev_auth_enabled=truthy("EYVAN_DEV_AUTH_ENABLED"),
            min_client_version=os.getenv("EYVAN_MIN_CLIENT_VERSION") or None,
            trusted_callable_projects=_csv(os.getenv("EYVAN_TRUSTED_CALLABLE_PROJECTS", "")),
            artifact_store_uri=os.getenv("EYVAN_ARTIFACT_STORE_URI", defaults.artifact_store_uri),
            artifact_token_key=os.getenv("EYVAN_ARTIFACT_TOKEN_KEY"),
            artifact_token_ttl_s=int(os.getenv("EYVAN_ARTIFACT_TOKEN_TTL_S", defaults.artifact_token_ttl_s)),
            artifact_max_callable_bytes=int(
                os.getenv("EYVAN_ARTIFACT_MAX_CALLABLE_BYTES", defaults.artifact_max_callable_bytes)
            ),
            artifact_max_result_bytes=int(
                os.getenv("EYVAN_ARTIFACT_MAX_RESULT_BYTES", defaults.artifact_max_result_bytes)
            ),
            secret_token_ttl_s=int(os.getenv("EYVAN_SECRET_TOKEN_TTL_S", defaults.secret_token_ttl_s)),
            artifact_max_workspace_bytes=int(
                os.getenv("EYVAN_ARTIFACT_MAX_WORKSPACE_BYTES", defaults.artifact_max_workspace_bytes)
            ),
            scheduler_interval_s=float(os.getenv("EYVAN_SCHEDULER_INTERVAL_S", defaults.scheduler_interval_s)),
            cell_reconcile_interval_s=float(
                os.getenv("EYVAN_CELL_RECONCILE_INTERVAL_S", defaults.cell_reconcile_interval_s)
            ),
            cell_reconcile_concurrency=int(
                os.getenv("EYVAN_CELL_RECONCILE_CONCURRENCY", defaults.cell_reconcile_concurrency)
            ),
            lease_ttl_s=int(os.getenv("EYVAN_LEASE_TTL_S", defaults.lease_ttl_s)),
            checkpoint_grace_s=int(os.getenv("EYVAN_CHECKPOINT_GRACE_S", defaults.checkpoint_grace_s)),
            preemption_stop_grace_s=int(os.getenv("EYVAN_PREEMPTION_STOP_GRACE_S", defaults.preemption_stop_grace_s)),
            outbox_claim_ttl_s=int(os.getenv("EYVAN_OUTBOX_CLAIM_TTL_S", defaults.outbox_claim_ttl_s)),
            outbox_retention_s=int(os.getenv("EYVAN_OUTBOX_RETENTION_S", defaults.outbox_retention_s)),
            audit_retention_s=int(os.getenv("EYVAN_AUDIT_RETENTION_S", defaults.audit_retention_s)),
            idempotency_retention_s=int(os.getenv("EYVAN_IDEMPOTENCY_RETENTION_S", defaults.idempotency_retention_s)),
            plane_id=os.getenv("EYVAN_PLANE_ID", defaults.plane_id),
            kubernetes_namespace=os.getenv("EYVAN_KUBERNETES_NAMESPACE", defaults.kubernetes_namespace),
            kubernetes_worker_image=os.getenv("EYVAN_KUBERNETES_WORKER_IMAGE", defaults.kubernetes_worker_image),
            retention_interval_s=float(os.getenv("EYVAN_RETENTION_INTERVAL_S", defaults.retention_interval_s)),
            artifact_collection_interval_s=float(
                os.getenv("EYVAN_ARTIFACT_COLLECTION_INTERVAL_S", defaults.artifact_collection_interval_s)
            ),
            artifact_destructive_collection_enabled=truthy(
                "EYVAN_ARTIFACT_DESTRUCTIVE_COLLECTION_ENABLED",
                defaults.artifact_destructive_collection_enabled,
            ),
            artifact_recovery_grace_s=float(
                os.getenv("EYVAN_ARTIFACT_RECOVERY_GRACE_S", defaults.artifact_recovery_grace_s)
            ),
            artifact_retention_s=int(os.getenv("EYVAN_ARTIFACT_RETENTION_S", defaults.artifact_retention_s)),
            log_retention_s=int(os.getenv("EYVAN_LOG_RETENTION_S", defaults.log_retention_s)),
            service_reconcile_interval_s=float(
                os.getenv("EYVAN_SERVICE_RECONCILE_INTERVAL_S", defaults.service_reconcile_interval_s)
            ),
            metrics_export_interval_s=float(
                os.getenv("EYVAN_METRICS_EXPORT_INTERVAL_S", defaults.metrics_export_interval_s)
            ),
            grant_expiry_interval_s=float(os.getenv("EYVAN_GRANT_EXPIRY_INTERVAL_S", defaults.grant_expiry_interval_s)),
            cell_billing_interval_s=float(os.getenv("EYVAN_CELL_BILLING_INTERVAL_S", defaults.cell_billing_interval_s)),
            cell_stage_interval_s=float(os.getenv("EYVAN_CELL_STAGE_INTERVAL_S", defaults.cell_stage_interval_s)),
            cell_stage_budget_bytes=int(os.getenv("EYVAN_CELL_STAGE_BUDGET_BYTES", defaults.cell_stage_budget_bytes)),
            cell_stage_max_bytes=int(os.getenv("EYVAN_CELL_STAGE_MAX_BYTES", defaults.cell_stage_max_bytes)),
            cell_stage_eviction_policy=os.getenv(
                "EYVAN_CELL_STAGE_EVICTION_POLICY", defaults.cell_stage_eviction_policy
            ),
            cell_stage_eviction_enabled=truthy(
                "EYVAN_CELL_STAGE_EVICTION_ENABLED",
                defaults.cell_stage_eviction_enabled,
            ),
            cell_stage_max_attempts=int(os.getenv("EYVAN_CELL_STAGE_MAX_ATTEMPTS", defaults.cell_stage_max_attempts)),
            retention_batch_limit=int(os.getenv("EYVAN_RETENTION_BATCH_LIMIT", defaults.retention_batch_limit)),
            agent_shared_secret=os.getenv("EYVAN_AGENT_SHARED_SECRET"),
            metrics_token=os.getenv("EYVAN_METRICS_TOKEN"),
            native_worker_root_secret=os.getenv("EYVAN_NATIVE_WORKER_ROOT_SECRET"),
            fleet_state_uri=os.getenv("EYVAN_FLEET_STATE_URI"),
            capacity_backend=os.getenv("EYVAN_CAPACITY_BACKEND", defaults.capacity_backend),
            execution_backend=os.getenv("EYVAN_EXECUTION_BACKEND", defaults.execution_backend),
            agent_state_dir=os.getenv("EYVAN_AGENT_STATE_DIR", defaults.agent_state_dir),
            ray_jobs_port=int(os.getenv("EYVAN_RAY_JOBS_PORT", defaults.ray_jobs_port)),
            native_worker_port=int(os.getenv("EYVAN_NATIVE_WORKER_PORT", defaults.native_worker_port)),
            native_worker_package=os.getenv("EYVAN_NATIVE_WORKER_PACKAGE"),
            native_worker_package_sha256=os.getenv("EYVAN_NATIVE_WORKER_PACKAGE_SHA256") or None,
            native_worker_package_generation=os.getenv("EYVAN_NATIVE_WORKER_PACKAGE_GENERATION") or None,
            native_worker_wheelhouse=os.getenv("EYVAN_NATIVE_WORKER_WHEELHOUSE"),
            native_worker_wheelhouse_sha256=os.getenv("EYVAN_NATIVE_WORKER_WHEELHOUSE_SHA256") or None,
            native_worker_wheelhouse_generation=os.getenv("EYVAN_NATIVE_WORKER_WHEELHOUSE_GENERATION") or None,
            native_worker_state_dir=os.getenv("EYVAN_NATIVE_WORKER_STATE_DIR", defaults.native_worker_state_dir),
            # Both are read by _gcp_factory (drivers/registry.py) and were set only by
            # from_gcp_native_runtime_profile, so a locally-profiled agent had them and an
            # env-configured production agent silently got None: no artifact root for its
            # native workers, and no ingress network tag.
            native_worker_artifact_root_uri=os.getenv("EYVAN_NATIVE_WORKER_ARTIFACT_ROOT_URI"),
            native_worker_public_ingress_tag=os.getenv("EYVAN_NATIVE_WORKER_PUBLIC_INGRESS_TAG"),
            platform_config_path=os.getenv("EYVAN_PLATFORM_CONFIG_PATH", defaults.platform_config_path),
        )
