# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Transactional application service for Eyvan control-plane operations."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import subprocess
import typing as tp
import uuid
from datetime import datetime, timedelta

from pydantic import BaseModel

from eyvan._internal.json_types import JsonObject, JsonValue, model_json
from eyvan.artifacts.managed import (
    CONTENT_ADDRESSED_KINDS,
    MAX_MANAGED_ARTIFACT_INVENTORY_PAGE_SIZE,
    ManagedArtifactInventoryError,
    ManagedArtifactInventoryStore,
    ManagedArtifactKind,
    ManagedArtifactObjectMetadata,
    ManagedArtifactRef,
    ManagedArtifactStore,
    is_managed_uri,
    parse_managed_uri,
)
from eyvan.artifacts.managed import result_uri as managed_result_uri
from eyvan.capacity.planning import cell_matches_template, cell_provides_pool_supply
from eyvan.domain import costing
from eyvan.domain.availability import p95_latency
from eyvan.domain.carbon import CarbonSource, CarbonUnavailable, NoCarbonSource, fresh_reading
from eyvan.domain.models import SchedulingResult
from eyvan.records import (
    BILLABLE_CELL_STATES,
    CALLABLE_SIGNING_KEY_ENV,
    DEFAULT_MAX_RETRIES,
    MAX_GCP_TPU_MULTISLICE_WORKERS,
    REMOTE_CALLABLE_SIGNATURE_ERROR,
    REMOTE_CALLABLE_SIGNING_KEY_ERROR,
    WORKER_COUNT_LABEL,
    ArtifactCollection,
    ArtifactHold,
    ArtifactInventoryEntry,
    ArtifactInventoryReport,
    ArtifactInventoryStatus,
    ArtifactReferenceRecord,
    ArtifactUsage,
    AuditEvent,
    AutoscaleBlockReason,
    BudgetRecord,
    BudgetStatus,
    CallableExecution,
    CapacityClass,
    CapacityPoolRecord,
    CarbonReport,
    CellBillingAccrual,
    CellBillingReport,
    CellRecord,
    CellRejection,
    CellState,
    CommandExecution,
    CoordinationKind,
    FederatedBudgetStatus,
    GcpQueueAdmissionDenialTarget,
    IdempotencyRecord,
    LaunchLatencyReport,
    LeaseRecord,
    LeaseState,
    ManagedArtifactRecord,
    OrganizationRecord,
    OutboxRecord,
    OutboxState,
    Permission,
    PolicyRecord,
    PoolEstimate,
    PoolLaunchLatency,
    PoolWarmth,
    Principal,
    ProjectRecord,
    QueueTrend,
    QueueTrendPoint,
    QuotaRecord,
    RegionCarbon,
    ResourceGang,
    ResourceLimits,
    Role,
    RoleGrant,
    RoleGrantRecord,
    UserProgressReport,
    WarmthReport,
    WorkerResourceScope,
    WorkloadDefinition,
    WorkloadDispatchReservation,
    WorkloadEstimate,
    WorkloadExplanation,
    WorkloadRecord,
    WorkloadState,
    WorkspaceRef,
    artifact_reference_id,
    resource_quantities,
    utcnow,
    validate_capacity_pool_quota_groups,
)
from eyvan.secrets import matching_secret_reference_root
from eyvan.server.persistence.base import Repository, RepositorySession
from eyvan.server.service.authz import Authorizer
from eyvan.server.service.batch import (
    DEFAULT_MAX_FAN_OUT,
    BatchRequest,
    child_idempotency_key,
    expand,
    task_index,
)
from eyvan.server.service.callable_trust import CallableProjectTrustPolicy
from eyvan.server.service.dag import DagNode
from eyvan.server.service.dag import validate as dag_validate
from eyvan.server.service.errors import (
    AuthorizationError,
    ConflictError,
    InputError,
    NoMatchingCapacityPoolError,
    NotFoundError,
)
from eyvan.server.service.federation import (
    BudgetGrantRecord,
    GrantState,
    PlanePeerRecord,
    QuotaGrantRecord,
    exceeds_quota_headroom,
    federated_spend,
    issuable_amount,
    outstanding_total,
    quota_headroom,
)
from eyvan.server.service.groups import MAX_GROUP_MEMBERS, GroupRole
from eyvan.server.service.groups import member_names as group_member_names
from eyvan.server.service.groups import validate as group_validate
from eyvan.server.service.locality import build_index as locality_index
from eyvan.server.service.locality import preferred_cells_for
from eyvan.server.service.pricing_resolver import accelerator_unit_price
from eyvan.server.service.scheduling import SchedulingService
from eyvan.server.service.services import CHILD_REVISION, ServicePolicy, service_metadata, service_policy
from eyvan.server.service.state import (
    ACTIVE_LEASE_STATES,
    TERMINAL_WORKLOAD_STATES,
    InvalidTransition,
    ensure_lease_transition,
    ensure_workload_transition,
)
from eyvan.server.service.subjobs import (
    MAX_JOB_TREE_DEPTH,
    MAX_LIVE_CHILDREN,
    JobTreeTooDeepError,
    JobTreeTooWideError,
    collect_descendants,
    parent_depth,
    validate_parent,
)


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class WorkloadTransitionUpdates(tp.TypedDict, total=False):
    """Fields that may change alongside a workload state transition."""

    remote_id: str | None
    checkpoint_uri: str | None
    failure_reason: str | None
    preemption_id: str | None
    scheduling_reason: str | None
    autoscale_block_reason: AutoscaleBlockReason | None
    queue_admission_denial: GcpQueueAdmissionDenialTarget | None


def _fingerprint(value: BaseModel | tp.Mapping[str, JsonValue]) -> str:
    payload = model_json(value) if isinstance(value, BaseModel) else dict(value)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def storage_scope(org_id: str, project_id: str) -> str:
    payload = json.dumps((org_id, project_id), ensure_ascii=True, separators=(",", ":")).encode()
    return f"scope_{hashlib.sha256(payload).hexdigest()}"


_scope = storage_scope


def _validate_idempotency_key(key: str) -> str:
    key = key.strip()
    if not key:
        raise InputError("Idempotency-Key must not be empty")
    if len(key) > 200:
        raise InputError("Idempotency-Key must be at most 200 characters")
    return key


def _resource_quantities(resources) -> dict[str, float]:
    return {
        "cpu": float(resources.cpu),
        "memory_gib": float(resources.memory_gib),
        "gpu": float(resources.gpu),
        "tpu": float(resources.tpu),
        **{name: float(value) for name, value in resources.custom.items()},
    }


def _dump_limits(limits: ResourceLimits) -> JsonObject:
    """Render a limit vector for a fingerprint or an audit payload.

    Only the non-zero dimensions, so re-issuing the same grant produces the same
    fingerprint whether or not the caller spelled out the zeros.
    """

    return {name: quantity for name, quantity in sorted(resource_quantities(limits).items()) if quantity}


def _capacity_apply_payload(cell: CellRecord) -> JsonObject:
    """Build the durable agent request for one registered cell generation."""

    return {
        "cell_id": cell.id,
        "provider": cell.provider,
        "capacity_backend": cell.capacity_backend,
        "project": cell.provider_project,
        "region": cell.region,
        "zone": cell.zone,
        "accelerator_type": cell.accelerator_type,
        "capacity_class": cell.capacity_class.value,
        "executor": cell.executor,
        "agent_endpoint": cell.endpoint,
        "pool_id": cell.pool_id,
        "provider_options": cell.provider_options,
        "runtime": cell.runtime.model_dump(mode="json"),
    }


def _changed_cell_capacity_identity(existing: CellRecord, requested: CellRecord) -> tuple[str, ...]:
    """Return immutable capacity identity fields changed by an upsert."""

    changed: list[str] = []
    if existing.provider != requested.provider:
        changed.append("provider")
    if existing.capacity_backend != requested.capacity_backend:
        changed.append("capacity_backend")
    if existing.provider_project != requested.provider_project:
        changed.append("provider_project")
    if existing.region != requested.region:
        changed.append("region")
    if existing.zone != requested.zone:
        changed.append("zone")
    if existing.accelerator_type != requested.accelerator_type:
        changed.append("accelerator_type")
    if existing.capacity_class != requested.capacity_class:
        changed.append("capacity_class")
    if existing.executor != requested.executor:
        changed.append("executor")
    if existing.pool_id != requested.pool_id:
        changed.append("pool_id")
    if existing.provider_options != requested.provider_options:
        changed.append("provider_options")
    if existing.runtime != requested.runtime:
        changed.append("runtime")
    return tuple(changed)


_CELL_LIFECYCLE_TOPIC_BY_STATE: tp.Final[dict[CellState, str]] = {
    CellState.DRAINING: "capacity.drain",
    CellState.DELETING: "capacity.release",
}
_ACTIVE_CAPACITY_APPLY_STATES: tp.Final[frozenset[OutboxState]] = frozenset(
    {OutboxState.PENDING, OutboxState.PROCESSING, OutboxState.FAILED}
)


def _is_active_capacity_apply(command: OutboxRecord, cell: CellRecord) -> bool:
    """Return whether an outbox command still owns this cell's current provisioning generation."""

    return (
        command.topic == "capacity.apply"
        and command.aggregate_type == "cell"
        and command.aggregate_id == cell.id
        and command.generation == cell.generation
        and command.state in _ACTIVE_CAPACITY_APPLY_STATES
    )


def _decimal_sum(*values: float) -> float:
    """Deprecated alias; every money computation lives in ``eyvan.domain.costing``."""

    return costing.total(*values)


logger = logging.getLogger(__name__)

_EXPLAIN_MAX_CELLS = 32

#: Ledger rows one queue-trend query reads. Bounded because this backs a dashboard
#: panel that polls: an unbounded scan of the fastest-growing table in the system
#: would make an idle browser tab the heaviest query the control plane serves.
_QUEUE_TREND_MAX_EVENTS = 5_000
#: Prevent one dashboard or CLI query from allocating an arbitrarily large
#: response. Forty-eight half-hour buckets over a day is already denser than the
#: shipped chart renders; callers that need raw time series should use metrics.
QUEUE_TREND_MAX_BUCKETS = 48
#: A sub-minute trend is scheduler jitter, not a useful operations window; a
#: window wider than 31 days belongs in retained metrics rather than a polling
#: audit-ledger query.
QUEUE_TREND_MIN_WINDOW_S = 60.0
QUEUE_TREND_MAX_WINDOW_S = 31 * 86_400.0

#: How long an indexed artifact is protected from collection regardless of its
#: references. Closes a race that is easy to miss: an upload indexes the object
#: *before* any workload can name it -- the submitter needs the URI to build the
#: spec -- so a freshly uploaded callable is indistinguishable from abandoned
#: garbage until the submission lands. One day is far longer than that window and
#: costs only delayed reclamation.
DEFAULT_ARTIFACT_GRACE_PERIOD_S = 86_400.0

#: URI prefix of the managed result kind. Compared as a prefix rather than parsed:
#: the supersession sweep runs over every reference of every workload, and the only
#: question it asks is which kind the URI names.
_MANAGED_RESULT_PREFIX = f"eyvan://{ManagedArtifactKind.RESULTS.value}/"


def _explain_base(workload: WorkloadRecord) -> dict[str, tp.Any]:
    """Return the durable explanation fields shared by every scheduling verdict."""

    return {
        "workload_id": workload.id,
        "state": workload.state,
        "scheduling_reason": workload.scheduling_reason,
        "autoscale_block_reason": workload.autoscale_block_reason,
        "queue_admission_denial": workload.queue_admission_denial,
    }


def _explain_from_scheduling_result(workload: WorkloadRecord, result: SchedulingResult) -> WorkloadExplanation:
    """Project one pending workload's outcome out of a whole scheduling pass."""

    base = _explain_base(workload)
    placement = next((candidate for candidate in result.placements if candidate.workload_id == workload.id), None)
    if placement is not None:
        return WorkloadExplanation(
            **base,
            placeable=True,
            placement_cell_id=placement.lease.cell_id,
            message="the scheduler would place this workload now",
        )
    preemption = next((candidate for candidate in result.preemptions if candidate.workload_id == workload.id), None)
    if preemption is not None:
        victims = ", ".join(sorted({victim.workload_id for victim in preemption.victims}))
        return WorkloadExplanation(
            **base,
            placeable=True,
            placement_cell_id=preemption.cell_id,
            message=f"the scheduler would place this workload by preempting: {victims}",
        )
    infeasibility = next(
        (candidate for candidate in result.infeasibilities if candidate.workload_id == workload.id), None
    )
    if infeasibility is None:
        return WorkloadExplanation(
            **base,
            placeable=False,
            message="the scheduler did not consider this workload in the current pass",
        )
    rejected = sorted(infeasibility.cells, key=lambda cell: cell.cell_id)
    return WorkloadExplanation(
        **base,
        placeable=False,
        code=infeasibility.code,
        message=infeasibility.message,
        cells=tuple(
            CellRejection(cell_id=cell.cell_id, code=cell.code, message=cell.message)
            for cell in rejected[:_EXPLAIN_MAX_CELLS]
        ),
        considered_cell_count=len(rejected),
    )


#: How long a cancellation must have been failing before it counts as stuck
#: rather than in flight.
#:
#: Matches ``STUCK_OUTBOX_AFTER_S``, the point at which I3 reports the same
#: command. An operator should not be able to force-finalize something the
#: invariants still call healthy -- two thresholds that disagree would mean the
#: break-glass and the alarm describe different fleets.
STUCK_CANCEL_AFTER_S = 900.0


class ControlPlaneService:
    """Own authorization, lifecycle validation, idempotency, and audit atomicity."""

    def __init__(
        self,
        repository: Repository,
        *,
        authorizer: Authorizer | None = None,
        callable_trust: CallableProjectTrustPolicy = CallableProjectTrustPolicy(),
        scheduler: SchedulingService | None = None,
        #: Only the artifact collector needs it, and only to *delete*. Optional so
        #: every deployment that does not run a collector -- which is all of them
        #: until an operator opts in -- carries no path to removing bytes at all.
        artifact_store: ManagedArtifactStore | None = None,
        #: Read-only/additive inventory can be exposed before the deletion state
        #: machine. It is deliberately separate from ``artifact_store`` so wiring
        #: API uploads for inventory does not silently enable the current unsafe
        #: collector path in a deployment.
        inventory_store: ManagedArtifactInventoryStore | None = None,
        #: This plane's identity among peers. Empty means "not federated", which is
        #: the default: one plane per region with projects pinned to one region
        #: already works, and a plane with no id simply cannot issue grants.
        plane_id: str = "",
        #: Where grid intensity comes from. ``NoCarbonSource`` by default and in
        #: every shipped configuration: a provider Eyvan invented would publish
        #: numbers nobody can check, and the reporting path is the only place a
        #: real one may ever be consulted.
        carbon: CarbonSource | None = None,
    ):
        self.repository = repository
        self.authorizer = authorizer or Authorizer()
        self.callable_trust = callable_trust
        self.scheduler = scheduler
        self.artifact_store = artifact_store
        self.inventory_store = inventory_store
        self.plane_id = plane_id
        self.carbon = carbon or NoCarbonSource()

    async def schedule_once(self, principal: Principal):
        """Return one pure scheduling plan without committing its proposed leases."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        if self.scheduler is None:
            raise InputError("a scheduling service is not configured")
        async with self.repository.transaction() as tx:
            workloads = [
                workload
                for workload in await tx.list_workloads()
                if workload.state is not WorkloadState.PENDING or workload.preemption_id is None
            ]
            cells = await tx.list_cells()
            leases = await tx.list_leases()
            budgets = await tx.list_budgets()
            quotas = await tx.list_quotas()
            policies = await tx.list_policies()
            # The read that made grants real. They were issued, reported on and
            # expired, and never consulted by the thing that decides whether a
            # workload may run -- so a grant was recorded and licensed nothing.
            grants = await tx.list_budget_grants() if self.plane_id else []
            quota_grants = await tx.list_quota_grants() if self.plane_id else []
        return self.scheduler.schedule(
            workloads,
            cells,
            leases=leases,
            budgets=budgets,
            quotas=quotas,
            policies=policies,
            grants=grants,
            quota_grants=quota_grants,
            plane_id=self.plane_id,
        )

    async def explain_workload(self, principal: Principal, workload_id: str) -> WorkloadExplanation:
        """Return why one workload is or is not placeable right now.

        The scheduler is pure, so this re-runs it against current state instead
        of reporting a verdict cached when the workload was last considered.
        The answer therefore reflects the fleet as it is now, and no second
        durable field has to be kept in sync at every ``scheduling_reason``
        write site.
        """

        if self.scheduler is None:
            raise InputError("a scheduling service is not configured")
        async with self.repository.transaction() as tx:
            workload = await self._require_workload(tx, workload_id)
            self.authorizer.require(principal, Permission.READ, org_id=workload.org_id, project_id=workload.project_id)
            if workload.state is not WorkloadState.PENDING:
                # Only pending workloads are scheduled, so skip the full snapshot load.
                return WorkloadExplanation(
                    **_explain_base(workload),
                    placeable=False,
                    message=f"workload is {workload.state.value}; only pending workloads are scheduled",
                )
            workloads = [
                candidate
                for candidate in await tx.list_workloads()
                if candidate.state is not WorkloadState.PENDING or candidate.preemption_id is None
            ]
            cells = await tx.list_cells()
            leases = await tx.list_leases()
            budgets = await tx.list_budgets()
            quotas = await tx.list_quotas()
            policies = await tx.list_policies()
            # The read that made grants real. They were issued, reported on and
            # expired, and never consulted by the thing that decides whether a
            # workload may run -- so a grant was recorded and licensed nothing.
            grants = await tx.list_budget_grants() if self.plane_id else []
            quota_grants = await tx.list_quota_grants() if self.plane_id else []
        result = self.scheduler.schedule(
            workloads,
            cells,
            leases=leases,
            budgets=budgets,
            quotas=quotas,
            policies=policies,
            grants=grants,
            quota_grants=quota_grants,
            plane_id=self.plane_id,
        )
        explanation = _explain_from_scheduling_result(workload, result)
        if explanation.placeable:
            return explanation
        provisioning = tuple(
            sorted(
                cell.id
                for cell in cells
                if cell.autoscale_request_workload_id == workload.id
                and cell.state in {CellState.REGISTERING, CellState.READY}
            )
        )
        if not provisioning:
            return explanation
        # The scheduler only sees cells that exist, so on its own it reports a
        # workload waiting for capacity exactly like one that can never run.
        return explanation.model_copy(
            update={
                "provisioning_cell_ids": provisioning,
                "message": (f"capacity is being provisioned for this workload: {', '.join(provisioning)}"),
            }
        )

    async def resolve_locality_preferences(
        self,
        spec: WorkloadDefinition,
        *,
        now: datetime | None = None,
    ) -> WorkloadDefinition:
        """Fill ``preferred_cells`` from where this spec's named assets recently ran.

        An explicit ``preferred_cells`` is never overwritten. A pin is a statement
        the submitter made about placement, and a cache guess has no standing to
        argue with it -- silently replacing it would make an explicit pin behave
        differently depending on unrelated fleet history.
        """

        if not spec.named_assets or spec.preferred_cells:
            return spec
        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            index = locality_index(
                await tx.list_workloads(),
                await tx.list_leases(),
                now=moment,
                cells=await tx.list_cells(),
            )
        preferred = preferred_cells_for(spec.named_assets, index)
        if not preferred:
            return spec
        return spec.model_copy(update={"preferred_cells": preferred})

    async def group_is_placeable(self, principal: Principal, *, member_ids: tp.Sequence[str]) -> bool:
        """Return whether every named member could be placed in one scheduling pass.

        Members are presented to the scheduler as schedulable, because the flag
        this answer decides is the very one the scheduler filters on -- asking
        without flipping it would report every gated member as unplaceable and the
        group would never open.

        One pass over all of them together, not one call each: the members compete
        for the same cells, and evaluating them separately would happily report
        that two members each fit a single free cell.
        """

        if self.scheduler is None:
            raise InputError("a scheduling service is not configured")
        wanted = set(member_ids)
        if not wanted:
            return False
        async with self.repository.transaction() as tx:
            candidates: list[WorkloadRecord] = []
            for workload in await tx.list_workloads():
                if workload.id in wanted:
                    self.authorizer.require(
                        principal, Permission.READ, org_id=workload.org_id, project_id=workload.project_id
                    )
                    candidates.append(workload.model_copy(update={"schedulable": True}))
                elif workload.state is not WorkloadState.PENDING or workload.preemption_id is None:
                    candidates.append(workload)
            cells = await tx.list_cells()
            leases = await tx.list_leases()
            budgets = await tx.list_budgets()
            quotas = await tx.list_quotas()
            policies = await tx.list_policies()
            # The read that made grants real. They were issued, reported on and
            # expired, and never consulted by the thing that decides whether a
            # workload may run -- so a grant was recorded and licensed nothing.
            grants = await tx.list_budget_grants() if self.plane_id else []
            quota_grants = await tx.list_quota_grants() if self.plane_id else []
        result = self.scheduler.schedule(
            candidates,
            cells,
            leases=leases,
            budgets=budgets,
            quotas=quotas,
            policies=policies,
            grants=grants,
            quota_grants=quota_grants,
            plane_id=self.plane_id,
        )
        placed = {placement.workload_id for placement in result.placements}
        # Subset, not intersection: a member that no longer exists cannot appear in
        # the placements, so this one comparison also answers "is a member missing"
        # without a separate existence check that could drift from it.
        return wanted <= placed

    async def submit_workload(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        spec: WorkloadDefinition,
        idempotency_key: str,
        parent_workload_id: str | None = None,
    ) -> tuple[WorkloadRecord, bool]:
        """Create a workload or replay the resource bound to the same request key.

        ``parent_workload_id`` places the new workload in a job tree. A child
        inherits its parent's scope and competes as an ordinary sibling; the
        relation exists for cancellation cascade and lineage, not scheduling.
        """

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        if isinstance(spec.execution, CallableExecution):
            self._require_trusted_callable_project(org_id, project_id)
            if spec.execution.signature is not None:
                raise InputError(REMOTE_CALLABLE_SIGNATURE_ERROR)
            if CALLABLE_SIGNING_KEY_ENV in spec.environment:
                raise InputError(REMOTE_CALLABLE_SIGNING_KEY_ERROR)
        key = _validate_idempotency_key(idempotency_key)
        scope = _scope(org_id, project_id)
        # Fingerprint the spec as submitted, before locality fills anything in. A
        # resolved preference depends on fleet history, so folding it into the
        # fingerprint would make the same request hash differently minute to minute
        # and turn every replay into a conflict.
        fingerprint = _fingerprint(
            {
                "org_id": org_id,
                "project_id": project_id,
                "spec": spec.model_dump(mode="json"),
                "parent_workload_id": parent_workload_id,
            }
        )
        spec = await self.resolve_locality_preferences(spec)
        async with self.repository.transaction() as tx:
            return await self._store_workload(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                spec=spec,
                scope=scope,
                operation="workload.submit",
                key=key,
                fingerprint=fingerprint,
                parent_workload_id=parent_workload_id,
            )

    async def submit_pool_workload(
        self,
        principal: Principal,
        *,
        fleet_name: str,
        node_type: str,
        org_id: str,
        project_id: str,
        command: str,
        name: str | None = None,
        environment: tp.Mapping[str, str] | None = None,
        secret_mounts: tp.Mapping[str, str] | None = None,
        metadata: tp.Mapping[str, str] | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        max_failure_retries: int = 0,
        max_preemption_retries: int = 10,
        workspace: WorkspaceRef | None = None,
        idempotency_key: str,
    ) -> tuple[WorkloadRecord, bool]:
        """Queue one command on a logical fleet node type, deriving all placement intent."""

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        key = _validate_idempotency_key(idempotency_key)
        scope = _scope(org_id, project_id)
        operation = "workload.submit_pool"
        fingerprint = _fingerprint(
            {
                "fleet_name": fleet_name,
                "node_type": node_type,
                "org_id": org_id,
                "project_id": project_id,
                "command": command,
                "name": name,
                "environment": dict(environment or {}),
                "secret_mounts": dict(secret_mounts or {}),
                "metadata": dict(metadata or {}),
                "max_retries": max_retries,
                "max_failure_retries": max_failure_retries,
                # Part of the fingerprint: the same command against a different
                # working tree is a different submission, and a replay that
                # ignored the tree would silently run yesterday's code.
                "workspace": workspace.sha256 if workspace is not None else None,
            }
        )
        async with self.repository.transaction() as tx:
            replayed = await self._replayed_workload(
                tx,
                scope=scope,
                operation=operation,
                key=key,
                fingerprint=fingerprint,
            )
            if replayed is not None:
                return replayed, True

            matching_pools = [pool for pool in await self._fleet_pools(tx, fleet_name) if pool.node_type == node_type]
            if not matching_pools:
                raise NotFoundError(f"fleet node type {fleet_name}/{node_type} was not found")
            if len(matching_pools) > 1:
                raise ConflictError(f"fleet node type {fleet_name}/{node_type} is ambiguous")
            pool = matching_pools[0]
            if not pool.enabled:
                raise ConflictError(f"fleet node type {fleet_name}/{node_type} is disabled")
            if pool.max_cells == 0:
                raise ConflictError(f"fleet node type {fleet_name}/{node_type} has max_workers set to zero")
            if pool.max_replica_resources is not None and pool.max_replica_resources != pool.allocatable:
                raise ConflictError(
                    f"fleet node type {fleet_name}/{node_type} partitions workers with max_replica_resources; "
                    "use the advanced workload submission API"
                )

            spec = self._pinned_pool_workload_spec(
                pool=pool,
                name=name if name is not None else f"{fleet_name}/{node_type}",
                execution=CommandExecution(command=command),
                environment=dict(environment or {}),
                secret_mounts=dict(secret_mounts or {}),
                metadata=dict(metadata or {}),
                max_retries=max_retries,
                max_failure_retries=max_failure_retries,
                max_preemption_retries=max_preemption_retries,
                cell_group_count=1,
                workspace=workspace,
            )
            return await self._store_workload(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                spec=spec,
                scope=scope,
                operation=operation,
                key=key,
                fingerprint=fingerprint,
            )

    async def submit_pool_callable(
        self,
        principal: Principal,
        *,
        fleet_name: str,
        org_id: str,
        project_id: str,
        accelerator_type: str,
        pods: int,
        execution: CallableExecution,
        capacity_class: CapacityClass = CapacityClass.SPOT,
        name: str | None = None,
        environment: tp.Mapping[str, str] | None = None,
        secret_mounts: tp.Mapping[str, str] | None = None,
        metadata: tp.Mapping[str, str] | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        max_failure_retries: int = 0,
        max_preemption_retries: int = 10,
        idempotency_key: str,
    ) -> tuple[WorkloadRecord, bool]:
        """Queue one callable on an atomic ``pods``-worker TPU cell."""

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        self._require_trusted_callable_project(org_id, project_id)
        if execution.signature is not None:
            raise InputError(REMOTE_CALLABLE_SIGNATURE_ERROR)
        if environment is not None and CALLABLE_SIGNING_KEY_ENV in environment:
            raise InputError(REMOTE_CALLABLE_SIGNING_KEY_ERROR)
        if isinstance(pods, bool) or not isinstance(pods, int) or pods < 1:
            raise InputError("pods must be a positive integer")
        if pods > MAX_GCP_TPU_MULTISLICE_WORKERS:
            raise InputError(f"pods must not exceed the GCP TPU multislice limit of {MAX_GCP_TPU_MULTISLICE_WORKERS}")
        if not accelerator_type or accelerator_type != accelerator_type.strip():
            raise InputError("accelerator_type must be non-empty and trimmed")

        key = _validate_idempotency_key(idempotency_key)
        scope = _scope(org_id, project_id)
        operation = "workload.submit_pool_callable"
        fingerprint = _fingerprint(
            {
                "fleet_name": fleet_name,
                "org_id": org_id,
                "project_id": project_id,
                "accelerator_type": accelerator_type,
                "pods": pods,
                "capacity_class": capacity_class.value,
                "execution": execution.model_dump(mode="json"),
                "name": name,
                "environment": dict(environment or {}),
                "secret_mounts": dict(secret_mounts or {}),
                "metadata": dict(metadata or {}),
                "max_retries": max_retries,
                "max_failure_retries": max_failure_retries,
            }
        )
        async with self.repository.transaction() as tx:
            replayed = await self._replayed_workload(
                tx,
                scope=scope,
                operation=operation,
                key=key,
                fingerprint=fingerprint,
            )
            if replayed is not None:
                return replayed, True

            fleet_pools = await self._fleet_pools(tx, fleet_name)
            exact_matches = [pool for pool in fleet_pools if pool.enabled and pool.accelerator_type == accelerator_type]
            if not exact_matches:
                raise NoMatchingCapacityPoolError(
                    fleet=fleet_name,
                    requested_accelerator_type=accelerator_type,
                    available_enabled_accelerator_types=self._callable_accelerator_types(fleet_pools, pods=pods),
                )
            tpu_matches = [
                pool
                for pool in exact_matches
                if self._is_complete_tpu_pool(pool) and pool.capacity_class is capacity_class
            ]
            if not tpu_matches:
                available_capacity_classes = sorted(
                    {pool.capacity_class.value for pool in exact_matches if self._is_complete_tpu_pool(pool)}
                )
                if available_capacity_classes:
                    available = ", ".join(available_capacity_classes)
                    raise ConflictError(
                        f"enabled TPU pool for {accelerator_type!r} in fleet {fleet_name!r} does not offer "
                        f"{capacity_class.value!r} capacity; available: {available}"
                    )
                raise ConflictError(
                    f"enabled capacity pool for {accelerator_type!r} in fleet {fleet_name!r} is not a TPU pool"
                )
            if len(tpu_matches) > 1:
                raise ConflictError(
                    f"TPU accelerator type {accelerator_type!r} is ambiguous in fleet {fleet_name!r}; "
                    "configure exactly one enabled pool"
                )
            pool = tpu_matches[0]
            if pool.max_cells == 0:
                raise ConflictError(
                    f"TPU accelerator type {accelerator_type!r} in fleet {fleet_name!r} has max_workers set to zero"
                )
            if pods > 1 and pool.executor != "eyvan-native":
                raise ConflictError(
                    "multi-worker TPU callables require an eyvan-native pool so Eyvan can fence the physical cell"
                )
            if pool.max_replica_resources is not None and pool.max_replica_resources != pool.allocatable:
                raise ConflictError(
                    f"TPU accelerator type {accelerator_type!r} in fleet {fleet_name!r} partitions workers with "
                    "max_replica_resources; callable execution requires one complete slice"
                )
            if pods > pool.max_cells:
                raise ConflictError(
                    f"TPU pool {pool.id!r} is capped at {pool.max_cells} workers, but the callable requested pods={pods}"
                )

            spec = self._pinned_pool_workload_spec(
                pool=pool,
                name=name if name is not None else f"{fleet_name}/{accelerator_type}",
                execution=execution,
                environment=dict(environment or {}),
                secret_mounts=dict(secret_mounts or {}),
                metadata=dict(metadata or {}),
                max_retries=max_retries,
                max_failure_retries=max_failure_retries,
                max_preemption_retries=max_preemption_retries,
                cell_group_count=1,
                worker_count=pods,
            )
            return await self._store_workload(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                spec=spec,
                scope=scope,
                operation=operation,
                key=key,
                fingerprint=fingerprint,
            )

    def _require_trusted_callable_project(self, org_id: str, project_id: str) -> None:
        self.callable_trust.require(org_id, project_id)

    def authorize_callable_artifact(self, principal: Principal, *, org_id: str, project_id: str) -> None:
        """Authorize managed callable storage for one trusted project."""

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        self._require_trusted_callable_project(org_id, project_id)

    @staticmethod
    def result_spec_for_generation(
        spec: WorkloadDefinition,
        *,
        workload_id: str,
        generation: int,
    ) -> WorkloadDefinition:
        """Assign a server-owned destination to one managed callable attempt."""

        execution = spec.execution
        if not isinstance(execution, CallableExecution) or not execution.result_uri:
            return spec
        if not execution.result_uri.startswith("eyvan://"):
            return spec
        try:
            reference = parse_managed_uri(execution.result_uri)
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        if reference.kind is not ManagedArtifactKind.RESULTS:
            raise InputError("callable result_uri must reference an eyvan://results object")
        owned_uri = managed_result_uri(f"{workload_id}-g{generation}")
        return spec.model_copy(update={"execution": execution.model_copy(update={"result_uri": owned_uri})})

    @staticmethod
    def _is_complete_tpu_pool(pool: CapacityPoolRecord) -> bool:
        kind = pool.labels.get("eyvan_resource_kind")
        declared_tpu = kind == "tpu" if kind is not None else pool.capacity_backend == "gcp-tpu"
        return declared_tpu and pool.allocatable.tpu > 0 and pool.allocatable.gpu == 0

    @classmethod
    def _callable_accelerator_types(
        cls,
        pools: tp.Iterable[CapacityPoolRecord],
        *,
        pods: int,
    ) -> tuple[str, ...]:
        """Return unambiguous TPU types that can accept this callable shape."""

        complete_pool_counts: dict[str, int] = {}
        compatible: set[str] = set()
        for pool in pools:
            accelerator_type = pool.accelerator_type
            if not pool.enabled or accelerator_type is None or not cls._is_complete_tpu_pool(pool):
                continue
            complete_pool_counts[accelerator_type] = complete_pool_counts.get(accelerator_type, 0) + 1
            if (
                pool.max_cells < pods
                or (pods > 1 and pool.executor != "eyvan-native")
                or (pool.max_replica_resources is not None and pool.max_replica_resources != pool.allocatable)
            ):
                continue
            compatible.add(accelerator_type)
        return tuple(accelerator_type for accelerator_type in compatible if complete_pool_counts[accelerator_type] == 1)

    @staticmethod
    async def _fleet_pools(tx: RepositorySession, fleet_name: str) -> list[CapacityPoolRecord]:
        """Load one fleet's pools inside the caller's existing transaction."""

        return await tx.list_capacity_pools(fleet_name=fleet_name)

    @staticmethod
    def _pinned_pool_workload_spec(
        *,
        pool: CapacityPoolRecord,
        name: str,
        execution: CommandExecution | CallableExecution,
        environment: tp.Mapping[str, str],
        secret_mounts: tp.Mapping[str, str],
        metadata: tp.Mapping[str, str],
        max_retries: int,
        max_failure_retries: int,
        cell_group_count: int,
        max_preemption_retries: int = 10,
        worker_count: int = 1,
        workspace: WorkspaceRef | None = None,
    ) -> WorkloadDefinition:
        """Build a workload whose placement contract is pinned to one fleet pool."""

        pool_labels = {
            "eyvan_pool": pool.id,
            "eyvan_pool_template": pool.template_fingerprint,
        }
        required_labels = {**pool_labels, WORKER_COUNT_LABEL: str(worker_count)}
        return WorkloadDefinition(
            name=name,
            execution=execution,
            workspace=workspace,
            gang=ResourceGang(
                replicas=1,
                per_replica=pool.allocatable,
                required_topology=pool.topology,
            ),
            environment=dict(environment),
            secret_mounts=dict(secret_mounts),
            metadata={**dict(metadata), **pool_labels},
            provider=pool.provider,
            region=pool.region,
            accelerator_type=pool.accelerator_type,
            capacity_class=pool.capacity_class,
            required_executor=pool.executor,
            required_labels=required_labels,
            max_retries=max_retries,
            max_failure_retries=max_failure_retries,
            max_preemption_retries=max_preemption_retries,
            cell_group_count=cell_group_count,
            worker_count=worker_count,
            worker_resource_scope=WorkerResourceScope.PER_WORKER,
        )

    async def _replayed_workload(
        self,
        tx: RepositorySession,
        *,
        scope: str,
        operation: str,
        key: str,
        fingerprint: str,
    ) -> WorkloadRecord | None:
        """Return a replayed workload before any pool or policy lookup occurs."""

        existing = await self._idempotent_resource(tx, scope, operation, key, fingerprint)
        if existing is None:
            return None
        workload = await tx.get_workload(existing.resource_id)
        if workload is None:
            raise ConflictError("idempotency record points to a missing workload")
        return workload

    async def _store_workload(
        self,
        tx: RepositorySession,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        spec: WorkloadDefinition,
        scope: str,
        operation: str,
        key: str,
        fingerprint: str,
        parent_workload_id: str | None = None,
        schedulable: bool = True,
        coordination_kind: CoordinationKind | None = None,
        depends_on: tuple[str, ...] = (),
        created_by: str | None = None,
    ) -> tuple[WorkloadRecord, bool]:
        replayed = await self._replayed_workload(
            tx,
            scope=scope,
            operation=operation,
            key=key,
            fingerprint=fingerprint,
        )
        if replayed is not None:
            return replayed, True
        await self._require_registered_scope(tx, org_id, project_id)
        policy = await tx.get_policy(org_id, project_id)
        if policy is not None:
            self._validate_workload_policy(spec, policy)
        self._validate_secret_mounts(spec, policy)
        if parent_workload_id is not None:
            parent = await tx.get_workload(parent_workload_id)
            if parent is None:
                raise NotFoundError(f"parent workload {parent_workload_id!r} was not found")
            validate_parent((org_id, project_id), parent)
            if parent.state in TERMINAL_WORKLOAD_STATES or parent.state is WorkloadState.CANCELLING:
                # CANCELLING counts: the cascade that is already in flight has
                # committed its transaction, so a child admitted now would never
                # be cancelled by it -- a permanent orphan holding capacity.
                raise ConflictError(
                    f"parent workload {parent_workload_id!r} is {parent.state.value}; it cannot gain new children"
                )
        workload_id = _new_id("wl")
        spec = self.result_spec_for_generation(spec, workload_id=workload_id, generation=1)
        workload = WorkloadRecord(
            id=workload_id,
            org_id=org_id,
            project_id=project_id,
            spec=spec,
            # ``principal`` is the *actor* -- who made this call, truthfully
            # ``workload:<id>`` for an in-job submission -- while ``created_by``
            # is the *identity* the work belongs to. They are the same for a
            # human submit and deliberately different for a child.
            #
            # A synthetic ``created_by`` would create a fresh per-principal
            # fair-share leaf and a fresh QuotaRecord scope, so a launcher could
            # escape the quota its owner is subject to simply by fanning out.
            created_by=created_by or principal.subject,
            parent_workload_id=parent_workload_id,
            schedulable=schedulable,
            coordination_kind=coordination_kind,
            depends_on=depends_on,
        )
        await self._put_workload_keeping_references(tx, workload)
        await self._bind_idempotency(tx, scope, operation, key, fingerprint, workload)
        await self._audit(tx, principal, workload, "workload.submitted", new_state=workload.state.value)
        return workload, False

    @staticmethod
    def referenced_artifact_uris(spec: WorkloadDefinition) -> tuple[str, ...]:
        """Return every managed artifact URI one spec claims.

        The result URI is included even though nothing has written it yet: the
        workload owns that address for its whole life, and a collector that only
        counted *existing* objects would treat an allocated-but-unwritten result
        as free to reuse.
        """

        execution = spec.execution
        if not isinstance(execution, CallableExecution):
            return ()
        candidates = (execution.artifact_uri, execution.workspace_uri, execution.result_uri)
        return tuple(dict.fromkeys(uri for uri in candidates if uri and is_managed_uri(uri)))

    async def _put_workload_keeping_references(self, tx: RepositorySession, workload: WorkloadRecord) -> None:
        """Persist a workload whose spec may name artifacts, and claim them.

        Used by every path that bumps the generation: ``result_spec_for_generation``
        mints a *new* result URI per attempt, so a retried or resumed workload owns
        an address the submit-time reference set never saw. Recording here is safe
        to repeat because reference ids are derived from (uri, workload).
        """

        await tx.put_workload(workload)
        await self._record_artifact_references(tx, workload)

    async def _record_artifact_references(self, tx: RepositorySession, workload: WorkloadRecord) -> None:
        """Claim every artifact this workload names, in its own transaction.

        Written beside the workload rather than from a background sweep because a
        reference that lands late is a window in which the artifact looks
        unreferenced -- and the whole point of this index is that a collector can
        eventually trust it.
        """

        for uri in self.referenced_artifact_uris(workload.spec):
            await tx.put_artifact_reference(
                ArtifactReferenceRecord(
                    id=artifact_reference_id(workload.org_id, workload.project_id, uri, workload.id),
                    uri=uri,
                    workload_id=workload.id,
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                )
            )

    async def _release_artifact_references(
        self,
        tx: RepositorySession,
        workload: WorkloadRecord,
        *,
        now: datetime,
    ) -> None:
        """Mark this workload's claims released once it can no longer run.

        The rows stay: who referenced what is exactly the history that makes a
        later deletion auditable, and a released reference is what proves an
        artifact was *once* in use rather than never indexed at all.
        """

        for reference in await tx.list_artifact_references(
            org_id=workload.org_id,
            project_id=workload.project_id,
            workload_id=workload.id,
            active_only=True,
        ):
            await tx.put_artifact_reference(reference.model_copy(update={"released_at": now}))

    @staticmethod
    def _validate_workload_policy(spec: WorkloadDefinition, policy: PolicyRecord) -> None:
        if spec.intra_band_priority > policy.max_priority:
            raise InputError(f"intra-band priority exceeds project maximum {policy.max_priority}")
        if spec.provider and policy.allowed_providers and spec.provider not in policy.allowed_providers:
            raise InputError(f"provider {spec.provider!r} is not allowed by project policy")
        if spec.region and policy.allowed_regions and spec.region not in policy.allowed_regions:
            raise InputError(f"region {spec.region!r} is not allowed by project policy")
        if policy.allowed_capacity_classes and spec.capacity_class.value not in policy.allowed_capacity_classes:
            raise InputError(f"capacity class {spec.capacity_class.value!r} is not allowed by project policy")
        if spec.accelerator_type and spec.accelerator_type in policy.denied_accelerator_types:
            raise InputError(f"accelerator type {spec.accelerator_type!r} is denied by project policy")

    async def list_workloads(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        state: WorkloadState | None = None,
    ) -> list[WorkloadRecord]:
        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            return await tx.list_workloads(org_id=org_id, project_id=project_id, state=state)

    async def get_workload(self, principal: Principal, workload_id: str) -> WorkloadRecord:
        async with self.repository.transaction() as tx:
            workload = await self._require_workload(tx, workload_id)
            self.authorizer.require(principal, Permission.READ, org_id=workload.org_id, project_id=workload.project_id)
            return workload

    async def submit_batch(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        template: WorkloadDefinition,
        count: int,
        idempotency_key: str,
        name_prefix: str = "",
        max_fan_out: int = DEFAULT_MAX_FAN_OUT,
    ) -> tuple[WorkloadRecord, list[WorkloadRecord]]:
        """Create one parent workload plus ``count`` ordinary child workloads.

        Every task is a normal durable workload -- same admission, scheduling,
        leasing, settlement, and audit -- so batch adds a shape, not a second
        execution runtime with its own retry and preemption semantics.

        The whole expansion commits in one transaction: a partially-created
        array would leave orphan tasks holding capacity with no parent to cancel
        them. Child keys are derived from the parent's key, so a retried request
        replays the same array rather than creating a second one.

        The parent is a coordinator, not a task. It is stored with
        ``schedulable=False`` so the scheduler never places it -- otherwise a
        batch of N would occupy N+1 slices and the extra one would run the
        template command with no task index. Its state is derived from its
        children by the controller.
        """

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        request = BatchRequest(template=template, count=count, name_prefix=name_prefix)
        tasks = expand(request, max_fan_out=max_fan_out)
        key = _validate_idempotency_key(idempotency_key)
        # Derived child keys must themselves be valid. Validating the longest
        # one up front turns a mid-transaction failure on task 100 into a clear
        # rejection of the request.
        _validate_idempotency_key(child_idempotency_key(key, max(count - 1, 0)))
        scope = _scope(org_id, project_id)
        async with self.repository.transaction() as tx:
            parent, replayed = await self._store_workload(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                spec=template.model_copy(update={"name": name_prefix or template.name}),
                scope=scope,
                operation="workload.batch",
                key=key,
                fingerprint=_fingerprint(
                    {
                        "org_id": org_id,
                        "project_id": project_id,
                        "template": template.model_dump(mode="json"),
                        "count": count,
                        # Both change what the array *is*. Omitting them let a
                        # replay with a different prefix or a lowered fan-out
                        # limit silently return the original array instead of
                        # being rejected as a conflicting reuse of the key.
                        "name_prefix": name_prefix,
                        "max_fan_out": max_fan_out,
                    }
                ),
                schedulable=False,
                coordination_kind=CoordinationKind.BATCH,
            )
            if replayed:
                # Ordered by task index, not by the children's random ids:
                # callers index into this list, so children[3] must be task 3
                # on a replay exactly as it was on the original submission.
                # Filtered to batch tasks: the parent may also have acquired
                # ordinary sub-jobs, which are children but not array members.
                stored = await tx.list_child_workloads(parent.id)
                members = [child for child in stored if task_index(child.spec.environment) >= 0]
                return parent, sorted(members, key=lambda child: task_index(child.spec.environment))
            children: list[WorkloadRecord] = []
            for index, task in enumerate(tasks):
                child_key = child_idempotency_key(key, index)
                child, _ = await self._store_workload(
                    tx,
                    principal,
                    org_id=org_id,
                    project_id=project_id,
                    spec=task,
                    scope=scope,
                    operation="workload.batch_task",
                    key=child_key,
                    fingerprint=_fingerprint({"parent": parent.id, "index": index}),
                    parent_workload_id=parent.id,
                )
                children.append(child)
            return parent, children

    async def submit_dag(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        nodes: tp.Sequence[DagNode],
        idempotency_key: str,
        name_prefix: str = "",
        max_fan_out: int = DEFAULT_MAX_FAN_OUT,
    ) -> tuple[WorkloadRecord, list[WorkloadRecord]]:
        """Create one coordinator plus a node per stage, gated by dependencies.

        Nodes are created in topological order, so a node's predecessors always
        already exist and their durable ids can be resolved without a second pass.

        A node with predecessors is stored ``schedulable=False``. That is the entire
        gate: ``CoreSchedulingService.schedule`` already filters on the flag, so a
        waiting node is invisible to placement without a second admission concept.
        The controller flips it when every predecessor has succeeded.
        """

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        order = dag_validate(nodes, max_fan_out=max_fan_out)
        key = _validate_idempotency_key(idempotency_key)
        _validate_idempotency_key(child_idempotency_key(key, max(len(nodes) - 1, 0)))
        by_name = {node.name: node for node in nodes}
        scope = _scope(org_id, project_id)

        async with self.repository.transaction() as tx:
            parent, replayed = await self._store_workload(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                spec=by_name[order[0]].template.model_copy(update={"name": name_prefix or "dag"}),
                scope=scope,
                operation="workload.dag",
                key=key,
                fingerprint=_fingerprint(
                    {
                        "org_id": org_id,
                        "project_id": project_id,
                        "name_prefix": name_prefix,
                        "max_fan_out": max_fan_out,
                        "nodes": [
                            {
                                "name": node.name,
                                "depends_on": sorted(node.depends_on),
                                "template": node.template.model_dump(mode="json"),
                            }
                            for node in sorted(nodes, key=lambda candidate: candidate.name)
                        ],
                    }
                ),
                schedulable=False,
                coordination_kind=CoordinationKind.DAG,
            )
            if replayed:
                stored = await tx.list_child_workloads(parent.id)
                return parent, sorted(stored, key=lambda child: child.spec.name)

            ids_by_name: dict[str, str] = {}
            children: list[WorkloadRecord] = []
            for index, name in enumerate(order):
                node = by_name[name]
                predecessors = tuple(ids_by_name[parent_name] for parent_name in node.depends_on)
                child, _ = await self._store_workload(
                    tx,
                    principal,
                    org_id=org_id,
                    project_id=project_id,
                    spec=node.template.model_copy(update={"name": f"{name_prefix or 'dag'}-{name}"}),
                    scope=scope,
                    operation="workload.dag_node",
                    key=child_idempotency_key(key, index),
                    fingerprint=_fingerprint({"parent": parent.id, "node": name}),
                    parent_workload_id=parent.id,
                    schedulable=not predecessors,
                    depends_on=predecessors,
                )
                ids_by_name[name] = child.id
                children.append(child)
            return parent, children

    async def submit_group(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        roles: tp.Sequence[GroupRole],
        idempotency_key: str,
        name_prefix: str = "",
        max_members: int = MAX_GROUP_MEMBERS,
    ) -> tuple[WorkloadRecord, list[WorkloadRecord]]:
        """Create one coordinator plus a member per role replica, all gated together.

        Every member is stored ``schedulable=False``. That is the whole gate --
        ``CoreSchedulingService.schedule`` already filters on the flag, so a waiting
        member is invisible to placement without inventing a second admission
        concept. The controller releases all of them in one transaction once every
        member is independently placeable in the same evaluation pass.
        """

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        members = group_validate(roles, max_members=max_members)
        key = _validate_idempotency_key(idempotency_key)
        _validate_idempotency_key(child_idempotency_key(key, max(members - 1, 0)))
        scope = _scope(org_id, project_id)
        names = group_member_names(roles, prefix=name_prefix)
        templates = [role.template for role in roles for _ in range(role.count)]

        async with self.repository.transaction() as tx:
            parent, replayed = await self._store_workload(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                spec=templates[0].model_copy(update={"name": name_prefix or "group"}),
                scope=scope,
                operation="workload.group",
                key=key,
                fingerprint=_fingerprint(
                    {
                        "org_id": org_id,
                        "project_id": project_id,
                        "name_prefix": name_prefix,
                        "max_members": max_members,
                        "roles": [
                            {
                                "name": role.name,
                                "count": role.count,
                                "template": role.template.model_dump(mode="json"),
                            }
                            for role in sorted(roles, key=lambda candidate: candidate.name)
                        ],
                    }
                ),
                schedulable=False,
                coordination_kind=CoordinationKind.GROUP,
            )
            if replayed:
                stored = await tx.list_child_workloads(parent.id)
                return parent, sorted(stored, key=lambda child: child.spec.name)

            children: list[WorkloadRecord] = []
            for index, (name, template) in enumerate(zip(names, templates, strict=True)):
                child, _ = await self._store_workload(
                    tx,
                    principal,
                    org_id=org_id,
                    project_id=project_id,
                    spec=template.model_copy(update={"name": name}),
                    scope=scope,
                    operation="workload.group_member",
                    key=child_idempotency_key(key, index),
                    fingerprint=_fingerprint({"parent": parent.id, "member": name}),
                    parent_workload_id=parent.id,
                    schedulable=False,
                )
                children.append(child)
            return parent, children

    async def list_child_workloads(self, principal: Principal, workload_id: str) -> list[WorkloadRecord]:
        """Return the direct children of one workload, batch tasks in task order.

        Backends return children in whatever order their index yields, which for
        both the in-memory and SQL stores is effectively random UUID order. That
        is fine for a job tree but wrong for an array, where the public
        ``GET /workloads/{id}/children`` response is indexed by task. Ordering
        here rather than per backend keeps the two from drifting.
        """

        async with self.repository.transaction() as tx:
            workload = await self._require_workload(tx, workload_id)
            self.authorizer.require(principal, Permission.READ, org_id=workload.org_id, project_id=workload.project_id)
            children = await tx.list_child_workloads(workload_id)
        # Batch tasks first in index order; non-array sub-jobs keep a stable
        # id ordering after them (task_index returns -1 for those).
        return sorted(children, key=lambda child: (task_index(child.spec.environment), child.id))

    async def cancel_workload(
        self, principal: Principal, workload_id: str, *, idempotency_key: str, cascade: bool = True
    ) -> tuple[WorkloadRecord, bool]:
        """Request cancellation; pending work is cancelled synchronously.

        Cancelling a parent cancels its whole subtree by default: a launcher's
        children have no independent meaning once the launcher is gone, and
        leaving them running would silently keep spending the project's budget.
        Pass ``cascade=False`` to cancel exactly one workload.
        """

        key = _validate_idempotency_key(idempotency_key)
        async with self.repository.transaction() as tx:
            workload = await self._require_workload(tx, workload_id)
            self.authorizer.require(
                principal, Permission.OPERATE, org_id=workload.org_id, project_id=workload.project_id
            )
            scope = _scope(workload.org_id, workload.project_id)
            fingerprint = _fingerprint({"workload_id": workload_id})
            existing = await self._idempotent_resource(tx, scope, "workload.cancel", key, fingerprint)
            if existing is not None:
                return workload, True
            if workload.state in {WorkloadState.SUCCEEDED, WorkloadState.FAILED}:
                raise ConflictError(f"cannot cancel a {workload.state.value} workload")
            if await self._preemption_execution_started(tx, workload):
                raise ConflictError("preemption is already executing; retry cancellation after it completes")
            updated = await self._cancel_one(tx, principal, workload, "workload.cancel_requested")
            if cascade:
                # Descendants are cancelled through the same path as the root, so
                # a RUNNING child receives a `workload.cancel` command and is
                # actually stopped. Transitioning its state alone would leave it
                # holding its accelerator and spending the project's budget while
                # merely *recorded* as cancelling.
                for child in await collect_descendants(workload_id, tx.list_child_workloads):
                    if child.state in TERMINAL_WORKLOAD_STATES:
                        continue
                    await self._cancel_one(tx, principal, child, "workload.cancel_cascaded")
            await self._bind_idempotency(tx, scope, "workload.cancel", key, fingerprint, updated)
            return updated, False

    async def _cancel_one(
        self,
        tx: RepositorySession,
        principal: Principal,
        workload: WorkloadRecord,
        action: str,
    ) -> WorkloadRecord:
        """Transition one workload toward cancellation and enqueue its stop command.

        A PENDING workload has nothing running, so it cancels synchronously.
        Anything further along needs a durable ``workload.cancel`` command: that
        outbox record is the only thing the controller's dispatcher reacts to,
        so omitting it leaves the remote process alive.
        """

        target = WorkloadState.CANCELLED if workload.state is WorkloadState.PENDING else WorkloadState.CANCELLING
        updated = await self._transition_workload(
            tx,
            principal,
            workload,
            target,
            action,
            updates={"preemption_id": None},
        )
        if target is WorkloadState.CANCELLING:
            for lease in await self._current_leases(tx, updated):
                if lease.revocation_reason == "preemption":
                    await tx.put_lease(lease.model_copy(update={"revocation_reason": None, "updated_at": utcnow()}))
            await self._outbox(
                tx,
                topic="workload.cancel",
                aggregate_type="workload",
                aggregate_id=updated.id,
                idempotency_key=f"cancel:{updated.id}:{updated.generation}",
                generation=updated.generation,
                payload={
                    "workload_id": updated.id,
                    "lease_id": updated.lease_id,
                    "lease_ids": list(updated.lease_ids),
                    "remote_id": updated.remote_id,
                },
            )
        return updated

    async def checkpoint_workload(
        self, principal: Principal, workload_id: str, *, idempotency_key: str
    ) -> tuple[WorkloadRecord, bool]:
        """Request an execution-cell checkpoint for a running workload."""

        key = _validate_idempotency_key(idempotency_key)
        async with self.repository.transaction() as tx:
            workload = await self._require_workload(tx, workload_id)
            self.authorizer.require(
                principal, Permission.OPERATE, org_id=workload.org_id, project_id=workload.project_id
            )
            scope = _scope(workload.org_id, workload.project_id)
            fingerprint = _fingerprint({"workload_id": workload_id})
            existing = await self._idempotent_resource(tx, scope, "workload.checkpoint", key, fingerprint)
            if existing is not None:
                return workload, True
            if not workload.spec.checkpointable:
                raise InputError("workload is not checkpointable")
            if workload.spec.cell_group_count > 1:
                # A multi-*cell* group spans independently fenced cells, so a hook
                # would run under separate mutation contexts with no shared
                # cluster. A multi-*host* slice is different and now supported:
                # NativeSliceExecutor fans the hook across ranks that inherit
                # their own JAX identity. Only the cross-cell case stays rejected.
                raise InputError("checkpoint hooks are not supported for multi-cell TPU pod groups")
            updated = await self._transition_workload(
                tx, principal, workload, WorkloadState.CHECKPOINTING, "workload.checkpoint_requested"
            )
            await self._outbox(
                tx,
                topic="workload.checkpoint",
                aggregate_type="workload",
                aggregate_id=updated.id,
                idempotency_key=(
                    f"checkpoint:{updated.id}:{updated.generation}:{hashlib.sha256(key.encode()).hexdigest()[:16]}"
                ),
                generation=updated.generation,
                payload={
                    "workload_id": updated.id,
                    "lease_id": updated.lease_id,
                    "remote_id": updated.remote_id,
                    "hook": updated.spec.checkpoint_hook,
                    "output_uri": updated.spec.checkpoint_output_uri,
                },
            )
            await self._bind_idempotency(tx, scope, "workload.checkpoint", key, fingerprint, updated)
            return updated, False

    @staticmethod
    def _manual_retry_budget_update(workload: WorkloadRecord) -> dict[str, int]:
        """Consume one retry from the budget matching the workload's failure class.

        A manual retry of a ``FAILED`` workload draws from the failure budget
        first; a ``CANCELLED`` workload draws from the capacity-loss budget
        first. When the matching budget is exhausted the other budget may
        cover the retry, and when both are exhausted the retry is rejected.
        """

        budgets: tuple[tuple[str, int, int], ...]
        if workload.state is WorkloadState.FAILED:
            budgets = (
                ("failure_count", workload.failure_count, workload.spec.max_failure_retries),
                ("capacity_loss_count", workload.capacity_loss_count, workload.spec.max_retries),
            )
        else:
            budgets = (
                ("capacity_loss_count", workload.capacity_loss_count, workload.spec.max_retries),
                ("failure_count", workload.failure_count, workload.spec.max_failure_retries),
            )
        for field_name, count, limit in budgets:
            if count < limit:
                return {field_name: count + 1}
        raise ConflictError("workload retry limit has been exhausted")

    async def retry_workload(
        self, principal: Principal, workload_id: str, *, idempotency_key: str
    ) -> tuple[WorkloadRecord, bool]:
        """Return a failed or cancelled workload to pending with a new generation."""

        key = _validate_idempotency_key(idempotency_key)
        async with self.repository.transaction() as tx:
            workload = await self._require_workload(tx, workload_id)
            self.authorizer.require(
                principal, Permission.OPERATE, org_id=workload.org_id, project_id=workload.project_id
            )
            scope = _scope(workload.org_id, workload.project_id)
            fingerprint = _fingerprint({"workload_id": workload_id})
            existing = await self._idempotent_resource(tx, scope, "workload.retry", key, fingerprint)
            if existing is not None:
                return workload, True
            if workload.state not in {WorkloadState.FAILED, WorkloadState.CANCELLED}:
                raise ConflictError("only failed or cancelled workloads can be retried")
            budget_update = self._manual_retry_budget_update(workload)
            ensure_workload_transition(workload.state, WorkloadState.PENDING)
            now = utcnow()
            next_generation = workload.generation + 1
            updated = workload.model_copy(
                update={
                    "state": WorkloadState.PENDING,
                    "attempt": workload.attempt + 1,
                    **budget_update,
                    "generation": next_generation,
                    "spec": self.result_spec_for_generation(
                        workload.spec,
                        workload_id=workload.id,
                        generation=next_generation,
                    ),
                    "lease_id": None,
                    "lease_ids": (),
                    "dispatch_reservation": None,
                    "remote_id": None,
                    "failure_reason": None,
                    "autoscale_block_reason": None,
                    "queue_admission_denial": None,
                    "scheduling_reason": None,
                    "updated_at": now,
                }
            )
            await self._put_workload_keeping_references(tx, updated)
            await self._audit(
                tx,
                principal,
                updated,
                "workload.retried",
                previous_state=workload.state.value,
                new_state=updated.state.value,
                payload={"attempt": updated.attempt, "generation": updated.generation},
            )
            await self._bind_idempotency(tx, scope, "workload.retry", key, fingerprint, updated)
            return updated, False

    async def force_finalize_stuck_workload(
        self,
        principal: Principal,
        workload_id: str,
        *,
        reason: str,
        idempotency_key: str,
        now: datetime | None = None,
    ) -> tuple[WorkloadRecord, bool]:
        """Settle a workload whose stop the worker will never confirm.

        The escape from a deadlock observed on hardware: a native worker answered
        500 to every ``/stop``, the agent retried indefinitely, and the workload
        stayed in CANCELLING. Its lease therefore stayed active, so the cell
        could not be deleted -- ``cell-delete`` refuses while leases are held --
        so it never left DRAINING and ``eyvan down`` never completed. Nothing in
        the CLI resolved it; ``force_finalize_preemption`` attests *preemption*
        victims and does not apply to an ordinary cancel.

        **This declares a process dead that nobody confirmed stopping.** That is
        why the cell is drained rather than returned to service: the workload may
        still be running on it, and scheduling something else onto a host in that
        state would be worse than the deadlock. The capacity is given up, not
        recycled.

        Refused unless the stop has genuinely stopped making progress -- a live
        cancel in flight is not this, and a break-glass that also works on
        healthy work is one that gets used on healthy work.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        reason = reason.strip()
        if not reason:
            raise InputError("force-finalize reason must not be empty")
        moment = now or utcnow()
        fingerprint = _fingerprint({"workload_id": workload_id, "reason": reason})
        async with self.repository.transaction() as tx:
            existing = await self._idempotent_resource(tx, "fleet", "workload.force_finalize", key, fingerprint)
            workload = await self._require_workload(tx, workload_id)
            if existing is not None:
                return workload, True
            if workload.state in TERMINAL_WORKLOAD_STATES:
                raise ConflictError(f"workload {workload_id!r} is already {workload.state.value}")
            if workload.state is not WorkloadState.CANCELLING:
                raise ConflictError(
                    f"workload {workload_id!r} is {workload.state.value}; only a stuck cancellation is "
                    "force-finalized, because this gives up the cell rather than returning it"
                )
            stuck = [
                command
                for command in await tx.list_outbox_for_aggregates(
                    "workload", {workload_id: workload.generation}, topic="workload.cancel"
                )
                if command.state in {OutboxState.FAILED, OutboxState.PENDING, OutboxState.PROCESSING}
                and command.created_at <= moment - timedelta(seconds=STUCK_CANCEL_AFTER_S)
            ]
            if not stuck:
                raise ConflictError(
                    f"workload {workload_id!r} has no cancel older than {STUCK_CANCEL_AFTER_S:g}s; "
                    "a cancellation still in flight is not stuck"
                )
            for command in stuck:
                await tx.put_outbox(
                    command.model_copy(
                        update={
                            "state": OutboxState.DISCARDED,
                            "claimed_by": None,
                            "claimed_until": None,
                            "last_error": f"force-finalized: {reason}",
                            "updated_at": moment,
                        }
                    )
                )
            drained: set[str] = set()
            for lease in await tx.list_leases(workload_id=workload_id):
                if lease.state not in ACTIVE_LEASE_STATES:
                    continue
                await self._settle_lease(tx, principal, workload, lease, reason="force-finalized")
                drained.add(lease.cell_id)
            for cell_id in sorted(drained):
                cell = await tx.get_cell(cell_id)
                if cell is None or cell.state in {CellState.DELETED, CellState.DELETING}:
                    continue
                await tx.put_cell(cell.model_copy(update={"state": CellState.DRAINING, "updated_at": moment}))
            settled = workload.model_copy(
                update={
                    "state": WorkloadState.CANCELLED,
                    "dispatch_reservation": None,
                    "failure_reason": f"force-finalized after an unconfirmed stop: {reason}",
                    "updated_at": moment,
                }
            )
            await tx.put_workload(settled)
            await self._bind_idempotency(tx, "fleet", "workload.force_finalize", key, fingerprint, settled)
            await self._audit_scoped(
                tx,
                principal,
                org_id=workload.org_id,
                project_id=workload.project_id,
                entity_type="workload",
                entity_id=workload_id,
                action="workload.force_finalized",
                payload={
                    "reason": reason,
                    "discarded_commands": sorted(command.id for command in stuck),
                    "drained_cells": sorted(drained),
                },
            )
            return settled, False

    async def force_finalize_preemption(
        self,
        principal: Principal,
        command_id: str,
        *,
        reason: str,
        idempotency_key: str,
    ) -> tuple[WorkloadRecord, bool]:
        """Attest that every unresolved victim is terminal and retry settlement."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        reason = reason.strip()
        if not reason:
            raise InputError("force-finalize reason must not be empty")
        fingerprint = _fingerprint({"command_id": command_id, "reason": reason})
        async with self.repository.transaction() as tx:
            existing = await self._idempotent_resource(tx, "fleet", "preemption.force_finalize", key, fingerprint)
            command = await tx.get_outbox(command_id)
            if command is None or command.topic != "workload.preempt":
                raise NotFoundError(f"preemption command {command_id!r} was not found")
            target = await self._require_workload(tx, str(command.payload["target_workload_id"]))
            if existing is not None:
                return target, True
            if command.payload.get("execution_completed"):
                raise ConflictError("preemption command is already complete")
            if str(command.payload.get("phase")) != "stopping":
                raise ConflictError("only a preemption in the stop phase can be force-finalized")
            victims = command.payload.get("victims")
            if not isinstance(victims, list) or not victims:
                raise ConflictError("preemption command has no durable victim set")
            outcomes = {
                str(lease_id): str(outcome)
                for lease_id, outcome in dict(command.payload.get("victim_outcomes") or {}).items()
            }
            for victim in victims:
                if not isinstance(victim, dict) or "lease_id" not in victim:
                    raise ConflictError("preemption command contains an invalid victim")
                outcomes.setdefault(str(victim["lease_id"]), "preempted")
            now = utcnow()
            await tx.put_outbox(
                command.model_copy(
                    update={
                        "state": OutboxState.FAILED,
                        "claimed_by": None,
                        "claimed_until": None,
                        "available_at": now,
                        "last_error": "force-finalize requested",
                        "payload": {
                            **command.payload,
                            "victim_outcomes": outcomes,
                            "stopped_victims": list(outcomes),
                            "force_finalize_reason": reason,
                            "force_finalize_actor": principal.subject,
                        },
                        "updated_at": now,
                    }
                )
            )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="preemption.force_finalize",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="workload",
                    resource_id=target.id,
                )
            )
            await self._audit_scoped(
                tx,
                principal,
                org_id=target.org_id,
                project_id=target.project_id,
                entity_type="preemption",
                entity_id=command.id,
                action="preemption.force_finalize_requested",
                payload={"reason": reason, "victim_count": len(victims)},
            )
            return target, False

    async def transition_workload(
        self,
        principal: Principal,
        workload_id: str,
        target: WorkloadState,
        *,
        action: str = "workload.state_changed",
        remote_id: str | None = None,
        checkpoint_uri: str | None = None,
        failure_reason: str | None = None,
        expected_generation: int | None = None,
        expected_lease_id: str | None = None,
        expected_state: WorkloadState | None = None,
    ) -> WorkloadRecord:
        """Apply a reconciler-observed workload transition with audit history."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            workload = await self._require_workload(tx, workload_id)
            self._validate_expected_workload(workload, expected_generation, expected_lease_id, expected_state)
            updates = {
                key: value
                for key, value in {
                    "remote_id": remote_id,
                    "checkpoint_uri": checkpoint_uri,
                    "failure_reason": failure_reason,
                }.items()
                if value is not None
            }
            return await self._transition_workload(tx, principal, workload, target, action, updates=updates)

    async def settle_terminal_workload(
        self,
        principal: Principal,
        workload_id: str,
        target: WorkloadState,
        *,
        action: str = "workload.terminal",
        remote_id: str | None = None,
        checkpoint_uri: str | None = None,
        failure_reason: str | None = None,
        expected_generation: int | None = None,
        expected_lease_id: str | None = None,
        expected_state: WorkloadState | None = None,
        expected_cell_generation: int | None = None,
        expected_cell_state: CellState | None = None,
        expected_unavailable_reason: str | None = None,
        expected_cell_id: str | None = None,
        #: Finish a workload whose lease rows are gone. Off by default: a missing
        #: lease is normally a real inconsistency worth refusing, and only the
        #: controller's orphan sweep -- which exists precisely to clear that state --
        #: has the standing to say the absence is the point.
        settle_without_leases: bool = False,
    ) -> tuple[WorkloadRecord, LeaseRecord | None, BudgetRecord | None]:
        """Atomically finish a workload and settle its current lease reservation.

        A lease's durable ``settled_at`` marker is the idempotency boundary. Replaying
        the same observed terminal state therefore returns the current records without
        incrementing budget spend or emitting duplicate settlement events.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        if target not in TERMINAL_WORKLOAD_STATES:
            raise InputError(f"{target.value} is not a terminal workload state")
        async with self.repository.transaction() as tx:
            await self._lock_settlement(tx, workload_id)
            workload = await self._require_workload(tx, workload_id)
            self._validate_expected_workload(workload, expected_generation, expected_lease_id, expected_state)
            await self._lock_admission_scope(tx, workload.org_id, workload.project_id)
            await self._validate_expected_cell_snapshot(
                tx,
                workload,
                expected_cell_generation,
                expected_cell_state,
                expected_unavailable_reason,
                expected_cell_id,
            )
            updates = {
                key: value
                for key, value in {
                    "remote_id": remote_id,
                    "checkpoint_uri": checkpoint_uri,
                    "failure_reason": failure_reason,
                }.items()
                if value is not None
            }
            if workload.state is not target:
                workload = await self._transition_workload(
                    tx,
                    principal,
                    workload,
                    target,
                    action,
                    updates=updates,
                )
            try:
                leases = await self._current_leases(tx, workload)
            except ConflictError:
                if not settle_without_leases:
                    raise
                # The lease rows are gone, which is exactly why this workload could
                # not be settled by any ordinary path. There is nothing to settle
                # against -- no reserved cost to charge, no capacity to release -- so
                # the workload is finished on its own.
                leases = ()
            if not leases:
                return workload, None, await tx.get_budget(workload.org_id, workload.project_id)
            settled_leases: list[LeaseRecord] = []
            budget: BudgetRecord | None = None
            for lease in leases:
                settled_lease, budget, _ = await self._settle_lease(
                    tx,
                    principal,
                    workload,
                    lease,
                    reason=target.value,
                )
                settled_leases.append(settled_lease)
            return workload, settled_leases[0], budget

    async def settle_preempted_workload(
        self,
        principal: Principal,
        workload_id: str,
        *,
        checkpoint_uri: str | None = None,
        scheduling_reason: str = "preempted; waiting to resume",
        action: str = "workload.preempted",
        expected_generation: int | None = None,
        expected_lease_id: str | None = None,
        expected_state: WorkloadState | None = None,
        require_preemption_owner: bool = False,
        provider_preempted: bool = False,
        expected_cell_generation: int | None = None,
        expected_cell_state: CellState | None = None,
        expected_unavailable_reason: str | None = None,
        expected_cell_id: str | None = None,
    ) -> tuple[WorkloadRecord, LeaseRecord | None, BudgetRecord | None]:
        """Atomically settle a preempted lease and requeue its workload.

        ``provider_preempted`` charges the restart to the preemption budget
        instead of the capacity-loss one. Not the same thing as
        ``require_preemption_owner``, which is about *Eyvan's own* priority
        preemption -- one is the fleet taking capacity back from a lower-priority
        job, the other is the cloud taking it back from the fleet.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            await self._lock_settlement(tx, workload_id)
            workload = await self._require_workload(tx, workload_id)
            self._validate_expected_workload(workload, expected_generation, expected_lease_id, expected_state)
            await self._lock_admission_scope(tx, workload.org_id, workload.project_id)
            await self._validate_expected_cell_snapshot(
                tx,
                workload,
                expected_cell_generation,
                expected_cell_state,
                expected_unavailable_reason,
                expected_cell_id,
            )
            if workload.state is WorkloadState.PENDING and workload.lease_id is None:
                return workload, None, await tx.get_budget(workload.org_id, workload.project_id)
            if await self._dispatch_reservation_may_have_started(tx, workload):
                raise ConflictError("cannot requeue while a workload dispatch may have started")
            if workload.state not in {
                WorkloadState.LEASED,
                WorkloadState.DISPATCHING,
                WorkloadState.RUNNING,
                WorkloadState.CHECKPOINTING,
                WorkloadState.CANCELLING,
            }:
                raise ConflictError(f"cannot requeue a preempted {workload.state.value} workload")
            leases = await self._current_leases(tx, workload)
            if not leases:
                raise ConflictError("preempted workload does not have a current lease")
            if require_preemption_owner and any(lease.revocation_reason != "preemption" for lease in leases):
                raise ConflictError("preemption no longer owns the workload lease")
            settled_leases: list[LeaseRecord] = []
            budget: BudgetRecord | None = None
            for lease in leases:
                settled_lease, budget, _ = await self._settle_lease(
                    tx,
                    principal,
                    workload,
                    lease,
                    reason="preempted",
                )
                settled_leases.append(settled_lease)
            now = utcnow()
            next_generation = workload.generation + 1
            resumed = workload.model_copy(
                update={
                    "state": WorkloadState.PENDING,
                    "attempt": workload.attempt + 1,
                    **(
                        {"preemption_count": workload.preemption_count + 1}
                        if provider_preempted
                        else {"capacity_loss_count": workload.capacity_loss_count + 1}
                    ),
                    "generation": next_generation,
                    "spec": self.result_spec_for_generation(
                        workload.spec,
                        workload_id=workload.id,
                        generation=next_generation,
                    ),
                    "lease_id": None,
                    "lease_ids": (),
                    "dispatch_reservation": None,
                    "remote_id": None,
                    "checkpoint_uri": checkpoint_uri,
                    "autoscale_block_reason": None,
                    "queue_admission_denial": None,
                    "scheduling_reason": scheduling_reason,
                    "updated_at": now,
                }
            )
            await self._put_workload_keeping_references(tx, resumed)
            await self._audit(
                tx,
                principal,
                resumed,
                action,
                previous_state=workload.state.value,
                new_state=resumed.state.value,
                payload={"checkpoint_uri": checkpoint_uri, "released_lease_ids": [lease.id for lease in settled_leases]},
            )
            return resumed, settled_leases[0], budget

    async def settle_failed_workload(
        self,
        principal: Principal,
        workload_id: str,
        *,
        checkpoint_uri: str | None = None,
        scheduling_reason: str = "execution failed; waiting to retry",
        action: str = "workload.failure_retry_queued",
        expected_generation: int | None = None,
        expected_lease_id: str | None = None,
        expected_state: WorkloadState | None = None,
        expected_cell_generation: int | None = None,
        expected_cell_state: CellState | None = None,
        expected_unavailable_reason: str | None = None,
        expected_cell_id: str | None = None,
    ) -> tuple[WorkloadRecord, LeaseRecord | None, BudgetRecord | None]:
        """Atomically settle a failed execution's lease and requeue its workload.

        This is the user-code failure class: it consumes the failure retry
        budget (``spec.max_failure_retries``), never the capacity-loss budget,
        and preserves any committed checkpoint so the retry resumes from it.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            await self._lock_settlement(tx, workload_id)
            workload = await self._require_workload(tx, workload_id)
            self._validate_expected_workload(workload, expected_generation, expected_lease_id, expected_state)
            await self._lock_admission_scope(tx, workload.org_id, workload.project_id)
            await self._validate_expected_cell_snapshot(
                tx,
                workload,
                expected_cell_generation,
                expected_cell_state,
                expected_unavailable_reason,
                expected_cell_id,
            )
            if workload.state is WorkloadState.PENDING and workload.lease_id is None:
                return workload, None, await tx.get_budget(workload.org_id, workload.project_id)
            if await self._dispatch_reservation_may_have_started(tx, workload):
                raise ConflictError("cannot requeue while a workload dispatch may have started")
            if workload.state not in {
                WorkloadState.LEASED,
                WorkloadState.DISPATCHING,
                WorkloadState.RUNNING,
                WorkloadState.CHECKPOINTING,
            }:
                raise ConflictError(f"cannot requeue a failed {workload.state.value} workload")
            if workload.failure_count >= workload.spec.max_failure_retries:
                raise ConflictError("workload failure retry limit has been exhausted")
            leases = await self._current_leases(tx, workload)
            if not leases:
                raise ConflictError("failed workload does not have a current lease")
            settled_leases: list[LeaseRecord] = []
            budget: BudgetRecord | None = None
            for lease in leases:
                settled_lease, budget, _ = await self._settle_lease(
                    tx,
                    principal,
                    workload,
                    lease,
                    reason="failed",
                )
                settled_leases.append(settled_lease)
            now = utcnow()
            next_generation = workload.generation + 1
            resumed = workload.model_copy(
                update={
                    "state": WorkloadState.PENDING,
                    "attempt": workload.attempt + 1,
                    "failure_count": workload.failure_count + 1,
                    "generation": next_generation,
                    "spec": self.result_spec_for_generation(
                        workload.spec,
                        workload_id=workload.id,
                        generation=next_generation,
                    ),
                    "lease_id": None,
                    "lease_ids": (),
                    "dispatch_reservation": None,
                    "remote_id": None,
                    "checkpoint_uri": checkpoint_uri,
                    "autoscale_block_reason": None,
                    "queue_admission_denial": None,
                    "scheduling_reason": scheduling_reason,
                    "updated_at": now,
                }
            )
            await self._put_workload_keeping_references(tx, resumed)
            await self._audit(
                tx,
                principal,
                resumed,
                action,
                previous_state=workload.state.value,
                new_state=resumed.state.value,
                payload={"checkpoint_uri": checkpoint_uri, "released_lease_ids": [lease.id for lease in settled_leases]},
            )
            return resumed, settled_leases[0], budget

    async def requeue_unstarted_workload(
        self,
        principal: Principal,
        workload_id: str,
        *,
        scheduling_reason: str,
        expected_generation: int,
        expected_lease_id: str,
        expected_cell_generation: int,
        expected_cell_state: CellState,
        expected_unavailable_reason: str | None,
        expected_cell_id: str | None = None,
    ) -> tuple[WorkloadRecord, LeaseRecord]:
        """Release an undispatched lease without charging its reservation."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            await self._lock_settlement(tx, workload_id)
            workload = await self._require_workload(tx, workload_id)
            self._validate_expected_workload(
                workload,
                expected_generation,
                expected_lease_id,
                WorkloadState.LEASED,
            )
            await self._lock_admission_scope(tx, workload.org_id, workload.project_id)
            await self._validate_expected_cell_snapshot(
                tx,
                workload,
                expected_cell_generation,
                expected_cell_state,
                expected_unavailable_reason,
                expected_cell_id,
            )
            if await self._dispatch_reservation_may_have_started(tx, workload):
                raise ConflictError("cannot requeue while a workload dispatch may have started")
            if workload.state is not WorkloadState.LEASED or workload.remote_id is not None:
                raise ConflictError("only an undispatched leased workload can be requeued without settlement")
            leases = await self._current_leases(tx, workload)
            if not leases or any(lease.state not in {LeaseState.PENDING, LeaseState.QUARANTINED} for lease in leases):
                raise ConflictError("undispatched workload does not own a releasable pending lease")
            now = utcnow()
            released_leases = tuple(
                lease.model_copy(
                    update={
                        "state": LeaseState.RELEASED,
                        "settlement_reason": "unstarted",
                        "settled_at": now,
                        "updated_at": now,
                    }
                )
                for lease in leases
            )
            next_generation = workload.generation + 1
            resumed = workload.model_copy(
                update={
                    "state": WorkloadState.PENDING,
                    "generation": next_generation,
                    "spec": self.result_spec_for_generation(
                        workload.spec,
                        workload_id=workload.id,
                        generation=next_generation,
                    ),
                    "lease_id": None,
                    "lease_ids": (),
                    "dispatch_reservation": None,
                    "remote_id": None,
                    "autoscale_block_reason": None,
                    "queue_admission_denial": None,
                    "scheduling_reason": scheduling_reason,
                    "updated_at": now,
                }
            )
            for released in released_leases:
                await tx.put_lease(released)
            await self._put_workload_keeping_references(tx, resumed)
            await self._audit_scoped(
                tx,
                principal,
                org_id=workload.org_id,
                project_id=workload.project_id,
                entity_type="lease",
                entity_id=leases[0].id,
                action="lease.released_unstarted",
                previous_state=leases[0].state.value,
                new_state=released_leases[0].state.value,
            )
            await self._audit(
                tx,
                principal,
                resumed,
                "workload.dispatch_requeued",
                previous_state=workload.state.value,
                new_state=resumed.state.value,
                payload={"released_lease_ids": [lease.id for lease in released_leases]},
            )
            return resumed, released_leases[0]

    async def upsert_cell(
        self, principal: Principal, cell: CellRecord, *, idempotency_key: str
    ) -> tuple[CellRecord, bool]:
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint(cell.model_copy(update={"created_at": datetime.min, "updated_at": datetime.min}))
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "cell.upsert", key, fingerprint)
            if existing_key is not None:
                existing_cell = await tx.get_cell(existing_key.resource_id)
                if existing_cell is None:
                    raise ConflictError("idempotency record points to a missing cell")
                return existing_cell, True
            existing = await tx.get_cell(cell.id)
            endpoint_changed = existing is not None and existing.endpoint != cell.endpoint
            if existing is not None and existing.state is not CellState.DELETED:
                changed_identity = _changed_cell_capacity_identity(existing, cell)
                if changed_identity:
                    raise ConflictError(
                        f"cell capacity identity cannot change before deletion: {', '.join(changed_identity)}"
                    )
                if existing.state in {CellState.DRAINING, CellState.DELETING}:
                    raise ConflictError(f"cannot update a {existing.state.value} cell")
                active_leases = [
                    lease
                    for lease in await tx.list_leases()
                    if lease.cell_id == cell.id
                    and lease.state
                    in {LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.REVOKING, LeaseState.QUARANTINED}
                ]
                if active_leases and existing.endpoint != cell.endpoint:
                    raise ConflictError("cell agent endpoint cannot change while leases are active")
                requested: dict[str, float] = {}
                for lease in active_leases:
                    workload = await self._require_workload(tx, lease.workload_id)
                    worker_multiplier = (
                        workload.spec.worker_count
                        if workload.spec.worker_resource_scope is WorkerResourceScope.PER_WORKER
                        else 1
                    )
                    per_replica = _resource_quantities(lease.resources.per_replica)
                    for resource, quantity in per_replica.items():
                        requested[resource] = requested.get(resource, 0) + (
                            quantity * lease.resources.replicas * worker_multiplier
                        )
                allocatable = _resource_quantities(cell.allocatable)
                exceeded = sorted(
                    resource for resource, quantity in requested.items() if quantity > allocatable.get(resource, 0)
                )
                if exceeded:
                    raise ConflictError(f"updated cell capacity cannot contain active lease resource {exceeded[0]!r}")
                if cell.max_replica_resources is not None:
                    maximum = _resource_quantities(cell.max_replica_resources)
                    for lease in active_leases:
                        oversized = sorted(
                            resource
                            for resource, quantity in _resource_quantities(lease.resources.per_replica).items()
                            if quantity > maximum.get(resource, 0)
                        )
                        if oversized:
                            raise ConflictError(
                                f"updated per-replica capacity cannot contain active lease resource {oversized[0]!r}"
                            )
                if cell.max_gang_replicas is not None:
                    oversized_gang = next(
                        (lease for lease in active_leases if lease.resources.replicas > cell.max_gang_replicas),
                        None,
                    )
                    if oversized_gang is not None:
                        raise ConflictError(f"updated replica limit cannot contain active lease {oversized_gang.id!r}")
            now = utcnow()
            template_changed_while_unprovisioned = (
                existing is not None
                and existing.pool_id is not None
                and existing.state is CellState.REGISTERING
                and existing.provider_phase is None
                and not cell_matches_template(existing, cell)
            )
            requires_provision = (
                existing is None
                or existing.state is CellState.DELETED
                or endpoint_changed
                or template_changed_while_unprovisioned
            )
            stored = cell.model_copy(
                update={
                    "generation": (
                        1 if existing is None else existing.generation + 1 if requires_provision else existing.generation
                    ),
                    "state": CellState.REGISTERING if requires_provision else existing.state,
                    "created_at": now if existing is None else existing.created_at,
                    "availability": cell.availability if existing is None else existing.availability,
                    "provider_phase": None if requires_provision else existing.provider_phase,
                    "provider_failure_code": None if requires_provision else existing.provider_failure_code,
                    # Provider-owned rank width is observation state, not
                    # declarative template metadata. A labels/prices update can
                    # happen while leases are active and must not reset it to
                    # the model's legacy single-host default.
                    "runtime_process_count": (
                        cell.runtime_process_count
                        if existing is None or requires_provision
                        else existing.runtime_process_count
                    ),
                    "provider_phase_started_at": None if requires_provision else existing.provider_phase_started_at,
                    "unavailable_reason": None if requires_provision else existing.unavailable_reason,
                    "last_heartbeat_at": None if requires_provision else existing.last_heartbeat_at,
                    "updated_at": now,
                }
            )
            await tx.put_cell(stored)
            if requires_provision:
                await self._outbox(
                    tx,
                    topic="capacity.apply",
                    aggregate_type="cell",
                    aggregate_id=stored.id,
                    idempotency_key=f"capacity:{stored.id}:{stored.generation}",
                    generation=stored.generation,
                    payload=_capacity_apply_payload(stored),
                )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="cell.upsert",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="cell",
                    resource_id=stored.id,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="cell",
                    entity_id=stored.id,
                    action="cell.registered" if existing is None else "cell.updated",
                    previous_state=None if existing is None else existing.state.value,
                    new_state=stored.state.value,
                    payload={"generation": stored.generation},
                )
            )
            return stored, False

    async def recreate_cell(
        self,
        principal: Principal,
        cell_id: str,
        *,
        expected_generation: int,
        idempotency_key: str,
    ) -> tuple[CellRecord, bool]:
        """Rotate an unavailable unleased cell to a fresh fenced capacity generation."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint({"cell_id": cell_id, "expected_generation": expected_generation})
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "cell.recreate", key, fingerprint)
            cell = await tx.get_cell(cell_id)
            if cell is None:
                raise NotFoundError(f"cell {cell_id!r} was not found")
            if existing_key is not None:
                return cell, True
            if cell.generation != expected_generation:
                raise ConflictError(
                    f"cell {cell.id!r} is generation {cell.generation}, expected {expected_generation} for recreation"
                )
            if cell.state is not CellState.UNAVAILABLE:
                raise ConflictError(f"cell {cell.id!r} is {cell.state.value}, not unavailable for recreation")
            active = [
                lease
                for lease in await tx.list_leases()
                if lease.cell_id == cell.id and lease.state not in {LeaseState.RELEASED, LeaseState.EXPIRED}
            ]
            if active:
                raise ConflictError(f"cell {cell.id!r} still owns {len(active)} active lease(s)")
            now = utcnow()
            recreated = cell.model_copy(
                update={
                    "state": CellState.REGISTERING,
                    "generation": cell.generation + 1,
                    "provider_phase": None,
                    "provider_failure_code": None,
                    "provider_phase_started_at": None,
                    "unavailable_reason": None,
                    "last_heartbeat_at": None,
                    "updated_at": now,
                }
            )
            await tx.put_cell(recreated)
            await self._outbox(
                tx,
                topic="capacity.apply",
                aggregate_type="cell",
                aggregate_id=recreated.id,
                idempotency_key=f"capacity:{recreated.id}:{recreated.generation}",
                generation=recreated.generation,
                payload=_capacity_apply_payload(recreated),
            )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="cell.recreate",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="cell",
                    resource_id=recreated.id,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="cell",
                    entity_id=recreated.id,
                    action="cell.recreation_requested",
                    previous_state=cell.state.value,
                    new_state=recreated.state.value,
                    payload={"generation": recreated.generation},
                )
            )
            return recreated, False

    async def requeue_unprovisioned_cell(
        self,
        principal: Principal,
        cell_id: str,
        *,
        expected_generation: int,
    ) -> tuple[CellRecord, bool]:
        """Fence and requeue a registering cell that has no live capacity apply command.

        The absence of an active apply command is the durable recovery condition.
        This can happen when a fleet template supersedes an already claimed
        command. Bumping the cell generation ensures a delayed predecessor can
        never provision the repaired template.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            cell = await tx.get_cell(cell_id)
            if cell is None:
                raise NotFoundError(f"cell {cell_id!r} was not found")
            if cell.generation != expected_generation:
                raise ConflictError(
                    f"cell {cell.id!r} is generation {cell.generation}, expected {expected_generation} for provisioning"
                )
            if cell.state is not CellState.REGISTERING or cell.provider_phase is not None:
                raise ConflictError(f"cell {cell.id!r} is not an unprovisioned registering cell")
            # Scoped to this one cell and generation rather than scanning the
            # whole outbox: a limit-bounded scan could miss the very command
            # this check exists to find, and then requeue a live provision.
            commands = await tx.list_outbox_for_aggregates("cell", {cell.id: cell.generation}, topic="capacity.apply")
            if any(_is_active_capacity_apply(command, cell) for command in commands):
                return cell, True
            now = utcnow()
            requeued = cell.model_copy(update={"generation": cell.generation + 1, "updated_at": now})
            await tx.put_cell(requeued)
            await self._outbox(
                tx,
                topic="capacity.apply",
                aggregate_type="cell",
                aggregate_id=requeued.id,
                idempotency_key=f"capacity:{requeued.id}:{requeued.generation}",
                generation=requeued.generation,
                payload=_capacity_apply_payload(requeued),
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="cell",
                    entity_id=requeued.id,
                    action="cell.provisioning_requeued",
                    previous_state=cell.state.value,
                    new_state=requeued.state.value,
                    payload={"generation": requeued.generation},
                )
            )
            return requeued, False

    async def refence_cell_lifecycle_command(
        self,
        principal: Principal,
        cell_id: str,
        *,
        expected_generation: int,
        expected_state: CellState,
        idempotency_key: str,
    ) -> tuple[CellRecord, bool]:
        """Replace a stale drain or release command with a strictly newer cell fence."""

        topic = _CELL_LIFECYCLE_TOPIC_BY_STATE.get(expected_state)
        if topic is None:
            raise InputError(f"cell state {expected_state.value!r} has no fenced lifecycle command")
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint(
            {
                "cell_id": cell_id,
                "expected_generation": expected_generation,
                "expected_state": expected_state.value,
            }
        )
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "cell.lifecycle_refence", key, fingerprint)
            cell = await tx.get_cell(cell_id)
            if cell is None:
                raise NotFoundError(f"cell {cell_id!r} was not found")
            if existing_key is not None:
                return cell, True
            if cell.generation != expected_generation:
                raise ConflictError(
                    f"cell {cell.id!r} is generation {cell.generation}, expected {expected_generation} "
                    "for lifecycle refence"
                )
            if cell.state is not expected_state:
                raise ConflictError(
                    f"cell {cell.id!r} is {cell.state.value}, expected {expected_state.value} for lifecycle refence"
                )
            now = utcnow()
            refenced = cell.model_copy(update={"generation": cell.generation + 1, "updated_at": now})
            await tx.put_cell(refenced)
            await self._outbox(
                tx,
                topic=topic,
                aggregate_type="cell",
                aggregate_id=refenced.id,
                idempotency_key=f"{topic}:{refenced.id}:{refenced.generation}",
                generation=refenced.generation,
                payload={"cell_id": refenced.id},
            )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="cell.lifecycle_refence",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="cell",
                    resource_id=refenced.id,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="cell",
                    entity_id=refenced.id,
                    action="cell.lifecycle_refenced",
                    previous_state=cell.state.value,
                    new_state=refenced.state.value,
                    payload={"generation": refenced.generation, "topic": topic},
                )
            )
            return refenced, False

    def _require_capacity_administration(
        self,
        principal: Principal,
        *,
        declared_owner: str | None,
        existing_owner: str | None,
    ) -> None:
        """Require authority over one pool, by fleet or by ownership.

        A fleet administrator may do anything, as before. An organization
        administrator may administer a pool its own organization owns -- which
        is the whole point of ``MANAGE_OWN_CAPACITY``: sizing capacity you pay
        for should not require rights over every other tenant's cells and the
        provider configuration underneath them.

        Both the existing and the declared owner are checked, and they are
        different refusals:

        * a pool someone else owns cannot be *taken over* by declaring your own
          organization on an update, and
        * a pool you own cannot be *handed away* -- to another organization, or
          to ``None``, which would make it shared capacity you can no longer
          reach and nobody asked the fleet operator about.

        Unowned pools stay fleet-only. ``owner_org_id`` defaults to ``None``, so
        every pool that existed before ownership did keeps exactly the authority
        it had.
        """

        if self.authorizer.allows(principal, Permission.MANAGE_FLEET):
            return
        if existing_owner is None and declared_owner is None:
            raise AuthorizationError(f"{principal.subject!r} lacks fleet administration for unowned capacity")
        for org_id, failure in (
            (existing_owner, "is owned by another organization"),
            (declared_owner, "would be owned by another organization"),
        ):
            if org_id is None:
                # A None on either side means the pool is shared before or after
                # this change, and moving capacity in or out of shared ownership
                # is a fleet decision.
                raise AuthorizationError(f"{principal.subject!r} cannot move capacity in or out of shared ownership")
            if not self.authorizer.allows(principal, Permission.MANAGE_OWN_CAPACITY, org_id=org_id):
                raise AuthorizationError(f"{principal.subject!r} cannot administer capacity that {failure}")

    async def upsert_capacity_pool(
        self,
        principal: Principal,
        pool: CapacityPoolRecord,
        *,
        idempotency_key: str,
    ) -> tuple[CapacityPoolRecord, bool]:
        """Create or update one declarative autoscaling node type."""

        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint(
            pool.model_copy(update={"generation": 1, "created_at": datetime.min, "updated_at": datetime.min})
        )
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "capacity-pool.upsert", key, fingerprint)
            if existing_key is not None:
                existing = await tx.get_capacity_pool(existing_key.resource_id)
                if existing is None:
                    raise ConflictError("idempotency record points to a missing capacity pool")
                return existing, True
            existing = await tx.get_capacity_pool(pool.id)
            # Authorized inside the transaction, against the stored owner rather
            # than the one the request asserts: reading it outside would leave a
            # window in which a pool changes hands between the check and the write.
            self._require_capacity_administration(
                principal,
                declared_owner=pool.owner_org_id,
                existing_owner=None if existing is None else existing.owner_org_id,
            )
            if existing is not None and pool.owner_org_id != existing.owner_org_id:
                # Reassignment is a fleet act even for a fleet admin coming
                # through this route: it changes who may administer the pool
                # from here on, which deserves its own deliberate call rather
                # than being a side effect of an update that changed max_cells.
                self.authorizer.require(principal, Permission.MANAGE_FLEET)
            now = utcnow()
            stored = pool.model_copy(
                update={
                    "generation": 1 if existing is None else existing.generation + 1,
                    "created_at": now if existing is None else existing.created_at,
                    "updated_at": now,
                }
            )
            await tx.put_capacity_pool(stored)
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="capacity-pool.upsert",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="capacity_pool",
                    resource_id=stored.id,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="capacity_pool",
                    entity_id=stored.id,
                    action="capacity_pool.created" if existing is None else "capacity_pool.updated",
                    payload={
                        "fleet_name": stored.fleet_name,
                        "node_type": stored.node_type,
                        "generation": stored.generation,
                    },
                )
            )
            return stored, False

    async def apply_fleet(
        self,
        principal: Principal,
        fleet_name: str,
        pools: tp.Sequence[CapacityPoolRecord],
        *,
        idempotency_key: str,
    ) -> tuple[list[CapacityPoolRecord], bool]:
        """Atomically apply every node type in one declarative fleet.

        Fleet administration only, deliberately, even though single-pool
        mutations now accept an owning organization. This is a *declarative*
        apply: pools absent from the request are disabled, so an organization
        admin running it would take down every pool in the fleet they do not
        own. Per-pool ``upsert``/``disable`` are the ownership-scoped surface.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        if not fleet_name or fleet_name != fleet_name.strip():
            raise InputError("fleet name must be non-empty and trimmed")
        if not pools:
            raise InputError("fleet configuration must contain at least one capacity pool")
        if any(pool.fleet_name != fleet_name for pool in pools):
            raise InputError("capacity pool fleet_name must match the fleet URL")
        if len({pool.id for pool in pools}) != len(pools):
            raise InputError("fleet configuration contains duplicate capacity pool IDs")
        try:
            validate_capacity_pool_quota_groups(pools)
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        key = _validate_idempotency_key(idempotency_key)
        normalized = [
            pool.model_copy(update={"generation": 1, "created_at": datetime.min, "updated_at": datetime.min}).model_dump(
                mode="json"
            )
            for pool in sorted(pools, key=lambda item: item.id)
        ]
        fingerprint = _fingerprint({"fleet_name": fleet_name, "pools": normalized})
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "fleet.apply", key, fingerprint)
            if existing_key is not None:
                return await tx.list_capacity_pools(fleet_name=fleet_name), True
            existing = {pool.id: pool for pool in await tx.list_capacity_pools(fleet_name=fleet_name)}
            now = utcnow()
            supplied_ids = {pool.id for pool in pools}
            for pool in pools:
                previous = existing.get(pool.id)
                await tx.put_capacity_pool(
                    pool.model_copy(
                        update={
                            "enabled": True,
                            "initialized_at": None,
                            "generation": 1 if previous is None else previous.generation + 1,
                            "created_at": now if previous is None else previous.created_at,
                            "updated_at": now,
                        }
                    )
                )
            for pool_id, previous in existing.items():
                if pool_id in supplied_ids or not previous.enabled:
                    continue
                await tx.put_capacity_pool(
                    previous.model_copy(
                        update={
                            "enabled": False,
                            "min_cells": 0,
                            "max_cells": 0,
                            # Zeroed with the others, and not an afterthought:
                            # the record requires min <= initial <= max, so a
                            # pool that declared a floor kept `initial_cells` at
                            # 1 and became invalid the moment anything
                            # re-validated it. `model_copy` does not re-run
                            # validators, so the bad record was written silently
                            # and surfaced later as a 422 on an unrelated call --
                            # with the fleet still enabled and still billing.
                            "initial_cells": 0,
                            "generation": previous.generation + 1,
                            "updated_at": now,
                        }
                    )
                )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="fleet.apply",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="fleet",
                    resource_id=fleet_name,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="fleet",
                    entity_id=fleet_name,
                    action="fleet.applied",
                    payload={"capacity_pools": sorted(supplied_ids)},
                )
            )
            return await tx.list_capacity_pools(fleet_name=fleet_name), False

    async def list_submit_fleets(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
    ) -> list[str]:
        """Return sorted runnable fleet names without exposing pool topology."""

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            return sorted(
                {pool.fleet_name for pool in await tx.list_capacity_pools() if pool.enabled and pool.max_cells > 0}
            )

    async def list_capacity_pools(
        self,
        principal: Principal,
        *,
        fleet_name: str | None = None,
    ) -> list[CapacityPoolRecord]:
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            return await tx.list_capacity_pools(fleet_name=fleet_name)

    async def get_capacity_pool(self, principal: Principal, *, pool_id: str) -> CapacityPoolRecord:
        """Return one capacity pool record."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            pool = await tx.get_capacity_pool(pool_id)
            if pool is None:
                raise NotFoundError(f"capacity pool {pool_id!r} was not found")
            return pool

    async def list_capacity(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        accelerator_type: str | None = None,
        capacity_class: CapacityClass | None = None,
    ) -> list[CapacityPoolRecord]:
        """Return enabled pools a caller may submit to, optionally filtered by shape."""

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            pools = await tx.list_capacity_pools()
        result: list[CapacityPoolRecord] = []
        for pool in pools:
            if not pool.enabled or pool.max_cells <= 0:
                continue
            if accelerator_type is not None and pool.accelerator_type != accelerator_type:
                continue
            if capacity_class is not None and pool.capacity_class != capacity_class:
                continue
            result.append(pool)
        return result

    async def disable_capacity_pool(
        self,
        principal: Principal,
        pool_id: str,
        *,
        idempotency_key: str,
    ) -> tuple[CapacityPoolRecord, bool]:
        """Disable a pool so its managed cells drain to zero."""

        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint({"pool_id": pool_id, "enabled": False})
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "capacity-pool.disable", key, fingerprint)
            pool = await tx.get_capacity_pool(pool_id)
            if pool is None:
                raise NotFoundError(f"capacity pool {pool_id!r} was not found")
            self._require_capacity_administration(
                principal,
                declared_owner=pool.owner_org_id,
                existing_owner=pool.owner_org_id,
            )
            if existing_key is not None:
                return pool, True
            stored = pool.model_copy(
                update={
                    "enabled": False,
                    "min_cells": 0,
                    "max_cells": 0,
                    "generation": pool.generation + 1,
                    "updated_at": utcnow(),
                }
            )
            await tx.put_capacity_pool(stored)
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="capacity-pool.disable",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="capacity_pool",
                    resource_id=stored.id,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="capacity_pool",
                    entity_id=stored.id,
                    action="capacity_pool.disabled",
                    payload={"fleet_name": stored.fleet_name, "node_type": stored.node_type},
                )
            )
            return stored, False

    async def disable_fleet(
        self,
        principal: Principal,
        fleet_name: str,
        *,
        idempotency_key: str,
    ) -> tuple[list[CapacityPoolRecord], bool]:
        """Disable every pool in a fleet so the autoscaler drains it to zero."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint({"fleet_name": fleet_name, "enabled": False})
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "fleet.disable", key, fingerprint)
            pools = await tx.list_capacity_pools(fleet_name=fleet_name)
            if not pools:
                raise NotFoundError(f"fleet {fleet_name!r} was not found")
            if existing_key is not None:
                return pools, True
            now = utcnow()
            for pool in pools:
                if not pool.enabled and pool.max_cells == 0:
                    continue
                await tx.put_capacity_pool(
                    pool.model_copy(
                        update={
                            "enabled": False,
                            "min_cells": 0,
                            "max_cells": 0,
                            # Same reason as the prune in ``apply_fleet``: the
                            # record requires min <= initial <= max, so leaving a
                            # floor's ``initial_cells`` at 1 wrote a pool that is
                            # invalid on re-read. ``eyvan down`` then failed with
                            # a 422 naming a field the operator never set, and
                            # the fleet stayed enabled and billing.
                            "initial_cells": 0,
                            "generation": pool.generation + 1,
                            "updated_at": now,
                        }
                    )
                )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="fleet.disable",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="fleet",
                    resource_id=fleet_name,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="fleet",
                    entity_id=fleet_name,
                    action="fleet.disabled",
                )
            )
            return await tx.list_capacity_pools(fleet_name=fleet_name), False

    async def list_cells(self, principal: Principal) -> list[CellRecord]:
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            return await tx.list_cells()

    async def get_cell(self, principal: Principal, cell_id: str) -> CellRecord:
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            cell = await tx.get_cell(cell_id)
            if cell is None:
                raise NotFoundError(f"cell {cell_id!r} was not found")
            return cell

    async def drain_cell(
        self,
        principal: Principal,
        cell_id: str,
        *,
        idempotency_key: str,
    ) -> tuple[CellRecord, bool]:
        """Stop new placement and ask the cell agent to drain capacity."""

        return await self._change_cell_state(
            principal,
            cell_id,
            target=CellState.DRAINING,
            topic="capacity.drain",
            operation="cell.drain",
            idempotency_key=idempotency_key,
        )

    async def delete_cell(
        self,
        principal: Principal,
        cell_id: str,
        *,
        idempotency_key: str,
    ) -> tuple[CellRecord, bool]:
        """Release provider capacity after every lease has left the cell."""

        return await self._change_cell_state(
            principal,
            cell_id,
            target=CellState.DELETING,
            topic="capacity.release",
            operation="cell.delete",
            idempotency_key=idempotency_key,
            require_empty=True,
            bump_generation=True,
        )

    async def _change_cell_state(
        self,
        principal: Principal,
        cell_id: str,
        *,
        target: CellState,
        topic: str,
        operation: str,
        idempotency_key: str,
        require_empty: bool = False,
        bump_generation: bool = False,
    ) -> tuple[CellRecord, bool]:
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint({"cell_id": cell_id, "target": target.value})
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", operation, key, fingerprint)
            cell = await tx.get_cell(cell_id)
            if cell is None:
                raise NotFoundError(f"cell {cell_id!r} was not found")
            if existing_key is not None:
                return cell, True
            if require_empty:
                active = [
                    lease
                    for lease in await tx.list_leases()
                    if lease.cell_id == cell_id and lease.state not in {LeaseState.RELEASED, LeaseState.EXPIRED}
                ]
                if active:
                    raise ConflictError(f"cell {cell_id!r} still owns {len(active)} active lease(s)")
            stored = cell.model_copy(
                update={
                    "state": target,
                    "generation": cell.generation + int(bump_generation),
                    "updated_at": utcnow(),
                }
            )
            await tx.put_cell(stored)
            await self._outbox(
                tx,
                topic=topic,
                aggregate_type="cell",
                aggregate_id=stored.id,
                idempotency_key=f"{operation}:{stored.id}:{stored.generation}",
                generation=stored.generation,
                payload={"cell_id": stored.id},
            )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation=operation,
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="cell",
                    resource_id=stored.id,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="cell",
                    entity_id=stored.id,
                    action=topic,
                    previous_state=cell.state.value,
                    new_state=target.value,
                )
            )
            return stored, False

    async def create_lease(
        self, principal: Principal, lease: LeaseRecord, *, idempotency_key: str
    ) -> tuple[LeaseRecord, bool]:
        """Persist a scheduler lease and bind it atomically to its workload."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        scope = _scope(lease.org_id, lease.project_id)
        fingerprint = _fingerprint(lease.model_copy(update={"created_at": datetime.min, "updated_at": datetime.min}))
        async with self.repository.transaction() as tx:
            existing = await self._idempotent_resource(tx, scope, "lease.create", key, fingerprint)
            if existing is not None:
                stored = await tx.get_lease(existing.resource_id)
                if stored is None:
                    raise ConflictError("idempotency record points to a missing lease")
                return stored, True
            workload = await self._require_workload(tx, lease.workload_id)
            await self._lock_admission_scope(tx, workload.org_id, workload.project_id)
            if workload.org_id != lease.org_id or workload.project_id != lease.project_id:
                raise InputError("lease scope does not match its workload")
            if workload.state is not WorkloadState.PENDING:
                raise ConflictError(f"workload {workload.id!r} is {workload.state.value}, not pending")
            if workload.preemption_id is not None:
                raise ConflictError("workload is reserved by an executing preemption plan")
            if lease.generation != workload.generation:
                raise ConflictError(
                    f"lease generation {lease.generation} does not match workload generation {workload.generation}"
                )
            cell = await tx.get_cell(lease.cell_id)
            if cell is None:
                raise NotFoundError(f"cell {lease.cell_id!r} was not found")
            if cell.state is not CellState.READY:
                raise ConflictError(f"cell {lease.cell_id!r} is {cell.state.value}, not ready for a new lease")
            if lease.cell_generation != cell.generation:
                raise ConflictError(
                    f"lease cell generation {lease.cell_generation} does not match cell generation {cell.generation}"
                )
            self._require_autoscale_request_ownership(cell, workload.id)
            if self.scheduler is not None:
                policy = await tx.get_policy(workload.org_id, workload.project_id)
                if policy is not None:
                    self._validate_workload_policy(workload.spec, policy)
                budget = await tx.get_budget(workload.org_id, workload.project_id)
                active_leases = [
                    candidate for candidate in await tx.list_leases() if candidate.state in ACTIVE_LEASE_STATES
                ]
                active_workload_ids = {active.workload_id for active in active_leases}
                validation_workloads = [
                    candidate
                    for candidate in await tx.list_workloads()
                    if candidate.id == workload.id or candidate.id in active_workload_ids
                ]
                validation = self.scheduler.schedule(
                    validation_workloads,
                    await tx.list_cells(),
                    leases=active_leases,
                    budgets=() if budget is None else (budget,),
                    quotas=await tx.list_quotas(org_id=workload.org_id),
                    policies=() if policy is None else (policy,),
                )
                placement = next(
                    (candidate for candidate in validation.placements if candidate.workload_id == workload.id),
                    None,
                )
                if placement is None or placement.lease.cell_id != lease.cell_id:
                    reason = (
                        validation.infeasibilities[0].message
                        if validation.infeasibilities
                        else "placement decision is no longer immediately admissible"
                    )
                    raise ConflictError(reason)
                if placement.lease.lease_id != lease.id:
                    raise ConflictError("lease identifier does not match the current placement decision")
                if lease.resources != workload.spec.gang:
                    raise ConflictError("lease resources do not match the current workload gang")
                if lease.hourly_cost != float(placement.lease.hourly_cost) or lease.reserved_cost != float(
                    placement.lease.reserved_cost
                ):
                    raise ConflictError("lease cost does not match current cell pricing")
                actual_hours = {
                    name: quantity
                    for name, quantity in _resource_quantities(lease.resource_hours_reserved).items()
                    if quantity
                }
                expected_hours = {
                    name: float(quantity) for name, quantity in placement.lease.resource_hours_reserved.items()
                }
                if actual_hours != expected_hours:
                    raise ConflictError("lease resource-hour reservation is stale")
            workload = await self._transition_workload(
                tx,
                principal,
                workload,
                WorkloadState.ADMITTED,
                "workload.admitted",
            )
            ensure_workload_transition(workload.state, WorkloadState.LEASED)
            now = utcnow()
            await self._clear_autoscale_request_ownership(tx, cell, workload.id, now)
            await tx.put_lease(lease)
            updated_workload = workload.model_copy(
                update={
                    "state": WorkloadState.LEASED,
                    "lease_id": lease.id,
                    "dispatch_reservation": None,
                    "preemption_id": None,
                    "autoscale_block_reason": None,
                    "queue_admission_denial": None,
                    "scheduling_reason": None,
                    "updated_at": now,
                }
            )
            await tx.put_workload(updated_workload)
            await self._outbox(
                tx,
                topic="workload.dispatch",
                aggregate_type="workload",
                aggregate_id=updated_workload.id,
                idempotency_key=f"dispatch:{lease.id}:{lease.generation}",
                generation=lease.generation,
                payload={
                    "workload_id": updated_workload.id,
                    "lease_id": lease.id,
                    "cell_id": lease.cell_id,
                },
            )
            await self._bind_idempotency(tx, scope, "lease.create", key, fingerprint, lease)
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    org_id=lease.org_id,
                    project_id=lease.project_id,
                    entity_type="lease",
                    entity_id=lease.id,
                    action="lease.created",
                    new_state=lease.state.value,
                    payload={"workload_id": lease.workload_id, "cell_id": lease.cell_id},
                )
            )
            await self._audit(
                tx,
                principal,
                updated_workload,
                "workload.leased",
                previous_state=workload.state.value,
                new_state=updated_workload.state.value,
                payload={"lease_id": lease.id},
            )
            return lease, False

    async def create_lease_group(
        self,
        principal: Principal,
        leases: tp.Sequence[LeaseRecord],
        *,
        idempotency_key: str,
    ) -> tuple[tuple[LeaseRecord, ...], bool]:
        """Persist a complete spread-cell placement in one transaction.

        The first member remains the legacy workload ``lease_id`` leader, but
        ``lease_ids`` is the authoritative ordered membership list. A caller
        never receives a partially persisted TPU pod group.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        members = tuple(leases)
        if len(members) < 2:
            raise InputError("a lease group requires at least two members")
        key = _validate_idempotency_key(idempotency_key)
        leader = members[0]
        scope = _scope(leader.org_id, leader.project_id)
        fingerprint = _fingerprint(
            {"leases": [member.model_dump(mode="json", exclude={"created_at", "updated_at"}) for member in members]}
        )
        workload_ids = {member.workload_id for member in members}
        cell_ids = [member.cell_id for member in members]
        group_ids = {member.group_id for member in members}
        if len(workload_ids) != 1:
            raise InputError("lease group members must belong to one workload")
        if len(cell_ids) != len(set(cell_ids)):
            raise InputError("lease group members must use distinct cells")
        if len(group_ids) != 1 or None in group_ids:
            raise InputError("lease group members must share a non-empty group_id")
        if tuple(member.group_index for member in members) != tuple(range(len(members))):
            raise InputError("lease group members must have contiguous group_index values starting at zero")
        if any(member.org_id != leader.org_id or member.project_id != leader.project_id for member in members):
            raise InputError("lease group members must share one organization and project")

        async with self.repository.transaction() as tx:
            existing = await self._idempotent_resource(tx, scope, "lease.group.create", key, fingerprint)
            if existing is not None:
                workload = await tx.get_workload(existing.resource_id)
                if workload is None:
                    raise ConflictError("idempotency record points to a missing workload")
                existing_ids = workload.lease_ids or (() if workload.lease_id is None else (workload.lease_id,))
                stored = tuple([await tx.get_lease(lease_id) for lease_id in existing_ids])
                if len(stored) != len(members) or any(member is None for member in stored):
                    raise ConflictError("idempotency record points to an incomplete lease group")
                return tuple(member for member in stored if member is not None), True

            workload = await self._require_workload(tx, leader.workload_id)
            await self._lock_admission_scope(tx, workload.org_id, workload.project_id)
            if workload.org_id != leader.org_id or workload.project_id != leader.project_id:
                raise InputError("lease group scope does not match its workload")
            if workload.state is not WorkloadState.PENDING:
                raise ConflictError(f"workload {workload.id!r} is {workload.state.value}, not pending")
            if workload.preemption_id is not None:
                raise ConflictError("workload is reserved by an executing preemption plan")
            if workload.spec.cell_group_count != len(members):
                raise ConflictError(f"workload requires {workload.spec.cell_group_count} cells, not {len(members)}")
            if any(member.generation != workload.generation for member in members):
                raise ConflictError("lease group generation does not match the workload generation")

            cells: dict[str, CellRecord] = {}
            for member in members:
                cell = await tx.get_cell(member.cell_id)
                if cell is None:
                    raise NotFoundError(f"cell {member.cell_id!r} was not found")
                if cell.state is not CellState.READY:
                    raise ConflictError(f"cell {member.cell_id!r} is {cell.state.value}, not ready for a new lease")
                if member.cell_generation != cell.generation:
                    raise ConflictError(
                        f"lease cell generation {member.cell_generation} does not match "
                        f"cell generation {cell.generation}"
                    )
                self._require_autoscale_request_ownership(cell, workload.id)
                cells[cell.id] = cell

            if self.scheduler is not None:
                policy = await tx.get_policy(workload.org_id, workload.project_id)
                if policy is not None:
                    self._validate_workload_policy(workload.spec, policy)
                budget = await tx.get_budget(workload.org_id, workload.project_id)
                active_leases = [
                    candidate for candidate in await tx.list_leases() if candidate.state in ACTIVE_LEASE_STATES
                ]
                active_workload_ids = {active.workload_id for active in active_leases}
                validation_workloads = [
                    candidate
                    for candidate in await tx.list_workloads()
                    if candidate.id == workload.id or candidate.id in active_workload_ids
                ]
                validation = self.scheduler.schedule(
                    validation_workloads,
                    await tx.list_cells(),
                    leases=active_leases,
                    budgets=() if budget is None else (budget,),
                    quotas=await tx.list_quotas(org_id=workload.org_id),
                    policies=() if policy is None else (policy,),
                )
                placement = next(
                    (candidate for candidate in validation.placements if candidate.workload_id == workload.id),
                    None,
                )
                if placement is None or len(placement.all_leases) != len(members):
                    reason = (
                        validation.infeasibilities[0].message
                        if validation.infeasibilities
                        else "pod group placement is no longer immediately admissible"
                    )
                    raise ConflictError(reason)
                expected = {candidate.lease_id: candidate for candidate in placement.all_leases}
                if set(expected) != {member.id for member in members}:
                    raise ConflictError("lease group identifiers do not match the current placement decision")
                for member in members:
                    candidate = expected[member.id]
                    if candidate.cell_id != member.cell_id:
                        raise ConflictError("lease group cell does not match the current placement decision")
                    if member.resources != workload.spec.gang:
                        raise ConflictError("lease group resources do not match the current workload gang")
                    if member.hourly_cost != float(candidate.hourly_cost) or member.reserved_cost != float(
                        candidate.reserved_cost
                    ):
                        raise ConflictError("lease group cost does not match current cell pricing")
                    actual_hours = {
                        name: quantity
                        for name, quantity in _resource_quantities(member.resource_hours_reserved).items()
                        if quantity
                    }
                    expected_hours = {
                        name: float(quantity) for name, quantity in candidate.resource_hours_reserved.items()
                    }
                    if actual_hours != expected_hours:
                        raise ConflictError("lease group resource-hour reservation is stale")

            workload = await self._transition_workload(
                tx,
                principal,
                workload,
                WorkloadState.ADMITTED,
                "workload.admitted",
            )
            ensure_workload_transition(workload.state, WorkloadState.LEASED)
            now = utcnow()
            for member in members:
                cell = cells[member.cell_id]
                await self._clear_autoscale_request_ownership(tx, cell, workload.id, now)
                await tx.put_lease(member)
            updated_workload = workload.model_copy(
                update={
                    "state": WorkloadState.LEASED,
                    "lease_id": leader.id,
                    "lease_ids": tuple(member.id for member in members),
                    "dispatch_reservation": None,
                    "preemption_id": None,
                    "autoscale_block_reason": None,
                    "queue_admission_denial": None,
                    "scheduling_reason": None,
                    "updated_at": now,
                }
            )
            await tx.put_workload(updated_workload)
            await self._outbox(
                tx,
                topic="workload.dispatch",
                aggregate_type="workload",
                aggregate_id=updated_workload.id,
                idempotency_key=f"dispatch:{leader.id}:{leader.generation}",
                generation=leader.generation,
                payload={
                    "workload_id": updated_workload.id,
                    "lease_id": leader.id,
                    "lease_ids": list(updated_workload.lease_ids),
                    "cell_id": leader.cell_id,
                    "cell_ids": cell_ids,
                },
            )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope=scope,
                    operation="lease.group.create",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="workload",
                    resource_id=updated_workload.id,
                )
            )
            for member in members:
                await tx.append_event(
                    AuditEvent(
                        id=_new_id("evt"),
                        actor=principal.subject,
                        org_id=member.org_id,
                        project_id=member.project_id,
                        entity_type="lease",
                        entity_id=member.id,
                        action="lease.created",
                        new_state=member.state.value,
                        payload={
                            "workload_id": member.workload_id,
                            "cell_id": member.cell_id,
                            "group_id": member.group_id,
                            "group_index": member.group_index,
                        },
                    )
                )
            await self._audit(
                tx,
                principal,
                updated_workload,
                "workload.leased",
                previous_state=workload.state.value,
                new_state=updated_workload.state.value,
                payload={"lease_ids": list(updated_workload.lease_ids)},
            )
            return members, False

    async def transition_lease(
        self, principal: Principal, lease_id: str, target: LeaseState, *, action: str = "lease.state_changed"
    ) -> LeaseRecord:
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            lease = await tx.get_lease(lease_id)
            if lease is None:
                raise NotFoundError(f"lease {lease_id!r} was not found")
            try:
                ensure_lease_transition(lease.state, target)
            except InvalidTransition as exc:
                raise ConflictError(str(exc)) from exc
            now = utcnow()
            changes: JsonObject | dict[str, object] = {"state": target, "updated_at": now}
            if target is LeaseState.ACTIVE and lease.run_started_at is None:
                # Same run boundary the controller's dispatch path stamps. Recorded
                # here too so every route into ACTIVE starts the settlement clock,
                # rather than only the one the controller happens to take.
                changes["run_started_at"] = now
            updated = lease.model_copy(update=changes)
            await tx.put_lease(updated)
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    org_id=lease.org_id,
                    project_id=lease.project_id,
                    entity_type="lease",
                    entity_id=lease.id,
                    action=action,
                    previous_state=lease.state.value,
                    new_state=target.value,
                )
            )
            return updated

    async def list_leases(
        self, principal: Principal, *, org_id: str, project_id: str, workload_id: str | None = None
    ) -> list[LeaseRecord]:
        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            return await tx.list_leases(org_id=org_id, project_id=project_id, workload_id=workload_id)

    async def get_lease(self, principal: Principal, lease_id: str) -> LeaseRecord:
        async with self.repository.transaction() as tx:
            lease = await tx.get_lease(lease_id)
            if lease is None:
                raise NotFoundError(f"lease {lease_id!r} was not found")
            self.authorizer.require(principal, Permission.READ, org_id=lease.org_id, project_id=lease.project_id)
            return lease

    async def put_policy(
        self, principal: Principal, policy: PolicyRecord, *, idempotency_key: str
    ) -> tuple[PolicyRecord, bool]:
        self.authorizer.require(principal, Permission.MANAGE_PROJECT, org_id=policy.org_id, project_id=policy.project_id)
        key = _validate_idempotency_key(idempotency_key)
        scope = _scope(policy.org_id, policy.project_id)
        fingerprint = _fingerprint(
            policy.model_copy(update={"revision": 0, "updated_by": "", "updated_at": datetime.min})
        )
        async with self.repository.transaction() as tx:
            await self._lock_admission_scope(tx, policy.org_id, policy.project_id)
            await self._require_registered_scope(tx, policy.org_id, policy.project_id)
            existing_key = await self._idempotent_resource(tx, scope, "policy.put", key, fingerprint)
            if existing_key is not None:
                existing = await tx.get_policy(policy.org_id, policy.project_id)
                if existing is None:
                    raise ConflictError("idempotency record points to a missing policy")
                return existing, True
            previous = await tx.get_policy(policy.org_id, policy.project_id)
            stored = policy.model_copy(
                update={
                    "revision": 1 if previous is None else previous.revision + 1,
                    "updated_by": principal.subject,
                    "updated_at": utcnow(),
                }
            )
            await tx.put_policy(stored)
            await self._bind_idempotency(tx, scope, "policy.put", key, fingerprint, stored)
            await self._audit_scoped(
                tx,
                principal,
                org_id=stored.org_id,
                project_id=stored.project_id,
                entity_type="policy",
                entity_id=_scope(stored.org_id, stored.project_id),
                action="policy.updated",
                payload={"revision": stored.revision},
            )
            return stored, False

    async def get_policy(self, principal: Principal, *, org_id: str, project_id: str) -> PolicyRecord:
        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            policy = await tx.get_policy(org_id, project_id)
            if policy is None:
                raise NotFoundError(f"policy for {org_id}/{project_id} was not found")
            return policy

    async def list_policies(self, principal: Principal, *, org_id: str) -> list[PolicyRecord]:
        self.authorizer.require(principal, Permission.READ, org_id=org_id)
        async with self.repository.transaction() as tx:
            policies = await tx.list_policies(org_id=org_id)
            return [
                policy
                for policy in policies
                if self.authorizer.allows(principal, Permission.READ, org_id=policy.org_id, project_id=policy.project_id)
            ]

    async def put_budget(
        self, principal: Principal, budget: BudgetRecord, *, idempotency_key: str
    ) -> tuple[BudgetRecord, bool]:
        self.authorizer.require(principal, Permission.MANAGE_PROJECT, org_id=budget.org_id, project_id=budget.project_id)
        key = _validate_idempotency_key(idempotency_key)
        scope = _scope(budget.org_id, budget.project_id)
        fingerprint = _fingerprint(
            budget.model_copy(
                update={
                    "spent": 0,
                    "accelerator_hours_spent": {},
                    "revision": 0,
                    "updated_by": "",
                    "updated_at": datetime.min,
                }
            )
        )
        async with self.repository.transaction() as tx:
            await self._lock_admission_scope(tx, budget.org_id, budget.project_id)
            await self._require_registered_scope(tx, budget.org_id, budget.project_id)
            existing_key = await self._idempotent_resource(tx, scope, "budget.put", key, fingerprint)
            if existing_key is not None:
                existing = await tx.get_budget(budget.org_id, budget.project_id)
                if existing is None:
                    raise ConflictError("idempotency record points to a missing budget")
                return existing, True
            await tx.get_idempotency(scope, "budget.settle", scope)
            previous = await tx.get_budget(budget.org_id, budget.project_id)
            if previous is not None and (previous.spent or previous.accelerator_hours_spent):
                if budget.currency != previous.currency or budget.period != previous.period:
                    raise ConflictError("budget currency and period cannot change after usage has been settled")
            stored = budget.model_copy(
                update={
                    "spent": budget.spent if previous is None else previous.spent,
                    "accelerator_hours_spent": (
                        budget.accelerator_hours_spent if previous is None else previous.accelerator_hours_spent
                    ),
                    "revision": 1 if previous is None else previous.revision + 1,
                    "updated_by": principal.subject,
                    "updated_at": utcnow(),
                }
            )
            await tx.put_budget(stored)
            await self._bind_idempotency(tx, scope, "budget.put", key, fingerprint, stored)
            await self._audit_scoped(
                tx,
                principal,
                org_id=stored.org_id,
                project_id=stored.project_id,
                entity_type="budget",
                entity_id=_scope(stored.org_id, stored.project_id),
                action="budget.updated",
                payload={
                    "revision": stored.revision,
                    "limit": stored.limit,
                    "spent": stored.spent,
                    "accelerator_hour_limits": stored.accelerator_hour_limits,
                    "accelerator_hours_spent": stored.accelerator_hours_spent,
                },
            )
            return stored, False

    async def put_quota(
        self, principal: Principal, quota: QuotaRecord, *, idempotency_key: str
    ) -> tuple[QuotaRecord, bool]:
        self._require_quota_admin(principal, quota.org_id, quota.project_id)
        key = _validate_idempotency_key(idempotency_key)
        scope = self._quota_id(quota.org_id, quota.project_id, quota.principal_id)
        fingerprint = _fingerprint(
            quota.model_copy(update={"revision": 0, "updated_by": "", "updated_at": datetime.min})
        )
        async with self.repository.transaction() as tx:
            await self._lock_admission_scope(tx, quota.org_id, quota.project_id)
            await self._require_registered_scope(tx, quota.org_id, quota.project_id)
            existing_key = await self._idempotent_resource(tx, scope, "quota.put", key, fingerprint)
            if existing_key is not None:
                existing = await tx.get_quota(quota.org_id, quota.project_id, quota.principal_id)
                if existing is None:
                    raise ConflictError("idempotency record points to a missing quota")
                return existing, True
            previous = await tx.get_quota(quota.org_id, quota.project_id, quota.principal_id)
            stored = quota.model_copy(
                update={
                    "revision": 1 if previous is None else previous.revision + 1,
                    "updated_by": principal.subject,
                    "updated_at": utcnow(),
                }
            )
            await tx.put_quota(stored)
            await self._bind_idempotency(tx, scope, "quota.put", key, fingerprint, stored)
            await self._audit_scoped(
                tx,
                principal,
                org_id=stored.org_id,
                project_id=stored.project_id,
                entity_type="quota",
                entity_id=self._quota_id(stored.org_id, stored.project_id, stored.principal_id),
                action="quota.updated",
                payload={"revision": stored.revision, "weight": stored.weight, "limits": stored.limits.model_dump()},
            )
            return stored, False

    async def get_quota(
        self, principal: Principal, *, org_id: str, project_id: str | None, principal_id: str | None
    ) -> QuotaRecord:
        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            quota = await tx.get_quota(org_id, project_id, principal_id)
            if quota is None:
                raise NotFoundError("quota was not found")
            return quota

    async def list_quotas(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str | None = None,
        principal_id: str | None = None,
    ) -> list[QuotaRecord]:
        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            quotas = await tx.list_quotas(org_id=org_id, project_id=project_id, principal_id=principal_id)
            return [
                quota
                for quota in quotas
                if self.authorizer.allows(principal, Permission.READ, org_id=quota.org_id, project_id=quota.project_id)
            ]

    async def delete_quota(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str | None,
        principal_id: str | None,
        idempotency_key: str,
    ) -> bool:
        self._require_quota_admin(principal, org_id, project_id)
        key = _validate_idempotency_key(idempotency_key)
        scope = self._quota_id(org_id, project_id, principal_id)
        fingerprint = _fingerprint({"org_id": org_id, "project_id": project_id, "principal_id": principal_id})
        async with self.repository.transaction() as tx:
            await self._lock_admission_scope(tx, org_id, project_id)
            if await self._idempotent_resource(tx, scope, "quota.delete", key, fingerprint) is not None:
                return True
            if not await tx.delete_quota(org_id, project_id, principal_id):
                raise NotFoundError("quota was not found")
            quota_id = self._quota_id(org_id, project_id, principal_id)
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope=scope,
                    operation="quota.delete",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="quota",
                    resource_id=quota_id,
                )
            )
            await self._audit_scoped(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                entity_type="quota",
                entity_id=quota_id,
                action="quota.deleted",
            )
            return False

    async def get_budget(self, principal: Principal, *, org_id: str, project_id: str) -> BudgetRecord:
        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            budget = await tx.get_budget(org_id, project_id)
            if budget is None:
                raise NotFoundError(f"budget for {org_id}/{project_id} was not found")
            return budget

    # ----------------------------------------------------------------- tenancy

    def _require_org_administration(self, principal: Principal, org_id: str) -> None:
        """Require authority over one whole organization, not one project in it.

        ``MANAGE_PROJECT`` is the wrong gate here: a project admin holds it, and
        registering or disabling a *project* changes what its siblings can be
        named and whether the organization accepts work at all. The same shape
        as ``_require_quota_admin``'s unscoped branch, for the same reason.
        """

        if not any(
            grant.role == Role.FLEET_ADMIN or (grant.role == Role.ORG_ADMIN and grant.org_id == org_id)
            for grant in principal.grants
        ):
            raise AuthorizationError(f"{principal.subject!r} lacks administration of organization {org_id!r}")

    async def register_organization(
        self,
        principal: Principal,
        organization: OrganizationRecord,
        *,
        idempotency_key: str,
    ) -> tuple[OrganizationRecord, bool]:
        """Record a tenant this control plane serves.

        Fleet administration only. An organization admin can administer the
        organization it was granted, and creating a *new* one is how a tenant
        would otherwise mint itself scopes nobody allocated -- including scopes
        that then collect quota and budget rows.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint(
            organization.model_copy(update={"created_at": datetime.min, "updated_at": datetime.min, "created_by": ""})
        )
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(
                tx, organization.id, "organization.register", key, fingerprint
            )
            if existing_key is not None:
                existing = await tx.get_organization(organization.id)
                if existing is None:
                    raise ConflictError("idempotency record points to a missing organization")
                return existing, True
            previous = await tx.get_organization(organization.id)
            stored = organization.model_copy(
                update={
                    "created_at": organization.created_at if previous is None else previous.created_at,
                    "created_by": principal.subject if previous is None else previous.created_by,
                    "updated_at": utcnow(),
                }
            )
            await tx.put_organization(stored)
            await self._bind_idempotency(tx, organization.id, "organization.register", key, fingerprint, stored)
            await self._audit_scoped(
                tx,
                principal,
                org_id=stored.id,
                project_id="",
                entity_type="organization",
                entity_id=stored.id,
                action="organization.registered" if previous is None else "organization.updated",
                payload={"owner": stored.owner, "enabled": stored.enabled},
            )
            return stored, False

    async def list_organizations(self, principal: Principal) -> list[OrganizationRecord]:
        """List tenants, narrowed to those the caller has any grant for.

        A viewer in one organization asking "which tenants exist" must not learn
        the customer list. Fleet administrators see everything, which is what
        makes the route usable for operating the plane.
        """

        async with self.repository.transaction() as tx:
            organizations = await tx.list_organizations()
        if self.authorizer.allows(principal, Permission.MANAGE_FLEET):
            return organizations
        visible = {grant.org_id for grant in principal.grants if grant.org_id is not None}
        return [organization for organization in organizations if organization.id in visible]

    async def get_organization(self, principal: Principal, org_id: str) -> OrganizationRecord:
        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=None)
        async with self.repository.transaction() as tx:
            organization = await tx.get_organization(org_id)
        if organization is None:
            raise NotFoundError(f"organization {org_id!r} is not registered")
        return organization

    async def register_project(
        self,
        principal: Principal,
        project: ProjectRecord,
        *,
        idempotency_key: str,
    ) -> tuple[ProjectRecord, bool]:
        """Record a project inside a registered organization.

        Refuses an unregistered organization rather than creating one
        implicitly: the whole point of the registry is that a scope exists on
        purpose, and auto-creating the parent from a typo in the child would
        reproduce exactly the problem it was built to remove.
        """

        self._require_org_administration(principal, project.org_id)
        key = _validate_idempotency_key(idempotency_key)
        scope = _scope(project.org_id, project.id)
        fingerprint = _fingerprint(
            project.model_copy(update={"created_at": datetime.min, "updated_at": datetime.min, "created_by": ""})
        )
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, scope, "project.register", key, fingerprint)
            if existing_key is not None:
                existing = await tx.get_project(project.org_id, project.id)
                if existing is None:
                    raise ConflictError("idempotency record points to a missing project")
                return existing, True
            organization = await tx.get_organization(project.org_id)
            if organization is None:
                raise ConflictError(
                    f"organization {project.org_id!r} is not registered; register it before its projects"
                )
            previous = await tx.get_project(project.org_id, project.id)
            stored = project.model_copy(
                update={
                    "created_at": project.created_at if previous is None else previous.created_at,
                    "created_by": principal.subject if previous is None else previous.created_by,
                    "updated_at": utcnow(),
                }
            )
            await tx.put_project(stored)
            await self._bind_idempotency(tx, scope, "project.register", key, fingerprint, stored)
            await self._audit_scoped(
                tx,
                principal,
                org_id=stored.org_id,
                project_id=stored.id,
                entity_type="project",
                entity_id=scope,
                action="project.registered" if previous is None else "project.updated",
                payload={"owner": stored.owner, "enabled": stored.enabled},
            )
            return stored, False

    async def list_projects(self, principal: Principal, *, org_id: str) -> list[ProjectRecord]:
        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=None)
        async with self.repository.transaction() as tx:
            return await tx.list_projects(org_id=org_id)

    async def _require_registered_scope(
        self,
        tx: RepositorySession,
        org_id: str,
        project_id: str | None,
    ) -> None:
        """Refuse a scope the registry contradicts.

        Scoped enforcement, not a global switch: an organization nobody
        registered is the pre-registry world and is left alone, so upgrading
        breaks no existing deployment. Registering one is the operator saying
        they are managing tenancy here -- from then on its projects must be real,
        which is what turns a typo in ``PUT /quotas`` from a silent success into
        an error naming the scope.
        """

        organization = await tx.get_organization(org_id)
        if organization is None:
            return
        if not organization.enabled:
            raise ConflictError(f"organization {org_id!r} is disabled and accepts no new work")
        if project_id is None or not project_id:
            return
        project = await tx.get_project(org_id, project_id)
        if project is None:
            raise ConflictError(
                f"project {project_id!r} is not registered in organization {org_id!r}; register it or correct the scope"
            )
        if not project.enabled:
            raise ConflictError(f"project {org_id}/{project_id} is disabled and accepts no new work")

    # ------------------------------------------------------------------ grants

    def _require_grant_administration(self, principal: Principal, grant: RoleGrant) -> None:
        """Require authority to issue the specific role being granted.

        Escalation is the whole hazard: whoever can issue grants can otherwise
        write themselves a better one. So the gate scales with what is being
        handed out rather than being a single "may administer grants"
        permission.

        * ``fleet_admin`` -- fleet administration, because it is unscoped and
          reaches every tenant.
        * ``org_admin`` -- administration of that organization. An org admin may
          appoint a peer in their own organization and nowhere else.
        * project roles -- ``MANAGE_PROJECT`` in that exact project, which is
          ordinary delegation: a project admin adding an operator to their team.
        """

        if grant.role is Role.FLEET_ADMIN:
            self.authorizer.require(principal, Permission.MANAGE_FLEET)
            return
        if grant.role is Role.ORG_ADMIN:
            self._require_org_administration(principal, grant.org_id or "")
            return
        self.authorizer.require(principal, Permission.MANAGE_PROJECT, org_id=grant.org_id, project_id=grant.project_id)

    async def issue_role_grant(
        self,
        principal: Principal,
        record: RoleGrantRecord,
        *,
        idempotency_key: str,
    ) -> tuple[RoleGrantRecord, bool]:
        """Grant a role durably, so authority does not depend on an IdP claim."""

        self._require_grant_administration(principal, record.grant)
        key = _validate_idempotency_key(idempotency_key)
        scope = ":".join(record.key)
        fingerprint = _fingerprint(record.model_copy(update={"granted_at": datetime.min, "granted_by": ""}))
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, scope, "grant.issue", key, fingerprint)
            if existing_key is not None:
                existing = next(
                    (item for item in await tx.list_role_grants(subject=record.subject) if item.key == record.key),
                    None,
                )
                if existing is None:
                    raise ConflictError("idempotency record points to a missing role grant")
                return existing, True
            stored = record.model_copy(update={"granted_by": principal.subject, "granted_at": utcnow()})
            await tx.put_role_grant(stored)
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope=scope,
                    operation="grant.issue",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="role_grant",
                    resource_id=scope,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    org_id=stored.grant.org_id,
                    project_id=stored.grant.project_id,
                    entity_type="role_grant",
                    entity_id=scope,
                    action="grant.issued",
                    payload={
                        "subject": stored.subject,
                        "role": stored.grant.role.value,
                        "expires_at": None if stored.expires_at is None else stored.expires_at.isoformat(),
                        "reason": stored.reason,
                    },
                )
            )
            return stored, False

    async def revoke_role_grant(self, principal: Principal, record_key: tuple[str, str, str, str]) -> bool:
        """Withdraw a durable grant.

        Revocation takes the same authority as issuance: someone who could grant
        a role should be able to take it back, and nobody else should be able to
        strip access they could not have given.
        """

        subject, role, org_id, project_id = record_key
        try:
            grant = RoleGrant(role=Role(role), org_id=org_id or None, project_id=project_id or None)
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        self._require_grant_administration(principal, grant)
        async with self.repository.transaction() as tx:
            removed = await tx.delete_role_grant(record_key)
            if removed:
                await tx.append_event(
                    AuditEvent(
                        id=_new_id("evt"),
                        actor=principal.subject,
                        org_id=grant.org_id,
                        project_id=grant.project_id,
                        entity_type="role_grant",
                        entity_id=":".join(record_key),
                        action="grant.revoked",
                        payload={"subject": subject, "role": role},
                    )
                )
            return removed

    async def list_role_grants(
        self,
        principal: Principal,
        *,
        subject: str | None = None,
        org_id: str | None = None,
    ) -> list[RoleGrantRecord]:
        """List durable grants the caller is entitled to see.

        Narrowed the same way the organization list is: a grant names a person
        and what they can reach, so an unfiltered listing would be a directory
        of every tenant's staff.
        """

        async with self.repository.transaction() as tx:
            grants = await tx.list_role_grants(subject=subject)
        if self.authorizer.allows(principal, Permission.MANAGE_FLEET):
            return [item for item in grants if org_id is None or item.grant.org_id == org_id]
        visible = {
            grant.org_id
            for grant in principal.grants
            if grant.org_id is not None and grant.role in {Role.ORG_ADMIN, Role.PROJECT_ADMIN}
        }
        return [
            item for item in grants if item.grant.org_id in visible and (org_id is None or item.grant.org_id == org_id)
        ]

    async def active_grants_for(self, subject: str, *, now: datetime | None = None) -> tuple[RoleGrant, ...]:
        """Return the unexpired durable grants held by one subject.

        Read on every authenticated request rather than cached: a revocation
        that takes effect on the next request is the behaviour an operator
        assumes, and a cache would make "I removed their access" true only
        eventually. It is one indexed read on an indexed column.
        """

        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            grants = await tx.list_role_grants(subject=subject)
        return tuple(item.grant for item in grants if item.active_at(moment))

    @staticmethod
    def _validate_secret_mounts(spec: WorkloadDefinition, policy: PolicyRecord | None) -> None:
        """Refuse a secret reference this project has not been allowed to mount.

        At submission, not at dispatch. A reference authorized only when it is
        resolved would be checked on a host holding the agent's identity, long
        after the submitter is gone -- so the refusal would arrive as a failed
        workload rather than a rejected request, and the audit trail would
        attribute the attempt to the agent.

        Empty allow-list means no mounts, deliberately. The permissive default
        is the one that turns "may submit jobs" into "may read every secret the
        agent can reach".
        """

        if not spec.secret_mounts:
            return
        allowed = () if policy is None else policy.allowed_secret_references
        for name, reference in sorted(spec.secret_mounts.items()):
            if matching_secret_reference_root(reference, allowed) is None:
                raise AuthorizationError(
                    f"secret reference {reference!r} for {name} is not permitted in this project; "
                    "add a scheme-aware root to the project policy's allowed_secret_references"
                )

    async def submit_service(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        spec: WorkloadDefinition,
        policy: ServicePolicy,
        idempotency_key: str,
    ) -> tuple[WorkloadRecord, bool]:
        """Create a service: a coordinator whose children are its replicas.

        The replica template is the spec, stored on the parent. The parent never
        runs -- ``schedulable=False`` -- so the template is inert here and every
        replica is an ordinary workload submitted from it.
        """

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        key = _validate_idempotency_key(idempotency_key)
        scope = _scope(org_id, project_id)
        # The policy travels in metadata so the reconciler can read desired state
        # off the parent without a second record type and a migration.
        stored_spec = spec.model_copy(update={"metadata": {**spec.metadata, **service_metadata(policy)}})
        async with self.repository.transaction() as tx:
            await self._require_registered_scope(tx, org_id, project_id)
            return await self._store_workload(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                spec=stored_spec,
                scope=scope,
                operation="service.create",
                key=key,
                fingerprint=_fingerprint(stored_spec),
                schedulable=False,
                coordination_kind=CoordinationKind.SERVICE,
            )

    async def register_service_endpoint(self, *, workload: WorkloadRecord, endpoint: str) -> WorkloadRecord:
        """Record where a replica says it can be reached.

        Called by the replica itself, on its own capability, after it can
        serve. That is what makes this readiness rather than an inference:
        RUNNING says a process started, and registration says it answered.

        Refused once the workload is terminal. A replica that finished cannot
        subsequently become reachable, and accepting the write would publish an
        endpoint nothing is listening on.
        """

        if not endpoint.strip() or endpoint != endpoint.strip():
            raise InputError("a service endpoint must be non-empty and trimmed")
        if workload.state in TERMINAL_WORKLOAD_STATES:
            raise ConflictError(f"workload {workload.id!r} is {workload.state.value} and cannot register an endpoint")
        async with self.repository.transaction() as tx:
            current = await tx.get_workload(workload.id)
            if current is None:
                raise NotFoundError(f"workload {workload.id!r} was not found")
            if current.generation != workload.generation:
                # The capability was minted for a superseded attempt. Its
                # endpoint belongs to a process that is being replaced.
                raise ConflictError(f"workload {workload.id!r} has moved to generation {current.generation}")
            stored = current.model_copy(update={"service_endpoint": endpoint, "updated_at": utcnow()})
            await tx.put_workload(stored)
            return stored

    async def service_endpoints(self, principal: Principal, service_id: str) -> list[JsonObject]:
        """Return the endpoints of every replica currently able to serve.

        Liveness is derived, never stored: a replica appears here only while its
        record is RUNNING and carries an endpoint. Nothing expires and nothing
        heartbeats, so a stale row is not possible -- deletion is garbage
        collection rather than correctness.
        """

        service = await self.get_workload(principal, service_id)
        async with self.repository.transaction() as tx:
            children = await tx.list_child_workloads(service_id)
        policy = service_policy(service.spec.metadata)
        revision = "" if policy is None else policy.revision
        endpoints: list[JsonObject] = []
        for child in children:
            if child.state is not WorkloadState.RUNNING or not child.service_endpoint:
                continue
            if revision and child.spec.metadata.get(CHILD_REVISION, "") != revision:
                # A replica of a superseded revision is still serving, and is
                # deliberately not advertised: a roll exists to move traffic,
                # and returning both revisions would make it a no-op.
                continue
            endpoints.append(
                {
                    "workload_id": child.id,
                    "endpoint": child.service_endpoint,
                    "revision": child.spec.metadata.get(CHILD_REVISION, ""),
                }
            )
        return endpoints

    async def submit_child_workload(
        self,
        *,
        parent: WorkloadRecord,
        spec: WorkloadDefinition,
        idempotency_key: str,
    ) -> tuple[WorkloadRecord, bool]:
        """Admit a workload submitted by a running job on its own authority.

        The caller has already been authenticated by its workload capability, so
        there is no ``Principal`` here to authorize against -- and inventing one
        would mean synthesizing a grant no identity provider issued, inside the
        most security-sensitive route in the package.

        Instead the parent *is* the authorization: a child inherits its scope,
        its identity, and its place in the fair-share tree, and can therefore
        reach nothing its parent could not.
        """

        if not parent.spec.can_submit_children:
            raise AuthorizationError(f"workload {parent.id!r} was not submitted with can_submit_children")
        if parent.state in TERMINAL_WORKLOAD_STATES or parent.state is WorkloadState.CANCELLING:
            raise ConflictError(f"workload {parent.id!r} is {parent.state.value}; it cannot gain new children")

        # Derived, not supplied. A launcher's natural key is a step number, and
        # two siblings both calling their first child "step-1" would otherwise
        # collide -- the second one silently returning the first one's handle.
        scoped_key = _validate_idempotency_key(f"{parent.id}:{idempotency_key}")
        scope = _scope(parent.org_id, parent.project_id)
        fingerprint = _fingerprint(spec)
        async with self.repository.transaction() as tx:

            async def _parent_of(workload_id: str) -> WorkloadRecord | None:
                """Return a workload's parent, not the workload itself.

                Spelled out because ``get_workload`` reads as a plausible parent
                loader and is not one: passing it directly makes every workload
                its own parent, which the cycle guard then reports as a corrupt
                tree on the very first submission.
                """

                record = await tx.get_workload(workload_id)
                if record is None or record.parent_workload_id is None:
                    return None
                return await tx.get_workload(record.parent_workload_id)

            depth = await parent_depth(parent.id, _parent_of)
            if depth + 1 >= MAX_JOB_TREE_DEPTH:
                raise JobTreeTooDeepError(
                    f"workload {parent.id!r} is already {depth} levels deep; a child would exceed {MAX_JOB_TREE_DEPTH}"
                )
            live = [
                child
                for child in await tx.list_child_workloads(parent.id)
                if child.state not in TERMINAL_WORKLOAD_STATES
            ]
            if len(live) >= MAX_LIVE_CHILDREN:
                raise JobTreeTooWideError(
                    f"workload {parent.id!r} already has {len(live)} live children; "
                    f"at most {MAX_LIVE_CHILDREN} may run at once"
                )
            return await self._store_workload(
                tx,
                # The actor is truthfully the workload, which is what the audit
                # trail should show. The identity is the parent's owner, which
                # is what quota and fair share must see.
                Principal(subject=f"workload:{parent.id}", grants=()),
                org_id=parent.org_id,
                project_id=parent.project_id,
                spec=spec,
                scope=scope,
                operation="workload.submit_child",
                key=scoped_key,
                fingerprint=fingerprint,
                parent_workload_id=parent.id,
                created_by=parent.created_by,
            )

    async def estimate_workload(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        spec: WorkloadDefinition,
    ) -> WorkloadEstimate:
        """Estimate what one workload would cost, before any record exists.

        Previously the only way to learn a price was to submit and read the
        resulting lease's ``reserved_cost`` -- which means the answer to "can I
        afford this" required spending some of the budget to find out.

        Deliberately *not* a scheduling dry run. It does not consult cell
        availability, quota, fair share or the placement scorer, because a
        pre-submission estimate that claimed to predict placement would be wrong
        the moment the fleet moved, and the number an operator actually wants is
        the price of the shape, not a promise about a particular cell.
        """

        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            pools = await tx.list_capacity_pools()
            budget = await tx.get_budget(org_id, project_id)
        hours = spec.estimated_duration_seconds / 3_600
        candidates: list[PoolEstimate] = []
        unpriced: list[str] = []
        for pool in pools:
            if not pool.enabled or not self._pool_admits_spec(pool, spec):
                continue
            cells = spec.cell_group_count * spec.worker_count
            template = pool.make_cell(f"estimate-{pool.id}", worker_count=spec.worker_count)
            # Not costing.scale: that drops zero entries by design, because it exists
            # for accelerator-hour vectors where a zero reads as "used no GPU". A
            # scalar price of zero is a real answer an unpriced pool must be able to
            # give, so this multiplies through the money primitive directly.
            hourly = costing.cost_for_hours(template.hourly_cost(), cells)
            priced = accelerator_unit_price(pool.unit_prices) is not None
            if not priced:
                unpriced.append(pool.id)
            candidates.append(
                PoolEstimate(
                    pool_id=pool.id,
                    accelerator_type=pool.accelerator_type,
                    capacity_class=str(pool.capacity_class),
                    region=pool.region,
                    zone=pool.zone,
                    cells=cells,
                    hourly_cost=hourly,
                    estimated_cost=costing.cost_for_hours(hourly, hours),
                    price_source=f"pool:{pool.id}" if priced else None,
                )
            )
        candidates.sort(key=lambda estimate: (estimate.estimated_cost, estimate.pool_id))
        blocked: tuple[str, ...] = ()
        if budget is not None and budget.hard_limit:
            remaining = max(0.0, budget.limit - budget.spent)
            blocked = tuple(estimate.pool_id for estimate in candidates if estimate.estimated_cost > remaining)
        return WorkloadEstimate(
            org_id=org_id,
            project_id=project_id,
            estimated_duration_seconds=spec.estimated_duration_seconds,
            candidates=tuple(candidates),
            budget_limit=None if budget is None else budget.limit,
            budget_spent=None if budget is None else budget.spent,
            budget_hard_limit=True if budget is None else budget.hard_limit,
            budget_blocked_pool_ids=blocked,
            unpriced_pool_ids=tuple(sorted(unpriced)),
        )

    @staticmethod
    def _pool_admits_spec(pool: CapacityPoolRecord, spec: WorkloadDefinition) -> bool:
        """Return whether a pool could host this shape, on declared attributes only.

        Attributes the operator wrote, never live state: an estimate that skipped
        a pool because it happened to be full right now would tell an operator
        their workload is unaffordable when it is merely queued.
        """

        if spec.accelerator_type and pool.accelerator_type != spec.accelerator_type:
            return False
        if spec.capacity_class is not pool.capacity_class:
            return False
        if spec.region and pool.region != spec.region:
            return False
        if spec.provider and pool.provider != spec.provider:
            return False
        return all(pool.labels.get(name) == value for name, value in spec.required_labels.items())

    async def queue_trend(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        window_s: float = 86_400.0,
        buckets: int = 24,
        now: datetime | None = None,
    ) -> QueueTrend:
        """Report submitted-vs-admitted counts over recent time buckets.

        Derived from the audit ledger rather than a new time-series store. The
        ledger already records ``workload.submitted`` and the admission transition
        with timestamps and a monotonic sequence, so the trend is a query over data
        that exists -- and a second store would be one more thing to prune, back up
        and keep consistent with the events it duplicates.

        Bucketed by wall clock rather than by event count, because the question is
        "is the queue growing", and a count-based bucket compresses exactly the
        quiet periods that answer it.
        """

        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        if not math.isfinite(window_s) or window_s < QUEUE_TREND_MIN_WINDOW_S or window_s > QUEUE_TREND_MAX_WINDOW_S:
            raise InputError(
                "queue trend window must be finite and between "
                f"{QUEUE_TREND_MIN_WINDOW_S:g} and {QUEUE_TREND_MAX_WINDOW_S:g} seconds"
            )
        if buckets < 1 or buckets > QUEUE_TREND_MAX_BUCKETS:
            raise InputError(f"queue trend requires between 1 and {QUEUE_TREND_MAX_BUCKETS} buckets")
        moment = now or utcnow()
        start = moment - timedelta(seconds=window_s)
        width = window_s / buckets
        submitted = [0] * buckets
        admitted = [0] * buckets
        async with self.repository.transaction() as tx:
            # Filter the finite time range in storage and read newest-first. The
            # old query asked for the first 5,000 sequences, so once a project
            # crossed that lifetime count every current event disappeared from
            # this chart forever. cap+1 makes overload explicit: returning a
            # plausible partial chart would be worse than refusing it.
            events = await tx.list_events(
                org_id=org_id,
                project_id=project_id,
                entity_type="workload",
                occurred_at_or_after=start,
                occurred_before=moment,
                newest_first=True,
                limit=_QUEUE_TREND_MAX_EVENTS + 1,
            )
        if len(events) > _QUEUE_TREND_MAX_EVENTS:
            raise InputError(
                "queue trend is incomplete: the requested window contains more than "
                f"{_QUEUE_TREND_MAX_EVENTS} workload events; request a shorter window"
            )
        for event in events:
            index = min(buckets - 1, int((event.timestamp - start).total_seconds() / width))
            if event.action == "workload.submitted":
                submitted[index] += 1
            elif event.new_state == WorkloadState.ADMITTED.value:
                admitted[index] += 1
        return QueueTrend(
            observed_at=moment,
            bucket_seconds=width,
            points=tuple(
                QueueTrendPoint(
                    started_at=start + timedelta(seconds=width * index),
                    submitted=submitted[index],
                    admitted=admitted[index],
                )
                for index in range(buckets)
            ),
        )

    async def budget_status(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        now: datetime | None = None,
    ) -> BudgetStatus:
        """Report one budget's window and its projected end-of-period spend.

        The forecast is derived here rather than stored, for the reason a stored
        one fails: it changes whenever any lease starts or ends, so a persisted
        copy is wrong between every pair of writes and there is no event that
        would reliably refresh it.

        The projection assumes every currently active lease runs to the end of the
        period. That is deliberately the pessimistic reading -- a forecast that
        assumed leases end early would under-warn, and the whole purpose of this
        number is to warn before a hard limit refuses admission.
        """

        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            budget = await tx.get_budget(org_id, project_id)
            if budget is None:
                raise NotFoundError(f"budget for {org_id}/{project_id} was not found")
            budget = self._rolled_budget(budget, moment)
            leases = [
                lease
                for lease in await tx.list_leases(org_id=org_id, project_id=project_id)
                if lease.state in ACTIVE_LEASE_STATES
            ]
        length = self._period_seconds(budget.period)
        started = budget.period_started_at
        ends_at = None if length is None or started is None else started + timedelta(seconds=length)
        forecast = None
        if ends_at is not None:
            hours_left = max(0.0, (ends_at - moment).total_seconds()) / 3_600
            burn = costing.total(*(lease.hourly_cost for lease in leases))
            forecast = costing.total(budget.spent, costing.cost_for_hours(burn, hours_left))
        return BudgetStatus(
            org_id=budget.org_id,
            project_id=budget.project_id,
            limit=budget.limit,
            spent=budget.spent,
            currency=budget.currency,
            period=budget.period,
            period_started_at=started,
            period_ends_at=ends_at,
            hard_limit=budget.hard_limit,
            forecast=forecast,
            alert_thresholds=budget.alert_thresholds,
            alerted_thresholds=budget.alerted_thresholds,
        )

    async def put_plane_peer(
        self,
        principal: Principal,
        *,
        plane_id: str,
        endpoint: str,
        description: str = "",
        enabled: bool = True,
    ) -> PlanePeerRecord:
        """Register or update another control plane this one may grant budget to.

        Peers are configured, never discovered. Discovery would let a plane claim a
        tenant's budget by asserting it is the home plane, and nothing in this
        design has the authority to refuse that claim -- so the list of who counts
        as a peer stays something an operator wrote down.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        if plane_id == self.plane_id:
            raise InputError("a plane does not register itself as its own peer")
        async with self.repository.transaction() as tx:
            existing = await tx.get_plane_peer(plane_id)
            peer = PlanePeerRecord(
                plane_id=plane_id,
                endpoint=endpoint,
                description=description,
                enabled=enabled,
                created_at=existing.created_at if existing is not None else utcnow(),
            )
            await tx.put_plane_peer(peer)
            await self._audit_scoped(
                tx,
                principal,
                org_id=None,
                project_id=None,
                entity_type="plane_peer",
                entity_id=plane_id,
                action="plane.peer_registered",
                payload={"endpoint": endpoint, "enabled": enabled},
            )
            return peer

    async def list_plane_peers(self, principal: Principal) -> list[PlanePeerRecord]:
        """List the control planes this one knows about."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            return await tx.list_plane_peers()

    async def record_grant_spend(
        self,
        *,
        grant_id: str,
        reported_spend: float,
        now: datetime | None = None,
    ) -> BudgetGrantRecord:
        """Record what a grantee reports having spent against its grant.

        Monotone: a report lower than one already recorded is ignored rather than
        applied. A peer restarting mid-period and reporting its fresh counter would
        otherwise walk the fleet-wide total *backwards*, which reads as money
        returned and is the one direction this number must never move on its own.
        """

        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            grant = await tx.get_budget_grant(grant_id)
            if grant is None:
                raise NotFoundError(f"budget grant {grant_id!r} was not found")
            if reported_spend <= grant.reported_spend:
                return grant
            updated = grant.model_copy(update={"reported_spend": reported_spend, "reported_at": moment})
            await tx.put_budget_grant(updated)
            return updated

    async def issue_budget_grant(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        grantee_plane_id: str,
        amount: float,
        expires_at: datetime,
        idempotency_key: str,
        now: datetime | None = None,
    ) -> BudgetGrantRecord:
        """Grant a peer plane a slice of this tenant's budget.

        The home plane refuses to issue past its own limit, counting both what it
        has already granted away and what it has itself spent. That single check is
        the entire cross-plane guarantee: no plane consults another at admission
        time, and the fleet-wide total is bounded because the slices were bounded
        when they were cut.

        Re-issuing to a plane that already holds a grant *replaces* it rather than
        adding to it, so a peer's authority is always one number an operator can
        read. The replacement is measured against headroom excluding the old grant,
        which is what makes raising a peer's slice possible at all.

        Raises:
            InputError: If the grant would exceed the tenant's remaining limit, or
                if this plane has no identity and therefore cannot be a home plane.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        if not self.plane_id:
            raise InputError("this control plane has no plane_id, so it cannot issue budget grants")
        if grantee_plane_id == self.plane_id:
            raise InputError("a plane does not grant budget to itself; it holds the limit directly")
        moment = now or utcnow()
        key = _validate_idempotency_key(idempotency_key)
        scope = _scope(org_id, project_id)
        fingerprint = _fingerprint({"grantee": grantee_plane_id, "amount": amount, "expires_at": expires_at.isoformat()})
        async with self.repository.transaction() as tx:
            await self._lock_admission_scope(tx, org_id, project_id)
            existing_key = await self._idempotent_resource(tx, scope, "budget.grant", key, fingerprint)
            if existing_key is not None:
                stored = await tx.get_budget_grant(existing_key.resource_id)
                if stored is not None:
                    return stored
            budget = await tx.get_budget(org_id, project_id)
            if budget is None:
                raise NotFoundError(f"budget for {org_id}/{project_id} was not found")
            peer = await tx.get_plane_peer(grantee_plane_id)
            if peer is None or not peer.enabled:
                raise NotFoundError(f"plane peer {grantee_plane_id!r} is not registered or is disabled")
            grants = await tx.list_budget_grants(org_id=org_id, project_id=project_id)
            replacing = next(
                (grant for grant in grants if grant.grantee_plane_id == grantee_plane_id and grant.active_at(moment)),
                None,
            )
            headroom = issuable_amount(
                budget.limit,
                grants,
                now=moment,
                local_spent=budget.spent,
                replacing=None if replacing is None else replacing.id,
            )
            if amount > headroom:
                raise InputError(
                    f"grant of {amount} exceeds the {headroom} remaining on {org_id}/{project_id}; "
                    f"the limit is {budget.limit}, {budget.spent} is spent here and the rest is granted away"
                )
            if replacing is not None:
                await tx.put_budget_grant(replacing.model_copy(update={"state": GrantState.REVOKED}))
            grant = BudgetGrantRecord(
                id=_new_id("bgr"),
                org_id=org_id,
                project_id=project_id,
                home_plane_id=self.plane_id,
                grantee_plane_id=grantee_plane_id,
                amount=amount,
                currency=budget.currency,
                issued_at=moment,
                expires_at=expires_at,
            )
            await tx.put_budget_grant(grant)
            await self._bind_idempotency(tx, scope, "budget.grant", key, fingerprint, grant)
            await self._audit_scoped(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                entity_type="budget_grant",
                entity_id=grant.id,
                action="budget.granted",
                payload={
                    "grantee_plane_id": grantee_plane_id,
                    "amount": amount,
                    "replaced": None if replacing is None else replacing.id,
                    "remaining_headroom": headroom - amount,
                },
            )
            return grant

    async def list_budget_grants(
        self,
        principal: Principal,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        include_inactive: bool = False,
        now: datetime | None = None,
    ) -> list[BudgetGrantRecord]:
        """List the budget slices this plane has issued or been issued.

        Narrowed exactly as ``list_role_grants`` is. A grant names a tenant and a
        sum of money, so an unfiltered listing would be every tenant's budget
        posture readable by anyone with a token.

        Expired grants are excluded by default and not deleted: the sweep marks
        them EXPIRED and the record stays, because "this peer used to hold
        headroom and stopped" is the first question asked when a grantee suddenly
        refuses work. ``include_inactive`` is how that question gets answered.
        """

        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            grants = await tx.list_budget_grants(org_id=org_id, project_id=project_id)
        if not include_inactive:
            grants = [grant for grant in grants if grant.active_at(moment)]
        if self.authorizer.allows(principal, Permission.MANAGE_FLEET):
            return list(grants)
        visible = {
            grant.org_id
            for grant in principal.grants
            if grant.org_id is not None and grant.role in {Role.ORG_ADMIN, Role.PROJECT_ADMIN}
        }
        return [grant for grant in grants if grant.org_id in visible]

    async def revoke_budget_grant(self, principal: Principal, *, grant_id: str) -> BudgetGrantRecord:
        """Withdraw a peer's authority to spend.

        Stops *new* admission on the grantee and says nothing about what already
        ran: that money is spent, and pretending otherwise by clawing back would
        make the home plane's ledger disagree with the invoice.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            grant = await tx.get_budget_grant(grant_id)
            if grant is None:
                raise NotFoundError(f"budget grant {grant_id!r} was not found")
            if grant.state is not GrantState.ACTIVE:
                return grant
            revoked = grant.model_copy(update={"state": GrantState.REVOKED})
            await tx.put_budget_grant(revoked)
            await self._audit_scoped(
                tx,
                principal,
                org_id=grant.org_id,
                project_id=grant.project_id,
                entity_type="budget_grant",
                entity_id=grant.id,
                action="budget.grant_revoked",
                payload={"grantee_plane_id": grant.grantee_plane_id, "amount": grant.amount},
            )
            return revoked

    async def federated_budget_status(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        now: datetime | None = None,
    ) -> FederatedBudgetStatus:
        """Report a tenant's spend across every plane that holds a grant.

        The peer figures are as fresh as each peer's last report and no fresher,
        which is why this informs and alerts but never admits. Admission stays
        local, against a grant that was bounded when it was issued -- so a stale
        report can make this number wrong without ever making spend unbounded.
        """

        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            budget = await tx.get_budget(org_id, project_id)
            if budget is None:
                raise NotFoundError(f"budget for {org_id}/{project_id} was not found")
            grants = await tx.list_budget_grants(org_id=org_id, project_id=project_id)
        active = [grant for grant in grants if grant.active_at(moment)]
        return FederatedBudgetStatus(
            org_id=org_id,
            project_id=project_id,
            home_plane_id=self.plane_id or None,
            limit=budget.limit,
            local_spent=budget.spent,
            granted_away=outstanding_total(grants, now=moment),
            reported_peer_spend=federated_spend(0.0, active),
            total_spent=federated_spend(budget.spent, active),
            active_grants=len(active),
            stale_reports=sum(1 for grant in active if grant.reported_at is None),
        )

    async def issue_quota_grant(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        grantee_plane_id: str,
        limits: ResourceLimits,
        expires_at: datetime,
        idempotency_key: str,
        now: datetime | None = None,
    ) -> QuotaGrantRecord:
        """Grant a peer plane a slice of this tenant's quota.

        The budget analogue with one difference that matters: headroom has to hold
        in *every* dimension at once. A grant of 8 TPU and 64 CPU against a quota
        with TPU to spare and no CPU left is refused, and the refusal names every
        overrun dimension rather than the first -- an operator correcting one
        number at a time against a four-dimensional ceiling is four round trips,
        and the fourth is where they grant themselves fleet admin instead.

        Raises:
            InputError: If the grant overruns the tenant's remaining ceiling in any
                dimension, or if this plane has no identity.
            NotFoundError: If the tenant has no quota, or the peer is unregistered.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        if not self.plane_id:
            raise InputError("this control plane has no plane_id, so it cannot issue quota grants")
        if grantee_plane_id == self.plane_id:
            raise InputError("a plane does not grant quota to itself; it holds the limit directly")
        moment = now or utcnow()
        key = _validate_idempotency_key(idempotency_key)
        scope = _scope(org_id, project_id)
        fingerprint = _fingerprint(
            {"grantee": grantee_plane_id, "limits": _dump_limits(limits), "expires_at": expires_at.isoformat()}
        )
        async with self.repository.transaction() as tx:
            await self._lock_admission_scope(tx, org_id, project_id)
            existing_key = await self._idempotent_resource(tx, scope, "quota.grant", key, fingerprint)
            if existing_key is not None:
                stored = await tx.get_quota_grant(existing_key.resource_id)
                if stored is not None:
                    return stored
            quota = await tx.get_quota(org_id, project_id, None)
            if quota is None:
                raise NotFoundError(f"quota for {org_id}/{project_id} was not found")
            peer = await tx.get_plane_peer(grantee_plane_id)
            if peer is None or not peer.enabled:
                raise NotFoundError(f"plane peer {grantee_plane_id!r} is not registered or is disabled")
            grants = await tx.list_quota_grants(org_id=org_id, project_id=project_id)
            replacing = next(
                (grant for grant in grants if grant.grantee_plane_id == grantee_plane_id and grant.active_at(moment)),
                None,
            )
            headroom = quota_headroom(
                quota.limits, grants, now=moment, replacing=None if replacing is None else replacing.id
            )
            overrun = exceeds_quota_headroom(limits, headroom)
            if overrun:
                detail = ", ".join(
                    f"{name} {resource_quantities(limits)[name]} > {headroom.get(name, 0.0)}" for name in overrun
                )
                raise InputError(f"quota grant exceeds the headroom on {org_id}/{project_id}: {detail}")
            if replacing is not None:
                await tx.put_quota_grant(replacing.model_copy(update={"state": GrantState.REVOKED}))
            grant = QuotaGrantRecord(
                id=_new_id("qgr"),
                org_id=org_id,
                project_id=project_id,
                home_plane_id=self.plane_id,
                grantee_plane_id=grantee_plane_id,
                limits=limits,
                issued_at=moment,
                expires_at=expires_at,
            )
            await tx.put_quota_grant(grant)
            await self._bind_idempotency(tx, scope, "quota.grant", key, fingerprint, grant)
            await self._audit_scoped(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                entity_type="quota_grant",
                entity_id=grant.id,
                action="quota.granted",
                payload={
                    "grantee_plane_id": grantee_plane_id,
                    "limits": _dump_limits(limits),
                    "replaced": None if replacing is None else replacing.id,
                },
            )
            return grant

    async def list_quota_grants(
        self,
        principal: Principal,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        include_inactive: bool = False,
        now: datetime | None = None,
    ) -> list[QuotaGrantRecord]:
        """List the quota slices this plane has issued or been issued."""

        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            grants = await tx.list_quota_grants(org_id=org_id, project_id=project_id)
        if not include_inactive:
            grants = [grant for grant in grants if grant.active_at(moment)]
        if self.authorizer.allows(principal, Permission.MANAGE_FLEET):
            return list(grants)
        visible = {
            grant.org_id
            for grant in principal.grants
            if grant.org_id is not None and grant.role in {Role.ORG_ADMIN, Role.PROJECT_ADMIN}
        }
        return [grant for grant in grants if grant.org_id in visible]

    async def revoke_quota_grant(self, principal: Principal, *, grant_id: str) -> QuotaGrantRecord:
        """Withdraw a peer's authority to place work against this tenant's quota.

        Stops *new* admission on the grantee and does not disturb what is already
        running: a quota is a concurrent ceiling, and evicting live work to enforce
        a ceiling change would make revocation a fleet-wide outage rather than an
        administrative act.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            grant = await tx.get_quota_grant(grant_id)
            if grant is None:
                raise NotFoundError(f"quota grant {grant_id!r} was not found")
            if grant.state is not GrantState.ACTIVE:
                return grant
            revoked = grant.model_copy(update={"state": GrantState.REVOKED})
            await tx.put_quota_grant(revoked)
            await self._audit_scoped(
                tx,
                principal,
                org_id=grant.org_id,
                project_id=grant.project_id,
                entity_type="quota_grant",
                entity_id=grant.id,
                action="quota.grant_revoked",
                payload={"grantee_plane_id": grant.grantee_plane_id},
            )
            return revoked

    async def expire_budget_grants_once(self, *, now: datetime | None = None) -> int:
        """Mark elapsed grants EXPIRED so an unreachable home plane fails closed.

        The grantee already refuses to admit past ``expires_at`` -- the check is on
        the timestamp, not on this sweep -- so this exists to make the *state* match
        what enforcement already does, which is what an operator reads when asking
        why a peer stopped admitting.
        """

        moment = now or utcnow()
        expired = 0
        async with self.repository.transaction() as tx:
            for grant in await tx.list_budget_grants():
                if grant.state is GrantState.ACTIVE and moment >= grant.expires_at:
                    await tx.put_budget_grant(grant.model_copy(update={"state": GrantState.EXPIRED}))
                    expired += 1
            # Quota slices lapse in the same pass rather than in a second leader
            # phase. They expire on the same clock for the same reason, and a
            # separate phase would be one more thing to forget to register --
            # which is exactly how the budget sweep spent its first life.
            for quota_grant in await tx.list_quota_grants():
                if quota_grant.state is GrantState.ACTIVE and moment >= quota_grant.expires_at:
                    await tx.put_quota_grant(quota_grant.model_copy(update={"state": GrantState.EXPIRED}))
                    expired += 1
        return expired

    async def roll_budget_period(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        now: datetime | None = None,
    ) -> BudgetRecord:
        """Close the current budget period if it has elapsed, and persist the result.

        Settlement already rolls a stale period before charging, so this exists for
        the fleet that stops running work: a project that spends nothing for two
        months keeps reporting last quarter's ``spent`` until something settles,
        and an operator looking at that number cannot tell a stale window from a
        genuinely expensive one.

        Deliberately *not* a force-reset. Zeroing a period that has not elapsed
        would erase spend the limit is still supposed to bind, which is a way to
        silently disable a budget rather than to administer one.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            await self._lock_admission_scope(tx, org_id, project_id)
            budget = await tx.get_budget(org_id, project_id)
            if budget is None:
                raise NotFoundError(f"budget for {org_id}/{project_id} was not found")
            rolled = self._rolled_budget(budget, moment)
            if rolled.period_started_at == budget.period_started_at:
                return budget
            rolled = rolled.model_copy(
                update={"revision": budget.revision + 1, "updated_by": principal.subject, "updated_at": moment}
            )
            await tx.put_budget(rolled)
            await self._audit_scoped(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                entity_type="budget",
                entity_id=_scope(org_id, project_id),
                action="budget.period_rolled",
                payload={
                    "period": rolled.period,
                    "closed_spend": budget.spent,
                    "period_started_at": rolled.period_started_at.isoformat()
                    if rolled.period_started_at is not None
                    else None,
                },
            )
            return rolled

    async def list_budgets(self, principal: Principal, *, org_id: str) -> list[BudgetRecord]:
        self.authorizer.require(principal, Permission.READ, org_id=org_id)
        async with self.repository.transaction() as tx:
            budgets = await tx.list_budgets(org_id=org_id)
            return [
                budget
                for budget in budgets
                if self.authorizer.allows(principal, Permission.READ, org_id=budget.org_id, project_id=budget.project_id)
            ]

    async def list_events(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        after_sequence: int = 0,
        entity_type: str | None = None,
        entity_id: str | None = None,
        limit: int = 100,
    ) -> list[AuditEvent]:
        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            return await tx.list_events(
                after_sequence=after_sequence,
                org_id=org_id,
                project_id=project_id,
                entity_type=entity_type,
                entity_id=entity_id,
                limit=limit,
            )

    async def list_fleet_events(
        self,
        principal: Principal,
        *,
        after_sequence: int = 0,
        entity_type: str | None = None,
        entity_id: str | None = None,
        limit: int = 100,
    ) -> list[AuditEvent]:
        """Return fleet-wide audit history to fleet administrators."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            return await tx.list_events(
                after_sequence=after_sequence,
                entity_type=entity_type,
                entity_id=entity_id,
                limit=limit,
            )

    async def wait_for_events(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        after_sequence: int,
        timeout: float | None = None,
    ) -> list[AuditEvent]:
        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        return await self.repository.wait_for_events(
            after_sequence=after_sequence,
            org_id=org_id,
            project_id=project_id,
            timeout=timeout,
        )

    @staticmethod
    def _require_autoscale_request_ownership(cell: CellRecord, workload_id: str) -> None:
        """Reject a lease that would take capacity requested by another workload."""

        owner_id = cell.autoscale_request_workload_id
        if owner_id is not None and owner_id != workload_id:
            raise ConflictError("cell is reserved for a different autoscale workload")

    @classmethod
    async def _clear_autoscale_request_ownership(
        cls,
        tx: RepositorySession,
        cell: CellRecord,
        workload_id: str,
        now: datetime,
    ) -> None:
        """Release a one-job provisioning attribution once a cell becomes shared supply."""

        cls._require_autoscale_request_ownership(cell, workload_id)
        if cell.autoscale_request_workload_id is None:
            return
        await tx.put_cell(
            cell.model_copy(
                update={
                    "autoscale_request_workload_id": None,
                    "updated_at": now,
                }
            )
        )

    async def _require_workload(self, tx: RepositorySession, workload_id: str) -> WorkloadRecord:
        workload = await tx.get_workload(workload_id)
        if workload is None:
            raise NotFoundError(f"workload {workload_id!r} was not found")
        return workload

    @staticmethod
    def _validate_expected_workload(
        workload: WorkloadRecord,
        expected_generation: int | None,
        expected_lease_id: str | None,
        expected_state: WorkloadState | None = None,
    ) -> None:
        if expected_generation is not None and workload.generation != expected_generation:
            raise ConflictError(
                f"workload generation {workload.generation} does not match observed generation {expected_generation}"
            )
        if expected_lease_id is not None and workload.lease_id != expected_lease_id:
            raise ConflictError(
                f"workload lease {workload.lease_id!r} does not match observed lease {expected_lease_id!r}"
            )
        if expected_state is not None and workload.state is not expected_state:
            raise ConflictError(
                f"workload state {workload.state.value} does not match observed state {expected_state.value}"
            )

    @staticmethod
    async def _lock_settlement(tx: RepositorySession, workload_id: str) -> None:
        # PostgreSQL's repository acquires a transaction advisory lock as part of
        # idempotency lookup. The durable lease marker remains the source of truth.
        await tx.get_idempotency("fleet", "workload.settle", workload_id)

    @staticmethod
    async def _lock_admission_scope(
        tx: RepositorySession,
        org_id: str,
        project_id: str | None,
    ) -> None:
        await tx.get_idempotency("fleet", "admission.org", _scope(org_id, ""))
        if project_id is not None:
            await tx.get_idempotency("fleet", "admission.project", _scope(org_id, project_id))

    @staticmethod
    async def _current_lease(tx: RepositorySession, workload: WorkloadRecord) -> LeaseRecord | None:
        leases = await ControlPlaneService._current_leases(tx, workload)
        return leases[0] if leases else None

    @staticmethod
    async def _current_leases(tx: RepositorySession, workload: WorkloadRecord) -> tuple[LeaseRecord, ...]:
        """Return the complete active lease group in deterministic rank order."""

        lease_ids = workload.lease_ids or (() if workload.lease_id is None else (workload.lease_id,))
        if not lease_ids:
            return ()
        if workload.lease_id != lease_ids[0]:
            raise ConflictError("workload leader lease does not match its lease group")
        leases = tuple([await tx.get_lease(lease_id) for lease_id in lease_ids])
        if any(lease is None for lease in leases):
            missing = lease_ids[next(index for index, lease in enumerate(leases) if lease is None)]
            raise ConflictError(f"workload references missing lease {missing!r}")
        concrete = tuple(lease for lease in leases if lease is not None)
        for index, lease in enumerate(concrete):
            if (
                lease.workload_id != workload.id
                or lease.org_id != workload.org_id
                or lease.project_id != workload.project_id
            ):
                raise ConflictError("workload current lease has inconsistent ownership")
            if len(concrete) > 1 and lease.group_index != index:
                raise ConflictError("workload lease group has an inconsistent member order")
        if len(concrete) > 1:
            group_ids = {lease.group_id for lease in concrete}
            if len(group_ids) != 1 or None in group_ids:
                raise ConflictError("workload lease group has inconsistent membership")
        return concrete

    @classmethod
    async def _validate_expected_cell_snapshot(
        cls,
        tx: RepositorySession,
        workload: WorkloadRecord,
        expected_generation: int | None,
        expected_state: CellState | None,
        expected_unavailable_reason: str | None,
        expected_cell_id: str | None = None,
    ) -> None:
        if expected_state is None:
            return
        leases = await cls._current_leases(tx, workload)
        if not leases:
            raise ConflictError("workload no longer owns the observed cell lease")
        lease = (
            next((candidate for candidate in leases if candidate.cell_id == expected_cell_id), None)
            if expected_cell_id is not None
            else leases[0]
        )
        if lease is None:
            raise ConflictError("observed cell is not a member of the workload lease group")
        cell = await tx.get_cell(lease.cell_id)
        if cell is None:
            return
        if expected_generation is not None and cell.generation != expected_generation:
            raise ConflictError("execution cell generation changed after the loss observation")
        if cell.state is not expected_state or cell.unavailable_reason != expected_unavailable_reason:
            raise ConflictError("execution cell recovered or changed after the loss observation")

    async def settle_orphaned_lease(
        self,
        principal: Principal,
        lease_id: str,
        *,
        reason: str,
        expected_generation: int,
    ) -> bool:
        """Release one lease that no live workload claims, and charge what it held.

        The counterpart to ``settle_orphaned_workloads_once``, which settles by
        *workload* and therefore cannot see this: a lease whose workload finished,
        vanished, or stopped referencing it is reachable from no other path. It
        keeps its cell allocated and its cost unsettled indefinitely -- capacity
        the fleet is paying for and no tenant is charged for.

        Re-verified inside the transaction rather than trusted from the scan. A
        lease that a dispatch re-attached between the two is not orphaned, and
        releasing it would kill live work; the generation check makes that a
        no-op instead of a race.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            lease = await tx.get_lease(lease_id)
            if lease is None or lease.state not in ACTIVE_LEASE_STATES:
                return False
            if lease.generation != expected_generation:
                return False
            workload = await tx.get_workload(lease.workload_id)
            if not self._lease_is_orphaned(lease, workload):
                return False
            # ``_settle_lease`` persists the lease and the budget itself, so this
            # only decides *whether* to call it. Re-writing what it returned would
            # be redundant at best, and a mutation test proved it was never
            # load-bearing.
            _settled, _budget, changed = await self._settle_lease(tx, principal, workload, lease, reason=reason)
            return changed

    @staticmethod
    def _lease_is_orphaned(lease: LeaseRecord, workload: WorkloadRecord | None) -> bool:
        """Return whether no live workload still claims this lease.

        Three ways, and the third is the one the workload sweep cannot reach: the
        workload is gone, the workload is terminal, or the workload is live and
        has moved on to different leases. A retry or a preemption resume produces
        exactly that third shape.
        """

        if workload is None or workload.state in TERMINAL_WORKLOAD_STATES:
            return True
        claimed = set(workload.lease_ids) | ({workload.lease_id} if workload.lease_id else set())
        return lease.id not in claimed

    async def _settle_lease(
        self,
        tx: RepositorySession,
        principal: Principal,
        workload: WorkloadRecord | None,
        lease: LeaseRecord,
        *,
        reason: str,
    ) -> tuple[LeaseRecord, BudgetRecord | None, bool]:
        """Release one lease and charge what it actually held.

        ``workload`` is optional because a lease can outlive its workload record
        -- that is the condition the orphaned-lease sweep exists to clear, and
        refusing to settle without a record would leave the capacity charged and
        held forever. The lease already carries the workload id, which is all
        the audit payload ever needed.
        """

        scope = _scope(lease.org_id, lease.project_id)
        await tx.get_idempotency(scope, "budget.settle", scope)
        budget = await tx.get_budget(lease.org_id, lease.project_id)
        if lease.settled_at is not None:
            return lease, budget, False
        if lease.state not in {LeaseState.RELEASED, LeaseState.EXPIRED}:
            try:
                ensure_lease_transition(lease.state, LeaseState.RELEASED)
            except InvalidTransition as exc:
                raise ConflictError(str(exc)) from exc
        now = utcnow()
        # Settle what was actually held, not what was estimated at submission.
        # `settled_cost = reserved_cost` charged the submitter's guess: a 6-hour
        # estimate preempted after 10 minutes billed 6 hours, and a 1-hour estimate
        # that ran 40 hours billed 1 -- and the second direction is the one that
        # quietly overruns a hard budget.
        queued_seconds, running_seconds = self._lease_clock(lease, now)
        elapsed_hours = (queued_seconds + running_seconds) / 3_600
        settled_cost = costing.cost_for_hours(lease.hourly_cost, elapsed_hours)
        hours = self._accelerator_hours(lease, elapsed_hours)
        settled = lease.model_copy(
            update={
                "state": lease.state if lease.state is LeaseState.EXPIRED else LeaseState.RELEASED,
                "settled_cost": settled_cost,
                "accelerator_hours_settled": hours,
                "settlement_reason": reason,
                "settled_at": now,
                "queued_seconds": queued_seconds,
                "running_seconds": running_seconds,
                "updated_at": now,
            }
        )
        await tx.put_lease(settled)
        await self._audit_scoped(
            tx,
            principal,
            org_id=lease.org_id,
            project_id=lease.project_id,
            entity_type="lease",
            entity_id=lease.id,
            action="lease.settled",
            previous_state=lease.state.value,
            new_state=settled.state.value,
            payload={
                "workload_id": lease.workload_id,
                "reason": reason,
                "settled_cost": settled.settled_cost,
                "accelerator_hours_settled": hours,
            },
        )
        if budget is None:
            return settled, None, True
        # Roll the period before adding, so a settlement that lands after the window
        # closed opens the new one rather than being absorbed by the old.
        budget = self._rolled_budget(budget, now)
        accelerator_hours_spent = dict(budget.accelerator_hours_spent)
        for accelerator, quantity in hours.items():
            accelerator_hours_spent[accelerator] = _decimal_sum(
                accelerator_hours_spent.get(accelerator, 0),
                quantity,
            )
        spent = _decimal_sum(budget.spent, settled_cost)
        crossed = budget.thresholds_crossed_by(spent)
        updated_budget = budget.model_copy(
            update={
                "spent": spent,
                "accelerator_hours_spent": accelerator_hours_spent,
                "alerted_thresholds": tuple(sorted({*budget.alerted_thresholds, *crossed})),
                "revision": budget.revision + 1,
                "updated_by": principal.subject,
                "updated_at": now,
            }
        )
        await tx.put_budget(updated_budget)
        for threshold in crossed:
            # One event per newly crossed threshold, in the same transaction as the
            # spend that crossed it. An alert emitted outside this transaction could
            # survive a rollback and warn about spend that never happened.
            await self._audit_scoped(
                tx,
                principal,
                org_id=lease.org_id,
                project_id=lease.project_id,
                entity_type="budget",
                entity_id=_scope(lease.org_id, lease.project_id),
                action="budget.threshold_crossed",
                payload={
                    "threshold": threshold,
                    "spent": updated_budget.spent,
                    "limit": updated_budget.limit,
                    "period": updated_budget.period,
                    "hard_limit": updated_budget.hard_limit,
                },
            )
        await self._audit_scoped(
            tx,
            principal,
            org_id=lease.org_id,
            project_id=lease.project_id,
            entity_type="budget",
            entity_id=_scope(lease.org_id, lease.project_id),
            action="budget.lease_settled",
            payload={
                "lease_id": lease.id,
                "cost_increment": settled_cost,
                "accelerator_hours_increment": hours,
                "spent": updated_budget.spent,
                "accelerator_hours_spent": updated_budget.accelerator_hours_spent,
            },
        )
        return settled, updated_budget, True

    @staticmethod
    def _lease_clock(lease: LeaseRecord, settled_at: datetime) -> tuple[float, float]:
        """Return ``(queued_seconds, running_seconds)`` for one settled lease.

        Queued is capacity reserved for this workload but not yet executing its
        payload; running is from the lease's first entry into ACTIVE. A lease that
        never ran is pure queue, and both are clamped at zero so a clock skew cannot
        mint negative time.
        """

        held = max((settled_at - lease.created_at).total_seconds(), 0.0)
        if lease.run_started_at is None:
            return held, 0.0
        running = max((settled_at - lease.run_started_at).total_seconds(), 0.0)
        return max(held - running, 0.0), min(running, held)

    @staticmethod
    def _accelerator_hours(lease: LeaseRecord, elapsed_hours: float) -> dict[str, float]:
        """Scale the lease's reserved resource shape by hours actually held.

        ``resource_hours_reserved`` is the *shape* multiplied by the submitted
        duration estimate, so dividing it back out recovers the per-hour vector and
        re-scaling by real elapsed time gives settled accelerator-hours. When the
        estimate was zero there is no shape to scale and the result is empty.
        """

        reserved = lease.resource_hours_reserved
        values = {"gpu": float(reserved.gpu), "tpu": float(reserved.tpu), **reserved.custom}
        estimated_hours = lease.reserved_cost / lease.hourly_cost if lease.hourly_cost else 0.0
        if not estimated_hours:
            return {name: quantity for name, quantity in values.items() if quantity}
        return costing.scale(values, elapsed_hours / estimated_hours)

    @staticmethod
    def _period_seconds(period: str) -> float | None:
        """Return the length of a budget period, or None when it never rolls."""

        return {
            "hourly": 3_600.0,
            "daily": 86_400.0,
            "weekly": 604_800.0,
            "monthly": 2_592_000.0,
            "quarterly": 7_776_000.0,
            "yearly": 31_536_000.0,
        }.get(period.strip().lower())

    @classmethod
    def _rolled_budget(cls, budget: BudgetRecord, now: datetime) -> BudgetRecord:
        """Reset a budget whose declared period has elapsed.

        ``period`` was a string nothing acted on. That was survivable only while
        ``spent`` held a duration estimate; once it became actual elapsed cost it is a
        true monotone total, so without this a "monthly" budget is a lifetime budget
        and every alert threshold fires exactly once, ever.
        """

        length = cls._period_seconds(budget.period)
        if length is None:
            return budget
        started = budget.period_started_at
        if started is None:
            return budget.model_copy(update={"period_started_at": now})
        if (now - started).total_seconds() < length:
            return budget
        # Advance by whole periods so a fleet idle for three months does not need
        # three settlements to catch up, and the window stays aligned to its origin.
        elapsed_periods = int((now - started).total_seconds() // length)
        return budget.model_copy(
            update={
                "spent": 0.0,
                "accelerator_hours_spent": {},
                # Alerts are per-period. Carrying them across the boundary would
                # silence every warning for the rest of the fleet's life after the
                # first month that reached them.
                "alerted_thresholds": (),
                "period_started_at": started + timedelta(seconds=length * elapsed_periods),
            }
        )

    async def _transition_workload(
        self,
        tx: RepositorySession,
        principal: Principal,
        workload: WorkloadRecord,
        target: WorkloadState,
        action: str,
        *,
        updates: WorkloadTransitionUpdates | None = None,
    ) -> WorkloadRecord:
        try:
            ensure_workload_transition(workload.state, target)
        except InvalidTransition as exc:
            raise ConflictError(str(exc)) from exc
        now = utcnow()
        transition_updates: dict[str, tp.Any] = {"state": target, "updated_at": now, **(updates or {})}
        if target in TERMINAL_WORKLOAD_STATES:
            transition_updates["dispatch_reservation"] = None
        updated = workload.model_copy(update=transition_updates)
        await tx.put_workload(updated)
        if target in TERMINAL_WORKLOAD_STATES:
            # Every terminal path funnels through here -- ordinary settlement,
            # cancellation, the batch and DAG coordinators -- so releasing claims
            # at this one point is what makes the reference index complete rather
            # than complete-for-the-paths-someone-remembered.
            await self._release_artifact_references(tx, updated, now=now)
        await self._audit(
            tx,
            principal,
            updated,
            action,
            previous_state=workload.state.value,
            new_state=target.value,
        )
        return updated

    async def _idempotent_resource(
        self,
        tx: RepositorySession,
        scope: str,
        operation: str,
        key: str,
        fingerprint: str,
    ) -> IdempotencyRecord | None:
        existing = await tx.get_idempotency(scope, operation, key)
        if existing is not None and existing.fingerprint != fingerprint:
            raise ConflictError("Idempotency-Key was already used with a different request")
        return existing

    @staticmethod
    async def _dispatch_reservation_may_have_started(
        tx: RepositorySession,
        workload: WorkloadRecord,
    ) -> bool:
        """Return whether requeueing could orphan a cell-agent submission."""

        reservation = workload.dispatch_reservation
        lease_ids = workload.lease_ids or (() if workload.lease_id is None else (workload.lease_id,))
        if (
            reservation is None
            or reservation.generation != workload.generation
            or reservation.workload_attempt != workload.attempt
            or reservation.lease_ids != lease_ids
        ):
            return False
        command = await tx.get_outbox(reservation.outbox_id)
        persisted: WorkloadDispatchReservation | None = None
        if command is not None:
            try:
                persisted = WorkloadDispatchReservation.model_validate(command.payload.get("dispatch_reservation"))
            except Exception:
                persisted = None
        return bool(
            command is not None
            and command.topic == "workload.dispatch"
            and command.aggregate_type == "workload"
            and command.aggregate_id == workload.id
            and command.generation == workload.generation
            and command.state in {OutboxState.PENDING, OutboxState.PROCESSING, OutboxState.FAILED}
            and persisted == reservation
        )

    @staticmethod
    async def _preemption_execution_started(tx: RepositorySession, workload: WorkloadRecord) -> bool:
        if workload.preemption_id is None:
            return False
        command = await tx.get_outbox(workload.preemption_id)
        return bool(
            command is not None
            and command.topic == "workload.preempt"
            and command.state is not OutboxState.SUCCEEDED
            and command.payload.get("execution_started")
            and not command.payload.get("execution_completed")
        )

    async def _bind_idempotency(
        self,
        tx: RepositorySession,
        scope: str,
        operation: str,
        key: str,
        fingerprint: str,
        resource: (
            WorkloadRecord
            | LeaseRecord
            | PolicyRecord
            | BudgetRecord
            | QuotaRecord
            | BudgetGrantRecord
            | OrganizationRecord
            | ProjectRecord
        ),
    ) -> None:
        # Explicit on every branch, including the last. This used to fall through
        # to "budget" for anything unrecognized, which silently bound a new record
        # type to its *scope* instead of its own id -- so a replay looked up the
        # wrong resource and issued a second one.
        if isinstance(resource, WorkloadRecord):
            resource_type, resource_id = "workload", resource.id
        elif isinstance(resource, LeaseRecord):
            resource_type, resource_id = "lease", resource.id
        elif isinstance(resource, PolicyRecord):
            resource_type, resource_id = "policy", _scope(resource.org_id, resource.project_id)
        elif isinstance(resource, QuotaRecord):
            resource_type = "quota"
            resource_id = self._quota_id(resource.org_id, resource.project_id, resource.principal_id)
        elif isinstance(resource, BudgetGrantRecord):
            resource_type, resource_id = "budget_grant", resource.id
        elif isinstance(resource, QuotaGrantRecord):
            resource_type, resource_id = "quota_grant", resource.id
        elif isinstance(resource, BudgetRecord):
            resource_type, resource_id = "budget", _scope(resource.org_id, resource.project_id)
        elif isinstance(resource, ProjectRecord):
            # Before OrganizationRecord: a project carries an org_id too, and
            # ordering these the other way would bind every project under its
            # organization's id, so a replay would return the wrong record.
            resource_type, resource_id = "project", _scope(resource.org_id, resource.id)
        elif isinstance(resource, OrganizationRecord):
            resource_type, resource_id = "organization", resource.id
        else:
            raise TypeError(f"no idempotency binding is defined for {type(resource).__name__}")
        await tx.put_idempotency(
            IdempotencyRecord(
                scope=scope,
                operation=operation,
                key=key,
                fingerprint=fingerprint,
                resource_type=resource_type,
                resource_id=resource_id,
            )
        )

    @staticmethod
    def _quota_id(org_id: str, project_id: str | None, principal_id: str | None) -> str:
        payload = json.dumps((org_id, project_id, principal_id), ensure_ascii=True, separators=(",", ":")).encode()
        return f"quota_{hashlib.sha256(payload).hexdigest()}"

    def _require_quota_admin(self, principal: Principal, org_id: str, project_id: str | None) -> None:
        if project_id is not None:
            self.authorizer.require(principal, Permission.MANAGE_PROJECT, org_id=org_id, project_id=project_id)
            return
        if not any(
            grant.role == Role.FLEET_ADMIN or (grant.role == Role.ORG_ADMIN and grant.org_id == org_id)
            for grant in principal.grants
        ):
            raise AuthorizationError(f"{principal.subject!r} lacks organization quota administration")

    async def _audit(
        self,
        tx: RepositorySession,
        principal: Principal,
        workload: WorkloadRecord,
        action: str,
        *,
        previous_state: str | None = None,
        new_state: str | None = None,
        payload: JsonObject | None = None,
    ) -> AuditEvent:
        return await self._audit_scoped(
            tx,
            principal,
            org_id=workload.org_id,
            project_id=workload.project_id,
            entity_type="workload",
            entity_id=workload.id,
            action=action,
            previous_state=previous_state,
            new_state=new_state,
            payload=payload,
        )

    async def accrue_cell_billing(self, *, now: datetime | None = None) -> CellBillingAccrual:
        """Advance every billable cell's clock and accrue its idle time.

        Eyvan has always charged *leases*: a workload runs, its lease settles, the
        project's budget moves. A provisioned cell holding no lease costs exactly
        as much and appeared in no ledger at all, so a fleet could burn a TPU pod
        for a week between jobs while every budget reported compliance.

        Two properties make this safe to run from an at-least-once leader loop:

        The clock charges ``now - billing_ticked_at``, not one fixed interval, so
        a cycle that runs late, twice, or after a failover charges the elapsed
        time once rather than the number of times the loop happened to fire.

        Only a cell the provider is actually charging for accrues. A queued TPU
        request is a record Eyvan holds and a bill nobody is paying; billing
        starts when the resource exists and stops when it is released.
        """

        moment = now or utcnow()
        accrued_seconds = 0.0
        idle_seconds = 0.0
        accrued_cost = 0.0
        idle_cost = 0.0
        counted = 0
        async with self.repository.transaction() as tx:
            leased_cells = {lease.cell_id for lease in await tx.list_leases() if lease.state in ACTIVE_LEASE_STATES}
            for cell in await tx.list_cells():
                billable = cell.state in BILLABLE_CELL_STATES
                if not billable:
                    if cell.billing_started_at is None:
                        continue
                    # The provider resource is gone. Close the clock rather than
                    # leaving a started_at that a later re-READY would charge from,
                    # which would bill for the whole gap the cell did not exist.
                    await tx.put_cell(cell.model_copy(update={"billing_started_at": None, "billing_ticked_at": None}))
                    continue
                if cell.billing_started_at is None:
                    # First tick for this incarnation: start the clock, charge
                    # nothing. Charging from created_at would bill for the queue
                    # wait, which is the one interval that is genuinely free.
                    await tx.put_cell(
                        cell.model_copy(update={"billing_started_at": moment, "billing_ticked_at": moment})
                    )
                    continue
                elapsed = max(0.0, (moment - (cell.billing_ticked_at or cell.billing_started_at)).total_seconds())
                if elapsed == 0.0:
                    continue
                idle = cell.id not in leased_cells
                hourly = cell.hourly_cost()
                cost = costing.cost_for_hours(hourly, elapsed / 3_600)
                await tx.put_cell(
                    cell.model_copy(
                        update={
                            "billing_ticked_at": moment,
                            "billed_seconds": costing.total(cell.billed_seconds, elapsed),
                            "idle_seconds": costing.total(cell.idle_seconds, elapsed if idle else 0.0),
                        }
                    )
                )
                counted += 1
                accrued_seconds += elapsed
                accrued_cost = costing.total(accrued_cost, cost)
                if idle:
                    idle_seconds += elapsed
                    idle_cost = costing.total(idle_cost, cost)
        return CellBillingAccrual(
            cells=counted,
            accrued_seconds=accrued_seconds,
            idle_seconds=idle_seconds,
            accrued_cost=accrued_cost,
            idle_cost=idle_cost,
        )

    async def carbon_report(self, principal: Principal, *, now: datetime | None = None) -> CarbonReport:
        """Report grid intensity for the regions the fleet actually occupies.

        Only regions with a pool, because a global table of grid intensities is a
        dataset rather than a report -- and one whose accuracy nobody here could
        vouch for.

        Every entry today carries ``no_provider``: none ships. Reporting the
        absence is the deliverable. A carbon feature whose unconfigured state is
        an empty response is one an operator cannot distinguish from a broken
        one, and a carbon feature whose unconfigured state is *zero* is one that
        claims every grid is clean.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            pools = await tx.list_capacity_pools()
        regions: list[RegionCarbon] = []
        for region in sorted({pool.region for pool in pools if pool.region}):
            reading = fresh_reading(self.carbon.read(region), now=moment)
            if isinstance(reading, CarbonUnavailable):
                regions.append(RegionCarbon(region=region, reason=reading.reason.value, detail=reading.detail))
                continue
            regions.append(
                RegionCarbon(
                    region=region,
                    intensity=reading.grams_co2e_per_kwh,
                    source=reading.source,
                    observed_at=reading.observed_at,
                )
            )
        return CarbonReport(observed_at=moment, regions=tuple(regions))

    async def warmth_report(self, principal: Principal, *, now: datetime | None = None) -> WarmthReport:
        """Report every pool's warm floor and whether it is currently held.

        Uses the same ``cell_provides_pool_supply`` predicate the planner uses to
        decide what counts toward a floor. Reporting on a different definition
        than the one that acts would produce the worst possible answer: a report
        that says the floor is met while the planner keeps launching.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            pools = await tx.list_capacity_pools()
            cells = await tx.list_cells()
            leased = {lease.cell_id for lease in await tx.list_leases() if lease.state in ACTIVE_LEASE_STATES}
        warmth: list[PoolWarmth] = []
        for pool in pools:
            supplying = [
                cell
                for cell in cells
                if cell.pool_id == pool.id
                and cell.state is not CellState.DELETED
                and cell_provides_pool_supply(cell, pool, moment)
            ]
            floor = pool.warm_floor(moment)
            # Only the cells the floor actually reserves are warm; anything above it
            # is ordinary demand-driven capacity and belongs to no warm promise.
            warm = supplying[: max(floor, 0)]
            warmth.append(
                PoolWarmth(
                    pool_id=pool.id,
                    floor=floor,
                    window_active=any(window.covers(moment) for window in pool.warm_windows),
                    warm_cells=len(warm),
                    warm_idle_cells=sum(1 for cell in warm if cell.id not in leased),
                    hourly_cost=costing.total(*(cell.hourly_cost() for cell in warm)),
                )
            )
        warmth.sort(key=lambda entry: entry.pool_id)
        return WarmthReport(observed_at=moment, pools=tuple(warmth))

    async def launch_latency_report(self, principal: Principal, *, now: datetime | None = None) -> LaunchLatencyReport:
        """Report how long each pool takes to hand over usable capacity.

        Read from the same ``provision_latencies_s`` samples the scheduler ranks
        on, rather than from a second time series. A separate store would drift
        from the numbers that actually decide placement, and the first symptom
        would be a dashboard disagreeing with a scheduling verdict -- which is
        worse than no dashboard.

        Percentiles are computed over the pool's cells pooled together, because
        an operator picks a *pool* and lives with whichever cell it hands them.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            pools = await tx.list_capacity_pools()
            cells = await tx.list_cells()
        by_pool: dict[str, list[CellRecord]] = {}
        for cell in cells:
            if cell.pool_id is not None and cell.state is not CellState.DELETED:
                by_pool.setdefault(cell.pool_id, []).append(cell)
        entries: list[PoolLaunchLatency] = []
        for pool in pools:
            members = by_pool.get(pool.id, [])
            samples = sorted(latency for cell in members for latency in cell.availability.provision_latencies_s)
            entries.append(
                PoolLaunchLatency(
                    pool_id=pool.id,
                    accelerator_type=pool.accelerator_type,
                    p95_seconds=p95_latency(samples),
                    median_seconds=samples[len(samples) // 2] if samples else None,
                    slowest_seconds=samples[-1] if samples else None,
                    samples=len(samples),
                    cells_in_cooldown=sum(
                        1
                        for cell in members
                        if cell.availability.cooldown_until is not None and cell.availability.cooldown_until > moment
                    ),
                )
            )
        entries.sort(key=lambda entry: entry.pool_id)
        return LaunchLatencyReport(observed_at=moment, pools=tuple(entries))

    async def cell_billing_report(self, principal: Principal, *, pool_id: str | None = None) -> CellBillingReport:
        """Report accrued and idle cell cost, optionally for one pool.

        The idle figure is the one an operator cannot get anywhere else: it is
        capacity the fleet paid for and no lease used, and no lease-derived ledger
        can produce it, because an idle cell has no lease to settle.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            cells = [cell for cell in await tx.list_cells() if pool_id is None or cell.pool_id == pool_id]
        billed = sum(cell.billed_seconds for cell in cells)
        idle = sum(cell.idle_seconds for cell in cells)
        accrued_cost = costing.total(
            *(costing.cost_for_hours(cell.hourly_cost(), cell.billed_seconds / 3_600) for cell in cells)
        )
        idle_cost = costing.total(
            *(costing.cost_for_hours(cell.hourly_cost(), cell.idle_seconds / 3_600) for cell in cells)
        )
        return CellBillingReport(
            pool_id=pool_id,
            cells=len(cells),
            billed_seconds=billed,
            idle_seconds=idle,
            accrued_cost=accrued_cost,
            idle_cost=idle_cost,
            unpriced_cells=sum(1 for cell in cells if not cell.unit_prices),
        )

    async def index_managed_artifact(
        self,
        *,
        org_id: str,
        project_id: str,
        uri: str,
        size_bytes: int,
        sha256: str | None = None,
    ) -> ManagedArtifactRecord:
        """Record one stored object's size, enforcing the project's byte quota.

        Called *before* the bytes reach the store, so the quota is a genuine
        admission gate rather than an after-the-fact report: refusing a write
        that already landed would leave the store and the index disagreeing about
        what exists, which is the one thing this index must never do.

        Re-indexing a content-addressed URI is an update only within the requested
        project. The physical store namespaces bytes by organization and project,
        so another project using the same URI owns an independent row and charge.

        Raises:
            InputError: If the write would exceed the scope's artifact byte quota.
        """

        reference = parse_managed_uri(uri)
        now = utcnow()
        async with self.repository.transaction() as tx:
            existing = await tx.get_managed_artifact(org_id, project_id, uri)
            quota = await self._artifact_quota(tx, org_id=org_id, project_id=project_id)
            if quota is not None:
                stored = await tx.sum_managed_artifact_bytes(org_id=org_id, project_id=project_id)
                # Subtract what this URI already occupies: an overwrite replaces
                # bytes rather than adding them, and charging the full size again
                # would make a re-upload fail at half the real limit.
                projected = stored - (existing.size_bytes if existing is not None else 0) + size_bytes
                if projected > quota:
                    raise InputError(
                        f"artifact would put {org_id}/{project_id} at {projected} bytes, "
                        f"above its {quota}-byte artifact quota"
                    )
            record = ManagedArtifactRecord(
                uri=uri,
                kind=reference.kind.value,
                org_id=org_id,
                project_id=project_id,
                size_bytes=size_bytes,
                sha256=sha256,
                created_at=existing.created_at if existing is not None else now,
                updated_at=now,
                last_accessed_at=existing.last_accessed_at if existing is not None else None,
            )
            await tx.put_managed_artifact(record)
            return record

    @staticmethod
    async def _artifact_quota(
        tx: RepositorySession,
        *,
        org_id: str,
        project_id: str,
    ) -> int | None:
        """Return the tightest artifact byte ceiling that applies, or ``None``.

        Project scope wins over org scope when both are set, matching how the
        resource quota hierarchy already resolves: the narrower grant is the one
        an operator wrote about this project specifically.
        """

        for scope_project in (project_id, None):
            quota = await tx.get_quota(org_id, scope_project, None)
            if quota is not None and quota.artifact_bytes_limit is not None:
                return quota.artifact_bytes_limit
        return None

    async def list_managed_artifacts(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        kind: str | None = None,
        limit: int = 1000,
    ) -> list[ManagedArtifactRecord]:
        """List indexed artifacts in one project."""

        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            return await tx.list_managed_artifacts(
                org_id=org_id,
                project_id=project_id,
                kind=kind,
                limit=limit,
            )

    async def inventory_managed_artifacts(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        cursor: str | None = None,
        limit: int = 100,
        reindex_missing: bool = False,
        acknowledge_scope: str | None = None,
    ) -> ArtifactInventoryReport:
        """Compare one bounded physical page with one scoped index page.

        The comparison is built completely before an optional reindex begins.
        Reindexing is additive only: it can create metadata for a physical-only
        URI observed in this exact scope, and cannot update an existing row,
        repair a discrepancy, infer another tenant, or delete bytes.
        """

        permission = Permission.MANAGE_PROJECT if reindex_missing else Permission.READ
        self.authorizer.require(principal, permission, org_id=org_id, project_id=project_id)
        configured_inventory_store = self.inventory_store
        if configured_inventory_store is None and isinstance(self.artifact_store, ManagedArtifactInventoryStore):
            configured_inventory_store = self.artifact_store
        if configured_inventory_store is None:
            raise InputError("the configured managed artifact store does not support bounded inventory")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_MANAGED_ARTIFACT_INVENTORY_PAGE_SIZE
        ):
            raise InputError(
                f"artifact inventory limit must be between 1 and {MAX_MANAGED_ARTIFACT_INVENTORY_PAGE_SIZE}"
            )
        if cursor is not None:
            try:
                parse_managed_uri(cursor)
            except ValueError as exc:
                raise InputError("artifact inventory cursor must be a canonical managed artifact URI") from exc
        expected_acknowledgement = f"{org_id}/{project_id}"
        if reindex_missing and acknowledge_scope != expected_acknowledgement:
            raise InputError(
                "reindexing missing artifact metadata requires acknowledge_scope="
                f"{expected_acknowledgement!r}; run the report-only inventory first"
            )

        store = configured_inventory_store
        physical_page = await asyncio.to_thread(
            store.list_scope,
            org_id=org_id,
            project_id=project_id,
            cursor=cursor,
            limit=limit,
        )
        async with self.repository.transaction() as tx:
            indexed_page = await tx.list_managed_artifacts_page(
                org_id=org_id,
                project_id=project_id,
                after_uri=cursor,
                limit=limit + 1,
            )
        physical = {item.uri: item for item in physical_page.objects}
        indexed = {item.uri: item for item in indexed_page}
        union = sorted(set(physical) | set(indexed))
        selected = union[:limit]
        has_more = len(union) > limit or physical_page.next_cursor is not None or len(indexed_page) > limit

        entries: list[ArtifactInventoryEntry] = []
        for uri in selected:
            record = indexed.get(uri)
            observed = physical.get(uri)
            if observed is None and record is not None:
                # A point observation distinguishes genuinely missing bytes from
                # list pagination or a concurrent create. Any stat error raises;
                # it is never interpreted as absence.
                observed = await asyncio.to_thread(
                    store.stat,
                    uri,
                    org_id=org_id,
                    project_id=project_id,
                )
            if record is None:
                if observed is None:
                    # Every selected URI came from at least one source. Reaching
                    # neither means the observations changed underneath us; an
                    # additive repair from that evidence would be unsafe.
                    raise ConflictError("artifact inventory changed during observation; rerun the dry-run")
                status = ArtifactInventoryStatus.PHYSICAL_ONLY
            elif observed is None:
                status = ArtifactInventoryStatus.INDEX_ONLY
            elif record.size_bytes != observed.size_bytes:
                status = ArtifactInventoryStatus.SIZE_MISMATCH
            else:
                status = ArtifactInventoryStatus.MATCHED
            entries.append(
                ArtifactInventoryEntry(
                    uri=uri,
                    status=status,
                    indexed_size_bytes=None if record is None else record.size_bytes,
                    observed_size_bytes=None if observed is None else observed.size_bytes,
                    observed_object_version=None if observed is None else observed.object_version,
                )
            )

        reindexed = 0
        if reindex_missing:
            # Validate every physical-only observation before the first database
            # write. A late list/stat/digest error therefore leaves the whole
            # page report-only instead of partially reindexed.
            validated: list[ManagedArtifactObjectMetadata] = []
            for entry in entries:
                if entry.status is not ArtifactInventoryStatus.PHYSICAL_ONLY:
                    continue
                assert entry.observed_size_bytes is not None
                assert entry.observed_object_version is not None
                expected = ManagedArtifactObjectMetadata(
                    uri=entry.uri,
                    size_bytes=entry.observed_size_bytes,
                    object_version=entry.observed_object_version,
                )
                reference = parse_managed_uri(entry.uri)
                if reference.kind in CONTENT_ADDRESSED_KINDS:
                    try:
                        payload = await asyncio.to_thread(
                            store.get,
                            entry.uri,
                            org_id=org_id,
                            project_id=project_id,
                        )
                    except (OSError, subprocess.CalledProcessError, ValueError) as exc:
                        raise ManagedArtifactInventoryError(
                            f"could not verify content-addressed inventory object {entry.uri!r}"
                        ) from exc
                    if len(payload) != expected.size_bytes:
                        raise ConflictError(
                            f"artifact {entry.uri!r} changed while its content digest was verified; rerun inventory"
                        )
                validated.append(expected)

            # This is the last provider observation before the additive database
            # transaction. In particular it runs after content-addressed reads,
            # so a replacement during digest validation cannot reindex metadata
            # from the older listed generation. ``stat`` means the current live
            # GCS object; noncurrent/deleted generations never satisfy equality.
            for expected in validated:
                current = await asyncio.to_thread(
                    store.stat,
                    expected.uri,
                    org_id=org_id,
                    project_id=project_id,
                )
                if current != expected:
                    raise ConflictError(
                        f"artifact {expected.uri!r} changed after the dry-run observation; rerun inventory"
                    )
            added_uris = await self._reindex_observed_artifacts(
                principal,
                org_id=org_id,
                project_id=project_id,
                metadata=tuple(validated),
            )
            reindexed = len(added_uris)
            entries = [entry.model_copy(update={"reindexed": entry.uri in added_uris}) for entry in entries]

        return ArtifactInventoryReport(
            org_id=org_id,
            project_id=project_id,
            entries=tuple(entries),
            next_cursor=selected[-1] if has_more and selected else None,
            dry_run=not reindex_missing,
            reindexed_count=reindexed,
        )

    async def _reindex_observed_artifacts(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        metadata: tuple[ManagedArtifactObjectMetadata, ...],
    ) -> frozenset[str]:
        """Atomically add observed physical-only rows and their audit events."""

        if not metadata:
            return frozenset()
        now = utcnow()
        async with self.repository.transaction() as tx:
            missing: list[tuple[ManagedArtifactObjectMetadata, ManagedArtifactRef]] = []
            for observed in metadata:
                if await tx.get_managed_artifact(org_id, project_id, observed.uri) is None:
                    missing.append((observed, parse_managed_uri(observed.uri)))
            quota = await self._artifact_quota(tx, org_id=org_id, project_id=project_id)
            stored = await tx.sum_managed_artifact_bytes(org_id=org_id, project_id=project_id)
            projected = stored + sum(observed.size_bytes for observed, _reference in missing)
            if quota is not None and projected > quota:
                raise InputError(
                    f"reindexing observed artifacts would put {org_id}/{project_id} at {projected} bytes, "
                    f"above its {quota}-byte artifact quota"
                )
            for observed, reference in missing:
                record = ManagedArtifactRecord(
                    uri=observed.uri,
                    kind=reference.kind.value,
                    org_id=org_id,
                    project_id=project_id,
                    size_bytes=observed.size_bytes,
                    sha256=reference.object_id if reference.kind in CONTENT_ADDRESSED_KINDS else None,
                    created_at=now,
                    updated_at=now,
                )
                await tx.put_managed_artifact(record)
                await self._audit_scoped(
                    tx,
                    principal,
                    org_id=org_id,
                    project_id=project_id,
                    entity_type="artifact",
                    entity_id=observed.uri[:256],
                    action="artifact.inventory_reindexed",
                    payload={
                        "size_bytes": observed.size_bytes,
                        "object_version": observed.object_version,
                    },
                )
        return frozenset(observed.uri for observed, _reference in missing)

    async def artifact_usage(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str | None = None,
    ) -> ArtifactUsage:
        """Report indexed bytes and the quota they are measured against."""

        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            stored = await tx.sum_managed_artifact_bytes(org_id=org_id, project_id=project_id)
            limit = None if project_id is None else await self._artifact_quota(tx, org_id=org_id, project_id=project_id)
            artifact_count = await tx.count_managed_artifacts(org_id=org_id, project_id=project_id)
        return ArtifactUsage(
            org_id=org_id,
            project_id=project_id,
            artifact_count=artifact_count,
            stored_bytes=stored,
            quota_bytes=limit,
        )

    async def hold_artifact(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        uri: str,
        reason: str,
        expires_at: datetime | None = None,
    ) -> ManagedArtifactRecord:
        """Place an explicit human claim on one scoped artifact.

        The caller supplies the scope and is authorized before any row lookup. A
        URI alone is never used to discover which tenant owns an artifact.
        """

        self.authorizer.require(principal, Permission.MANAGE_PROJECT, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            record = await tx.get_managed_artifact(org_id, project_id, uri)
            if record is None:
                raise NotFoundError(f"artifact {uri!r} is not indexed in {org_id}/{project_id}")
            hold = ArtifactHold(reason=reason, held_by=principal.subject, expires_at=expires_at)
            held = record.model_copy(update={"hold": hold, "updated_at": utcnow()})
            await tx.put_managed_artifact(held)
            await self._audit_scoped(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                entity_type="artifact",
                entity_id=uri[:256],
                action="artifact.held",
                payload={"reason": reason, "expires_at": expires_at.isoformat() if expires_at else None},
            )
            return held

    async def release_artifact_hold(
        self, principal: Principal, *, org_id: str, project_id: str, uri: str
    ) -> ManagedArtifactRecord:
        """Remove the hold on one scoped artifact.

        Anyone with manage permission in the requested project may release,
        including a hold someone else placed. Authorization happens before the
        scoped lookup so another tenant's URI is never an ownership oracle.
        """

        self.authorizer.require(principal, Permission.MANAGE_PROJECT, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            record = await tx.get_managed_artifact(org_id, project_id, uri)
            if record is None:
                raise NotFoundError(f"artifact {uri!r} is not indexed in {org_id}/{project_id}")
            if record.hold is None:
                return record
            released = record.model_copy(update={"hold": None, "updated_at": utcnow()})
            await tx.put_managed_artifact(released)
            await self._audit_scoped(
                tx,
                principal,
                org_id=org_id,
                project_id=project_id,
                entity_type="artifact",
                entity_id=uri[:256],
                action="artifact.hold_released",
                payload={"previous_holder": record.hold.held_by, "previous_reason": record.hold.reason},
            )
            return released

    async def mark_superseded_results_once(self, *, now: datetime | None = None) -> int:
        """Record which result artifacts a later attempt replaced.

        Each retry mints a new result URI, so a workload on attempt 3 owns three
        result addresses and only the newest can be written to again. The older
        ones are the outputs of runs that were retried -- worth *naming*, because
        an operator asking why attempt 3 differs from attempt 1 needs attempt 1's
        output, and worth naming *durably*, because the URI is derived from a
        generation number the record no longer holds once it moves on.

        Marking only. Nothing here deletes: the index has to be trusted before a
        collector may run against it, and being able to see what a collector
        *would* take is how that trust is earned.
        """

        moment = now or utcnow()
        marked = 0
        async with self.repository.transaction() as tx:
            for workload in await tx.list_workloads():
                execution = workload.spec.execution
                if not isinstance(execution, CallableExecution) or not execution.result_uri:
                    continue
                current = execution.result_uri
                if not is_managed_uri(current):
                    continue
                for reference in await tx.list_artifact_references(
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    workload_id=workload.id,
                ):
                    if reference.uri == current or not reference.uri.startswith(_MANAGED_RESULT_PREFIX):
                        continue
                    record = await tx.get_managed_artifact(workload.org_id, workload.project_id, reference.uri)
                    if record is None or record.superseded_at is not None:
                        continue
                    await tx.put_managed_artifact(
                        record.model_copy(
                            update={"superseded_at": moment, "superseded_by_uri": current, "updated_at": moment}
                        )
                    )
                    marked += 1
        return marked

    async def collectable_artifacts(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        now: datetime | None = None,
    ) -> list[ManagedArtifactRecord]:
        """List artifacts nothing currently protects.

        A *candidate* list, not a deletion queue, and the distinction is the point
        of shipping it before any collector: an operator can compare what this
        returns against what they expected to be reclaimable, and find the gap in
        the index before a collector finds it for them.
        """

        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        moment = now or utcnow()
        collectable: list[ManagedArtifactRecord] = []
        async with self.repository.transaction() as tx:
            for record in await tx.list_managed_artifacts(org_id=org_id, project_id=project_id, limit=100_000):
                live = await tx.list_artifact_references(
                    org_id=org_id,
                    project_id=project_id,
                    uri=record.uri,
                    active_only=True,
                )
                if record.collectable_at(moment, has_live_reference=bool(live)):
                    collectable.append(record)
        return collectable

    async def collect_artifacts_once(
        self,
        *,
        org_id: str,
        project_id: str,
        principal: Principal | None = None,
        grace_period_s: float = DEFAULT_ARTIFACT_GRACE_PERIOD_S,
        retention_by_kind: tp.Mapping[str, float | None] | None = None,
        dry_run: bool = True,
        limit: int = 500,
        now: datetime | None = None,
    ) -> ArtifactCollection:
        """Reclaim artifacts nothing protects, newest-safe and dry by default.

        **The grace period is the load-bearing part, and the race it closes is not
        obvious.** An upload indexes the object *before* any workload names it --
        it has to, since the submitter needs the URI to build the spec. So between
        the upload and the submission the artifact has zero references and looks
        exactly like abandoned garbage. A collector without a grace period would
        delete callables out from under jobs being submitted right now, and would
        do it more often the busier the fleet is.

        ``dry_run`` defaults to true for the same reason the index shipped before
        any collector: the first real pass on a fleet should be one a human read
        first. The legacy controller can call the destructive branch when a
        deletion store is explicitly wired, but M1.5 does not add the serialized,
        version-conditional state machine that would make that branch race-free.

        Order matters on failure. The store object is removed first and the index
        row second, so a crash between them leaves an index entry for bytes that
        are gone -- which the next pass re-attempts and resolves, since deleting an
        absent object is not an error. The reverse order would leak: bytes with no
        index row are bytes nothing will ever look at again.
        """

        if self.artifact_store is None:
            raise InputError("a managed artifact store is not configured")
        moment = now or utcnow()
        # Per kind, because they are not the same thing. A callable is
        # reproducible from its source; an attempt's logs are the only record of
        # what happened, and they are wanted long after the bytes that produced
        # them stopped mattering. One window for both means either deleting the
        # evidence early or keeping every pickle forever.
        #
        # ``None`` means never collect this kind. Spelled out rather than as an
        # infinite window: ``timedelta(seconds=inf)`` overflows, so the clever
        # version is not merely less clear, it does not run.
        windows: dict[str, float | None] = dict(retention_by_kind or {})
        collected: list[str] = []
        reclaimed = 0
        held = referenced = within_grace = 0
        async with self.repository.transaction() as tx:
            candidates = await tx.list_managed_artifacts(org_id=org_id, project_id=project_id, limit=100_000)
        for record in candidates:
            if len(collected) >= limit:
                break
            if record.hold is not None and record.hold.active_at(moment):
                held += 1
                continue
            window = windows.get(str(record.kind), grace_period_s)
            if window is None:
                within_grace += 1
                continue
            if record.created_at > moment - timedelta(seconds=window):
                within_grace += 1
                continue
            async with self.repository.transaction() as tx:
                if await tx.list_artifact_references(
                    org_id=org_id,
                    project_id=project_id,
                    uri=record.uri,
                    active_only=True,
                ):
                    referenced += 1
                    continue
            if dry_run:
                collected.append(record.uri)
                reclaimed += record.size_bytes
                continue
            removed = await asyncio.to_thread(
                self.artifact_store.delete,
                record.uri,
                org_id=record.org_id,
                project_id=record.project_id,
            )
            async with self.repository.transaction() as tx:
                current = await tx.get_managed_artifact(org_id, project_id, record.uri)
                if current is None:
                    continue
                held_after_delete = current.hold is not None and current.hold.active_at(utcnow())
                if held_after_delete:
                    # Someone claimed it between the check and the delete. The
                    # bytes are already gone, so the honest move is to drop the
                    # index row and let the hold's owner find it missing rather
                    # than leave a row pointing at nothing.
                    logger.warning("artifact %s was held after its bytes were removed", record.uri)
                await tx.put_managed_artifact(current.model_copy(update={"size_bytes": 0, "updated_at": utcnow()}))
                if principal is not None:
                    # In the same transaction as the index update, so a deletion
                    # cannot be unaudited: bytes removed with no ledger entry is
                    # exactly the gap that makes a loss unexplainable later.
                    await self._audit_scoped(
                        tx,
                        principal,
                        org_id=record.org_id,
                        project_id=record.project_id,
                        entity_type="artifact",
                        entity_id=record.uri[:256],
                        action="artifact.collected",
                        payload={
                            "kind": str(record.kind),
                            "size_bytes": record.size_bytes,
                            "removed": removed,
                            "held_after_delete": held_after_delete,
                        },
                    )
            if removed:
                collected.append(record.uri)
                reclaimed += record.size_bytes
        return ArtifactCollection(
            collected_uris=tuple(collected),
            reclaimed_bytes=reclaimed,
            held=held,
            referenced=referenced,
            within_grace=within_grace,
            dry_run=dry_run,
        )

    async def artifact_references(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        uri: str,
        active_only: bool = False,
    ) -> list[ArtifactReferenceRecord]:
        """List the workloads that claim one scoped artifact.

        The requested scope is authorized before lookup. The same URI in another
        project is a different artifact and neither its existence nor its
        references are discoverable through this request.
        """

        self.authorizer.require(principal, Permission.READ, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            record = await tx.get_managed_artifact(org_id, project_id, uri)
            if record is None:
                raise NotFoundError(f"artifact {uri!r} is not indexed in {org_id}/{project_id}")
            return await tx.list_artifact_references(
                org_id=org_id,
                project_id=project_id,
                uri=uri,
                active_only=active_only,
            )

    async def record_progress_report(
        self,
        *,
        workload_id: str,
        rank: int,
        attempt: int,
        step: int | None = None,
        total: int | None = None,
        message: str = "",
        metrics: tp.Mapping[str, float] | None = None,
        now: datetime | None = None,
    ) -> UserProgressReport:
        """Record one rank's latest self-reported progress.

        Authenticated by the *artifact token*, not a principal -- the exact
        pattern the result-upload route already uses, because the caller is a
        running attempt rather than an identity the IdP issued.

        A report from a stale attempt is dropped rather than stored. Attempt 1
        finishing its last step after attempt 2 has started would otherwise walk
        the displayed progress backwards, which reads as a job that lost work.
        """

        moment = now or utcnow()
        async with self.repository.transaction() as tx:
            workload = await self._require_workload(tx, workload_id)
            if attempt < workload.attempt:
                existing = await tx.list_progress_reports(workload_id)
                return next(
                    (item for item in existing if item.rank == rank),
                    UserProgressReport(rank=rank, attempt=workload.attempt),
                )
            report = UserProgressReport(
                rank=rank,
                attempt=attempt,
                step=step,
                total=total,
                message=message,
                metrics=dict(metrics or {}),
                reported_at=moment,
            )
            await tx.put_progress_report(workload_id, report)
            return report

    async def list_progress_reports(self, principal: Principal, workload_id: str) -> list[UserProgressReport]:
        """Return every rank's latest report for one workload."""

        async with self.repository.transaction() as tx:
            workload = await self._require_workload(tx, workload_id)
            self.authorizer.require(principal, Permission.READ, org_id=workload.org_id, project_id=workload.project_id)
            reports = await tx.list_progress_reports(workload_id)
        # Reports from a superseded attempt are withheld rather than deleted: the
        # row is overwritten on the next report from that rank, and showing an old
        # attempt's step beside a new one's would be the backwards-walking display
        # the write path already refuses to create.
        return [report for report in reports if report.attempt >= workload.attempt]

    async def record_artifact_access(
        self,
        *,
        org_id: str,
        project_id: str,
        workload_id: str,
        kind: str,
        object_id: str,
        action: str,
        tx: RepositorySession | None = None,
    ) -> None:
        """Audit one capability-authenticated artifact transfer, granted or refused.

        The actor is a *workload*, not a principal: these three routes are the only
        ones in the API that authenticate a running attempt via
        ``X-Eyvan-Artifact-Token`` instead of an identity, so there is no ``Principal``
        to attribute. Recording the workload id keeps the ledger truthful about that.

        Pass ``tx`` to write the event in the caller's transaction. The result-upload
        route does, so a granted write cannot be committed unaudited; the read routes
        do not open one at all, and for a read there is no state change to be atomic
        with -- the event records only that a token was presented and how it was
        answered.
        """

        event = AuditEvent(
            id=_new_id("evt"),
            actor=f"workload:{workload_id}",
            org_id=org_id,
            project_id=project_id,
            entity_type="artifact",
            entity_id=f"{kind}/{object_id}"[:256],
            action=action,
            payload={"workload_id": workload_id, "kind": kind},
        )
        if tx is not None:
            await tx.append_event(event)
            await self._touch_artifact(
                tx,
                org_id=org_id,
                project_id=project_id,
                kind=kind,
                object_id=object_id,
            )
            return
        async with self.repository.transaction() as transaction:
            await transaction.append_event(event)
            await self._touch_artifact(
                transaction,
                org_id=org_id,
                project_id=project_id,
                kind=kind,
                object_id=object_id,
            )

    async def record_secret_issuance(
        self,
        *,
        org_id: str,
        project_id: str,
        workload_id: str,
        attempt: int,
        generation: int,
        dispatch_reservation: WorkloadDispatchReservation | None,
        names: tuple[str, ...],
        tx: RepositorySession | None = None,
    ) -> None:
        """Record which secret mounts one attempt was granted capabilities for.

        Half of a pair. The other half is written when a capability is redeemed,
        and it is the pairing that carries the signal: a ``secret.read`` for an
        attempt with no ``secret.issued`` naming that mount is a capability being
        used outside the dispatch that minted it. Recording only reads would make
        that indistinguishable from ordinary use.

        Mount **names**, never references and never values. A reference can name
        a production database; the ledger is read by more people than the spec
        is.
        """

        if names and dispatch_reservation is None:
            raise RuntimeError("secret issuance requires a durable dispatch reservation")
        event = AuditEvent(
            id=_new_id("evt"),
            # A workload, not a principal: the dispatch that mints these
            # capabilities is the control plane acting on the workload's behalf,
            # and attributing it to whoever submitted would put a human's name on
            # an event they were not present for.
            actor=f"workload:{workload_id}",
            org_id=org_id,
            project_id=project_id,
            entity_type="workload",
            entity_id=workload_id,
            action="secret.issued",
            payload={
                "workload_id": workload_id,
                "attempt": attempt,
                "generation": generation,
                "outbox_id": None if dispatch_reservation is None else dispatch_reservation.outbox_id,
                "outbox_attempt": None if dispatch_reservation is None else dispatch_reservation.outbox_attempt,
                "names": list(names),
            },
        )
        if tx is not None:
            await tx.append_event(event)
            return
        async with self.repository.transaction() as transaction:
            await transaction.append_event(event)

    async def secret_redemption_scope(self, workload_id: str) -> tuple[str, str] | None:
        """Resolve an audit/verification scope from a durable workload.

        Query-string scope is unauthenticated on the capability endpoint.  It
        must never become an audit scope merely because a caller supplied it.
        """

        async with self.repository.transaction() as tx:
            workload = await tx.get_workload(workload_id)
        if workload is None:
            return None
        return workload.org_id, workload.project_id

    async def record_secret_rejection(
        self,
        *,
        workload_id: str,
        reason: str,
        verified_name: str | None = None,
    ) -> None:
        """Audit a rejected capability only against a real workload record.

        ``verified_name`` may come only from signed claims.  Invalid tokens have
        no trustworthy mount name, so their event intentionally omits one.
        """

        safe_reason = (
            reason
            if reason in {"expired", "invalid_capability", "mount_mismatch", "out_of_scope"}
            else "invalid_capability"
        )
        async with self.repository.transaction() as tx:
            workload = await tx.get_workload(workload_id)
            if workload is None:
                return
            payload: JsonObject = {"workload_id": workload.id, "reason": safe_reason}
            if verified_name is not None:
                payload["name"] = verified_name
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=f"workload:{workload.id}",
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    entity_type="workload",
                    entity_id=workload.id,
                    action="secret.token_rejected",
                    payload=payload,
                )
            )

    async def authorize_secret_mount(
        self,
        *,
        org_id: str,
        project_id: str,
        workload_id: str,
        attempt: int,
        generation: int,
        dispatch_fence: str,
        name: str,
    ) -> tuple[str, str]:
        """Return ``(reference, matched policy root)`` for one issued dispatch.

        A workload id and attempt are not enough: an outbox retry can mint a
        second set of capabilities without advancing either.  The signed fence
        must match the exact durable reservation currently attached to the
        workload, its generation, and every lease in that reservation.  While
        the workload is still LEASED (the synchronous submit callback window),
        the originating outbox claim must also remain live.
        """

        async with self.repository.transaction() as tx:
            workload = await tx.get_workload(workload_id)
            if workload is None:
                raise AuthorizationError("secret mount is not current for this workload dispatch")
            reference = ""
            allowed_root: str | None = None
            granted = False
            reservation = workload.dispatch_reservation
            lease_ids = workload.lease_ids or (() if workload.lease_id is None else (workload.lease_id,))
            exact_reservation = bool(
                reservation is not None
                and reservation.fence == dispatch_fence
                and reservation.generation == generation == workload.generation
                and reservation.workload_attempt == attempt == workload.attempt
                and reservation.lease_ids == lease_ids
            )
            leases = tuple([await tx.get_lease(lease_id) for lease_id in lease_ids]) if exact_reservation else ()
            exact_leases = bool(
                leases
                and all(
                    lease is not None and lease.workload_id == workload.id and lease.generation == workload.generation
                    for lease in leases
                )
            )
            live_state = False
            if exact_reservation and exact_leases and reservation is not None:
                if workload.state is WorkloadState.LEASED:
                    command = await tx.get_outbox(reservation.outbox_id)
                    persisted_reservation: WorkloadDispatchReservation | None = None
                    if command is not None:
                        try:
                            persisted_reservation = WorkloadDispatchReservation.model_validate(
                                command.payload.get("dispatch_reservation")
                            )
                        except Exception:
                            persisted_reservation = None
                    live_state = bool(
                        command is not None
                        and command.topic == "workload.dispatch"
                        and command.aggregate_type == "workload"
                        and command.aggregate_id == workload.id
                        and command.generation == workload.generation
                        and command.attempts == reservation.outbox_attempt
                        and command.state is OutboxState.PROCESSING
                        and command.claimed_until is not None
                        and command.claimed_until > utcnow()
                        and persisted_reservation == reservation
                        and all(
                            lease is not None and lease.state in {LeaseState.PENDING, LeaseState.QUARANTINED}
                            for lease in leases
                        )
                    )
                elif workload.state in {
                    WorkloadState.DISPATCHING,
                    WorkloadState.RUNNING,
                    WorkloadState.CHECKPOINTING,
                }:
                    live_state = bool(
                        workload.remote_id
                        and all(lease is not None and lease.state is LeaseState.ACTIVE for lease in leases)
                    )
            if workload.org_id == org_id and workload.project_id == project_id and live_state:
                candidate = workload.spec.secret_mounts.get(name, "")
                if candidate:
                    policy = await tx.get_policy(workload.org_id, workload.project_id)
                    allowed = () if policy is None else policy.allowed_secret_references
                    allowed_root = matching_secret_reference_root(candidate, allowed)
                    if allowed_root is not None:
                        reference, granted = candidate, True
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=f"workload:{workload.id}",
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    entity_type="workload",
                    entity_id=workload.id,
                    action="secret.read" if granted else "secret.read_denied",
                    payload={
                        "workload_id": workload.id,
                        "attempt": attempt,
                        "generation": generation,
                        "name": name,
                    },
                )
            )
        if not granted or allowed_root is None:
            raise AuthorizationError("secret mount is not current for this workload dispatch")
        return reference, allowed_root

    @staticmethod
    async def _touch_artifact(
        tx: RepositorySession,
        *,
        org_id: str,
        project_id: str,
        kind: str,
        object_id: str,
    ) -> None:
        """Stamp an indexed artifact's last access, if it is indexed at all.

        Best-effort by construction: an artifact stored before this index existed
        has no row, and a read must not fail because the ledger is incomplete.
        The stamp informs a human deciding what looks abandoned -- it is never a
        liveness signal, because a queued workload can reference an artifact that
        nothing has read for months.
        """

        try:
            uri = ManagedArtifactRef(ManagedArtifactKind(kind), object_id).uri
        except ValueError:
            return
        record = await tx.get_managed_artifact(org_id, project_id, uri)
        if record is None:
            return
        await tx.put_managed_artifact(record.model_copy(update={"last_accessed_at": utcnow()}))

    async def _audit_scoped(
        self,
        tx: RepositorySession,
        principal: Principal,
        *,
        org_id: str | None,
        project_id: str | None,
        entity_type: str,
        entity_id: str,
        action: str,
        previous_state: str | None = None,
        new_state: str | None = None,
        payload: JsonObject | None = None,
    ) -> AuditEvent:
        return await tx.append_event(
            AuditEvent(
                id=_new_id("evt"),
                actor=principal.subject,
                org_id=org_id,
                project_id=project_id,
                entity_type=entity_type,
                entity_id=entity_id,
                action=action,
                previous_state=previous_state,
                new_state=new_state,
                payload=payload or {},
            )
        )

    async def _outbox(
        self,
        tx: RepositorySession,
        *,
        topic: str,
        aggregate_type: str,
        aggregate_id: str,
        idempotency_key: str,
        generation: int,
        payload: JsonObject,
    ) -> OutboxRecord:
        record = OutboxRecord(
            id=_new_id("cmd"),
            topic=topic,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            idempotency_key=idempotency_key,
            generation=generation,
            payload=payload,
        )
        await tx.put_outbox(record)
        return record
