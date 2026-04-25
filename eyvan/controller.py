# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Leader-only scheduling, outbox delivery, and cell observation loops."""

from __future__ import annotations

import asyncio
import logging
import secrets
import shlex
import socket
import time
import typing as tp
import uuid
import weakref
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta

from pydantic import BaseModel, ConfigDict, Field, RootModel, ValidationError, model_validator

from eyvan._compat import StrEnum
from eyvan._json import JsonObject, model_json
from eyvan.agent.client import (
    CellAgentBusyError,
    CellAgentCapacityProviderError,
    CellAgentClient,
    CellAgentInvalidConfigurationError,
    CellAgentStaleMutationError,
    CellAgentUnavailableError,
    CellAgentUnsupportedCapabilityError,
)
from eyvan.agent.models import (
    CapacityRequest,
    CheckpointRequest,
    GangWorkloadMember,
    GangWorkloadMutationRequest,
    GangWorkloadRequest,
    MutationModel,
    WorkloadMutationRequest,
    WorkloadRequest,
)
from eyvan.artifacts.transfers import ManagedArtifactAccess
from eyvan.capacity import (
    DEFINITIVE_CAPACITY_LOSS_STATUSES,
    PENDING_CAPACITY_STATUSES,
    QUEUED_CAPACITY_STATUSES,
    READY_CAPACITY_STATUSES,
    CapacityStatus,
)
from eyvan.capacity.audit import CapacityLifecycleEvent, CapacityLifecycleLogger, CapacityReplacementReason
from eyvan.capacity.planning import (
    CapacityPlanner,
    CapacitySimulation,
    cell_execution_runtime_is_pending,
    cell_matches_pool,
    cell_requires_replacement,
    next_pool_cell_index,
    pool_cell_id,
)
from eyvan.config import EyvanSettings
from eyvan.core import (
    CapacityBelief,
    CapacityPoolKey,
    PlacementDecision,
    PreemptionActionKind,
    PreemptionPlan,
    ProvisionOutcome,
    SchedulingResult,
)
from eyvan.drivers.base import CapacityProviderFailureCode, ExecutionWorkloadStatus
from eyvan.identifiers import stable_id
from eyvan.observability import EyvanMetrics
from eyvan.persistence.base import Repository, RepositorySession
from eyvan.service.control_plane import ControlPlaneService, storage_scope
from eyvan.service.errors import ConflictError, InputError, NotFoundError
from eyvan.service.models import (
    AuditEvent,
    AutoscaleBlockReason,
    CapacityBeliefState,
    CapacityPoolRecord,
    CellRecord,
    CellState,
    CoordinationKind,
    GcpQueueAdmissionDenialTarget,
    LeaseRecord,
    LeaseState,
    OutboxRecord,
    OutboxState,
    Principal,
    ResourceGang,
    ResourceHours,
    Role,
    RoleGrant,
    WorkloadRecord,
    WorkloadState,
    WorkspaceRef,
    utcnow,
)
from eyvan.service.provider_diagnostics import safe_provider_diagnostic
from eyvan.service.state import ACTIVE_LEASE_STATES, TERMINAL_WORKLOAD_STATES

logger = logging.getLogger(__name__)

_SHARED_TPU_QUOTA_SCHEDULING_REASON = "shared Spot TPU quota cannot fit this job's requested complete worker capacity"

_PREEMPTION_TERMINAL_WORKLOAD_STATUSES = frozenset(
    {
        ExecutionWorkloadStatus.STOPPED,
        ExecutionWorkloadStatus.FAILED,
        ExecutionWorkloadStatus.SUCCEEDED,
        ExecutionWorkloadStatus.LOST,
        ExecutionWorkloadStatus.NOT_FOUND,
    }
)
_RECOVERABLE_WORKLOAD_STATUSES = frozenset({ExecutionWorkloadStatus.PENDING, ExecutionWorkloadStatus.RUNNING})
_PREEMPTION_OWNED_TERMINAL_WORKLOAD_STATUSES = frozenset(
    {
        ExecutionWorkloadStatus.SUCCEEDED,
        ExecutionWorkloadStatus.FAILED,
        ExecutionWorkloadStatus.STOPPED,
        ExecutionWorkloadStatus.LOST,
        ExecutionWorkloadStatus.NOT_FOUND,
    }
)
_RETRIABLE_OUTBOX_STATES = frozenset({OutboxState.PENDING, OutboxState.PROCESSING, OutboxState.FAILED})
_ACTIVE_CAPACITY_APPLY_STATES = _RETRIABLE_OUTBOX_STATES
_CAPACITY_APPLY_PROVIDER_RPC_STARTED_KEY = "provider_rpc_started"


class _FleetCapacityGate:
    """Allow concurrent cell work while a fleet mutation briefly closes admission."""

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._writer_lock = asyncio.Lock()
        self._closing = False
        self._active_mutations = 0

    @asynccontextmanager
    async def operation(self) -> tp.AsyncIterator[None]:
        """Admit one normal cell operation unless a fleet mutation is closing."""

        async with self._condition:
            await self._condition.wait_for(lambda: not self._closing)
        yield

    @asynccontextmanager
    async def mutation(self) -> tp.AsyncIterator[None]:
        """Serialize a short autoscaler state write with fleet configuration changes.

        Provider RPCs use :meth:`operation` so fleet shutdown never waits for
        a long cloud request. This narrower scope covers only transactional
        autoscaler writes that must not race a pool-template replacement.
        """

        async with self._condition:
            await self._condition.wait_for(lambda: not self._closing)
            self._active_mutations += 1
        try:
            yield
        finally:
            async with self._condition:
                self._active_mutations -= 1
                if self._active_mutations == 0:
                    self._condition.notify_all()

    @asynccontextmanager
    async def closing(self) -> tp.AsyncIterator[None]:
        """Block new cell work while a fleet mutation is persisted."""

        async with self._writer_lock:
            is_closing = False
            try:
                async with self._condition:
                    self._closing = True
                    is_closing = True
                    await self._condition.wait_for(lambda: self._active_mutations == 0)
                yield
            finally:
                if is_closing:
                    async with self._condition:
                        self._closing = False
                        self._condition.notify_all()


class StaleOutboxCommand(RuntimeError):
    """An at-least-once command no longer owns the current aggregate generation."""


class CapacityApplySuperseded(StaleOutboxCommand):
    """A capacity apply no longer has a live desired cell to provision."""

    def __init__(self, message: str, *, provider_diagnostic: str | None = None) -> None:
        super().__init__(message)
        self.provider_diagnostic = provider_diagnostic


class PreemptionCheckpointError(RuntimeError):
    """A preemption checkpoint phase failed before any victim was stopped."""


class _PreemptionVictimOutcome(StrEnum):
    """Durable settlement result for one preemption victim."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PREEMPTED = "preempted"


class _ProviderPhaseGroup(StrEnum):
    """Groups provider phases whose timeout budget must be continuous."""

    REQUESTING = "REQUESTING"
    QUEUED = "QUEUED"
    PROVISIONING = "PROVISIONING"


class _PreemptionVictim(BaseModel):
    """Strict durable identity and rollback state for one preemption victim."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    workload_id: str = Field(min_length=1, max_length=96)
    lease_id: str = Field(min_length=1, max_length=96)
    cell_id: str = Field(min_length=1, max_length=96)
    generation: int = Field(ge=0, strict=True)
    checkpoint: bool = Field(strict=True)
    grace_period_seconds: int = Field(ge=0, strict=True)
    hook: str | None
    output_uri: str | None
    workload_state: WorkloadState
    lease_state: LeaseState

    @model_validator(mode="after")
    def validate_restore_states(self) -> _PreemptionVictim:
        if self.workload_state not in {
            WorkloadState.LEASED,
            WorkloadState.DISPATCHING,
            WorkloadState.RUNNING,
        }:
            raise ValueError("preemption victim has an invalid workload restore state")
        if self.lease_state not in {LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.QUARANTINED}:
            raise ValueError("preemption victim has an invalid lease restore state")
        return self


class _DispatchLeaseMembership(BaseModel):
    """Validated lease membership that a workload-dispatch command may submit."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    lease_id: str = Field(min_length=1, max_length=96)
    lease_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_membership(self) -> _DispatchLeaseMembership:
        if not self.lease_ids:
            return self
        if len(self.lease_ids) < 2:
            raise ValueError("dispatch lease_ids must contain a complete group")
        if len(self.lease_ids) != len(set(self.lease_ids)):
            raise ValueError("dispatch lease_ids must not contain duplicates")
        if self.lease_ids[0] != self.lease_id:
            raise ValueError("dispatch leader lease must be the first group member")
        return self

    @property
    def ids(self) -> tuple[str, ...]:
        """Return the actual single-cell or group dispatch membership."""

        return self.lease_ids or (self.lease_id,)


class _PreemptionCheckpointUris(RootModel[dict[str, str | None]]):
    """Validated durable checkpoint outputs keyed by victim workload ID."""


class _PreemptionVictimOutcomes(RootModel[dict[str, _PreemptionVictimOutcome]]):
    """Validated durable terminal outcomes keyed by victim lease ID."""


class _PreemptionStoppedVictims(RootModel[list[str]]):
    """Validated durable ordered set of stopped victim lease IDs."""


#: Workload states that hold, or are about to hold, real capacity. Shared so the
#: observation loop and the orphan sweep cannot drift apart -- a state one considers
#: live and the other does not is a workload nothing ever settles.
_LIVE_WORKLOAD_STATES: tp.Final[frozenset[WorkloadState]] = frozenset(
    {
        WorkloadState.LEASED,
        WorkloadState.DISPATCHING,
        WorkloadState.RUNNING,
        WorkloadState.CHECKPOINTING,
        WorkloadState.CANCELLING,
    }
)


@dataclass(frozen=True, slots=True)
class LeaderPhase:
    """One step of the leader cycle, with the policy it needs to run.

    Interval semantics differ per phase and are therefore declared per phase rather
    than inferred: a zero ``cell_reconcile_interval_s`` has always meant "every
    cycle", while a zero ``retention_interval_s`` is documented as "disabled".
    """

    name: str
    run: tp.Callable[[], tp.Awaitable[object]]
    #: None runs the phase every cycle; a number gives it its own slower cadence.
    interval_s: float | None = None
    zero_interval_disables: bool = False
    #: Housekeeping phases log and continue; phases the fleet depends on propagate.
    isolate_failures: bool = False


@dataclass(frozen=True, slots=True)
class _LaunchedCheckpointHook:
    """A checkpoint hook unit this attempt started and therefore owns.

    In-memory only, and deliberately so: it is scoped to a single execution of
    the checkpointing phase, which is the only window in which an orphaned hook
    can exist. A crash mid-phase leaves the plan in ``checkpointing`` for the
    next leader to re-drive, and the hook's own fence rejects a stale writer.
    """

    cell: CellRecord
    remote_id: str
    lease_id: str
    generation: int
    fencing_token: str


class LeaderElector(tp.Protocol):
    """Exclusive ownership for global scheduling decisions."""

    async def acquire(self) -> bool: ...

    async def release(self) -> None: ...


class SingleProcessLeader:
    """Leader implementation for the in-memory development repository."""

    async def acquire(self) -> bool:
        return True

    async def release(self) -> None:
        return None


def _system_principal() -> Principal:
    return Principal(subject="eyvan-controller", grants=(RoleGrant(role=Role.FLEET_ADMIN),))


def _api_resources(resources) -> ResourceHours:
    values = {name: float(value) for name, value in resources.items()}
    known = {name: values.pop(name, 0) for name in ("cpu", "memory_gib", "gpu", "tpu")}
    return ResourceHours(**known, custom=values)


def _provider_phase_group(
    phase: CapacityStatus | None,
) -> CapacityStatus | _ProviderPhaseGroup | None:
    if phase is CapacityStatus.REQUESTING:
        return _ProviderPhaseGroup.REQUESTING
    if phase in QUEUED_CAPACITY_STATUSES:
        return _ProviderPhaseGroup.QUEUED
    if phase in PENDING_CAPACITY_STATUSES:
        return _ProviderPhaseGroup.PROVISIONING
    return phase


def _provider_phase_start(cell: CellRecord, remote_state: CapacityStatus, observed_at: datetime) -> datetime:
    if cell.provider_phase_started_at is not None and _provider_phase_group(
        cell.provider_phase
    ) == _provider_phase_group(remote_state):
        return cell.provider_phase_started_at
    return observed_at


def _replacement_reason(cell: CellRecord, pool: CapacityPoolRecord) -> CapacityReplacementReason:
    """Return why a non-supplying cell must be retired before replacement."""

    if cell.provider_phase in DEFINITIVE_CAPACITY_LOSS_STATUSES:
        return CapacityReplacementReason.PROVIDER_LOST
    if cell_requires_replacement(cell, pool):
        return CapacityReplacementReason.TEMPLATE_CHANGED
    if cell.state is CellState.REGISTERING:
        return CapacityReplacementReason.PROVISIONING_TIMEOUT
    return CapacityReplacementReason.UNHEALTHY


class EyvanController:
    """Turn pure scheduler decisions into fenced, recoverable backend actions."""

    def __init__(
        self,
        repository: Repository,
        service: ControlPlaneService,
        *,
        settings: EyvanSettings | None = None,
        leader: LeaderElector | None = None,
        agent_factory: tp.Callable[[CellRecord], CellAgentClient] | None = None,
        metrics: EyvanMetrics | None = None,
        artifact_access: ManagedArtifactAccess | None = None,
    ):
        self.repository = repository
        self.service = service
        self.settings = settings or EyvanSettings.from_env()
        self.leader = leader or SingleProcessLeader()
        self.worker_id = f"{socket.gethostname()}:{uuid.uuid4().hex[:12]}"
        self._principal = _system_principal()
        self._agent_clients: dict[str, CellAgentClient] = {}
        self._owns_agent_clients = agent_factory is None
        self._agent_factory = agent_factory or self._default_agent
        self._leader_cycle_lock = asyncio.Lock()
        self._fleet_capacity_gates: weakref.WeakValueDictionary[str, _FleetCapacityGate] = weakref.WeakValueDictionary()
        self._cell_capacity_locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()
        self.metrics = metrics
        self.artifact_access = artifact_access
        self._capacity_lifecycle = CapacityLifecycleLogger(logger)

    def _default_agent(self, cell: CellRecord) -> CellAgentClient:
        if not cell.endpoint:
            raise RuntimeError(f"cell {cell.id!r} has no cell-agent endpoint")
        key = f"{cell.endpoint}\0{self.settings.agent_shared_secret or ''}"
        if key not in self._agent_clients:
            self._agent_clients[key] = CellAgentClient(cell.endpoint, token=self.settings.agent_shared_secret)
        return self._agent_clients[key]

    async def close(self) -> None:
        """Release cached HTTP pools and scheduler leadership."""

        if self._owns_agent_clients:
            clients, self._agent_clients = list(self._agent_clients.values()), {}
            for client in clients:
                await asyncio.to_thread(client.close)
        await self.leader.release()

    def _fleet_capacity_gate(self, fleet_name: str) -> _FleetCapacityGate:
        """Return the admission gate for one fleet's capacity operations."""

        gate = self._fleet_capacity_gates.get(fleet_name)
        if gate is None:
            gate = _FleetCapacityGate()
            self._fleet_capacity_gates[fleet_name] = gate
        return gate

    def _cell_capacity_lock(self, cell_id: str) -> asyncio.Lock:
        """Return the lifecycle gate for one cell."""

        lock = self._cell_capacity_locks.get(cell_id)
        if lock is None:
            lock = asyncio.Lock()
            self._cell_capacity_locks[cell_id] = lock
        return lock

    async def _fleet_name_for_cell(self, cell: CellRecord) -> str | None:
        """Resolve the fleet that owns a managed cell, if any."""

        if cell.pool_id is None:
            return None
        async with self.repository.transaction() as transaction:
            pool = await transaction.get_capacity_pool(cell.pool_id)
        return None if pool is None else pool.fleet_name

    @asynccontextmanager
    async def _capacity_operation(self, cell: CellRecord) -> tp.AsyncIterator[None]:
        """Serialize one cell while admitting concurrent operations in its fleet."""

        fleet_name = await self._fleet_name_for_cell(cell)
        cell_lock = self._cell_capacity_lock(cell.id)
        if fleet_name is None:
            async with cell_lock:
                yield
            return
        async with self._fleet_capacity_gate(fleet_name).operation():
            async with cell_lock:
                yield

    async def reconcile_if_leader(self) -> SchedulingResult | None:
        """Run a manual reconciliation only on the elected scheduler replica.

        Leadership is re-checked *before every phase*, not once at the top. A
        full cycle spans many provider round trips, so a replica can lose the
        advisory lock partway through; continuing would let two leaders mint
        autoscaler cell ids from the same ``len(managed) + 1`` sequence and
        collide. Losing leadership mid-cycle is a normal outcome, so it returns
        ``None`` rather than raising.
        """

        if not await self.leader.acquire():
            return None
        async with self._leader_cycle_lock:
            for phase in (
                self.reconcile_unmanaged_capacity_recoveries_once,
                self.reconcile_capacity_pools_once,
                self._observe_cells_once,
                self.quarantine_expired_leases_once,
                self.observe_workloads_once,
            ):
                if not await self.leader.acquire():
                    return None
                await phase()
            if not await self.leader.acquire():
                return None
            return await self.reconcile_once()

    async def disable_fleet(
        self,
        principal: Principal,
        fleet_name: str,
        *,
        idempotency_key: str,
    ) -> tuple[list[CapacityPoolRecord], bool]:
        """Persist fleet shutdown before any newly admitted capacity operation can start."""

        async with self._fleet_capacity_gate(fleet_name).closing():
            return await self.service.disable_fleet(principal, fleet_name, idempotency_key=idempotency_key)

    async def apply_fleet(
        self,
        principal: Principal,
        fleet_name: str,
        pools: tp.Sequence[CapacityPoolRecord],
        *,
        idempotency_key: str,
    ) -> tuple[list[CapacityPoolRecord], bool]:
        """Apply one fleet after closing its capacity-operation admission gate."""

        async with self._fleet_capacity_gate(fleet_name).closing():
            return await self.service.apply_fleet(principal, fleet_name, pools, idempotency_key=idempotency_key)

    async def upsert_capacity_pool(
        self,
        principal: Principal,
        pool: CapacityPoolRecord,
        *,
        idempotency_key: str,
    ) -> tuple[CapacityPoolRecord, bool]:
        """Update one pool after closing its fleet's capacity-operation admission gate."""

        async with self._fleet_capacity_gate(pool.fleet_name).closing():
            return await self.service.upsert_capacity_pool(principal, pool, idempotency_key=idempotency_key)

    async def disable_capacity_pool(
        self,
        principal: Principal,
        pool_id: str,
        *,
        idempotency_key: str,
    ) -> tuple[CapacityPoolRecord, bool]:
        """Disable one pool after closing its fleet's capacity-operation admission gate."""

        async with self.repository.transaction() as transaction:
            pool = await transaction.get_capacity_pool(pool_id)
        if pool is None:
            return await self.service.disable_capacity_pool(principal, pool_id, idempotency_key=idempotency_key)
        async with self._fleet_capacity_gate(pool.fleet_name).closing():
            return await self.service.disable_capacity_pool(principal, pool_id, idempotency_key=idempotency_key)

    async def drain_cell(
        self,
        principal: Principal,
        cell_id: str,
        *,
        idempotency_key: str,
    ) -> tuple[CellRecord, bool]:
        """Persist a cell drain while serializing that cell's future capacity work."""

        cell = await self.service.get_cell(principal, cell_id)
        async with self._capacity_operation(cell):
            return await self.service.drain_cell(principal, cell_id, idempotency_key=idempotency_key)

    async def delete_cell(
        self,
        principal: Principal,
        cell_id: str,
        *,
        idempotency_key: str,
    ) -> tuple[CellRecord, bool]:
        """Persist a cell release while serializing that cell's future capacity work."""

        cell = await self.service.get_cell(principal, cell_id)
        async with self._capacity_operation(cell):
            return await self.service.delete_cell(principal, cell_id, idempotency_key=idempotency_key)

    async def _autoscale_upsert_cell(
        self,
        pool: CapacityPoolRecord,
        cell_id: str,
        worker_count: int,
        *,
        autoscale_request_workload_id: str | None = None,
        idempotency_key: str,
    ) -> tuple[CellRecord | None, bool]:
        """Upsert one planned cell only while its pool template remains current."""

        async with self._fleet_capacity_gate(pool.fleet_name).mutation():
            async with self.repository.transaction() as transaction:
                current_pool = await transaction.get_capacity_pool(pool.id)
            if current_pool is None or not current_pool.enabled or current_pool.generation != pool.generation:
                return None, False
            return await self.service.upsert_cell(
                self._principal,
                current_pool.make_cell(
                    cell_id,
                    worker_count=worker_count,
                    autoscale_request_workload_id=autoscale_request_workload_id,
                ),
                idempotency_key=idempotency_key,
            )

    async def _autoscale_requeue_unprovisioned_cell(
        self,
        pool: CapacityPoolRecord,
        cell: CellRecord,
    ) -> tuple[CellRecord | None, bool]:
        """Restore a current-template registering cell whose apply command was superseded."""

        async with self._fleet_capacity_gate(pool.fleet_name).mutation():
            async with self.repository.transaction() as transaction:
                current_pool = await transaction.get_capacity_pool(pool.id)
                current_cell = await transaction.get_cell(cell.id)
            if (
                current_pool is None
                or not current_pool.enabled
                or current_pool.generation != pool.generation
                or current_cell is None
                or current_cell.generation != cell.generation
                or not cell_matches_pool(current_cell, current_pool)
                or current_cell.state is not CellState.REGISTERING
                or current_cell.provider_phase is not None
            ):
                return None, False
            return await self.service.requeue_unprovisioned_cell(
                self._principal,
                current_cell.id,
                expected_generation=current_cell.generation,
            )

    async def reconcile_once(self) -> SchedulingResult:
        """Schedule one durable snapshot and commit its plans."""

        started = time.perf_counter()
        result = await self.service.schedule_once(self._principal)
        if self.metrics is not None:
            self.metrics.scheduler_duration.observe(time.perf_counter() - started)
            for _ in result.placements:
                self.metrics.scheduler_decisions.labels(outcome="placed", reason="ready_capacity").inc()
            for _ in result.preemptions:
                self.metrics.scheduler_decisions.labels(outcome="preempt", reason="priority").inc()
            for failure in result.infeasibilities:
                self.metrics.scheduler_decisions.labels(outcome="pending", reason=failure.code.value).inc()
        for decision in result.placements:
            if not await self.leader.acquire():
                return result
            await self._commit_placement(decision)
        for plan in result.preemptions:
            if not await self.leader.acquire():
                return result
            await self._commit_preemption(plan)
        await self._record_infeasibilities(result)
        return result

    async def _release_inactive_autoscale_request_ownership(
        self,
        cells: list[CellRecord],
    ) -> list[CellRecord]:
        """Return source-attributed capacity to shared supply after its owner stops waiting.

        A fresh autoscaled cell is reserved only while the workload that caused
        it to be requested remains pending. Lease creation clears the
        reservation atomically. This recovery path handles cancellation,
        terminal failure, and deleted legacy workloads before a lease exists.
        """

        candidates = tuple(cell for cell in cells if cell.autoscale_request_workload_id is not None)
        if not candidates:
            return cells

        released_by_id: dict[str, CellRecord] = {}
        async with self.repository.transaction() as transaction:
            for snapshot in candidates:
                current = await transaction.get_cell(snapshot.id)
                if (
                    current is None
                    or current.generation != snapshot.generation
                    or current.autoscale_request_workload_id != snapshot.autoscale_request_workload_id
                ):
                    continue
                owner_id = current.autoscale_request_workload_id
                assert owner_id is not None
                owner = await transaction.get_workload(owner_id)
                if owner is not None and owner.state is WorkloadState.PENDING:
                    continue
                released = current.model_copy(
                    update={
                        "autoscale_request_workload_id": None,
                        "updated_at": utcnow(),
                    }
                )
                await transaction.put_cell(released)
                released_by_id[released.id] = released
                logger.info(
                    "releasing autoscale reservation on cell %s because workload %s is no longer pending",
                    released.id,
                    owner_id,
                )
        return [released_by_id.get(cell.id, cell) for cell in cells]

    async def reconcile_capacity_pools_once(self) -> dict[str, int]:
        """Reconcile declarative pools against global submitted workload demand."""

        if self.service.scheduler is None:
            return {"launched": 0, "drained": 0, "deleted": 0}
        async with self.repository.transaction() as transaction:
            pools = await transaction.list_capacity_pools()
            if not pools:
                return {"launched": 0, "drained": 0, "deleted": 0}
            workloads = await transaction.list_workloads()
            cells = await transaction.list_cells()
            leases = await transaction.list_leases()
            budgets = await transaction.list_budgets()
            quotas = await transaction.list_quotas()
            policies = await transaction.list_policies()

        cells = await self._release_inactive_autoscale_request_ownership(cells)
        pools_by_id = {pool.id: pool for pool in pools}
        now = utcnow()
        refreshed_cells: list[CellRecord] = []
        for cell in cells:
            pool = pools_by_id.get(cell.pool_id or "")
            if (
                pool is not None
                and pool.enabled
                and not cell_requires_replacement(cell, pool)
                and not cell_matches_pool(cell, pool)
                and cell.state not in {CellState.DRAINING, CellState.DELETING, CellState.DELETED}
            ):
                try:
                    updated, _ = await self._autoscale_upsert_cell(
                        pool,
                        cell.id,
                        cell.worker_count,
                        autoscale_request_workload_id=cell.autoscale_request_workload_id,
                        idempotency_key=f"autoscale:update:{cell.id}:g{pool.generation}",
                    )
                    if updated is not None:
                        cell = updated
                except ConflictError as exc:
                    logger.info("cell %s metadata update is waiting for active leases: %s", cell.id, exc)
            if (
                pool is not None
                and pool.enabled
                and cell_matches_pool(cell, pool)
                and cell.state is CellState.REGISTERING
                and cell.provider_phase is None
            ):
                try:
                    repaired, _ = await self._autoscale_requeue_unprovisioned_cell(pool, cell)
                    if repaired is not None:
                        cell = repaired
                except ConflictError as exc:
                    logger.info("cell %s provisioning recovery became stale: %s", cell.id, exc)
            refreshed_cells.append(cell)
        cells = refreshed_cells
        cells_by_pool: dict[str, list[CellRecord]] = {pool.id: [] for pool in pools}
        for cell in cells:
            pool_id = cell.pool_id
            if pool_id is not None and pool_id in cells_by_pool:
                cells_by_pool[pool_id].append(cell)

        pool_records = tuple(pools)
        simulation = CapacitySimulation.build(pool_records, tuple(cells), tuple(workloads), tuple(leases), now)
        simulated = self.service.scheduler.schedule(
            workloads,
            simulation.cells,
            leases=leases,
            budgets=budgets,
            quotas=quotas,
            policies=policies,
        )
        demand_by_pool = simulation.demand(pool_records, tuple(leases), simulated)
        planner = CapacityPlanner(pool_records, cells_by_pool, now)
        plans = planner.plan(demand_by_pool)
        await self._record_autoscale_quota_blocks(tuple(workloads), planner.quota_blocked_workload_ids)

        launched = 0
        drained = 0
        deleted = 0
        active_lease_cells = {lease.cell_id for lease in leases if lease.state in ACTIVE_LEASE_STATES}
        cell_ids = {cell.id for cell in cells}

        for pool in sorted(pools, key=lambda item: item.id):
            plan = plans[pool.id]
            live = list(plan.live)
            matching_supply = list(plan.matching_supply)
            target = plan.target_worker_units
            # Monotonic, from every cell this pool has ever had -- including
            # deleted ones, whose records are the evidence their index is spent.
            # `len(managed) + 1` walked backwards on deletion and handed the same
            # id out twice; the second holder then inherited the first's provider
            # resource under that name.
            next_index = next_pool_cell_index(pool, cells)
            for worker_count, autoscale_request_workload_id in zip(
                plan.launch_worker_counts,
                plan.launch_workload_ids,
                strict=True,
            ):
                # `pool_cell_id` carries a per-pool-record epoch. Without it the
                # counter restarts at 0001 on a control plane with an empty
                # database and collides with the agent's durable cell tombstones,
                # which fence every apply forever.
                cell_id = pool_cell_id(pool, next_index)
                while cell_id in cell_ids:
                    next_index += 1
                    cell_id = pool_cell_id(pool, next_index)
                created, replayed = await self._autoscale_upsert_cell(
                    pool,
                    cell_id,
                    worker_count,
                    autoscale_request_workload_id=autoscale_request_workload_id,
                    idempotency_key=f"autoscale:create:{cell_id}:g{pool.generation}",
                )
                if created is None:
                    continue
                if not replayed:
                    self._capacity_lifecycle.event(
                        CapacityLifecycleEvent.PLANNED,
                        created,
                        reason=f"autoscaler launch for {worker_count} worker(s), target {target}",
                    )
                cell_ids.add(cell_id)
                next_index += 1
                launched += 1

            if (
                pool.enabled
                and pool.initialized_at is None
                and plan.supply_worker_units_after_launch >= pool.initial_target
            ):
                async with self.repository.transaction() as transaction:
                    current = await transaction.get_capacity_pool(pool.id)
                    if current is not None and current.generation == pool.generation and current.initialized_at is None:
                        await transaction.put_capacity_pool(current.model_copy(update={"initialized_at": now}))

            draining = [cell for cell in live if cell.state is CellState.DRAINING]
            for cell in draining:
                if cell.id in active_lease_cells:
                    continue
                await self.delete_cell(
                    self._principal,
                    cell.id,
                    idempotency_key=f"autoscale:delete:{cell.id}:g{cell.generation + 1}",
                )
                deleted += 1

            current_candidates = [
                cell
                for cell in matching_supply
                if cell.state not in {CellState.DRAINING, CellState.DELETING} and cell.id not in active_lease_cells
            ]
            protected = set(plan.protected_cell_ids)

            updates: list[CellRecord] = []
            stale_candidates = [
                cell
                for cell in live
                if cell not in matching_supply
                and cell.state not in {CellState.DRAINING, CellState.DELETING}
                and cell.id not in active_lease_cells
            ]
            ready_drain_budget = max(
                sum(cell.worker_count for cell in live if cell.state is CellState.READY) - target,
                0,
            )
            to_drain: list[CellRecord] = []
            replacement_reasons: dict[str, CapacityReplacementReason] = {}
            for cell in stale_candidates:
                if pool.enabled and cell.state is CellState.READY:
                    if cell.id in plan.forced_drain_cell_ids:
                        pass
                    elif ready_drain_budget > 0:
                        ready_drain_budget = max(ready_drain_budget - cell.worker_count, 0)
                    else:
                        continue
                to_drain.append(cell)
                replacement_reasons[cell.id] = _replacement_reason(cell, pool)
            idle_timeout_s = pool.idle_timeout_s if pool.enabled else 0
            for cell in current_candidates:
                if cell.id in plan.forced_drain_cell_ids:
                    to_drain.append(cell)
                    replacement_reasons[cell.id] = CapacityReplacementReason.SHARED_QUOTA_REBALANCE
                    continue
                if cell.id in protected:
                    if cell.idle_since is not None:
                        updates.append(cell.model_copy(update={"idle_since": None, "updated_at": now}))
                    continue
                idle_since = cell.idle_since or now
                if cell.idle_since is None:
                    updates.append(cell.model_copy(update={"idle_since": idle_since, "updated_at": now}))
                if (now - idle_since).total_seconds() >= idle_timeout_s:
                    to_drain.append(cell)
            if updates:
                async with self.repository.transaction() as transaction:
                    for update in updates:
                        current = await transaction.get_cell(update.id)
                        if current is not None and current.generation == update.generation:
                            await transaction.put_cell(update)
            for cell in {candidate.id: candidate for candidate in to_drain}.values():
                draining_cell, replayed = await self.drain_cell(
                    self._principal,
                    cell.id,
                    idempotency_key=f"autoscale:drain:{cell.id}:g{cell.generation}",
                )
                if not replayed and cell.state is not CellState.READY:
                    # This cell is being reclaimed without ever having delivered
                    # capacity -- it exceeded queue_timeout_s or provisioning_timeout_s
                    # and stopped counting as supply. Nothing else records that: the
                    # observation paths key off the provider's reported state, and a
                    # cell parked in WAITING keeps reporting a *pending* status, which
                    # leaves its belief untouched. Without this the autoscaler never
                    # learns which zone is empty and keeps choosing it first forever.
                    # `replayed` makes it once per (cell, generation).
                    await self._record_capacity_stockout(cell)
                replacement_reason = replacement_reasons.get(cell.id)
                if replacement_reason is not None and not replayed:
                    self._capacity_lifecycle.event(
                        CapacityLifecycleEvent.REPLACEMENT,
                        draining_cell,
                        previous=cell,
                        reason=replacement_reason.value,
                    )
                drained += 1

        return {"launched": launched, "drained": drained, "deleted": deleted}

    async def reconcile_unmanaged_capacity_recoveries_once(self) -> int:
        """Resume manual cells whose agent fence conflict survived a controller interruption."""

        async with self.repository.transaction() as transaction:
            candidates = [
                cell
                for cell in await transaction.list_cells()
                if cell.pool_id is None
                and cell.state is CellState.UNAVAILABLE
                and cell.provider_phase is CapacityStatus.NEEDS_RECREATE
            ]
        recovered = 0
        for cell in candidates:
            try:
                recreated, replayed = await self.service.recreate_cell(
                    self._principal,
                    cell.id,
                    expected_generation=cell.generation,
                    idempotency_key=f"agent-fence-recreate:{cell.id}:g{cell.generation}",
                )
            except (ConflictError, NotFoundError) as exc:
                logger.debug("manual cell %s cannot yet recover from an agent fence conflict: %s", cell.id, exc)
                continue
            if not replayed:
                self._capacity_lifecycle.transition(
                    cell,
                    recreated,
                    reason="resuming manual cell after an agent fence conflict",
                )
                recovered += 1
        return recovered

    async def _commit_placement(self, decision: PlacementDecision) -> None:
        if len(decision.all_leases) > 1:
            await self._commit_group_placement(decision)
            return
        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(decision.workload_id)
            cell = await transaction.get_cell(decision.lease.cell_id)
        if (
            workload is None
            or workload.state is not WorkloadState.PENDING
            or workload.preemption_id is not None
            or cell is None
            or cell.state is not CellState.READY
            or cell.autoscale_request_workload_id not in {None, workload.id}
        ):
            return
        lease = decision.lease
        record = LeaseRecord(
            id=lease.lease_id,
            workload_id=workload.id,
            cell_id=lease.cell_id,
            cell_generation=cell.generation,
            org_id=workload.org_id,
            project_id=workload.project_id,
            resources=ResourceGang(
                replicas=workload.spec.gang.replicas,
                per_replica=workload.spec.gang.per_replica,
                required_topology=workload.spec.gang.required_topology,
            ),
            hourly_cost=float(lease.hourly_cost),
            reserved_cost=float(lease.reserved_cost),
            resource_hours_reserved=_api_resources(lease.resource_hours_reserved),
            generation=workload.generation,
            fencing_token=secrets.token_urlsafe(32),
            expires_at=utcnow() + timedelta(seconds=self.settings.lease_ttl_s),
        )
        await self.service.create_lease(
            self._principal,
            record,
            idempotency_key=f"schedule:{record.id}:{record.generation}",
        )

    async def _commit_group_placement(self, decision: PlacementDecision) -> None:
        """Persist every member of a spread-cell pod placement atomically."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(decision.workload_id)
            cells = {lease.cell_id: await transaction.get_cell(lease.cell_id) for lease in decision.all_leases}
        if (
            workload is None
            or workload.state is not WorkloadState.PENDING
            or workload.preemption_id is not None
            or len(cells) != len(decision.all_leases)
            or any(
                cell is None
                or cell.state is not CellState.READY
                or cell.autoscale_request_workload_id not in {None, workload.id}
                for cell in cells.values()
            )
        ):
            return
        group_id = stable_id("pod_group", workload.id, workload.attempt, workload.generation)
        records: list[LeaseRecord] = []
        for index, proposed in enumerate(decision.all_leases):
            cell = cells[proposed.cell_id]
            assert cell is not None
            records.append(
                LeaseRecord(
                    id=proposed.lease_id,
                    workload_id=workload.id,
                    cell_id=proposed.cell_id,
                    cell_generation=cell.generation,
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    resources=ResourceGang(
                        replicas=workload.spec.gang.replicas,
                        per_replica=workload.spec.gang.per_replica,
                        required_topology=workload.spec.gang.required_topology,
                    ),
                    hourly_cost=float(proposed.hourly_cost),
                    reserved_cost=float(proposed.reserved_cost),
                    resource_hours_reserved=_api_resources(proposed.resource_hours_reserved),
                    generation=workload.generation,
                    fencing_token=secrets.token_urlsafe(32),
                    group_id=group_id,
                    group_index=index,
                    expires_at=utcnow() + timedelta(seconds=self.settings.lease_ttl_s),
                )
            )
        await self.service.create_lease_group(
            self._principal,
            tuple(records),
            idempotency_key=f"schedule-group:{group_id}",
        )

    async def _commit_preemption(self, plan: PreemptionPlan) -> None:
        now = utcnow()
        async with self.repository.transaction() as transaction:
            target = await transaction.get_workload(plan.workload_id)
            if target is None or target.state is not WorkloadState.PENDING or self.service.scheduler is None:
                return
            if target.preemption_id is not None:
                existing = await transaction.get_outbox(target.preemption_id)
                if existing is not None and existing.state is not OutboxState.SUCCEEDED:
                    return
            active_leases = [lease for lease in await transaction.list_leases() if lease.state in ACTIVE_LEASE_STATES]
            lease_snapshots = {lease.id: lease for lease in active_leases}
            victims: list[tuple[LeaseRecord, WorkloadRecord]] = []
            for proposed in plan.victims:
                snapshot = lease_snapshots.get(proposed.lease_id)
                if snapshot is None:
                    return
                workload = await transaction.get_workload(snapshot.workload_id)
                lease = await transaction.get_lease(snapshot.id)
                if workload is None or lease is None:
                    return
                victims.append((lease, workload))

            await transaction.get_idempotency(
                "fleet",
                "admission.org",
                storage_scope(target.org_id, ""),
            )
            await transaction.get_idempotency(
                "fleet",
                "admission.project",
                storage_scope(target.org_id, target.project_id),
            )
            victim_scopes = sorted({(workload.org_id, workload.project_id) for _, workload in victims})
            for org_id, project_id in victim_scopes:
                if (org_id, project_id) == (target.org_id, target.project_id):
                    continue
                await transaction.get_idempotency("fleet", "admission.org", storage_scope(org_id, ""))
                await transaction.get_idempotency(
                    "fleet",
                    "admission.project",
                    storage_scope(org_id, project_id),
                )
            cell = await transaction.get_cell(plan.cell_id)
            if cell is None or cell.state is not CellState.READY:
                return
            policy = await transaction.get_policy(target.org_id, target.project_id)
            if policy is not None:
                try:
                    self.service._validate_workload_policy(target.spec, policy)
                except InputError:
                    return
            budget = await transaction.get_budget(target.org_id, target.project_id)
            active_workload_ids = {active.workload_id for active in active_leases}
            validation_workloads = [
                workload
                for workload in await transaction.list_workloads()
                if workload.id == target.id or workload.id in active_workload_ids
            ]
            refreshed = self.service.scheduler.schedule(
                validation_workloads,
                await transaction.list_cells(),
                leases=active_leases,
                budgets=() if budget is None else (budget,),
                quotas=await transaction.list_quotas(org_id=target.org_id),
                policies=await transaction.list_policies(),
            )
            current_plan = next(
                (candidate for candidate in refreshed.preemptions if candidate.workload_id == target.id),
                None,
            )
            if current_plan is None or current_plan.cell_id != plan.cell_id:
                return
            if tuple(victim.lease_id for victim in current_plan.victims) != tuple(
                victim.lease_id for victim in plan.victims
            ):
                return
            plan = current_plan

            by_lease = {lease.id: (lease, workload) for lease, workload in victims}
            for proposed in plan.victims:
                lease, workload = by_lease[proposed.lease_id]
                if (
                    lease.state not in {LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.QUARANTINED}
                    or workload.lease_id != lease.id
                    or workload.generation != lease.generation
                    or workload.state not in {WorkloadState.LEASED, WorkloadState.DISPATCHING, WorkloadState.RUNNING}
                ):
                    return

            command_id = stable_id(
                "cmd_preempt_plan",
                target.id,
                target.attempt,
                target.generation,
                uuid.uuid4().hex,
            )
            target = target.model_copy(
                update={
                    "scheduling_reason": "waiting for checkpoint-aware preemption",
                    "preemption_id": command_id,
                    "updated_at": now,
                }
            )
            await transaction.put_workload(target)
            victim_payloads: list[JsonObject] = []
            for victim in plan.victims:
                lease, workload = by_lease[victim.lease_id]
                victim_lease_state = lease.state
                victim_workload_state = workload.state
                lease = lease.model_copy(
                    update={
                        "state": LeaseState.REVOKING,
                        "revocation_reason": "preemption",
                        "updated_at": now,
                    }
                )
                should_checkpoint = (
                    victim.action is PreemptionActionKind.CHECKPOINT_AND_STOP and workload.remote_id is not None
                )
                next_state = WorkloadState.CHECKPOINTING if should_checkpoint else WorkloadState.CANCELLING
                workload = workload.model_copy(
                    update={"state": next_state, "preemption_id": command_id, "updated_at": now}
                )
                await transaction.put_lease(lease)
                await transaction.put_workload(workload)
                victim_payloads.append(
                    model_json(
                        _PreemptionVictim(
                            workload_id=workload.id,
                            lease_id=lease.id,
                            cell_id=lease.cell_id,
                            generation=workload.generation,
                            checkpoint=should_checkpoint,
                            grace_period_seconds=victim.grace_period_seconds,
                            hook=workload.spec.checkpoint_hook if should_checkpoint else None,
                            output_uri=workload.spec.checkpoint_output_uri if should_checkpoint else None,
                            workload_state=victim_workload_state,
                            lease_state=victim_lease_state,
                        )
                    )
                )
                await transaction.append_event(
                    AuditEvent(
                        id=f"evt_{uuid.uuid4().hex}",
                        actor=self._principal.subject,
                        org_id=workload.org_id,
                        project_id=workload.project_id,
                        entity_type="workload",
                        entity_id=workload.id,
                        action="workload.preemption_started",
                        new_state=next_state.value,
                        payload={"target_workload_id": target.id, "lease_id": lease.id},
                    )
                )
            await transaction.put_outbox(
                OutboxRecord(
                    id=command_id,
                    topic="workload.preempt",
                    aggregate_type="workload",
                    aggregate_id=target.id,
                    idempotency_key=command_id,
                    generation=target.generation,
                    payload={
                        "target_workload_id": target.id,
                        "target_generation": target.generation,
                        "target_attempt": target.attempt,
                        "cell_id": plan.cell_id,
                        "execution_started": False,
                        "victims": victim_payloads,
                    },
                )
            )

    async def _record_infeasibilities(self, result: SchedulingResult) -> None:
        async with self.repository.transaction() as transaction:
            for failure in result.infeasibilities:
                workload = await transaction.get_workload(failure.workload_id)
                if workload is None or workload.state is not WorkloadState.PENDING:
                    continue
                if workload.autoscale_block_reason is not None:
                    continue
                if await self._has_active_autoscale_capacity(transaction, workload):
                    continue
                reason = f"{failure.code.value}: {failure.message}"
                if workload.scheduling_reason != reason:
                    await transaction.put_workload(
                        workload.model_copy(update={"scheduling_reason": reason, "updated_at": utcnow()})
                    )

    @staticmethod
    async def _has_active_autoscale_capacity(
        transaction: RepositorySession,
        workload: WorkloadRecord,
    ) -> bool:
        """Return whether an owned physical cell is still being provisioned."""

        return any(
            cell.state is CellState.REGISTERING
            for cell in await transaction.list_cells_for_autoscale_request(workload.id)
        )

    async def _record_autoscale_quota_blocks(
        self,
        workloads: tuple[WorkloadRecord, ...],
        blocked_workload_ids: frozenset[str],
    ) -> None:
        """Persist the local shared-Spot-quota outcome for current pending workloads."""

        pending_workload_ids = tuple(
            sorted({workload.id for workload in workloads if workload.state is WorkloadState.PENDING})
        )
        if not pending_workload_ids:
            return

        async with self.repository.transaction() as transaction:
            for workload_id in pending_workload_ids:
                workload = await transaction.get_workload(workload_id)
                if workload is None or workload.state is not WorkloadState.PENDING:
                    continue

                quota_blocked = workload.id in blocked_workload_ids
                if quota_blocked:
                    if (
                        workload.autoscale_block_reason is AutoscaleBlockReason.SHARED_TPU_QUOTA
                        and workload.scheduling_reason == _SHARED_TPU_QUOTA_SCHEDULING_REASON
                    ):
                        continue
                    await transaction.put_workload(
                        workload.model_copy(
                            update={
                                "autoscale_block_reason": AutoscaleBlockReason.SHARED_TPU_QUOTA,
                                "queue_admission_denial": None,
                                "scheduling_reason": _SHARED_TPU_QUOTA_SCHEDULING_REASON,
                                "updated_at": utcnow(),
                            }
                        )
                    )
                    continue

                if workload.autoscale_block_reason is not AutoscaleBlockReason.SHARED_TPU_QUOTA:
                    continue
                scheduling_reason = workload.scheduling_reason
                if scheduling_reason == _SHARED_TPU_QUOTA_SCHEDULING_REASON:
                    scheduling_reason = None
                await transaction.put_workload(
                    workload.model_copy(
                        update={
                            "autoscale_block_reason": None,
                            "queue_admission_denial": None,
                            "scheduling_reason": scheduling_reason,
                            "updated_at": utcnow(),
                        }
                    )
                )

    async def dispatch_outbox_once(self, *, limit: int = 20) -> int:
        if limit <= 0:
            return 0
        command_ids = await self.repository.snapshot_claimable_outbox_ids(limit=limit)
        dispatched = 0
        for command_id in command_ids:
            command = await self.repository.claim_outbox_id(
                worker_id=self.worker_id,
                command_id=command_id,
                lease_seconds=self.settings.outbox_claim_ttl_s,
            )
            if command is None:
                continue
            await self._dispatch_claimed_outbox(command)
            dispatched += 1
        return dispatched

    async def _dispatch_claimed_outbox(self, command: OutboxRecord) -> None:
        """Deliver and settle one command that this controller has just claimed."""

        try:
            await self._dispatch_with_claim_heartbeat(command)
        except CapacityApplySuperseded as exc:
            logger.info("discarding superseded capacity apply %s: %s", command.id, exc)
            await self._discard_outbox(command, provider_diagnostic=exc.provider_diagnostic)
        except StaleOutboxCommand as exc:
            logger.info("discarding stale outbox command %s: %s", command.id, exc)
            await self._finish_outbox(command)
        except CellAgentStaleMutationError as exc:
            logger.info("discarding cell-agent stale mutation %s: %s", command.id, exc)
            diagnostic = str(exc)
            if command.topic == "capacity.apply":
                await self._record_cell_agent_fence_conflict(
                    cell_id=command.aggregate_id,
                    generation=command.generation,
                    diagnostic=diagnostic,
                )
                await self._discard_outbox(command, provider_diagnostic=diagnostic)
            elif command.topic == "capacity.drain":
                await self._recover_stale_cell_lifecycle_command(
                    command,
                    expected_state=CellState.DRAINING,
                    diagnostic=diagnostic,
                )
            elif command.topic == "capacity.release":
                await self._recover_stale_cell_lifecycle_command(
                    command,
                    expected_state=CellState.DELETING,
                    diagnostic=diagnostic,
                )
            elif command.topic == "workload.dispatch":
                await self._recover_stale_workload_command(
                    command,
                    expected_state=WorkloadState.LEASED,
                    diagnostic=diagnostic,
                )
            elif command.topic == "workload.cancel":
                await self._recover_stale_workload_command(
                    command,
                    expected_state=WorkloadState.CANCELLING,
                    diagnostic=diagnostic,
                )
            elif command.topic == "workload.checkpoint":
                await self._recover_stale_workload_command(
                    command,
                    expected_state=WorkloadState.CHECKPOINTING,
                    diagnostic=diagnostic,
                )
            elif command.topic == "workload.preempt":
                await self._recover_stale_preemption_command(command, diagnostic=diagnostic)
            else:
                await self._discard_outbox(command, provider_diagnostic=diagnostic)
        except CellAgentBusyError as exc:
            await self._finish_outbox(command, error=str(exc), transient=True)
        except CellAgentUnsupportedCapabilityError as exc:
            # The backend does not implement this operation; retrying cannot
            # change that. Settle it for good with the diagnostic intact rather
            # than backing off against a permanent 501.
            logger.warning(
                "cell agent does not support %s for command %s: %s",
                command.topic,
                command.id,
                exc,
            )
            await self._finish_outbox(command, error=f"unsupported by cell agent backend: {exc}", terminal=True)
        except CellAgentInvalidConfigurationError as exc:
            # A rejected provider option cannot start working on a retry, so
            # settle it terminally with the agent's own sentence -- which names
            # the exact contradiction and is usually one line of YAML to fix.
            logger.warning("cell agent rejected the configuration for command %s: %s", command.id, exc)
            await self._finish_outbox(command, error=f"invalid configuration: {exc}", terminal=True)
        except CellAgentCapacityProviderError as exc:
            await self._finish_outbox(command, error=str(exc), failure_code=exc.code)
        except Exception as exc:
            await self._finish_outbox(command, error=str(exc))
        else:
            await self._finish_outbox(command)

    def _outbox_claim_heartbeat_s(self) -> float:
        """Return a bounded renewal cadence for long-running outbox commands."""

        claim_ttl_s = max(self.settings.outbox_claim_ttl_s, 1)
        return min(30.0, max(0.1, claim_ttl_s / 3))

    @staticmethod
    def _consume_background_dispatch_result(task: asyncio.Task[None]) -> None:
        """Consume a detached RPC result after its durable claim was lost."""

        if task.cancelled():
            return
        try:
            task.exception()
        except asyncio.CancelledError:
            return

    async def _dispatch_with_claim_heartbeat(self, command: OutboxRecord) -> None:
        """Deliver one command while keeping its durable ownership lease alive.

        Provider creates can legitimately run for minutes. The command remains
        idempotent, but a process that dies during the RPC must be reclaimable
        soon rather than hiding a pending workload behind a long claim TTL.
        """

        active_command = await self._renew_outbox_claim(command)
        dispatch = asyncio.create_task(self._dispatch(active_command))
        try:
            while not dispatch.done():
                completed, _ = await asyncio.wait((dispatch,), timeout=self._outbox_claim_heartbeat_s())
                if not completed:
                    active_command = await self._renew_outbox_claim(active_command)
            await dispatch
        except asyncio.CancelledError:
            if not dispatch.done():
                dispatch.add_done_callback(self._consume_background_dispatch_result)
            raise
        except Exception:
            if not dispatch.done():
                dispatch.add_done_callback(self._consume_background_dispatch_result)
            raise

    async def _renew_outbox_claim(self, command: OutboxRecord) -> OutboxRecord:
        """Extend this controller's current claim without reviving a stale command."""

        async with self.repository.transaction() as transaction:
            current = await transaction.get_outbox(command.id)
            if current is None:
                raise StaleOutboxCommand(f"outbox command {command.id!r} no longer exists")
            now = utcnow()
            if (
                current.state is not OutboxState.PROCESSING
                or current.claimed_by != self.worker_id
                or current.claimed_until is None
                or current.claimed_until <= now
                or current.topic != command.topic
                or current.aggregate_type != command.aggregate_type
                or current.aggregate_id != command.aggregate_id
                or current.generation != command.generation
            ):
                raise StaleOutboxCommand(f"outbox command {command.id!r} claim expired or moved to another worker")
            renewed = current.model_copy(
                update={
                    "claimed_until": now + timedelta(seconds=max(self.settings.outbox_claim_ttl_s, 1)),
                    "updated_at": now,
                }
            )
            await transaction.put_outbox(renewed)
            return renewed

    async def _dispatch(self, command: OutboxRecord) -> None:
        if command.topic == "capacity.apply":
            await self._dispatch_capacity(command)
        elif command.topic == "capacity.drain":
            await self._dispatch_capacity_drain(command)
        elif command.topic == "capacity.release":
            await self._dispatch_capacity_release(command)
        elif command.topic == "workload.dispatch":
            await self._dispatch_workload(command)
        elif command.topic == "workload.cancel":
            await self._dispatch_cancel(command)
        elif command.topic == "workload.checkpoint":
            await self._dispatch_checkpoint(command)
        elif command.topic == "workload.preempt":
            await self._dispatch_preemption(command)
        else:
            raise ValueError(f"unsupported outbox topic {command.topic!r}")

    async def _recover_stale_cell_lifecycle_command(
        self,
        command: OutboxRecord,
        *,
        expected_state: CellState,
        diagnostic: str,
    ) -> None:
        """Replace an obsolete drain or release mutation before retiring its outbox record."""

        try:
            previous = await self.service.get_cell(self._principal, command.aggregate_id)
            refenced, replayed = await self.service.refence_cell_lifecycle_command(
                self._principal,
                command.aggregate_id,
                expected_generation=command.generation,
                expected_state=expected_state,
                idempotency_key=f"agent-fence-refence:{command.id}",
            )
        except (ConflictError, NotFoundError) as exc:
            logger.info("stale %s command %s no longer owns its cell: %s", command.topic, command.id, exc)
        except Exception as exc:
            logger.warning("could not refence stale %s command %s: %s", command.topic, command.id, exc)
            await self._finish_outbox(command, error=f"stale mutation recovery failed: {exc}", transient=True)
            return
        else:
            if not replayed:
                self._capacity_lifecycle.event(
                    CapacityLifecycleEvent.RETRY,
                    refenced,
                    previous=previous,
                    reason=f"cell-agent rejected {command.topic}; issued generation {refenced.generation}",
                )
        await self._discard_outbox(command, provider_diagnostic=diagnostic)

    async def _recover_stale_workload_command(
        self,
        command: OutboxRecord,
        *,
        expected_state: WorkloadState,
        diagnostic: str,
    ) -> None:
        """Settle a workload whose agent fence makes remote execution ownership uncertain."""

        raw_lease_id = command.payload.get("lease_id")
        if not isinstance(raw_lease_id, str) or not raw_lease_id:
            await self._discard_outbox(command, provider_diagnostic=diagnostic)
            return
        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
        if (
            workload is None
            or workload.generation != command.generation
            or workload.lease_id != raw_lease_id
            or workload.state is not expected_state
        ):
            await self._discard_outbox(command, provider_diagnostic=diagnostic)
            return
        try:
            members = await self._load_workload_members(workload)
        except StaleOutboxCommand as exc:
            logger.info("stale %s command %s has no current workload members: %s", command.topic, command.id, exc)
            await self._discard_outbox(command, provider_diagnostic=diagnostic)
            return
        if any(lease.generation != command.generation for lease, _cell in members):
            await self._discard_outbox(command, provider_diagnostic=diagnostic)
            return
        for lease, cell in members:
            await self._record_cell_agent_fence_conflict(
                cell_id=cell.id,
                generation=lease.cell_generation,
                diagnostic=f"workload {workload.id!r} {command.topic} fence conflict: {diagnostic}",
            )
        failure_reason = f"cell-agent {command.topic.removeprefix('workload.')} fence conflict: {diagnostic}"
        try:
            await self.service.settle_terminal_workload(
                self._principal,
                workload.id,
                WorkloadState.FAILED,
                action="workload.agent_fence_conflict",
                failure_reason=failure_reason,
                expected_generation=workload.generation,
                expected_lease_id=raw_lease_id,
                expected_state=expected_state,
            )
        except (ConflictError, NotFoundError) as exc:
            logger.info("stale %s command %s settlement lost ownership: %s", command.topic, command.id, exc)
        else:
            await self.reconcile_unmanaged_capacity_recoveries_once()
        await self._discard_outbox(command, provider_diagnostic=diagnostic)

    async def _recover_stale_preemption_command(self, command: OutboxRecord, *, diagnostic: str) -> None:
        """Abort a preemption before stopping, or retain it for explicit operator finalization."""

        try:
            aborted = await self._abort_preemption_checkpoint_phase(command, diagnostic)
        except StaleOutboxCommand as exc:
            logger.info("stale preemption command %s already changed ownership: %s", command.id, exc)
            await self._discard_outbox(command, provider_diagnostic=diagnostic)
            return
        if aborted:
            await self._finish_outbox(command)
            return
        logger.error(
            "preemption command %s lost an agent fence after stopping began; explicit force-finalization is required",
            command.id,
        )
        await self._discard_outbox(command, provider_diagnostic=diagnostic)

    async def _load_cell(self, command: OutboxRecord) -> CellRecord:
        async with self.repository.transaction() as transaction:
            cell = await transaction.get_cell(command.aggregate_id)
        if cell is None:
            raise StaleOutboxCommand(f"cell {command.aggregate_id!r} no longer exists")
        if cell.generation != command.generation:
            raise StaleOutboxCommand(
                f"cell {cell.id!r} is generation {cell.generation}, command is {command.generation}"
            )
        return cell

    @staticmethod
    def _mutation(command: OutboxRecord, fencing_token: str) -> MutationModel:
        return MutationModel(
            idempotency_key=command.idempotency_key,
            generation=command.generation,
            fencing_token=fencing_token,
        )

    @staticmethod
    def _workload_lease_ids(workload: WorkloadRecord) -> tuple[str, ...]:
        """Return one workload's ordered lease membership, including legacy records."""

        if workload.lease_ids:
            return workload.lease_ids
        return () if workload.lease_id is None else (workload.lease_id,)

    @staticmethod
    def _has_live_dispatch_command(
        workload: WorkloadRecord,
        lease_ids: tuple[str, ...],
        commands: tp.Iterable[OutboxRecord],
    ) -> bool:
        """Return whether a retryable command can still submit this exact lease membership.

        A malformed active command is deliberately treated as live. It must be
        retired by normal outbox delivery before releasing its lease, because a
        controller cannot prove that a malformed persisted payload has no
        submission side effect.
        """

        for command in commands:
            if (
                command.topic != "workload.dispatch"
                or command.aggregate_type != "workload"
                or command.aggregate_id != workload.id
                or command.generation != workload.generation
                or command.state not in _RETRIABLE_OUTBOX_STATES
            ):
                continue
            try:
                membership = _DispatchLeaseMembership.model_validate(command.payload)
            except ValidationError:
                return True
            if membership.ids == lease_ids:
                return True
        return False

    @staticmethod
    def _ordered_gang_member_pairs(
        members: tp.Sequence[tuple[LeaseRecord, CellRecord]],
    ) -> tuple[tuple[LeaseRecord, CellRecord], ...]:
        """Validate and deterministically order native group members by cell ID."""

        ordered = tuple(sorted(members, key=lambda member: member[0].cell_id))
        if len(ordered) < 2:
            raise RuntimeError("a native workload gang requires at least two leased cells")
        if len({lease.cell_id for lease, _cell in ordered}) != len(ordered):
            raise RuntimeError("a native workload gang cannot contain duplicate cells")
        endpoints = {cell.endpoint for _lease, cell in ordered}
        if None in endpoints or len(endpoints) != 1:
            raise RuntimeError("native workload gang cells must use one trusted cell-agent endpoint")
        return ordered

    @staticmethod
    def _required_agent_endpoint(cell: CellRecord) -> str:
        """Return a cell endpoint after the gang endpoint invariant is established."""

        endpoint = cell.endpoint
        if endpoint is None:
            raise RuntimeError("native workload gang cells require a trusted cell-agent endpoint")
        return endpoint

    @classmethod
    def _gang_members(
        cls,
        members: tp.Sequence[tuple[LeaseRecord, CellRecord]],
        *,
        idempotency_prefix: str,
    ) -> tuple[GangWorkloadMember, ...]:
        """Build per-cell fenced mutations for one native workload group."""

        return tuple(
            GangWorkloadMember(
                cell_id=lease.cell_id,
                agent_endpoint=cls._required_agent_endpoint(cell),
                mutation=MutationModel(
                    idempotency_key=f"{idempotency_prefix}:{lease.id}",
                    generation=lease.generation,
                    fencing_token=lease.fencing_token,
                ),
            )
            for lease, cell in cls._ordered_gang_member_pairs(members)
        )

    async def _load_workload_members(
        self,
        workload: WorkloadRecord,
    ) -> tuple[tuple[LeaseRecord, CellRecord], ...]:
        """Load every current lease and cell belonging to a workload group."""

        lease_ids = self._workload_lease_ids(workload)
        if not lease_ids:
            raise StaleOutboxCommand(f"workload {workload.id!r} has no current lease")
        async with self.repository.transaction() as transaction:
            leases = tuple([await transaction.get_lease(lease_id) for lease_id in lease_ids])
            concrete_leases = tuple(lease for lease in leases if lease is not None)
            if len(concrete_leases) != len(leases):
                raise StaleOutboxCommand(f"workload {workload.id!r} references a missing lease")
            cells = tuple([await transaction.get_cell(lease.cell_id) for lease in concrete_leases])
        concrete_cells = tuple(cell for cell in cells if cell is not None)
        if len(concrete_cells) != len(cells):
            raise StaleOutboxCommand(f"workload {workload.id!r} references a missing execution cell")
        if any(lease.workload_id != workload.id for lease in concrete_leases):
            raise StaleOutboxCommand(f"workload {workload.id!r} lease membership changed")
        return tuple(zip(concrete_leases, concrete_cells, strict=True))

    async def _require_current_pool_template(
        self,
        transaction: RepositorySession,
        cell: CellRecord,
    ) -> None:
        """Reject capacity work whose managed pool was disabled, removed, or replaced."""

        if cell.pool_id is None:
            return
        pool = await transaction.get_capacity_pool(cell.pool_id)
        if pool is None:
            raise CapacityApplySuperseded(f"cell {cell.id!r} belongs to a removed capacity pool")
        if not pool.enabled:
            raise CapacityApplySuperseded(f"cell {cell.id!r} belongs to disabled fleet {pool.fleet_name!r}")
        if not cell_matches_pool(cell, pool):
            raise CapacityApplySuperseded(f"cell {cell.id!r} no longer matches pool {pool.id!r}'s active template")

    async def _owned_capacity_apply_command(
        self,
        transaction: RepositorySession,
        command: OutboxRecord,
    ) -> OutboxRecord:
        """Load an exact, unexpired capacity-apply claim owned by this controller."""

        current = await transaction.get_outbox(command.id)
        now = utcnow()
        if (
            current is None
            or current.topic != "capacity.apply"
            or current.aggregate_type != command.aggregate_type
            or current.aggregate_id != command.aggregate_id
            or current.generation != command.generation
            or current.state is not OutboxState.PROCESSING
            or current.claimed_by != self.worker_id
            or current.claimed_until is None
            or current.claimed_until <= now
        ):
            raise StaleOutboxCommand(f"capacity apply command {command.id!r} claim expired or moved to another worker")
        return current

    @staticmethod
    def _capacity_apply_provider_rpc_is_proven_unstarted(command: OutboxRecord) -> bool:
        """Return whether this command reached the durable pre-RPC boundary only."""

        return command.payload.get(_CAPACITY_APPLY_PROVIDER_RPC_STARTED_KEY) is False

    async def _clear_unstarted_capacity_apply_requesting(
        self,
        transaction: RepositorySession,
        command: OutboxRecord,
        cell: CellRecord,
    ) -> CellRecord | None:
        """Clear a local request marker only when no provider RPC could have started."""

        current_command = await self._owned_capacity_apply_command(transaction, command)
        if (
            not self._capacity_apply_provider_rpc_is_proven_unstarted(current_command)
            or cell.generation != command.generation
            or cell.state is not CellState.REGISTERING
            or cell.provider_phase is not CapacityStatus.REQUESTING
        ):
            return None
        cleared = cell.model_copy(
            update={
                "provider_phase": None,
                "provider_phase_started_at": None,
                "updated_at": utcnow(),
            }
        )
        await transaction.put_cell(cleared)
        return cleared

    async def _recover_superseded_capacity_apply_template(
        self,
        transaction: RepositorySession,
        command: OutboxRecord,
        cell: CellRecord,
    ) -> CapacityApplySuperseded | None:
        """Recover a proven pre-RPC marker and return a template supersession."""

        try:
            await self._require_current_pool_template(transaction, cell)
        except CapacityApplySuperseded as superseded:
            await self._clear_unstarted_capacity_apply_requesting(transaction, command, cell)
            return superseded
        return None

    async def _dispatch_capacity(self, command: OutboxRecord) -> None:
        cell = await self._load_cell(command)
        async with self._capacity_operation(cell):
            await self._dispatch_capacity_locked(command)

    async def _dispatch_capacity_locked(self, command: OutboxRecord) -> None:
        cell = await self._load_cell(command)
        if cell.state is CellState.UNAVAILABLE:
            raise CapacityApplySuperseded(f"cell {cell.id!r} is unavailable after a prior capacity apply failure")
        if cell.state is not CellState.REGISTERING:
            raise StaleOutboxCommand(f"cell {cell.id!r} is {cell.state.value}, not registering")
        mutation = self._mutation(command, f"cell:{cell.id}:{cell.generation}")
        desired = {
            key: value
            for key, value in command.payload.items()
            if key != _CAPACITY_APPLY_PROVIDER_RPC_STARTED_KEY and value is not None
        }
        cell = await self._mark_capacity_apply_requesting(command, cell)
        await self._mark_capacity_apply_provider_rpc_started(command, cell)
        started = time.perf_counter()
        try:
            observation = await asyncio.to_thread(
                self._agent_factory(cell).apply_capacity,
                cell.id,
                CapacityRequest(mutation=mutation, desired=desired),
            )
        except CellAgentStaleMutationError:
            raise
        except Exception as exc:
            superseded: CapacityApplySuperseded | None = None
            async with self.repository.transaction() as transaction:
                current = await transaction.get_cell(cell.id)
                if (
                    current is None
                    or current.generation != command.generation
                    or current.state is not CellState.REGISTERING
                ):
                    diagnostic = str(exc).strip() or None
                    superseded = CapacityApplySuperseded(
                        "capacity apply lost ownership while the remote mutation was running",
                        provider_diagnostic=diagnostic,
                    )
                else:
                    template_superseded = await self._recover_superseded_capacity_apply_template(
                        transaction,
                        command,
                        current,
                    )
                    if template_superseded is not None:
                        diagnostic = str(exc).strip() or None
                        superseded = CapacityApplySuperseded(
                            str(template_superseded),
                            provider_diagnostic=diagnostic,
                        )
            if superseded is not None:
                raise superseded from exc
            raise
        remote_state = observation.state
        if remote_state in READY_CAPACITY_STATUSES:
            state = CellState.READY
            availability = self._observe_capacity_belief(
                cell,
                ProvisionOutcome.SUCCESS,
                latency_s=time.perf_counter() - started,
            )
        elif remote_state in PENDING_CAPACITY_STATUSES:
            state = CellState.REGISTERING
            availability = cell.availability
        else:
            state = CellState.UNAVAILABLE
            outcome = (
                ProvisionOutcome.QUOTA_ERROR
                if remote_state is CapacityStatus.HALTED_QUOTA
                else ProvisionOutcome.STOCKOUT
            )
            availability = self._observe_capacity_belief(cell, outcome)
        observed_at = utcnow()
        transition: tuple[CellRecord, CellRecord] | None = None
        superseded: CapacityApplySuperseded | None = None
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if (
                current is not None
                and current.generation == command.generation
                and current.state is CellState.REGISTERING
            ):
                template_superseded = await self._recover_superseded_capacity_apply_template(
                    transaction,
                    command,
                    current,
                )
                if template_superseded is not None:
                    superseded = template_superseded
                else:
                    phase_started_at = _provider_phase_start(current, remote_state, observed_at)
                    updated = current.model_copy(
                        update={
                            "state": state,
                            "provider_phase": remote_state,
                            "provider_failure_code": None,
                            "observed_tpu_vms": observation.observed_tpu_vms,
                            "provider_phase_started_at": phase_started_at,
                            "availability": availability,
                            "last_heartbeat_at": observed_at,
                            "updated_at": observed_at,
                        }
                    )
                    await transaction.put_cell(updated)
                    transition = current, updated
            else:
                superseded = CapacityApplySuperseded("capacity apply lost ownership before its observation was recorded")
        if superseded is not None:
            raise superseded
        if transition is not None:
            self._capacity_lifecycle.transition(*transition, reason=observation.detail or None)

    async def _mark_capacity_apply_requesting(self, command: OutboxRecord, cell: CellRecord) -> CellRecord:
        """Persist Eyvan's pre-provider request phase before a slow agent RPC."""

        transition: tuple[CellRecord, CellRecord] | None = None
        superseded: CapacityApplySuperseded | None = None
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if current is None or current.generation != command.generation or current.state is not CellState.REGISTERING:
                raise CapacityApplySuperseded("capacity apply lost ownership before capacity requesting began")
            superseded = await self._recover_superseded_capacity_apply_template(transaction, command, current)
            if superseded is None:
                current_command = await self._owned_capacity_apply_command(transaction, command)
                if current.provider_phase not in {None, CapacityStatus.REQUESTING}:
                    raise CapacityApplySuperseded(
                        f"cell {current.id!r} entered provider phase "
                        f"{current.provider_phase.value} before capacity apply"
                    )
                if current.provider_phase is CapacityStatus.REQUESTING:
                    return current
                observed_at = utcnow()
                if current_command.payload.get(_CAPACITY_APPLY_PROVIDER_RPC_STARTED_KEY) is True:
                    raise CapacityApplySuperseded(
                        f"capacity apply {command.id!r} may have started a provider RPC before requesting was persisted"
                    )
                payload = dict(current_command.payload)
                payload[_CAPACITY_APPLY_PROVIDER_RPC_STARTED_KEY] = False
                await transaction.put_outbox(
                    current_command.model_copy(
                        update={
                            "payload": payload,
                            "last_error": None,
                            "failure_code": None,
                            "updated_at": observed_at,
                        }
                    )
                )
                updated = current.model_copy(
                    update={
                        "provider_phase": CapacityStatus.REQUESTING,
                        "provider_failure_code": None,
                        "provider_phase_started_at": _provider_phase_start(
                            current,
                            CapacityStatus.REQUESTING,
                            observed_at,
                        ),
                        "updated_at": observed_at,
                    }
                )
                await transaction.put_cell(updated)
                transition = current, updated
        if superseded is not None:
            raise superseded
        if transition is not None:
            self._capacity_lifecycle.transition(*transition, reason="sending capacity request to cell agent")
            return transition[1]
        raise RuntimeError("capacity requesting transition unexpectedly had no persisted cell")

    async def _mark_capacity_apply_provider_rpc_started(self, command: OutboxRecord, cell: CellRecord) -> None:
        """Durably mark the point after which a provider request may have started."""

        superseded: CapacityApplySuperseded | None = None
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if current is None or current.generation != command.generation or current.state is not CellState.REGISTERING:
                raise CapacityApplySuperseded("capacity apply lost ownership before its provider RPC began")
            superseded = await self._recover_superseded_capacity_apply_template(transaction, command, current)
            if superseded is None:
                if current.provider_phase is not CapacityStatus.REQUESTING:
                    raise CapacityApplySuperseded(
                        f"cell {current.id!r} left capacity requesting before the provider RPC began"
                    )
                current_command = await self._owned_capacity_apply_command(transaction, command)
                if current_command.payload.get(_CAPACITY_APPLY_PROVIDER_RPC_STARTED_KEY) is True:
                    return
                payload = dict(current_command.payload)
                payload[_CAPACITY_APPLY_PROVIDER_RPC_STARTED_KEY] = True
                await transaction.put_outbox(
                    current_command.model_copy(update={"payload": payload, "updated_at": utcnow()})
                )
        if superseded is not None:
            raise superseded

    async def _dispatch_capacity_drain(self, command: OutboxRecord) -> None:
        cell = await self._load_cell(command)
        async with self._capacity_operation(cell):
            await self._dispatch_capacity_drain_locked(command)

    async def _dispatch_capacity_drain_locked(self, command: OutboxRecord) -> None:
        cell = await self._load_cell(command)
        request = WorkloadMutationRequest(mutation=self._mutation(command, f"cell:{cell.id}:{cell.generation}"))
        await asyncio.to_thread(self._agent_factory(cell).drain, cell.id, request)
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if current is not None and current.generation == command.generation and current.state is CellState.DRAINING:
                await transaction.put_cell(
                    current.model_copy(update={"state": CellState.DRAINING, "updated_at": utcnow()})
                )

    async def _dispatch_capacity_release(self, command: OutboxRecord) -> None:
        cell = await self._load_cell(command)
        async with self._capacity_operation(cell):
            await self._dispatch_capacity_release_locked(command)

    async def _dispatch_capacity_release_locked(self, command: OutboxRecord) -> None:
        cell = await self._load_cell(command)
        request = WorkloadMutationRequest(mutation=self._mutation(command, f"cell:{cell.id}:{cell.generation}"))
        await asyncio.to_thread(self._agent_factory(cell).release, cell.id, request)
        transition: tuple[CellRecord, CellRecord] | None = None
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if current is not None and current.generation == command.generation and current.state is CellState.DELETING:
                updated = current.model_copy(update={"state": CellState.DELETED, "updated_at": utcnow()})
                await transaction.put_cell(updated)
                transition = current, updated
        if transition is not None:
            self._capacity_lifecycle.transition(*transition, reason="provider capacity released")

    def _workspace_entrypoint(
        self,
        workload: WorkloadRecord,
        workspace: WorkspaceRef,
        command: str,
        environment: dict[str, str],
    ) -> tuple[str, dict[str, str]]:
        """Wrap a command so its workspace is materialized before it runs.

        The wrapper `exec`s the command, so the workload keeps the PID systemd
        started and signals, exit status and process introspection continue to
        describe the job rather than a shim.
        """

        parts = [
            "python",
            "-m",
            "eyvan.runtime.workspace_entry",
            "--workspace-sha256",
            workspace.sha256,
        ]
        if workspace.signature:
            parts.extend(("--workspace-signature", workspace.signature))
        if workspace.uri.startswith("eyvan://"):
            if self.artifact_access is None:
                raise RuntimeError("managed workspaces are not configured on this control-plane replica")
            transfer = self.artifact_access.workspace_download_transfer(
                workspace.uri,
                org_id=workload.org_id,
                project_id=workload.project_id,
                workload_id=workload.id,
            )
            environment["EYVAN_MANAGED_WORKSPACE_URL"] = transfer.url
            environment["EYVAN_MANAGED_WORKSPACE_TOKEN"] = transfer.token
            parts.extend(
                (
                    "--workspace-uri-env",
                    "EYVAN_MANAGED_WORKSPACE_URL",
                    "--workspace-token-env",
                    "EYVAN_MANAGED_WORKSPACE_TOKEN",
                )
            )
        else:
            parts.extend(("--workspace-uri", workspace.uri))
        parts.append("--")
        return " ".join(shlex.quote(part) for part in parts) + " " + command, environment

    def _render_execution(
        self,
        workload: WorkloadRecord,
        *,
        runtime_environment: tp.Mapping[str, str],
    ) -> tuple[str, dict[str, str]]:
        """Return the entrypoint and environment for one workload dispatch.

        Shared by the single-cell and gang dispatch paths. The callable contract
        -- which managed URIs are minted, which env vars carry them, and which
        worker flags name those vars -- must stay identical on both paths, so it
        lives here rather than being written out twice.
        """

        environment = {
            **runtime_environment,
            **workload.spec.environment,
            "EYVAN_WORKLOAD_ID": workload.id,
            "EYVAN_ATTEMPT": str(workload.attempt),
            "EYVAN_RESUME_CHECKPOINT_URI": workload.checkpoint_uri or "",
        }
        execution = workload.spec.execution
        if execution.kind == "command":
            # A workspace belongs to the workload, not to one execution kind, so
            # it is resolved before this branch rather than inside the callable
            # one. Everything workspace-related used to live below this early
            # return, which is precisely why `eyvan run` could not ship a
            # working tree and running your own code meant baking an image.
            workspace = workload.spec.resolved_workspace()
            if workspace is None:
                return execution.command, environment
            return self._workspace_entrypoint(workload, workspace, execution.command, environment)

        artifact_uri = execution.artifact_uri
        result_uri = execution.result_uri
        workspace_uri = execution.workspace_uri
        managed_transfer = (
            artifact_uri.startswith("eyvan://")
            or bool(result_uri and result_uri.startswith("eyvan://"))
            or bool(workspace_uri and workspace_uri.startswith("eyvan://"))
        )
        if managed_transfer and self.artifact_access is None:
            raise RuntimeError("managed callable artifacts are not configured on this control-plane replica")
        parts = [
            "python",
            "-m",
            "eyvan.runtime.callable_worker",
            "--sha256",
            execution.sha256,
            "--python",
            execution.python,
            "--format-version",
            str(execution.format_version),
        ]
        if artifact_uri.startswith("eyvan://"):
            assert self.artifact_access is not None
            transfer = self.artifact_access.callable_download_transfer(
                artifact_uri,
                org_id=workload.org_id,
                project_id=workload.project_id,
                workload_id=workload.id,
            )
            environment["EYVAN_MANAGED_CALLABLE_URL"] = transfer.url
            environment["EYVAN_MANAGED_CALLABLE_TOKEN"] = transfer.token
            parts.extend(
                (
                    "--uri-env",
                    "EYVAN_MANAGED_CALLABLE_URL",
                    "--uri-token-env",
                    "EYVAN_MANAGED_CALLABLE_TOKEN",
                )
            )
        else:
            parts.extend(("--uri", artifact_uri))
        if execution.signature:
            parts.extend(("--signature", execution.signature))
        if workspace_uri:
            parts.extend(("--workspace-sha256", str(execution.workspace_sha256)))
            if execution.workspace_signature:
                parts.extend(("--workspace-signature", execution.workspace_signature))
            if workspace_uri.startswith("eyvan://"):
                assert self.artifact_access is not None
                transfer = self.artifact_access.workspace_download_transfer(
                    workspace_uri,
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    workload_id=workload.id,
                )
                environment["EYVAN_MANAGED_WORKSPACE_URL"] = transfer.url
                environment["EYVAN_MANAGED_WORKSPACE_TOKEN"] = transfer.token
                parts.extend(
                    (
                        "--workspace-uri-env",
                        "EYVAN_MANAGED_WORKSPACE_URL",
                        "--workspace-token-env",
                        "EYVAN_MANAGED_WORKSPACE_TOKEN",
                    )
                )
            else:
                parts.extend(("--workspace-uri", workspace_uri))
        if result_uri and result_uri.startswith("eyvan://"):
            assert self.artifact_access is not None
            transfer = self.artifact_access.result_upload_transfer(
                result_uri,
                org_id=workload.org_id,
                project_id=workload.project_id,
                workload_id=workload.id,
            )
            environment["EYVAN_MANAGED_RESULT_URL"] = transfer.url
            environment["EYVAN_MANAGED_RESULT_TOKEN"] = transfer.token
            parts.extend(
                (
                    "--result-uri-env",
                    "EYVAN_MANAGED_RESULT_URL",
                    "--result-token-env",
                    "EYVAN_MANAGED_RESULT_TOKEN",
                )
            )
        elif result_uri:
            parts.extend(("--result-uri", result_uri))
        if (
            managed_transfer
            and self.artifact_access is not None
            and self.artifact_access.public_url.startswith("http://")
        ):
            if not self.settings.dev_auth_enabled:
                raise RuntimeError("managed artifact transfer requires an HTTPS EYVAN_PUBLIC_URL")
            environment["EYVAN_ALLOW_INSECURE_ARTIFACT_HTTP"] = "1"
        return shlex.join(parts), environment

    async def _dispatch_workload(self, command: OutboxRecord) -> None:
        lease_ids = tuple(str(value) for value in command.payload.get("lease_ids") or ())
        if len(lease_ids) > 1:
            await self._dispatch_workload_gang(command, lease_ids)
            return
        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
            lease = await transaction.get_lease(str(command.payload["lease_id"]))
            cell = await transaction.get_cell(str(command.payload["cell_id"]))
            if workload is None or lease is None or cell is None:
                raise StaleOutboxCommand("dispatch command references a missing workload, lease, or cell")
            self._validate_workload_command(command, workload, lease, cell)
            if workload.state is not WorkloadState.LEASED:
                raise StaleOutboxCommand(f"workload is {workload.state.value}, not leased")
            if lease.state not in {LeaseState.PENDING, LeaseState.QUARANTINED}:
                raise StaleOutboxCommand(f"lease is {lease.state.value}, not pending")
            if cell.state is not CellState.READY:
                raise RuntimeError(f"cell {cell.id!r} is {cell.state.value}, not ready for dispatch")
            if lease.state is LeaseState.QUARANTINED:
                observation = await asyncio.to_thread(self._agent_factory(cell).observe, cell.id)
                if observation.state not in READY_CAPACITY_STATUSES:
                    raise RuntimeError(f"cell {cell.id!r} has not recovered the quarantined lease")

            entrypoint, environment = self._render_execution(workload, runtime_environment=cell.runtime.env)
            request = WorkloadRequest(
                mutation=self._mutation(command, lease.fencing_token),
                spec={
                    "entrypoint": entrypoint,
                    "submission_id": f"{workload.id}-a{workload.attempt}-g{workload.generation}",
                    "environment": environment,
                    "backend_options": workload.spec.backend_options,
                    "gang": {
                        **workload.spec.gang.model_dump(mode="json"),
                        "placement_strategy": "PACK",
                    },
                    "metadata": {
                        **workload.spec.metadata,
                        "eyvan_workload_id": workload.id,
                        "eyvan_org_id": workload.org_id,
                        "eyvan_project_id": workload.project_id,
                        "eyvan_lease_id": lease.id,
                    },
                },
            )
            remote = await asyncio.to_thread(self._agent_factory(cell).submit, cell.id, request)
            now = utcnow()
            active_lease = lease.model_copy(
                update={
                    "state": LeaseState.ACTIVE,
                    "expires_at": now + timedelta(seconds=self.settings.lease_ttl_s),
                    # Write-once: a lease re-entering ACTIVE after a quarantine
                    # recovery is the same run, so its start must not move.
                    "run_started_at": lease.run_started_at or now,
                    "updated_at": now,
                }
            )
            dispatched = workload.model_copy(
                update={"state": WorkloadState.DISPATCHING, "remote_id": remote.remote_id, "updated_at": now}
            )
            await transaction.put_lease(active_lease)
            await transaction.put_workload(dispatched)
            await transaction.append_event(
                AuditEvent(
                    id=f"evt_{uuid.uuid4().hex}",
                    actor=self._principal.subject,
                    org_id=lease.org_id,
                    project_id=lease.project_id,
                    entity_type="lease",
                    entity_id=lease.id,
                    action="lease.activated",
                    previous_state=lease.state.value,
                    new_state=active_lease.state.value,
                )
            )
            await transaction.append_event(
                AuditEvent(
                    id=f"evt_{uuid.uuid4().hex}",
                    actor=self._principal.subject,
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    entity_type="workload",
                    entity_id=workload.id,
                    action="workload.dispatched",
                    previous_state=workload.state.value,
                    new_state=dispatched.state.value,
                    payload={"remote_id": remote.remote_id},
                )
            )

    async def _dispatch_workload_gang(self, command: OutboxRecord, lease_ids: tuple[str, ...]) -> None:
        """Launch one no-Ray native JAX job across independently leased cells."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
            leases = tuple([await transaction.get_lease(lease_id) for lease_id in lease_ids])
            members = tuple(lease for lease in leases if lease is not None)
            if workload is None or len(members) != len(leases):
                raise StaleOutboxCommand("gang dispatch command references a missing workload or lease")
            if workload.state is not WorkloadState.LEASED:
                raise StaleOutboxCommand(f"workload is {workload.state.value}, not leased")
            if workload.lease_ids != lease_ids or workload.lease_id != lease_ids[0]:
                raise StaleOutboxCommand("gang dispatch command no longer owns the workload lease group")
            if any(lease.workload_id != workload.id or lease.generation != command.generation for lease in members):
                raise StaleOutboxCommand("gang dispatch members no longer own the workload generation")
            if any(lease.state not in {LeaseState.PENDING, LeaseState.QUARANTINED} for lease in members):
                raise StaleOutboxCommand("gang dispatch lease is not pending")
            cells = tuple([await transaction.get_cell(lease.cell_id) for lease in members])
            concrete_cells = tuple(cell for cell in cells if cell is not None)
            if len(concrete_cells) != len(cells):
                raise StaleOutboxCommand("gang dispatch references a missing execution cell")
            if any(cell.state is not CellState.READY for cell in concrete_cells):
                bad = next(cell for cell in concrete_cells if cell.state is not CellState.READY)
                raise RuntimeError(f"cell {bad.id!r} is {bad.state.value}, not ready for gang dispatch")
            if any(
                lease.cell_generation != cell.generation for lease, cell in zip(members, concrete_cells, strict=True)
            ):
                raise StaleOutboxCommand("gang dispatch cell generation changed after placement")
            if workload.spec.required_executor != "eyvan-native":
                raise RuntimeError("multi-cell pod groups currently require the eyvan-native executor")
            endpoints = {cell.endpoint for cell in concrete_cells}
            if None in endpoints or len(endpoints) != 1:
                raise RuntimeError("native pod group cells must use the same trusted cell-agent endpoint")

            leader_lease = members[0]
            leader_cell = concrete_cells[0]
            entrypoint, environment = self._render_execution(workload, runtime_environment=leader_cell.runtime.env)

            spec = {
                "entrypoint": entrypoint,
                "submission_id": f"{workload.id}-a{workload.attempt}-g{workload.generation}",
                "environment": environment,
                "backend_options": workload.spec.backend_options,
                "gang": {
                    **workload.spec.gang.model_dump(mode="json"),
                    "placement_strategy": "PACK",
                },
                "metadata": {
                    **workload.spec.metadata,
                    "eyvan_workload_id": workload.id,
                    "eyvan_org_id": workload.org_id,
                    "eyvan_project_id": workload.project_id,
                    "eyvan_lease_id": leader_lease.id,
                    "eyvan_lease_ids": ",".join(lease.id for lease in members),
                },
            }
            paired = tuple(sorted(zip(members, concrete_cells, strict=True), key=lambda item: item[0].cell_id))
            coordinator_lease, coordinator_cell = paired[0]
            gang_request = GangWorkloadRequest(
                members=tuple(
                    GangWorkloadMember(
                        cell_id=lease.cell_id,
                        agent_endpoint=self._required_agent_endpoint(cell),
                        mutation=MutationModel(
                            idempotency_key=f"{command.idempotency_key}:{lease.id}",
                            generation=lease.generation,
                            fencing_token=lease.fencing_token,
                        ),
                    )
                    for lease, cell in paired
                ),
                spec=spec,
            )
            remote = await asyncio.to_thread(
                self._agent_factory(coordinator_cell).submit_gang,
                coordinator_lease.cell_id,
                gang_request,
            )
            now = utcnow()
            for lease in members:
                await transaction.put_lease(
                    lease.model_copy(
                        update={
                            "state": LeaseState.ACTIVE,
                            "expires_at": now + timedelta(seconds=self.settings.lease_ttl_s),
                            "run_started_at": lease.run_started_at or now,
                            "updated_at": now,
                        }
                    )
                )
            dispatched = workload.model_copy(
                update={"state": WorkloadState.DISPATCHING, "remote_id": remote.remote_id, "updated_at": now}
            )
            await transaction.put_workload(dispatched)
            for lease in members:
                await transaction.append_event(
                    AuditEvent(
                        id=f"evt_{uuid.uuid4().hex}",
                        actor=self._principal.subject,
                        org_id=lease.org_id,
                        project_id=lease.project_id,
                        entity_type="lease",
                        entity_id=lease.id,
                        action="lease.activated",
                        previous_state=lease.state.value,
                        new_state=LeaseState.ACTIVE.value,
                    )
                )
            await transaction.append_event(
                AuditEvent(
                    id=f"evt_{uuid.uuid4().hex}",
                    actor=self._principal.subject,
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    entity_type="workload",
                    entity_id=workload.id,
                    action="workload.dispatched",
                    previous_state=workload.state.value,
                    new_state=dispatched.state.value,
                    payload={"remote_id": remote.remote_id, "lease_ids": list(lease_ids)},
                )
            )

    @staticmethod
    def _validate_workload_command(
        command: OutboxRecord,
        workload: WorkloadRecord,
        lease: LeaseRecord,
        cell: CellRecord,
    ) -> None:
        if command.generation != workload.generation or lease.generation != workload.generation:
            raise StaleOutboxCommand(
                f"workload/lease generation is {workload.generation}/{lease.generation}, command is {command.generation}"
            )
        if workload.lease_id != lease.id or lease.workload_id != workload.id:
            raise StaleOutboxCommand("command lease no longer owns the workload")
        payload_cell = command.payload.get("cell_id")
        if lease.cell_id != cell.id or (payload_cell is not None and str(payload_cell) != cell.id):
            raise StaleOutboxCommand("command cell no longer owns the lease")
        if lease.cell_generation != cell.generation:
            raise StaleOutboxCommand(
                f"lease cell generation is {lease.cell_generation}, current cell generation is {cell.generation}"
            )

    async def _workload_command_state(
        self,
        command: OutboxRecord,
        *,
        workload_states: set[WorkloadState],
        lease_states: set[LeaseState],
    ):
        workload_id = str(command.payload["workload_id"])
        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
            lease = await transaction.get_lease(str(command.payload["lease_id"]))
            cell = await transaction.get_cell(lease.cell_id) if lease is not None else None
        if workload is None or lease is None or cell is None:
            raise StaleOutboxCommand("workload command references a missing workload, lease, or cell")
        self._validate_workload_command(command, workload, lease, cell)
        if workload.state not in workload_states:
            raise StaleOutboxCommand(f"workload is {workload.state.value}, expected one of {workload_states}")
        if lease.state not in lease_states:
            raise StaleOutboxCommand(f"lease is {lease.state.value}, expected one of {lease_states}")
        return workload, lease, cell

    async def _dispatch_cancel(self, command: OutboxRecord) -> None:
        lease_ids = tuple(str(value) for value in command.payload.get("lease_ids") or ())
        if len(lease_ids) > 1:
            await self._dispatch_gang_cancel(command, lease_ids)
            return
        workload, lease, cell = await self._workload_command_state(
            command,
            workload_states={WorkloadState.CANCELLING},
            lease_states={LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.REVOKING, LeaseState.QUARANTINED},
        )
        remote_id = workload.remote_id or command.payload.get("remote_id")
        if remote_id:
            request = WorkloadMutationRequest(mutation=self._mutation(command, lease.fencing_token))
            await asyncio.to_thread(self._agent_factory(cell).stop, cell.id, str(remote_id), request)
        await self._release_terminal(workload, lease, WorkloadState.CANCELLED, "workload.cancelled")

    async def _dispatch_gang_cancel(self, command: OutboxRecord, lease_ids: tuple[str, ...]) -> None:
        """Stop every member of a multi-cell workload before settling its leases."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(str(command.payload["workload_id"]))
            leases = tuple([await transaction.get_lease(lease_id) for lease_id in lease_ids])
            members = tuple(lease for lease in leases if lease is not None)
            if workload is None or len(members) != len(leases):
                raise StaleOutboxCommand("gang cancel command references a missing workload or lease")
            if workload.state is not WorkloadState.CANCELLING:
                raise StaleOutboxCommand(f"workload is {workload.state.value}, not cancelling")
            if self._workload_lease_ids(workload) != lease_ids or workload.lease_id != lease_ids[0]:
                raise StaleOutboxCommand("gang cancel command no longer owns the workload lease group")
            if any(lease.workload_id != workload.id or lease.generation != command.generation for lease in members):
                raise StaleOutboxCommand("gang cancel members no longer own the workload generation")
            if any(
                lease.state not in {LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.REVOKING, LeaseState.QUARANTINED}
                for lease in members
            ):
                raise StaleOutboxCommand("gang cancel lease is no longer active")
            cells = tuple([await transaction.get_cell(lease.cell_id) for lease in members])
            concrete_cells = tuple(cell for cell in cells if cell is not None)
            if len(concrete_cells) != len(cells):
                raise StaleOutboxCommand("gang cancel command references a missing execution cell")
            pairs = tuple(zip(members, concrete_cells, strict=True))

        remote_id = workload.remote_id or command.payload.get("remote_id")
        if remote_id:
            ordered = self._ordered_gang_member_pairs(pairs)
            request = GangWorkloadMutationRequest(
                members=self._gang_members(ordered, idempotency_prefix=command.idempotency_key)
            )
            coordinator_lease, coordinator_cell = ordered[0]
            await asyncio.to_thread(
                self._agent_factory(coordinator_cell).stop_gang,
                coordinator_lease.cell_id,
                workload.id,
                str(remote_id),
                request,
            )
        await self._release_terminal(workload, members[0], WorkloadState.CANCELLED, "workload.cancelled")

    async def _dispatch_checkpoint(self, command: OutboxRecord) -> None:
        workload, lease, cell = await self._workload_command_state(
            command,
            workload_states={WorkloadState.CHECKPOINTING},
            lease_states={LeaseState.ACTIVE, LeaseState.QUARANTINED},
        )
        hook = command.payload.get("hook")
        if not isinstance(hook, str) or not hook or not workload.remote_id:
            raise RuntimeError("checkpoint command requires a hook and active remote workload")
        output_uri = command.payload.get("output_uri")
        if output_uri is not None and not isinstance(output_uri, str):
            raise RuntimeError("checkpoint command output URI must be a string when set")
        request = CheckpointRequest(
            mutation=self._mutation(command, lease.fencing_token),
            hook=hook,
            output_uri=output_uri,
        )
        observation = await asyncio.to_thread(
            self._agent_factory(cell).checkpoint,
            cell.id,
            workload.remote_id,
            request,
        )
        await self._wait_for_checkpoint(
            self._agent_factory(cell),
            cell.id,
            observation.remote_id,
            timeout_s=self.settings.checkpoint_grace_s,
        )
        await self.service.transition_workload(
            self._principal,
            workload.id,
            WorkloadState.RUNNING,
            action="workload.checkpoint_completed",
            checkpoint_uri=observation.checkpoint_uri,
            expected_generation=command.generation,
            expected_lease_id=lease.id,
            expected_state=WorkloadState.CHECKPOINTING,
        )

    async def _dispatch_preemption(self, command: OutboxRecord) -> None:
        prepared = await self._prepare_preemption_plan(command)
        if prepared is None:
            raise StaleOutboxCommand("preemption plan is no longer admissible")
        if prepared.payload.get("execution_completed"):
            return

        phase = str(prepared.payload.get("phase") or "checkpointing")
        if phase == "checkpointing":
            checkpoint_uris: dict[str, str | None] = {}
            victim_outcomes: dict[str, _PreemptionVictimOutcome] = {}
            launched_hooks: list[_LaunchedCheckpointHook] = []
            try:
                for victim in self._preemption_victims(prepared):
                    prepared = await self._renew_preemption_claim(prepared)
                    state = await self._load_preemption_victim(prepared, victim)
                    if state is None:
                        continue
                    workload, lease, cell = state
                    checkpoint_uri: str | None = None
                    agent = self._agent_factory(cell)
                    if workload.remote_id:
                        source = await asyncio.to_thread(agent.workload, cell.id, workload.remote_id)
                        if source.status in _PREEMPTION_TERMINAL_WORKLOAD_STATUSES:
                            victim_outcomes[lease.id] = self._preemption_outcome(source.status)
                            checkpoint_uris[workload.id] = None
                            continue
                    if victim.checkpoint and victim.hook and workload.remote_id:
                        mutation = MutationModel(
                            idempotency_key=f"{prepared.idempotency_key}:{lease.id}",
                            generation=victim.generation,
                            fencing_token=lease.fencing_token,
                        )
                        request = CheckpointRequest(
                            mutation=mutation,
                            hook=victim.hook,
                            output_uri=victim.output_uri,
                        )
                        checkpoint = await asyncio.to_thread(
                            agent.checkpoint,
                            cell.id,
                            workload.remote_id,
                            request,
                        )
                        # Record it before waiting: a hook that times out is still
                        # running and still holding the victim's accelerators, and
                        # the abort path below is the only thing that can retire it.
                        launched_hooks.append(
                            _LaunchedCheckpointHook(
                                cell=cell,
                                remote_id=checkpoint.remote_id,
                                lease_id=lease.id,
                                generation=victim.generation,
                                fencing_token=lease.fencing_token,
                            )
                        )
                        await self._wait_for_checkpoint(
                            agent,
                            cell.id,
                            checkpoint.remote_id,
                            timeout_s=victim.grace_period_seconds or self.settings.checkpoint_grace_s,
                        )
                        checkpoint_uri = checkpoint.checkpoint_uri or victim.output_uri
                    checkpoint_uris[workload.id] = checkpoint_uri
                prepared = await self._advance_preemption_to_stopping(
                    prepared,
                    checkpoint_uris,
                    victim_outcomes,
                )
            except StaleOutboxCommand:
                raise
            except Exception as exc:
                # Rolling back durable state restores the victims to RUNNING. Any
                # hook still executing would then share the accelerators with the
                # trainer it was meant to checkpoint, and could still write output
                # under a plan that no longer exists.
                retire_errors = await self._retire_checkpoint_hooks(prepared, launched_hooks)
                reason = "; ".join([str(exc), *retire_errors])
                if await self._abort_preemption_checkpoint_phase(prepared, reason):
                    raise StaleOutboxCommand(f"preemption checkpoint phase aborted: {reason}") from exc
                prepared = await self._renew_preemption_claim(prepared)

        if str(prepared.payload.get("phase")) != "stopping":
            raise RuntimeError("started preemption plan has an invalid durable phase")
        stop_errors: list[str] = []
        for victim in self._preemption_victims(prepared):
            prepared = await self._renew_preemption_claim(prepared)
            outcomes = self._preemption_victim_outcomes(prepared)
            if victim.lease_id in outcomes:
                continue
            try:
                outcome = _PreemptionVictimOutcome.PREEMPTED
                state = await self._load_preemption_victim(prepared, victim)
                if state is not None:
                    workload, lease, cell = state
                    if workload.remote_id:
                        agent = self._agent_factory(cell)
                        observation = await asyncio.to_thread(agent.workload, cell.id, workload.remote_id)
                        status = observation.status
                        if status not in _PREEMPTION_TERMINAL_WORKLOAD_STATUSES:
                            request = WorkloadMutationRequest(
                                mutation=MutationModel(
                                    idempotency_key=f"{prepared.idempotency_key}:{lease.id}",
                                    generation=victim.generation,
                                    fencing_token=lease.fencing_token,
                                )
                            )
                            await asyncio.to_thread(agent.stop, cell.id, workload.remote_id, request)
                            status = await self._wait_for_workload_terminal(
                                agent,
                                cell.id,
                                workload.remote_id,
                                timeout_s=self.settings.preemption_stop_grace_s,
                            )
                        outcome = self._preemption_outcome(status)
                prepared = await self._record_preemption_victim_stopped(
                    prepared.id,
                    victim.lease_id,
                    outcome,
                )
            except StaleOutboxCommand:
                raise
            except CellAgentStaleMutationError:
                raise
            except Exception as exc:
                stop_errors.append(f"{victim.lease_id}: {exc}")
        if stop_errors:
            raise RuntimeError("preemption stop phase incomplete: " + "; ".join(stop_errors))

        checkpoint_uris = self._preemption_checkpoint_uris(prepared)
        await self._finalize_preemption_plan(prepared, checkpoint_uris)

    @staticmethod
    def _preemption_outcome(status: ExecutionWorkloadStatus) -> _PreemptionVictimOutcome:
        if status is ExecutionWorkloadStatus.SUCCEEDED:
            return _PreemptionVictimOutcome.SUCCEEDED
        if status is ExecutionWorkloadStatus.FAILED:
            return _PreemptionVictimOutcome.FAILED
        if status in {
            ExecutionWorkloadStatus.STOPPED,
            ExecutionWorkloadStatus.LOST,
            ExecutionWorkloadStatus.NOT_FOUND,
        }:
            return _PreemptionVictimOutcome.PREEMPTED
        raise RuntimeError(f"preemption outcome requires a terminal workload status, got {status.value}")

    @staticmethod
    def _preemption_victims(command: OutboxRecord) -> tuple[_PreemptionVictim, ...]:
        raw = command.payload.get("victims")
        if not isinstance(raw, list) or not raw:
            raise StaleOutboxCommand("preemption plan has no victim set")
        try:
            return tuple(_PreemptionVictim.model_validate(item) for item in raw)
        except ValidationError as exc:
            raise StaleOutboxCommand("preemption plan contains an invalid victim") from exc

    @staticmethod
    def _preemption_checkpoint_uris(command: OutboxRecord) -> dict[str, str | None]:
        raw = command.payload.get("checkpoint_uris", {})
        try:
            return _PreemptionCheckpointUris.model_validate(raw).root
        except ValidationError as exc:
            raise StaleOutboxCommand("preemption plan contains invalid checkpoint URIs") from exc

    @staticmethod
    def _preemption_victim_outcomes(command: OutboxRecord) -> dict[str, _PreemptionVictimOutcome]:
        raw = command.payload.get("victim_outcomes", {})
        try:
            return _PreemptionVictimOutcomes.model_validate(raw).root
        except ValidationError as exc:
            raise StaleOutboxCommand("preemption plan contains invalid victim outcomes") from exc

    @staticmethod
    def _preemption_stopped_victims(command: OutboxRecord) -> list[str]:
        raw = command.payload.get("stopped_victims", [])
        try:
            return _PreemptionStoppedVictims.model_validate(raw).root
        except ValidationError as exc:
            raise StaleOutboxCommand("preemption plan contains an invalid stopped-victim set") from exc

    async def _prepare_preemption_plan(self, command: OutboxRecord) -> OutboxRecord | None:
        async with self.repository.transaction() as transaction:
            current = await transaction.get_outbox(command.id)
            if current is None:
                raise StaleOutboxCommand("preemption command no longer exists")
            self._require_preemption_claim(current)
            if current.payload.get("execution_started"):
                return current

            victims = self._preemption_victims(current)
            victim_workload_ids = sorted({victim.workload_id for victim in victims})
            for workload_id in victim_workload_ids:
                await self.service._lock_settlement(transaction, workload_id)

            workload_ids = sorted({str(current.payload["target_workload_id"]), *victim_workload_ids})
            workloads = {workload_id: await transaction.get_workload(workload_id) for workload_id in workload_ids}
            lease_ids = sorted({victim.lease_id for victim in victims})
            leases = {lease_id: await transaction.get_lease(lease_id) for lease_id in lease_ids}
            target = workloads.get(str(current.payload["target_workload_id"]))

            scopes = sorted(
                {(candidate.org_id, candidate.project_id) for candidate in workloads.values() if candidate is not None}
            )
            for org_id, project_id in scopes:
                await self.service._lock_admission_scope(transaction, org_id, project_id)

            stale = (
                target is None
                or target.state is not WorkloadState.PENDING
                or target.generation != int(current.payload["target_generation"])
                or target.attempt != int(current.payload["target_attempt"])
                or target.preemption_id != current.id
                or self.service.scheduler is None
            )
            for victim in victims:
                workload = workloads.get(victim.workload_id)
                lease = leases.get(victim.lease_id)
                stale = stale or (
                    workload is None
                    or lease is None
                    or workload.generation != victim.generation
                    or workload.lease_id != victim.lease_id
                    or workload.preemption_id != current.id
                    or lease.generation != victim.generation
                    or lease.cell_id != victim.cell_id
                    or lease.revocation_reason != "preemption"
                    or workload.state not in {WorkloadState.CHECKPOINTING, WorkloadState.CANCELLING}
                    or lease.state not in {LeaseState.REVOKING, LeaseState.QUARANTINED}
                )

            if not stale and target is not None and self.service.scheduler is not None:
                cell = await transaction.get_cell(str(current.payload["cell_id"]))
                stale = cell is None or cell.state is not CellState.READY
                if not stale:
                    stale = any(
                        lease is None or lease.cell_id != cell.id or lease.cell_generation != cell.generation
                        for lease in leases.values()
                    )
                policy = await transaction.get_policy(target.org_id, target.project_id)
                if not stale and policy is not None:
                    try:
                        self.service._validate_workload_policy(target.spec, policy)
                    except InputError:
                        stale = True
                if not stale:
                    budget = await transaction.get_budget(target.org_id, target.project_id)
                    active_leases = [
                        candidate
                        for candidate in await transaction.list_leases()
                        if candidate.state in ACTIVE_LEASE_STATES
                    ]
                    active_workload_ids = {candidate.workload_id for candidate in active_leases}
                    validation_workloads = [
                        candidate
                        for candidate in await transaction.list_workloads()
                        if candidate.id == target.id or candidate.id in active_workload_ids
                    ]
                    refreshed = self.service.scheduler.schedule(
                        validation_workloads,
                        await transaction.list_cells(),
                        leases=active_leases,
                        budgets=() if budget is None else (budget,),
                        quotas=await transaction.list_quotas(org_id=target.org_id),
                        policies=await transaction.list_policies(),
                    )
                    current_plan = next(
                        (candidate for candidate in refreshed.preemptions if candidate.workload_id == target.id),
                        None,
                    )
                    expected_lease_ids = tuple(victim.lease_id for victim in victims)
                    stale = (
                        current_plan is None
                        or current_plan.cell_id != str(current.payload["cell_id"])
                        or tuple(victim.lease_id for victim in current_plan.victims) != expected_lease_ids
                    )

            if stale:
                await self._rollback_preemption_plan(transaction, current, workloads, leases)
                return None

            prepared = current.model_copy(
                update={
                    "payload": {
                        **current.payload,
                        "execution_started": True,
                        "execution_started_at": utcnow(),
                        "phase": "checkpointing",
                        "checkpoint_uris": {},
                        "victim_outcomes": {},
                        "stopped_victims": [],
                    },
                    "updated_at": utcnow(),
                }
            )
            await transaction.put_outbox(prepared)
            return prepared

    def _require_preemption_claim(self, command: OutboxRecord) -> None:
        if (
            command.state is not OutboxState.PROCESSING
            or command.claimed_by != self.worker_id
            or command.claimed_until is None
            or command.claimed_until <= utcnow()
        ):
            raise StaleOutboxCommand("preemption command claim expired or moved to another worker")

    async def _renew_preemption_claim(self, command: OutboxRecord) -> OutboxRecord:
        """Renew the current preemption command through the shared claim fence."""

        if command.topic != "workload.preempt":
            raise StaleOutboxCommand(f"outbox command {command.id!r} is not a preemption command")
        return await self._renew_outbox_claim(command)

    async def _advance_preemption_to_stopping(
        self,
        command: OutboxRecord,
        checkpoint_uris: dict[str, str | None],
        victim_outcomes: dict[str, _PreemptionVictimOutcome],
    ) -> OutboxRecord:
        async with self.repository.transaction() as transaction:
            current = await transaction.get_outbox(command.id)
            if current is None:
                raise StaleOutboxCommand("preemption command no longer exists")
            self._require_preemption_claim(current)
            phase = str(current.payload.get("phase") or "checkpointing")
            if phase == "stopping":
                return current
            if phase != "checkpointing" or current.payload.get("execution_completed"):
                raise StaleOutboxCommand("preemption checkpoint phase is no longer active")
            advanced = current.model_copy(
                update={
                    "payload": {
                        **current.payload,
                        "phase": "stopping",
                        "checkpoint_uris": checkpoint_uris,
                        "victim_outcomes": {lease_id: outcome.value for lease_id, outcome in victim_outcomes.items()},
                        "stopped_victims": self._preemption_stopped_victims(current),
                        "stopping_started_at": utcnow(),
                    },
                    "claimed_until": utcnow() + timedelta(seconds=max(self.settings.outbox_claim_ttl_s, 1)),
                    "updated_at": utcnow(),
                }
            )
            await transaction.put_outbox(advanced)
            return advanced

    async def _record_preemption_victim_stopped(
        self,
        command_id: str,
        lease_id: str,
        outcome: _PreemptionVictimOutcome,
    ) -> OutboxRecord:
        async with self.repository.transaction() as transaction:
            current = await transaction.get_outbox(command_id)
            if current is None:
                raise StaleOutboxCommand("preemption command no longer exists")
            self._require_preemption_claim(current)
            if str(current.payload.get("phase")) != "stopping" or current.payload.get("execution_completed"):
                raise StaleOutboxCommand("preemption stop phase is no longer active")
            stopped = list(dict.fromkeys([*self._preemption_stopped_victims(current), lease_id]))
            outcomes = {**self._preemption_victim_outcomes(current), lease_id: outcome}
            updated = current.model_copy(
                update={
                    "payload": {
                        **current.payload,
                        "stopped_victims": stopped,
                        "victim_outcomes": {
                            victim_lease_id: victim_outcome.value for victim_lease_id, victim_outcome in outcomes.items()
                        },
                    },
                    "claimed_until": utcnow() + timedelta(seconds=max(self.settings.outbox_claim_ttl_s, 1)),
                    "updated_at": utcnow(),
                }
            )
            await transaction.put_outbox(updated)
            return updated

    async def _rollback_preemption_plan(
        self,
        transaction: RepositorySession,
        command: OutboxRecord,
        workloads: dict[str, WorkloadRecord | None],
        leases: dict[str, LeaseRecord | None],
    ) -> None:
        now = utcnow()
        for victim in self._preemption_victims(command):
            workload = workloads.get(victim.workload_id)
            lease = leases.get(victim.lease_id)
            if (
                workload is None
                or lease is None
                or workload.generation != victim.generation
                or workload.lease_id != lease.id
                or workload.preemption_id != command.id
                or lease.revocation_reason != "preemption"
                or workload.state not in {WorkloadState.CHECKPOINTING, WorkloadState.CANCELLING}
                or lease.state not in {LeaseState.REVOKING, LeaseState.QUARANTINED}
            ):
                continue
            restored_workload = workload.model_copy(
                update={"state": victim.workload_state, "preemption_id": None, "updated_at": now}
            )
            restored_lease = lease.model_copy(
                update={"state": victim.lease_state, "revocation_reason": None, "updated_at": now}
            )
            await transaction.put_workload(restored_workload)
            await transaction.put_lease(restored_lease)
            if victim.workload_state is WorkloadState.LEASED and workload.remote_id is None:
                await self.service._outbox(
                    transaction,
                    topic="workload.dispatch",
                    aggregate_type="workload",
                    aggregate_id=workload.id,
                    idempotency_key=f"dispatch:{lease.id}:{lease.generation}:rollback:{command.id}",
                    generation=lease.generation,
                    payload={"workload_id": workload.id, "lease_id": lease.id, "cell_id": lease.cell_id},
                )

        target = workloads.get(str(command.payload["target_workload_id"]))
        if target is not None and target.state is WorkloadState.PENDING and target.preemption_id == command.id:
            await transaction.put_workload(
                target.model_copy(
                    update={
                        "preemption_id": None,
                        "scheduling_reason": "preemption invalidated; waiting for scheduling",
                        "updated_at": now,
                    }
                )
            )

    async def _retire_checkpoint_hooks(
        self,
        command: OutboxRecord,
        hooks: tp.Sequence[_LaunchedCheckpointHook],
    ) -> list[str]:
        """Retire every hook this attempt launched; return the failures as text.

        Best effort by design. A hook that cannot be retired is worse than one
        that can, but it is strictly better to roll the victims back to RUNNING
        and surface the stray unit in ``aborted_reason`` than to leave them
        pinned in CHECKPOINTING because cleanup failed.
        """

        errors: list[str] = []
        for hook in hooks:
            request = WorkloadMutationRequest(
                mutation=MutationModel(
                    idempotency_key=f"{command.idempotency_key}:{hook.lease_id}:hook-retire",
                    generation=hook.generation,
                    fencing_token=hook.fencing_token,
                )
            )
            try:
                await asyncio.to_thread(
                    self._agent_factory(hook.cell).retire,
                    hook.cell.id,
                    hook.remote_id,
                    request,
                )
            except Exception as exc:  # reported, never raised
                errors.append(f"checkpoint hook {hook.remote_id} was not retired: {exc}")
                logger.warning(
                    "orphaned checkpoint hook %s on cell %s after aborted preemption %s: %s",
                    hook.remote_id,
                    hook.cell.id,
                    command.id,
                    exc,
                )
        return errors

    async def _abort_preemption_checkpoint_phase(self, command: OutboxRecord, reason: str) -> bool:
        victims = self._preemption_victims(command)
        async with self.repository.transaction() as transaction:
            current = await transaction.get_outbox(command.id)
            if current is None:
                raise StaleOutboxCommand("preemption command no longer exists")
            self._require_preemption_claim(current)
            if str(current.payload.get("phase") or "checkpointing") != "checkpointing":
                return False
            for workload_id in sorted({victim.workload_id for victim in victims}):
                await self.service._lock_settlement(transaction, workload_id)
            workload_ids = sorted(
                {str(command.payload["target_workload_id"]), *(victim.workload_id for victim in victims)}
            )
            workloads = {workload_id: await transaction.get_workload(workload_id) for workload_id in workload_ids}
            leases = {victim.lease_id: await transaction.get_lease(victim.lease_id) for victim in victims}
            scopes = sorted(
                {(workload.org_id, workload.project_id) for workload in workloads.values() if workload is not None}
            )
            for org_id, project_id in scopes:
                await self.service._lock_admission_scope(transaction, org_id, project_id)
            await self._rollback_preemption_plan(transaction, current, workloads, leases)
            await transaction.put_outbox(
                current.model_copy(
                    update={
                        "payload": {
                            **current.payload,
                            "execution_completed": True,
                            "aborted_reason": reason[:1000],
                            "execution_completed_at": utcnow(),
                        },
                        "updated_at": utcnow(),
                    }
                )
            )
            return True

    async def _load_preemption_victim(
        self,
        command: OutboxRecord,
        victim: _PreemptionVictim,
    ) -> tuple[WorkloadRecord, LeaseRecord, CellRecord] | None:
        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(victim.workload_id)
            lease = await transaction.get_lease(victim.lease_id)
            if (
                workload is not None
                and lease is not None
                and lease.state is LeaseState.RELEASED
                and workload.generation > victim.generation
                and workload.lease_id != lease.id
            ):
                return None
            cell = await transaction.get_cell(victim.cell_id)
            if workload is None or lease is None or cell is None:
                raise RuntimeError("started preemption plan references a missing workload, lease, or cell")
            if (
                workload.generation != victim.generation
                or workload.lease_id != lease.id
                or workload.preemption_id != command.id
                or lease.generation != workload.generation
                or lease.cell_id != victim.cell_id
                or lease.revocation_reason != "preemption"
                or workload.state not in {WorkloadState.CHECKPOINTING, WorkloadState.CANCELLING}
                or lease.state not in {LeaseState.REVOKING, LeaseState.QUARANTINED}
            ):
                raise RuntimeError("started preemption plan lost ownership of a victim")
            return workload, lease, cell

    async def _finalize_preemption_plan(
        self,
        command: OutboxRecord,
        checkpoint_uris: dict[str, str | None],
    ) -> None:
        victims = self._preemption_victims(command)
        async with self.repository.transaction() as transaction:
            current_command = await transaction.get_outbox(command.id)
            if current_command is None:
                raise StaleOutboxCommand("preemption command no longer exists")
            self._require_preemption_claim(current_command)
            if current_command.payload.get("execution_completed"):
                return
            outcomes = self._preemption_victim_outcomes(current_command)
            expected_lease_ids = {victim.lease_id for victim in victims}
            if str(current_command.payload.get("phase")) != "stopping" or not expected_lease_ids <= outcomes.keys():
                raise RuntimeError("preemption plan cannot finalize before every victim is terminal")
            for workload_id in sorted({victim.workload_id for victim in victims}):
                await self.service._lock_settlement(transaction, workload_id)
            workloads = {victim.workload_id: await transaction.get_workload(victim.workload_id) for victim in victims}
            leases = {victim.lease_id: await transaction.get_lease(victim.lease_id) for victim in victims}
            scopes = sorted(
                {(workload.org_id, workload.project_id) for workload in workloads.values() if workload is not None}
            )
            for org_id, project_id in scopes:
                await self.service._lock_admission_scope(transaction, org_id, project_id)
                scope = storage_scope(org_id, project_id)
                await transaction.get_idempotency(scope, "budget.settle", scope)

            for victim in victims:
                workload = workloads.get(victim.workload_id)
                lease = leases.get(victim.lease_id)
                if (
                    workload is not None
                    and lease is not None
                    and lease.state is LeaseState.RELEASED
                    and workload.generation > victim.generation
                    and workload.lease_id != lease.id
                ):
                    continue
                if (
                    workload is None
                    or lease is None
                    or workload.generation != victim.generation
                    or workload.lease_id != lease.id
                    or workload.preemption_id != command.id
                    or lease.revocation_reason != "preemption"
                    or workload.state not in {WorkloadState.CHECKPOINTING, WorkloadState.CANCELLING}
                    or lease.state not in {LeaseState.REVOKING, LeaseState.QUARANTINED}
                ):
                    raise RuntimeError("started preemption plan cannot settle a victim it no longer owns")
                outcome = outcomes[lease.id]
                if outcome in {_PreemptionVictimOutcome.SUCCEEDED, _PreemptionVictimOutcome.FAILED}:
                    target_state = (
                        WorkloadState.SUCCEEDED
                        if outcome is _PreemptionVictimOutcome.SUCCEEDED
                        else WorkloadState.FAILED
                    )
                    terminal = await self.service._transition_workload(
                        transaction,
                        self._principal,
                        workload,
                        target_state,
                        f"workload.{outcome.value}_during_preemption",
                    )
                    terminal = terminal.model_copy(update={"preemption_id": None, "updated_at": utcnow()})
                    await transaction.put_workload(terminal)
                    await self.service._settle_lease(
                        transaction,
                        self._principal,
                        terminal,
                        lease,
                        reason=outcome.value,
                    )
                    continue
                settled, _, _ = await self.service._settle_lease(
                    transaction,
                    self._principal,
                    workload,
                    lease,
                    reason="preempted",
                )
                now = utcnow()
                checkpoint_uri = checkpoint_uris.get(workload.id) or workload.checkpoint_uri
                next_generation = workload.generation + 1
                resumed = workload.model_copy(
                    update={
                        "state": WorkloadState.PENDING,
                        "attempt": workload.attempt + 1,
                        "generation": next_generation,
                        "spec": self.service.result_spec_for_generation(
                            workload.spec,
                            workload_id=workload.id,
                            generation=next_generation,
                        ),
                        "lease_id": None,
                        "remote_id": None,
                        "checkpoint_uri": checkpoint_uri,
                        "scheduling_reason": "preempted; waiting to resume",
                        "preemption_id": None,
                        "updated_at": now,
                    }
                )
                await transaction.put_workload(resumed)
                await self.service._audit(
                    transaction,
                    self._principal,
                    resumed,
                    "workload.preempted",
                    previous_state=workload.state.value,
                    new_state=resumed.state.value,
                    payload={"checkpoint_uri": checkpoint_uri, "released_lease_id": settled.id},
                )

            await transaction.put_outbox(
                current_command.model_copy(
                    update={
                        "payload": {
                            **current_command.payload,
                            "execution_completed": True,
                            "execution_completed_at": utcnow(),
                        },
                        "updated_at": utcnow(),
                    }
                )
            )
            target = await transaction.get_workload(str(command.payload["target_workload_id"]))
            if target is not None and target.preemption_id == command.id:
                await transaction.put_workload(target.model_copy(update={"preemption_id": None, "updated_at": utcnow()}))

    async def _wait_for_checkpoint(
        self,
        agent: CellAgentClient,
        cell_id: str,
        checkpoint_job_id: str,
        *,
        timeout_s: int,
    ) -> None:
        deadline = asyncio.get_running_loop().time() + max(timeout_s, 1)
        while True:
            observation = await asyncio.to_thread(agent.workload, cell_id, checkpoint_job_id)
            status = observation.status
            if status is ExecutionWorkloadStatus.SUCCEEDED:
                return
            if status in {ExecutionWorkloadStatus.FAILED, ExecutionWorkloadStatus.STOPPED}:
                raise PreemptionCheckpointError(f"checkpoint hook {checkpoint_job_id!r} ended in {status.value}")
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(f"checkpoint hook {checkpoint_job_id!r} exceeded {timeout_s}s grace")
            await asyncio.sleep(1)

    async def _wait_for_workload_terminal(
        self,
        agent: CellAgentClient,
        cell_id: str,
        remote_id: str,
        *,
        timeout_s: int,
    ) -> ExecutionWorkloadStatus:
        deadline = asyncio.get_running_loop().time() + max(timeout_s, 1)
        while True:
            observation = await asyncio.to_thread(agent.workload, cell_id, remote_id)
            if observation.status in _PREEMPTION_TERMINAL_WORKLOAD_STATUSES:
                return observation.status
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(f"remote workload {remote_id!r} did not stop within {timeout_s}s grace")
            await asyncio.sleep(1)

    async def _release_terminal(
        self,
        workload: WorkloadRecord,
        lease: LeaseRecord,
        target: WorkloadState,
        action: str,
        *,
        failure_reason: str | None = None,
    ) -> None:
        if workload.lease_id != lease.id:
            raise StaleOutboxCommand(f"lease {lease.id!r} no longer owns workload {workload.id!r}")
        await self.service.settle_terminal_workload(
            self._principal,
            workload.id,
            target,
            action=action,
            failure_reason=failure_reason,
            expected_generation=workload.generation,
            expected_lease_id=lease.id,
            expected_state=workload.state,
        )

    async def _finish_outbox(
        self,
        command: OutboxRecord,
        *,
        error: str | None = None,
        failure_code: CapacityProviderFailureCode | None = None,
        transient: bool = False,
        terminal: bool = False,
    ) -> bool:
        """Settle an outbox command only while this controller still owns its live claim.

        ``transient`` retries soon without spending an attempt. ``terminal``
        settles the command for good, keeping its diagnostic: it is for answers
        that will never change on a retry, such as a backend reporting that it
        does not implement the operation at all.
        """

        now = utcnow()
        async with self.repository.transaction() as transaction:
            current = await transaction.get_outbox(command.id)
            if (
                current is None
                or current.state is not OutboxState.PROCESSING
                or current.claimed_by != self.worker_id
                or current.claimed_until is None
                or current.claimed_until <= now
            ):
                return False
            if error is None:
                updated = current.model_copy(
                    update={
                        "state": OutboxState.SUCCEEDED,
                        "claimed_by": None,
                        "claimed_until": None,
                        "last_error": None,
                        "failure_code": None,
                        "updated_at": now,
                    }
                )
            elif terminal or failure_code is CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED:
                updated = current.model_copy(
                    update={
                        "state": OutboxState.DISCARDED,
                        "claimed_by": None,
                        "claimed_until": None,
                        "last_error": error[:2000],
                        "failure_code": failure_code,
                        "updated_at": now,
                    }
                )
            else:
                delay = 1 if transient else min(300, 5 * (2 ** min(current.attempts - 1, 6)))
                updated = current.model_copy(
                    update={
                        "state": OutboxState.FAILED,
                        "attempts": max(0, current.attempts - 1) if transient else current.attempts,
                        "claimed_by": None,
                        "claimed_until": None,
                        "last_error": error[:2000],
                        "failure_code": failure_code,
                        "available_at": now + timedelta(seconds=delay),
                        "updated_at": now,
                    }
                )
            await transaction.put_outbox(updated)
        if error is not None and command.topic == "capacity.apply" and not transient:
            await self._record_capacity_failure(command, error, failure_code=failure_code)
        return True

    async def _discard_outbox(self, command: OutboxRecord, *, provider_diagnostic: str | None = None) -> None:
        """Finish a superseded command without erasing its prior provider diagnostic."""

        now = utcnow()
        async with self.repository.transaction() as transaction:
            current = await transaction.get_outbox(command.id)
            if (
                current is None
                or current.state is not OutboxState.PROCESSING
                or current.claimed_by != self.worker_id
                or current.claimed_until is None
                or current.claimed_until <= now
            ):
                return
            retained_diagnostic = current.last_error if current.last_error is not None else provider_diagnostic
            await transaction.put_outbox(
                current.model_copy(
                    update={
                        "state": OutboxState.DISCARDED,
                        "claimed_by": None,
                        "claimed_until": None,
                        "last_error": None if retained_diagnostic is None else retained_diagnostic[:2000],
                        "updated_at": now,
                    }
                )
            )

    async def _record_capacity_stockout(self, cell: CellRecord) -> None:
        """Persist a STOCKOUT belief for a cell reclaimed before it ever became READY.

        Guarded on the cell's generation so a concurrent recreate does not have its
        fresh belief overwritten by this late write.
        """

        availability = self._observe_capacity_belief(cell, ProvisionOutcome.STOCKOUT)
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if current is None or current.generation != cell.generation:
                return
            await transaction.put_cell(current.model_copy(update={"availability": availability, "updated_at": utcnow()}))

    @staticmethod
    def _observe_capacity_belief(
        cell: CellRecord,
        outcome: ProvisionOutcome,
        *,
        latency_s: float | None = None,
    ) -> CapacityBeliefState:
        current = cell.availability
        belief = CapacityBelief(
            key=CapacityPoolKey(
                provider=cell.provider,
                project=cell.provider_project or "",
                zone=cell.zone or cell.region,
                accelerator_type=cell.accelerator_type or "unknown",
                capacity_class=cell.capacity_class.value,
            ),
            success_evidence=current.success_evidence,
            failure_evidence=current.failure_evidence,
            provision_latencies_s=current.provision_latencies_s,
            interruptions=current.interruptions,
            successful_hours=current.successful_hours,
            consecutive_failures=current.consecutive_failures,
            cooldown_until=current.cooldown_until,
            last_observed_at=current.last_observed_at,
        ).observe(outcome, latency_s=latency_s)
        return CapacityBeliefState(
            success_evidence=belief.success_evidence,
            failure_evidence=belief.failure_evidence,
            provision_latencies_s=belief.provision_latencies_s,
            interruptions=belief.interruptions,
            successful_hours=belief.successful_hours,
            consecutive_failures=belief.consecutive_failures,
            cooldown_until=belief.cooldown_until,
            last_observed_at=belief.last_observed_at,
        )

    async def _record_capacity_failure(
        self,
        command: OutboxRecord,
        error: str,
        *,
        failure_code: CapacityProviderFailureCode | None,
    ) -> None:
        transition: tuple[CellRecord, CellRecord] | None = None
        async with self.repository.transaction() as transaction:
            cell = await transaction.get_cell(command.aggregate_id)
            if cell is None or cell.generation != command.generation or cell.state is not CellState.REGISTERING:
                return
            lowered = error.lower()
            if failure_code is CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED:
                outcome = ProvisionOutcome.PROVIDER_ERROR
            elif "quota" in lowered:
                outcome = ProvisionOutcome.QUOTA_ERROR
            elif "stockout" in lowered or "capacity" in lowered:
                outcome = ProvisionOutcome.STOCKOUT
            else:
                outcome = ProvisionOutcome.PROVIDER_ERROR
            observed_at = utcnow()
            unavailable_reason = failure_code.value if failure_code is not None else "provider_failure"
            updated = cell.model_copy(
                update={
                    "state": CellState.UNAVAILABLE,
                    "provider_phase": CapacityStatus.PROVIDER_ERROR,
                    "provider_failure_code": failure_code,
                    "provider_phase_started_at": _provider_phase_start(
                        cell,
                        CapacityStatus.PROVIDER_ERROR,
                        observed_at,
                    ),
                    "unavailable_reason": unavailable_reason,
                    "availability": self._observe_capacity_belief(cell, outcome),
                    "updated_at": observed_at,
                }
            )
            await transaction.put_cell(updated)
            if failure_code is CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED:
                await self._fail_autoscale_workload_for_queue_admission_denial(transaction, updated, error)
            transition = cell, updated
        if transition is not None:
            self._capacity_lifecycle.transition(*transition, reason=error)

    @staticmethod
    async def _has_alternative_pool(
        transaction: RepositorySession,
        workload: WorkloadRecord,
        *,
        denied_cell: CellRecord,
    ) -> bool:
        """Return whether another enabled pool could still host this workload.

        Deliberately coarse: it asks whether an *enabled pool of a different
        shape* exists that the workload's declared portfolio admits, not whether
        that pool has capacity right now. Capacity is what the next reconcile
        cycle decides, and predicting it here would duplicate the planner while
        being wrong more often.
        """

        spec = workload.spec
        classes = {entry.value for entry in spec.capacity_class_portfolio}
        accelerators = set(spec.allowed_accelerator_types) | (
            {spec.accelerator_type} if spec.accelerator_type else set()
        )
        regions = set(spec.allowed_regions) | ({spec.region} if spec.region else set())
        for pool in await transaction.list_capacity_pools():
            if not pool.enabled or pool.id == denied_cell.pool_id:
                continue
            if str(pool.capacity_class) not in classes:
                continue
            if accelerators and pool.accelerator_type not in accelerators:
                continue
            if regions and pool.region not in regions:
                continue
            return True
        return False

    async def _fail_autoscale_workload_for_queue_admission_denial(
        self,
        transaction: RepositorySession,
        cell: CellRecord,
        diagnostic: str,
    ) -> None:
        """Block the pending workload that owns a denied queue request.

        **Terminal only when this pool was the last option.** A denial says this
        *shape in this zone* is unavailable; with a portfolio the workload has
        other admissible pools, and failing it outright discards a run the fleet
        could still place. When another pool remains, the workload stays PENDING
        with ``autoscale_block_reason`` set for explainability, and the ordinary
        reconcile cycle tries the next-best pool -- which is machinery that
        already ships, because ``cell_provides_pool_supply`` stops counting a
        failed cell as supply and ``CapacitySimulation.build`` already generates a
        candidate per admissible pool.
        """

        workload_id = cell.autoscale_request_workload_id
        if workload_id is None:
            return
        workload = await transaction.get_workload(workload_id)
        if workload is None or workload.state is not WorkloadState.PENDING:
            return
        failure_reason = safe_provider_diagnostic(diagnostic)
        if await self._has_alternative_pool(transaction, workload, denied_cell=cell):
            # Not terminal. Record why this attempt stalled so ``workload explain``
            # can say "one pool refused, others remain" rather than going quiet.
            await transaction.put_workload(
                workload.model_copy(
                    update={
                        "autoscale_block_reason": AutoscaleBlockReason.QUEUE_ADMISSION_DENIED,
                        "scheduling_reason": (f"{failure_reason}; other admissible pools remain, waiting to retry"),
                        "updated_at": utcnow(),
                    }
                )
            )
            return
        project = cell.provider_project
        zone = cell.zone
        accelerator_type = cell.accelerator_type
        target = (
            None
            if cell.provider != "gcp" or project is None or zone is None or accelerator_type is None
            else GcpQueueAdmissionDenialTarget(
                provider_project=project,
                zone=zone,
                accelerator_type=accelerator_type,
                capacity_class=cell.capacity_class,
            )
        )
        await self.service._transition_workload(
            transaction,
            self._principal,
            workload,
            WorkloadState.FAILED,
            "workload.queue_admission_denied",
            updates={
                "failure_reason": failure_reason,
                "autoscale_block_reason": AutoscaleBlockReason.QUEUE_ADMISSION_DENIED,
                "queue_admission_denial": target,
                "scheduling_reason": None,
            },
        )

    async def _record_cell_agent_fence_conflict(
        self,
        *,
        cell_id: str,
        generation: int,
        diagnostic: str,
    ) -> None:
        """Mark a cell for replacement without treating an agent fence conflict as cloud failure."""

        transition: tuple[CellRecord, CellRecord] | None = None
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell_id)
            if (
                current is None
                or current.generation != generation
                or current.state not in {CellState.REGISTERING, CellState.READY, CellState.UNAVAILABLE}
            ):
                return
            observed_at = utcnow()
            updated = current.model_copy(
                update={
                    "state": CellState.UNAVAILABLE,
                    "provider_phase": CapacityStatus.NEEDS_RECREATE,
                    "provider_phase_started_at": _provider_phase_start(
                        current,
                        CapacityStatus.NEEDS_RECREATE,
                        observed_at,
                    ),
                    "unavailable_reason": "agent_fence_conflict",
                    "unavailable_detail": diagnostic[:2048],
                    "availability": self._observe_capacity_belief(current, ProvisionOutcome.INTERRUPTED),
                    "updated_at": observed_at,
                }
            )
            await transaction.put_cell(updated)
            transition = current, updated
        if transition is not None:
            self._capacity_lifecycle.transition(*transition, reason=diagnostic)
        if transition is None:
            return
        _previous, unavailable = transition
        if unavailable.pool_id is not None:
            return
        try:
            recreated, replayed = await self.service.recreate_cell(
                self._principal,
                unavailable.id,
                expected_generation=unavailable.generation,
                idempotency_key=f"agent-fence-recreate:{unavailable.id}:g{unavailable.generation}",
            )
        except (ConflictError, NotFoundError) as exc:
            logger.info(
                "cell %s will wait for lease-safe recreation after an agent fence conflict: %s", unavailable.id, exc
            )
            return
        if not replayed:
            self._capacity_lifecycle.transition(
                unavailable,
                recreated,
                reason="cell-agent fence conflict requested a fresh capacity generation",
            )

    async def release_ready_dag_nodes_once(self) -> int:
        """Place-enable DAG nodes whose predecessors have all succeeded.

        A gated node is stored ``schedulable=False``, which the scheduler already
        filters on, so releasing it is a single flag flip rather than a second
        admission concept.

        A predecessor that FAILED or was CANCELLED cancels its dependents instead of
        stranding them. Leaving them PENDING forever is the same defect the orphan
        sweep exists to clear -- a workload nothing will ever settle, counted against
        quota and fair share while holding nothing -- and CANCELLED states the truth:
        this never ran because what it waited for did not succeed.
        """

        released = 0
        async with self.repository.transaction() as transaction:
            gated = [
                workload
                for workload in await transaction.list_workloads(state=WorkloadState.PENDING)
                if workload.depends_on and not workload.schedulable
            ]
            predecessor_states = {
                workload.id: workload.state
                for workload in await transaction.list_workloads()
                if any(workload.id in node.depends_on for node in gated)
            }

        for node in gated:
            states = [predecessor_states.get(predecessor) for predecessor in node.depends_on]
            if any(state in {WorkloadState.FAILED, WorkloadState.CANCELLED} or state is None for state in states):
                # A missing predecessor counts as failed: it was pruned or lost, and
                # a dependent that can never learn its outcome must not wait forever.
                try:
                    await self.service.cancel_workload(
                        self._principal,
                        node.id,
                        idempotency_key=f"dag:abandon:{node.id}:g{node.generation}",
                    )
                except (ConflictError, NotFoundError) as exc:
                    logger.warning("could not abandon DAG node %s: %s", node.id, exc)
                    continue
                released += 1
                continue
            if not all(state is WorkloadState.SUCCEEDED for state in states):
                continue
            async with self.repository.transaction() as transaction:
                current = await transaction.get_workload(node.id)
                if current is None or current.schedulable or current.state is not WorkloadState.PENDING:
                    continue
                await transaction.put_workload(
                    current.model_copy(
                        update={
                            "schedulable": True,
                            "scheduling_reason": "dependencies satisfied; waiting for scheduling",
                            "updated_at": utcnow(),
                        }
                    )
                )
                await transaction.append_event(
                    AuditEvent(
                        id=f"evt_{uuid.uuid4().hex}",
                        actor=self._principal.subject,
                        org_id=current.org_id,
                        project_id=current.project_id,
                        entity_type="workload",
                        entity_id=current.id,
                        action="workload.dag_released",
                    )
                )
            released += 1
        return released

    async def release_admitted_groups_once(self) -> int:
        """Release a heterogeneous group once every member can be placed.

        A group's members are differently shaped, so they cannot be one gang. Left
        as independent workloads, the members that fit lease immediately and burn
        accelerator hours for as long as the members that do not stay unplaceable.
        Gating them all on the whole group's feasibility is the one thing groups buy
        over siblings.

        The check is a *feasibility* gate, not a reservation, and the difference is
        worth stating where it is implemented: between this answer and the members'
        actual leases another workload can take the capacity, because the core
        scheduler has no reservation primitive. What this prevents is half a group
        holding accelerators while the rest is structurally infeasible -- the common
        failure -- not a race between two placement passes.

        Members are evaluated as if schedulable, since the scheduler filters out the
        flag this method exists to flip; and they are released in one transaction, so
        a leader that dies mid-release cannot leave a group half-open.
        """

        released = 0
        async with self.repository.transaction() as transaction:
            coordinators = [
                workload
                for workload in await transaction.list_workloads(state=WorkloadState.PENDING)
                if workload.coordination_kind is CoordinationKind.GROUP
            ]
            members_by_parent = {
                coordinator.id: await transaction.list_child_workloads(coordinator.id) for coordinator in coordinators
            }

        for coordinator in coordinators:
            members = members_by_parent.get(coordinator.id, [])
            gated = [member for member in members if not member.schedulable]
            if not gated or any(member.state is not WorkloadState.PENDING for member in gated):
                continue
            feasible = await self.service.group_is_placeable(
                self._principal, member_ids=tuple(member.id for member in gated)
            )
            if not feasible:
                continue
            async with self.repository.transaction() as transaction:
                current = [await transaction.get_workload(member.id) for member in gated]
                if any(item is None or item.schedulable or item.state is not WorkloadState.PENDING for item in current):
                    # State moved under us. Re-evaluate next cycle rather than
                    # releasing a partial group, which is the exact outcome the
                    # gate exists to prevent.
                    continue
                for item in current:
                    assert item is not None
                    await transaction.put_workload(
                        item.model_copy(
                            update={
                                "schedulable": True,
                                "scheduling_reason": "group admitted; waiting for scheduling",
                                "updated_at": utcnow(),
                            }
                        )
                    )
                    await transaction.append_event(
                        AuditEvent(
                            id=f"evt_{uuid.uuid4().hex}",
                            actor=self._principal.subject,
                            org_id=item.org_id,
                            project_id=item.project_id,
                            entity_type="workload",
                            entity_id=item.id,
                            action="workload.group_released",
                            payload={"group_id": coordinator.id, "members": len(current)},
                        )
                    )
                released += len(current)
        return released

    async def accrue_cell_billing_once(self) -> int:
        """Advance the fleet's cell billing clocks and return the cells charged.

        Delegates the arithmetic to the service so the accrual runs in one
        transaction with the lease read that decides which cells were idle -- a
        lease released between the two reads would otherwise make a busy cell
        look idle for the whole interval.
        """

        accrual = await self.service.accrue_cell_billing()
        if accrual.idle_seconds > 0:
            logger.debug(
                "Accrued %.0fs across %d cells, %.0fs idle (%.4f idle cost)",
                accrual.accrued_seconds,
                accrual.cells,
                accrual.idle_seconds,
                accrual.idle_cost,
            )
        return accrual.cells

    async def settle_orphaned_workloads_once(self) -> int:
        """Settle a live workload whose lease or cell row no longer exists.

        ``observe_workloads_once`` skips such a workload -- it cannot observe what it
        cannot resolve -- so without this it stays RUNNING forever: never polled,
        never settled, and counted against its project's quota, budget and fair share
        while holding no capacity at all. One hand-repaired or lost row therefore
        permanently shrinks a tenant's effective quota.

        Guarded by a grace period rather than firing on the first miss. Nothing should
        write a workload and its lease in separate transactions -- dispatch commits
        both together -- but settling a workload is irreversible, so requiring the
        condition to persist past the lease TTL costs one cycle and removes the whole
        class of "a race made this look orphaned".
        """

        grace = timedelta(seconds=max(self.settings.lease_ttl_s, 1))
        now = utcnow()
        async with self.repository.transaction() as transaction:
            live = [
                workload
                for workload in await transaction.list_workloads()
                if workload.state in _LIVE_WORKLOAD_STATES and now - workload.updated_at > grace
            ]
            lease_ids = {lease.id for lease in await transaction.list_leases()}
            cell_ids = {cell.id for cell in await transaction.list_cells()}
            leases_by_id = {lease.id: lease for lease in await transaction.list_leases()}

        orphans: list[tuple[WorkloadRecord, str]] = []
        for workload in live:
            members = self._workload_lease_ids(workload)
            if not members:
                continue
            missing_leases = sorted(set(members) - lease_ids)
            if missing_leases:
                orphans.append((workload, f"lease {missing_leases[0]!r} no longer exists"))
                continue
            missing_cells = sorted(
                {leases_by_id[lease_id].cell_id for lease_id in members} - cell_ids,
            )
            if missing_cells:
                orphans.append((workload, f"cell {missing_cells[0]!r} no longer exists"))

        settled = 0
        for workload, reason in orphans:
            try:
                await self.service.settle_terminal_workload(
                    self._principal,
                    workload.id,
                    WorkloadState.FAILED,
                    action="workload.orphaned",
                    failure_reason=f"workload is unobservable: {reason}",
                    expected_generation=workload.generation,
                    expected_state=workload.state,
                    settle_without_leases=True,
                )
            except (ConflictError, NotFoundError) as exc:
                # It recovered or moved on between the scan and the write; the next
                # cycle re-evaluates from fresh state. Logged rather than silently
                # skipped: a sweep that exists to clear stuck workloads must not be
                # able to fail quietly at the one step that clears them.
                logger.warning("orphan sweep could not settle workload %s: %s", workload.id, exc)
                continue
            settled += 1
        return settled

    async def observe_workloads_once(self) -> int:
        async with self.repository.transaction() as transaction:
            workloads = [
                workload
                for workload in await transaction.list_workloads()
                if workload.state in _LIVE_WORKLOAD_STATES
                and (workload.remote_id or workload.state is WorkloadState.LEASED)
            ]
            leases = {lease.id: lease for lease in await transaction.list_leases()}
            cells = {cell.id: cell for cell in await transaction.list_cells()}
            dispatch_commands = await transaction.list_outbox_for_aggregates(
                "workload",
                {workload.id: workload.generation for workload in workloads},
                topic="workload.dispatch",
            )
        observed = 0
        for workload in workloads:
            lease_ids = self._workload_lease_ids(workload)
            members = tuple(leases.get(lease_id) for lease_id in lease_ids)
            concrete_leases = tuple(lease for lease in members if lease is not None)
            if not members or len(concrete_leases) != len(members):
                continue
            member_cells = tuple(cells.get(lease.cell_id) for lease in concrete_leases)
            concrete_cells = tuple(cell for cell in member_cells if cell is not None)
            if len(concrete_cells) != len(member_cells):
                continue
            pairs = tuple(zip(concrete_leases, concrete_cells, strict=True))
            lease, cell = pairs[0]
            is_group = len(pairs) > 1

            lost_pair = next(
                (
                    member
                    for member in pairs
                    if member[1].state in {CellState.DELETING, CellState.DELETED}
                    or (
                        member[1].state is CellState.UNAVAILABLE
                        and member[1].provider_phase in DEFINITIVE_CAPACITY_LOSS_STATUSES
                    )
                    or (member[1].state is CellState.REGISTERING and not cell_execution_runtime_is_pending(member[1]))
                ),
                None,
            )
            if lost_pair is not None:
                _lost_lease, lost_cell = lost_pair
                capacity_lost = lost_cell.state in {CellState.DELETING, CellState.DELETED} or (
                    lost_cell.state is CellState.UNAVAILABLE
                    and lost_cell.provider_phase in DEFINITIVE_CAPACITY_LOSS_STATUSES
                )
                reason = (
                    f"execution cell capacity is {lost_cell.unavailable_reason}"
                    if lost_cell.state is CellState.UNAVAILABLE
                    else f"execution cell is {lost_cell.state.value}"
                    if capacity_lost
                    else "execution cell capacity was recreated"
                )
                try:
                    if workload.state is WorkloadState.LEASED and workload.remote_id is None:
                        await self.service.requeue_unstarted_workload(
                            self._principal,
                            workload.id,
                            scheduling_reason=reason,
                            expected_generation=workload.generation,
                            expected_lease_id=lease.id,
                            expected_cell_generation=lost_cell.generation,
                            expected_cell_state=lost_cell.state,
                            expected_unavailable_reason=lost_cell.unavailable_reason,
                            expected_cell_id=lost_cell.id,
                        )
                    else:
                        if is_group:
                            await self._stop_terminal_workload_peers(
                                workload,
                                lease,
                                cell,
                                ExecutionWorkloadStatus.LOST,
                            )
                        await self._recover_lost_workload(workload, lease, reason, lost_cell=lost_cell)
                except Exception as exc:
                    logger.info("workload %s loss reconciliation became stale: %s", workload.id, exc)
                observed += 1
                continue
            changed_pair = next(
                (member for member in pairs if member[0].cell_generation != member[1].generation),
                None,
            )
            if changed_pair is not None and workload.remote_id is None:
                _changed_lease, changed_cell = changed_pair
                try:
                    await self.service.requeue_unstarted_workload(
                        self._principal,
                        workload.id,
                        scheduling_reason="cell generation changed before dispatch",
                        expected_generation=workload.generation,
                        expected_lease_id=lease.id,
                        expected_cell_generation=changed_cell.generation,
                        expected_cell_state=changed_cell.state,
                        expected_unavailable_reason=changed_cell.unavailable_reason,
                        expected_cell_id=changed_cell.id,
                    )
                except Exception as exc:
                    logger.info("workload %s undispatched recovery became stale: %s", workload.id, exc)
                observed += 1
                continue
            if workload.remote_id is None:
                releasable_undispatched_leases = all(
                    member.state in {LeaseState.PENDING, LeaseState.QUARANTINED} for member in concrete_leases
                ) and any(member.state is LeaseState.QUARANTINED for member in concrete_leases)
                if releasable_undispatched_leases and not self._has_live_dispatch_command(
                    workload,
                    lease_ids,
                    dispatch_commands,
                ):
                    try:
                        await self.service.requeue_unstarted_workload(
                            self._principal,
                            workload.id,
                            scheduling_reason="undispatched lease expired after its dispatch command retired",
                            expected_generation=workload.generation,
                            expected_lease_id=lease.id,
                            expected_cell_generation=cell.generation,
                            expected_cell_state=cell.state,
                            expected_unavailable_reason=cell.unavailable_reason,
                            expected_cell_id=cell.id,
                        )
                    except Exception as exc:
                        logger.info("workload %s expired undispatched recovery became stale: %s", workload.id, exc)
                    observed += 1
                continue
            try:
                if is_group:
                    coordinator_lease, coordinator_cell = self._ordered_gang_member_pairs(pairs)[0]
                    remote = await asyncio.to_thread(
                        self._agent_factory(coordinator_cell).gang_workload,
                        coordinator_lease.cell_id,
                        workload.id,
                        workload.remote_id,
                    )
                else:
                    remote = await asyncio.to_thread(self._agent_factory(cell).workload, cell.id, workload.remote_id)
            except Exception as exc:
                logger.warning("workload %s observation failed: %s", workload.id, exc)
                continue
            try:
                if is_group:
                    await self._recover_group_member_leases(pairs, remote.status)
                await self._apply_workload_observation(
                    workload,
                    lease,
                    cell,
                    remote.status,
                    remote.message,
                )
            except Exception as exc:
                logger.info("workload %s observation became stale: %s", workload.id, exc)
            observed += 1
        return observed

    async def _recover_group_member_leases(
        self,
        members: tuple[tuple[LeaseRecord, CellRecord], ...],
        status: ExecutionWorkloadStatus,
    ) -> None:
        """Renew quarantined non-leader group leases after a healthy group observation."""

        if status not in _RECOVERABLE_WORKLOAD_STATUSES:
            return
        for lease, _cell in members[1:]:
            if lease.state is not LeaseState.QUARANTINED or lease.revocation_reason == "preemption":
                continue
            await self.service.transition_lease(
                self._principal,
                lease.id,
                LeaseState.ACTIVE,
                action="lease.recovered",
            )
            async with self.repository.transaction() as transaction:
                current = await transaction.get_lease(lease.id)
                if current is not None and current.state is LeaseState.ACTIVE:
                    now = utcnow()
                    await transaction.put_lease(
                        current.model_copy(
                            update={
                                "expires_at": now + timedelta(seconds=self.settings.lease_ttl_s),
                                "updated_at": now,
                            }
                        )
                    )

    async def _apply_workload_observation(
        self,
        workload: WorkloadRecord,
        lease: LeaseRecord,
        cell: CellRecord,
        status: ExecutionWorkloadStatus,
        message: str | None,
    ) -> None:
        if status is ExecutionWorkloadStatus.INDETERMINATE:
            return
        preemption_owned = lease.revocation_reason == "preemption" and lease.state in {
            LeaseState.REVOKING,
            LeaseState.QUARANTINED,
        }
        if preemption_owned and status in _PREEMPTION_OWNED_TERMINAL_WORKLOAD_STATUSES:
            return
        if lease.state is LeaseState.QUARANTINED and not preemption_owned and status in _RECOVERABLE_WORKLOAD_STATUSES:
            await self.service.transition_lease(
                self._principal,
                lease.id,
                LeaseState.ACTIVE,
                action="lease.recovered",
            )
            async with self.repository.transaction() as transaction:
                recovered = await transaction.get_lease(lease.id)
                if recovered is not None:
                    await transaction.put_lease(
                        recovered.model_copy(
                            update={
                                "expires_at": utcnow() + timedelta(seconds=self.settings.lease_ttl_s),
                                "updated_at": utcnow(),
                            }
                        )
                    )
        if status is ExecutionWorkloadStatus.RUNNING and workload.state is WorkloadState.DISPATCHING:
            await self.service.transition_workload(
                self._principal,
                workload.id,
                WorkloadState.RUNNING,
                expected_generation=workload.generation,
                expected_lease_id=lease.id,
                expected_state=workload.state,
            )
        elif status is ExecutionWorkloadStatus.SUCCEEDED:
            current = workload
            if workload.state is WorkloadState.DISPATCHING:
                current = await self.service.transition_workload(
                    self._principal,
                    workload.id,
                    WorkloadState.RUNNING,
                    expected_generation=workload.generation,
                    expected_lease_id=lease.id,
                    expected_state=workload.state,
                )
            await self._release_terminal(current, lease, WorkloadState.SUCCEEDED, "workload.succeeded")
        elif status in {ExecutionWorkloadStatus.FAILED, ExecutionWorkloadStatus.STOPPED}:
            if status is ExecutionWorkloadStatus.FAILED:
                await self._stop_terminal_workload_peers(workload, lease, cell, status)
            if workload.state is WorkloadState.CANCELLING and not preemption_owned:
                await self._release_terminal(workload, lease, WorkloadState.CANCELLED, "workload.cancelled")
            elif (
                status is ExecutionWorkloadStatus.FAILED
                and not preemption_owned
                and workload.failure_count < workload.spec.max_failure_retries
            ):
                if workload.lease_id != lease.id:
                    raise StaleOutboxCommand(f"lease {lease.id!r} no longer owns workload {workload.id!r}")
                await self.service.settle_failed_workload(
                    self._principal,
                    workload.id,
                    checkpoint_uri=workload.checkpoint_uri,
                    scheduling_reason=(
                        f"execution failed; retrying user-code failure "
                        f"({workload.failure_count + 1} of {workload.spec.max_failure_retries})"
                    ),
                    expected_generation=workload.generation,
                    expected_lease_id=lease.id,
                    expected_state=workload.state,
                )
            elif not preemption_owned:
                # The executor's diagnostic is right here and used one branch
                # below for LOST, but the terminal-failure path used to drop it:
                # a failed workload recorded `failure_reason: None`, so the only
                # account of why it died lived in logs on a cell that is about to
                # be released. Observed live 2026-08-01.
                await self._release_terminal(
                    workload,
                    lease,
                    WorkloadState.FAILED,
                    "workload.failed",
                    failure_reason=message or "execution backend reported failure without a diagnostic",
                )
        elif status in {ExecutionWorkloadStatus.LOST, ExecutionWorkloadStatus.NOT_FOUND}:
            await self._stop_terminal_workload_peers(workload, lease, cell, status)
            await self._recover_lost_workload(
                workload,
                lease,
                message or f"execution backend reported {status.value}",
            )

    async def _stop_terminal_workload_peers(
        self,
        workload: WorkloadRecord,
        lease: LeaseRecord,
        cell: CellRecord,
        status: ExecutionWorkloadStatus,
    ) -> None:
        """Fence every reachable rank before settling a failed slice workload.

        A native multi-host execution can report a failed or lost rank while
        peers are still waiting in a JAX collective. The original lease fence
        remains authoritative for this attempt, so best-effort fan-out stop
        happens before the workload is released or requeued. A missing host is
        expected for ``LOST`` and must not prevent recovery of the lease.
        """

        if workload.remote_id is None:
            return
        if len(self._workload_lease_ids(workload)) > 1:
            try:
                members = await self._load_workload_members(workload)
                ordered = self._ordered_gang_member_pairs(members)
                coordinator_lease, coordinator_cell = ordered[0]
                request = GangWorkloadMutationRequest(
                    members=self._gang_members(
                        ordered,
                        idempotency_prefix=f"terminal-stop:{workload.id}:g{workload.generation}",
                    )
                )
                await asyncio.to_thread(
                    self._agent_factory(coordinator_cell).stop_gang,
                    coordinator_lease.cell_id,
                    workload.id,
                    workload.remote_id,
                    request,
                )
            except Exception as exc:
                logger.warning(
                    "workload %s reported %s and group peer stop was incomplete: %s",
                    workload.id,
                    status.value,
                    exc,
                )
            return
        request = WorkloadMutationRequest(
            mutation=MutationModel(
                idempotency_key=f"terminal-stop:{workload.id}:g{workload.generation}",
                generation=workload.generation,
                fencing_token=lease.fencing_token,
            )
        )
        try:
            await asyncio.to_thread(self._agent_factory(cell).stop, cell.id, workload.remote_id, request)
        except Exception as exc:
            logger.warning(
                "workload %s reported %s and peer stop was incomplete: %s",
                workload.id,
                status.value,
                exc,
            )

    async def _recover_lost_workload(
        self,
        workload: WorkloadRecord,
        lease: LeaseRecord,
        reason: str,
        *,
        lost_cell: CellRecord | None = None,
    ) -> None:
        if workload.lease_id != lease.id:
            return
        preemption_owned = lease.revocation_reason == "preemption" and lease.state in {
            LeaseState.REVOKING,
            LeaseState.QUARANTINED,
        }
        if workload.state is WorkloadState.CANCELLING and not preemption_owned:
            await self.service.settle_terminal_workload(
                self._principal,
                workload.id,
                WorkloadState.CANCELLED,
                action="workload.cancelled_after_loss",
                expected_generation=workload.generation,
                expected_lease_id=lease.id,
                expected_state=workload.state,
                expected_cell_generation=None if lost_cell is None else lost_cell.generation,
                expected_cell_state=None if lost_cell is None else lost_cell.state,
                expected_unavailable_reason=None if lost_cell is None else lost_cell.unavailable_reason,
                expected_cell_id=None if lost_cell is None else lost_cell.id,
            )
            return
        if (workload.checkpoint_uri or preemption_owned) and (workload.capacity_loss_count < workload.spec.max_retries):
            await self.service.settle_preempted_workload(
                self._principal,
                workload.id,
                checkpoint_uri=workload.checkpoint_uri,
                scheduling_reason=(
                    f"cell lost; resuming from {workload.checkpoint_uri} "
                    f"({workload.capacity_loss_count + 1} of {workload.spec.max_retries})"
                    if workload.checkpoint_uri
                    else "cell lost during preemption; restarting without a checkpoint "
                    f"({workload.capacity_loss_count + 1} of {workload.spec.max_retries})"
                ),
                action="workload.recovery_queued",
                expected_generation=workload.generation,
                expected_lease_id=lease.id,
                expected_state=workload.state,
                require_preemption_owner=preemption_owned,
                expected_cell_generation=None if lost_cell is None else lost_cell.generation,
                expected_cell_state=None if lost_cell is None else lost_cell.state,
                expected_unavailable_reason=None if lost_cell is None else lost_cell.unavailable_reason,
                expected_cell_id=None if lost_cell is None else lost_cell.id,
            )
            return
        if lost_cell is not None and workload.capacity_loss_count < workload.spec.max_retries:
            await self.service.settle_preempted_workload(
                self._principal,
                workload.id,
                scheduling_reason=(
                    f"{reason}; retrying capacity loss "
                    f"({workload.capacity_loss_count + 1} of {workload.spec.max_retries})"
                ),
                action="workload.capacity_recovery_queued",
                expected_generation=workload.generation,
                expected_lease_id=lease.id,
                expected_state=workload.state,
                expected_cell_generation=lost_cell.generation,
                expected_cell_state=lost_cell.state,
                expected_unavailable_reason=lost_cell.unavailable_reason,
                expected_cell_id=lost_cell.id,
            )
            return
        await self.service.settle_terminal_workload(
            self._principal,
            workload.id,
            WorkloadState.FAILED,
            action="workload.lost",
            failure_reason=reason,
            expected_generation=workload.generation,
            expected_lease_id=lease.id,
            expected_state=workload.state,
            expected_cell_generation=None if lost_cell is None else lost_cell.generation,
            expected_cell_state=None if lost_cell is None else lost_cell.state,
            expected_unavailable_reason=None if lost_cell is None else lost_cell.unavailable_reason,
            expected_cell_id=None if lost_cell is None else lost_cell.id,
        )

    async def prune_retained_state_once(self) -> dict[str, int]:
        """Delete state past its retention window and report what was removed.

        Runs leader-only and in bounded batches. A window of ``0`` disables
        pruning for that table, preserving the historical behaviour where
        nothing was ever deleted -- which also meant an idempotency key was
        honored forever, so a caller reusing one months later silently replayed
        the original response instead of acting.
        """

        now = utcnow()
        windows = (
            ("outbox", self.settings.outbox_retention_s, "prune_outbox"),
            ("audit_events", self.settings.audit_retention_s, "prune_events"),
            ("idempotency_keys", self.settings.idempotency_retention_s, "prune_idempotency"),
        )
        removed: dict[str, int] = {}
        failures: list[str] = []
        for name, retention_s, method in windows:
            if retention_s <= 0:
                continue
            cutoff = now - timedelta(seconds=retention_s)
            try:
                async with self.repository.transaction() as transaction:
                    prune = getattr(transaction, method)
                    count = await prune(before=cutoff, limit=self.settings.retention_batch_limit)
            except Exception as exc:  # one broken window must not mask the rest
                # These windows are independent tables with independent hazards.
                # Aborting the loop on the first failure is how a backend that
                # could not prune audit events also stopped pruning idempotency
                # keys -- so replays kept being honored forever, silently.
                failures.append(f"{name}: {exc}")
                logger.exception("retention pruning failed for %s", name)
                continue
            if count:
                removed[name] = count
        if removed:
            logger.info("retention pruned %s", ", ".join(f"{name}={count}" for name, count in sorted(removed.items())))
        if failures:
            raise RuntimeError("retention pruning incomplete: " + "; ".join(failures))
        return removed

    async def observe_cells_once(self) -> int:
        """Advance capacity state while serializing with lifecycle transitions."""

        return await self._observe_cells_once()

    async def _observe_cells_once(self) -> int:
        """Advance provider capacity state and refresh the schedulable inventory."""

        now = utcnow()
        async with self.repository.transaction() as transaction:
            disabled_pool_ids = {pool.id for pool in await transaction.list_capacity_pools() if not pool.enabled}
            candidates = [
                cell
                for cell in await transaction.list_cells()
                if cell.state in {CellState.REGISTERING, CellState.READY, CellState.UNAVAILABLE}
                and (cell.availability.cooldown_until is None or cell.availability.cooldown_until <= now)
                and cell.pool_id not in disabled_pool_ids
            ]
            # Only REGISTERING cells consult the outbox, and only for their own
            # current generation. Query exactly those aggregates instead of
            # scanning the whole table: the previous limit=10_000 scan was
            # O(cells x outbox) per cycle and, past that many rows, could miss
            # an active command entirely and observe a cell mid-provision.
            registering = {cell.id: cell.generation for cell in candidates if cell.state is CellState.REGISTERING}
            active_capacity_applies = {
                (command.aggregate_id, command.generation)
                for command in await transaction.list_outbox_for_aggregates("cell", registering, topic="capacity.apply")
                if self._is_active_capacity_apply(command)
            }
            cells = [
                cell
                for cell in candidates
                if not (
                    cell.state is CellState.REGISTERING
                    and (cell.provider_phase is None or (cell.id, cell.generation) in active_capacity_applies)
                )
            ]
        semaphore = asyncio.Semaphore(max(1, self.settings.cell_reconcile_concurrency))

        async def reconcile(cell: CellRecord) -> bool:
            async with semaphore:
                return await self._observe_cell_once(cell, now)

        return sum(await asyncio.gather(*(reconcile(cell) for cell in cells)))

    async def _observe_cell_once(self, cell: CellRecord, now: datetime) -> bool:
        """Fence one agent observation against its fleet and cell lifecycle changes."""

        async with self._capacity_operation(cell):
            current = await self._current_observable_cell(cell, now)
            if current is None:
                return False
            return await self._observe_cell_once_locked(current, now)

    async def _current_observable_cell(self, cell: CellRecord, now: datetime) -> CellRecord | None:
        """Return the current cell only when its original observation remains safe."""

        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if current is None or current.generation != cell.generation or current.state is not cell.state:
                return None
            if current.state not in {CellState.REGISTERING, CellState.READY, CellState.UNAVAILABLE}:
                return None
            if current.state is CellState.REGISTERING:
                # One aggregate, one generation: an indexed lookup rather than a
                # full-table scan repeated per cell.
                if any(
                    self._is_active_capacity_apply(command)
                    for command in await transaction.list_outbox_for_aggregates(
                        "cell", {current.id: current.generation}, topic="capacity.apply"
                    )
                ):
                    return None
                if current.provider_phase is None:
                    return None
            if current.availability.cooldown_until is not None and current.availability.cooldown_until > now:
                return None
            if current.pool_id is not None:
                pool = await transaction.get_capacity_pool(current.pool_id)
                if pool is not None and not pool.enabled:
                    return None
            return current

    @staticmethod
    def _is_active_capacity_apply(command: OutboxRecord) -> bool:
        """Return whether a capacity apply may still enter or own a provider RPC."""

        return command.topic == "capacity.apply" and command.state in _ACTIVE_CAPACITY_APPLY_STATES

    async def _observe_cell_once_locked(self, cell: CellRecord, now: datetime) -> bool:
        """Reconcile and persist one cell without blocking unrelated cells."""

        agent_responded = False
        provider_detail: str | None = None
        provider_failure_code = cell.provider_failure_code
        unavailable_reason: str | None = (
            cell.unavailable_reason if provider_failure_code is not None else "agent_unreachable"
        )
        observed_tpu_vms: int | None = None
        try:
            mutation = MutationModel(
                idempotency_key=f"reconcile:{cell.id}:{cell.generation}",
                generation=cell.generation,
                fencing_token=f"cell:{cell.id}:{cell.generation}",
            )
            observation = await asyncio.to_thread(
                self._agent_factory(cell).reconcile,
                cell.id,
                WorkloadMutationRequest(mutation=mutation),
            )
        except CellAgentBusyError:
            logger.debug("cell %s capacity reconciliation is already active", cell.id)
            return False
        except CellAgentStaleMutationError as exc:
            logger.debug("cell %s reconciliation was superseded by a newer agent fence", cell.id)
            # Keep the agent's own words. It distinguishes three different causes
            # -- an older generation, a fencing token that does not own the
            # current one, and a tombstoned cell -- and this path used to collapse
            # all three into one sentence that says only "a fence was rejected".
            # That is the difference between an operator reading the cause and
            # inferring it from a GCS document by hand.
            await self._record_cell_agent_fence_conflict(
                cell_id=cell.id,
                generation=cell.generation,
                diagnostic=(
                    f"cell-agent reconciliation rejected the current fence: {exc}"
                    if str(exc)
                    else "cell-agent reconciliation rejected the current fence"
                ),
            )
            return False
        except CellAgentUnavailableError as exc:
            # Nothing was learned, so nothing is written. Recording this as
            # PROVIDER_ERROR would restart the provider phase clock and feed the
            # zone's belief a failure the provider never caused; see
            # CellAgentUnavailableError for the incident that motivated it.
            #
            # Leaving the record untouched is also what bounds the blast radius:
            # the cell keeps its real phase and its real clock, so a permanently
            # dead agent still retires the cell on `queue_timeout_s` /
            # `provisioning_timeout_s` exactly as it would have. Capacity cannot
            # leak because of this branch -- it can only survive a blip.
            logger.warning("cell %s could not be observed: %s", cell.id, exc)
            return False
        except CellAgentCapacityProviderError as exc:
            provider_detail = str(exc)
            provider_failure_code = exc.code
            unavailable_reason = exc.code.value
            state = CellState.UNAVAILABLE
            remote_state = CapacityStatus.PROVIDER_ERROR
            availability = self._observe_capacity_belief(cell, ProvisionOutcome.PROVIDER_ERROR)
        except Exception as exc:
            provider_detail = str(exc) or type(exc).__name__
            state = CellState.UNAVAILABLE
            remote_state = CapacityStatus.PROVIDER_ERROR
            availability = self._observe_capacity_belief(cell, ProvisionOutcome.PROVIDER_ERROR)
        else:
            agent_responded = True
            provider_detail = observation.detail or None
            remote_state = observation.state
            observed_tpu_vms = observation.observed_tpu_vms
            if observation.generation != cell.generation and remote_state not in DEFINITIVE_CAPACITY_LOSS_STATUSES:
                logger.warning(
                    "cell %s reported generation %s while generation %s is desired",
                    cell.id,
                    observation.generation,
                    cell.generation,
                )
                return False
            if remote_state in READY_CAPACITY_STATUSES:
                state = CellState.READY
                unavailable_reason = None
                provider_failure_code = None
                availability = (
                    self._observe_capacity_belief(
                        cell,
                        ProvisionOutcome.SUCCESS,
                        latency_s=max(0.0, (now - cell.created_at).total_seconds()),
                    )
                    if cell.state is not CellState.READY
                    else cell.availability
                )
            elif remote_state in PENDING_CAPACITY_STATUSES:
                state = CellState.REGISTERING
                unavailable_reason = None
                provider_failure_code = None
                availability = (
                    self._observe_capacity_belief(cell, ProvisionOutcome.INTERRUPTED)
                    if cell.state is CellState.READY
                    else cell.availability
                )
            else:
                state = CellState.UNAVAILABLE
                unavailable_reason = cell.unavailable_reason if provider_failure_code is not None else remote_state.value
                outcome = (
                    ProvisionOutcome.QUOTA_ERROR
                    if remote_state is CapacityStatus.HALTED_QUOTA
                    else ProvisionOutcome.STOCKOUT
                )
                availability = self._observe_capacity_belief(cell, outcome)
        transition: tuple[CellRecord, CellRecord] | None = None
        async with self.repository.transaction() as transaction:
            current = await transaction.get_cell(cell.id)
            if current is None or current.generation != cell.generation or current.state is not cell.state:
                return False
            phase_started_at = _provider_phase_start(current, remote_state, now)
            updated = current.model_copy(
                update={
                    "state": state,
                    "provider_phase": remote_state,
                    "provider_failure_code": provider_failure_code,
                    "observed_tpu_vms": observed_tpu_vms,
                    "provider_phase_started_at": phase_started_at,
                    "unavailable_reason": unavailable_reason,
                    "unavailable_detail": (provider_detail or None) if unavailable_reason else None,
                    "availability": availability,
                    "last_heartbeat_at": now if agent_responded else current.last_heartbeat_at,
                    "updated_at": now,
                }
            )
            await transaction.put_cell(updated)
            if provider_failure_code is CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED:
                await self._fail_autoscale_workload_for_queue_admission_denial(
                    transaction,
                    updated,
                    provider_detail or "",
                )
            transition = current, updated
        if transition is not None:
            self._capacity_lifecycle.transition(*transition, reason=provider_detail)
        return True

    async def quarantine_expired_leases_once(self) -> int:
        """Fence expired leases without killing work or reallocating ambiguous capacity."""

        now = utcnow()
        changed = 0
        async with self.repository.transaction() as transaction:
            candidate_ids = [
                lease.id
                for lease in await transaction.list_leases()
                if lease.state in {LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.REVOKING}
                and lease.expires_at <= now
            ]
        for lease_id in candidate_ids:
            async with self.repository.transaction() as transaction:
                lease = await transaction.get_lease(lease_id)
                if lease is None:
                    continue
                if lease.state not in {LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.REVOKING}:
                    continue
                if lease.expires_at > now:
                    continue
                await transaction.put_lease(
                    lease.model_copy(update={"state": LeaseState.QUARANTINED, "updated_at": now})
                )
                await transaction.append_event(
                    AuditEvent(
                        id=f"evt_{uuid.uuid4().hex}",
                        actor=self._principal.subject,
                        org_id=lease.org_id,
                        project_id=lease.project_id,
                        entity_type="lease",
                        entity_id=lease.id,
                        action="lease.quarantined",
                        previous_state=lease.state.value,
                        new_state=LeaseState.QUARANTINED.value,
                    )
                )
                changed += 1
        return changed

    async def settle_coordinator_workloads_once(self) -> int:
        """Derive a batch parent's terminal state from its children.

        A coordinator never holds a lease, so nothing else can ever settle it --
        it would sit PENDING forever while all 500 of its tasks succeeded, and
        every listing would report the array as still queued.

        The mapping is deliberately pessimistic: one failed task fails the
        array, because a partial array is not a result. A cancelled task without
        any failure reports CANCELLED, which is what the cascade produced.
        """

        settled = 0
        async with self.repository.transaction() as transaction:
            # Filtered on the indexed state column rather than scanning every
            # workload the fleet has ever run. This is a leader hot-path phase, and
            # the terminal records it used to read back are the fastest-growing table
            # in the system. Narrowing ``coordination_kind`` itself needs a typed
            # column and is deferred to the coordination-columns migration; PENDING
            # alone already removes the unbounded part.
            candidate_ids = [
                workload.id
                for workload in await transaction.list_workloads(state=WorkloadState.PENDING)
                if workload.coordination_kind is not None
            ]
        for workload_id in candidate_ids:
            async with self.repository.transaction() as transaction:
                parent = await transaction.get_workload(workload_id)
                if parent is None or parent.coordination_kind is None or parent.state is not WorkloadState.PENDING:
                    continue
                children = await transaction.list_child_workloads(workload_id)
                if not children or any(child.state not in TERMINAL_WORKLOAD_STATES for child in children):
                    continue
                states = {child.state for child in children}
                if WorkloadState.FAILED in states:
                    target = WorkloadState.FAILED
                elif WorkloadState.CANCELLED in states:
                    target = WorkloadState.CANCELLED
                else:
                    target = WorkloadState.SUCCEEDED
                await transaction.put_workload(
                    parent.model_copy(
                        update={
                            "state": target,
                            "scheduling_reason": (
                                f"{parent.coordination_kind.value} of {len(children)} members settled"
                            ),
                            "updated_at": utcnow(),
                        }
                    )
                )
                await transaction.append_event(
                    AuditEvent(
                        id=f"evt_{uuid.uuid4().hex}",
                        actor=self._principal.subject,
                        org_id=parent.org_id,
                        project_id=parent.project_id,
                        entity_type="workload",
                        entity_id=parent.id,
                        action="workload.batch_settled",
                        previous_state=parent.state.value,
                        new_state=target.value,
                    )
                )
                settled += 1
        return settled

    async def capacity_orphans(self) -> list[dict[str, str | int | None]]:
        """Return cells an agent still holds that no control-plane record claims.

        A fleet-state document outlives the control plane that wrote it. When a
        control plane's database is replaced, its cells are left behind in the
        agent -- and any left ``desired_state: up`` are re-ensured forever, so
        the agent keeps provisioning capacity nobody asked for, invisible to the
        control plane that would otherwise own it.

        Found on 2026-08-01: a ``v5p-128`` queued resource being recreated days
        after its fleet was deleted, discoverable only by reading the agent's
        state document out of object storage by hand.

        Read-only, and deliberately not automatic. One agent can legitimately be
        shared by more than one control plane, so a record this control plane
        does not recognise is not necessarily unowned -- reclaiming it is an
        operator decision, and this is the information that decision needs.
        """

        async with self.repository.transaction() as transaction:
            cells = await transaction.list_cells()
        known = {cell.id for cell in cells}
        endpoints: dict[str, CellRecord] = {}
        for cell in cells:
            if cell.endpoint and cell.state is not CellState.DELETED:
                endpoints.setdefault(cell.endpoint, cell)
        orphans: list[dict[str, str | int | None]] = []
        for endpoint, sample in sorted(endpoints.items()):
            try:
                inventory = await asyncio.to_thread(self._agent_factory(sample).inventory)
            except Exception as exc:
                logger.warning("cell agent %s inventory is unavailable: %s", endpoint, exc)
                continue
            for entry in inventory:
                name = entry.get("name")
                if isinstance(name, str) and name not in known:
                    orphans.append({**entry, "endpoint": endpoint})
        return orphans

    async def workload_logs(self, workload_id: str) -> str:
        """Read driver logs for an active or terminal workload attempt."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
        if workload is None:
            raise LookupError(f"workload {workload_id!r} does not exist")
        if workload.remote_id is None:
            return ""
        members = await self._load_workload_members(workload)
        if len(members) > 1:
            coordinator_lease, coordinator_cell = self._ordered_gang_member_pairs(members)[0]
            return await asyncio.to_thread(
                self._agent_factory(coordinator_cell).gang_logs,
                coordinator_lease.cell_id,
                workload.id,
                workload.remote_id,
            )
        _lease, cell = members[0]
        return await asyncio.to_thread(self._agent_factory(cell).logs, cell.id, workload.remote_id)

    async def workload_process_status(self, workload_id: str) -> JsonObject:
        """Read live process detail for one workload attempt.

        For a gang this reports the coordinator cell, whose slice executor
        already fans out across every rank -- which is the view that makes a
        hung collective legible.
        """

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
        if workload is None:
            raise LookupError(f"workload {workload_id!r} does not exist")
        if workload.remote_id is None:
            return {"workload_id": workload_id, "observed": False, "detail": "workload has no dispatched attempt"}
        members = await self._load_workload_members(workload)
        lease, cell = self._ordered_gang_member_pairs(members)[0] if len(members) > 1 else members[0]
        return await asyncio.to_thread(
            self._agent_factory(cell).process_status,
            lease.cell_id,
            workload.remote_id,
        )

    async def run(self, stop: asyncio.Event) -> None:
        """Run independent leader-scheduling and replica-local outbox loops."""

        await asyncio.gather(
            self._run_scheduler_loop(stop),
            self._run_outbox_loop(stop),
        )

    def _leader_phases(self) -> tuple[LeaderPhase, ...]:
        """Declare the leader cycle in order.

        A list rather than straight-line code because adding a phase used to mean
        editing the runner *and* threading another deadline through a positional
        tuple in its signature -- so any two changes that each added a phase
        conflicted. Order is the only thing this tuple encodes; every policy a phase
        needs travels with it.
        """

        return (
            LeaderPhase("unmanaged-capacity-recovery", self.reconcile_unmanaged_capacity_recoveries_once),
            LeaderPhase("capacity-pools", self.reconcile_capacity_pools_once),
            LeaderPhase(
                "observe-cells",
                self._observe_cells_once,
                interval_s=self.settings.cell_reconcile_interval_s,
            ),
            LeaderPhase("quarantine-leases", self.quarantine_expired_leases_once),
            LeaderPhase("observe-workloads", self.observe_workloads_once),
            # After observation: a workload the observer could resolve this cycle is
            # not an orphan, whatever the previous cycle saw.
            LeaderPhase("settle-orphans", self.settle_orphaned_workloads_once),
            # After observation, so a coordinator settles in the same cycle its last
            # task does rather than a full interval later.
            LeaderPhase("settle-coordinators", self.settle_coordinator_workloads_once),
            # Before scheduling, so a node released this cycle can be placed in the
            # same cycle rather than waiting a full interval.
            LeaderPhase("release-dag-nodes", self.release_ready_dag_nodes_once),
            # Before scheduling, for the same reason: a group that became feasible
            # this cycle should be placed in it, not a full interval later.
            LeaderPhase("release-groups", self.release_admitted_groups_once),
            LeaderPhase("schedule", self.reconcile_once),
            # After scheduling, so a cell leased this cycle is not charged as idle
            # for the interval in which it was actually being placed. Failures are
            # isolated: an accounting gap must never stop placement.
            LeaderPhase(
                "cell-billing",
                self.accrue_cell_billing_once,
                interval_s=self.settings.cell_billing_interval_s,
                zero_interval_disables=True,
                isolate_failures=True,
            ),
            # Retention is housekeeping and must never delay placement, so it runs
            # last, on its own slow interval, and its failures are isolated.
            LeaderPhase(
                "retention",
                self.prune_retained_state_once,
                interval_s=self.settings.retention_interval_s,
                zero_interval_disables=True,
                isolate_failures=True,
            ),
        )

    async def _run_leader_phases(self, *, now: float, due: dict[str, float]) -> dict[str, float]:
        """Run one leader cycle, re-checking leadership before every phase.

        A full cycle spans many provider round trips, so this replica can lose the
        advisory lock partway through. Continuing after that would let two leaders
        mint autoscaler cell ids from the same ``len(managed) + 1`` sequence and
        collide, so each phase re-acquires and the cycle stops the moment leadership
        is gone. Losing it mid-cycle is ordinary, not an error.

        ``due`` maps phase name to its next-eligible monotonic time and is returned
        updated. Keyed by name rather than positionally so a new phase adds an entry
        instead of changing this signature.
        """

        for phase in self._leader_phases():
            if phase.interval_s is not None:
                if phase.zero_interval_disables and phase.interval_s <= 0:
                    continue
                if now < due.get(phase.name, 0.0):
                    continue
                # Advance before the call, so a persistently failing phase cannot pin
                # the loop retrying it on every scheduler tick.
                due[phase.name] = now + phase.interval_s
            if not await self.leader.acquire():
                return due
            if not phase.isolate_failures:
                await phase.run()
                continue
            try:
                await phase.run()
            except Exception:
                logger.exception("leader phase %s failed; continuing", phase.name)
        return due

    async def _run_scheduler_loop(self, stop: asyncio.Event) -> None:
        """Run leader-only global reconciliation without blocking outbox delivery."""

        due: dict[str, float] = {}
        try:
            while not stop.is_set():
                try:
                    if await self.leader.acquire():
                        async with self._leader_cycle_lock:
                            now = asyncio.get_running_loop().time()
                            due = await self._run_leader_phases(now=now, due=due)
                except Exception:
                    logger.exception("global scheduling reconciliation failed")
                await self._wait_for_next_cycle(stop)
        finally:
            await self.leader.release()

    async def _run_outbox_loop(self, stop: asyncio.Event) -> None:
        """Deliver durable mutations independently of leader reconciliation."""

        while not stop.is_set():
            try:
                await self.dispatch_outbox_once()
            except Exception:
                logger.exception("outbox reconciliation failed")
            await self._wait_for_next_cycle(stop)

    async def _wait_for_next_cycle(self, stop: asyncio.Event) -> None:
        """Sleep until the next controller interval or shutdown."""

        try:
            await asyncio.wait_for(stop.wait(), timeout=self.settings.scheduler_interval_s)
        except asyncio.TimeoutError:
            pass
