# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Construction, shutdown, and the locks every other concern takes."""

from __future__ import annotations

import asyncio
import contextlib
import socket
import typing as tp
import uuid
import weakref
from contextlib import asynccontextmanager
from datetime import datetime

from eyvan._internal.observability import EyvanMetrics
from eyvan.artifacts.secret_capability import SecretCapabilitySigner
from eyvan.artifacts.transfers import ManagedArtifactAccess
from eyvan.artifacts.workload_capability import WorkloadCapabilitySigner
from eyvan.capacity.audit import CapacityLifecycleLogger
from eyvan.config import EyvanSettings
from eyvan.records import (
    CellRecord,
    OutboxRecord,
)
from eyvan.server.controller._support import (
    LeaderElector,
    SingleProcessLeader,
    StaleOutboxCommand,
    _FleetCapacityGate,
    _system_principal,
    logger,
)
from eyvan.server.controller.log_archival import _LogChunkProgress
from eyvan.server.persistence.base import Repository
from eyvan.server.service.control_plane import ControlPlaneService
from eyvan.server.service.stage import CellStageExecutor
from eyvan.worker.agent.client import (
    CellAgentClient,
)
from eyvan.worker.agent.models import (
    MutationModel,
)
from eyvan.worker.runtime.node_health import HostReplacementBudget, host_replacement_budget_from_settings


class _ControllerLifecycle:
    """Construction, shutdown, and the locks every other concern takes.

    A concern of :class:`~eyvan.server.controller.EyvanController`; not
    instantiated on its own.
    """

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
        workload_capability: WorkloadCapabilitySigner | None = None,
        secret_capability: SecretCapabilitySigner | None = None,
        #: Puts named assets on a host and takes them off again. Optional, and
        #: unset in every shipped configuration: staging is a property of the
        #: asset store rather than of the capacity provider, so binding one here
        #: would make the contract unimplementable for anything but today's
        #: agent. Without it nothing is staged and every stage reads as unknown,
        #: which is the honest description of a plane that cannot reach a disk.
        stage_executor: CellStageExecutor | None = None,
        #: How many hosts one pool may replace for node health in one window.
        #: Defaults to the environment, which defaults to zero -- promotion off,
        #: a failed attempt behaves exactly as it did before node health existed.
        host_replacement_budget: HostReplacementBudget | None = None,
        #: Optional JSONL file every capacity lifecycle event is appended to,
        #: so lifecycle auditing does not depend on log collection. Unset by
        #: default: log lines remain the always-on record.
        capacity_audit_sink: str | None = None,
    ):
        self.repository = repository
        self.service = service
        self.settings = settings or EyvanSettings.from_env()
        self.leader = leader or SingleProcessLeader()
        self.worker_id = f"{socket.gethostname()}:{uuid.uuid4().hex[:12]}"
        self._principal = _system_principal()
        self._agent_clients: dict[str, CellAgentClient] = {}
        # When each (workload, generation) was first observed unreadable. Held in
        # memory on purpose: losing it on a leader change only restarts the
        # grace period, which delays an escalation rather than triggering one
        # early -- the safe direction for something that ends in draining a host.
        self._indeterminate_since: dict[tuple[str, int], datetime] = {}
        # Where each (workload, attempt) incremental log archive has reached.
        # In memory for the same reason: every position is also stored in the
        # chunk it was captured with, so losing this map costs one listing per
        # attempt rather than a replayed archive.
        self._log_chunk_progress: dict[tuple[str, int], _LogChunkProgress] = {}
        self._owns_agent_clients = agent_factory is None
        self._agent_factory = agent_factory or self._default_agent
        self._leader_cycle_lock = asyncio.Lock()
        self._fleet_capacity_gates: weakref.WeakValueDictionary[str, _FleetCapacityGate] = weakref.WeakValueDictionary()
        self._cell_capacity_locks: weakref.WeakValueDictionary[str, asyncio.Lock] = weakref.WeakValueDictionary()
        self.metrics = metrics
        self.artifact_access = artifact_access
        self.workload_capability = workload_capability
        self.secret_capability = secret_capability
        self._stage_executor = stage_executor
        self._host_replacement_budget = host_replacement_budget or host_replacement_budget_from_settings(self.settings)
        self._capacity_lifecycle = CapacityLifecycleLogger(logger, sink_path=capacity_audit_sink)

    def _measure_reconcile(self, operation: str) -> tp.ContextManager[None]:
        """Time one reconciliation operation when metrics are collected.

        Metrics are optional on the controller -- most tests and every embedded
        use construct it without a registry -- so every call site would
        otherwise repeat the same ``if self.metrics is None`` dance around a
        ``with``. Returning a null context keeps that decision in one place.
        """

        if self.metrics is None:
            return contextlib.nullcontext()
        return self.metrics.measure_reconcile(operation)

    async def close(self) -> None:
        """Release cached HTTP pools and scheduler leadership."""

        if self._owns_agent_clients:
            clients, self._agent_clients = list(self._agent_clients.values()), {}
            for client in clients:
                await asyncio.to_thread(client.close)
        await self.leader.release()

    def _default_agent(self, cell: CellRecord) -> CellAgentClient:
        if not cell.endpoint:
            raise RuntimeError(f"cell {cell.id!r} has no cell-agent endpoint")
        key = f"{cell.endpoint}\0{self.settings.agent_shared_secret or ''}"
        if key not in self._agent_clients:
            self._agent_clients[key] = CellAgentClient(cell.endpoint, token=self.settings.agent_shared_secret)
        return self._agent_clients[key]

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

    @staticmethod
    def _mutation(command: OutboxRecord, fencing_token: str) -> MutationModel:
        return MutationModel(
            idempotency_key=command.idempotency_key,
            generation=command.generation,
            fencing_token=fencing_token,
        )

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
    def _required_agent_endpoint(cell: CellRecord) -> str:
        """Return a cell endpoint after the gang endpoint invariant is established."""

        endpoint = cell.endpoint
        if endpoint is None:
            raise RuntimeError("native workload gang cells require a trusted cell-agent endpoint")
        return endpoint
