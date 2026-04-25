# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Typed, read-only autoscaling progress for one workload."""

from __future__ import annotations

from dataclasses import dataclass

from eyvan.capacity import PENDING_CAPACITY_STATUSES, QUEUED_CAPACITY_STATUSES, CapacityStatus
from eyvan.drivers.base import CapacityProviderFailureCode
from eyvan.persistence.base import Repository
from eyvan.service.control_plane import ControlPlaneService
from eyvan.service.models import (
    AutoscaleBlockReason,
    CapacityRetryProgress,
    CellRecord,
    CellState,
    OutboxRecord,
    OutboxState,
    Principal,
    WorkloadCapacityPhaseProgress,
    WorkloadCapacityRequest,
    WorkloadCapacitySummary,
    WorkloadPoolProgress,
    WorkloadProgress,
    WorkloadProgressPhase,
    WorkloadRecord,
    WorkloadState,
    scale_resources,
)
from eyvan.service.provider_diagnostics import safe_provider_diagnostic
from eyvan.service.state import ACTIVE_LEASE_STATES, TERMINAL_WORKLOAD_STATES

_POOL_LABEL = "eyvan_pool"
_POOL_TEMPLATE_LABEL = "eyvan_pool_template"


@dataclass(frozen=True, slots=True)
class WorkloadProgressResolver:
    """Resolve a workload's exact physical capacity request and current supply."""

    service: ControlPlaneService
    repository: Repository

    async def resolve(self, principal: Principal, workload_id: str) -> WorkloadProgress:
        """Return one authorized workload's pool, capacity, and retry progress."""

        workload = await self.service.get_workload(principal, workload_id)
        pool_id = workload.spec.required_labels.get(_POOL_LABEL)
        async with self.repository.transaction() as transaction:
            pool = None if pool_id is None else await transaction.get_capacity_pool(pool_id)
            request_cells = await transaction.list_cells_for_autoscale_request(workload.id)
            leases = await transaction.list_leases(workload_id=workload.id)
            leased_cells: list[CellRecord] = []
            for lease in leases:
                if lease.state not in ACTIVE_LEASE_STATES:
                    continue
                cell = await transaction.get_cell(lease.cell_id)
                if cell is not None:
                    leased_cells.append(cell)

            cells_by_id = {cell.id: cell for cell in request_cells}
            for cell in leased_cells:
                cells_by_id[cell.id] = cell
            matching_cells = tuple(
                cell
                for cell in sorted(cells_by_id.values(), key=lambda candidate: candidate.id)
                if _matches_workload_capacity(cell, workload, pool_id)
            )
            commands = await transaction.list_outbox_for_aggregates(
                "cell",
                {cell.id: cell.generation for cell in matching_cells},
                topic="capacity.apply",
            )
            # Read in the same transaction as everything else on this route. The
            # dashboard and ``client/callables.py`` already poll it, so a training
            # loop's own progress needs no new polling loop anywhere.
            reports = await transaction.list_progress_reports(workload.id)

        capacity = _capacity_summary(matching_cells)
        retry = _capacity_retry(matching_cells, commands)
        resources_per_cell = workload.spec.resources_per_cell
        return WorkloadProgress(
            workload_id=workload.id,
            workload_state=workload.state,
            phase=_phase(workload, capacity, retry),
            scheduling_reason=workload.scheduling_reason,
            failure_reason=workload.failure_reason,
            autoscale_block_reason=workload.autoscale_block_reason,
            queue_admission_denial=workload.queue_admission_denial,
            request=WorkloadCapacityRequest(
                cells=workload.spec.cell_group_count,
                workers_per_cell=workload.spec.worker_count,
                resources_per_cell=resources_per_cell,
                total_resources=scale_resources(resources_per_cell, workload.spec.cell_group_count),
            ),
            pool=None
            if pool is None
            else WorkloadPoolProgress(
                id=pool.id,
                fleet_name=pool.fleet_name,
                node_type=pool.node_type,
                provider=pool.provider,
                provider_project=pool.provider_project,
                region=pool.region,
                zone=pool.zone,
                accelerator_type=pool.accelerator_type,
                capacity_class=pool.capacity_class,
            ),
            capacity=capacity,
            retry=retry,
            user_progress=tuple(report for report in reports if report.attempt >= workload.attempt),
        )


def _matches_workload_capacity(cell: CellRecord, workload: WorkloadRecord, pool_id: str | None) -> bool:
    """Return whether a live cell is an exact physical candidate for a pinned workload."""

    if pool_id is None or cell.state is CellState.DELETED:
        return False
    if cell.pool_id != pool_id or cell.worker_count != workload.spec.worker_count:
        return False
    template = workload.spec.required_labels.get(_POOL_TEMPLATE_LABEL)
    return template is None or cell.labels.get(_POOL_TEMPLATE_LABEL) == template


def _capacity_summary(cells: tuple[CellRecord, ...]) -> WorkloadCapacitySummary:
    """Aggregate matching cells without exposing unrelated fleet inventory."""

    grouped: dict[tuple[CellState, CapacityStatus | None], tuple[int, int]] = {}
    ready_cells = ready_workers = 0
    requesting_cells = requesting_workers = 0
    waiting_cells = waiting_workers = 0
    provisioning_cells = provisioning_workers = 0
    lost_cells = lost_workers = 0

    for cell in cells:
        key = (cell.state, cell.provider_phase)
        previous_cells, previous_workers = grouped.get(key, (0, 0))
        grouped[key] = (previous_cells + 1, previous_workers + cell.worker_count)

        if cell.state is CellState.READY:
            ready_cells += 1
            ready_workers += cell.worker_count
        if cell.provider_phase is CapacityStatus.REQUESTING:
            requesting_cells += 1
            requesting_workers += cell.worker_count
        if cell.provider_phase in QUEUED_CAPACITY_STATUSES:
            waiting_cells += 1
            waiting_workers += cell.worker_count
        if _is_provisioning(cell):
            provisioning_cells += 1
            provisioning_workers += cell.worker_count
        if cell.state is CellState.UNAVAILABLE or cell.provider_phase is CapacityStatus.PROVIDER_ERROR:
            lost_cells += 1
            lost_workers += cell.worker_count

    phases = tuple(
        WorkloadCapacityPhaseProgress(
            state=state,
            provider_phase=provider_phase,
            cells=cell_count,
            workers=worker_count,
        )
        for (state, provider_phase), (cell_count, worker_count) in sorted(
            grouped.items(), key=lambda item: (item[0][0].value, "" if item[0][1] is None else item[0][1].value)
        )
    )
    active_cells = len(cells)
    active_workers = sum(cell.worker_count for cell in cells)
    return WorkloadCapacitySummary(
        active_cells=active_cells,
        active_workers=active_workers,
        ready_cells=ready_cells,
        ready_workers=ready_workers,
        requesting_cells=requesting_cells,
        requesting_workers=requesting_workers,
        waiting_cells=waiting_cells,
        waiting_workers=waiting_workers,
        provisioning_cells=provisioning_cells,
        provisioning_workers=provisioning_workers,
        lost_cells=lost_cells,
        lost_workers=lost_workers,
        observed_tpu_vms=_observed_tpu_vms(cells),
        phases=phases,
    )


def _observed_tpu_vms(cells: tuple[CellRecord, ...]) -> int | None:
    """Return a total only when GCP observed every matching TPU allocation."""

    if not cells:
        return None
    total = 0
    for cell in cells:
        count = cell.observed_tpu_vms
        if count is None:
            return None
        total += count
    return total


def _is_provisioning(cell: CellRecord) -> bool:
    """Return whether one registered cell is actively acquiring or connecting capacity."""

    if cell.state is not CellState.REGISTERING:
        return False
    return (
        cell.provider_phase is not None
        and cell.provider_phase is not CapacityStatus.REQUESTING
        and (cell.provider_phase in PENDING_CAPACITY_STATUSES and cell.provider_phase not in QUEUED_CAPACITY_STATUSES)
    )


def _capacity_retry(
    cells: tuple[CellRecord, ...],
    commands: list[OutboxRecord],
) -> CapacityRetryProgress | None:
    """Return the newest current-generation capacity mutation for matching cells."""

    generation_by_cell = {cell.id: cell.generation for cell in cells}
    relevant = tuple(
        command
        for command in commands
        if command.topic == "capacity.apply"
        and command.aggregate_type == "cell"
        and generation_by_cell.get(command.aggregate_id) == command.generation
    )
    if not relevant:
        return None
    command = max(relevant, key=lambda candidate: (candidate.updated_at, candidate.created_at, candidate.id))
    return CapacityRetryProgress(
        state=command.state,
        attempts=command.attempts,
        next_attempt_at=command.available_at if command.state is OutboxState.FAILED else None,
        diagnostic=safe_provider_diagnostic(command.last_error),
        failure_code=command.failure_code,
    )


def _phase(
    workload: WorkloadRecord,
    capacity: WorkloadCapacitySummary,
    retry: CapacityRetryProgress | None,
) -> WorkloadProgressPhase:
    """Map durable workload and capacity state to one user-facing phase."""

    if workload.autoscale_block_reason is AutoscaleBlockReason.QUEUE_ADMISSION_DENIED:
        return WorkloadProgressPhase.QUEUE_ADMISSION_DENIED
    if workload.state in TERMINAL_WORKLOAD_STATES:
        return WorkloadProgressPhase.TERMINAL
    if workload.state is WorkloadState.CANCELLING:
        return WorkloadProgressPhase.CANCELLING
    if workload.state is WorkloadState.CHECKPOINTING:
        return WorkloadProgressPhase.CHECKPOINTING
    if workload.state is WorkloadState.RUNNING:
        return WorkloadProgressPhase.RUNNING
    if workload.state is WorkloadState.DISPATCHING:
        return WorkloadProgressPhase.DISPATCHING
    if workload.preemption_id is not None:
        return WorkloadProgressPhase.PREEMPTED
    if workload.state is WorkloadState.LEASED:
        return WorkloadProgressPhase.WAITING_FOR_DISPATCH
    if workload.autoscale_block_reason is AutoscaleBlockReason.SHARED_TPU_QUOTA:
        return WorkloadProgressPhase.QUOTA_BLOCKED
    if workload.state is WorkloadState.ADMITTED:
        return WorkloadProgressPhase.WAITING_FOR_SCHEDULER
    requested_workers = workload.spec.cell_group_count * workload.spec.worker_count
    if capacity.ready_workers >= requested_workers:
        return WorkloadProgressPhase.READY_FOR_DISPATCH
    if capacity.requesting_workers:
        return WorkloadProgressPhase.REQUESTING_CAPACITY
    if any(entry.provider_phase is CapacityStatus.EXECUTION_CONNECTING for entry in capacity.phases):
        return WorkloadProgressPhase.NATIVE_WORKERS_CONNECTING
    if any(entry.provider_phase is CapacityStatus.ACTIVE for entry in capacity.phases):
        return WorkloadProgressPhase.CAPACITY_ALLOCATED
    if capacity.provisioning_workers:
        return WorkloadProgressPhase.PROVISIONING
    if capacity.waiting_workers:
        return WorkloadProgressPhase.WAITING_FOR_CAPACITY
    if any(entry.provider_phase is CapacityStatus.HALTED_QUOTA for entry in capacity.phases):
        return WorkloadProgressPhase.QUOTA_BLOCKED
    if retry is not None and retry.failure_code is CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED:
        return WorkloadProgressPhase.QUEUE_ADMISSION_DENIED
    if capacity.lost_workers or (retry is not None and retry.diagnostic is not None):
        return WorkloadProgressPhase.PROVIDER_FAILURE
    return WorkloadProgressPhase.WAITING_FOR_AUTOSCALER


__all__ = ("WorkloadProgressResolver",)
