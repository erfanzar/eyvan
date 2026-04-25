# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Contract tests for typed Eyvan workload autoscaling progress."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from eyvan.api.app import create_app
from eyvan.capacity import CapacityStatus
from eyvan.config import EyvanSettings
from eyvan.drivers.base import CapacityProviderFailureCode
from eyvan.persistence import InMemoryRepository
from eyvan.service import ControlPlaneService
from eyvan.service.models import (
    WORKER_COUNT_LABEL,
    AutoscaleBlockReason,
    CapacityClass,
    CapacityPoolRecord,
    CellRecord,
    CellState,
    CommandExecution,
    GcpQueueAdmissionDenialTarget,
    OutboxRecord,
    OutboxState,
    Principal,
    ResourceGang,
    ResourceVector,
    Role,
    RoleGrant,
    WorkloadDefinition,
    WorkloadProgress,
    WorkloadProgressPhase,
    WorkloadRecord,
    WorkloadState,
)
from eyvan.service.workload_progress import WorkloadProgressResolver


def _operator() -> Principal:
    return Principal(
        subject="alice",
        grants=(RoleGrant(role=Role.PROJECT_OPERATOR, org_id="acme", project_id="research"),),
    )


def _headers() -> dict[str, str]:
    return {
        "X-Eyvan-User": "alice",
        "X-Eyvan-Org": "acme",
        "X-Eyvan-Project": "research",
        "X-Eyvan-Roles": "operator",
    }


def _pool() -> CapacityPoolRecord:
    resources = ResourceVector(cpu=32, tpu=16)
    return CapacityPoolRecord(
        id="research-us-central1-c-v6e-16",
        fleet_name="research-us-central1-c",
        node_type="v6e-16",
        provider="gcp",
        capacity_backend="gcp-tpu",
        provider_project="example-project",
        region="us-central1",
        zone="us-central1-c",
        capacity=resources,
        allocatable=resources,
        accelerator_type="v6e-16",
        capacity_class=CapacityClass.SPOT,
        executor="eyvan-native",
        agent_endpoint="http://127.0.0.1:8090",
        labels={"eyvan_resource_kind": "tpu"},
        max_cells=10_000,
    )


def _workload(pool: CapacityPoolRecord) -> WorkloadRecord:
    return WorkloadRecord(
        id="wl-v6e-16-pods-2",
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(
            name="tests.train",
            execution=CommandExecution(command="python train.py"),
            gang=ResourceGang(per_replica=ResourceVector(cpu=64, tpu=32)),
            provider="gcp",
            region="us-central1",
            accelerator_type="v6e-16",
            capacity_class=CapacityClass.SPOT,
            required_executor="eyvan-native",
            required_labels={
                "eyvan_pool": pool.id,
                "eyvan_pool_template": pool.template_fingerprint,
                WORKER_COUNT_LABEL: "2",
            },
            cell_group_count=1,
            worker_count=2,
        ),
        created_by="alice",
    )


async def _seed(
    repository: InMemoryRepository,
    pool: CapacityPoolRecord,
    workload: WorkloadRecord,
    *,
    cells: tuple[CellRecord, ...] = (),
    commands: tuple[OutboxRecord, ...] = (),
) -> None:
    async with repository.transaction() as transaction:
        await transaction.put_capacity_pool(pool)
        await transaction.put_workload(workload)
        for cell in cells:
            await transaction.put_cell(cell)
        for command in commands:
            await transaction.put_outbox(command)


def test_workload_progress_endpoint_reports_scale_from_zero_for_two_tpu_workers() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    workload = _workload(pool)
    asyncio.run(_seed(repository, pool, workload))
    app = create_app(
        settings=EyvanSettings(controller_enabled=False, dev_auth_enabled=True),
        repository=repository,
        service=service,
    )

    with TestClient(app) as client:
        response = client.get(f"/api/v1/workloads/{workload.id}/progress", headers=_headers())

    assert response.status_code == 200
    progress = WorkloadProgress.model_validate(response.json())
    assert progress.phase is WorkloadProgressPhase.WAITING_FOR_AUTOSCALER
    assert progress.request.cells == 1
    assert progress.request.workers_per_cell == 2
    assert progress.request.resources_per_cell == ResourceVector(cpu=64, tpu=32)
    assert progress.request.total_resources == ResourceVector(cpu=64, tpu=32)
    assert progress.capacity.active_cells == 0
    assert progress.capacity.ready_workers == 0
    assert progress.pool is not None
    assert progress.pool.id == pool.id


@pytest.mark.asyncio
async def test_workload_progress_expands_gang_replicas_before_expanding_physical_cells() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    workload = WorkloadRecord(
        id="wl-gang-replicas",
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(
            name="tests.gang",
            execution=CommandExecution(command="python train.py"),
            gang=ResourceGang(replicas=3, per_replica=ResourceVector(cpu=4, memory_gib=0, tpu=8)),
            provider="gcp",
            region="us-central1",
            accelerator_type="v6e-16",
            capacity_class=CapacityClass.SPOT,
            required_executor="eyvan-native",
            required_labels={
                "eyvan_pool": pool.id,
                "eyvan_pool_template": pool.template_fingerprint,
                WORKER_COUNT_LABEL: "1",
            },
            cell_group_count=2,
        ),
        created_by="alice",
    )
    await _seed(repository, pool, workload)

    progress = await WorkloadProgressResolver(service, repository).resolve(_operator(), workload.id)

    assert progress.request.resources_per_cell == ResourceVector(cpu=12, memory_gib=0, tpu=24)
    assert progress.request.total_resources == ResourceVector(cpu=24, memory_gib=0, tpu=48)


@pytest.mark.asyncio
async def test_workload_progress_reports_leased_capacity_as_waiting_for_dispatch() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    workload = _workload(pool).model_copy(
        update={"state": WorkloadState.LEASED, "lease_id": "lease-v6e-16", "lease_ids": ("lease-v6e-16",)}
    )
    await _seed(repository, pool, workload)

    progress = await WorkloadProgressResolver(service, repository).resolve(_operator(), workload.id)

    assert progress.phase is WorkloadProgressPhase.WAITING_FOR_DISPATCH


@pytest.mark.asyncio
async def test_workload_progress_maps_shared_tpu_quota_block_reason() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    workload = _workload(pool).model_copy(update={"autoscale_block_reason": AutoscaleBlockReason.SHARED_TPU_QUOTA})
    legacy_payload = workload.model_dump()
    legacy_payload.pop("autoscale_block_reason")
    assert WorkloadRecord.model_validate(legacy_payload).autoscale_block_reason is None
    await _seed(repository, pool, workload)

    progress = await WorkloadProgressResolver(service, repository).resolve(_operator(), workload.id)

    assert progress.phase is WorkloadProgressPhase.QUOTA_BLOCKED


@pytest.mark.asyncio
async def test_workload_progress_reports_queued_capacity_without_claiming_it_is_provisioning() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    workload = _workload(pool)
    queued = pool.make_cell(
        "research-us-central1-c-v6e-16-0001",
        worker_count=2,
        autoscale_request_workload_id=workload.id,
    ).model_copy(update={"provider_phase": CapacityStatus.WAITING})
    await _seed(repository, pool, workload, cells=(queued,))

    progress = await WorkloadProgressResolver(service, repository).resolve(_operator(), workload.id)

    assert progress.phase is WorkloadProgressPhase.WAITING_FOR_CAPACITY
    assert progress.capacity.active_cells == 1
    assert progress.capacity.active_workers == 2
    assert progress.capacity.waiting_cells == 1
    assert progress.capacity.waiting_workers == 2
    assert progress.capacity.provisioning_cells == 0
    assert progress.capacity.provisioning_workers == 0


@pytest.mark.asyncio
async def test_workload_progress_reports_pre_rpc_request_without_claiming_gcp_is_provisioning() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    workload = _workload(pool)
    requesting = pool.make_cell(
        "research-us-central1-c-v6e-16-0001",
        worker_count=2,
        autoscale_request_workload_id=workload.id,
    ).model_copy(update={"provider_phase": CapacityStatus.REQUESTING})
    command = OutboxRecord(
        id="capacity-apply-requesting-v6e-16",
        topic="capacity.apply",
        aggregate_type="cell",
        aggregate_id=requesting.id,
        idempotency_key="capacity-apply-requesting-v6e-16",
        generation=requesting.generation,
        state=OutboxState.PROCESSING,
    )
    await _seed(repository, pool, workload, cells=(requesting,), commands=(command,))

    progress = await WorkloadProgressResolver(service, repository).resolve(_operator(), workload.id)

    assert progress.phase is WorkloadProgressPhase.REQUESTING_CAPACITY
    assert progress.capacity.requesting_cells == 1
    assert progress.capacity.requesting_workers == 2
    assert progress.capacity.waiting_workers == 0
    assert progress.capacity.provisioning_workers == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_phase", "expected_phase"),
    (
        (CapacityStatus.PROVISIONING, WorkloadProgressPhase.PROVISIONING),
        (CapacityStatus.ACTIVE, WorkloadProgressPhase.CAPACITY_ALLOCATED),
        (CapacityStatus.EXECUTION_CONNECTING, WorkloadProgressPhase.NATIVE_WORKERS_CONNECTING),
    ),
)
async def test_workload_progress_preserves_distinct_provider_and_native_readiness_states(
    provider_phase: CapacityStatus,
    expected_phase: WorkloadProgressPhase,
) -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    workload = _workload(pool)
    cell = pool.make_cell(
        "research-us-central1-c-v6e-16-0003",
        worker_count=2,
        autoscale_request_workload_id=workload.id,
    ).model_copy(update={"provider_phase": provider_phase})
    await _seed(repository, pool, workload, cells=(cell,))

    progress = await WorkloadProgressResolver(service, repository).resolve(_operator(), workload.id)

    assert progress.phase is expected_phase
    assert progress.capacity.provisioning_workers == 2
    assert progress.capacity.observed_tpu_vms is None


@pytest.mark.asyncio
async def test_workload_progress_reports_only_complete_gcp_observed_tpu_vm_counts() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    workload = _workload(pool)
    observed = pool.make_cell(
        "research-us-central1-c-v6e-16-0004",
        worker_count=2,
        autoscale_request_workload_id=workload.id,
    ).model_copy(
        update={
            "provider_phase": CapacityStatus.EXECUTION_CONNECTING,
            "observed_tpu_vms": 8,
        }
    )
    await _seed(repository, pool, workload, cells=(observed,))

    progress = await WorkloadProgressResolver(service, repository).resolve(_operator(), workload.id)

    assert progress.phase is WorkloadProgressPhase.NATIVE_WORKERS_CONNECTING
    assert progress.capacity.observed_tpu_vms == 8


@pytest.mark.asyncio
async def test_workload_progress_preserves_plain_gcloud_permission_denial_without_raw_stderr() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    workload = _workload(pool)
    failed_cell = pool.make_cell(
        "research-us-central1-c-v6e-16-0002",
        worker_count=2,
        autoscale_request_workload_id=workload.id,
    ).model_copy(update={"state": CellState.UNAVAILABLE, "provider_phase": CapacityStatus.PROVIDER_ERROR})
    stderr = (
        "Creating queued resource...\n"
        "ERROR: (gcloud.compute.tpus.queued-resources.create) \x1b[31mPERMISSION_DENIED: "
        'User does not have permission to submit requests into this queue for accelerator type "v6e-16" '
        "in location us-central1-c. [EID: 0xc1d610436a386e57]\n"
        "gcloud command output omitted from user-facing progress."
    )
    command = OutboxRecord(
        id="capacity-apply-v6e-16",
        topic="capacity.apply",
        aggregate_type="cell",
        aggregate_id=failed_cell.id,
        idempotency_key="capacity-apply-v6e-16",
        generation=failed_cell.generation,
        state=OutboxState.FAILED,
        attempts=1,
        last_error=stderr,
        failure_code=CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED,
    )
    await _seed(repository, pool, workload, cells=(failed_cell,), commands=(command,))

    progress = await WorkloadProgressResolver(service, repository).resolve(_operator(), workload.id)

    assert progress.phase is WorkloadProgressPhase.QUEUE_ADMISSION_DENIED
    assert progress.capacity.lost_cells == 1
    assert progress.capacity.lost_workers == 2
    assert progress.retry is not None
    assert progress.retry.state is OutboxState.FAILED
    assert progress.retry.failure_code is CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED
    assert progress.retry.diagnostic is not None
    assert "PERMISSION_DENIED" in progress.retry.diagnostic
    assert "does not have permission to submit requests into this queue" in progress.retry.diagnostic
    assert "Creating queued resource" not in progress.retry.diagnostic
    assert "omitted from user-facing progress" not in progress.retry.diagnostic
    assert "\x1b" not in progress.retry.diagnostic
    assert len(progress.retry.diagnostic) <= 500


@pytest.mark.asyncio
async def test_terminal_queue_admission_denial_keeps_its_immutable_target() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    denied_target = GcpQueueAdmissionDenialTarget(
        provider_project="example-project",
        zone="us-east5-a",
        accelerator_type="v5p-16",
        capacity_class=CapacityClass.SPOT,
    )
    workload = _workload(pool).model_copy(
        update={
            "state": WorkloadState.FAILED,
            "failure_reason": "GCP denied queue admission",
            "autoscale_block_reason": AutoscaleBlockReason.QUEUE_ADMISSION_DENIED,
            "queue_admission_denial": denied_target,
        }
    )
    await _seed(repository, pool, workload)

    progress = await WorkloadProgressResolver(service, repository).resolve(_operator(), workload.id)

    assert progress.workload_state is WorkloadState.FAILED
    assert progress.phase is WorkloadProgressPhase.QUEUE_ADMISSION_DENIED
    assert progress.queue_admission_denial == denied_target
    assert progress.pool is not None
    assert progress.pool.zone == "us-central1-c"


@pytest.mark.asyncio
async def test_workload_progress_does_not_report_another_workloads_same_pool_capacity() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    pool = _pool()
    workload = _workload(pool)
    foreign_cell = pool.make_cell(
        "research-us-central1-c-v6e-16-foreign",
        worker_count=2,
        autoscale_request_workload_id="wl-other-project",
    ).model_copy(update={"state": CellState.READY, "provider_phase": CapacityStatus.HEALTHY})
    foreign_command = OutboxRecord(
        id="foreign-capacity-apply",
        topic="capacity.apply",
        aggregate_type="cell",
        aggregate_id=foreign_cell.id,
        idempotency_key="foreign-capacity-apply",
        generation=foreign_cell.generation,
        state=OutboxState.FAILED,
        attempts=1,
        last_error="ERROR: PERMISSION_DENIED: foreign diagnostic",
    )
    await _seed(repository, pool, workload, cells=(foreign_cell,), commands=(foreign_command,))

    progress = await WorkloadProgressResolver(service, repository).resolve(_operator(), workload.id)

    assert progress.phase is WorkloadProgressPhase.WAITING_FOR_AUTOSCALER
    assert progress.capacity.active_cells == 0
    assert progress.capacity.ready_workers == 0
    assert progress.retry is None
