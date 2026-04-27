# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from eyvan.artifacts.managed import result_uri
from eyvan.records import (
    WORKER_COUNT_LABEL,
    AutoscaleBlockReason,
    CallableExecution,
    CapacityClass,
    CapacityPoolRecord,
    CellRecord,
    CellState,
    CommandExecution,
    LeaseRecord,
    LeaseState,
    PolicyRecord,
    Principal,
    QuotaRecord,
    ResourceGang,
    ResourceHours,
    ResourceVector,
    Role,
    RoleGrant,
    WorkerResourceScope,
    WorkloadDefinition,
    WorkloadRecord,
    WorkloadState,
    utcnow,
)
from eyvan.server.persistence import InMemoryRepository
from eyvan.server.service import ControlPlaneService
from eyvan.server.service.callable_trust import CallableProjectTrustPolicy
from eyvan.server.service.errors import (
    AuthorizationError,
    ConflictError,
    InputError,
    NoMatchingCapacityPoolError,
    NotFoundError,
    ServiceErrorCode,
)
from eyvan.server.service.scheduling import CoreSchedulingService, to_core_cell


def _principal(role: Role, *, subject: str = "alice", project: str = "research") -> Principal:
    return Principal(subject=subject, grants=(RoleGrant(role=role, org_id="acme", project_id=project),))


def _spec(command: str = "python train.py", *, retries: int = 0) -> WorkloadDefinition:
    return WorkloadDefinition(
        name="train",
        execution=CommandExecution(command=command),
        max_retries=retries,
    )


def _capacity_pool(
    *,
    pool_id: str = "opaque-provider-id",
    node_type: str = "v5p-workers",
    enabled: bool = True,
    max_cells: int = 4,
    max_replica_resources: ResourceVector | None = None,
) -> CapacityPoolRecord:
    resources = ResourceVector(cpu=0, memory_gib=0, tpu=8)
    return CapacityPoolRecord(
        id=pool_id,
        fleet_name="research-fleet",
        node_type=node_type,
        provider="gcp",
        capacity_backend="gcp-tpu",
        provider_project="gcp-project-a",
        region="us-east5",
        zone="us-east5-a",
        capacity=resources,
        allocatable=resources,
        accelerator_type="v5p-8",
        capacity_class="spot",
        executor="native-local",
        agent_endpoint="http://agent:8090",
        topology={"fabric": "ici"},
        max_replica_resources=max_replica_resources,
        min_cells=0,
        initial_cells=0,
        max_cells=max_cells,
        enabled=enabled,
    )


def test_callable_project_trust_policy_supports_scoped_and_project_entries() -> None:
    policy = CallableProjectTrustPolicy.from_projects({"acme/research", "shared"})

    assert policy.allows("acme", "research")
    assert policy.allows("another-org", "shared")
    assert not policy.allows("acme", "other")
    with pytest.raises(
        AuthorizationError,
        match="callable workloads are not trusted for project acme/other",
    ):
        policy.require("acme", "other")


@pytest.mark.asyncio
async def test_fleet_apply_validates_shared_tpu_quota_budget_for_direct_service_callers() -> None:
    service = ControlPlaneService(InMemoryRepository())
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    first = _capacity_pool(pool_id="research/v5p-16", node_type="v5p-16").model_copy(
        update={
            "initial_cells": 1,
            "min_cells": 1,
            "quota_group": "gcp-project-us-east5-a-v5p-spot",
            "quota_limit": 47,
            "quota_units_per_cell": 16,
        }
    )
    second = _capacity_pool(pool_id="research/v5p-32", node_type="v5p-32").model_copy(
        update={
            "accelerator_type": "v5p-32",
            "capacity": ResourceVector(cpu=32, tpu=16),
            "allocatable": ResourceVector(cpu=32, tpu=16),
            "initial_cells": 1,
            "min_cells": 1,
            "quota_group": "gcp-project-us-east5-a-v5p-spot",
            "quota_limit": 47,
            "quota_units_per_cell": 32,
        }
    )

    with pytest.raises(InputError, match="initial capacity requires 48 quota units"):
        await service.apply_fleet(
            fleet_admin,
            "research-fleet",
            (first, second),
            idempotency_key="quota-budget",
        )

    stored, replayed = await service.apply_fleet(
        fleet_admin,
        "research-fleet",
        (
            first.model_copy(update={"quota_limit": 48}),
            second.model_copy(update={"quota_limit": 48}),
        ),
        idempotency_key="quota-budget-valid",
    )

    assert not replayed
    assert {(pool.node_type, pool.quota_limit, pool.quota_units_per_cell) for pool in stored} == {
        ("v5p-16", 48, 16),
        ("v5p-32", 48, 32),
    }


@pytest.mark.asyncio
async def test_submit_is_atomic_and_idempotent_under_concurrency() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    operator = _principal(Role.PROJECT_OPERATOR)

    first, second = await asyncio.gather(
        service.submit_workload(
            operator, org_id="acme", project_id="research", spec=_spec(), idempotency_key="request-1"
        ),
        service.submit_workload(
            operator, org_id="acme", project_id="research", spec=_spec(), idempotency_key="request-1"
        ),
    )

    assert first[0].id == second[0].id
    assert {first[1], second[1]} == {False, True}
    events = await service.list_events(operator, org_id="acme", project_id="research")
    assert [event.action for event in events] == ["workload.submitted"]

    with pytest.raises(ConflictError, match="different request"):
        await service.submit_workload(
            operator,
            org_id="acme",
            project_id="research",
            spec=_spec("python other.py"),
            idempotency_key="request-1",
        )


@pytest.mark.asyncio
async def test_project_rbac_and_callable_trust_are_enforced() -> None:
    service = ControlPlaneService(InMemoryRepository())
    operator = _principal(Role.PROJECT_OPERATOR)
    other_project = _principal(Role.PROJECT_OPERATOR, project="other")
    callable_spec = WorkloadDefinition(
        name="callable",
        execution=CallableExecution(
            artifact_uri="gs://trusted/artifact.pkl",
            sha256="a" * 64,
            python="3.12",
        ),
    )

    with pytest.raises(AuthorizationError, match="not trusted"):
        await service.submit_workload(
            operator,
            org_id="acme",
            project_id="research",
            spec=callable_spec,
            idempotency_key="callable-1",
        )
    with pytest.raises(AuthorizationError, match="lacks submit"):
        await service.submit_workload(
            other_project,
            org_id="acme",
            project_id="research",
            spec=_spec(),
            idempotency_key="other-1",
        )


@pytest.mark.asyncio
async def test_cancel_and_retry_follow_public_state_machine() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    operator = _principal(Role.PROJECT_OPERATOR)
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    workload, _ = await service.submit_workload(
        operator,
        org_id="acme",
        project_id="research",
        spec=_spec(retries=1),
        idempotency_key="submit",
    )
    cancelled, _ = await service.cancel_workload(operator, workload.id, idempotency_key="cancel")
    assert cancelled.state == WorkloadState.CANCELLED
    async with repository.transaction() as transaction:
        await transaction.put_workload(
            cancelled.model_copy(
                update={
                    "autoscale_block_reason": AutoscaleBlockReason.SHARED_TPU_QUOTA,
                    "scheduling_reason": "previous scheduling reason",
                }
            )
        )
    retried, _ = await service.retry_workload(operator, workload.id, idempotency_key="retry")
    assert (retried.state, retried.attempt, retried.generation) == (WorkloadState.PENDING, 2, 2)
    assert retried.autoscale_block_reason is None
    assert retried.scheduling_reason is None

    failed = await service.transition_workload(fleet_admin, retried.id, WorkloadState.FAILED)
    assert failed.state == WorkloadState.FAILED
    with pytest.raises(ConflictError, match="exhausted"):
        await service.retry_workload(operator, workload.id, idempotency_key="retry-2")


@pytest.mark.asyncio
async def test_manual_checkpoints_in_one_attempt_use_distinct_backend_operations() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    operator = _principal(Role.PROJECT_OPERATOR)
    spec = WorkloadDefinition(
        name="checkpointable",
        execution=CommandExecution(command="python train.py"),
        checkpointable=True,
        checkpoint_hook="python checkpoint.py",
        checkpoint_output_uri="gs://checkpoints/train",
    )
    workload = WorkloadRecord(
        id="wl-checkpoint",
        org_id="acme",
        project_id="research",
        spec=spec,
        state=WorkloadState.RUNNING,
        lease_id="lease-checkpoint",
        remote_id="ray-job",
        created_by="alice",
    )
    lease = LeaseRecord(
        id="lease-checkpoint",
        workload_id=workload.id,
        cell_id="cell-a",
        org_id=workload.org_id,
        project_id=workload.project_id,
        state=LeaseState.ACTIVE,
        resources=spec.gang,
        fencing_token="fence",
        expires_at=utcnow() + timedelta(minutes=5),
    )
    async with repository.transaction() as transaction:
        await transaction.put_workload(workload)
        await transaction.put_lease(lease)

    first, replayed = await service.checkpoint_workload(
        operator,
        workload.id,
        idempotency_key="checkpoint-one",
    )
    assert not replayed
    replay, replayed = await service.checkpoint_workload(
        operator,
        workload.id,
        idempotency_key="checkpoint-one",
    )
    assert replayed and replay.id == first.id
    async with repository.transaction() as transaction:
        current = await transaction.get_workload(workload.id)
        assert current is not None
        await transaction.put_workload(current.model_copy(update={"state": WorkloadState.RUNNING}))

    _, replayed = await service.checkpoint_workload(
        operator,
        workload.id,
        idempotency_key="checkpoint-two",
    )
    assert not replayed
    async with repository.transaction() as transaction:
        checkpoint_commands = [
            command for command in await transaction.list_outbox() if command.topic == "workload.checkpoint"
        ]
    assert len(checkpoint_commands) == 2
    assert len({command.idempotency_key for command in checkpoint_commands}) == 2


@pytest.mark.asyncio
async def test_submission_enforces_project_placement_policy() -> None:
    service = ControlPlaneService(InMemoryRepository())
    admin = _principal(Role.PROJECT_ADMIN)
    operator = _principal(Role.PROJECT_OPERATOR)
    await service.put_policy(
        admin,
        PolicyRecord(
            org_id="acme",
            project_id="research",
            allowed_providers=("gcp",),
            allowed_regions=("us-central2",),
            allowed_capacity_classes=("spot",),
            denied_accelerator_types=("v4-8",),
            updated_by="admin",
        ),
        idempotency_key="policy",
    )

    with pytest.raises(InputError, match="region"):
        await service.submit_workload(
            operator,
            org_id="acme",
            project_id="research",
            spec=_spec().model_copy(
                update={"provider": "gcp", "region": "europe-west4", "capacity_class": CapacityClass.SPOT}
            ),
            idempotency_key="submit-denied",
        )


@pytest.mark.asyncio
async def test_pool_submission_derives_and_pins_the_complete_capacity_contract() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    operator = _principal(Role.PROJECT_OPERATOR)
    pool = _capacity_pool()
    assert pool.model_copy(update={"unit_prices": {"tpu": 1.0}}).template_fingerprint == pool.template_fingerprint
    assert (
        pool.model_copy(update={"provider_options": {"runtime_version": "v2-alpha-tpuv5"}}).template_fingerprint
        != pool.template_fingerprint
    )
    await service.upsert_capacity_pool(fleet_admin, pool, idempotency_key="pool")

    workload, replayed = await service.submit_pool_workload(
        operator,
        fleet_name="research-fleet",
        node_type="v5p-workers",
        org_id="acme",
        project_id="research",
        command="python train.py --steps 10",
        environment={"RUN_ID": "experiment-1"},
        metadata={"owner": "research"},
        max_retries=2,
        idempotency_key="run-1",
    )

    assert not replayed
    assert workload.spec.name == "research-fleet/v5p-workers"
    assert workload.spec.execution.command == "python train.py --steps 10"
    assert workload.spec.gang.replicas == 1
    assert workload.spec.gang.per_replica == pool.allocatable
    assert workload.spec.gang.required_topology == {"fabric": "ici"}
    assert workload.spec.provider == "gcp"
    assert workload.spec.region == "us-east5"
    assert workload.spec.accelerator_type == "v5p-8"
    assert workload.spec.capacity_class is CapacityClass.SPOT
    assert workload.spec.required_executor == "native-local"
    assert workload.spec.required_labels == {
        "eyvan_pool": pool.id,
        "eyvan_pool_template": pool.template_fingerprint,
        WORKER_COUNT_LABEL: "1",
    }
    assert workload.spec.environment == {"RUN_ID": "experiment-1"}
    assert workload.spec.metadata == {
        "owner": "research",
        "eyvan_pool": pool.id,
        "eyvan_pool_template": pool.template_fingerprint,
    }
    assert workload.spec.max_retries == 2

    await service.disable_capacity_pool(fleet_admin, pool.id, idempotency_key="disable")
    replay, replayed = await service.submit_pool_workload(
        operator,
        fleet_name="research-fleet",
        node_type="v5p-workers",
        org_id="acme",
        project_id="research",
        command="python train.py --steps 10",
        environment={"RUN_ID": "experiment-1"},
        metadata={"owner": "research"},
        max_retries=2,
        idempotency_key="run-1",
    )
    assert replayed and replay.id == workload.id
    with pytest.raises(ConflictError, match="different request"):
        await service.submit_pool_workload(
            operator,
            fleet_name="research-fleet",
            node_type="v5p-workers",
            org_id="acme",
            project_id="research",
            command="python other.py",
            idempotency_key="run-1",
        )


@pytest.mark.asyncio
async def test_pool_submission_authorizes_before_pool_lookup() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)

    with pytest.raises(AuthorizationError, match="lacks submit"):
        await service.submit_pool_workload(
            _principal(Role.PROJECT_VIEWER),
            fleet_name="missing-fleet",
            node_type="missing-workers",
            org_id="acme",
            project_id="research",
            command="python train.py",
            idempotency_key="unauthorized",
        )
    async with repository.transaction() as transaction:
        assert await transaction.list_workloads() == []
        assert await transaction.list_events() == []


@pytest.mark.asyncio
async def test_submit_fleet_discovery_returns_sorted_runnable_names_for_the_authorized_scope() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    pools = (
        _capacity_pool(pool_id="zeta-v5p").model_copy(update={"fleet_name": "zeta-fleet"}),
        _capacity_pool(pool_id="alpha-v5p", node_type="v6e-workers").model_copy(
            update={"accelerator_type": "v6e-8", "fleet_name": "alpha-fleet"}
        ),
        _capacity_pool(pool_id="alpha-v6e", node_type="v6e-large").model_copy(
            update={"accelerator_type": "v6e-16", "fleet_name": "alpha-fleet"}
        ),
        _capacity_pool(pool_id="disabled-v5p", enabled=False).model_copy(update={"fleet_name": "disabled-fleet"}),
        _capacity_pool(pool_id="zero-v5p", max_cells=0).model_copy(update={"fleet_name": "zero-fleet"}),
    )
    for index, pool in enumerate(pools):
        await service.upsert_capacity_pool(fleet_admin, pool, idempotency_key=f"pool-{index}")

    fleets = await service.list_submit_fleets(
        _principal(Role.PROJECT_OPERATOR),
        org_id="acme",
        project_id="research",
    )

    assert fleets == ["alpha-fleet", "zeta-fleet"]
    with pytest.raises(AuthorizationError, match="lacks submit"):
        await service.list_submit_fleets(
            _principal(Role.PROJECT_VIEWER),
            org_id="acme",
            project_id="research",
        )
    with pytest.raises(AuthorizationError, match="lacks submit"):
        await service.list_submit_fleets(
            _principal(Role.PROJECT_OPERATOR, project="other"),
            org_id="acme",
            project_id="research",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pool", "match"),
    [
        (_capacity_pool(enabled=False), "disabled"),
        (_capacity_pool(max_cells=0), "max_workers set to zero"),
        (
            _capacity_pool(max_replica_resources=ResourceVector(cpu=0, memory_gib=0, tpu=4)),
            "partitions workers",
        ),
    ],
)
async def test_pool_submission_rejects_unschedulable_simple_run(pool: CapacityPoolRecord, match: str) -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    await service.upsert_capacity_pool(fleet_admin, pool, idempotency_key="pool")

    with pytest.raises(ConflictError, match=match):
        await service.submit_pool_workload(
            _principal(Role.PROJECT_OPERATOR),
            fleet_name=pool.fleet_name,
            node_type=pool.node_type,
            org_id="acme",
            project_id="research",
            command="python train.py",
            idempotency_key="run",
        )
    async with repository.transaction() as transaction:
        assert await transaction.list_workloads() == []


@pytest.mark.asyncio
async def test_pool_submission_enforces_policy_on_server_derived_placement() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    await service.upsert_capacity_pool(fleet_admin, _capacity_pool(), idempotency_key="pool")
    await service.put_policy(
        _principal(Role.PROJECT_ADMIN),
        PolicyRecord(
            org_id="acme",
            project_id="research",
            allowed_regions=("us-central2",),
            updated_by="admin",
        ),
        idempotency_key="policy",
    )

    with pytest.raises(InputError, match="region"):
        await service.submit_pool_workload(
            _principal(Role.PROJECT_OPERATOR),
            fleet_name="research-fleet",
            node_type="v5p-workers",
            org_id="acme",
            project_id="research",
            command="python train.py",
            idempotency_key="run",
        )


@pytest.mark.asyncio
async def test_pool_callable_submission_selects_and_pins_one_complete_tpu_slice() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(
        repository,
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    operator = _principal(Role.PROJECT_OPERATOR)
    pool = _capacity_pool()
    other = _capacity_pool(pool_id="other-pool", node_type="v6e-workers").model_copy(
        update={"accelerator_type": "v6e-8"}
    )
    await service.upsert_capacity_pool(fleet_admin, pool, idempotency_key="pool-v5p")
    await service.upsert_capacity_pool(fleet_admin, other, idempotency_key="pool-v6e")
    execution = CallableExecution(
        artifact_uri="gs://trusted/train.pkl",
        sha256="a" * 64,
        python="3.12",
        result_uri="gs://trusted/results/callable-v1.pkl",
    )

    workload, replayed = await service.submit_pool_callable(
        operator,
        fleet_name="research-fleet",
        org_id="acme",
        project_id="research",
        accelerator_type="v5p-8",
        pods=1,
        execution=execution,
        environment={"RUN_ID": "callable-1"},
        metadata={"owner": "research"},
        max_retries=3,
        idempotency_key="callable-v1",
    )

    assert not replayed
    assert workload.spec.name == "research-fleet/v5p-8"
    assert workload.spec.execution == execution
    assert workload.spec.gang.replicas == 1
    assert workload.spec.gang.per_replica == pool.allocatable
    assert workload.spec.gang.required_topology == pool.topology
    assert workload.spec.provider == pool.provider
    assert workload.spec.region == pool.region
    assert workload.spec.accelerator_type == pool.accelerator_type
    assert workload.spec.capacity_class == pool.capacity_class
    assert workload.spec.required_executor == pool.executor
    assert workload.spec.required_labels == {
        "eyvan_pool": pool.id,
        "eyvan_pool_template": pool.template_fingerprint,
        WORKER_COUNT_LABEL: "1",
    }
    assert workload.spec.environment == {"RUN_ID": "callable-1"}
    assert workload.spec.metadata == {
        "owner": "research",
        "eyvan_pool": pool.id,
        "eyvan_pool_template": pool.template_fingerprint,
    }
    assert workload.spec.max_retries == 3

    await service.disable_capacity_pool(fleet_admin, pool.id, idempotency_key="disable-v5p")
    replay, replayed = await service.submit_pool_callable(
        operator,
        fleet_name="research-fleet",
        org_id="acme",
        project_id="research",
        accelerator_type="v5p-8",
        pods=1,
        execution=execution,
        environment={"RUN_ID": "callable-1"},
        metadata={"owner": "research"},
        max_retries=3,
        idempotency_key="callable-v1",
    )
    assert replayed and replay.id == workload.id
    with pytest.raises(ConflictError, match="different request"):
        await service.submit_pool_callable(
            operator,
            fleet_name="research-fleet",
            org_id="acme",
            project_id="research",
            accelerator_type="v5p-8",
            pods=1,
            execution=execution.model_copy(update={"artifact_uri": "gs://trusted/other.pkl"}),
            idempotency_key="callable-v1",
        )


@pytest.mark.asyncio
async def test_pool_callable_submission_defaults_to_spot_for_duplicate_accelerator_types() -> None:
    """A callable must not silently select an on-demand pool of the same type."""

    repository = InMemoryRepository()
    service = ControlPlaneService(
        repository,
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    operator = _principal(Role.PROJECT_OPERATOR)
    on_demand = _capacity_pool(pool_id="v5p-on-demand").model_copy(update={"capacity_class": CapacityClass.ON_DEMAND})
    spot = _capacity_pool(pool_id="v5p-spot")
    await service.upsert_capacity_pool(fleet_admin, on_demand, idempotency_key="on-demand")
    await service.upsert_capacity_pool(fleet_admin, spot, idempotency_key="spot")
    execution = CallableExecution(
        artifact_uri="gs://trusted/train.pkl",
        sha256="b" * 64,
        python="3.12",
        result_uri="gs://trusted/results/callable-spot.pkl",
    )

    workload, replayed = await service.submit_pool_callable(
        operator,
        fleet_name="research-fleet",
        org_id="acme",
        project_id="research",
        accelerator_type="v5p-8",
        pods=1,
        execution=execution,
        idempotency_key="spot-default",
    )

    assert not replayed
    assert workload.spec.capacity_class is CapacityClass.SPOT
    assert workload.spec.required_labels["eyvan_pool"] == spot.id


@pytest.mark.asyncio
async def test_managed_callable_results_are_server_owned_unique_and_rotated_on_retry() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(
        repository,
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    operator = _principal(Role.PROJECT_OPERATOR)
    await service.upsert_capacity_pool(fleet_admin, _capacity_pool(), idempotency_key="managed-result-pool")
    requested_result = result_uri("caller-selected-shared-result")
    execution = CallableExecution(
        artifact_uri="eyvan://callables/" + "a" * 64,
        sha256="a" * 64,
        python="3.13",
        result_uri=requested_result,
    )

    first, _ = await service.submit_pool_callable(
        operator,
        fleet_name="research-fleet",
        org_id="acme",
        project_id="research",
        accelerator_type="v5p-8",
        pods=1,
        execution=execution,
        max_retries=1,
        idempotency_key="managed-result-first",
    )
    second, _ = await service.submit_pool_callable(
        operator,
        fleet_name="research-fleet",
        org_id="acme",
        project_id="research",
        accelerator_type="v5p-8",
        pods=1,
        execution=execution,
        max_retries=1,
        idempotency_key="managed-result-second",
    )
    first_execution = first.spec.execution
    second_execution = second.spec.execution
    assert isinstance(first_execution, CallableExecution)
    assert isinstance(second_execution, CallableExecution)
    assert first_execution.result_uri == result_uri(f"{first.id}-g1")
    assert second_execution.result_uri == result_uri(f"{second.id}-g1")
    assert first_execution.result_uri != second_execution.result_uri

    async with repository.transaction() as transaction:
        await transaction.put_workload(first.model_copy(update={"state": WorkloadState.FAILED}))
    retried, replayed = await service.retry_workload(
        operator,
        first.id,
        idempotency_key="retry-managed-result",
    )
    retried_execution = retried.spec.execution
    assert not replayed
    assert isinstance(retried_execution, CallableExecution)
    assert retried.generation == 2
    assert retried_execution.result_uri == result_uri(f"{first.id}-g2")


@pytest.mark.asyncio
async def test_pool_callable_submission_enforces_trust_and_requires_a_matching_atomic_pool() -> None:
    operator = _principal(Role.PROJECT_OPERATOR)
    execution = CallableExecution(artifact_uri="gs://trusted/train.pkl", sha256="a" * 64, python="3.12")

    with pytest.raises(AuthorizationError, match="not trusted"):
        await ControlPlaneService(InMemoryRepository()).submit_pool_callable(
            operator,
            fleet_name="missing",
            org_id="acme",
            project_id="research",
            accelerator_type="v5p-8",
            pods=1,
            execution=execution,
            idempotency_key="untrusted",
        )

    with pytest.raises(NoMatchingCapacityPoolError, match="no enabled capacity pool") as raised:
        await ControlPlaneService(
            InMemoryRepository(),
            callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
        ).submit_pool_callable(
            operator,
            fleet_name="missing",
            org_id="acme",
            project_id="research",
            accelerator_type="v5p-8",
            pods=2,
            execution=execution,
            idempotency_key="two-pods",
        )

    assert raised.value.code is ServiceErrorCode.NO_MATCHING_CAPACITY_POOL
    assert raised.value.fleet == "missing"
    assert raised.value.requested_accelerator_type == "v5p-8"
    assert raised.value.available_enabled_accelerator_types == ()


@pytest.mark.asyncio
async def test_callable_submission_rejects_caller_signing_inputs_on_every_service_entrypoint() -> None:
    service = ControlPlaneService(
        InMemoryRepository(),
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )
    operator = _principal(Role.PROJECT_OPERATOR)
    signed_execution = CallableExecution(
        artifact_uri="gs://trusted/train.pkl",
        sha256="a" * 64,
        python="3.12",
        signature="caller-signature",
    )

    with pytest.raises(InputError, match="remote callable signatures are not supported"):
        await service.submit_workload(
            operator,
            org_id="acme",
            project_id="research",
            spec=WorkloadDefinition(name="signed", execution=signed_execution),
            idempotency_key="signed-generic",
        )

    unsigned_execution = signed_execution.model_copy(update={"signature": None})
    with pytest.raises(InputError, match="remote callable signing keys are not supported"):
        await service.submit_pool_callable(
            operator,
            fleet_name="research-fleet",
            org_id="acme",
            project_id="research",
            accelerator_type="v5p-16",
            pods=1,
            execution=unsigned_execution,
            environment={"EYVAN_CALLABLE_SIGNING_KEY": "caller-secret"},
            idempotency_key="signed-pool",
        )


@pytest.mark.asyncio
async def test_missing_callable_pool_reports_only_enabled_accelerator_types() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(
        repository,
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    enabled = _capacity_pool(pool_id="v6e-enabled", node_type="v6e-enabled").model_copy(
        update={"accelerator_type": "v6e-8"}
    )
    disabled = _capacity_pool(
        pool_id="v5p-disabled",
        node_type="v5p-disabled",
        enabled=False,
    ).model_copy(update={"accelerator_type": "v5p-8"})
    await service.upsert_capacity_pool(fleet_admin, enabled, idempotency_key="enabled")
    await service.upsert_capacity_pool(fleet_admin, disabled, idempotency_key="disabled")

    with pytest.raises(NoMatchingCapacityPoolError) as raised:
        await service.submit_pool_callable(
            _principal(Role.PROJECT_OPERATOR),
            fleet_name="research-fleet",
            org_id="acme",
            project_id="research",
            accelerator_type="v5p-16",
            pods=1,
            execution=CallableExecution(
                artifact_uri="gs://trusted/train.pkl",
                sha256="a" * 64,
                python="3.12",
            ),
            idempotency_key="missing-v5p-16",
        )

    assert raised.value.available_enabled_accelerator_types == ("v6e-8",)


@pytest.mark.asyncio
async def test_missing_callable_pool_recommends_only_compatible_unambiguous_tpu_types() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(
        repository,
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    compatible = _capacity_pool(pool_id="v6e-compatible", node_type="v6e-compatible").model_copy(
        update={"accelerator_type": "v6e-8", "executor": "eyvan-native", "max_cells": 2}
    )
    unique_compatible = compatible.model_copy(
        update={"id": "v6e-64", "node_type": "v6e-64", "accelerator_type": "v6e-64"}
    )
    incompatible_duplicate = compatible.model_copy(
        update={"id": "v6e-8-too-small", "node_type": "v6e-8-too-small", "max_cells": 1}
    )
    insufficient_workers = compatible.model_copy(
        update={"id": "v5p-too-small", "node_type": "v5p-too-small", "accelerator_type": "v5p-8", "max_cells": 1}
    )
    partitioned = compatible.model_copy(
        update={
            "id": "v6e-partitioned",
            "node_type": "v6e-partitioned",
            "accelerator_type": "v6e-16",
            "max_replica_resources": ResourceVector(cpu=0, memory_gib=0, tpu=4),
        }
    )
    duplicate_one = compatible.model_copy(
        update={"id": "v6e-32-a", "node_type": "v6e-32-a", "accelerator_type": "v6e-32"}
    )
    duplicate_two = compatible.model_copy(
        update={"id": "v6e-32-b", "node_type": "v6e-32-b", "accelerator_type": "v6e-32"}
    )
    gpu = compatible.model_copy(
        update={
            "id": "gpu-pool",
            "node_type": "gpu-pool",
            "accelerator_type": "h100-8",
            "capacity_backend": "gcp-compute",
            "capacity": ResourceVector(cpu=32, gpu=8),
            "allocatable": ResourceVector(cpu=32, gpu=8),
        }
    )
    pools = (
        compatible,
        unique_compatible,
        incompatible_duplicate,
        insufficient_workers,
        partitioned,
        duplicate_one,
        duplicate_two,
        gpu,
    )
    for index, pool in enumerate(pools, start=1):
        await service.upsert_capacity_pool(fleet_admin, pool, idempotency_key=f"pool-{index}")

    with pytest.raises(NoMatchingCapacityPoolError) as raised:
        await service.submit_pool_callable(
            _principal(Role.PROJECT_OPERATOR),
            fleet_name="research-fleet",
            org_id="acme",
            project_id="research",
            accelerator_type="v5p-16",
            pods=2,
            execution=CallableExecution(
                artifact_uri="gs://trusted/train.pkl",
                sha256="a" * 64,
                python="3.12",
            ),
            idempotency_key="missing-v5p-16-two-pods",
        )

    assert raised.value.available_enabled_accelerator_types == ("v6e-64",)


@pytest.mark.asyncio
async def test_pool_callable_submission_requests_one_atomic_multi_worker_cell() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(
        repository,
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    operator = _principal(Role.PROJECT_OPERATOR)
    resources = ResourceVector(cpu=0, memory_gib=0, tpu=8)
    pool = _capacity_pool().model_copy(
        update={
            "accelerator_type": "v5p-16",
            "capacity": resources,
            "allocatable": resources,
            "provider_options": {},
            "executor": "eyvan-native",
        }
    )
    await service.upsert_capacity_pool(fleet_admin, pool, idempotency_key="v5p-16-pool")
    execution = CallableExecution(artifact_uri="gs://trusted/train.pkl", sha256="a" * 64, python="3.12")

    workload, replayed = await service.submit_pool_callable(
        operator,
        fleet_name="research-fleet",
        org_id="acme",
        project_id="research",
        accelerator_type="v5p-16",
        pods=2,
        execution=execution,
        idempotency_key="v5p-16-callable",
    )

    assert not replayed
    assert workload.spec.gang.replicas == 1
    assert workload.spec.gang.per_replica.tpu == 8
    assert workload.spec.cell_group_count == 1
    assert workload.spec.worker_count == 2
    assert workload.spec.worker_resource_scope is WorkerResourceScope.PER_WORKER
    assert workload.spec.resources_per_cell.tpu == 16
    assert workload.spec.required_labels[WORKER_COUNT_LABEL] == "2"
    with pytest.raises(ConflictError, match="capped at 4 workers"):
        await service.submit_pool_callable(
            operator,
            fleet_name="research-fleet",
            org_id="acme",
            project_id="research",
            accelerator_type="v5p-16",
            pods=5,
            execution=execution,
            idempotency_key="v5p-16-wrong-pods",
        )


@pytest.mark.asyncio
async def test_pool_callable_submission_enforces_the_gcp_multislice_worker_limit() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(
        repository,
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    pool = _capacity_pool(max_cells=1_000).model_copy(
        update={
            "accelerator_type": "v5p-16",
            "executor": "eyvan-native",
        }
    )
    await service.upsert_capacity_pool(fleet_admin, pool, idempotency_key="multislice-limit-pool")
    execution = CallableExecution(artifact_uri="gs://trusted/train.pkl", sha256="a" * 64, python="3.12")

    accepted, replayed = await service.submit_pool_callable(
        _principal(Role.PROJECT_OPERATOR),
        fleet_name=pool.fleet_name,
        org_id="acme",
        project_id="research",
        accelerator_type=pool.accelerator_type or "v5p-16",
        pods=256,
        execution=execution,
        idempotency_key="multislice-limit-accepted",
    )

    assert not replayed
    assert accepted.spec.worker_count == 256
    assert accepted.spec.gang.per_replica.tpu == pool.allocatable.tpu
    assert accepted.spec.resources_per_cell.tpu == pool.allocatable.tpu * 256
    with pytest.raises(InputError, match="must not exceed the GCP TPU multislice limit of 256"):
        await service.submit_pool_callable(
            _principal(Role.PROJECT_OPERATOR),
            fleet_name=pool.fleet_name,
            org_id="acme",
            project_id="research",
            accelerator_type=pool.accelerator_type or "v5p-16",
            pods=257,
            execution=execution,
            idempotency_key="multislice-limit-rejected",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "error", "match"),
    [
        ("missing", NotFoundError, "no enabled capacity pool"),
        ("disabled", NotFoundError, "no enabled capacity pool"),
        ("ambiguous", ConflictError, "ambiguous"),
        ("wrong-kind", ConflictError, "not a TPU pool"),
        ("max-cells", ConflictError, "max_workers set to zero"),
    ],
)
async def test_pool_callable_submission_rejects_invalid_tpu_pool_selection(
    case: str,
    error: type[Exception],
    match: str,
) -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(
        repository,
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    pools: list[CapacityPoolRecord] = []
    if case == "disabled":
        pools = [_capacity_pool(enabled=False)]
    elif case == "ambiguous":
        pools = [
            _capacity_pool(pool_id="v5p-a", node_type="v5p-a"),
            _capacity_pool(pool_id="v5p-b", node_type="v5p-b"),
        ]
    elif case == "wrong-kind":
        pools = [
            CapacityPoolRecord.model_validate(
                {
                    **_capacity_pool().model_dump(),
                    "capacity_backend": "gcp-compute",
                    "capacity": {"gpu": 1},
                    "allocatable": {"gpu": 1},
                    "labels": {"eyvan_resource_kind": "gpu"},
                }
            )
        ]
    elif case == "max-cells":
        pools = [_capacity_pool(max_cells=0)]
    for index, pool in enumerate(pools):
        await service.upsert_capacity_pool(fleet_admin, pool, idempotency_key=f"pool-{index}")

    with pytest.raises(error, match=match):
        await service.submit_pool_callable(
            _principal(Role.PROJECT_OPERATOR),
            fleet_name="research-fleet",
            org_id="acme",
            project_id="research",
            accelerator_type="v5p-8",
            pods=1,
            execution=CallableExecution(
                artifact_uri="gs://trusted/train.pkl",
                sha256="a" * 64,
                python="3.12",
            ),
            idempotency_key="callable",
        )


@pytest.mark.asyncio
async def test_cell_capacity_identity_is_immutable_until_deletion() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    cell = CellRecord(
        id="cell-a",
        provider="local",
        region="local-a",
        capacity=ResourceVector(cpu=8),
        allocatable=ResourceVector(cpu=8),
    )
    await service.upsert_cell(admin, cell, idempotency_key="cell-v1")

    with pytest.raises(
        ConflictError,
        match=r"^cell capacity identity cannot change before deletion: region, executor$",
    ):
        await service.upsert_cell(
            admin,
            cell.model_copy(update={"region": "local-b", "executor": "native-local"}),
            idempotency_key="cell-v2",
        )


@pytest.mark.asyncio
async def test_cell_capacity_cannot_shrink_below_active_leases() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    operator = _principal(Role.PROJECT_OPERATOR)
    cell = CellRecord(
        id="cell-a",
        provider="local",
        region="local",
        capacity=ResourceVector(cpu=8),
        allocatable=ResourceVector(cpu=8),
    )
    stored, _ = await service.upsert_cell(admin, cell, idempotency_key="cell-v1")
    async with repository.transaction() as transaction:
        await transaction.put_cell(stored.model_copy(update={"state": CellState.READY}))
    gang = ResourceGang(per_replica=ResourceVector(cpu=4, memory_gib=0))
    workload, _ = await service.submit_workload(
        operator,
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(name="train", execution=CommandExecution(command="python train.py"), gang=gang),
        idempotency_key="workload",
    )
    await service.create_lease(
        admin,
        LeaseRecord(
            id="lease-a",
            workload_id=workload.id,
            cell_id=cell.id,
            org_id=workload.org_id,
            project_id=workload.project_id,
            resources=gang,
            fencing_token="fence-a",
            expires_at=utcnow() + timedelta(minutes=5),
        ),
        idempotency_key="lease-a",
    )

    with pytest.raises(ConflictError, match="active lease"):
        await service.upsert_cell(
            admin,
            cell.model_copy(update={"capacity": ResourceVector(cpu=2), "allocatable": ResourceVector(cpu=2)}),
            idempotency_key="cell-v2",
        )


@pytest.mark.asyncio
async def test_cell_capacity_cannot_shrink_below_an_active_multislice_lease() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(
        repository,
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )
    admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    operator = _principal(Role.PROJECT_OPERATOR)
    pool = _capacity_pool().model_copy(
        update={
            "accelerator_type": "v5p-16",
            "capacity_backend": "gcp-tpu",
            "executor": "eyvan-native",
        }
    )
    await service.upsert_capacity_pool(admin, pool, idempotency_key="pool-v1")
    cell, _ = await service.upsert_cell(
        admin,
        pool.make_cell("cell-a", worker_count=2),
        idempotency_key="cell-v1",
    )
    async with repository.transaction() as transaction:
        await transaction.put_cell(cell.model_copy(update={"state": CellState.READY}))
    workload, _ = await service.submit_pool_callable(
        operator,
        fleet_name=pool.fleet_name,
        org_id="acme",
        project_id="research",
        accelerator_type="v5p-16",
        pods=2,
        execution=CallableExecution(artifact_uri="gs://trusted/train.pkl", sha256="a" * 64, python="3.12"),
        idempotency_key="workload-v1",
    )
    await service.create_lease(
        admin,
        LeaseRecord(
            id="lease-a",
            workload_id=workload.id,
            cell_id=cell.id,
            org_id=workload.org_id,
            project_id=workload.project_id,
            resources=workload.spec.gang,
            fencing_token="fence-a",
            expires_at=utcnow() + timedelta(minutes=5),
        ),
        idempotency_key="lease-a",
    )

    with pytest.raises(ConflictError, match="active lease"):
        await service.upsert_cell(
            admin,
            cell.model_copy(update={"capacity": ResourceVector(tpu=8), "allocatable": ResourceVector(tpu=8)}),
            idempotency_key="cell-v2",
        )


@pytest.mark.asyncio
async def test_lease_commit_clears_autoscale_request_ownership() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository)
    fleet_admin = _principal(Role.FLEET_ADMIN)
    project_operator = _principal(Role.PROJECT_OPERATOR)
    pool = _capacity_pool()
    await service.upsert_capacity_pool(fleet_admin, pool, idempotency_key="pool-v1")
    cell, _ = await service.upsert_cell(
        fleet_admin,
        pool.make_cell("autoscale-owned-cell", autoscale_request_workload_id="wl-pending"),
        idempotency_key="cell-v1",
    )
    async with repository.transaction() as transaction:
        await transaction.put_cell(cell.model_copy(update={"state": CellState.READY}))
    workload, _ = await service.submit_workload(
        project_operator,
        org_id="acme",
        project_id="research",
        spec=_spec(),
        idempotency_key="workload-v1",
    )
    async with repository.transaction() as transaction:
        stored = await transaction.get_cell(cell.id)
        assert stored is not None
        await transaction.put_cell(stored.model_copy(update={"autoscale_request_workload_id": workload.id}))
        await transaction.put_workload(
            workload.model_copy(
                update={
                    "autoscale_block_reason": AutoscaleBlockReason.SHARED_TPU_QUOTA,
                    "scheduling_reason": "previous scheduling reason",
                }
            )
        )

    await service.create_lease(
        fleet_admin,
        LeaseRecord(
            id="lease-owned-cell",
            workload_id=workload.id,
            cell_id=cell.id,
            org_id=workload.org_id,
            project_id=workload.project_id,
            resources=workload.spec.gang,
            fencing_token="lease-owned-cell",
            expires_at=utcnow() + timedelta(minutes=5),
        ),
        idempotency_key="lease-v1",
    )

    async with repository.transaction() as transaction:
        stored = await transaction.get_cell(cell.id)
        leased_workload = await transaction.get_workload(workload.id)
    assert stored is not None
    assert stored.autoscale_request_workload_id is None
    assert leased_workload is not None
    assert leased_workload.autoscale_block_reason is None
    assert leased_workload.scheduling_reason is None


def test_core_cell_preserves_autoscale_request_ownership() -> None:
    record = CellRecord(
        id="autoscale-owned-cell",
        provider="local",
        region="local",
        state=CellState.READY,
        capacity=ResourceVector(cpu=8),
        allocatable=ResourceVector(cpu=8),
        pool_id="local-pool",
        autoscale_request_workload_id="wl-owner",
    )

    cell = to_core_cell(record)

    assert cell.autoscale_request_workload_id == "wl-owner"


@pytest.mark.asyncio
async def test_lease_commit_revalidates_a_new_zero_quota() -> None:
    repository = InMemoryRepository()
    service = ControlPlaneService(repository, scheduler=CoreSchedulingService())
    fleet_admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    operator = _principal(Role.PROJECT_OPERATOR)
    async with repository.transaction() as transaction:
        await transaction.put_cell(
            CellRecord(
                id="cell-a",
                provider="local",
                region="local",
                state=CellState.READY,
                capacity=ResourceVector(cpu=8, memory_gib=8),
                allocatable=ResourceVector(cpu=8, memory_gib=8),
            )
        )
    workload, _ = await service.submit_workload(
        operator,
        org_id="acme",
        project_id="research",
        spec=_spec(),
        idempotency_key="workload",
    )
    decision = (await service.schedule_once(fleet_admin)).placements[0]
    await service.put_quota(
        _principal(Role.PROJECT_ADMIN),
        QuotaRecord(
            org_id="acme",
            project_id="research",
            limits={"cpu": 0},
            updated_by="admin",
        ),
        idempotency_key="quota",
    )
    hours = decision.lease.resource_hours_reserved.as_dict()
    lease = LeaseRecord(
        id=decision.lease.lease_id,
        workload_id=workload.id,
        cell_id=decision.lease.cell_id,
        cell_generation=1,
        org_id=workload.org_id,
        project_id=workload.project_id,
        resources=workload.spec.gang,
        hourly_cost=float(decision.lease.hourly_cost),
        reserved_cost=float(decision.lease.reserved_cost),
        resource_hours_reserved=ResourceHours(
            cpu=float(hours.get("cpu", 0)),
            memory_gib=float(hours.get("memory_gib", 0)),
            gpu=float(hours.get("gpu", 0)),
            tpu=float(hours.get("tpu", 0)),
        ),
        generation=workload.generation,
        fencing_token="fence-a",
        expires_at=utcnow() + timedelta(minutes=5),
    )

    with pytest.raises(ConflictError, match="quota"):
        await service.create_lease(fleet_admin, lease, idempotency_key="stale-placement")
