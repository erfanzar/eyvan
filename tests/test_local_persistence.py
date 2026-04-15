"""Restart and ownership coverage for Eyvan's durable local control plane."""

from __future__ import annotations

import stat
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from eyvan._json import JsonObject, model_json
from eyvan.api.app import create_app
from eyvan.api.server_bind import ServerBindPolicy
from eyvan.config import EyvanSettings, RepositoryBackend
from eyvan.managed_artifacts import ArtifactAction, result_uri
from eyvan.persistence.local import LocalControlPlaneInUseError, LocalJsonRepository
from eyvan.service import ControlPlaneService
from eyvan.service.models import (
    CommandExecution,
    OutboxRecord,
    OutboxState,
    Principal,
    Role,
    RoleGrant,
    WorkloadDefinition,
    WorkloadRecord,
)


def _principal() -> Principal:
    return Principal(subject="alice", grants=(RoleGrant(role=Role.FLEET_ADMIN),))


def _workload_spec() -> WorkloadDefinition:
    return WorkloadDefinition(name="train", execution=CommandExecution(command="python train.py"))


def _workload_request() -> JsonObject:
    return {
        "org_id": "acme",
        "project_id": "research",
        "spec": model_json(_workload_spec()),
    }


def _headers() -> dict[str, str]:
    return {
        "X-Eyvan-User": "alice",
        "X-Eyvan-Org": "acme",
        "X-Eyvan-Project": "research",
        "X-Eyvan-Roles": "operator",
    }


def _settings(tmp_path: Path) -> EyvanSettings:
    profile = tmp_path / "profile"
    return EyvanSettings(
        repository_backend=RepositoryBackend.LOCAL,
        local_state_path=profile / "control-plane.json",
        local_artifact_token_key_path=profile / "artifact-token.key",
        artifact_store_uri=str(profile / "managed-artifacts"),
        controller_enabled=False,
        dev_auth_enabled=True,
    )


@pytest.mark.asyncio
async def test_local_repository_read_only_transaction_does_not_create_or_rewrite_state(tmp_path: Path) -> None:
    path = tmp_path / "control-plane.json"
    repository = LocalJsonRepository(path)
    try:
        async with repository.transaction() as transaction:
            assert await transaction.list_workloads() == []
        assert not path.exists()

        async with repository.transaction() as transaction:
            await transaction.put_workload(
                WorkloadRecord(
                    id="wl-1",
                    org_id="acme",
                    project_id="research",
                    spec=_workload_spec(),
                    created_by="alice",
                )
            )
        original = path.read_bytes()

        async with repository.transaction() as transaction:
            assert (await transaction.get_workload("wl-1")) is not None

        assert path.read_bytes() == original
    finally:
        await repository.close()


@pytest.mark.asyncio
async def test_local_repository_persists_workload_and_idempotency_across_restart(tmp_path: Path) -> None:
    path = tmp_path / "control-plane.json"
    first_repository = LocalJsonRepository(path)
    first_service = ControlPlaneService(first_repository)

    try:
        created, replayed = await first_service.submit_workload(
            _principal(),
            org_id="acme",
            project_id="research",
            spec=_workload_spec(),
            idempotency_key="submit-1",
        )
        assert replayed is False
    finally:
        await first_repository.close()

    second_repository = LocalJsonRepository(path)
    second_service = ControlPlaneService(second_repository)
    try:
        replay, replayed = await second_service.submit_workload(
            _principal(),
            org_id="acme",
            project_id="research",
            spec=_workload_spec(),
            idempotency_key="submit-1",
        )
        restored = await second_service.get_workload(_principal(), created.id)
        events = await second_repository.wait_for_events(after_sequence=0)
    finally:
        await second_repository.close()

    assert replayed is True
    assert replay.id == created.id
    assert restored == created
    assert [(event.sequence, event.action) for event in events] == [(1, "workload.submitted")]


@pytest.mark.asyncio
async def test_local_repository_reclaims_processing_outbox_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "control-plane.json"
    first_repository = LocalJsonRepository(path)
    try:
        await first_repository.open()
        async with first_repository.transaction() as transaction:
            await transaction.put_outbox(
                OutboxRecord(
                    id="command-1",
                    topic="capacity.apply",
                    aggregate_type="capacity_pool",
                    aggregate_id="pool-1",
                    idempotency_key="apply-1",
                    generation=1,
                )
            )
        claimed = await first_repository.claim_outbox(worker_id="first-server", lease_seconds=3600)
        assert [record.id for record in claimed] == ["command-1"]
        assert claimed[0].state is OutboxState.PROCESSING
    finally:
        await first_repository.close()

    second_repository = LocalJsonRepository(path)
    try:
        await second_repository.open()
        reclaimed = await second_repository.claim_outbox(worker_id="second-server", lease_seconds=3600)
    finally:
        await second_repository.close()

    assert len(reclaimed) == 1
    assert reclaimed[0].id == "command-1"
    assert reclaimed[0].attempts == 2
    assert reclaimed[0].claimed_by == "second-server"


@pytest.mark.asyncio
async def test_local_repository_rejects_a_second_process_owner(tmp_path: Path) -> None:
    path = tmp_path / "control-plane.json"
    owner = LocalJsonRepository(path)
    contender = LocalJsonRepository(path)
    await owner.open()
    try:
        with pytest.raises(LocalControlPlaneInUseError, match="already owned"):
            await contender.open()
    finally:
        await contender.close()
        await owner.close()


def test_local_app_reuses_state_and_artifact_key_after_restart(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    bind = ServerBindPolicy.from_host("127.0.0.1")
    first_app = create_app(settings=settings, server_bind=bind)

    with TestClient(first_app) as first_client:
        submitted = first_client.post(
            "/api/v1/workloads",
            headers={**_headers(), "Idempotency-Key": "submit-1"},
            json=_workload_request(),
        )
        assert submitted.status_code == 201
        submitted_workload = WorkloadRecord.model_validate(submitted.json())
        workload_id = submitted_workload.id
        token = first_app.state.artifact_access.signer.issue(
            ArtifactAction.WRITE,
            result_uri("restart-result"),
            org_id="acme",
            project_id="research",
            workload_id=workload_id,
            expires=False,
        )

    second_app = create_app(settings=settings, server_bind=bind)
    with TestClient(second_app) as second_client:
        restored = second_client.get(f"/api/v1/workloads/{workload_id}", headers=_headers())
        restored_workload = WorkloadRecord.model_validate(restored.json())
        claims = second_app.state.artifact_access.signer.verify(
            token,
            ArtifactAction.WRITE,
            result_uri("restart-result"),
            org_id="acme",
            project_id="research",
            workload_id=workload_id,
        )

    assert restored.status_code == 200
    assert restored_workload.id == workload_id
    assert claims.workload_id == workload_id
    assert stat.S_IMODE(settings.local_state_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(settings.local_artifact_token_key_path.stat().st_mode) == 0o600
