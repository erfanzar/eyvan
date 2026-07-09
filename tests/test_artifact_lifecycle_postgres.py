# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""PostgreSQL serialization proofs for artifact lifecycle races."""

from __future__ import annotations

import asyncio
import hashlib
import threading
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from eyvan.artifacts.managed import LocalManagedArtifactStore, callable_uri
from eyvan.records import (
    ArtifactReferenceRecord,
    CallableExecution,
    Principal,
    QuotaRecord,
    ResourceGang,
    ResourceVector,
    Role,
    RoleGrant,
    WorkloadDefinition,
    artifact_reference_id,
)
from eyvan.server.persistence.base import TransientRepositoryError
from eyvan.server.persistence.sqlalchemy import SqlAlchemyRepository, create_schema
from eyvan.server.service import ControlPlaneService
from eyvan.server.service.callable_trust import CallableProjectTrustPolicy
from eyvan.server.service.errors import ConflictError, InputError


class BlockingDeleteStore(LocalManagedArtifactStore):
    def __init__(self, root) -> None:
        super().__init__(root)
        self.delete_started = threading.Event()
        self.allow_delete = threading.Event()
        self.delete_calls = 0
        self.block_delete = False

    def delete_versioned(self, uri, *, org_id, project_id, object_version):
        self.delete_calls += 1
        self.delete_started.set()
        if self.block_delete and not self.allow_delete.wait(timeout=3):
            raise RuntimeError("delete barrier timed out")
        return super().delete_versioned(
            uri,
            org_id=org_id,
            project_id=project_id,
            object_version=object_version,
        )


@pytest.fixture
async def repository(postgres_url: str, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("EYVAN_DATABASE_URL", raising=False)
    engine = create_async_engine(postgres_url, poolclass=None)
    async with engine.begin() as connection:
        await connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        await connection.execute(text("CREATE SCHEMA public"))
    await create_schema(engine)
    repository = SqlAlchemyRepository(engine)
    await repository.open()
    try:
        yield repository
    finally:
        await engine.dispose()


def _principal() -> Principal:
    return Principal(
        subject="alice",
        grants=(RoleGrant(role=Role.PROJECT_ADMIN, org_id="acme", project_id="research"),),
    )


def _fleet_admin() -> Principal:
    return Principal(subject="root", grants=(RoleGrant(role=Role.FLEET_ADMIN),))


def _service(repository, store) -> ControlPlaneService:
    return ControlPlaneService(
        repository,
        artifact_store=store,
        callable_trust=CallableProjectTrustPolicy.from_projects(("acme/research",)),
    )


def _spec(uri: str) -> WorkloadDefinition:
    digest = uri.rsplit("/", 1)[-1]
    return WorkloadDefinition(
        name="postgres-artifact-race",
        execution=CallableExecution(kind="callable", artifact_uri=uri, sha256=digest, python="3.11"),
        gang=ResourceGang(replicas=1, per_replica=ResourceVector(cpu=1)),
    )


@pytest.mark.asyncio
async def test_concurrent_upload_reservations_cannot_both_exceed_one_quota(repository, tmp_path) -> None:
    service = _service(repository, LocalManagedArtifactStore(tmp_path / "objects"))
    await service.put_quota(
        _fleet_admin(),
        quota=QuotaRecord(
            org_id="acme",
            project_id="research",
            limits=ResourceVector(cpu=10),
            artifact_bytes_limit=10,
            updated_by="root",
        ),
        idempotency_key="postgres-artifact-quota",
    )
    payloads = (b"aaaaaa", b"bbbbbb")

    async def upload(payload: bytes):
        return await service.upload_managed_artifact(
            org_id="acme",
            project_id="research",
            uri=callable_uri(hashlib.sha256(payload).hexdigest()),
            payload=payload,
        )

    outcomes = await asyncio.gather(*(upload(payload) for payload in payloads), return_exceptions=True)
    assert sum(not isinstance(outcome, BaseException) for outcome in outcomes) == 1
    losers = [outcome for outcome in outcomes if isinstance(outcome, BaseException)]
    assert len(losers) == 1
    assert isinstance(losers[0], (InputError, TransientRepositoryError))
    usage = await service.artifact_usage(_principal(), org_id="acme", project_id="research")
    assert usage.stored_bytes == 6


@pytest.mark.asyncio
async def test_reference_commit_before_claim_never_allows_physical_delete(repository, tmp_path) -> None:
    store = BlockingDeleteStore(tmp_path / "objects")
    service = _service(repository, store)
    payload = b"reference-wins"
    uri = callable_uri(hashlib.sha256(payload).hexdigest())
    active = await service.upload_managed_artifact(org_id="acme", project_id="research", uri=uri, payload=payload)
    locked = asyncio.Event()
    release = asyncio.Event()

    async def commit_reference() -> None:
        async with repository.transaction() as transaction:
            record = await transaction.get_managed_artifact("acme", "research", uri)
            assert record is not None
            locked.set()
            await release.wait()
            await transaction.put_artifact_reference(
                ArtifactReferenceRecord(
                    id=artifact_reference_id("acme", "research", uri, "wl-reference-wins"),
                    uri=uri,
                    workload_id="wl-reference-wins",
                    org_id="acme",
                    project_id="research",
                )
            )

    reference_task = asyncio.create_task(commit_reference())
    await locked.wait()
    collection_task = asyncio.create_task(
        service.collect_artifacts_once(
            org_id="acme",
            project_id="research",
            grace_period_s=0,
            dry_run=False,
            now=(active.activated_at or active.updated_at) + timedelta(seconds=1),
        )
    )
    await asyncio.sleep(0.05)
    release.set()
    await reference_task
    try:
        first = await collection_task
    except TransientRepositoryError:
        first = None
    assert store.delete_calls == 0

    retry = await service.collect_artifacts_once(
        org_id="acme",
        project_id="research",
        grace_period_s=0,
        dry_run=False,
        now=(active.activated_at or active.updated_at) + timedelta(seconds=2),
    )
    assert retry.referenced == 1
    assert store.delete_calls == 0
    assert store.exists(uri, org_id="acme", project_id="research")
    if first is not None:
        assert first.referenced == 1


@pytest.mark.asyncio
async def test_claim_commit_rejects_reference_hold_and_reupload_until_delete_finishes(repository, tmp_path) -> None:
    store = BlockingDeleteStore(tmp_path / "objects")
    service = _service(repository, store)
    payload = b"claim-wins"
    uri = callable_uri(hashlib.sha256(payload).hexdigest())
    active = await service.upload_managed_artifact(org_id="acme", project_id="research", uri=uri, payload=payload)
    store.block_delete = True
    collection_task = asyncio.create_task(
        service.collect_artifacts_once(
            org_id="acme",
            project_id="research",
            grace_period_s=0,
            dry_run=False,
            now=(active.activated_at or active.updated_at) + timedelta(seconds=1),
        )
    )
    assert await asyncio.to_thread(store.delete_started.wait, 2)

    with pytest.raises(ConflictError, match="deleting"):
        await service.submit_workload(
            _principal(),
            org_id="acme",
            project_id="research",
            spec=_spec(uri),
            idempotency_key="postgres-claim-reference",
        )
    with pytest.raises(ConflictError, match="deleting"):
        await service.hold_artifact(_principal(), org_id="acme", project_id="research", uri=uri, reason="claim wins")
    with pytest.raises(ConflictError, match="deletion"):
        await service.upload_managed_artifact(org_id="acme", project_id="research", uri=uri, payload=payload)

    store.allow_delete.set()
    result = await collection_task
    assert result.collected_uris == (uri,)
