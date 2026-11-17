# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The hard per-user TPU cap holds under concurrent submissions, on every backend.

Six submissions of 4 TPU units race against a cap of 8, spread over two
projects. Whatever interleaving the backend produces, at most two may be
admitted. On PostgreSQL a loser may also be aborted with a retryable
serialization error; it is retried, as a client would, and must then be refused
by the cap -- never admitted. The persisted rows are the evidence.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from eyvan.records import (
    CommandExecution,
    Principal,
    ResourceGang,
    ResourceVector,
    Role,
    RoleGrant,
    UserRecord,
    WorkloadDefinition,
)
from eyvan.server.persistence import InMemoryRepository
from eyvan.server.persistence.base import TransientRepositoryError
from eyvan.server.persistence.local import LocalJsonRepository
from eyvan.server.persistence.sqlalchemy import SqlAlchemyRepository, create_schema
from eyvan.server.service import ControlPlaneService
from eyvan.server.service.errors import ConflictError
from eyvan.server.service.scheduling import CoreSchedulingService

ADMIN = Principal(subject="root", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
CAP = 8
EACH = 4
SUBMISSIONS = 6


def _alice(project: str) -> Principal:
    return Principal(subject="alice", grants=(RoleGrant(role=Role.PROJECT_OPERATOR, org_id="acme", project_id=project),))


def _job(index: int) -> WorkloadDefinition:
    return WorkloadDefinition(
        name=f"job-{index}",
        execution=CommandExecution(command="python train.py"),
        gang=ResourceGang(replicas=1, per_replica=ResourceVector(cpu=1, memory_gib=1, tpu=EACH)),
    )


async def _race(repository) -> None:
    service = ControlPlaneService(repository, scheduler=CoreSchedulingService(), governance_enabled=True)
    async with repository.transaction() as tx:
        await tx.put_user(UserRecord(subject="alice", email="alice@example.com", created_by="root"))
    await service.set_user_rules(
        ADMIN, "alice", allowed_fleets=None, approval_tpu_threshold=None, guaranteed_tpu_units=None, max_tpu_units=CAP
    )

    async def submit(index: int) -> str:
        project = "research" if index % 2 == 0 else "other"
        for _ in range(50):
            try:
                await service.submit_workload(
                    _alice(project), org_id="acme", project_id=project, spec=_job(index), idempotency_key=f"k{index}"
                )
                return "admitted"
            except ConflictError as exc:
                assert "TPU quota" in str(exc), exc
                return "refused"
            except TransientRepositoryError:
                await asyncio.sleep(0.01)
        raise AssertionError("submission never settled")

    outcomes = await asyncio.gather(*(submit(index) for index in range(SUBMISSIONS)))

    async with repository.transaction() as tx:
        stored = [item for item in await tx.list_workloads() if item.created_by == "alice"]
    used = sum(item.spec.gang.per_replica.tpu for item in stored)
    assert used <= CAP, f"{used} TPU units admitted against a cap of {CAP}"
    assert outcomes.count("admitted") == len(stored) == CAP // EACH
    assert outcomes.count("refused") == SUBMISSIONS - CAP // EACH
    assert {item.project_id for item in stored} <= {"research", "other"}


@pytest.mark.asyncio
async def test_concurrent_submissions_never_exceed_the_cap_in_memory() -> None:
    repository = InMemoryRepository()
    await repository.open()
    await _race(repository)


@pytest.mark.asyncio
async def test_concurrent_submissions_never_exceed_the_cap_in_local_json(tmp_path) -> None:
    repository = LocalJsonRepository(tmp_path / "control-plane.json")
    await repository.open()
    try:
        await _race(repository)
    finally:
        await repository.close()


@pytest.mark.asyncio
async def test_concurrent_submissions_never_exceed_the_cap_on_postgresql(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url, poolclass=None)
    async with engine.begin() as connection:
        await connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        await connection.execute(text("CREATE SCHEMA public"))
    try:
        await create_schema(engine)
        repository = SqlAlchemyRepository(engine)
        await repository.open()
        try:
            await _race(repository)
        finally:
            await repository.close()
    finally:
        await engine.dispose()
