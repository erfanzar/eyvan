# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The managed-artifact janitor: orphans die, referenced bytes survive.

Every case here is a state transition someone will actually hit: a running
workload's artifacts must be untouchable, a yesterday's success must still be
readable, a week-old failure must clean up, and an inventory error must abort
rather than delete blindly.
"""

from datetime import datetime, timedelta, timezone

import pytest

from eyvan.artifacts.managed import LocalManagedArtifactStore
from eyvan.records import CallableExecution, WorkloadDefinition, WorkloadRecord, WorkloadState
from eyvan.server.service.artifact_gc import plan_gc, run_gc

ORG = "acme"
PROJECT = "research"
NOW = datetime(2026, 8, 23, 12, 0, tzinfo=timezone.utc)


def _workload(workload_id: str, *, state: WorkloadState, artifact_uri: str, updated: datetime) -> WorkloadRecord:
    return WorkloadRecord(
        id=workload_id,
        org_id=ORG,
        project_id=PROJECT,
        spec=WorkloadDefinition(
            name=workload_id,
            execution=CallableExecution(
                artifact_uri=artifact_uri,
                sha256=artifact_uri.rsplit("/", 1)[-1],
                python="3.13",
            ),
        ),
        state=state,
        created_by="alice",
        updated_at=updated,
    )


class _Repository:
    def __init__(self, *workloads: WorkloadRecord) -> None:
        self.workloads = list(workloads)

    async def list_workloads(self, **_: object) -> list[WorkloadRecord]:
        return list(self.workloads)


import hashlib


def _uri(name: str, payload: bytes | None = None) -> str:
    return f"eyvan://callables/{hashlib.sha256(_payload(name) if payload is None else payload).hexdigest()}"


def _payload(name: str) -> bytes:
    return f"payload-{name}".encode()


@pytest.fixture()
def store(tmp_path):
    return LocalManagedArtifactStore(tmp_path / "managed")


async def test_a_live_workload_protects_every_artifact_it_names(store) -> None:
    store.put_versioned(_uri("live"), _payload("live"), org_id=ORG, project_id=PROJECT)
    repo = _Repository(
        _workload("wl_live", state=WorkloadState.RUNNING, artifact_uri=_uri("live", _payload("live")), updated=NOW - timedelta(days=30))
    )

    plan = await plan_gc(store, repo, org_id=ORG, project_id=PROJECT, now=NOW)

    assert plan.orphans == ()
    assert plan.protected == 1


async def test_a_recent_success_keeps_its_result_readable(store) -> None:
    store.put_versioned(_uri("recent"), _payload("recent"), org_id=ORG, project_id=PROJECT)
    repo = _Repository(
        _workload(
            "wl_done", state=WorkloadState.SUCCEEDED, artifact_uri=_uri("recent", _payload("recent")), updated=NOW - timedelta(days=2)
        )
    )

    plan = await plan_gc(store, repo, org_id=ORG, project_id=PROJECT, now=NOW)

    assert plan.orphans == ()


async def test_an_expired_failure_becomes_collectable(store) -> None:
    store.put_versioned(_uri("old"), _payload("old"), org_id=ORG, project_id=PROJECT)
    repo = _Repository(
        _workload("wl_old", state=WorkloadState.FAILED, artifact_uri=_uri("old", _payload("old")), updated=NOW - timedelta(days=9))
    )

    plan = await run_gc(store, repo, org_id=ORG, project_id=PROJECT, now=NOW)

    assert plan.orphans == (_uri("old"),)
    assert plan.deleted == ()
    # Dry-run by default: the object survives until apply is requested.
    assert store.stat(_uri("old"), org_id=ORG, project_id=PROJECT) is not None


async def test_apply_deletes_orphans_and_never_the_referenced(store) -> None:
    store.put_versioned(_uri("old"), _payload("old"), org_id=ORG, project_id=PROJECT)
    store.put_versioned(_uri("kept"), _payload("kept"), org_id=ORG, project_id=PROJECT)
    repo = _Repository(
        _workload("wl_old", state=WorkloadState.CANCELLED, artifact_uri=_uri("old", _payload("old")), updated=NOW - timedelta(days=9)),
        _workload("wl_new", state=WorkloadState.RUNNING, artifact_uri=_uri("kept", _payload("kept")), updated=NOW),
    )

    report = await run_gc(store, repo, org_id=ORG, project_id=PROJECT, now=NOW, apply=True)

    assert report.deleted == (_uri("old"),)
    assert store.stat(_uri("old"), org_id=ORG, project_id=PROJECT) is None
    assert store.stat(_uri("kept"), org_id=ORG, project_id=PROJECT) is not None


async def test_workspace_and_result_uris_are_protected_too(store) -> None:
    for name in ("ws", "result"):
        store.put_versioned(_uri(name), _payload(name), org_id=ORG, project_id=PROJECT)
    record = _workload(
        "wl_ws", state=WorkloadState.SUCCEEDED, artifact_uri=_uri("ws", _payload("ws")), updated=NOW - timedelta(days=2)
    )
    record = record.model_copy(deep=True)
    spec = record.spec
    execution = spec.execution
    updated_execution = execution.__class__(
        artifact_uri=execution.artifact_uri,
        sha256=execution.sha256,
        python=execution.python,
        workspace_uri=_uri("ws", _payload("ws")),
        workspace_sha256=hashlib.sha256(_payload("ws")).hexdigest(),
        result_uri=_uri("result", _payload("result")),
    )
    object.__setattr__(
        record,
        "spec",
        spec.__class__(**{**spec.model_dump(exclude={"execution"}), "execution": updated_execution}),
    )
    repo = _Repository(record)

    plan = await plan_gc(store, repo, org_id=ORG, project_id=PROJECT, now=NOW)

    assert plan.orphans == ()
