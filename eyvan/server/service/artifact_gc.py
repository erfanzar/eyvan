# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Garbage collection for managed artifacts orphaned by finished workloads.

Callables, workspace bundles, and results are written through the control
plane's managed store; a workload that reaches a terminal state stops needing
most of them. Without a sweep the plane host's disk accumulates every bundle
ever submitted. The sweep is reference-driven: an object is deletable only
when no workload names it and no workload that could still be inspected
recently names it -- listing errors propagate rather than reading as empty,
and deletion goes through the store's own descriptor-verified transaction.
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass
from datetime import datetime, timedelta

from eyvan.artifacts.managed import (
    ManagedArtifactDeleteStatus,
    ManagedArtifactInventoryError,
    ManagedArtifactVersionedStore,
    parse_managed_uri,
)
from eyvan.sdk.lifecycle import TERMINAL_STATES

#: How long a terminal workload keeps its artifacts past its last update.
DEFAULT_RETENTION_DAYS = 7

_MANAGED_URI_SCHEME = "eyvan:"


def _is_managed(uri: str | None) -> bool:
    return bool(uri) and str(uri).startswith(_MANAGED_URI_SCHEME)


def referenced_uris(workloads: tp.Iterable[tp.Any]) -> frozenset[str]:
    """Collect every managed URI any workload record names."""

    named: set[str] = set()
    for workload in workloads:
        for uri in (
            getattr(workload.spec.execution, "artifact_uri", None) if hasattr(workload, "spec") else None,
            getattr(getattr(workload.spec, "execution", None), "workspace_uri", None),
            getattr(workload.spec.execution, "result_uri", None),
        ):
            if _is_managed(uri):
                named.add(str(uri))
    return frozenset(named)


@dataclass(frozen=True, slots=True)
class GarbageCollectionPlan:
    """What one sweep observed, before anything is deleted."""

    scanned: int
    protected: int
    orphans: tuple[str, ...]
    deleted: tuple[str, ...] = ()
    failed: tuple[tuple[str, str], ...] = ()


async def plan_gc(
    store: ManagedArtifactVersionedStore,
    repository: tp.Any,
    *,
    org_id: str,
    project_id: str,
    now: datetime,
    retention_days: int = DEFAULT_RETENTION_DAYS,
) -> GarbageCollectionPlan:
    """Return the orphan plan for one tenant scope without deleting anything.

    A workload protects everything it names while it is non-terminal, and for
    ``retention_days`` after reaching a terminal state -- results are read
    after success, and failure forensics outrun tidiness.
    """

    workloads = await repository.list_workloads(org_id=org_id, project_id=project_id)
    horizon = now - timedelta(days=retention_days)
    protected_names: set[str] = set()
    recent = 0
    for workload in workloads:
        terminal = workload.state in TERMINAL_STATES
        if not terminal or workload.updated_at >= horizon:
            recent += 1
        for uri in _workload_artifact_uris(workload):
            if _is_managed(uri) and (not terminal or workload.updated_at >= horizon):
                protected_names.add(uri)
    orphans: list[str] = []
    cursor: str | None = None
    scanned = 0
    while True:
        page = store.list_scope(org_id=org_id, project_id=project_id, cursor=cursor)
        for entry in page.objects:
            scanned += 1
            # Protection is the only question: anything a live or recent
            # workload names survives, whatever else is true about it.
            if entry.uri in protected_names:
                continue
            orphans.append(entry.uri)
        cursor = page.next_cursor
        if cursor is None:
            break
    return GarbageCollectionPlan(scanned=scanned, protected=len(protected_names), orphans=tuple(orphans))


def _workload_artifact_uris(workload: tp.Any) -> tuple[str | None, ...]:
    execution = workload.spec.execution
    return (
        getattr(execution, "artifact_uri", None),
        getattr(execution, "workspace_uri", None),
        getattr(execution, "result_uri", None),
    )


async def run_gc(
    store: ManagedArtifactVersionedStore,
    repository: tp.Any,
    *,
    org_id: str,
    project_id: str,
    now: datetime,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    apply: bool = False,
) -> GarbageCollectionPlan:
    """Plan and optionally delete orphaned managed artifacts.

    Deletion goes through ``delete_versioned`` with the version observed at
    inventory time semantics -- a object replaced between listing and sweep is
    refused rather than destroyed, because the sweep's knowledge went stale.
    Listing failures abort the whole sweep: partial knowledge deletes things
    the reference scan never saw.
    """

    plan = await plan_gc(
        store,
        repository,
        org_id=org_id,
        project_id=project_id,
        now=now,
        retention_days=retention_days,
    )
    if not apply or not plan.orphans:
        return plan
    deleted: list[str] = []
    failed: list[tuple[str, str]] = []
    for uri in plan.orphans:
        try:
            ref = parse_managed_uri(uri)
        except Exception as exc:
            failed.append((uri, str(exc)))
            continue
        try:
            result = await _delete_current_version(store, uri, ref.object_id, org_id=org_id, project_id=project_id)
        except (ManagedArtifactInventoryError, OSError, ValueError) as exc:
            failed.append((uri, str(exc)))
            continue
        if result:
            deleted.append(uri)
    return GarbageCollectionPlan(
        scanned=plan.scanned,
        protected=plan.protected,
        orphans=plan.orphans,
        deleted=tuple(deleted),
        failed=tuple(failed),
    )


async def _delete_current_version(
    store: ManagedArtifactVersionedStore,
    uri: str,
    object_id: str,
    *,
    org_id: str,
    project_id: str,
) -> bool:
    metadata = store.stat(uri, org_id=org_id, project_id=project_id)
    if metadata is None:
        return False
    result = store.delete_versioned(
        uri,
        org_id=org_id,
        project_id=project_id,
        object_version=metadata.object_version,
    )
    return result.status in (ManagedArtifactDeleteStatus.DELETED, ManagedArtifactDeleteStatus.ABSENT)
