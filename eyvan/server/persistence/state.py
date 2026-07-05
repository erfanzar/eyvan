# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Shared typed state and transactional behavior for local repositories."""

from __future__ import annotations

import asyncio
import copy
import typing as tp
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from eyvan.records import (
    ArtifactReferenceRecord,
    AuditEvent,
    BudgetRecord,
    CapacityPoolRecord,
    CellRecord,
    IdempotencyRecord,
    LeaseRecord,
    ManagedArtifactRecord,
    OrganizationRecord,
    OutboxRecord,
    OutboxState,
    PolicyRecord,
    ProjectRecord,
    QuotaRecord,
    RoleGrantRecord,
    UserProgressReport,
    WorkloadRecord,
    WorkloadState,
    utcnow,
)
from eyvan.server.persistence.base import RepositorySession
from eyvan.server.service.federation import BudgetGrantRecord, PlanePeerRecord, QuotaGrantRecord

#: Outbox states safe to delete. Both are absorbing, so removing a row cannot
#: drop a command that is still owed delivery.
_PRUNABLE_OUTBOX_STATES = frozenset({OutboxState.SUCCEEDED, OutboxState.DISCARDED})


@dataclass(slots=True)
class ControlPlaneState:
    """Complete typed state held by one single-process repository."""

    workloads: dict[str, WorkloadRecord] = field(default_factory=dict)
    cells: dict[str, CellRecord] = field(default_factory=dict)
    capacity_pools: dict[str, CapacityPoolRecord] = field(default_factory=dict)
    leases: dict[str, LeaseRecord] = field(default_factory=dict)
    policies: dict[tuple[str, str], PolicyRecord] = field(default_factory=dict)
    quotas: dict[tuple[str, str, str], QuotaRecord] = field(default_factory=dict)
    budgets: dict[tuple[str, str], BudgetRecord] = field(default_factory=dict)
    #: Keyed by (subject, role, org, project) so re-issuing one overwrites it.
    role_grants: dict[tuple[str, str, str, str], RoleGrantRecord] = field(default_factory=dict)
    organizations: dict[str, OrganizationRecord] = field(default_factory=dict)
    #: Keyed by (org, project): project names are unique only within a tenant,
    #: and two organizations both having a "research" project is the ordinary case.
    projects: dict[tuple[str, str], ProjectRecord] = field(default_factory=dict)
    #: workload id -> rank -> latest report. Keyed by rank so a new report for a
    #: rank replaces the old one rather than accumulating.
    progress_reports: dict[str, dict[int, UserProgressReport]] = field(default_factory=dict)
    plane_peers: dict[str, PlanePeerRecord] = field(default_factory=dict)
    budget_grants: dict[str, BudgetGrantRecord] = field(default_factory=dict)
    quota_grants: dict[str, QuotaGrantRecord] = field(default_factory=dict)
    managed_artifacts: dict[str, ManagedArtifactRecord] = field(default_factory=dict)
    artifact_references: dict[str, ArtifactReferenceRecord] = field(default_factory=dict)
    idempotency: dict[tuple[str, str, str], IdempotencyRecord] = field(default_factory=dict)
    outbox: dict[str, OutboxRecord] = field(default_factory=dict)
    events: list[AuditEvent] = field(default_factory=list)
    next_event_sequence: int = 1
    revision: int = 0


class StateSession(RepositorySession):
    """Transaction-local operations over a mutable :class:`ControlPlaneState`."""

    def __init__(self, state: ControlPlaneState):
        self._state = state

    async def get_workload(self, workload_id: str) -> WorkloadRecord | None:
        return self._state.workloads.get(workload_id)

    async def list_workloads(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        state: WorkloadState | None = None,
    ) -> list[WorkloadRecord]:
        items = (
            item
            for item in self._state.workloads.values()
            if (org_id is None or item.org_id == org_id)
            and (project_id is None or item.project_id == project_id)
            and (state is None or item.state == state)
        )
        return sorted(items, key=lambda item: (item.created_at, item.id))

    async def put_workload(self, workload: WorkloadRecord) -> None:
        self._state.workloads[workload.id] = workload

    async def get_cell(self, cell_id: str) -> CellRecord | None:
        return self._state.cells.get(cell_id)

    async def list_cells(self) -> list[CellRecord]:
        return sorted(self._state.cells.values(), key=lambda item: item.id)

    async def list_child_workloads(self, parent_workload_id: str) -> list[WorkloadRecord]:
        """Return the direct children of one workload in stable identifier order."""

        return sorted(
            (
                workload
                for workload in self._state.workloads.values()
                if workload.parent_workload_id == parent_workload_id
            ),
            key=lambda workload: workload.id,
        )

    async def list_cells_for_autoscale_request(self, workload_id: str) -> list[CellRecord]:
        """Return cells created by one autoscale request in stable identifier order."""

        return sorted(
            (cell for cell in self._state.cells.values() if cell.autoscale_request_workload_id == workload_id),
            key=lambda cell: cell.id,
        )

    async def put_cell(self, cell: CellRecord) -> None:
        self._state.cells[cell.id] = cell

    async def get_capacity_pool(self, pool_id: str) -> CapacityPoolRecord | None:
        return self._state.capacity_pools.get(pool_id)

    async def list_capacity_pools(self, *, fleet_name: str | None = None) -> list[CapacityPoolRecord]:
        items = (
            pool for pool in self._state.capacity_pools.values() if fleet_name is None or pool.fleet_name == fleet_name
        )
        return sorted(items, key=lambda item: (item.fleet_name, item.node_type, item.id))

    async def put_capacity_pool(self, pool: CapacityPoolRecord) -> None:
        self._state.capacity_pools[pool.id] = pool

    async def get_lease(self, lease_id: str) -> LeaseRecord | None:
        return self._state.leases.get(lease_id)

    async def list_leases(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        workload_id: str | None = None,
    ) -> list[LeaseRecord]:
        items = (
            item
            for item in self._state.leases.values()
            if (org_id is None or item.org_id == org_id)
            and (project_id is None or item.project_id == project_id)
            and (workload_id is None or item.workload_id == workload_id)
        )
        return sorted(items, key=lambda item: (item.created_at, item.id))

    async def put_lease(self, lease: LeaseRecord) -> None:
        self._state.leases[lease.id] = lease

    async def get_policy(self, org_id: str, project_id: str) -> PolicyRecord | None:
        return self._state.policies.get((org_id, project_id))

    async def list_policies(self, *, org_id: str | None = None) -> list[PolicyRecord]:
        items = (item for item in self._state.policies.values() if org_id is None or item.org_id == org_id)
        return sorted(items, key=lambda item: (item.org_id, item.project_id))

    async def put_policy(self, policy: PolicyRecord) -> None:
        self._state.policies[(policy.org_id, policy.project_id)] = policy

    async def get_quota(self, org_id: str, project_id: str | None, principal_id: str | None) -> QuotaRecord | None:
        return self._state.quotas.get((org_id, project_id or "", principal_id or ""))

    async def list_quotas(
        self, *, org_id: str | None = None, project_id: str | None = None, principal_id: str | None = None
    ) -> list[QuotaRecord]:
        items = (
            item
            for item in self._state.quotas.values()
            if (org_id is None or item.org_id == org_id)
            and (project_id is None or item.project_id == project_id)
            and (principal_id is None or item.principal_id == principal_id)
        )
        return sorted(items, key=lambda item: (item.org_id, item.project_id or "", item.principal_id or ""))

    async def put_quota(self, quota: QuotaRecord) -> None:
        self._state.quotas[(quota.org_id, quota.project_id or "", quota.principal_id or "")] = quota

    async def delete_quota(self, org_id: str, project_id: str | None, principal_id: str | None) -> bool:
        return self._state.quotas.pop((org_id, project_id or "", principal_id or ""), None) is not None

    async def list_role_grants(self, *, subject: str | None = None) -> list[RoleGrantRecord]:
        items = (item for item in self._state.role_grants.values() if subject is None or item.subject == subject)
        return sorted(items, key=lambda item: item.key)

    async def put_role_grant(self, grant: RoleGrantRecord) -> None:
        self._state.role_grants[grant.key] = grant

    async def delete_role_grant(self, key: tuple[str, str, str, str]) -> bool:
        return self._state.role_grants.pop(key, None) is not None

    async def get_organization(self, org_id: str) -> OrganizationRecord | None:
        return self._state.organizations.get(org_id)

    async def list_organizations(self) -> list[OrganizationRecord]:
        return sorted(self._state.organizations.values(), key=lambda item: item.id)

    async def put_organization(self, organization: OrganizationRecord) -> None:
        self._state.organizations[organization.id] = organization

    async def get_project(self, org_id: str, project_id: str) -> ProjectRecord | None:
        return self._state.projects.get((org_id, project_id))

    async def list_projects(self, *, org_id: str | None = None) -> list[ProjectRecord]:
        items = (item for item in self._state.projects.values() if org_id is None or item.org_id == org_id)
        return sorted(items, key=lambda item: (item.org_id, item.id))

    async def put_project(self, project: ProjectRecord) -> None:
        self._state.projects[project.key] = project

    async def get_budget(self, org_id: str, project_id: str) -> BudgetRecord | None:
        return self._state.budgets.get((org_id, project_id))

    async def list_budgets(self, *, org_id: str | None = None) -> list[BudgetRecord]:
        items = (item for item in self._state.budgets.values() if org_id is None or item.org_id == org_id)
        return sorted(items, key=lambda item: (item.org_id, item.project_id))

    async def put_budget(self, budget: BudgetRecord) -> None:
        self._state.budgets[(budget.org_id, budget.project_id)] = budget

    async def put_progress_report(self, workload_id: str, report: UserProgressReport) -> None:
        self._state.progress_reports.setdefault(workload_id, {})[report.rank] = report

    async def list_progress_reports(self, workload_id: str) -> list[UserProgressReport]:
        reports = self._state.progress_reports.get(workload_id, {})
        return [reports[rank] for rank in sorted(reports)]

    async def get_plane_peer(self, plane_id: str) -> PlanePeerRecord | None:
        return self._state.plane_peers.get(plane_id)

    async def put_plane_peer(self, peer: PlanePeerRecord) -> None:
        self._state.plane_peers[peer.plane_id] = peer

    async def list_plane_peers(self) -> list[PlanePeerRecord]:
        return sorted(self._state.plane_peers.values(), key=lambda peer: peer.plane_id)

    async def get_budget_grant(self, grant_id: str) -> BudgetGrantRecord | None:
        return self._state.budget_grants.get(grant_id)

    async def put_budget_grant(self, grant: BudgetGrantRecord) -> None:
        self._state.budget_grants[grant.id] = grant

    async def list_budget_grants(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        grantee_plane_id: str | None = None,
    ) -> list[BudgetGrantRecord]:
        items = [
            item
            for item in self._state.budget_grants.values()
            if (org_id is None or item.org_id == org_id)
            and (project_id is None or item.project_id == project_id)
            and (grantee_plane_id is None or item.grantee_plane_id == grantee_plane_id)
        ]
        return sorted(items, key=lambda item: (item.issued_at, item.id))

    async def get_quota_grant(self, grant_id: str) -> QuotaGrantRecord | None:
        return self._state.quota_grants.get(grant_id)

    async def put_quota_grant(self, grant: QuotaGrantRecord) -> None:
        self._state.quota_grants[grant.id] = grant

    async def list_quota_grants(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        grantee_plane_id: str | None = None,
    ) -> list[QuotaGrantRecord]:
        items = [
            item
            for item in self._state.quota_grants.values()
            if (org_id is None or item.org_id == org_id)
            and (project_id is None or item.project_id == project_id)
            and (grantee_plane_id is None or item.grantee_plane_id == grantee_plane_id)
        ]
        return sorted(items, key=lambda item: (item.issued_at, item.id))

    async def get_managed_artifact(self, uri: str) -> ManagedArtifactRecord | None:
        return self._state.managed_artifacts.get(uri)

    async def put_managed_artifact(self, record: ManagedArtifactRecord) -> None:
        self._state.managed_artifacts[record.uri] = record

    async def list_managed_artifacts(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        kind: str | None = None,
        limit: int = 1000,
    ) -> list[ManagedArtifactRecord]:
        items = [
            item
            for item in self._state.managed_artifacts.values()
            if (org_id is None or item.org_id == org_id)
            and (project_id is None or item.project_id == project_id)
            and (kind is None or item.kind == kind)
        ]
        items.sort(key=lambda item: (item.created_at, item.uri))
        return items[:limit]

    async def sum_managed_artifact_bytes(self, *, org_id: str, project_id: str | None = None) -> int:
        return sum(
            item.size_bytes
            for item in self._state.managed_artifacts.values()
            if item.org_id == org_id and (project_id is None or item.project_id == project_id)
        )

    async def put_artifact_reference(self, record: ArtifactReferenceRecord) -> None:
        self._state.artifact_references[record.id] = record

    async def list_artifact_references(
        self,
        *,
        uri: str | None = None,
        workload_id: str | None = None,
        active_only: bool = False,
    ) -> list[ArtifactReferenceRecord]:
        items = [
            item
            for item in self._state.artifact_references.values()
            if (uri is None or item.uri == uri)
            and (workload_id is None or item.workload_id == workload_id)
            and (not active_only or item.released_at is None)
        ]
        items.sort(key=lambda item: (item.created_at, item.id))
        return items

    async def get_idempotency(self, scope: str, operation: str, key: str) -> IdempotencyRecord | None:
        return self._state.idempotency.get((scope, operation, key))

    async def put_idempotency(self, record: IdempotencyRecord) -> None:
        self._state.idempotency[(record.scope, record.operation, record.key)] = record

    async def get_outbox(self, record_id: str) -> OutboxRecord | None:
        return self._state.outbox.get(record_id)

    async def put_outbox(self, record: OutboxRecord) -> None:
        self._state.outbox[record.id] = record

    async def list_outbox(self, *, state: OutboxState | None = None, limit: int = 100) -> list[OutboxRecord]:
        records = (record for record in self._state.outbox.values() if state is None or record.state is state)
        return sorted(records, key=lambda record: (record.available_at, record.created_at, record.id))[:limit]

    async def list_outbox_for_aggregates(
        self,
        aggregate_type: str,
        aggregate_generations: tp.Mapping[str, int],
        topic: str | None = None,
    ) -> list[OutboxRecord]:
        """Return every current-generation command for the supplied aggregates."""

        if not aggregate_generations:
            return []
        records = (
            record
            for record in self._state.outbox.values()
            if record.aggregate_type == aggregate_type
            and (topic is None or record.topic == topic)
            and record.aggregate_id in aggregate_generations
            and record.generation == aggregate_generations[record.aggregate_id]
        )
        return sorted(records, key=lambda record: (record.available_at, record.created_at, record.id))

    async def append_event(self, event: AuditEvent) -> AuditEvent:
        stored = event.model_copy(update={"sequence": self._state.next_event_sequence})
        self._state.next_event_sequence += 1
        self._state.events.append(stored)
        return stored

    async def list_events(
        self,
        *,
        after_sequence: int = 0,
        org_id: str | None = None,
        project_id: str | None = None,
        entity_type: str | None = None,
        entity_id: str | None = None,
        occurred_at_or_after: datetime | None = None,
        occurred_before: datetime | None = None,
        newest_first: bool = False,
        limit: int = 100,
    ) -> list[AuditEvent]:
        items = [
            event
            for event in self._state.events
            if event.sequence > after_sequence
            and (org_id is None or event.org_id == org_id)
            and (project_id is None or event.project_id == project_id)
            and (entity_type is None or event.entity_type == entity_type)
            and (entity_id is None or event.entity_id == entity_id)
            and (occurred_at_or_after is None or event.timestamp >= occurred_at_or_after)
            and (occurred_before is None or event.timestamp < occurred_before)
        ]
        items.sort(key=lambda event: event.sequence, reverse=newest_first)
        return items[:limit]

    async def prune_outbox(self, *, before: datetime, limit: int = 1000) -> int:
        """Delete settled outbox commands last updated before ``before``.

        Only SUCCEEDED and DISCARDED rows are eligible: those are absorbing
        states, so removing them cannot lose an undelivered command. PENDING,
        PROCESSING, and FAILED rows are all still owed delivery.
        """

        victims = [
            record.id
            for record in sorted(self._state.outbox.values(), key=lambda item: (item.updated_at, item.id))
            if record.state in _PRUNABLE_OUTBOX_STATES and record.updated_at < before
        ][:limit]
        for record_id in victims:
            self._state.outbox.pop(record_id, None)
        return len(victims)

    async def prune_events(self, *, before: datetime, limit: int = 1000) -> int:
        """Delete audit events emitted before ``before``.

        Safe here because sequences come from the monotonic
        ``next_event_sequence`` counter, not from list position, and readers
        filter on ``sequence > cursor`` rather than assuming contiguity.
        """

        keep: list[AuditEvent] = []
        removed = 0
        for event in self._state.events:
            if removed < limit and event.timestamp < before:
                removed += 1
                continue
            keep.append(event)
        self._state.events = keep
        return removed

    async def prune_idempotency(self, *, before: datetime, limit: int = 1000) -> int:
        """Delete idempotency bindings created before ``before``.

        Without this, a key is honored forever: a caller reusing one months
        later silently replays the original response instead of acting.
        """

        victims = [
            key
            for key, record in sorted(self._state.idempotency.items(), key=lambda item: (item[1].created_at, item[0]))
            if record.created_at < before
        ][:limit]
        for key in victims:
            self._state.idempotency.pop(key, None)
        return len(victims)

    @staticmethod
    def _is_outbox_claimable(record: OutboxRecord, now: datetime) -> bool:
        return record.available_at <= now and (
            record.state in {OutboxState.PENDING, OutboxState.FAILED}
            or (
                record.state is OutboxState.PROCESSING
                and record.claimed_until is not None
                and record.claimed_until <= now
            )
        )

    def _claimable_outbox_records(self, *, now: datetime, candidate_ids: frozenset[str] | None) -> list[OutboxRecord]:
        return sorted(
            (
                record
                for record in self._state.outbox.values()
                if self._is_outbox_claimable(record, now) and (candidate_ids is None or record.id in candidate_ids)
            ),
            key=lambda record: (record.available_at, record.created_at, record.id),
        )

    def claimable_outbox_ids(self, *, limit: int) -> tuple[str, ...]:
        """Return the ordered eligible command identifiers without leasing them."""

        records = self._claimable_outbox_records(now=utcnow(), candidate_ids=None)
        return tuple(record.id for record in records[:limit])

    def claim_ready_outbox(
        self,
        *,
        worker_id: str,
        limit: int,
        lease_seconds: int,
        candidate_ids: frozenset[str] | None = None,
    ) -> list[OutboxRecord]:
        """Claim ready outbox work and expired processing claims under the transaction lock."""

        now = utcnow()
        candidates = self._claimable_outbox_records(now=now, candidate_ids=candidate_ids)[:limit]
        claimed: list[OutboxRecord] = []
        for record in candidates:
            updated = record.model_copy(
                update={
                    "state": OutboxState.PROCESSING,
                    "attempts": record.attempts + 1,
                    "claimed_by": worker_id,
                    "claimed_until": now + timedelta(seconds=lease_seconds),
                    "updated_at": now,
                }
            )
            self._state.outbox[updated.id] = updated
            claimed.append(updated)
        return claimed


class StateRepository:
    """Copy-on-write repository mechanics shared by memory and local JSON stores."""

    session_type: type[StateSession] = StateSession

    def __init__(self) -> None:
        self._state = ControlPlaneState()
        self._transaction_lock = asyncio.Lock()
        self._event_condition = asyncio.Condition()

    async def open(self) -> None:
        """Open the repository lifecycle. In-memory state is always open."""

        return None

    async def close(self) -> None:
        """Close the repository lifecycle. In-memory state has no resources."""

        return None

    async def ping(self) -> None:
        """In-memory state is reachable whenever this process is."""

        return None

    def _persist(self, state: ControlPlaneState) -> None:
        """Durably commit one validated state snapshot before publishing it in process."""

        return None

    @asynccontextmanager
    async def transaction(self) -> tp.AsyncIterator[StateSession]:
        """Yield one serializable copy-on-write transaction."""

        await self.open()
        committed_events = False
        async with self._transaction_lock:
            working = copy.deepcopy(self._state)
            start_sequence = working.next_event_sequence
            yield self.session_type(working)
            if working != self._state:
                working.revision += 1
                self._persist(working)
                self._state = working
                committed_events = working.next_event_sequence > start_sequence
        if committed_events:
            async with self._event_condition:
                self._event_condition.notify_all()

    async def wait_for_events(
        self,
        *,
        after_sequence: int,
        org_id: str | None = None,
        project_id: str | None = None,
        timeout: float | None = None,
    ) -> list[AuditEvent]:
        """Wait until matching committed events exist or the timeout elapses."""

        await self.open()
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            async with self._transaction_lock:
                session = self.session_type(self._state)
                events = await session.list_events(
                    after_sequence=after_sequence,
                    org_id=org_id,
                    project_id=project_id,
                    limit=100,
                )
                observed_revision = self._state.revision
            if events:
                return events
            remaining = None if deadline is None else deadline - loop.time()
            if remaining is not None and remaining <= 0:
                return []
            try:
                async with self._event_condition:
                    await asyncio.wait_for(
                        self._event_condition.wait_for(
                            lambda observed_revision=observed_revision: self._state.revision != observed_revision
                        ),
                        timeout=remaining,
                    )
            except asyncio.TimeoutError:
                return []

    async def claim_outbox(
        self,
        *,
        worker_id: str,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[OutboxRecord]:
        """Atomically claim ready commands and expired processing claims."""

        async with self.transaction() as transaction:
            return transaction.claim_ready_outbox(
                worker_id=worker_id,
                limit=limit,
                lease_seconds=lease_seconds,
            )

    async def snapshot_claimable_outbox_ids(self, *, limit: int = 10) -> tuple[str, ...]:
        """Capture the current eligible outbox identifiers without claiming them."""

        async with self.transaction() as transaction:
            return transaction.claimable_outbox_ids(limit=limit)

    async def claim_outbox_id(
        self,
        *,
        worker_id: str,
        command_id: str,
        lease_seconds: int = 60,
    ) -> OutboxRecord | None:
        """Claim one specific command if it remains eligible at claim time."""

        async with self.transaction() as transaction:
            claimed = transaction.claim_ready_outbox(
                worker_id=worker_id,
                limit=1,
                lease_seconds=lease_seconds,
                candidate_ids=frozenset({command_id}),
            )
        return claimed[0] if claimed else None


__all__ = ("ControlPlaneState", "StateRepository", "StateSession")
