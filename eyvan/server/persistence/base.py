# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Transactional persistence contracts used by the control-plane service."""

from __future__ import annotations

import typing as tp
from datetime import datetime

from eyvan.records import (
    ArtifactReferenceRecord,
    AuditEvent,
    BudgetRecord,
    CapacityPoolRecord,
    CellRecord,
    CredentialRotationRecord,
    IdempotencyRecord,
    LeaseRecord,
    ManagedArtifactRecord,
    ManagedArtifactState,
    OrganizationRecord,
    OutboxRecord,
    OutboxState,
    PolicyRecord,
    ProjectRecord,
    QuotaRecord,
    RoleGrantRecord,
    UserProgressReport,
    WorkloadAttemptTopology,
    WorkloadRecord,
    WorkloadState,
)
from eyvan.server.service.federation import BudgetGrantRecord, PlanePeerRecord, QuotaGrantRecord


class TransientRepositoryError(RuntimeError):
    """A serializable transaction must be retried from a fresh snapshot."""


class SchemaRevisionMismatch(RuntimeError):
    """The database schema is not the revision this build was written against.

    Raised at startup rather than tolerated, in both directions. A database *behind*
    the code lacks columns the code indexes and pages on -- and this package has
    already shipped one bug where a prune silently deleted nothing because it paged
    on the wrong column. A database *ahead* of the code holds rows with fields the
    code cannot parse, which is the exact scenario the per-row list decoder exists to
    contain rather than to normalize.
    """


class RepositorySession(tp.Protocol):
    """One atomic read/write view of Eyvan state."""

    async def get_workload(self, workload_id: str) -> WorkloadRecord | None: ...

    async def list_workloads(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        state: WorkloadState | None = None,
    ) -> list[WorkloadRecord]: ...

    async def put_workload(self, workload: WorkloadRecord) -> None: ...

    async def get_cell(self, cell_id: str) -> CellRecord | None: ...

    async def list_cells(self) -> list[CellRecord]: ...

    async def list_child_workloads(self, parent_workload_id: str) -> list[WorkloadRecord]: ...

    async def list_cells_for_autoscale_request(self, workload_id: str) -> list[CellRecord]: ...

    async def put_cell(self, cell: CellRecord) -> None: ...

    async def get_capacity_pool(self, pool_id: str) -> CapacityPoolRecord | None: ...

    async def list_capacity_pools(self, *, fleet_name: str | None = None) -> list[CapacityPoolRecord]: ...

    async def put_capacity_pool(self, pool: CapacityPoolRecord) -> None: ...

    async def get_lease(self, lease_id: str) -> LeaseRecord | None: ...

    async def list_leases(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        workload_id: str | None = None,
    ) -> list[LeaseRecord]: ...

    async def put_lease(self, lease: LeaseRecord) -> None: ...

    async def get_policy(self, org_id: str, project_id: str) -> PolicyRecord | None: ...

    async def list_policies(self, *, org_id: str | None = None) -> list[PolicyRecord]: ...

    async def put_policy(self, policy: PolicyRecord) -> None: ...

    async def get_quota(self, org_id: str, project_id: str | None, principal_id: str | None) -> QuotaRecord | None: ...

    async def list_quotas(
        self, *, org_id: str | None = None, project_id: str | None = None, principal_id: str | None = None
    ) -> list[QuotaRecord]: ...

    async def put_quota(self, quota: QuotaRecord) -> None: ...

    async def delete_quota(self, org_id: str, project_id: str | None, principal_id: str | None) -> bool: ...

    async def list_role_grants(self, *, subject: str | None = None) -> list[RoleGrantRecord]: ...

    async def put_role_grant(self, grant: RoleGrantRecord) -> None: ...

    async def delete_role_grant(self, key: tuple[str, str, str, str]) -> bool: ...

    async def get_organization(self, org_id: str) -> OrganizationRecord | None: ...

    async def list_organizations(self) -> list[OrganizationRecord]: ...

    async def put_organization(self, organization: OrganizationRecord) -> None: ...

    async def get_project(self, org_id: str, project_id: str) -> ProjectRecord | None: ...

    async def list_projects(self, *, org_id: str | None = None) -> list[ProjectRecord]: ...

    async def put_project(self, project: ProjectRecord) -> None: ...

    async def get_budget(self, org_id: str, project_id: str) -> BudgetRecord | None: ...

    async def list_budgets(self, *, org_id: str | None = None) -> list[BudgetRecord]: ...

    async def put_budget(self, budget: BudgetRecord) -> None: ...

    async def get_managed_artifact(self, org_id: str, project_id: str, uri: str) -> ManagedArtifactRecord | None: ...

    async def put_managed_artifact(self, record: ManagedArtifactRecord) -> None: ...

    async def list_managed_artifacts(
        self,
        *,
        org_id: str,
        project_id: str,
        kind: str | None = None,
        limit: int = 1000,
    ) -> list[ManagedArtifactRecord]: ...

    async def list_managed_artifacts_page(
        self,
        *,
        org_id: str,
        project_id: str,
        after_uri: str | None,
        limit: int,
    ) -> list[ManagedArtifactRecord]:
        """Return a URI-ordered bounded page for physical inventory comparison."""
        ...

    async def list_managed_artifact_projects(self, *, limit: int = 1000) -> list[tuple[str, str]]:
        """Return a bounded ordered page of scopes that own artifact rows."""
        ...

    async def list_managed_artifacts_by_state(
        self,
        *,
        state: ManagedArtifactState,
        org_id: str | None = None,
        project_id: str | None = None,
        updated_before: datetime | None = None,
        limit: int = 100,
    ) -> list[ManagedArtifactRecord]:
        """Return a bounded oldest-first lifecycle recovery page."""
        ...

    async def sum_managed_artifact_bytes(self, *, org_id: str, project_id: str | None = None) -> int:
        """Return total indexed bytes in an organization or project scope.

        An aggregate rather than a list because the quota check runs on the
        upload path: pulling every row to sum a column would make each upload
        proportional to the project's artifact count.
        """
        ...

    async def count_managed_artifacts(self, *, org_id: str, project_id: str | None = None) -> int:
        """Return the number of indexed rows in an organization or project scope."""
        ...

    async def sum_all_managed_artifact_bytes(self) -> int:
        """Return fleet-wide indexed bytes without exposing an unscoped row listing."""
        ...

    async def put_artifact_reference(self, record: ArtifactReferenceRecord) -> None: ...

    async def list_artifact_references(
        self,
        *,
        org_id: str,
        project_id: str,
        uri: str | None = None,
        workload_id: str | None = None,
        active_only: bool = False,
    ) -> list[ArtifactReferenceRecord]: ...

    async def put_progress_report(self, workload_id: str, report: UserProgressReport) -> None:
        """Upsert one rank's latest report, replacing any earlier one."""
        ...

    async def list_progress_reports(self, workload_id: str) -> list[UserProgressReport]: ...

    async def get_workload_topology(self, workload_id: str, attempt: int) -> WorkloadAttemptTopology | None:
        """Return one attempt's frozen rank space, or ``None`` when it is unknown.

        ``None`` is a real answer and never an invitation to reconstruct one. An
        attempt dispatched before this record existed has no proven rank map, and
        deriving it from today's cells is exactly the mistake the record removes.
        """
        ...

    async def put_workload_topology(self, topology: WorkloadAttemptTopology) -> None:
        """Freeze one attempt's rank space, keyed by (workload id, attempt)."""
        ...

    async def list_workload_topologies(self, workload_id: str) -> list[WorkloadAttemptTopology]:
        """Return every frozen attempt topology for one workload, oldest attempt first."""
        ...

    async def get_credential_rotation(self, credential: str, generation: int) -> CredentialRotationRecord | None:
        """Return one credential rotation by its exact generation, or ``None``."""
        ...

    async def put_credential_rotation(self, rotation: CredentialRotationRecord) -> None:
        """Insert or replace one rotation row, keyed by (credential, generation)."""
        ...

    async def latest_credential_rotation(self, credential: str) -> CredentialRotationRecord | None:
        """Return the highest-generation rotation of one credential, or ``None``.

        ``None`` means this credential has never been rotated, which is a
        different statement from "its last rotation failed" -- the caller starts
        from generation 1 in the first case and refuses in the second.
        """
        ...

    async def get_plane_peer(self, plane_id: str) -> PlanePeerRecord | None: ...

    async def put_plane_peer(self, peer: PlanePeerRecord) -> None: ...

    async def list_plane_peers(self) -> list[PlanePeerRecord]: ...

    async def get_budget_grant(self, grant_id: str) -> BudgetGrantRecord | None: ...

    async def put_budget_grant(self, grant: BudgetGrantRecord) -> None: ...

    async def list_budget_grants(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        grantee_plane_id: str | None = None,
    ) -> list[BudgetGrantRecord]: ...

    async def get_quota_grant(self, grant_id: str) -> QuotaGrantRecord | None: ...

    async def put_quota_grant(self, grant: QuotaGrantRecord) -> None: ...

    async def list_quota_grants(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        grantee_plane_id: str | None = None,
    ) -> list[QuotaGrantRecord]: ...

    async def get_idempotency(self, scope: str, operation: str, key: str) -> IdempotencyRecord | None: ...

    async def put_idempotency(self, record: IdempotencyRecord) -> None: ...

    async def get_outbox(self, record_id: str) -> OutboxRecord | None: ...

    async def put_outbox(self, record: OutboxRecord) -> None: ...

    async def list_outbox(self, *, state: OutboxState | None = None, limit: int = 100) -> list[OutboxRecord]: ...

    async def list_outbox_for_aggregates(
        self,
        aggregate_type: str,
        aggregate_generations: tp.Mapping[str, int],
        topic: str | None = None,
    ) -> list[OutboxRecord]: ...

    async def prune_outbox(self, *, before: datetime, limit: int = 1000) -> int:
        """Delete settled outbox commands last updated before ``before``."""
        ...

    async def prune_events(self, *, before: datetime, limit: int = 1000) -> int:
        """Delete audit events created before ``before``."""
        ...

    async def prune_idempotency(self, *, before: datetime, limit: int = 1000) -> int:
        """Delete idempotency bindings created before ``before``."""
        ...

    async def append_event(self, event: AuditEvent) -> AuditEvent: ...

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
    ) -> list[AuditEvent]: ...


class Repository(tp.Protocol):
    """Repository capable of opening serializable application transactions."""

    async def open(self) -> None:
        """Acquire repository resources before the API starts serving requests."""

        ...

    async def close(self) -> None:
        """Release repository resources after controllers have stopped."""

        ...

    def transaction(self) -> tp.AsyncContextManager[RepositorySession]: ...

    async def ping(self) -> None:
        """Raise when this replica cannot currently reach its store.

        Separate from ``open`` because the answer changes: a connection that
        worked at startup can be gone an hour later, and a readiness probe that
        reported the startup answer would keep a replica in a Service's endpoints
        while every request it receives fails.

        Raises rather than returning a bool so the reason survives to the caller.
        """

        ...

    async def wait_for_events(
        self,
        *,
        after_sequence: int,
        org_id: str | None = None,
        project_id: str | None = None,
        timeout: float | None = None,
    ) -> list[AuditEvent]: ...

    async def snapshot_claimable_outbox_ids(self, *, limit: int = 10) -> tuple[str, ...]: ...

    async def claim_outbox_id(
        self,
        *,
        worker_id: str,
        command_id: str,
        lease_seconds: int = 60,
    ) -> OutboxRecord | None: ...

    async def claim_outbox(
        self,
        *,
        worker_id: str,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[OutboxRecord]: ...
