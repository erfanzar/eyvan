# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Async SQLAlchemy/Postgres repository implementing Eyvan transactions."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import typing as tp
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from pydantic import BaseModel, ValidationError
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Connection,
    DateTime,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    and_,
    func,
    or_,
    select,
    text,
)
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql.elements import ColumnElement

from eyvan._internal.json_types import JsonObject, model_json
from eyvan.records import (
    ArtifactReferenceRecord,
    AuditEvent,
    BudgetRecord,
    CapacityPoolRecord,
    CellRecord,
    ExpansionAdmissionRecord,
    ExpansionAdmissionState,
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
    utcnow,
)
from eyvan.server.persistence.base import SchemaRevisionMismatch, TransientRepositoryError
from eyvan.server.service.federation import BudgetGrantRecord, PlanePeerRecord, QuotaGrantRecord

SCHEMA_REVISION = "0016_expansion_admissions"


@tp.runtime_checkable
class _SqlStateError(tp.Protocol):
    """DBAPI exception exposing the SQL-standard state attribute."""

    sqlstate: str | None


@tp.runtime_checkable
class _PostgresCodeError(tp.Protocol):
    """Legacy psycopg exception exposing PostgreSQL's error code attribute."""

    pgcode: str | None


def _transaction_sqlstate(error: BaseException) -> str | None:
    """Read a typed transaction code from supported DBAPI exception contracts."""

    if isinstance(error, _SqlStateError) and error.sqlstate is not None:
        return error.sqlstate
    if isinstance(error, _PostgresCodeError):
        return error.pgcode
    return None


logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """Declarative metadata used as the Alembic migration source."""


class SchemaRevisionRow(Base):
    __tablename__ = "schema_revisions"
    revision: Mapped[str] = mapped_column(String(64), primary_key=True)
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class WorkloadRow(Base):
    __tablename__ = "workloads"
    id: Mapped[str] = mapped_column(String(96), primary_key=True)
    org_id: Mapped[str | None] = mapped_column(String(128), index=True)
    project_id: Mapped[str | None] = mapped_column(String(128), index=True)
    state: Mapped[str | None] = mapped_column(String(32), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class CellRow(Base):
    __tablename__ = "cells"
    id: Mapped[str] = mapped_column(String(96), primary_key=True)
    state: Mapped[str | None] = mapped_column(String(32), index=True)
    pool_id: Mapped[str | None] = mapped_column(String(192), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class CapacityPoolRow(Base):
    __tablename__ = "capacity_pools"
    id: Mapped[str] = mapped_column(String(192), primary_key=True)
    fleet_name: Mapped[str | None] = mapped_column(String(96), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class LeaseRow(Base):
    __tablename__ = "leases"
    id: Mapped[str] = mapped_column(String(96), primary_key=True)
    org_id: Mapped[str | None] = mapped_column(String(128), index=True)
    project_id: Mapped[str | None] = mapped_column(String(128), index=True)
    workload_id: Mapped[str | None] = mapped_column(String(96), index=True)
    state: Mapped[str | None] = mapped_column(String(32), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class PolicyRow(Base):
    __tablename__ = "policies"
    id: Mapped[str] = mapped_column(String(256), primary_key=True)
    org_id: Mapped[str | None] = mapped_column(String(128), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class QuotaRow(Base):
    __tablename__ = "quotas"
    id: Mapped[str] = mapped_column(String(384), primary_key=True)
    org_id: Mapped[str | None] = mapped_column(String(128), index=True)
    project_id: Mapped[str | None] = mapped_column(String(128), index=True)
    principal_id: Mapped[str | None] = mapped_column(String(128), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class BudgetRow(Base):
    __tablename__ = "budgets"
    id: Mapped[str] = mapped_column(String(256), primary_key=True)
    org_id: Mapped[str | None] = mapped_column(String(128), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class RoleGrantRow(Base):
    __tablename__ = "role_grants"
    subject: Mapped[str] = mapped_column(String(256), primary_key=True, index=True)
    role: Mapped[str] = mapped_column(String(32), primary_key=True)
    #: The empty string stands in for an absent scope. NULLs are not equal to
    #: each other in SQL, so a nullable key component would let the same
    #: fleet-admin grant be inserted an unbounded number of times.
    org_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    project_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class OrganizationRow(Base):
    __tablename__ = "organizations"
    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    #: A real column, not a field inside ``data``: listing enabled tenants is an
    #: index scan and a JSON extraction cannot use one.
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class ProjectRow(Base):
    __tablename__ = "projects"
    org_id: Mapped[str] = mapped_column(String(128), primary_key=True, index=True)
    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class ProgressReportRow(Base):
    __tablename__ = "progress_reports"
    #: Composite key, so a new report for a rank overwrites rather than appends.
    #: Unbounded per-rank history is the growth this design refuses.
    workload_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    rank: Mapped[int] = mapped_column(Integer, primary_key=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class WorkloadTopologyRow(Base):
    __tablename__ = "workload_topologies"
    __table_args__ = (
        CheckConstraint("attempt >= 1", name="ck_workload_topologies_attempt_positive"),
        CheckConstraint("total_rank_count >= 1", name="ck_workload_topologies_rank_count_positive"),
    )
    #: Composite key on the attempt, not on the workload: a retry is a different
    #: rank space, and overwriting the previous attempt's row would destroy the
    #: only proof of which host was rank N while that attempt was running.
    workload_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    attempt: Mapped[int] = mapped_column(Integer, primary_key=True)
    #: A real column so the digest can be compared without extracting JSON, and
    #: so a mismatch is visible to an operator reading the table directly.
    digest: Mapped[str] = mapped_column(String(71), nullable=False)
    total_rank_count: Mapped[int] = mapped_column(Integer, nullable=False)
    data: Mapped[JsonObject] = mapped_column(JSON)


class ExpansionAdmissionRow(Base):
    __tablename__ = "expansion_admissions"
    __table_args__ = (
        CheckConstraint("total_children >= 1", name="ck_expansion_admissions_total_positive"),
        CheckConstraint("wave_size >= 1", name="ck_expansion_admissions_wave_positive"),
    )
    #: Derived from the coordinator id and short by construction, so the ledger
    #: of one expansion is addressable in a 32-character key column.
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    #: Unique, not merely indexed: one coordinator has exactly one admission
    #: plan, and a second row for it would mean two leaders releasing waves from
    #: two different plans over the same children.
    workload_id: Mapped[str] = mapped_column(String(96), unique=True, nullable=False)
    #: A real column so the leader's wave phase scans the expansions still owing
    #: children instead of decoding every expansion the fleet has ever admitted.
    state: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    total_children: Mapped[int] = mapped_column(Integer, nullable=False)
    wave_size: Mapped[int] = mapped_column(Integer, nullable=False)
    data: Mapped[JsonObject] = mapped_column(JSON)


class PlanePeerRow(Base):
    __tablename__ = "plane_peers"
    plane_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class BudgetGrantRow(Base):
    __tablename__ = "budget_grants"
    id: Mapped[str] = mapped_column(String(96), primary_key=True)
    org_id: Mapped[str | None] = mapped_column(String(128), index=True)
    project_id: Mapped[str | None] = mapped_column(String(128), index=True)
    grantee_plane_id: Mapped[str | None] = mapped_column(String(128), index=True)
    #: Indexed alongside ``state`` because the expiry sweep filters *and* pages on
    #: both -- exactly the lesson migration 0005 was written to record.
    state: Mapped[str | None] = mapped_column(String(32), index=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    issued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class QuotaGrantRow(Base):
    __tablename__ = "quota_grants"
    id: Mapped[str] = mapped_column(String(96), primary_key=True)
    org_id: Mapped[str | None] = mapped_column(String(128), index=True)
    project_id: Mapped[str | None] = mapped_column(String(128), index=True)
    grantee_plane_id: Mapped[str | None] = mapped_column(String(128), index=True)
    #: ``state`` and ``expires_at`` indexed together for the same reason the
    #: budget grants are: the expiry sweep filters and pages on both.
    state: Mapped[str | None] = mapped_column(String(32), index=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    issued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class ManagedArtifactRow(Base):
    __tablename__ = "managed_artifacts"
    __table_args__ = (
        PrimaryKeyConstraint("org_id", "project_id", "uri", name="pk_managed_artifacts_scoped"),
        CheckConstraint(
            "state IN ('pending_upload', 'active', 'deleting', 'deleted', 'quarantined')",
            name="ck_managed_artifacts_lifecycle_state",
        ),
        CheckConstraint(
            "record_generation >= 1",
            name="ck_managed_artifacts_record_generation_positive",
        ),
        Index("ix_managed_artifacts_scope_bytes", "org_id", "project_id", "size_bytes"),
        Index("ix_managed_artifacts_scope_created", "org_id", "project_id", "created_at", "uri"),
        Index("ix_managed_artifacts_scope_state_updated", "org_id", "project_id", "state", "updated_at"),
    )
    #: The physical and durable identity. A content-addressed URI can appear in
    #: every project without merging ownership or quota accounting. The explicit
    #: constraint name matches migration 0012, including create_schema databases
    #: that are later conditionally downgraded.
    org_id: Mapped[str] = mapped_column(String(128), index=True)
    project_id: Mapped[str] = mapped_column(String(128), index=True)
    uri: Mapped[str] = mapped_column(String(512))
    kind: Mapped[str | None] = mapped_column(String(32), index=True)
    state: Mapped[str] = mapped_column(String(32), index=True)
    record_generation: Mapped[int] = mapped_column(BigInteger)
    object_version: Mapped[str | None] = mapped_column(String(256))
    #: Typed accounting/state columns make quota and recovery bounded without
    #: trusting a JSON projection that could disagree with the locked row.
    size_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class ArtifactReferenceRow(Base):
    __tablename__ = "artifact_references"
    __table_args__ = (
        PrimaryKeyConstraint("org_id", "project_id", "uri", "workload_id", name="pk_artifact_references_scoped"),
        Index("ix_artifact_references_scope_uri", "org_id", "project_id", "uri"),
        Index("ix_artifact_references_scope_workload", "org_id", "project_id", "workload_id"),
        Index(
            "ix_artifact_references_live",
            "org_id",
            "project_id",
            "uri",
            postgresql_where=text("released_at IS NULL"),
            sqlite_where=text("released_at IS NULL"),
        ),
    )
    #: Durable identity is explicit rather than delegated to the display hash.
    org_id: Mapped[str] = mapped_column(String(128))
    project_id: Mapped[str] = mapped_column(String(128))
    uri: Mapped[str] = mapped_column(String(512))
    workload_id: Mapped[str] = mapped_column(String(96))
    id: Mapped[str] = mapped_column(String(200))
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


def _managed_artifact_row_matches(row: ManagedArtifactRow, record: ManagedArtifactRecord) -> bool:
    """Return whether typed lifecycle columns agree with their JSON record."""

    return (
        (record.org_id, record.project_id, record.uri) == (row.org_id, row.project_id, row.uri)
        and row.kind == record.kind
        and row.state == record.state.value
        and row.record_generation == record.record_generation
        and row.object_version == record.object_version
        and row.size_bytes == record.size_bytes
    )


def _decode_managed_artifact_rows(rows: tp.Iterable[ManagedArtifactRow]) -> list[ManagedArtifactRecord]:
    records: list[ManagedArtifactRecord] = []
    for row in rows:
        try:
            record = ManagedArtifactRecord.model_validate(row.data)
        except ValidationError:
            logger.exception("skipping unreadable managed_artifacts row %r", row.uri)
            continue
        if not _managed_artifact_row_matches(row, record):
            logger.error("skipping managed_artifacts row %r with mismatched typed lifecycle", row.uri)
            continue
        records.append(record)
    return records


class IdempotencyRow(Base):
    __tablename__ = "idempotency_keys"
    __table_args__ = (UniqueConstraint("scope", "operation", "key", name="uq_idempotency_scope_operation_key"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scope: Mapped[str] = mapped_column(String(256), index=True)
    operation: Mapped[str] = mapped_column(String(96))
    key: Mapped[str] = mapped_column(String(200))
    #: Indexed copy of the record's ``created_at`` so retention can both filter
    #: and page on the same column. Paging by ``id`` while filtering on a value
    #: inside ``data`` meant a prune could return 0 forever: the oldest rows sat
    #: past the LIMIT and every call re-scanned the same ineligible window.
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class OutboxRow(Base):
    __tablename__ = "outbox"
    id: Mapped[str] = mapped_column(String(96), primary_key=True)
    state: Mapped[str] = mapped_column(String(32), index=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    claimed_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    #: Indexed copy of the record's ``updated_at``. ``available_at`` is set at
    #: enqueue and ``updated_at`` at settlement, so paging by the former while
    #: filtering on the latter let long-queued-but-recently-settled rows fill
    #: the LIMIT window and starve genuinely old rows behind them.
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


#: Outbox states safe to delete. Both are absorbing, so removing a row cannot
#: drop a command that is still owed delivery.
_PRUNABLE_OUTBOX_STATES = frozenset({OutboxState.SUCCEEDED, OutboxState.DISCARDED})


def _claimable_outbox_clause(now: datetime) -> ColumnElement[bool]:
    """Build the shared ready-or-expired-processing predicate for outbox ownership."""

    ready_state = OutboxRow.state.in_((OutboxState.PENDING.value, OutboxState.FAILED.value))
    abandoned = and_(OutboxRow.state == OutboxState.PROCESSING.value, OutboxRow.claimed_until <= now)
    return and_(OutboxRow.available_at <= now, or_(ready_state, abandoned))


class AuditEventRow(Base):
    __tablename__ = "audit_events"
    __table_args__ = (
        Index(
            "ix_audit_events_scope_type_occurred_at",
            "org_id",
            "project_id",
            "entity_type",
            "occurred_at",
        ),
    )
    sequence: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    id: Mapped[str] = mapped_column(String(96), unique=True, nullable=False)
    org_id: Mapped[str | None] = mapped_column(String(128), index=True)
    project_id: Mapped[str | None] = mapped_column(String(128), index=True)
    entity_type: Mapped[str] = mapped_column(String(64), index=True)
    entity_id: Mapped[str] = mapped_column(String(256), index=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    data: Mapped[JsonObject] = mapped_column(JSON)


_Record = tp.TypeVar("_Record", bound=BaseModel)


def _decode_rows(model: type[_Record], rows: tp.Iterable[tp.Any], *, table: str) -> list[_Record]:
    """Decode persisted rows one at a time, skipping any that will not parse.

    Every ``list_*`` used to decode inside a comprehension, so a single unreadable row
    raised out of the whole call. On ``list_workloads`` -- which the leader's
    reconcile loop calls unfiltered on its hot path -- that means one corrupt or
    forward-versioned row halts scheduling for the *entire fleet*, not just for its
    own workload. Every new persisted field makes that more likely, because a
    rollback to a build that predates the field leaves rows the old code cannot read.

    Skipping is the strictly better failure mode in every caller: one workload
    invisible beats nothing scheduling, one command undelivered beats no command
    delivered. It is deliberately loud -- a skipped row is logged with its identity at
    error level, because a silently shrinking list is its own kind of outage.
    """

    decoded: list[_Record] = []
    for row in rows:
        try:
            decoded.append(model.model_validate(row.data))
        except ValidationError:
            logger.exception(
                "skipping unreadable %s row %r; it is invisible to this process until repaired",
                table,
                getattr(row, "id", "<unknown>"),
            )
    return decoded


def _dump(model: BaseModel) -> JsonObject:
    return model_json(model)


def _composite_key(prefix: str, *parts: str | None) -> str:
    payload = json.dumps(parts, ensure_ascii=True, separators=(",", ":")).encode()
    return f"{prefix}_{hashlib.sha256(payload).hexdigest()}"


def _quota_key(org_id: str, project_id: str | None, principal_id: str | None) -> str:
    return _composite_key("quota", org_id, project_id, principal_id)


class SqlAlchemySession:
    """Repository transaction backed by one SQLAlchemy ``AsyncSession``."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def _get_locked(self, model, key):
        """Read one aggregate under a row lock for the transaction lifetime."""

        return await self.session.get(model, key, with_for_update=True)

    async def get_workload(self, workload_id: str) -> WorkloadRecord | None:
        row = await self._get_locked(WorkloadRow, workload_id)
        return None if row is None else WorkloadRecord.model_validate(row.data)

    async def list_workloads(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        state: WorkloadState | None = None,
    ) -> list[WorkloadRecord]:
        statement = select(WorkloadRow)
        if org_id is not None:
            statement = statement.where(WorkloadRow.org_id == org_id)
        if project_id is not None:
            statement = statement.where(WorkloadRow.project_id == project_id)
        if state is not None:
            statement = statement.where(WorkloadRow.state == state.value)
        rows = (await self.session.scalars(statement)).all()
        items = _decode_rows(WorkloadRecord, rows, table="workloads")
        return sorted(
            (
                item
                for item in items
                if (org_id is None or item.org_id == org_id)
                and (project_id is None or item.project_id == project_id)
                and (state is None or item.state == state)
            ),
            key=lambda item: (item.created_at, item.id),
        )

    async def put_workload(self, workload: WorkloadRecord) -> None:
        await self.session.merge(
            WorkloadRow(
                id=workload.id,
                org_id=workload.org_id,
                project_id=workload.project_id,
                state=workload.state.value,
                data=_dump(workload),
            )
        )

    async def get_cell(self, cell_id: str) -> CellRecord | None:
        row = await self._get_locked(CellRow, cell_id)
        return None if row is None else CellRecord.model_validate(row.data)

    async def list_cells(self) -> list[CellRecord]:
        rows = (await self.session.scalars(select(CellRow))).all()
        return sorted(_decode_rows(CellRecord, rows, table="cells"), key=lambda item: item.id)

    async def list_child_workloads(self, parent_workload_id: str) -> list[WorkloadRecord]:
        """Load the direct children of one workload in stable identifier order."""

        statement = (
            select(WorkloadRow)
            .where(WorkloadRow.data["parent_workload_id"].as_string() == parent_workload_id)
            .order_by(WorkloadRow.id)
        )
        rows = (await self.session.scalars(statement)).all()
        return _decode_rows(WorkloadRecord, rows, table="workloads")

    async def list_cells_for_autoscale_request(self, workload_id: str) -> list[CellRecord]:
        """Load exactly the cells materialized for one autoscale workload request."""

        statement = (
            select(CellRow)
            .where(CellRow.data["autoscale_request_workload_id"].as_string() == workload_id)
            .order_by(CellRow.id)
        )
        rows = (await self.session.scalars(statement)).all()
        return _decode_rows(CellRecord, rows, table="cells")

    async def put_cell(self, cell: CellRecord) -> None:
        await self.session.merge(CellRow(id=cell.id, state=cell.state.value, pool_id=cell.pool_id, data=_dump(cell)))

    async def get_capacity_pool(self, pool_id: str) -> CapacityPoolRecord | None:
        row = await self._get_locked(CapacityPoolRow, pool_id)
        return None if row is None else CapacityPoolRecord.model_validate(row.data)

    async def list_capacity_pools(self, *, fleet_name: str | None = None) -> list[CapacityPoolRecord]:
        statement = select(CapacityPoolRow)
        if fleet_name is not None:
            statement = statement.where(CapacityPoolRow.fleet_name == fleet_name)
        rows = (await self.session.scalars(statement)).all()
        items = _decode_rows(CapacityPoolRecord, rows, table="capacity_pools")
        return sorted(
            (item for item in items if fleet_name is None or item.fleet_name == fleet_name),
            key=lambda item: (item.fleet_name, item.node_type, item.id),
        )

    async def put_capacity_pool(self, pool: CapacityPoolRecord) -> None:
        await self.session.merge(CapacityPoolRow(id=pool.id, fleet_name=pool.fleet_name, data=_dump(pool)))

    async def get_lease(self, lease_id: str) -> LeaseRecord | None:
        row = await self._get_locked(LeaseRow, lease_id)
        return None if row is None else LeaseRecord.model_validate(row.data)

    async def list_leases(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        workload_id: str | None = None,
    ) -> list[LeaseRecord]:
        statement = select(LeaseRow)
        if org_id is not None:
            statement = statement.where(LeaseRow.org_id == org_id)
        if project_id is not None:
            statement = statement.where(LeaseRow.project_id == project_id)
        if workload_id is not None:
            statement = statement.where(LeaseRow.workload_id == workload_id)
        rows = (await self.session.scalars(statement)).all()
        items = _decode_rows(LeaseRecord, rows, table="leases")
        return sorted(
            (
                item
                for item in items
                if (org_id is None or item.org_id == org_id)
                and (project_id is None or item.project_id == project_id)
                and (workload_id is None or item.workload_id == workload_id)
            ),
            key=lambda item: (item.created_at, item.id),
        )

    async def put_lease(self, lease: LeaseRecord) -> None:
        await self.session.merge(
            LeaseRow(
                id=lease.id,
                org_id=lease.org_id,
                project_id=lease.project_id,
                workload_id=lease.workload_id,
                state=lease.state.value,
                data=_dump(lease),
            )
        )

    async def get_policy(self, org_id: str, project_id: str) -> PolicyRecord | None:
        row = await self._get_locked(PolicyRow, _composite_key("policy", org_id, project_id))
        return None if row is None else PolicyRecord.model_validate(row.data)

    async def list_policies(self, *, org_id: str | None = None) -> list[PolicyRecord]:
        statement = select(PolicyRow)
        if org_id is not None:
            statement = statement.where(PolicyRow.org_id == org_id)
        rows = (await self.session.scalars(statement)).all()
        items = _decode_rows(PolicyRecord, rows, table="policies")
        return sorted(
            (item for item in items if org_id is None or item.org_id == org_id),
            key=lambda item: (item.org_id, item.project_id),
        )

    async def put_policy(self, policy: PolicyRecord) -> None:
        await self.session.merge(
            PolicyRow(
                id=_composite_key("policy", policy.org_id, policy.project_id),
                org_id=policy.org_id,
                data=_dump(policy),
            )
        )

    async def get_quota(self, org_id: str, project_id: str | None, principal_id: str | None) -> QuotaRecord | None:
        row = await self._get_locked(QuotaRow, _quota_key(org_id, project_id, principal_id))
        return None if row is None else QuotaRecord.model_validate(row.data)

    async def list_quotas(
        self, *, org_id: str | None = None, project_id: str | None = None, principal_id: str | None = None
    ) -> list[QuotaRecord]:
        statement = select(QuotaRow)
        if org_id is not None:
            statement = statement.where(QuotaRow.org_id == org_id)
        if project_id is not None:
            statement = statement.where(QuotaRow.project_id == project_id)
        if principal_id is not None:
            statement = statement.where(QuotaRow.principal_id == principal_id)
        rows = (await self.session.scalars(statement)).all()
        items = _decode_rows(QuotaRecord, rows, table="quotas")
        return sorted(
            (
                item
                for item in items
                if (org_id is None or item.org_id == org_id)
                and (project_id is None or item.project_id == project_id)
                and (principal_id is None or item.principal_id == principal_id)
            ),
            key=lambda item: (item.org_id, item.project_id or "", item.principal_id or ""),
        )

    async def put_quota(self, quota: QuotaRecord) -> None:
        await self.session.merge(
            QuotaRow(
                id=_quota_key(quota.org_id, quota.project_id, quota.principal_id),
                org_id=quota.org_id,
                project_id=quota.project_id,
                principal_id=quota.principal_id,
                data=_dump(quota),
            )
        )

    async def delete_quota(self, org_id: str, project_id: str | None, principal_id: str | None) -> bool:
        row = await self._get_locked(QuotaRow, _quota_key(org_id, project_id, principal_id))
        if row is None:
            return False
        await self.session.delete(row)
        return True

    async def list_role_grants(self, *, subject: str | None = None) -> list[RoleGrantRecord]:
        statement = select(RoleGrantRow)
        if subject is not None:
            statement = statement.where(RoleGrantRow.subject == subject)
        rows = (await self.session.scalars(statement)).all()
        items = _decode_rows(RoleGrantRecord, rows, table="role_grants")
        return sorted(
            (item for item in items if subject is None or item.subject == subject),
            key=lambda item: item.key,
        )

    async def put_role_grant(self, grant: RoleGrantRecord) -> None:
        subject, role, org_id, project_id = grant.key
        await self.session.merge(
            RoleGrantRow(
                subject=subject,
                role=role,
                org_id=org_id,
                project_id=project_id,
                expires_at=grant.expires_at,
                data=_dump(grant),
            )
        )

    async def delete_role_grant(self, key: tuple[str, str, str, str]) -> bool:
        subject, role, org_id, project_id = key
        row = await self.session.get(
            RoleGrantRow,
            {"subject": subject, "role": role, "org_id": org_id, "project_id": project_id},
        )
        if row is None:
            return False
        await self.session.delete(row)
        return True

    async def get_organization(self, org_id: str) -> OrganizationRecord | None:
        row = await self._get_locked(OrganizationRow, org_id)
        return None if row is None else OrganizationRecord.model_validate(row.data)

    async def list_organizations(self) -> list[OrganizationRecord]:
        rows = (await self.session.scalars(select(OrganizationRow))).all()
        return sorted(_decode_rows(OrganizationRecord, rows, table="organizations"), key=lambda item: item.id)

    async def put_organization(self, organization: OrganizationRecord) -> None:
        await self.session.merge(
            OrganizationRow(id=organization.id, enabled=organization.enabled, data=_dump(organization))
        )

    async def get_project(self, org_id: str, project_id: str) -> ProjectRecord | None:
        row = await self.session.get(ProjectRow, {"org_id": org_id, "id": project_id}, with_for_update=True)
        return None if row is None else ProjectRecord.model_validate(row.data)

    async def list_projects(self, *, org_id: str | None = None) -> list[ProjectRecord]:
        statement = select(ProjectRow)
        if org_id is not None:
            statement = statement.where(ProjectRow.org_id == org_id)
        rows = (await self.session.scalars(statement)).all()
        items = _decode_rows(ProjectRecord, rows, table="projects")
        return sorted(
            (item for item in items if org_id is None or item.org_id == org_id),
            key=lambda item: (item.org_id, item.id),
        )

    async def put_project(self, project: ProjectRecord) -> None:
        await self.session.merge(
            ProjectRow(org_id=project.org_id, id=project.id, enabled=project.enabled, data=_dump(project))
        )

    async def get_budget(self, org_id: str, project_id: str) -> BudgetRecord | None:
        row = await self._get_locked(BudgetRow, _composite_key("budget", org_id, project_id))
        return None if row is None else BudgetRecord.model_validate(row.data)

    async def list_budgets(self, *, org_id: str | None = None) -> list[BudgetRecord]:
        statement = select(BudgetRow)
        if org_id is not None:
            statement = statement.where(BudgetRow.org_id == org_id)
        rows = (await self.session.scalars(statement)).all()
        items = _decode_rows(BudgetRecord, rows, table="budgets")
        return sorted(
            (item for item in items if org_id is None or item.org_id == org_id),
            key=lambda item: (item.org_id, item.project_id),
        )

    async def put_budget(self, budget: BudgetRecord) -> None:
        await self.session.merge(
            BudgetRow(
                id=_composite_key("budget", budget.org_id, budget.project_id),
                org_id=budget.org_id,
                data=_dump(budget),
            )
        )

    async def put_progress_report(self, workload_id: str, report: UserProgressReport) -> None:
        await self.session.merge(ProgressReportRow(workload_id=workload_id, rank=report.rank, data=_dump(report)))

    async def list_progress_reports(self, workload_id: str) -> list[UserProgressReport]:
        statement = (
            select(ProgressReportRow)
            .where(ProgressReportRow.workload_id == workload_id)
            .order_by(ProgressReportRow.rank)
        )
        rows = (await self.session.scalars(statement)).all()
        return _decode_rows(UserProgressReport, rows, table="progress_reports")

    async def get_workload_topology(self, workload_id: str, attempt: int) -> WorkloadAttemptTopology | None:
        row = await self._get_locked(WorkloadTopologyRow, {"workload_id": workload_id, "attempt": attempt})
        return None if row is None else WorkloadAttemptTopology.model_validate(row.data)

    async def put_workload_topology(self, topology: WorkloadAttemptTopology) -> None:
        await self.session.merge(
            WorkloadTopologyRow(
                workload_id=topology.workload_id,
                attempt=topology.attempt,
                digest=topology.digest,
                total_rank_count=topology.total_rank_count,
                data=_dump(topology),
            )
        )

    async def list_workload_topologies(self, workload_id: str) -> list[WorkloadAttemptTopology]:
        statement = (
            select(WorkloadTopologyRow)
            .where(WorkloadTopologyRow.workload_id == workload_id)
            .order_by(WorkloadTopologyRow.attempt)
        )
        rows = (await self.session.scalars(statement)).all()
        return _decode_rows(WorkloadAttemptTopology, rows, table="workload_topologies")

    async def get_expansion_admission(self, workload_id: str) -> ExpansionAdmissionRecord | None:
        statement = select(ExpansionAdmissionRow).where(ExpansionAdmissionRow.workload_id == workload_id)
        row = (await self.session.scalars(statement)).one_or_none()
        return None if row is None else ExpansionAdmissionRecord.model_validate(row.data)

    async def put_expansion_admission(self, record: ExpansionAdmissionRecord) -> None:
        await self.session.merge(
            ExpansionAdmissionRow(
                id=record.id,
                workload_id=record.workload_id,
                state=record.state.value,
                total_children=record.total_children,
                wave_size=record.wave_size,
                data=_dump(record),
            )
        )

    async def list_expansion_admissions(
        self,
        *,
        state: ExpansionAdmissionState | None = None,
    ) -> list[ExpansionAdmissionRecord]:
        statement = select(ExpansionAdmissionRow).order_by(ExpansionAdmissionRow.id)
        if state is not None:
            statement = statement.where(ExpansionAdmissionRow.state == state.value)
        rows = (await self.session.scalars(statement)).all()
        return _decode_rows(ExpansionAdmissionRecord, rows, table="expansion_admissions")

    async def get_plane_peer(self, plane_id: str) -> PlanePeerRecord | None:
        row = await self._get_locked(PlanePeerRow, plane_id)
        return None if row is None else PlanePeerRecord.model_validate(row.data)

    async def put_plane_peer(self, peer: PlanePeerRecord) -> None:
        await self.session.merge(PlanePeerRow(plane_id=peer.plane_id, data=_dump(peer)))

    async def list_plane_peers(self) -> list[PlanePeerRecord]:
        rows = (await self.session.scalars(select(PlanePeerRow).order_by(PlanePeerRow.plane_id))).all()
        return _decode_rows(PlanePeerRecord, rows, table="plane_peers")

    async def get_budget_grant(self, grant_id: str) -> BudgetGrantRecord | None:
        row = await self._get_locked(BudgetGrantRow, grant_id)
        return None if row is None else BudgetGrantRecord.model_validate(row.data)

    async def put_budget_grant(self, grant: BudgetGrantRecord) -> None:
        await self.session.merge(
            BudgetGrantRow(
                id=grant.id,
                org_id=grant.org_id,
                project_id=grant.project_id,
                grantee_plane_id=grant.grantee_plane_id,
                state=grant.state.value,
                expires_at=grant.expires_at,
                issued_at=grant.issued_at,
                data=_dump(grant),
            )
        )

    async def list_budget_grants(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        grantee_plane_id: str | None = None,
    ) -> list[BudgetGrantRecord]:
        statement = select(BudgetGrantRow)
        if org_id is not None:
            statement = statement.where(BudgetGrantRow.org_id == org_id)
        if project_id is not None:
            statement = statement.where(BudgetGrantRow.project_id == project_id)
        if grantee_plane_id is not None:
            statement = statement.where(BudgetGrantRow.grantee_plane_id == grantee_plane_id)
        statement = statement.order_by(BudgetGrantRow.issued_at, BudgetGrantRow.id)
        rows = (await self.session.scalars(statement)).all()
        return _decode_rows(BudgetGrantRecord, rows, table="budget_grants")

    async def get_quota_grant(self, grant_id: str) -> QuotaGrantRecord | None:
        row = await self._get_locked(QuotaGrantRow, grant_id)
        return None if row is None else QuotaGrantRecord.model_validate(row.data)

    async def put_quota_grant(self, grant: QuotaGrantRecord) -> None:
        await self.session.merge(
            QuotaGrantRow(
                id=grant.id,
                org_id=grant.org_id,
                project_id=grant.project_id,
                grantee_plane_id=grant.grantee_plane_id,
                state=grant.state.value,
                expires_at=grant.expires_at,
                issued_at=grant.issued_at,
                data=_dump(grant),
            )
        )

    async def list_quota_grants(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        grantee_plane_id: str | None = None,
    ) -> list[QuotaGrantRecord]:
        statement = select(QuotaGrantRow)
        if org_id is not None:
            statement = statement.where(QuotaGrantRow.org_id == org_id)
        if project_id is not None:
            statement = statement.where(QuotaGrantRow.project_id == project_id)
        if grantee_plane_id is not None:
            statement = statement.where(QuotaGrantRow.grantee_plane_id == grantee_plane_id)
        statement = statement.order_by(QuotaGrantRow.issued_at, QuotaGrantRow.id)
        rows = (await self.session.scalars(statement)).all()
        return _decode_rows(QuotaGrantRecord, rows, table="quota_grants")

    async def get_managed_artifact(self, org_id: str, project_id: str, uri: str) -> ManagedArtifactRecord | None:
        row = await self._get_locked(ManagedArtifactRow, (org_id, project_id, uri))
        if row is None:
            return None
        record = ManagedArtifactRecord.model_validate(row.data)
        if not _managed_artifact_row_matches(row, record):
            logger.error("managed_artifacts typed lifecycle disagrees with its JSON record for %r", uri)
            return None
        return record

    async def put_managed_artifact(self, record: ManagedArtifactRecord) -> None:
        await self.session.merge(
            ManagedArtifactRow(
                uri=record.uri,
                org_id=record.org_id,
                project_id=record.project_id,
                kind=record.kind,
                state=record.state.value,
                record_generation=record.record_generation,
                object_version=record.object_version,
                size_bytes=record.size_bytes,
                created_at=record.created_at,
                updated_at=record.updated_at,
                data=_dump(record),
            )
        )

    async def list_managed_artifacts(
        self,
        *,
        org_id: str,
        project_id: str,
        kind: str | None = None,
        limit: int = 1000,
    ) -> list[ManagedArtifactRecord]:
        statement = select(ManagedArtifactRow).where(
            ManagedArtifactRow.org_id == org_id,
            ManagedArtifactRow.project_id == project_id,
        )
        if kind is not None:
            statement = statement.where(ManagedArtifactRow.kind == kind)
        statement = statement.order_by(ManagedArtifactRow.created_at, ManagedArtifactRow.uri).limit(limit)
        rows = (await self.session.scalars(statement)).all()
        records = _decode_managed_artifact_rows(rows)
        return [record for record in records if record.org_id == org_id and record.project_id == project_id]

    async def list_managed_artifacts_page(
        self,
        *,
        org_id: str,
        project_id: str,
        after_uri: str | None,
        limit: int,
    ) -> list[ManagedArtifactRecord]:
        statement = select(ManagedArtifactRow).where(
            ManagedArtifactRow.org_id == org_id,
            ManagedArtifactRow.project_id == project_id,
            ManagedArtifactRow.state != ManagedArtifactState.DELETED.value,
        )
        if after_uri is not None:
            statement = statement.where(ManagedArtifactRow.uri > after_uri)
        statement = statement.order_by(ManagedArtifactRow.uri).limit(limit)
        rows = (await self.session.scalars(statement)).all()
        records = _decode_managed_artifact_rows(rows)
        return [
            record
            for record in records
            if record.org_id == org_id
            and record.project_id == project_id
            and record.state is not ManagedArtifactState.DELETED
            and (after_uri is None or record.uri > after_uri)
        ]

    async def list_managed_artifact_projects(self, *, limit: int = 1000) -> list[tuple[str, str]]:
        statement = (
            select(ManagedArtifactRow.org_id, ManagedArtifactRow.project_id)
            .distinct()
            .order_by(ManagedArtifactRow.org_id, ManagedArtifactRow.project_id)
            .limit(limit)
        )
        return [(str(org_id), str(project_id)) for org_id, project_id in (await self.session.execute(statement)).all()]

    async def list_managed_artifacts_by_state(
        self,
        *,
        state: ManagedArtifactState,
        org_id: str | None = None,
        project_id: str | None = None,
        updated_before: datetime | None = None,
        limit: int = 100,
    ) -> list[ManagedArtifactRecord]:
        statement = select(ManagedArtifactRow).where(ManagedArtifactRow.state == state.value)
        if org_id is not None:
            statement = statement.where(ManagedArtifactRow.org_id == org_id)
        if project_id is not None:
            statement = statement.where(ManagedArtifactRow.project_id == project_id)
        if updated_before is not None:
            statement = statement.where(ManagedArtifactRow.updated_at <= updated_before)
        statement = statement.order_by(
            ManagedArtifactRow.updated_at,
            ManagedArtifactRow.org_id,
            ManagedArtifactRow.project_id,
            ManagedArtifactRow.uri,
        ).limit(limit)
        rows = (await self.session.scalars(statement)).all()
        records = _decode_managed_artifact_rows(rows)
        return [
            record
            for record in records
            if record.state is state
            and (org_id is None or record.org_id == org_id)
            and (project_id is None or record.project_id == project_id)
            and (updated_before is None or record.updated_at <= updated_before)
        ]

    async def sum_managed_artifact_bytes(self, *, org_id: str, project_id: str | None = None) -> int:
        statement = select(func.coalesce(func.sum(ManagedArtifactRow.size_bytes), 0)).where(
            ManagedArtifactRow.org_id == org_id,
            ManagedArtifactRow.state != ManagedArtifactState.DELETED.value,
        )
        if project_id is not None:
            statement = statement.where(ManagedArtifactRow.project_id == project_id)
        total = await self.session.scalar(statement)
        return int(total or 0)

    async def count_managed_artifacts(self, *, org_id: str, project_id: str | None = None) -> int:
        statement = (
            select(func.count())
            .select_from(ManagedArtifactRow)
            .where(
                ManagedArtifactRow.org_id == org_id,
                ManagedArtifactRow.state != ManagedArtifactState.DELETED.value,
            )
        )
        if project_id is not None:
            statement = statement.where(ManagedArtifactRow.project_id == project_id)
        total = await self.session.scalar(statement)
        return int(total or 0)

    async def sum_all_managed_artifact_bytes(self) -> int:
        total = await self.session.scalar(
            select(func.coalesce(func.sum(ManagedArtifactRow.size_bytes), 0)).where(
                ManagedArtifactRow.state != ManagedArtifactState.DELETED.value
            )
        )
        return int(total or 0)

    async def put_artifact_reference(self, record: ArtifactReferenceRecord) -> None:
        await self.session.merge(
            ArtifactReferenceRow(
                id=record.id,
                org_id=record.org_id,
                project_id=record.project_id,
                uri=record.uri,
                workload_id=record.workload_id,
                released_at=record.released_at,
                created_at=record.created_at,
                data=_dump(record),
            )
        )

    async def list_artifact_references(
        self,
        *,
        org_id: str,
        project_id: str,
        uri: str | None = None,
        workload_id: str | None = None,
        active_only: bool = False,
    ) -> list[ArtifactReferenceRecord]:
        statement = select(ArtifactReferenceRow).where(
            ArtifactReferenceRow.org_id == org_id,
            ArtifactReferenceRow.project_id == project_id,
        )
        if uri is not None:
            statement = statement.where(ArtifactReferenceRow.uri == uri)
        if workload_id is not None:
            statement = statement.where(ArtifactReferenceRow.workload_id == workload_id)
        if active_only:
            statement = statement.where(ArtifactReferenceRow.released_at.is_(None))
        statement = statement.order_by(ArtifactReferenceRow.created_at, ArtifactReferenceRow.id)
        rows = (await self.session.scalars(statement)).all()
        records = _decode_rows(ArtifactReferenceRecord, rows, table="artifact_references")
        return [record for record in records if record.org_id == org_id and record.project_id == project_id]

    async def get_idempotency(self, scope: str, operation: str, key: str) -> IdempotencyRecord | None:
        lock_key = f"{scope}\x1f{operation}\x1f{key}"
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
            {"lock_key": lock_key},
        )
        statement = select(IdempotencyRow).where(
            IdempotencyRow.scope == scope,
            IdempotencyRow.operation == operation,
            IdempotencyRow.key == key,
        )
        row = (await self.session.scalars(statement)).one_or_none()
        return None if row is None else IdempotencyRecord.model_validate(row.data)

    async def put_idempotency(self, record: IdempotencyRecord) -> None:
        existing = await self.get_idempotency(record.scope, record.operation, record.key)
        if existing is not None:
            return
        self.session.add(
            IdempotencyRow(
                scope=record.scope,
                operation=record.operation,
                key=record.key,
                created_at=record.created_at,
                data=_dump(record),
            )
        )

    async def get_outbox(self, record_id: str) -> OutboxRecord | None:
        row = await self._get_locked(OutboxRow, record_id)
        return None if row is None else OutboxRecord.model_validate(row.data)

    async def put_outbox(self, record: OutboxRecord) -> None:
        await self.session.merge(
            OutboxRow(
                id=record.id,
                state=record.state.value,
                available_at=record.available_at,
                claimed_until=record.claimed_until,
                updated_at=record.updated_at,
                data=_dump(record),
            )
        )

    async def list_outbox(self, *, state: OutboxState | None = None, limit: int = 100) -> list[OutboxRecord]:
        statement = select(OutboxRow)
        if state is not None:
            statement = statement.where(OutboxRow.state == state.value)
        rows = (await self.session.scalars(statement.order_by(OutboxRow.available_at, OutboxRow.id).limit(limit))).all()
        return _decode_rows(OutboxRecord, rows, table="outbox")

    async def list_outbox_for_aggregates(
        self,
        aggregate_type: str,
        aggregate_generations: tp.Mapping[str, int],
        topic: str | None = None,
    ) -> list[OutboxRecord]:
        """Load every current-generation command for the supplied aggregates."""

        if not aggregate_generations:
            return []
        aggregate_ids = tuple(aggregate_generations)
        statement = select(OutboxRow).where(
            OutboxRow.data["aggregate_type"].as_string() == aggregate_type,
            OutboxRow.data["aggregate_id"].as_string().in_(aggregate_ids),
        )
        if topic is not None:
            statement = statement.where(OutboxRow.data["topic"].as_string() == topic)
        rows = (await self.session.scalars(statement.order_by(OutboxRow.available_at, OutboxRow.id))).all()
        records = _decode_rows(OutboxRecord, rows, table="outbox")
        return [
            record
            for record in records
            if record.aggregate_id in aggregate_generations
            and record.generation == aggregate_generations[record.aggregate_id]
        ]

    async def prune_outbox(self, *, before: datetime, limit: int = 1000) -> int:
        """Delete settled outbox commands last updated before ``before``.

        Only SUCCEEDED and DISCARDED rows are eligible; both are absorbing, so
        removal cannot drop a command still owed delivery.

        The cutoff is a WHERE clause on the same indexed column the page is
        ordered by, so every row the LIMIT returns is deletable. Applying it
        after the LIMIT instead -- against ``updated_at`` inside the JSON blob,
        while paging by ``available_at`` -- could return 0 forever: a batch of
        long-queued, recently-settled rows fills the window and the genuinely
        old rows behind them are never reached.
        """

        statement = (
            select(OutboxRow)
            .where(
                OutboxRow.state.in_(tuple(state.value for state in _PRUNABLE_OUTBOX_STATES)),
                OutboxRow.updated_at.is_not(None),
                OutboxRow.updated_at < before,
            )
            .order_by(OutboxRow.updated_at)
            .limit(limit)
        )
        removed = 0
        for row in (await self.session.scalars(statement)).all():
            await self.session.delete(row)
            removed += 1
        return removed

    async def prune_events(self, *, before: datetime, limit: int = 1000) -> int:
        """Delete audit events emitted before ``before``, oldest first."""

        statement = select(AuditEventRow).order_by(AuditEventRow.sequence).limit(limit)
        removed = 0
        for row in (await self.session.scalars(statement)).all():
            if AuditEvent.model_validate(row.data).timestamp < before:
                await self.session.delete(row)
                removed += 1
        return removed

    async def prune_idempotency(self, *, before: datetime, limit: int = 1000) -> int:
        """Delete idempotency bindings created before ``before``, oldest first.

        Filtered and paged on the same indexed column. Paging by the surrogate
        ``id`` -- which has no relationship to creation time -- while filtering
        on a value inside the JSON blob meant an expired key past the LIMIT was
        never reached, so it stayed honored forever: exactly the bug retention
        exists to fix.
        """

        statement = (
            select(IdempotencyRow)
            .where(IdempotencyRow.created_at.is_not(None), IdempotencyRow.created_at < before)
            .order_by(IdempotencyRow.created_at)
            .limit(limit)
        )
        removed = 0
        for row in (await self.session.scalars(statement)).all():
            await self.session.delete(row)
            removed += 1
        return removed

    async def append_event(self, event: AuditEvent) -> AuditEvent:
        data = _dump(event)
        data["sequence"] = 0
        sequence: int | None = None
        if self.session.get_bind().dialect.name == "sqlite":
            # SQLite auto-increments only a literal INTEGER PRIMARY KEY, while
            # production uses BIGSERIAL/BIGINT. Its single-writer lock makes a
            # max+1 allocation sufficient for the executable SQL contract tests;
            # PostgreSQL keeps the database-owned sequence and concurrency proof.
            sequence = int(
                await self.session.scalar(select(func.coalesce(func.max(AuditEventRow.sequence), 0) + 1)) or 1
            )
        row = AuditEventRow(
            sequence=sequence,
            id=event.id,
            org_id=event.org_id,
            project_id=event.project_id,
            entity_type=event.entity_type,
            entity_id=event.entity_id,
            occurred_at=event.timestamp,
            data=data,
        )
        self.session.add(row)
        await self.session.flush()
        stored = event.model_copy(update={"sequence": row.sequence})
        row.data = _dump(stored)
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
        statement = select(AuditEventRow).where(AuditEventRow.sequence > after_sequence)
        if org_id is not None:
            statement = statement.where(AuditEventRow.org_id == org_id)
        if project_id is not None:
            statement = statement.where(AuditEventRow.project_id == project_id)
        if entity_type is not None:
            statement = statement.where(AuditEventRow.entity_type == entity_type)
        if entity_id is not None:
            statement = statement.where(AuditEventRow.entity_id == entity_id)
        if occurred_at_or_after is not None:
            statement = statement.where(AuditEventRow.occurred_at >= occurred_at_or_after)
        if occurred_before is not None:
            statement = statement.where(AuditEventRow.occurred_at < occurred_before)
        ordering = AuditEventRow.sequence.desc() if newest_first else AuditEventRow.sequence
        rows = (await self.session.scalars(statement.order_by(ordering).limit(limit))).all()
        return _decode_rows(AuditEvent, rows, table="audit_events")


class SqlAlchemyRepository:
    """Production repository; each service operation commits as one DB transaction."""

    def __init__(self, engine: AsyncEngine):
        self.engine = engine.execution_options(isolation_level="SERIALIZABLE")
        self._sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        # Outbox claiming runs at READ COMMITTED, alone among the repository's
        # writes. ``FOR UPDATE SKIP LOCKED`` *is* the serialization here: each
        # row is locked before it is updated and rows another claimer holds are
        # skipped, so two workers cannot claim one command. Layering
        # SERIALIZABLE on top adds no safety and does add spurious 40001s that
        # scale with replica count -- measured against PostgreSQL 17, four
        # concurrent claimers failed on the first try. Active-active delivery is
        # the entire point of the claim, so making it fail more often the more
        # replicas you run is exactly backwards.
        self._claim_sessions = async_sessionmaker(
            engine.execution_options(isolation_level="READ COMMITTED"),
            expire_on_commit=False,
        )

    async def open(self) -> None:
        """Refuse to serve a database whose schema this build does not match.

        ``create_schema`` stamps ``schema_revisions`` on the development path, but the
        Alembic migrations only *create* that table -- they never insert a row -- so a
        production database's only truthful stamp is ``alembic_version``. Both are
        consulted, Alembic first.

        An unstamped database is accepted: that is a fresh one, and either
        ``auto_create_schema`` or ``eyvan-migrate`` is about to initialize it. Only a
        stamp that *disagrees* is fatal, which is what turns "this build has four
        migrations the database has never seen" from a silent wrong answer into a
        startup failure.
        """

        async with self.engine.connect() as connection:
            stamped = await self._stamped_revision(connection)
        if stamped is not None and stamped != SCHEMA_REVISION:
            raise SchemaRevisionMismatch(
                f"database schema is at revision {stamped!r} but this build expects "
                f"{SCHEMA_REVISION!r}; run `eyvan-migrate upgrade head` (or deploy the matching build)"
            )

    @staticmethod
    async def _stamped_revision(connection: AsyncConnection) -> str | None:
        """Return the database's own record of its schema revision, if it has one."""

        for statement in (
            text("SELECT version_num FROM alembic_version LIMIT 1"),
            text("SELECT revision FROM schema_revisions LIMIT 1"),
        ):
            try:
                revision = (await connection.execute(statement)).scalar_one_or_none()
            except DBAPIError:
                # The table does not exist on this database; try the next source.
                continue
            if revision is not None:
                return str(revision)
        return None

    async def ping(self) -> None:
        """Confirm this replica can still reach its database, right now.

        A real round trip rather than a pool-state inspection: a connection can
        be checked out and dead, and the only reliable question is whether the
        server answers.

        Deliberately cheap. This runs on every readiness probe of every replica,
        so anything heavier than ``SELECT 1`` would put a load on the database
        proportional to how closely the fleet is being watched.
        """

        async with self.engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def close(self) -> None:
        """Keep engine disposal owned by the API factory that created it."""

        return None

    @staticmethod
    def _translated(exc: DBAPIError) -> BaseException:
        """Return the typed error for a database failure callers can act on.

        A raw ``DBAPIError`` tells a caller nothing: a unique-violation race and
        a broken connection arrive as the same class, so either everything is
        retried or nothing is. Only these three SQLSTATEs mean "the same call
        would likely succeed now".
        """

        sqlstate = _transaction_sqlstate(exc.orig)
        if sqlstate in {"23505", "40001", "40P01"}:
            return TransientRepositoryError(f"retryable PostgreSQL transaction conflict ({sqlstate})")
        return exc

    @asynccontextmanager
    async def transaction(self) -> tp.AsyncIterator[SqlAlchemySession]:
        try:
            async with self._sessions.begin() as session:
                yield SqlAlchemySession(session)
        except DBAPIError as exc:
            translated = self._translated(exc)
            if translated is exc:
                raise
            raise translated from exc

    async def wait_for_events(
        self,
        *,
        after_sequence: int,
        org_id: str | None = None,
        project_id: str | None = None,
        timeout: float | None = None,
    ) -> list[AuditEvent]:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            async with self.transaction() as tx:
                events = await tx.list_events(
                    after_sequence=after_sequence,
                    org_id=org_id,
                    project_id=project_id,
                )
            if events:
                return events
            if deadline is not None and time.monotonic() >= deadline:
                return []
            await asyncio.sleep(0.25)

    async def claim_outbox(
        self,
        *,
        worker_id: str,
        limit: int = 10,
        lease_seconds: int = 60,
    ) -> list[OutboxRecord]:
        """Claim commands with ``FOR UPDATE SKIP LOCKED`` for active-active workers."""

        return await self._claim_outbox(
            worker_id=worker_id,
            limit=limit,
            lease_seconds=lease_seconds,
            candidate_id=None,
        )

    async def snapshot_claimable_outbox_ids(self, *, limit: int = 10) -> tuple[str, ...]:
        """Capture the current ordered eligible outbox identifiers without leasing them."""

        now = utcnow()
        statement = (
            select(OutboxRow.id)
            .where(_claimable_outbox_clause(now))
            .order_by(OutboxRow.available_at, OutboxRow.id)
            .limit(limit)
        )
        async with self._sessions.begin() as session:
            return tuple((await session.scalars(statement)).all())

    async def claim_outbox_id(
        self,
        *,
        worker_id: str,
        command_id: str,
        lease_seconds: int = 60,
    ) -> OutboxRecord | None:
        """Claim one specific command if it remains eligible at claim time."""

        claimed = await self._claim_outbox(
            worker_id=worker_id,
            limit=1,
            lease_seconds=lease_seconds,
            candidate_id=command_id,
        )
        return claimed[0] if claimed else None

    async def _claim_outbox(
        self,
        *,
        worker_id: str,
        limit: int,
        lease_seconds: int,
        candidate_id: str | None,
    ) -> list[OutboxRecord]:
        """Claim eligible commands, optionally restricting delivery to one snapshotted identifier."""

        now = utcnow()
        statement = select(OutboxRow).where(_claimable_outbox_clause(now))
        if candidate_id is not None:
            statement = statement.where(OutboxRow.id == candidate_id)
        statement = statement.order_by(OutboxRow.available_at, OutboxRow.id)
        statement = statement.with_for_update(skip_locked=True).limit(limit)
        claimed: list[OutboxRecord] = []
        try:
            async with self._claim_sessions.begin() as session:
                rows = (await session.scalars(statement)).all()
                for row in rows:
                    record = OutboxRecord.model_validate(row.data)
                    updated = record.model_copy(
                        update={
                            "state": OutboxState.PROCESSING,
                            "attempts": record.attempts + 1,
                            "claimed_by": worker_id,
                            "claimed_until": now + timedelta(seconds=lease_seconds),
                            "updated_at": now,
                        }
                    )
                    row.state = updated.state.value
                    row.claimed_until = updated.claimed_until
                    row.data = _dump(updated)
                    claimed.append(updated)
        except DBAPIError as exc:
            # This path used to bypass the translation entirely, so a conflict
            # here reached the outbox loop as a raw DBAPIError, was swallowed by
            # its broad handler, and cost the whole batch that cycle with only a
            # stack trace to show for it.
            translated = self._translated(exc)
            if translated is exc:
                raise
            raise translated from exc
        return claimed


def _stamp_alembic_head(connection: Connection) -> None:
    """Record the head revision in ``alembic_version``, as ``alembic stamp head`` would.

    Without this, a database bootstrapped by ``create_schema`` could never be
    migrated: Alembic would find no version row, conclude the database is empty,
    and replay every migration from ``0002`` against tables that already exist.
    The default was ``False`` and the shipped manifest sets it to ``"false"``, so
    the documented path avoided the trap -- which is exactly what made it a trap,
    since nothing said so.

    The revision comes from the migration scripts rather than a constant, so the
    two stamps cannot drift apart.
    """

    from alembic.migration import MigrationContext
    from alembic.script import ScriptDirectory

    from eyvan.server.persistence.migrate import build_alembic_config

    head = ScriptDirectory.from_config(build_alembic_config()).get_current_head()
    if head is None:
        raise RuntimeError("packaged Alembic migrations declare no head revision")
    if head != SCHEMA_REVISION:
        raise RuntimeError(
            f"packaged migrations are at {head!r} but this build declares SCHEMA_REVISION "
            f"{SCHEMA_REVISION!r}; the two stamps would disagree"
        )
    context = MigrationContext.configure(connection)
    context._ensure_version_table()
    context.stamp(ScriptDirectory.from_config(build_alembic_config()), head)


async def create_schema(engine: AsyncEngine) -> None:
    """Create the current schema and stamp its source revision for development.

    Both stamps, not one. ``schema_revisions`` is what this build reads back at
    startup; ``alembic_version`` is what a later ``eyvan-migrate upgrade head``
    reads, and a database missing it is one no migration can ever be applied to.
    """

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        await connection.run_sync(_stamp_alembic_head)
    async with async_sessionmaker(engine).begin() as session:
        await session.merge(SchemaRevisionRow(revision=SCHEMA_REVISION))
