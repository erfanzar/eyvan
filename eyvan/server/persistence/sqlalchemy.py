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
    Connection,
    DateTime,
    Integer,
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
    IdempotencyRecord,
    LeaseRecord,
    ManagedArtifactRecord,
    OutboxRecord,
    OutboxState,
    PolicyRecord,
    QuotaRecord,
    UserProgressReport,
    WorkloadRecord,
    WorkloadState,
    utcnow,
)
from eyvan.server.persistence.base import SchemaRevisionMismatch, TransientRepositoryError
from eyvan.server.service.federation import BudgetGrantRecord, PlanePeerRecord

SCHEMA_REVISION = "0008_progress_reports"


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


class ProgressReportRow(Base):
    __tablename__ = "progress_reports"
    #: Composite key, so a new report for a rank overwrites rather than appends.
    #: Unbounded per-rank history is the growth this design refuses.
    workload_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    rank: Mapped[int] = mapped_column(Integer, primary_key=True)
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


class ManagedArtifactRow(Base):
    __tablename__ = "managed_artifacts"
    #: The URI is the natural key: the store addresses objects by it, and for
    #: content-addressed kinds it already encodes the digest, so a re-upload
    #: merges onto the same row rather than double-counting its bytes.
    uri: Mapped[str] = mapped_column(String(512), primary_key=True)
    org_id: Mapped[str | None] = mapped_column(String(128), index=True)
    project_id: Mapped[str | None] = mapped_column(String(128), index=True)
    kind: Mapped[str | None] = mapped_column(String(32), index=True)
    #: A real column, not a field inside ``data``: the quota check SUMs it on
    #: the upload path, and a JSON extraction cannot use an index.
    size_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


class ArtifactReferenceRow(Base):
    __tablename__ = "artifact_references"
    id: Mapped[str] = mapped_column(String(200), primary_key=True)
    uri: Mapped[str] = mapped_column(String(512), index=True)
    workload_id: Mapped[str] = mapped_column(String(96), index=True)
    #: Indexed because the only interesting query is "does this URI still have
    #: an unreleased reference", and that must not scan released history.
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    data: Mapped[JsonObject] = mapped_column(JSON)


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
    sequence: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    id: Mapped[str] = mapped_column(String(96), unique=True, nullable=False)
    org_id: Mapped[str | None] = mapped_column(String(128), index=True)
    project_id: Mapped[str | None] = mapped_column(String(128), index=True)
    entity_type: Mapped[str] = mapped_column(String(64), index=True)
    entity_id: Mapped[str] = mapped_column(String(256), index=True)
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

    async def get_managed_artifact(self, uri: str) -> ManagedArtifactRecord | None:
        row = await self._get_locked(ManagedArtifactRow, uri)
        return None if row is None else ManagedArtifactRecord.model_validate(row.data)

    async def put_managed_artifact(self, record: ManagedArtifactRecord) -> None:
        await self.session.merge(
            ManagedArtifactRow(
                uri=record.uri,
                org_id=record.org_id,
                project_id=record.project_id,
                kind=record.kind,
                size_bytes=record.size_bytes,
                created_at=record.created_at,
                data=_dump(record),
            )
        )

    async def list_managed_artifacts(
        self,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        kind: str | None = None,
        limit: int = 1000,
    ) -> list[ManagedArtifactRecord]:
        statement = select(ManagedArtifactRow)
        if org_id is not None:
            statement = statement.where(ManagedArtifactRow.org_id == org_id)
        if project_id is not None:
            statement = statement.where(ManagedArtifactRow.project_id == project_id)
        if kind is not None:
            statement = statement.where(ManagedArtifactRow.kind == kind)
        statement = statement.order_by(ManagedArtifactRow.created_at, ManagedArtifactRow.uri).limit(limit)
        rows = (await self.session.scalars(statement)).all()
        return _decode_rows(ManagedArtifactRecord, rows, table="managed_artifacts")

    async def sum_managed_artifact_bytes(self, *, org_id: str, project_id: str | None = None) -> int:
        statement = select(func.coalesce(func.sum(ManagedArtifactRow.size_bytes), 0)).where(
            ManagedArtifactRow.org_id == org_id
        )
        if project_id is not None:
            statement = statement.where(ManagedArtifactRow.project_id == project_id)
        total = await self.session.scalar(statement)
        return int(total or 0)

    async def put_artifact_reference(self, record: ArtifactReferenceRecord) -> None:
        await self.session.merge(
            ArtifactReferenceRow(
                id=record.id,
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
        uri: str | None = None,
        workload_id: str | None = None,
        active_only: bool = False,
    ) -> list[ArtifactReferenceRecord]:
        statement = select(ArtifactReferenceRow)
        if uri is not None:
            statement = statement.where(ArtifactReferenceRow.uri == uri)
        if workload_id is not None:
            statement = statement.where(ArtifactReferenceRow.workload_id == workload_id)
        if active_only:
            statement = statement.where(ArtifactReferenceRow.released_at.is_(None))
        statement = statement.order_by(ArtifactReferenceRow.created_at, ArtifactReferenceRow.id)
        rows = (await self.session.scalars(statement)).all()
        return _decode_rows(ArtifactReferenceRecord, rows, table="artifact_references")

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
        row = AuditEventRow(
            id=event.id,
            org_id=event.org_id,
            project_id=event.project_id,
            entity_type=event.entity_type,
            entity_id=event.entity_id,
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
        rows = (await self.session.scalars(statement.order_by(AuditEventRow.sequence).limit(limit))).all()
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
