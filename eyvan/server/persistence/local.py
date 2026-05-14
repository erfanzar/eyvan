# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Durable single-machine JSON repository for the local Eyvan control plane."""

from __future__ import annotations

import asyncio
import errno
import json
import os
import stat
import tempfile
import typing as tp
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from eyvan._internal.json_types import is_json_object
from eyvan.records import (
    AuditEvent,
    BudgetRecord,
    CapacityPoolRecord,
    CellRecord,
    IdempotencyRecord,
    LeaseRecord,
    OrganizationRecord,
    OutboxRecord,
    OutboxState,
    PolicyRecord,
    ProjectRecord,
    QuotaRecord,
    RoleGrantRecord,
    WorkloadRecord,
    utcnow,
)
from eyvan.server.local.profile import DEFAULT_LOCAL_CONTROL_PLANE_PATH
from eyvan.server.persistence.state import ControlPlaneState, StateRepository, StateSession

LOCAL_CONTROL_PLANE_SCHEMA = "eyvan.control-plane"
LOCAL_CONTROL_PLANE_SCHEMA_VERSION = 1


class LocalControlPlaneError(RuntimeError):
    """Base error raised by the owner-only local control-plane store."""


class LocalControlPlaneSchemaError(LocalControlPlaneError):
    """The local state document is malformed, foreign, or unsupported."""


class LocalControlPlaneInUseError(LocalControlPlaneError):
    """Another local Eyvan server already owns the configured state file."""


class LocalControlPlaneDocument(BaseModel):
    """Versioned JSON representation of every durable control-plane aggregate."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False, populate_by_name=True)

    document_schema: Literal["eyvan.control-plane"] = Field(
        default=LOCAL_CONTROL_PLANE_SCHEMA,
        alias="schema",
        serialization_alias="schema",
    )
    version: Literal[1] = LOCAL_CONTROL_PLANE_SCHEMA_VERSION
    revision: int = Field(default=0, ge=0)
    workloads: tuple[WorkloadRecord, ...] = ()
    cells: tuple[CellRecord, ...] = ()
    capacity_pools: tuple[CapacityPoolRecord, ...] = ()
    leases: tuple[LeaseRecord, ...] = ()
    policies: tuple[PolicyRecord, ...] = ()
    quotas: tuple[QuotaRecord, ...] = ()
    budgets: tuple[BudgetRecord, ...] = ()
    role_grants: tuple[RoleGrantRecord, ...] = ()
    organizations: tuple[OrganizationRecord, ...] = ()
    projects: tuple[ProjectRecord, ...] = ()
    idempotency: tuple[IdempotencyRecord, ...] = ()
    outbox: tuple[OutboxRecord, ...] = ()
    events: tuple[AuditEvent, ...] = ()
    next_event_sequence: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def validate_document(self) -> LocalControlPlaneDocument:
        """Reject documents that would lose aggregate identity during restoration."""

        if len({record.id for record in self.workloads}) != len(self.workloads):
            raise ValueError("local control-plane workloads must have unique IDs")
        if len({record.id for record in self.cells}) != len(self.cells):
            raise ValueError("local control-plane cells must have unique IDs")
        if len({record.id for record in self.capacity_pools}) != len(self.capacity_pools):
            raise ValueError("local control-plane capacity pools must have unique IDs")
        if len({record.id for record in self.leases}) != len(self.leases):
            raise ValueError("local control-plane leases must have unique IDs")
        if len({(record.org_id, record.project_id) for record in self.policies}) != len(self.policies):
            raise ValueError("local control-plane policies must have unique organization/project scopes")
        if len({(record.org_id, record.project_id, record.principal_id) for record in self.quotas}) != len(self.quotas):
            raise ValueError("local control-plane quotas must have unique hierarchical scopes")
        if len({(record.org_id, record.project_id) for record in self.budgets}) != len(self.budgets):
            raise ValueError("local control-plane budgets must have unique organization/project scopes")
        if len({record.key for record in self.role_grants}) != len(self.role_grants):
            raise ValueError("local control-plane role grants must have unique subject/role/scope keys")
        if len({record.id for record in self.organizations}) != len(self.organizations):
            raise ValueError("local control-plane organizations must have unique IDs")
        if len({record.key for record in self.projects}) != len(self.projects):
            raise ValueError("local control-plane projects must have unique organization/project scopes")
        if len({(record.scope, record.operation, record.key) for record in self.idempotency}) != len(self.idempotency):
            raise ValueError("local control-plane idempotency records must have unique scopes")
        if len({record.id for record in self.outbox}) != len(self.outbox):
            raise ValueError("local control-plane outbox records must have unique IDs")
        # Strictly increasing, not contiguous-from-1. What this invariant protects
        # is reader ordering and sequence non-reuse: `list_events` filters on
        # `sequence > cursor` and never assumes contiguity. Requiring
        # `1..len(events)` additionally made retention structurally impossible --
        # pruning the oldest events left a valid document that failed to load.
        sequences = [event.sequence for event in self.events]
        if any(sequence < 1 for sequence in sequences):
            raise ValueError("local control-plane audit event sequences must be positive")
        if sequences != sorted(sequences) or len(set(sequences)) != len(sequences):
            raise ValueError("local control-plane audit event sequences must be strictly increasing")
        if self.next_event_sequence <= (sequences[-1] if sequences else 0):
            raise ValueError("local control-plane next_event_sequence must exceed the final audit event")
        return self

    @classmethod
    def from_state(cls, state: ControlPlaneState) -> LocalControlPlaneDocument:
        """Encode one in-process aggregate snapshot in deterministic order."""

        return cls(
            revision=state.revision,
            workloads=tuple(sorted(state.workloads.values(), key=lambda record: record.id)),
            cells=tuple(sorted(state.cells.values(), key=lambda record: record.id)),
            capacity_pools=tuple(sorted(state.capacity_pools.values(), key=lambda record: record.id)),
            leases=tuple(sorted(state.leases.values(), key=lambda record: record.id)),
            policies=tuple(sorted(state.policies.values(), key=lambda record: (record.org_id, record.project_id))),
            quotas=tuple(
                sorted(
                    state.quotas.values(),
                    key=lambda record: (record.org_id, record.project_id or "", record.principal_id or ""),
                )
            ),
            budgets=tuple(sorted(state.budgets.values(), key=lambda record: (record.org_id, record.project_id))),
            role_grants=tuple(sorted(state.role_grants.values(), key=lambda record: record.key)),
            organizations=tuple(sorted(state.organizations.values(), key=lambda record: record.id)),
            projects=tuple(sorted(state.projects.values(), key=lambda record: record.key)),
            idempotency=tuple(
                sorted(state.idempotency.values(), key=lambda record: (record.scope, record.operation, record.key))
            ),
            outbox=tuple(sorted(state.outbox.values(), key=lambda record: record.id)),
            events=tuple(state.events),
            next_event_sequence=state.next_event_sequence,
        )

    def to_state(self) -> ControlPlaneState:
        """Restore the typed mutable representation used by transactional sessions."""

        return ControlPlaneState(
            revision=self.revision,
            workloads={record.id: record for record in self.workloads},
            cells={record.id: record for record in self.cells},
            capacity_pools={record.id: record for record in self.capacity_pools},
            leases={record.id: record for record in self.leases},
            policies={(record.org_id, record.project_id): record for record in self.policies},
            quotas={
                (record.org_id, record.project_id or "", record.principal_id or ""): record for record in self.quotas
            },
            budgets={(record.org_id, record.project_id): record for record in self.budgets},
            role_grants={record.key: record for record in self.role_grants},
            organizations={record.id: record for record in self.organizations},
            projects={record.key: record for record in self.projects},
            idempotency={(record.scope, record.operation, record.key): record for record in self.idempotency},
            outbox={record.id: record for record in self.outbox},
            events=list(self.events),
            next_event_sequence=self.next_event_sequence,
        )


class LocalControlPlaneStore:
    """Read and atomically replace one owner-only local JSON control-plane document."""

    def __init__(self, path: Path | str = DEFAULT_LOCAL_CONTROL_PLANE_PATH):
        self.path = Path(path).expanduser()

    def load(self) -> ControlPlaneState:
        """Load the durable snapshot, returning an empty state before first use."""

        if self._existing_metadata() is None:
            return ControlPlaneState()
        try:
            raw = json.loads(self._read_text())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            message = f"could not read local Eyvan control-plane state {self.path}: {exc}"
            raise LocalControlPlaneSchemaError(message) from exc
        if not is_json_object(raw):
            raise LocalControlPlaneSchemaError(f"local Eyvan control-plane state {self.path} must be a JSON object")
        try:
            document = LocalControlPlaneDocument.model_validate(raw)
        except ValidationError as exc:
            raise LocalControlPlaneSchemaError(f"local Eyvan control-plane state {self.path} is invalid: {exc}") from exc
        return document.to_state()

    def save(self, state: ControlPlaneState) -> None:
        """Durably replace the state document only after it passes schema validation."""

        self._ensure_parent()
        self._existing_metadata()
        document = LocalControlPlaneDocument.from_state(state).model_dump(mode="json", by_alias=True)
        if not is_json_object(document):
            raise LocalControlPlaneError("local Eyvan control-plane state did not serialize to a JSON object")
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                json.dump(document, temporary, indent=2, sort_keys=True)
                temporary.write("\n")
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, self.path)
            temporary_path = None
            self._fsync_parent()
        except OSError as exc:
            message = f"could not persist local Eyvan control-plane state {self.path}: {exc}"
            raise LocalControlPlaneError(message) from exc
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def _ensure_parent(self) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass

    def _existing_metadata(self) -> os.stat_result | None:
        try:
            metadata = self.path.lstat()
        except FileNotFoundError:
            return None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise LocalControlPlaneSchemaError(f"local Eyvan control-plane state {self.path} must be a regular file")
        if stat.S_IMODE(metadata.st_mode) & 0o077:
            raise LocalControlPlaneSchemaError(
                f"local Eyvan control-plane state {self.path} must not be group- or world-readable"
            )
        return metadata

    def _read_text(self) -> str:
        descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise LocalControlPlaneSchemaError(f"local Eyvan control-plane state {self.path} must be a regular file")
            if stat.S_IMODE(metadata.st_mode) & 0o077:
                raise LocalControlPlaneSchemaError(
                    f"local Eyvan control-plane state {self.path} must not be group- or world-readable"
                )
            chunks: list[bytes] = []
            while chunk := os.read(descriptor, 65_536):
                chunks.append(chunk)
        finally:
            os.close(descriptor)
        return b"".join(chunks).decode("utf-8")

    def _fsync_parent(self) -> None:
        descriptor = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


class LocalControlPlaneLease:
    """Lifetime advisory lock that limits one local JSON store to one server process."""

    def __init__(self, state_path: Path):
        self.path = state_path.with_suffix(state_path.suffix + ".lock")
        self._handle: tp.TextIO | None = None

    def acquire(self) -> None:
        """Acquire the non-blocking process lock or report the existing server."""

        if self._handle is not None:
            return
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        try:
            descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        except OSError as exc:
            raise LocalControlPlaneError(f"could not open local Eyvan control-plane lock {self.path}: {exc}") from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise LocalControlPlaneError(f"local Eyvan control-plane lock {self.path} must be a regular file")
            os.fchmod(descriptor, 0o600)
            handle = os.fdopen(descriptor, "a+", encoding="utf-8")
        except BaseException:
            os.close(descriptor)
            raise
        import fcntl

        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            if exc.errno in {errno.EACCES, errno.EAGAIN}:
                raise LocalControlPlaneInUseError(
                    f"local Eyvan control-plane state {self.path.with_suffix('')} is already owned by another server"
                ) from None
            raise LocalControlPlaneError(f"could not lock local Eyvan control-plane state {self.path}: {exc}") from exc
        self._handle = handle

    def release(self) -> None:
        """Release the server process's local-state ownership lock."""

        handle = self._handle
        if handle is None:
            return
        import fcntl

        try:
            fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()
            self._handle = None


class LocalJsonSession(StateSession):
    """Transaction-local state view for :class:`LocalJsonRepository`."""


class LocalJsonRepository(StateRepository):
    """Durable single-server repository for the zero-configuration loopback API."""

    session_type = LocalJsonSession

    def __init__(self, path: Path | str = DEFAULT_LOCAL_CONTROL_PLANE_PATH):
        super().__init__()
        self.store = LocalControlPlaneStore(path)
        self._lease = LocalControlPlaneLease(self.store.path)
        self._opened = False
        self._lifecycle_lock = asyncio.Lock()

    async def open(self) -> None:
        """Acquire exclusive local ownership and restore durable state once per process lifecycle."""

        async with self._lifecycle_lock:
            if self._opened:
                return
            self._lease.acquire()
            try:
                self._state = self.store.load()
                if self._recover_interrupted_outbox_claims():
                    self.store.save(self._state)
                self._opened = True
            except BaseException:
                self._lease.release()
                raise

    async def close(self) -> None:
        """Release local ownership so a subsequent server can reopen the same state."""

        async with self._lifecycle_lock:
            if not self._opened:
                return
            self._opened = False
            self._lease.release()

    def _persist(self, state: ControlPlaneState) -> None:
        """Commit one transaction snapshot to the owner-only state document."""

        if not self._opened:
            raise LocalControlPlaneError("local Eyvan control-plane repository is not open")
        self.store.save(state)

    def _recover_interrupted_outbox_claims(self) -> bool:
        """Make commands claimed by a terminated local server immediately eligible for fenced retry."""

        now = utcnow()
        recovered = False
        for record_id, record in tuple(self._state.outbox.items()):
            if record.state is not OutboxState.PROCESSING:
                continue
            self._state.outbox[record_id] = record.model_copy(
                update={
                    "state": OutboxState.PENDING,
                    "claimed_by": None,
                    "claimed_until": None,
                    "available_at": now,
                    "updated_at": now,
                }
            )
            recovered = True
        if recovered:
            self._state.revision += 1
        return recovered


__all__ = (
    "LOCAL_CONTROL_PLANE_SCHEMA",
    "LOCAL_CONTROL_PLANE_SCHEMA_VERSION",
    "LocalControlPlaneDocument",
    "LocalControlPlaneError",
    "LocalControlPlaneInUseError",
    "LocalControlPlaneLease",
    "LocalControlPlaneSchemaError",
    "LocalControlPlaneStore",
    "LocalJsonRepository",
    "LocalJsonSession",
)
