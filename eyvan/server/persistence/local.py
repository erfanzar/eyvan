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
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from eyvan._internal.json_types import is_json_object
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
    utcnow,
)
from eyvan.server.local.profile import DEFAULT_LOCAL_CONTROL_PLANE_PATH
from eyvan.server.persistence.state import ControlPlaneState, StateRepository, StateSession
from eyvan.server.service.federation import BudgetGrantRecord, PlanePeerRecord, QuotaGrantRecord

LOCAL_CONTROL_PLANE_SCHEMA = "eyvan.control-plane"
LOCAL_CONTROL_PLANE_SCHEMA_VERSION = 2
LOCAL_CONTROL_PLANE_V1_BACKUP_SUFFIX = ".v1.bak"
LOCAL_CONTROL_PLANE_MAX_BYTES = 64 * 1024 * 1024
_LOCAL_CONTROL_PLANE_MAX_INTEGER_DIGITS = 128
_LOCAL_CONTROL_PLANE_MAX_JSON_DEPTH = 100


class LocalControlPlaneError(RuntimeError):
    """Base error raised by the owner-only local control-plane store."""


class LocalControlPlaneSchemaError(LocalControlPlaneError):
    """The local state document is malformed, foreign, or unsupported."""


class LocalControlPlaneAmbiguousCommitError(LocalControlPlaneError):
    """A state pathname was replaced but its directory entry could not be confirmed durable."""


class LocalControlPlaneInUseError(LocalControlPlaneError):
    """Another local Eyvan server already owns the configured state file."""


class LocalProgressReportDocument(BaseModel):
    """One workload/rank identity and its latest durable progress report."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    workload_id: str = Field(min_length=1, max_length=96)
    report: UserProgressReport


class _LocalControlPlaneDocumentBase(BaseModel):
    """Aggregates common to the v1 and v2 local document schemas."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False, populate_by_name=True)

    document_schema: Literal["eyvan.control-plane"] = Field(
        default=LOCAL_CONTROL_PLANE_SCHEMA,
        alias="schema",
        serialization_alias="schema",
    )
    version: int
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
    def validate_common_document(self) -> _LocalControlPlaneDocumentBase:
        """Reject documents that would lose an existing aggregate identity."""

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
        event_ids = [event.id for event in self.events]
        if len(set(event_ids)) != len(event_ids):
            raise ValueError("local control-plane audit events must have unique IDs")
        sequences = [event.sequence for event in self.events]
        if any(sequence < 1 for sequence in sequences):
            raise ValueError("local control-plane audit event sequences must be positive")
        if sequences != sorted(sequences) or len(set(sequences)) != len(sequences):
            raise ValueError("local control-plane audit event sequences must be strictly increasing")
        if self.next_event_sequence <= (sequences[-1] if sequences else 0):
            raise ValueError("local control-plane next_event_sequence must exceed the final audit event")
        return self

    def to_state(self) -> ControlPlaneState:
        """Restore aggregates common to both supported local document versions."""

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


class _LocalControlPlaneV1Document(_LocalControlPlaneDocumentBase):
    """Read-only compatibility model for documents emitted before schema v2."""

    version: Literal[1] = 1


class LocalControlPlaneDocument(_LocalControlPlaneDocumentBase):
    """Schema-v2 JSON representation of every durable control-plane aggregate."""

    version: Literal[2] = LOCAL_CONTROL_PLANE_SCHEMA_VERSION
    progress_reports: tuple[LocalProgressReportDocument, ...] = ()
    plane_peers: tuple[PlanePeerRecord, ...] = ()
    budget_grants: tuple[BudgetGrantRecord, ...] = ()
    quota_grants: tuple[QuotaGrantRecord, ...] = ()
    managed_artifacts: tuple[ManagedArtifactRecord, ...] = ()
    artifact_references: tuple[ArtifactReferenceRecord, ...] = ()

    @model_validator(mode="after")
    def validate_v2_document(self) -> LocalControlPlaneDocument:
        """Reject duplicate complete identities before dictionary restoration can merge them."""

        progress_keys = [(item.workload_id, item.report.rank) for item in self.progress_reports]
        if len(set(progress_keys)) != len(progress_keys):
            raise ValueError("local control-plane progress reports must have unique workload/rank identities")
        if len({record.plane_id for record in self.plane_peers}) != len(self.plane_peers):
            raise ValueError("local control-plane plane peers must have unique plane IDs")
        if len({record.id for record in self.budget_grants}) != len(self.budget_grants):
            raise ValueError("local control-plane budget grants must have unique IDs")
        if len({record.id for record in self.quota_grants}) != len(self.quota_grants):
            raise ValueError("local control-plane quota grants must have unique IDs")
        managed_keys = [(record.org_id, record.project_id, record.uri) for record in self.managed_artifacts]
        if len(set(managed_keys)) != len(managed_keys):
            raise ValueError(
                "local control-plane managed artifacts must have unique organization/project/URI identities"
            )
        reference_keys = [
            (record.org_id, record.project_id, record.uri, record.workload_id) for record in self.artifact_references
        ]
        if len(set(reference_keys)) != len(reference_keys):
            raise ValueError(
                "local control-plane artifact references must have unique organization/project/URI/workload identities"
            )
        return self

    @classmethod
    def from_state(cls, state: ControlPlaneState) -> LocalControlPlaneDocument:
        """Encode one in-process aggregate snapshot in deterministic identity order."""

        progress_reports = sorted(
            (
                LocalProgressReportDocument(workload_id=workload_id, report=report)
                for workload_id, reports in state.progress_reports.items()
                for report in reports.values()
            ),
            key=lambda item: (item.workload_id, item.report.rank),
        )
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
            events=tuple(sorted(state.events, key=lambda event: event.sequence)),
            next_event_sequence=state.next_event_sequence,
            progress_reports=tuple(progress_reports),
            plane_peers=tuple(sorted(state.plane_peers.values(), key=lambda record: record.plane_id)),
            budget_grants=tuple(sorted(state.budget_grants.values(), key=lambda record: record.id)),
            quota_grants=tuple(sorted(state.quota_grants.values(), key=lambda record: record.id)),
            managed_artifacts=tuple(
                sorted(
                    state.managed_artifacts.values(),
                    key=lambda record: (record.org_id, record.project_id, record.uri),
                )
            ),
            artifact_references=tuple(
                sorted(
                    state.artifact_references.values(),
                    key=lambda record: (record.org_id, record.project_id, record.uri, record.workload_id),
                )
            ),
        )

    def to_state(self) -> ControlPlaneState:
        """Restore the typed mutable representation used by transactional sessions."""

        state = super().to_state()
        for item in self.progress_reports:
            state.progress_reports.setdefault(item.workload_id, {})[item.report.rank] = item.report
        state.plane_peers = {record.plane_id: record for record in self.plane_peers}
        state.budget_grants = {record.id: record for record in self.budget_grants}
        state.quota_grants = {record.id: record for record in self.quota_grants}
        state.managed_artifacts = {
            (record.org_id, record.project_id, record.uri): record for record in self.managed_artifacts
        }
        state.artifact_references = {
            (record.org_id, record.project_id, record.uri, record.workload_id): record
            for record in self.artifact_references
        }
        return state


_LocalControlPlaneReadableDocument: tp.TypeAlias = _LocalControlPlaneV1Document | LocalControlPlaneDocument
_LocalFileIdentity: tp.TypeAlias = tuple[int, int, int, int, int]


class _LocalFileSnapshot(tp.NamedTuple):
    """Exact bytes and stable metadata observed through one no-follow descriptor."""

    payload: bytes
    identity: _LocalFileIdentity


class LocalControlPlaneStore:
    """Read and atomically replace one owner-only local JSON control-plane document."""

    def __init__(self, path: Path | str = DEFAULT_LOCAL_CONTROL_PLANE_PATH):
        self.path = Path(path).expanduser()
        self._commit_status_ambiguous = False

    @property
    def v1_backup_path(self) -> Path:
        """Return the non-overwritten backup path used for a v1-to-v2 transition."""

        return self.path.with_name(self.path.name + LOCAL_CONTROL_PLANE_V1_BACKUP_SUFFIX)

    @property
    def commit_status_ambiguous(self) -> bool:
        """Return whether a replace succeeded without a confirming directory fsync."""

        return self._commit_status_ambiguous

    def load(self) -> ControlPlaneState:
        """Load v1 or v2 state, returning an empty state before first use.

        A successful explicit reload also clears a prior ambiguous-commit poison:
        the caller has reconciled its view with the pathname that survived.
        """

        if self._existing_metadata() is None:
            state = ControlPlaneState()
        else:
            document, _ = self._read_document()
            state = document.to_state()
        self._commit_status_ambiguous = False
        return state

    def save(self, state: ControlPlaneState) -> None:
        """Durably write v2, preserving the exact v1 bytes before the first upgrade write."""

        self._raise_if_ambiguous_commit()
        temporary_path: Path | None = None
        replaced = False
        try:
            self._ensure_parent()
            existing_document: _LocalControlPlaneReadableDocument | None = None
            existing_snapshot: _LocalFileSnapshot | None = None
            if self._existing_metadata() is not None:
                existing_document, existing_snapshot = self._read_document()

            document = LocalControlPlaneDocument.from_state(state).model_dump(mode="json", by_alias=True)
            self._validate_json_depth(document, validate_integers=True)
            if not is_json_object(document):
                raise LocalControlPlaneError("local Eyvan control-plane state did not serialize to a JSON object")
            temporary_path = self._write_owner_only_json_temporary(document, prefix=f".{self.path.name}.")

            if isinstance(existing_document, _LocalControlPlaneV1Document):
                assert existing_snapshot is not None
                self._ensure_v1_backup(existing_snapshot.payload)
                self._revalidate_current_v1(existing_snapshot)

            os.replace(temporary_path, self.path)
            replaced = True
            temporary_path = None
            self._fsync_parent()
        except Exception as exc:
            if replaced:
                error = LocalControlPlaneAmbiguousCommitError(
                    f"local Eyvan control-plane state {self.path} was replaced, but its directory fsync failed; "
                    "commit durability is unknown and this store must be reloaded before another save"
                )
                self._commit_status_ambiguous = True
                raise error from exc
            if isinstance(exc, LocalControlPlaneError):
                raise
            if isinstance(exc, OSError):
                message = f"could not persist local Eyvan control-plane state {self.path}: {exc}"
                raise LocalControlPlaneError(message) from exc
            if isinstance(exc, (TypeError, ValueError, RecursionError)):
                message = f"could not serialize local Eyvan control-plane state {self.path}: {exc}"
                raise LocalControlPlaneError(message) from exc
            raise
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def _raise_if_ambiguous_commit(self) -> None:
        if not self._commit_status_ambiguous:
            return
        raise LocalControlPlaneAmbiguousCommitError(
            f"local Eyvan control-plane state {self.path} has an ambiguous prior commit; "
            "reload the state before another save"
        )

    def _read_document(self) -> tuple[_LocalControlPlaneReadableDocument, _LocalFileSnapshot]:
        try:
            snapshot = self._read_owner_only_regular_snapshot(self.path, description="state")
            raw = json.loads(
                snapshot.payload.decode("utf-8"),
                parse_int=self._parse_json_integer,
                parse_constant=self._reject_json_constant,
            )
            self._validate_json_depth(raw)
            if not is_json_object(raw):
                raise LocalControlPlaneSchemaError(
                    f"local Eyvan control-plane state {self.path} must be a finite JSON object"
                )
            if raw.get("schema") != LOCAL_CONTROL_PLANE_SCHEMA:
                raise LocalControlPlaneSchemaError(
                    f"local Eyvan control-plane state {self.path} has an unsupported or missing schema"
                )
            version = raw.get("version")
            if type(version) is not int or version not in (1, LOCAL_CONTROL_PLANE_SCHEMA_VERSION):
                raise LocalControlPlaneSchemaError(
                    f"local Eyvan control-plane state {self.path} has an unsupported schema version"
                )
            model: type[_LocalControlPlaneReadableDocument]
            model = _LocalControlPlaneV1Document if version == 1 else LocalControlPlaneDocument
            document = model.model_validate(raw)
        except LocalControlPlaneSchemaError:
            raise
        except (OSError, ValueError, RecursionError) as exc:
            detail = str(exc)
            if len(detail) > 2048:
                detail = detail[:2048] + "..."
            message = f"could not read local Eyvan control-plane state {self.path}: {detail}"
            raise LocalControlPlaneSchemaError(message) from exc
        return document, snapshot

    @staticmethod
    def _reject_json_constant(value: str) -> tp.NoReturn:
        raise ValueError(f"local Eyvan control-plane JSON constant {value!r} is not finite")

    @staticmethod
    def _parse_json_integer(value: str) -> int:
        digits = value[1:] if value.startswith("-") else value
        if len(digits) > _LOCAL_CONTROL_PLANE_MAX_INTEGER_DIGITS:
            raise ValueError(
                f"local Eyvan control-plane JSON integer exceeds {_LOCAL_CONTROL_PLANE_MAX_INTEGER_DIGITS} digits"
            )
        return int(value)

    @staticmethod
    def _validate_json_depth(value: object, *, validate_integers: bool = False) -> None:
        frames: list[tuple[tp.Iterator[object], int]] = [(iter((value,)), 0)]
        while frames:
            items, parent_depth = frames[-1]
            try:
                item = next(items)
            except StopIteration:
                frames.pop()
                continue
            if isinstance(item, dict):
                depth = parent_depth + 1
                children: tp.Iterator[object] = iter(item.values())
            elif isinstance(item, list):
                depth = parent_depth + 1
                children = iter(item)
            elif validate_integers and type(item) is int:
                if len(str(abs(item))) > _LOCAL_CONTROL_PLANE_MAX_INTEGER_DIGITS:
                    raise ValueError(
                        "local Eyvan control-plane JSON integer exceeds "
                        f"{_LOCAL_CONTROL_PLANE_MAX_INTEGER_DIGITS} digits"
                    )
                continue
            else:
                continue
            if depth > _LOCAL_CONTROL_PLANE_MAX_JSON_DEPTH:
                raise ValueError(
                    f"local Eyvan control-plane JSON nesting exceeds {_LOCAL_CONTROL_PLANE_MAX_JSON_DEPTH} containers"
                )
            frames.append((children, depth))

    def _ensure_v1_backup(self, payload: bytes) -> None:
        backup_path = self.v1_backup_path
        temporary_path: Path | None = None
        try:
            try:
                metadata = backup_path.lstat()
            except FileNotFoundError:
                metadata = None
            if metadata is not None:
                self._validate_owner_only_regular(metadata, backup_path, description="v1 backup")
            else:
                temporary_path = self._write_owner_only_temporary(payload, prefix=f".{backup_path.name}.")
                try:
                    os.link(temporary_path, backup_path, follow_symlinks=False)
                except FileExistsError:
                    metadata = backup_path.lstat()
                    self._validate_owner_only_regular(metadata, backup_path, description="v1 backup")
                else:
                    temporary_path.unlink()
                    temporary_path = None
            self._sync_exact_v1_backup(payload)
        except LocalControlPlaneError:
            raise
        except OSError as exc:
            raise LocalControlPlaneError(f"could not preserve local Eyvan v1 backup {backup_path}: {exc}") from exc
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def _sync_exact_v1_backup(self, payload: bytes) -> None:
        backup_path = self.v1_backup_path
        snapshot = self._read_owner_only_regular_snapshot(
            backup_path,
            description="v1 backup",
            synchronize=True,
        )
        if snapshot.payload != payload:
            raise LocalControlPlaneError(
                f"local Eyvan v1 backup {backup_path} does not match the state being upgraded; "
                "preserve or move it before retrying"
            )
        self._fsync_parent()

    def _revalidate_current_v1(self, expected: _LocalFileSnapshot) -> None:
        current = self._read_owner_only_regular_snapshot(self.path, description="state")
        if current.identity != expected.identity or current.payload != expected.payload:
            raise LocalControlPlaneError(
                f"local Eyvan v1 state {self.path} changed after its backup was secured; "
                "refusing to replace the newer pathname"
            )

    def _write_owner_only_json_temporary(self, document: tp.Mapping[str, object], *, prefix: str) -> Path:
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=self.path.parent,
                prefix=prefix,
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                os.fchmod(temporary.fileno(), 0o600)
                encoder = json.JSONEncoder(indent=2, sort_keys=True, allow_nan=False, ensure_ascii=True)
                written = 0
                for text in encoder.iterencode(document):
                    if written + len(text) > LOCAL_CONTROL_PLANE_MAX_BYTES:
                        self._raise_oversize_state()
                    for offset in range(0, len(text), 65_536):
                        chunk = text[offset : offset + 65_536].encode("ascii")
                        written = self._write_bounded_chunk(temporary, chunk, written=written)
                self._write_bounded_chunk(temporary, b"\n", written=written)
                temporary.flush()
                os.fsync(temporary.fileno())
            return temporary_path
        except BaseException:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
            raise

    def _write_owner_only_temporary(self, payload: bytes, *, prefix: str) -> Path:
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=self.path.parent,
                prefix=prefix,
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                os.fchmod(temporary.fileno(), 0o600)
                self._write_bounded_chunk(temporary, payload, written=0)
                temporary.flush()
                os.fsync(temporary.fileno())
            return temporary_path
        except BaseException:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
            raise

    def _write_bounded_chunk(self, stream: tp.BinaryIO, chunk: bytes, *, written: int) -> int:
        updated = written + len(chunk)
        if updated > LOCAL_CONTROL_PLANE_MAX_BYTES:
            self._raise_oversize_state()
        stream.write(chunk)
        return updated

    @staticmethod
    def _raise_oversize_state() -> tp.NoReturn:
        raise LocalControlPlaneError(
            f"local Eyvan control-plane state exceeds the {LOCAL_CONTROL_PLANE_MAX_BYTES}-byte local JSON limit"
        )

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
        self._validate_owner_only_regular(metadata, self.path, description="state")
        return metadata

    def _read_owner_only_regular_snapshot(
        self,
        path: Path,
        *,
        description: str,
        synchronize: bool = False,
    ) -> _LocalFileSnapshot:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            before = os.fstat(descriptor)
            self._validate_owner_only_regular(before, path, description=description)
            if before.st_size > LOCAL_CONTROL_PLANE_MAX_BYTES:
                raise LocalControlPlaneSchemaError(
                    f"local Eyvan control-plane {description} {path} exceeds the "
                    f"{LOCAL_CONTROL_PLANE_MAX_BYTES}-byte local JSON limit"
                )
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                payload = stream.read(LOCAL_CONTROL_PLANE_MAX_BYTES + 1)
            if len(payload) > LOCAL_CONTROL_PLANE_MAX_BYTES:
                raise LocalControlPlaneSchemaError(
                    f"local Eyvan control-plane {description} {path} exceeds the "
                    f"{LOCAL_CONTROL_PLANE_MAX_BYTES}-byte local JSON limit"
                )
            after_read = os.fstat(descriptor)
            self._validate_owner_only_regular(after_read, path, description=description)
            if (
                self._metadata_identity(before) != self._metadata_identity(after_read)
                or len(payload) != after_read.st_size
            ):
                raise LocalControlPlaneSchemaError(
                    f"local Eyvan control-plane {description} {path} changed while it was being read"
                )
            if synchronize:
                os.fsync(descriptor)
            final = os.fstat(descriptor)
            self._validate_owner_only_regular(final, path, description=description)
            identity = self._metadata_identity(final)
            if identity != self._metadata_identity(after_read):
                raise LocalControlPlaneSchemaError(
                    f"local Eyvan control-plane {description} {path} changed while it was being secured"
                )
            self._validate_current_path_identity(path, identity, description=description)
            return _LocalFileSnapshot(payload=payload, identity=identity)
        finally:
            os.close(descriptor)

    def _read_owner_only_regular(self, path: Path, *, description: str) -> bytes:
        return self._read_owner_only_regular_snapshot(path, description=description).payload

    @staticmethod
    def _metadata_identity(metadata: os.stat_result) -> _LocalFileIdentity:
        return (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
        )

    def _validate_current_path_identity(
        self,
        path: Path,
        identity: _LocalFileIdentity,
        *,
        description: str,
    ) -> None:
        metadata = path.lstat()
        self._validate_owner_only_regular(metadata, path, description=description)
        if self._metadata_identity(metadata) != identity:
            raise LocalControlPlaneSchemaError(
                f"local Eyvan control-plane {description} {path} changed pathname identity while it was being read"
            )

    @staticmethod
    def _validate_owner_only_regular(metadata: os.stat_result, path: Path, *, description: str) -> None:
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise LocalControlPlaneSchemaError(f"local Eyvan control-plane {description} {path} must be a regular file")
        if stat.S_IMODE(metadata.st_mode) & 0o077:
            raise LocalControlPlaneSchemaError(
                f"local Eyvan control-plane {description} {path} must not be group- or world-readable"
            )

    def _read_text(self) -> str:
        """Retain the private textual reader used by older local-store probes."""

        return self._read_owner_only_regular(self.path, description="state").decode("utf-8")

    def _fsync_parent(self) -> None:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        descriptor = os.open(self.path.parent, flags)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode):
                raise OSError(errno.ENOTDIR, "local Eyvan control-plane parent is not a directory", self.path.parent)
            path_metadata = self.path.parent.lstat()
            identity = (metadata.st_dev, metadata.st_ino)
            if not stat.S_ISDIR(path_metadata.st_mode) or (path_metadata.st_dev, path_metadata.st_ino) != identity:
                raise OSError(errno.ESTALE, "local Eyvan control-plane parent pathname changed", self.path.parent)
            os.fsync(descriptor)
            path_metadata = self.path.parent.lstat()
            if not stat.S_ISDIR(path_metadata.st_mode) or (path_metadata.st_dev, path_metadata.st_ino) != identity:
                raise OSError(errno.ESTALE, "local Eyvan control-plane parent pathname changed", self.path.parent)
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
                self.store._raise_if_ambiguous_commit()
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

    @asynccontextmanager
    async def transaction(self) -> tp.AsyncIterator[LocalJsonSession]:
        """Refuse a queued transaction if an earlier replace poisoned the store."""

        async with super().transaction() as transaction:
            self.store._raise_if_ambiguous_commit()
            yield tp.cast(LocalJsonSession, transaction)

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
        try:
            self.store.save(state)
        except LocalControlPlaneAmbiguousCommitError:
            # The rename returned successfully, so the pathname contains this
            # exact snapshot even though the directory fsync did not confirm
            # crash durability. Publish it in process before poisoning future
            # transactions so event sequences can never be reused.
            self._state = state
            raise

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
    "LOCAL_CONTROL_PLANE_MAX_BYTES",
    "LOCAL_CONTROL_PLANE_SCHEMA",
    "LOCAL_CONTROL_PLANE_SCHEMA_VERSION",
    "LOCAL_CONTROL_PLANE_V1_BACKUP_SUFFIX",
    "LocalControlPlaneAmbiguousCommitError",
    "LocalControlPlaneDocument",
    "LocalControlPlaneError",
    "LocalControlPlaneInUseError",
    "LocalControlPlaneLease",
    "LocalControlPlaneSchemaError",
    "LocalControlPlaneStore",
    "LocalJsonRepository",
    "LocalJsonSession",
    "LocalProgressReportDocument",
)
