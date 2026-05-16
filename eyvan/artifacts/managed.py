# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Managed callable and result artifacts with scoped transfer authorization."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import subprocess
import tempfile
import time
import typing as tp
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname

from eyvan._internal.compat import StrEnum

_CALLABLE_ID = re.compile(r"^[0-9a-f]{64}$")
_RESULT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_TOKEN_SEGMENT = re.compile(r"^[A-Za-z0-9_-]+$")
_TOKEN_VERSION = "v1"


class ManagedArtifactKind(StrEnum):
    """Kinds addressable through the managed artifact service."""

    CALLABLES = "callables"
    RESULTS = "results"
    WORKSPACES = "workspaces"
    #: Archived structured logs for one terminal attempt. The only durable copy:
    #: journald lives on the cell, and releasing a cell destroys it, so an
    #: attempt's output disappears exactly when the capacity bill stops -- which
    #: is also when someone starts asking what happened.
    LOGS = "logs"


#: Kinds whose object ID *is* the SHA-256 of the stored bytes, so the stored
#: payload is verified against its own URI on every write and read.
CONTENT_ADDRESSED_KINDS = frozenset({ManagedArtifactKind.CALLABLES, ManagedArtifactKind.WORKSPACES})


class ArtifactAction(StrEnum):
    """Operations authorized by a managed artifact transfer token."""

    READ = "read"
    WRITE = "write"


class InvalidManagedArtifactURI(ValueError):
    """Raised when a managed artifact URI is malformed or unsafe."""


class ManagedArtifactConflictError(RuntimeError):
    """Raised when an immutable managed object already has different bytes."""


class InvalidArtifactToken(ValueError):
    """Raised when a managed artifact transfer token is invalid."""


class ExpiredArtifactToken(InvalidArtifactToken):
    """Raised when a validly signed managed artifact token has expired."""


@dataclass(frozen=True, slots=True)
class ManagedArtifactRef:
    """Canonical logical reference to one managed artifact."""

    kind: ManagedArtifactKind
    object_id: str

    @property
    def uri(self) -> str:
        """Return the canonical managed URI."""

        return f"eyvan://{self.kind.value}/{self.object_id}"


@dataclass(frozen=True, slots=True)
class ArtifactTokenClaims:
    """Verified scope carried by a managed artifact transfer token."""

    action: ArtifactAction
    artifact: ManagedArtifactRef
    org_id: str
    project_id: str
    issued_at: int
    expires_at: int | None
    workload_id: str | None = None


class ManagedArtifactStore(tp.Protocol):
    """Tenant-scoped storage boundary for managed callable and result bytes."""

    def put(self, uri: str, payload: bytes, *, org_id: str, project_id: str) -> str:
        """Atomically create ``uri`` or accept an identical existing object."""

        ...

    def get(self, uri: str, *, org_id: str, project_id: str) -> bytes:
        """Read ``uri`` from the specified tenant scope."""

        ...

    def exists(self, uri: str, *, org_id: str, project_id: str) -> bool:
        """Return whether ``uri`` exists in the specified tenant scope."""

        ...

    def delete(self, uri: str, *, org_id: str, project_id: str) -> bool:
        """Remove ``uri`` from the specified tenant scope.

        Returns whether an object was removed. Deleting an absent object is not an
        error: the collector is at-least-once, so a retry after a partial failure
        must be able to finish rather than raise on the half it already did.
        """

        ...


class CommandRunner(tp.Protocol):
    """Injectable subprocess boundary used by the GCS backend."""

    def __call__(
        self,
        args: tp.Sequence[str],
        *,
        check: bool,
        capture_output: bool,
    ) -> subprocess.CompletedProcess[bytes]: ...


def _validate_object_id(kind: ManagedArtifactKind, object_id: str) -> str:
    content_addressed = kind in CONTENT_ADDRESSED_KINDS
    pattern = _CALLABLE_ID if content_addressed else _RESULT_ID
    if not isinstance(object_id, str) or not pattern.fullmatch(object_id):
        expected = "a lowercase SHA-256 digest" if content_addressed else "a safe result identifier"
        raise InvalidManagedArtifactURI(f"{kind.value} object ID must be {expected}")
    return object_id


def callable_uri(sha256: str) -> str:
    """Build a canonical managed URI for a content-addressed callable."""

    return ManagedArtifactRef(
        ManagedArtifactKind.CALLABLES, _validate_object_id(ManagedArtifactKind.CALLABLES, sha256)
    ).uri


def workspace_uri(sha256: str) -> str:
    """Build a canonical managed URI for a content-addressed workspace bundle."""

    return ManagedArtifactRef(
        ManagedArtifactKind.WORKSPACES, _validate_object_id(ManagedArtifactKind.WORKSPACES, sha256)
    ).uri


def log_uri(workload_id: str, attempt: int, rank: int) -> str:
    """Build a canonical managed URI for one rank's archived logs.

    Keyed by attempt as well as workload: a retry produces different output from
    a different execution, and overwriting the predecessor's would destroy the
    record of the failure that caused the retry -- the one an operator is most
    likely to be looking for.
    """

    if attempt < 1 or rank < 0:
        raise InvalidManagedArtifactURI("log archives require a positive attempt and a non-negative rank")
    object_id = f"{workload_id}-a{attempt}-r{rank}"
    return ManagedArtifactRef(ManagedArtifactKind.LOGS, _validate_object_id(ManagedArtifactKind.LOGS, object_id)).uri


def result_uri(result_id: str) -> str:
    """Build a canonical managed URI for a result object."""

    return ManagedArtifactRef(
        ManagedArtifactKind.RESULTS, _validate_object_id(ManagedArtifactKind.RESULTS, result_id)
    ).uri


def parse_managed_uri(uri: str) -> ManagedArtifactRef:
    """Parse a canonical ``eyvan://`` artifact URI without path normalization."""

    if not isinstance(uri, str):
        raise InvalidManagedArtifactURI("managed artifact URI must be a string")
    parsed = urlsplit(uri)
    if parsed.scheme != "eyvan" or parsed.query or parsed.fragment:
        raise InvalidManagedArtifactURI("managed artifact URI must use eyvan:// without query or fragment")
    try:
        kind = ManagedArtifactKind(parsed.netloc)
    except ValueError as exc:
        kinds = ", ".join(sorted(member.value for member in ManagedArtifactKind))
        raise InvalidManagedArtifactURI(f"managed artifact URI kind must be one of: {kinds}") from exc
    if not parsed.path.startswith("/") or parsed.path.count("/") != 1:
        raise InvalidManagedArtifactURI("managed artifact URI must contain exactly one object ID")
    object_id = _validate_object_id(kind, parsed.path[1:])
    reference = ManagedArtifactRef(kind, object_id)
    if uri != reference.uri:
        raise InvalidManagedArtifactURI("managed artifact URI must use its canonical form")
    return reference


def is_managed_uri(uri: str) -> bool:
    """Return whether ``uri`` is a canonical managed artifact URI.

    A predicate rather than a try/except at each call site: a workload may name
    a ``gs://`` or local result URI, and "not managed" is an ordinary answer
    there, not an error worth constructing an exception for.
    """

    try:
        parse_managed_uri(uri)
    except InvalidManagedArtifactURI:
        return False
    return True


def _validate_scope_value(value: str, name: str, *, max_length: int = 128) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise ValueError(f"{name} must be a non-empty string of at most {max_length} characters")
    return value


def storage_scope(org_id: str, project_id: str) -> str:
    """Return the opaque physical namespace for one organization project."""

    org_id = _validate_scope_value(org_id, "org_id")
    project_id = _validate_scope_value(project_id, "project_id")
    encoded = json.dumps((org_id, project_id), ensure_ascii=True, separators=(",", ":")).encode()
    return f"scope_{hashlib.sha256(encoded).hexdigest()}"


def _validate_payload(reference: ManagedArtifactRef, payload: bytes) -> bytes:
    if not isinstance(payload, bytes):
        raise TypeError("managed artifact payload must be bytes")
    if reference.kind in CONTENT_ADDRESSED_KINDS:
        digest = hashlib.sha256(payload).hexdigest()
        if not hmac.compare_digest(digest, reference.object_id):
            raise ValueError(f"{reference.kind.value} payload does not match its content-addressed URI")
    return payload


def _validate_loaded_payload(reference: ManagedArtifactRef, payload: bytes) -> bytes:
    if reference.kind in CONTENT_ADDRESSED_KINDS:
        digest = hashlib.sha256(payload).hexdigest()
        if not hmac.compare_digest(digest, reference.object_id):
            raise ValueError(f"stored {reference.kind.value} payload does not match its content-addressed URI")
    return payload


_OBJECT_EXTENSIONS = {
    ManagedArtifactKind.CALLABLES: ".pkl",
    ManagedArtifactKind.RESULTS: ".result",
    ManagedArtifactKind.WORKSPACES: ".tar.gz",
    # JSON lines, the same shape the live log page returns, so a reader needs
    # one parser for archived logs and streaming ones rather than two.
    ManagedArtifactKind.LOGS: ".jsonl",
}


def _relative_object_path(reference: ManagedArtifactRef, org_id: str, project_id: str) -> Path:
    extension = _OBJECT_EXTENSIONS[reference.kind]
    return Path("scopes", storage_scope(org_id, project_id), reference.kind.value, f"{reference.object_id}{extension}")


class LocalManagedArtifactStore:
    """Immutable managed artifact storage rooted in a local directory."""

    def __init__(self, root: str | os.PathLike[str]):
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, reference: ManagedArtifactRef, org_id: str, project_id: str, *, create_parent: bool) -> Path:
        relative = _relative_object_path(reference, org_id, project_id)
        if create_parent:
            current = self.root
            for component in relative.parts[:-1]:
                current /= component
                if current.is_symlink():
                    raise InvalidManagedArtifactURI("managed artifact storage path cannot contain symbolic links")
                try:
                    current.mkdir()
                except FileExistsError:
                    pass
                if current.is_symlink() or not current.is_dir():
                    raise InvalidManagedArtifactURI("managed artifact storage path must contain only directories")
        target = self.root / relative
        current = self.root
        for component in target.relative_to(self.root).parts:
            current /= component
            if current.is_symlink():
                raise InvalidManagedArtifactURI("managed artifact storage path cannot contain symbolic links")
            if not current.exists():
                break
        resolved = target.resolve(strict=False)
        try:
            resolved.relative_to(self.root)
        except ValueError as exc:
            raise InvalidManagedArtifactURI("managed artifact path escapes its storage root") from exc
        return target

    @staticmethod
    def _read_committed(target: Path) -> bytes | None:
        """Read a committed object without following symbolic links."""

        try:
            descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
        except OSError:
            return None
        try:
            with os.fdopen(descriptor, "rb") as existing:
                return existing.read()
        except OSError:
            return None

    @staticmethod
    def _commit(target: Path, payload: bytes) -> None:
        temporary_path: Path | None = None
        created = False
        try:
            with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".eyvan-", delete=False) as temporary:
                temporary.write(payload)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            try:
                os.link(temporary_path, target)
                created = True
            except FileExistsError:
                existing = LocalManagedArtifactStore._read_committed(target)
                if existing is None or not hmac.compare_digest(existing, payload):
                    raise ManagedArtifactConflictError(
                        f"managed object {target.name!r} already contains different bytes"
                    ) from None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        if created:
            flags = os.O_RDONLY | os.O_DIRECTORY
            directory_fd = os.open(target.parent, flags)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)

    def put(self, uri: str, payload: bytes, *, org_id: str, project_id: str) -> str:
        """Atomically create a scoped object, accepting idempotent retries."""

        reference = parse_managed_uri(uri)
        payload = _validate_payload(reference, payload)
        target = self._path(reference, org_id, project_id, create_parent=True)
        self._commit(target, payload)
        return reference.uri

    def get(self, uri: str, *, org_id: str, project_id: str) -> bytes:
        """Read and integrity-check a scoped local object."""

        reference = parse_managed_uri(uri)
        target = self._path(reference, org_id, project_id, create_parent=False)
        return _validate_loaded_payload(reference, target.read_bytes())

    def exists(self, uri: str, *, org_id: str, project_id: str) -> bool:
        """Return whether a regular scoped local object exists."""

        reference = parse_managed_uri(uri)
        target = self._path(reference, org_id, project_id, create_parent=False)
        return target.is_file()

    def delete(self, uri: str, *, org_id: str, project_id: str) -> bool:
        """Unlink one scoped local object, reporting whether it was there.

        Symlink safety comes from ``_path``, which raises on a link anywhere in the
        resolved path including the final component -- so a planted link cannot
        redirect this unlink at a target outside the store. The check here is only
        that a regular file is present: a directory or a missing object is "nothing
        to remove", not an error, because the collector is at-least-once.
        """

        reference = parse_managed_uri(uri)
        target = self._path(reference, org_id, project_id, create_parent=False)
        if not target.is_file():
            return False
        try:
            target.unlink()
        except FileNotFoundError:
            return False
        return True


class GCSManagedArtifactStore:
    """Immutable managed artifact storage backed by ``gcloud storage``."""

    def __init__(self, root_uri: str, *, runner: CommandRunner = subprocess.run):
        parsed = urlsplit(root_uri.rstrip("/"))
        if parsed.scheme != "gs" or not parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError("GCS managed artifact storage requires a gs://bucket[/prefix] URI")
        if "@" in parsed.netloc or ":" in parsed.netloc or "%" in parsed.path:
            raise ValueError("GCS managed artifact root must use a canonical bucket and prefix")
        components = tuple(component for component in parsed.path.split("/") if component)
        if any(component in {".", ".."} for component in components):
            raise ValueError("GCS managed artifact root cannot contain relative path components")
        suffix = f"/{'/'.join(components)}" if components else ""
        self.root_uri = f"gs://{parsed.netloc}{suffix}"
        self._runner = runner

    def _object_uri(self, reference: ManagedArtifactRef, org_id: str, project_id: str) -> str:
        relative = _relative_object_path(reference, org_id, project_id).as_posix()
        return f"{self.root_uri}/{relative}"

    def _get_object(self, object_uri: str) -> bytes:
        completed = self._runner(
            ["gcloud", "storage", "cat", object_uri],
            check=True,
            capture_output=True,
        )
        return completed.stdout

    def put(self, uri: str, payload: bytes, *, org_id: str, project_id: str) -> str:
        """Upload a scoped object with no-clobber, accepting idempotent retries."""

        reference = parse_managed_uri(uri)
        payload = _validate_payload(reference, payload)
        object_uri = self._object_uri(reference, org_id, project_id)
        if self.exists(uri, org_id=org_id, project_id=project_id):
            if hmac.compare_digest(self._get_object(object_uri), payload):
                return reference.uri
            raise ManagedArtifactConflictError(
                f"managed object {reference.object_id!r} already contains different bytes"
            )

        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temporary:
                temporary.write(payload)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            command = ["gcloud", "storage", "cp", "--no-clobber", str(temporary_path), object_uri]
            completed = self._runner(command, check=False, capture_output=True)
            if completed.returncode != 0:
                if self.exists(uri, org_id=org_id, project_id=project_id) and hmac.compare_digest(
                    self._get_object(object_uri), payload
                ):
                    return reference.uri
                raise subprocess.CalledProcessError(
                    completed.returncode,
                    command,
                    output=completed.stdout,
                    stderr=completed.stderr,
                )
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        return reference.uri

    def get(self, uri: str, *, org_id: str, project_id: str) -> bytes:
        """Read and integrity-check a scoped GCS object."""

        reference = parse_managed_uri(uri)
        payload = self._get_object(self._object_uri(reference, org_id, project_id))
        return _validate_loaded_payload(reference, payload)

    def exists(self, uri: str, *, org_id: str, project_id: str) -> bool:
        """Return whether a scoped GCS object exists."""

        reference = parse_managed_uri(uri)
        completed = self._runner(
            ["gcloud", "storage", "objects", "describe", self._object_uri(reference, org_id, project_id)],
            check=False,
            capture_output=True,
        )
        return completed.returncode == 0

    def delete(self, uri: str, *, org_id: str, project_id: str) -> bool:
        """Remove one scoped GCS object, reporting whether it was there.

        ``check=False``: a missing object exits non-zero, and that is the ordinary
        outcome of an at-least-once collector retrying work it already finished --
        not a failure worth propagating.
        """

        reference = parse_managed_uri(uri)
        completed = self._runner(
            ["gcloud", "storage", "rm", self._object_uri(reference, org_id, project_id)],
            check=False,
            capture_output=True,
        )
        return completed.returncode == 0


def managed_artifact_store_from_uri(uri: str) -> ManagedArtifactStore:
    """Create the managed artifact backend selected by a server-owned root URI."""

    if uri.startswith("gs://"):
        return GCSManagedArtifactStore(uri)
    parsed = urlsplit(uri)
    if parsed.scheme == "file":
        if parsed.netloc not in ("", "localhost") or parsed.query or parsed.fragment:
            raise ValueError("file managed artifact URIs must be local paths without query or fragment")
        return LocalManagedArtifactStore(url2pathname(parsed.path))
    if parsed.scheme and not (len(parsed.scheme) == 1 and uri[1] == ":"):
        raise ValueError("managed artifact storage requires a gs:// URI, a file:// URI, or a local path")
    return LocalManagedArtifactStore(uri)


def _b64encode(payload: bytes) -> str:
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _b64decode(segment: str) -> bytes:
    if not _TOKEN_SEGMENT.fullmatch(segment):
        raise InvalidArtifactToken("invalid artifact transfer token")
    padding = "=" * (-len(segment) % 4)
    try:
        decoded = base64.b64decode(segment + padding, altchars=b"-_", validate=True)
    except (ValueError, TypeError) as exc:
        raise InvalidArtifactToken("invalid artifact transfer token") from exc
    if not hmac.compare_digest(_b64encode(decoded), segment):
        raise InvalidArtifactToken("invalid artifact transfer token")
    return decoded


def _constant_time_text_equal(left: str, right: str) -> bool:
    return hmac.compare_digest(left.encode(), right.encode())


class ArtifactTokenSigner:
    """Issue and verify scoped HMAC capabilities for managed object actions."""

    def __init__(
        self,
        secret: bytes,
        *,
        default_ttl_seconds: int = 300,
        max_ttl_seconds: int = 3600,
        clock_skew_seconds: int = 0,
        clock: tp.Callable[[], float] = time.time,
    ):
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValueError("artifact token secret must contain at least 32 bytes")
        for value, name, allow_zero in (
            (default_ttl_seconds, "default_ttl_seconds", False),
            (max_ttl_seconds, "max_ttl_seconds", False),
            (clock_skew_seconds, "clock_skew_seconds", True),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < (0 if allow_zero else 1):
                qualifier = "non-negative" if allow_zero else "positive"
                raise ValueError(f"{name} must be a {qualifier} integer")
        if default_ttl_seconds > max_ttl_seconds:
            raise ValueError("default_ttl_seconds cannot exceed max_ttl_seconds")
        self._secret = secret
        self._default_ttl_seconds = default_ttl_seconds
        self._max_ttl_seconds = max_ttl_seconds
        self._clock_skew_seconds = clock_skew_seconds
        self._clock = clock

    @staticmethod
    def _action(value: ArtifactAction | str) -> ArtifactAction:
        try:
            return ArtifactAction(value)
        except ValueError as exc:
            raise ValueError("artifact action must be read or write") from exc

    @staticmethod
    def _workload(value: str | None) -> str | None:
        return None if value is None else _validate_scope_value(value, "workload_id", max_length=256)

    def issue(
        self,
        action: ArtifactAction | str,
        uri: str,
        *,
        org_id: str,
        project_id: str,
        workload_id: str | None = None,
        ttl_seconds: int | None = None,
        expires: bool = True,
    ) -> str:
        """Issue a URL-safe token bound to one action, object, and tenant."""

        action = self._action(action)
        reference = parse_managed_uri(uri)
        org_id = _validate_scope_value(org_id, "org_id")
        project_id = _validate_scope_value(project_id, "project_id")
        workload_id = self._workload(workload_id)
        issued_at = int(self._clock())
        if expires:
            ttl_seconds = self._default_ttl_seconds if ttl_seconds is None else ttl_seconds
            if (
                isinstance(ttl_seconds, bool)
                or not isinstance(ttl_seconds, int)
                or not 1 <= ttl_seconds <= self._max_ttl_seconds
            ):
                raise ValueError(f"ttl_seconds must be an integer between 1 and {self._max_ttl_seconds}")
            expires_at: int | None = issued_at + ttl_seconds
        else:
            if ttl_seconds is not None:
                raise ValueError("non-expiring artifact tokens do not accept ttl_seconds")
            if action is not ArtifactAction.WRITE or workload_id is None:
                raise ValueError("non-expiring artifact tokens require a workload-bound write action")
            expires_at = None
        payload: dict[str, str | int | None] = {
            "action": action.value,
            "expires_at": expires_at,
            "issued_at": issued_at,
            "kind": reference.kind.value,
            "object_id": reference.object_id,
            "org_id": org_id,
            "project_id": project_id,
            "version": 1,
        }
        if workload_id is not None:
            payload["workload_id"] = workload_id
        encoded_payload = _b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        )
        signed = f"{_TOKEN_VERSION}.{encoded_payload}".encode()
        signature = _b64encode(hmac.new(self._secret, signed, hashlib.sha256).digest())
        return f"{_TOKEN_VERSION}.{encoded_payload}.{signature}"

    def verify(
        self,
        token: str,
        action: ArtifactAction | str,
        uri: str,
        *,
        org_id: str,
        project_id: str,
        workload_id: str | None = None,
    ) -> ArtifactTokenClaims:
        """Verify signature, expiry, and the complete expected request scope."""

        action = self._action(action)
        reference = parse_managed_uri(uri)
        org_id = _validate_scope_value(org_id, "org_id")
        project_id = _validate_scope_value(project_id, "project_id")
        workload_id = self._workload(workload_id)
        if not isinstance(token, str):
            raise InvalidArtifactToken("invalid artifact transfer token")
        parts = token.split(".")
        if len(parts) != 3 or parts[0] != _TOKEN_VERSION:
            raise InvalidArtifactToken("invalid artifact transfer token")
        encoded_payload, encoded_signature = parts[1:]
        signature = _b64decode(encoded_signature)
        signed = f"{_TOKEN_VERSION}.{encoded_payload}".encode()
        expected_signature = hmac.new(self._secret, signed, hashlib.sha256).digest()
        if not hmac.compare_digest(expected_signature, signature):
            raise InvalidArtifactToken("invalid artifact transfer token")
        try:
            payload = json.loads(_b64decode(encoded_payload))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise InvalidArtifactToken("invalid artifact transfer token") from exc
        required = {
            "action",
            "expires_at",
            "issued_at",
            "kind",
            "object_id",
            "org_id",
            "project_id",
            "version",
        }
        if not isinstance(payload, dict) or frozenset(payload) not in {
            frozenset(required),
            frozenset((*required, "workload_id")),
        }:
            raise InvalidArtifactToken("invalid artifact transfer token")
        try:
            claims_action = ArtifactAction(payload["action"])
            claims_kind = ManagedArtifactKind(payload["kind"])
            claims_object_id = _validate_object_id(claims_kind, payload["object_id"])
            claims_org = _validate_scope_value(payload["org_id"], "org_id")
            claims_project = _validate_scope_value(payload["project_id"], "project_id")
            claims_workload = self._workload(payload.get("workload_id"))
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidArtifactToken("invalid artifact transfer token") from exc
        issued_at = payload.get("issued_at")
        expires_at = payload.get("expires_at")
        if (
            isinstance(payload.get("version"), bool)
            or not isinstance(payload.get("version"), int)
            or payload.get("version") != 1
            or isinstance(issued_at, bool)
            or not isinstance(issued_at, int)
        ):
            raise InvalidArtifactToken("invalid artifact transfer token")
        if expires_at is None:
            if claims_action is not ArtifactAction.WRITE or claims_workload is None:
                raise InvalidArtifactToken("invalid artifact transfer token")
        elif (
            isinstance(expires_at, bool)
            or not isinstance(expires_at, int)
            or expires_at <= issued_at
            or expires_at - issued_at > self._max_ttl_seconds
        ):
            raise InvalidArtifactToken("invalid artifact transfer token")
        now = int(self._clock())
        if issued_at > now + self._clock_skew_seconds:
            raise InvalidArtifactToken("invalid artifact transfer token")
        if expires_at is not None and expires_at <= now - self._clock_skew_seconds:
            raise ExpiredArtifactToken("artifact transfer token has expired")
        scope_matches = (
            claims_action is action
            and claims_kind is reference.kind
            and _constant_time_text_equal(claims_object_id, reference.object_id)
            and _constant_time_text_equal(claims_org, org_id)
            and _constant_time_text_equal(claims_project, project_id)
            and (
                (claims_workload is None and workload_id is None)
                or (
                    claims_workload is not None
                    and workload_id is not None
                    and _constant_time_text_equal(claims_workload, workload_id)
                )
            )
        )
        if not scope_matches:
            raise InvalidArtifactToken("artifact transfer token scope does not match request")
        return ArtifactTokenClaims(
            action=claims_action,
            artifact=ManagedArtifactRef(claims_kind, claims_object_id),
            org_id=claims_org,
            project_id=claims_project,
            issued_at=issued_at,
            expires_at=expires_at,
            workload_id=claims_workload,
        )


__all__ = (
    "ArtifactAction",
    "ArtifactTokenClaims",
    "ArtifactTokenSigner",
    "CommandRunner",
    "ExpiredArtifactToken",
    "GCSManagedArtifactStore",
    "InvalidArtifactToken",
    "InvalidManagedArtifactURI",
    "LocalManagedArtifactStore",
    "ManagedArtifactConflictError",
    "ManagedArtifactKind",
    "ManagedArtifactRef",
    "ManagedArtifactStore",
    "callable_uri",
    "is_managed_uri",
    "managed_artifact_store_from_uri",
    "parse_managed_uri",
    "result_uri",
    "storage_scope",
)
