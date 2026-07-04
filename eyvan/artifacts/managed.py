# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Managed callable and result artifacts with scoped transfer authorization."""

from __future__ import annotations

import base64
import hashlib
import heapq
import hmac
import json
import os
import re
import stat as stat_module
import subprocess
import tempfile
import time
import typing as tp
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import url2pathname

import httpx

from eyvan._internal.compat import StrEnum
from eyvan.artifacts._capability_tokens import InvalidCapabilityToken, sign_claims, verify_signature

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


class ManagedArtifactInventoryError(RuntimeError):
    """Raised when physical inventory cannot be observed conclusively.

    A list or metadata failure is never translated into an empty scope or a
    missing object. Callers must surface the inventory as inconclusive and must
    not reindex from a partial observation.
    """


MAX_MANAGED_ARTIFACT_INVENTORY_PAGE_SIZE = 1000

_GCS_JSON_API_ROOT = "https://storage.googleapis.com/storage/v1"
_GCS_INVENTORY_FIELDS = "items(name,size,generation),nextPageToken"
_GCS_STAT_FIELDS = "name,size,generation"
_GCS_INVENTORY_TIMEOUT_SECONDS = 30.0
_GCS_ACCESS_TOKEN_TIMEOUT_SECONDS = 10.0
_GCS_METADATA_TOKEN_TIMEOUT_SECONDS = 2.0
_GCS_METADATA_TOKEN_URL = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
_MAX_INVENTORY_CURSOR_BYTES = 512
_MAX_GCS_ACCESS_TOKEN_BYTES = 16 * 1024
_MAX_GCS_PAGE_TOKEN_BYTES = 16 * 1024
_MAX_GCS_OBJECT_NAME_BYTES = 1024
_MAX_GCS_INVENTORY_URL_BYTES = 16 * 1024
_MAX_GCS_INVENTORY_RESPONSE_BYTES = 2 * 1024 * 1024
_MAX_GCS_INTEGER_DIGITS = 20


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


@dataclass(frozen=True, slots=True)
class ManagedArtifactObjectMetadata:
    """Authoritative metadata captured from one physical object observation.

    ``object_version`` identifies the exact provider generation or local file
    instance observed alongside ``size_bytes``. It is report evidence, not the
    conditional-deletion state introduced by the later artifact state-machine
    migration.
    """

    uri: str
    size_bytes: int
    object_version: str

    def __post_init__(self) -> None:
        parse_managed_uri(self.uri)
        if self.size_bytes < 0:
            raise ValueError("managed artifact inventory size must be non-negative")
        if not self.object_version.strip():
            raise ValueError("managed artifact inventory object version must not be blank")


@dataclass(frozen=True, slots=True)
class ManagedArtifactInventoryPage:
    """One bounded page from exactly one known tenant prefix."""

    objects: tuple[ManagedArtifactObjectMetadata, ...]
    next_cursor: str | None = None

    def __post_init__(self) -> None:
        if self.next_cursor is not None and not self.next_cursor.strip():
            raise ValueError("managed artifact inventory cursor must not be blank")


@tp.runtime_checkable
class ManagedArtifactInventoryStore(ManagedArtifactStore, tp.Protocol):
    """The seam a dry-run inventory reconciler requires from every backend.

    Listing is scope-explicit and bounded. Errors propagate: a failed listing is
    inconclusive and must never be interpreted as an empty prefix. ``stat`` and
    page entries carry immutable object versions so a future reconciler can
    distinguish replacements without reading or deleting bytes.
    """

    def list_scope(
        self,
        *,
        org_id: str,
        project_id: str,
        cursor: str | None = None,
        limit: int = 1000,
    ) -> ManagedArtifactInventoryPage: ...

    def stat(self, uri: str, *, org_id: str, project_id: str) -> ManagedArtifactObjectMetadata | None: ...


def _validate_inventory_page_request(cursor: str | None, limit: int) -> str | None:
    if (
        isinstance(limit, bool)
        or not isinstance(limit, int)
        or not 1 <= limit <= MAX_MANAGED_ARTIFACT_INVENTORY_PAGE_SIZE
    ):
        raise ValueError(
            f"managed artifact inventory limit must be between 1 and {MAX_MANAGED_ARTIFACT_INVENTORY_PAGE_SIZE}"
        )
    if cursor is not None:
        if not isinstance(cursor, str) or not cursor.strip():
            raise ValueError("managed artifact inventory cursor must not be blank")
        try:
            cursor_size = len(cursor.encode("utf-8"))
        except UnicodeEncodeError:
            raise ValueError("managed artifact inventory cursor must be valid UTF-8") from None
        if cursor_size > _MAX_INVENTORY_CURSOR_BYTES:
            raise ValueError("managed artifact inventory cursor is too large")
        parse_managed_uri(cursor)
    return cursor


class CommandRunner(tp.Protocol):
    """Injectable subprocess boundary used by the GCS byte-transfer backend."""

    def __call__(
        self,
        args: tp.Sequence[str],
        *,
        check: bool,
        capture_output: bool,
    ) -> subprocess.CompletedProcess[bytes]: ...


class _GCSInventoryTransport(tp.Protocol):
    """Injectable, read-only HTTP boundary for the GCS JSON inventory API."""

    def __call__(
        self,
        url: str,
        *,
        headers: tp.Mapping[str, str],
        timeout_seconds: float,
        maximum_response_bytes: int,
    ) -> tuple[int, bytes]: ...


class _GCSInventoryResponseTooLarge(RuntimeError):
    """Internal signal that a streaming response crossed its fixed body cap."""


def _httpx_gcs_inventory_transport(
    url: str,
    *,
    headers: tp.Mapping[str, str],
    timeout_seconds: float,
    maximum_response_bytes: int,
) -> tuple[int, bytes]:
    """Perform one non-redirecting, response-bounded GCS JSON API request."""

    timeout = httpx.Timeout(timeout_seconds)
    try:
        with httpx.Client(timeout=timeout, follow_redirects=False) as client:
            with client.stream("GET", url, headers=dict(headers)) as response:
                raw_length = response.headers.get("content-length")
                if raw_length is not None:
                    if len(raw_length) > _MAX_GCS_INTEGER_DIGITS or not raw_length.isascii() or not raw_length.isdigit():
                        raise _GCSInventoryResponseTooLarge
                    if int(raw_length) > maximum_response_bytes:
                        raise _GCSInventoryResponseTooLarge
                body = bytearray()
                for chunk in response.iter_bytes():
                    if len(body) + len(chunk) > maximum_response_bytes:
                        raise _GCSInventoryResponseTooLarge
                    body.extend(chunk)
                return response.status_code, bytes(body)
    except _GCSInventoryResponseTooLarge:
        raise
    except httpx.TimeoutException as exc:
        raise TimeoutError from exc
    except httpx.HTTPError as exc:
        raise OSError from exc


def _gcloud_inventory_access_token() -> str:
    """Read an ephemeral OAuth token from the ambient gcloud/workload identity."""

    try:
        completed = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            check=False,
            capture_output=True,
            timeout=_GCS_ACCESS_TOKEN_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        raise ManagedArtifactInventoryError("ambient GCS credential lookup failed") from None
    if completed.returncode != 0:
        raise ManagedArtifactInventoryError("ambient GCS credential lookup failed")
    raw_token = completed.stdout
    if not isinstance(raw_token, bytes) or len(raw_token) > _MAX_GCS_ACCESS_TOKEN_BYTES:
        raise ManagedArtifactInventoryError("ambient GCS credential lookup returned an invalid access token")
    try:
        token = raw_token.decode("ascii").strip()
    except UnicodeDecodeError:
        raise ManagedArtifactInventoryError("ambient GCS credential lookup returned an invalid access token") from None
    return token


def _metadata_inventory_access_token() -> str:
    """Read an ephemeral token from the fixed Google workload metadata endpoint."""

    maximum_body_bytes = _MAX_GCS_ACCESS_TOKEN_BYTES * 2
    try:
        with httpx.Client(
            timeout=httpx.Timeout(_GCS_METADATA_TOKEN_TIMEOUT_SECONDS),
            follow_redirects=False,
            trust_env=False,
        ) as client:
            with client.stream(
                "GET",
                _GCS_METADATA_TOKEN_URL,
                headers={"Accept-Encoding": "identity", "Metadata-Flavor": "Google"},
            ) as response:
                if response.status_code != 200 or response.headers.get("metadata-flavor") != "Google":
                    raise ManagedArtifactInventoryError("ambient GCS credential lookup failed")
                body = bytearray()
                for chunk in response.iter_bytes():
                    if len(body) + len(chunk) > maximum_body_bytes:
                        raise ManagedArtifactInventoryError("ambient GCS credential lookup failed")
                    body.extend(chunk)
    except ManagedArtifactInventoryError:
        raise
    except (httpx.HTTPError, OSError):
        raise ManagedArtifactInventoryError("ambient GCS credential lookup failed") from None
    try:
        document = json.loads(bytes(body))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ManagedArtifactInventoryError("ambient GCS credential lookup failed") from None
    if not isinstance(document, dict) or set(document) - {"access_token", "expires_in", "token_type"}:
        raise ManagedArtifactInventoryError("ambient GCS credential lookup failed")
    token = document.get("access_token")
    if not isinstance(token, str):
        raise ManagedArtifactInventoryError("ambient GCS credential lookup failed")
    return token


def _ambient_gcs_inventory_access_token() -> str:
    """Use ambient gcloud credentials, then workload metadata when gcloud is absent."""

    try:
        return _gcloud_inventory_access_token()
    except ManagedArtifactInventoryError:
        try:
            return _metadata_inventory_access_token()
        except ManagedArtifactInventoryError:
            raise ManagedArtifactInventoryError("ambient GCS credential lookup failed") from None


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

    def _inventory_scope_directory(self, org_id: str, project_id: str) -> Path | None:
        """Return one known scope directory without following directory links."""

        relative = Path("scopes", storage_scope(org_id, project_id))
        current = self.root
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise ManagedArtifactInventoryError(f"could not stat managed artifact root: {exc}") from exc
        if stat_module.S_ISLNK(metadata.st_mode) or not stat_module.S_ISDIR(metadata.st_mode):
            raise ManagedArtifactInventoryError("managed artifact root must remain a real directory")
        for component in relative.parts:
            current /= component
            try:
                metadata = current.lstat()
            except FileNotFoundError:
                return None
            except OSError as exc:
                raise ManagedArtifactInventoryError(f"could not stat managed artifact inventory scope: {exc}") from exc
            if stat_module.S_ISLNK(metadata.st_mode) or not stat_module.S_ISDIR(metadata.st_mode):
                raise ManagedArtifactInventoryError(
                    "managed artifact inventory scope must contain only real directories"
                )
        return current

    @staticmethod
    def _local_inventory_metadata(target: Path, uri: str) -> ManagedArtifactObjectMetadata | None:
        """Observe size and replacement identity from one no-follow descriptor."""

        try:
            descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ManagedArtifactInventoryError(f"could not open managed artifact inventory object: {exc}") from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat_module.S_ISREG(metadata.st_mode):
                raise ManagedArtifactInventoryError("managed artifact inventory object must be a regular file")
            # dev+inode identifies the live file instance. ctime protects against
            # inode reuse and changes on in-place mutation; size is captured by
            # the same fstat call, so a report never pairs two observations.
            version = f"local:{metadata.st_dev}:{metadata.st_ino}:{metadata.st_ctime_ns}"
            return ManagedArtifactObjectMetadata(uri=uri, size_bytes=metadata.st_size, object_version=version)
        finally:
            os.close(descriptor)

    @staticmethod
    def _reference_from_local_entry(kind: ManagedArtifactKind, filename: str) -> ManagedArtifactRef:
        extension = _OBJECT_EXTENSIONS[kind]
        if not filename.endswith(extension):
            raise ManagedArtifactInventoryError(f"managed artifact inventory found an unexpected file in {kind.value!r}")
        object_id = filename[: -len(extension)]
        try:
            reference = ManagedArtifactRef(kind, _validate_object_id(kind, object_id))
        except (InvalidManagedArtifactURI, ValueError) as exc:
            raise ManagedArtifactInventoryError(
                f"managed artifact inventory found a non-canonical object in {kind.value!r}"
            ) from exc
        if filename != f"{reference.object_id}{extension}":
            raise ManagedArtifactInventoryError(
                f"managed artifact inventory found a non-canonical object in {kind.value!r}"
            )
        return reference

    def list_scope(
        self,
        *,
        org_id: str,
        project_id: str,
        cursor: str | None = None,
        limit: int = MAX_MANAGED_ARTIFACT_INVENTORY_PAGE_SIZE,
    ) -> ManagedArtifactInventoryPage:
        """List a bounded page from exactly one explicitly supplied local scope."""

        cursor = _validate_inventory_page_request(cursor, limit)
        scope = self._inventory_scope_directory(org_id, project_id)
        if scope is None:
            return ManagedArtifactInventoryPage(objects=())
        known = {kind.value: kind for kind in ManagedArtifactKind}

        def observed() -> tp.Iterator[ManagedArtifactObjectMetadata]:
            try:
                entries = tuple(os.scandir(scope))
            except OSError as exc:
                raise ManagedArtifactInventoryError(f"could not list managed artifact inventory scope: {exc}") from exc
            for entry in entries:
                if entry.name not in known or entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                    raise ManagedArtifactInventoryError("managed artifact inventory scope contains an unexpected entry")
                kind = known[entry.name]
                try:
                    children = os.scandir(entry.path)
                except OSError as exc:
                    raise ManagedArtifactInventoryError(
                        f"could not list managed artifact inventory kind {kind.value!r}: {exc}"
                    ) from exc
                with children:
                    for child in children:
                        if child.is_symlink() or not child.is_file(follow_symlinks=False):
                            raise ManagedArtifactInventoryError(
                                f"managed artifact inventory kind {kind.value!r} contains a non-file entry"
                            )
                        reference = self._reference_from_local_entry(kind, child.name)
                        if cursor is not None and reference.uri <= cursor:
                            continue
                        metadata = self._local_inventory_metadata(Path(child.path), reference.uri)
                        if metadata is None:
                            raise ManagedArtifactInventoryError(
                                "managed artifact disappeared while its inventory page was being observed"
                            )
                        yield metadata

        # nsmallest keeps memory bounded to one page even when a local scope has
        # millions of objects. Invalid entries are still visited and fail closed.
        page = heapq.nsmallest(limit + 1, observed(), key=lambda item: item.uri)
        has_more = len(page) > limit
        selected = tuple(page[:limit])
        return ManagedArtifactInventoryPage(
            objects=selected,
            next_cursor=selected[-1].uri if has_more else None,
        )

    def stat(
        self,
        uri: str,
        *,
        org_id: str,
        project_id: str,
    ) -> ManagedArtifactObjectMetadata | None:
        """Observe one scoped local object's authoritative size and file identity."""

        reference = parse_managed_uri(uri)
        try:
            target = self._path(reference, org_id, project_id, create_parent=False)
        except OSError as exc:
            raise ManagedArtifactInventoryError(f"could not resolve managed artifact inventory object: {exc}") from exc
        return self._local_inventory_metadata(target, reference.uri)

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
    """Immutable GCS storage with a bounded, live-only JSON API inventory path."""

    def __init__(
        self,
        root_uri: str,
        *,
        runner: CommandRunner = subprocess.run,
        inventory_transport: _GCSInventoryTransport = _httpx_gcs_inventory_transport,
        access_token_provider: tp.Callable[[], str] = _ambient_gcs_inventory_access_token,
        inventory_timeout_seconds: float = _GCS_INVENTORY_TIMEOUT_SECONDS,
    ):
        if not isinstance(root_uri, str) or any(
            ord(character) < 0x20 or ord(character) == 0x7F for character in root_uri
        ):
            raise ValueError("GCS managed artifact root must be a canonical URI without control characters")
        parsed = urlsplit(root_uri.rstrip("/"))
        if parsed.scheme != "gs" or not parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError("GCS managed artifact storage requires a gs://bucket[/prefix] URI")
        if (
            "@" in parsed.netloc
            or ":" in parsed.netloc
            or "%" in parsed.path
            or any(character in f"{parsed.netloc}{parsed.path}" for character in "*?[]\r\n\0")
        ):
            raise ValueError("GCS managed artifact root must use a canonical, non-wildcard bucket and prefix")
        components = tuple(component for component in parsed.path.split("/") if component)
        if any(component in {".", ".."} for component in components):
            raise ValueError("GCS managed artifact root cannot contain relative path components")
        suffix = f"/{'/'.join(components)}" if components else ""
        self.root_uri = f"gs://{parsed.netloc}{suffix}"
        self._bucket = parsed.netloc
        self._root_object_prefix = "/".join(components)
        self._runner = runner
        if not callable(inventory_transport) or not callable(access_token_provider):
            raise TypeError("GCS inventory transport and access token provider must be callable")
        if (
            isinstance(inventory_timeout_seconds, bool)
            or not isinstance(inventory_timeout_seconds, (int, float))
            or not 0 < inventory_timeout_seconds <= 120
        ):
            raise ValueError("GCS inventory timeout must be between 0 and 120 seconds")
        self._inventory_transport = inventory_transport
        self._access_token_provider = access_token_provider
        self._inventory_timeout_seconds = float(inventory_timeout_seconds)

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

    def _scope_object_name_prefix(self, org_id: str, project_id: str) -> str:
        suffix = f"scopes/{storage_scope(org_id, project_id)}/"
        prefix = f"{self._root_object_prefix}/{suffix}" if self._root_object_prefix else suffix
        self._validate_gcs_object_name(prefix, description="scope prefix")
        return prefix

    @staticmethod
    def _validate_gcs_object_name(name: str, *, description: str) -> str:
        if not isinstance(name, str) or not name:
            raise ManagedArtifactInventoryError(f"GCS inventory returned an invalid {description}")
        try:
            encoded = name.encode("utf-8")
        except UnicodeEncodeError:
            raise ManagedArtifactInventoryError(f"GCS inventory returned an invalid {description}") from None
        if len(encoded) > _MAX_GCS_OBJECT_NAME_BYTES:
            raise ManagedArtifactInventoryError(f"GCS inventory returned an oversized {description}")
        return name

    def _object_name(self, reference: ManagedArtifactRef, org_id: str, project_id: str) -> str:
        parsed = urlsplit(self._object_uri(reference, org_id, project_id))
        if parsed.scheme != "gs" or parsed.netloc != self._bucket:
            raise ManagedArtifactInventoryError("managed artifact inventory object escaped its configured GCS bucket")
        return self._validate_gcs_object_name(parsed.path.lstrip("/"), description="object name")

    def _reference_from_gcs_name(self, name: str, org_id: str, project_id: str) -> ManagedArtifactRef:
        name = self._validate_gcs_object_name(name, description="object name")
        prefix = self._scope_object_name_prefix(org_id, project_id)
        if not name.startswith(prefix):
            raise ManagedArtifactInventoryError("GCS inventory returned an object outside the requested scope prefix")
        relative = name[len(prefix) :]
        components = relative.split("/")
        if len(components) != 2:
            raise ManagedArtifactInventoryError("GCS inventory returned a non-canonical managed artifact path")
        try:
            kind = ManagedArtifactKind(components[0])
        except ValueError:
            raise ManagedArtifactInventoryError("GCS inventory returned an unknown managed artifact kind") from None
        filename = components[1]
        extension = _OBJECT_EXTENSIONS[kind]
        if not filename.endswith(extension):
            raise ManagedArtifactInventoryError("GCS inventory returned an object with an unexpected extension")
        object_id = filename[: -len(extension)]
        try:
            reference = ManagedArtifactRef(kind, _validate_object_id(kind, object_id))
        except (InvalidManagedArtifactURI, ValueError):
            raise ManagedArtifactInventoryError("GCS inventory returned a non-canonical managed artifact ID") from None
        if self._object_name(reference, org_id, project_id) != name:
            raise ManagedArtifactInventoryError("GCS inventory returned a non-canonical managed artifact path")
        return reference

    @staticmethod
    def _json_api_url(path: str, parameters: tp.Mapping[str, str]) -> str:
        query = urlencode(tuple(parameters.items()), doseq=False, safe="", quote_via=quote)
        url = f"{_GCS_JSON_API_ROOT}/{path}?{query}"
        try:
            url_size = len(url.encode("ascii"))
        except UnicodeEncodeError:
            raise ManagedArtifactInventoryError("GCS inventory request URL was invalid") from None
        if url_size > _MAX_GCS_INVENTORY_URL_BYTES:
            raise ManagedArtifactInventoryError("GCS inventory request URL was too large")
        return url

    def _access_token(self) -> str:
        try:
            token = self._access_token_provider()
        except Exception:
            raise ManagedArtifactInventoryError("ambient GCS credential lookup failed") from None
        if not isinstance(token, str):
            raise ManagedArtifactInventoryError("ambient GCS credential lookup returned an invalid access token")
        try:
            encoded = token.encode("ascii")
        except UnicodeEncodeError:
            raise ManagedArtifactInventoryError(
                "ambient GCS credential lookup returned an invalid access token"
            ) from None
        if (
            not token
            or token != token.strip()
            or len(encoded) > _MAX_GCS_ACCESS_TOKEN_BYTES
            or any(character < 0x21 or character > 0x7E for character in encoded)
        ):
            raise ManagedArtifactInventoryError("ambient GCS credential lookup returned an invalid access token")
        return token

    def _request_inventory_json(self, url: str, *, absent_on_not_found: bool = False) -> bytes | None:
        token = self._access_token()
        try:
            response = self._inventory_transport(
                url,
                headers={
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                    "Authorization": f"Bearer {token}",
                },
                timeout_seconds=self._inventory_timeout_seconds,
                maximum_response_bytes=_MAX_GCS_INVENTORY_RESPONSE_BYTES,
            )
        except _GCSInventoryResponseTooLarge:
            raise ManagedArtifactInventoryError("GCS inventory response exceeded the bounded body size") from None
        except (TimeoutError, httpx.TimeoutException):
            raise ManagedArtifactInventoryError("GCS inventory request timed out") from None
        except Exception:
            raise ManagedArtifactInventoryError("GCS inventory request failed") from None
        if not isinstance(response, tuple) or len(response) != 2:
            raise ManagedArtifactInventoryError("GCS inventory transport returned an invalid response")
        status_code, payload = response
        if isinstance(status_code, bool) or not isinstance(status_code, int) or not 100 <= status_code <= 599:
            raise ManagedArtifactInventoryError("GCS inventory transport returned an invalid status")
        if not isinstance(payload, bytes) or len(payload) > _MAX_GCS_INVENTORY_RESPONSE_BYTES:
            raise ManagedArtifactInventoryError("GCS inventory response exceeded the bounded body size")
        if absent_on_not_found and status_code == 404:
            return None
        if status_code != 200:
            # The status is useful and bounded. Provider bodies and URLs may
            # contain secrets, tenant identifiers, or attacker-controlled text.
            raise ManagedArtifactInventoryError(f"GCS inventory request failed with HTTP status {status_code}")
        return payload

    @staticmethod
    def _decode_gcs_json(payload: bytes) -> dict[str, tp.Any]:
        def unique_object(pairs: list[tuple[str, tp.Any]]) -> dict[str, tp.Any]:
            value: dict[str, tp.Any] = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError("duplicate JSON key")
                value[key] = item
            return value

        def reject_constant(_value: str) -> tp.NoReturn:
            raise ValueError("non-finite JSON number")

        try:
            decoded = json.loads(
                payload.decode("utf-8"),
                object_pairs_hook=unique_object,
                parse_constant=reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise ManagedArtifactInventoryError("GCS inventory returned malformed JSON") from None
        if not isinstance(decoded, dict):
            raise ManagedArtifactInventoryError("GCS inventory returned an invalid JSON document")
        return decoded

    @staticmethod
    def _parse_gcs_uint64(value: tp.Any, *, field: str, minimum: int) -> tuple[int, str]:
        if (
            not isinstance(value, str)
            or not value
            or len(value) > _MAX_GCS_INTEGER_DIGITS
            or not value.isascii()
            or not value.isdigit()
        ):
            raise ManagedArtifactInventoryError(f"GCS inventory returned an invalid {field}")
        parsed = int(value)
        if parsed < minimum or parsed > (1 << 64) - 1:
            raise ManagedArtifactInventoryError(f"GCS inventory returned an invalid {field}")
        return parsed, value

    def _metadata_from_gcs_item(
        self,
        item: tp.Any,
        *,
        org_id: str,
        project_id: str,
        expected_name: str | None = None,
    ) -> tuple[str, ManagedArtifactObjectMetadata]:
        if not isinstance(item, dict) or set(item) != {"name", "size", "generation"}:
            raise ManagedArtifactInventoryError("GCS inventory returned an invalid object metadata document")
        name = self._validate_gcs_object_name(item["name"], description="object name")
        if expected_name is not None and name != expected_name:
            raise ManagedArtifactInventoryError("GCS stat returned metadata for a different object")
        size_bytes, _size_text = self._parse_gcs_uint64(item["size"], field="object size", minimum=0)
        _generation, generation_text = self._parse_gcs_uint64(item["generation"], field="object generation", minimum=1)
        reference = self._reference_from_gcs_name(name, org_id, project_id)
        return (
            name,
            ManagedArtifactObjectMetadata(
                uri=reference.uri,
                size_bytes=size_bytes,
                object_version=f"gcs:{generation_text}",
            ),
        )

    def _parse_gcs_list_page(
        self,
        payload: bytes,
        *,
        org_id: str,
        project_id: str,
        maximum_items: int,
    ) -> tuple[list[tuple[str, ManagedArtifactObjectMetadata]], str | None]:
        decoded = self._decode_gcs_json(payload)
        if not set(decoded).issubset({"items", "nextPageToken"}):
            raise ManagedArtifactInventoryError("GCS inventory returned unexpected JSON fields")
        raw_items = decoded.get("items", [])
        if not isinstance(raw_items, list) or len(raw_items) > maximum_items:
            raise ManagedArtifactInventoryError("GCS inventory returned an invalid or unbounded object page")
        parsed: list[tuple[str, ManagedArtifactObjectMetadata]] = []
        names: set[str] = set()
        previous_name: str | None = None
        for item in raw_items:
            name, metadata = self._metadata_from_gcs_item(item, org_id=org_id, project_id=project_id)
            if name in names:
                raise ManagedArtifactInventoryError("GCS inventory returned duplicate object names")
            if previous_name is not None and name < previous_name:
                raise ManagedArtifactInventoryError("GCS inventory returned objects out of lexical order")
            names.add(name)
            previous_name = name
            parsed.append((name, metadata))

        next_page_token: str | None = None
        if "nextPageToken" in decoded:
            raw_token = decoded["nextPageToken"]
            if not isinstance(raw_token, str):
                raise ManagedArtifactInventoryError("GCS inventory returned an invalid page token")
            try:
                encoded_token = raw_token.encode("ascii")
            except UnicodeEncodeError:
                raise ManagedArtifactInventoryError("GCS inventory returned an invalid page token") from None
            if (
                not raw_token
                or raw_token != raw_token.strip()
                or len(encoded_token) > _MAX_GCS_PAGE_TOKEN_BYTES
                or any(character < 0x21 or character > 0x7E for character in encoded_token)
            ):
                raise ManagedArtifactInventoryError("GCS inventory returned an invalid page token")
            next_page_token = raw_token
        return parsed, next_page_token

    def list_scope(
        self,
        *,
        org_id: str,
        project_id: str,
        cursor: str | None = None,
        limit: int = MAX_MANAGED_ARTIFACT_INVENTORY_PAGE_SIZE,
    ) -> ManagedArtifactInventoryPage:
        """List one bounded page of current live objects from one exact scope.

        The GCS JSON API receives the scope as a server-side ``prefix``, an
        explicit ``versions=false``/``softDeleted=false``, and
        ``maxResults=limit+1``. The public
        cursor remains the last canonical managed URI; it resumes through a
        strict-successor ``startOffset`` while ``nextPageToken`` is used only as
        bounded evidence that the provider has another page. Exactly one object
        list request is consumed per call.
        """

        cursor = _validate_inventory_page_request(cursor, limit)
        maximum = limit + 1
        prefix = self._scope_object_name_prefix(org_id, project_id)
        parameters = {
            "prefix": prefix,
            "maxResults": str(maximum),
            "versions": "false",
            "softDeleted": "false",
            "projection": "noAcl",
            "fields": _GCS_INVENTORY_FIELDS,
        }
        cursor_name: str | None = None
        if cursor is not None:
            cursor_name = self._object_name(parse_managed_uri(cursor), org_id, project_id)
            # GCS startOffset is inclusive. ``!`` sorts before every character
            # permitted after a canonical result ID prefix (the minimum is
            # ``-``), but ``cursor!`` still sorts after the cursor itself. It is
            # therefore a strict lower bound that excludes the cursor even on a
            # short provider page without skipping a longer canonical filename.
            parameters["startOffset"] = f"{cursor_name}!"
        bucket = quote(self._bucket, safe="")
        url = self._json_api_url(f"b/{bucket}/o", parameters)
        payload = self._request_inventory_json(url)
        assert payload is not None
        parsed, next_page_token = self._parse_gcs_list_page(
            payload,
            org_id=org_id,
            project_id=project_id,
            maximum_items=maximum,
        )
        if cursor_name is not None and any(name <= cursor_name for name, _metadata in parsed):
            raise ManagedArtifactInventoryError("GCS inventory cursor did not advance monotonically")
        has_more = len(parsed) > limit or next_page_token is not None
        selected = tuple(metadata for _name, metadata in parsed[:limit])
        if has_more and not selected:
            raise ManagedArtifactInventoryError("GCS inventory page token did not advance the public cursor")
        return ManagedArtifactInventoryPage(
            objects=selected,
            next_cursor=selected[-1].uri if has_more else None,
        )

    def stat(
        self,
        uri: str,
        *,
        org_id: str,
        project_id: str,
    ) -> ManagedArtifactObjectMetadata | None:
        """GET metadata for exactly one current live object, never its versions."""

        reference = parse_managed_uri(uri)
        object_name = self._object_name(reference, org_id, project_id)
        bucket = quote(self._bucket, safe="")
        encoded_name = quote(object_name, safe="")
        url = self._json_api_url(
            f"b/{bucket}/o/{encoded_name}",
            {"projection": "noAcl", "softDeleted": "false", "fields": _GCS_STAT_FIELDS},
        )
        payload = self._request_inventory_json(url, absent_on_not_found=True)
        if payload is None:
            return None
        decoded = self._decode_gcs_json(payload)
        _name, metadata = self._metadata_from_gcs_item(
            decoded,
            org_id=org_id,
            project_id=project_id,
            expected_name=object_name,
        )
        return metadata

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
        return sign_claims(self._secret, _TOKEN_VERSION, payload)

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
        try:
            payload = verify_signature(self._secret, _TOKEN_VERSION, token)
        except InvalidCapabilityToken as exc:
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
    "MAX_MANAGED_ARTIFACT_INVENTORY_PAGE_SIZE",
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
    "ManagedArtifactInventoryError",
    "ManagedArtifactInventoryPage",
    "ManagedArtifactInventoryStore",
    "ManagedArtifactKind",
    "ManagedArtifactObjectMetadata",
    "ManagedArtifactRef",
    "ManagedArtifactStore",
    "callable_uri",
    "is_managed_uri",
    "managed_artifact_store_from_uri",
    "parse_managed_uri",
    "result_uri",
    "storage_scope",
)
