from __future__ import annotations

import hashlib
import re
import subprocess
import threading
import typing as tp
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from eyvan.artifact_transfers import ManagedArtifactAccess
from eyvan.managed_artifacts import (
    ArtifactAction,
    ArtifactTokenSigner,
    ExpiredArtifactToken,
    GCSManagedArtifactStore,
    InvalidArtifactToken,
    InvalidManagedArtifactURI,
    LocalManagedArtifactStore,
    ManagedArtifactConflictError,
    ManagedArtifactKind,
    callable_uri,
    parse_managed_uri,
    result_uri,
    storage_scope,
)


def test_managed_uri_helpers_are_canonical_and_reject_path_traversal() -> None:
    digest = hashlib.sha256(b"callable").hexdigest()

    callable_reference = parse_managed_uri(callable_uri(digest))
    result_reference = parse_managed_uri(result_uri("run-1.part_0"))

    assert callable_reference.kind is ManagedArtifactKind.CALLABLES
    assert callable_reference.object_id == digest
    assert result_reference.kind is ManagedArtifactKind.RESULTS
    assert result_reference.object_id == "run-1.part_0"

    invalid = (
        "eyvan://results/../secret",
        "eyvan://results/%2e%2e",
        "eyvan://results/a/b",
        "eyvan://results/run-1?download=true",
        "eyvan://results/run-1#fragment",
        "eyvan://unknown/run-1",
        "eyvan:///results/run-1",
        "EYVAN://results/run-1",
    )
    for uri in invalid:
        with pytest.raises(InvalidManagedArtifactURI):
            parse_managed_uri(uri)
    with pytest.raises(InvalidManagedArtifactURI):
        callable_uri("A" * 64)
    with pytest.raises(InvalidManagedArtifactURI):
        result_uri("../../secret")


def test_local_store_is_content_addressed_atomic_and_project_scoped(tmp_path: Path) -> None:
    store = LocalManagedArtifactStore(tmp_path / "managed")
    payload = b"serialized callable"
    uri = callable_uri(hashlib.sha256(payload).hexdigest())

    assert not store.exists(uri, org_id="acme", project_id="research")
    assert store.put(uri, payload, org_id="acme", project_id="research") == uri
    assert store.put(uri, payload, org_id="acme", project_id="research") == uri
    assert store.get(uri, org_id="acme", project_id="research") == payload
    assert not store.exists(uri, org_id="acme", project_id="other")

    expected = (
        store.root
        / "scopes"
        / storage_scope("acme", "research")
        / "callables"
        / f"{hashlib.sha256(payload).hexdigest()}.pkl"
    )
    assert expected.read_bytes() == payload
    assert "acme" not in expected.as_posix()
    assert "research" not in expected.as_posix()

    with pytest.raises(ValueError, match="content-addressed"):
        store.put(callable_uri("0" * 64), payload, org_id="acme", project_id="research")


def test_local_store_creates_a_missing_configured_root_before_first_write(tmp_path: Path) -> None:
    root = tmp_path / "missing" / "managed-artifacts"
    payload = b"first managed callable"
    uri = callable_uri(hashlib.sha256(payload).hexdigest())
    assert not root.exists()

    store = LocalManagedArtifactStore(root)
    stored_uri = store.put(uri, payload, org_id="acme", project_id="research")

    assert root.is_dir()
    assert stored_uri == uri
    assert store.get(uri, org_id="acme", project_id="research") == payload


def test_local_result_writes_are_immutable_and_idempotent(tmp_path: Path) -> None:
    store = LocalManagedArtifactStore(tmp_path)
    uri = result_uri("workload-1")

    store.put(uri, b"first", org_id="acme", project_id="research")
    store.put(uri, b"first", org_id="acme", project_id="research")

    with pytest.raises(ManagedArtifactConflictError, match="different bytes"):
        store.put(uri, b"second", org_id="acme", project_id="research")
    assert store.get(uri, org_id="acme", project_id="research") == b"first"


def test_local_result_race_commits_exactly_one_payload(tmp_path: Path) -> None:
    store = LocalManagedArtifactStore(tmp_path)
    uri = result_uri("workload-race")
    barrier = threading.Barrier(2)

    def write(payload: bytes) -> bytes | ManagedArtifactConflictError:
        barrier.wait()
        try:
            store.put(uri, payload, org_id="acme", project_id="research")
        except ManagedArtifactConflictError as exc:
            return exc
        return payload

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(write, (b"first", b"second")))

    assert sum(isinstance(outcome, ManagedArtifactConflictError) for outcome in outcomes) == 1
    assert store.get(uri, org_id="acme", project_id="research") in {b"first", b"second"}


def test_local_store_rejects_symlink_escape_before_writing(tmp_path: Path) -> None:
    root = tmp_path / "managed"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "scopes").symlink_to(outside, target_is_directory=True)
    store = LocalManagedArtifactStore(root)

    with pytest.raises(InvalidManagedArtifactURI, match="symbolic links"):
        store.put(result_uri("run-1"), b"secret", org_id="acme", project_id="research")

    assert list(outside.iterdir()) == []


class FakeGcloudRunner:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.calls: list[list[str]] = []

    def __call__(
        self,
        args: tp.Sequence[str],
        *,
        check: bool,
        capture_output: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        command = list(args)
        self.calls.append(command)
        uri = command[-1]
        if command[2:4] == ["objects", "describe"]:
            return subprocess.CompletedProcess(command, 0 if uri in self.objects else 1, stdout=b"", stderr=b"")
        if command[2] == "cp":
            if uri in self.objects:
                return subprocess.CompletedProcess(command, 1, stdout=b"", stderr=b"already exists")
            self.objects[uri] = Path(command[-2]).read_bytes()
            return subprocess.CompletedProcess(command, 0, stdout=b"", stderr=b"")
        if command[2] == "cat":
            if uri not in self.objects and check:
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0, stdout=self.objects[uri], stderr=b"")
        raise AssertionError(command)


def test_gcs_store_uses_scoped_no_clobber_objects() -> None:
    runner = FakeGcloudRunner()
    store = GCSManagedArtifactStore("gs://eyvan-managed/artifacts", runner=runner)
    uri = result_uri("run-1")

    store.put(uri, b"result", org_id="acme", project_id="research")
    store.put(uri, b"result", org_id="acme", project_id="research")

    object_uri = next(iter(runner.objects))
    assert object_uri == f"gs://eyvan-managed/artifacts/scopes/{storage_scope('acme', 'research')}/results/run-1.result"
    assert "--no-clobber" in next(command for command in runner.calls if command[2] == "cp")
    assert not store.exists(uri, org_id="acme", project_id="other")
    with pytest.raises(ManagedArtifactConflictError):
        store.put(uri, b"changed", org_id="acme", project_id="research")


def test_transfer_token_is_url_safe_time_bounded_and_fully_scoped() -> None:
    now = [1_000.0]
    signer = ArtifactTokenSigner(b"s" * 32, default_ttl_seconds=60, clock=lambda: now[0])
    uri = result_uri("run-1")
    token = signer.issue(
        ArtifactAction.WRITE,
        uri,
        org_id="acme",
        project_id="research",
        workload_id="workload-1",
    )

    assert re.fullmatch(r"[A-Za-z0-9_.-]+", token)
    assert "=" not in token
    claims = signer.verify(
        token,
        "write",
        uri,
        org_id="acme",
        project_id="research",
        workload_id="workload-1",
    )
    assert claims.action is ArtifactAction.WRITE
    assert claims.artifact.uri == uri
    assert claims.issued_at == 1_000
    assert claims.expires_at == 1_060
    assert claims.workload_id == "workload-1"

    mismatched_scopes = (
        ("read", uri, "acme", "research", "workload-1"),
        ("write", result_uri("run-2"), "acme", "research", "workload-1"),
        ("write", uri, "other", "research", "workload-1"),
        ("write", uri, "acme", "other", "workload-1"),
        ("write", uri, "acme", "research", "workload-2"),
        ("write", uri, "acme", "research", None),
    )
    for action, expected_uri, org_id, project_id, workload_id in mismatched_scopes:
        with pytest.raises(InvalidArtifactToken, match="scope"):
            signer.verify(
                token,
                action,
                expected_uri,
                org_id=org_id,
                project_id=project_id,
                workload_id=workload_id,
            )

    now[0] = 1_060.0
    with pytest.raises(ExpiredArtifactToken, match="expired"):
        signer.verify(
            token,
            "write",
            uri,
            org_id="acme",
            project_id="research",
            workload_id="workload-1",
        )


def test_transfer_token_rejects_tampering_and_unsafe_lifetimes() -> None:
    signer = ArtifactTokenSigner(
        b"s" * 32,
        default_ttl_seconds=60,
        max_ttl_seconds=120,
        clock=lambda: 1_000,
    )
    uri = result_uri("run-1")
    token = signer.issue("read", uri, org_id="acme", project_id="research")
    replacement = "A" if token[-1] != "A" else "B"
    tampered = f"{token[:-1]}{replacement}"

    with pytest.raises(InvalidArtifactToken, match="invalid"):
        signer.verify(tampered, "read", uri, org_id="acme", project_id="research")
    with pytest.raises(ValueError, match="between 1 and 120"):
        signer.issue("read", uri, org_id="acme", project_id="research", ttl_seconds=121)
    with pytest.raises(ValueError, match="at least 32 bytes"):
        ArtifactTokenSigner(b"too-short")
    with pytest.raises(ValueError, match="workload-bound write"):
        signer.issue(
            "read",
            uri,
            org_id="acme",
            project_id="research",
            workload_id="workload-1",
            expires=False,
        )


@pytest.mark.parametrize(
    "public_url",
    (
        "https://token@eyvan.example.com",
        "https://eyvan.example.com?token=secret",
        "https://eyvan.example.com#fragment",
        "file:///tmp/eyvan.sock",
    ),
)
def test_managed_artifact_access_rejects_unsafe_public_urls(tmp_path: Path, public_url: str) -> None:
    with pytest.raises(ValueError):
        ManagedArtifactAccess(
            store=LocalManagedArtifactStore(tmp_path),
            signer=ArtifactTokenSigner(b"s" * 32),
            public_url=public_url,
            token_ttl_seconds=60,
        )


def test_managed_artifact_access_rejects_explicit_zero_ttl(tmp_path: Path) -> None:
    access = ManagedArtifactAccess(
        store=LocalManagedArtifactStore(tmp_path),
        signer=ArtifactTokenSigner(b"s" * 32),
        public_url="https://eyvan.example.com/",
        token_ttl_seconds=60,
    )

    with pytest.raises(ValueError, match="between"):
        access.callable_download_transfer(
            callable_uri("a" * 64),
            org_id="acme",
            project_id="research",
            workload_id="workload-1",
            ttl_seconds=0,
        )


def test_result_transfer_token_is_durable_but_generation_revocable(tmp_path: Path) -> None:
    now = [1_000.0]
    signer = ArtifactTokenSigner(b"s" * 32, default_ttl_seconds=60, clock=lambda: now[0])
    access = ManagedArtifactAccess(
        store=LocalManagedArtifactStore(tmp_path),
        signer=signer,
        public_url="https://eyvan.example.com",
        token_ttl_seconds=60,
    )
    uri = result_uri("workload-1-g1")
    transfer = access.result_upload_transfer(
        uri,
        org_id="acme",
        project_id="research",
        workload_id="workload-1",
    )

    now[0] += 365 * 24 * 60 * 60
    claims = signer.verify(
        transfer.token,
        ArtifactAction.WRITE,
        uri,
        org_id="acme",
        project_id="research",
        workload_id="workload-1",
    )
    assert claims.expires_at is None
    with pytest.raises(InvalidArtifactToken, match="scope"):
        signer.verify(
            transfer.token,
            ArtifactAction.WRITE,
            result_uri("workload-1-g2"),
            org_id="acme",
            project_id="research",
            workload_id="workload-1",
        )
