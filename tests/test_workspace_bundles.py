# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""End-to-end workspace bundle flow: decorator upload through worker extraction."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
from pathlib import Path

import httpx
import pytest

import eyvan.sdk.client.sync as sync_module
import eyvan.sdk.execution as execution_module
from eyvan._internal.workspace import build_workspace
from eyvan.artifacts import LocalArtifactStore
from eyvan.records import CallableExecution
from eyvan.sdk.client.callables import EyvanCallableSubmitter
from eyvan.worker.runtime.callable_protocol import execute_callable
from eyvan.worker.runtime.callables import dump_callable


class _MemoryTransfer:
    """Minimal ArtifactTransfer over an in-memory URI table."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.written: dict[str, bytes] = {}

    def read(self, uri: str, *, token: str | None = None) -> bytes:
        del token
        return self.objects[uri]

    def write(self, uri: str, payload: bytes, *, token: str | None = None) -> None:
        del token
        self.written[uri] = payload


@pytest.fixture
def restore_process_state():
    """execute_callable chdirs and mutates sys.path; undo that for other tests."""

    cwd = Path.cwd()
    path = list(sys.path)
    yield
    os.chdir(cwd)
    sys.path[:] = path


def _shipped_workspace(root: Path) -> Path:
    root.mkdir(parents=True)
    (root / "shipped_pkg").mkdir()
    (root / "shipped_pkg" / "__init__.py").write_text("VALUE = 41\n")
    (root / "data.txt").write_text("from the workspace")
    return root


def _callable_using_the_workspace() -> int:
    """Import a shipped module and read a shipped file, both from the bundle."""

    import importlib

    module = importlib.import_module("shipped_pkg")
    text = Path("data.txt").read_text()
    return module.VALUE + len(text)


def test_worker_extracts_the_bundle_before_loading_the_callable(tmp_path: Path, restore_process_state) -> None:
    """The whole point: shipped modules and files are usable by the callable."""

    bundle = build_workspace(_shipped_workspace(tmp_path / "src"))
    artifact = dump_callable(_callable_using_the_workspace)
    transfer = _MemoryTransfer(
        {
            "mem://callable": artifact.payload,
            "mem://workspace": bundle.payload,
        }
    )
    destination = tmp_path / "unpacked"

    exit_code = execute_callable(
        [
            "--uri",
            "mem://callable",
            "--sha256",
            artifact.sha256,
            "--python",
            artifact.python,
            "--workspace-uri",
            "mem://workspace",
            "--workspace-sha256",
            bundle.sha256,
            "--result-uri",
            "mem://result",
        ],
        transfer=transfer,
        environment={"EYVAN_WORKSPACE_DIR": str(destination)},
    )

    assert exit_code == 0
    assert (destination / "shipped_pkg" / "__init__.py").exists()
    assert Path.cwd() == destination.resolve()
    # 41 from the shipped module + len("from the workspace") == 18
    assert transfer.written["mem://result"]


def test_worker_rejects_a_bundle_whose_digest_does_not_match(tmp_path: Path, restore_process_state) -> None:
    bundle = build_workspace(_shipped_workspace(tmp_path / "src"))
    artifact = dump_callable(_callable_using_the_workspace)
    transfer = _MemoryTransfer({"mem://callable": artifact.payload, "mem://workspace": bundle.payload})

    with pytest.raises(ValueError, match="does not match its content-addressed digest"):
        execute_callable(
            [
                "--uri",
                "mem://callable",
                "--sha256",
                artifact.sha256,
                "--python",
                artifact.python,
                "--workspace-uri",
                "mem://workspace",
                "--workspace-sha256",
                "b" * 64,
            ],
            transfer=transfer,
            environment={"EYVAN_WORKSPACE_DIR": str(tmp_path / "unpacked")},
        )


def test_worker_runs_without_a_workspace_exactly_as_before(tmp_path: Path, restore_process_state) -> None:
    """Opting out must not chdir or touch sys.path."""

    before = Path.cwd()
    artifact = dump_callable(lambda: 7)
    transfer = _MemoryTransfer({"mem://callable": artifact.payload})

    exit_code = execute_callable(
        ["--uri", "mem://callable", "--sha256", artifact.sha256, "--python", artifact.python],
        transfer=transfer,
        environment={},
    )

    assert exit_code == 0
    assert Path.cwd() == before


def test_decorator_uploads_the_bundle_and_submits_its_content_address(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")
    workspace = _shipped_workspace(tmp_path / "src")
    submitted: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "pools": [
                        {
                            "fleet": "research",
                            "pool": "v5p-pool",
                            "accelerator_type": "v5p-16",
                            "capacity_class": "spot",
                            "region": "us-east5",
                            "zone": "us-east5-a",
                            "min_cells": 0,
                            "max_cells": 4,
                            "enabled": True,
                        }
                    ]
                },
            )
        submitted.append(json.loads(request.content))
        return httpx.Response(
            201,
            json={
                "id": "wl-workspace",
                "org_id": "acme",
                "project_id": "research",
                "created_by": "alice",
                "spec": {
                    "name": "smoke",
                    "execution": {
                        "kind": "callable",
                        "artifact_uri": "gs://a/b.pkl",
                        "sha256": "a" * 64,
                        "python": "3.13",
                        "result_uri": "eyvan://results/wl-workspace-g1",
                    },
                },
            },
        )

    raw = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://test")
    client = sync_module.EyvanClient(client=raw)
    context = execution_module.ExecutionContext(
        fleet=None,
        org_id="acme",
        project_id="research",
        artifact_store=store,
        submitter=EyvanCallableSubmitter(client, store),
    )

    @execution_module.execute(execution_module.TpuConfig(type="v5p-16"), context=context, workspace=workspace)
    def train() -> int:
        return 1

    train.remote()

    expected = build_workspace(workspace)
    execution = submitted[0]["execution"]
    assert execution["workspace_sha256"] == expected.sha256
    assert execution["workspace_uri"].endswith(f"{expected.sha256}.tar.gz")
    # The bundle really landed in the store, addressed by its digest.
    assert (tmp_path / "artifacts" / "workspaces" / f"{expected.sha256}.tar.gz").exists()
    raw.close()


def test_decorator_without_a_workspace_sends_no_bundle_fields(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")
    submitted: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        submitted.append(json.loads(request.content))
        return httpx.Response(
            201,
            json={
                "id": "wl-plain",
                "org_id": "acme",
                "project_id": "research",
                "created_by": "alice",
                "spec": {
                    "name": "smoke",
                    "execution": {
                        "kind": "callable",
                        "artifact_uri": "gs://a/b.pkl",
                        "sha256": "a" * 64,
                        "python": "3.13",
                        "result_uri": "eyvan://results/wl-plain-g1",
                    },
                },
            },
        )

    raw = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://test")
    context = execution_module.ExecutionContext(
        fleet="research",
        org_id="acme",
        project_id="research",
        artifact_store=store,
        submitter=EyvanCallableSubmitter(sync_module.EyvanClient(client=raw), store),
    )

    @execution_module.execute(execution_module.TpuConfig(type="v5p-16"), context=context)
    def train() -> int:
        return 1

    train.remote()

    assert submitted[0]["execution"]["workspace_uri"] is None
    assert not (tmp_path / "artifacts" / "workspaces").exists()
    raw.close()


def test_execution_spec_requires_uri_and_digest_together() -> None:
    with pytest.raises(ValueError, match="must be provided together"):
        CallableExecution(
            artifact_uri="eyvan://callables/" + "a" * 64,
            sha256="a" * 64,
            python="3.13",
            workspace_uri="eyvan://workspaces/" + "b" * 64,
        )


# --------------------------------------------------------------------------- bundle signing


def _signed(payload: bytes, key: bytes) -> str:
    return hmac.new(key, payload, hashlib.sha256).hexdigest()


def test_an_unsigned_workspace_is_rejected_when_the_worker_holds_a_signing_key(
    tmp_path: Path,
    restore_process_state,
) -> None:
    """Otherwise signing the callable proves nothing about what it runs.

    cloudpickle serializes by reference: the pickle names ``shipped_pkg`` and
    the import resolves at load time. The workspace is ``sys.path[0]`` by then,
    so whoever controls the bundle chooses the code -- and the callable's HMAC
    covers none of it.
    """

    key = b"project-signing-key"
    bundle = build_workspace(_shipped_workspace(tmp_path / "src"))
    artifact = dump_callable(_callable_using_the_workspace, signing_key=key)
    transfer = _MemoryTransfer({"mem://callable": artifact.payload, "mem://workspace": bundle.payload})

    with pytest.raises(ValueError, match="workspace bundle signature mismatch"):
        execute_callable(
            [
                "--uri",
                "mem://callable",
                "--sha256",
                artifact.sha256,
                "--python",
                artifact.python,
                "--signature",
                str(artifact.signature),
                "--workspace-uri",
                "mem://workspace",
                "--workspace-sha256",
                bundle.sha256,
            ],
            transfer=transfer,
            environment={
                "EYVAN_WORKSPACE_DIR": str(tmp_path / "unpacked"),
                "EYVAN_CALLABLE_SIGNING_KEY": key.decode(),
            },
        )

    # Rejected before extraction: nothing from the bundle ever reached the disk.
    assert not (tmp_path / "unpacked").exists()


def test_a_workspace_signed_with_the_wrong_key_is_rejected(tmp_path: Path, restore_process_state) -> None:
    key = b"project-signing-key"
    bundle = build_workspace(_shipped_workspace(tmp_path / "src"))
    artifact = dump_callable(_callable_using_the_workspace, signing_key=key)
    transfer = _MemoryTransfer({"mem://callable": artifact.payload, "mem://workspace": bundle.payload})

    with pytest.raises(ValueError, match="workspace bundle signature mismatch"):
        execute_callable(
            [
                "--uri",
                "mem://callable",
                "--sha256",
                artifact.sha256,
                "--python",
                artifact.python,
                "--signature",
                str(artifact.signature),
                "--workspace-uri",
                "mem://workspace",
                "--workspace-sha256",
                bundle.sha256,
                "--workspace-signature",
                _signed(bundle.payload, b"attacker-key"),
            ],
            transfer=transfer,
            environment={
                "EYVAN_WORKSPACE_DIR": str(tmp_path / "unpacked"),
                "EYVAN_CALLABLE_SIGNING_KEY": key.decode(),
            },
        )


def test_a_correctly_signed_workspace_runs(tmp_path: Path, restore_process_state) -> None:
    key = b"project-signing-key"
    bundle = build_workspace(_shipped_workspace(tmp_path / "src"))
    artifact = dump_callable(_callable_using_the_workspace, signing_key=key)
    transfer = _MemoryTransfer({"mem://callable": artifact.payload, "mem://workspace": bundle.payload})
    destination = tmp_path / "unpacked"

    exit_code = execute_callable(
        [
            "--uri",
            "mem://callable",
            "--sha256",
            artifact.sha256,
            "--python",
            artifact.python,
            "--signature",
            str(artifact.signature),
            "--workspace-uri",
            "mem://workspace",
            "--workspace-sha256",
            bundle.sha256,
            "--workspace-signature",
            _signed(bundle.payload, key),
            "--result-uri",
            "mem://result",
        ],
        transfer=transfer,
        environment={
            "EYVAN_WORKSPACE_DIR": str(destination),
            "EYVAN_CALLABLE_SIGNING_KEY": key.decode(),
        },
    )

    assert exit_code == 0
    assert (destination / "shipped_pkg" / "__init__.py").exists()


def test_a_workspace_without_a_digest_is_refused(tmp_path: Path, restore_process_state) -> None:
    """The digest was optional in the worker even though every caller sends it."""

    bundle = build_workspace(_shipped_workspace(tmp_path / "src"))
    artifact = dump_callable(_callable_using_the_workspace)
    transfer = _MemoryTransfer({"mem://callable": artifact.payload, "mem://workspace": bundle.payload})

    with pytest.raises(ValueError, match="--workspace-sha256 is required"):
        execute_callable(
            [
                "--uri",
                "mem://callable",
                "--sha256",
                artifact.sha256,
                "--python",
                artifact.python,
                "--workspace-uri",
                "mem://workspace",
            ],
            transfer=transfer,
            environment={"EYVAN_WORKSPACE_DIR": str(tmp_path / "unpacked")},
        )


def test_a_signed_callable_may_not_carry_an_unsigned_workspace() -> None:
    """Rejected at the API boundary, not just at the worker.

    A spec that signs the callable and not the bundle is asking for a guarantee
    the runtime cannot give, so it is refused where it is written down.
    """

    with pytest.raises(ValueError, match="must also sign the workspace bundle"):
        CallableExecution(
            artifact_uri="eyvan://callables/" + "a" * 64,
            sha256="a" * 64,
            python="3.13",
            signature="c" * 64,
            workspace_uri="eyvan://workspaces/" + "b" * 64,
            workspace_sha256="b" * 64,
        )


def test_a_workspace_signature_without_a_workspace_is_rejected() -> None:
    with pytest.raises(ValueError, match="requires a workspace URI"):
        CallableExecution(
            artifact_uri="eyvan://callables/" + "a" * 64,
            sha256="a" * 64,
            python="3.13",
            workspace_signature="d" * 64,
        )


def test_the_decorator_signs_the_bundle_whenever_it_signs_the_callable(tmp_path: Path) -> None:
    """Both signatures are minted together, under the same key.

    Asserted at the submission boundary rather than over HTTP because
    ``EyvanCallableSubmitter`` refuses to send a signed callable to a remote
    control plane at all -- there is no controller-managed verifier yet. That
    guard is what keeps today's remote path honest; this test is what keeps the
    bundle covered if and when the guard lifts.
    """

    store = LocalArtifactStore(tmp_path / "artifacts")
    workspace = _shipped_workspace(tmp_path / "src")
    key = b"project-signing-key"
    captured: list = []

    class CapturingSubmitter:
        def submit_callable(self, request):
            captured.append(request)
            return None

    context = execution_module.ExecutionContext(
        fleet="research",
        org_id="acme",
        project_id="research",
        artifact_store=store,
        submitter=CapturingSubmitter(),
        signing_key=key,
    )

    @execution_module.execute(execution_module.TpuConfig(type="v5p-16"), context=context, workspace=workspace)
    def train() -> int:
        return 1

    train.remote()

    request = captured[0]
    assert request.workspace_signature == _signed(build_workspace(workspace).payload, key)
    assert request.artifact.signature is not None


def test_no_signing_key_means_no_bundle_signature(tmp_path: Path) -> None:
    """The unsigned path stays exactly as it was; nothing becomes mandatory."""

    store = LocalArtifactStore(tmp_path / "artifacts")
    workspace = _shipped_workspace(tmp_path / "src")
    captured: list = []

    class CapturingSubmitter:
        def submit_callable(self, request):
            captured.append(request)
            return None

    context = execution_module.ExecutionContext(
        fleet="research",
        org_id="acme",
        project_id="research",
        artifact_store=store,
        submitter=CapturingSubmitter(),
    )

    @execution_module.execute(execution_module.TpuConfig(type="v5p-16"), context=context, workspace=workspace)
    def train() -> int:
        return 1

    train.remote()

    assert captured[0].workspace_signature is None
    assert captured[0].workspace_sha256 is not None


def test_a_preemption_retry_serves_the_callable_from_the_cell_cache(
    tmp_path: Path,
    restore_process_state,
) -> None:
    """Second run with the source gone still works: sha256 is the validity check."""

    cache_root = tmp_path / "callable-cache"
    artifact = dump_callable(lambda: 41)
    first = _MemoryTransfer({"mem://callable": artifact.payload})
    environment = {
        "EYVAN_NATIVE_WORKER_STATE_DIR": str(tmp_path),
        "EYVAN_CALL_ARGS_JSON": "[]",
    }

    common = [
        "--uri",
        "mem://callable",
        "--sha256",
        artifact.sha256,
        "--python",
        artifact.python,
    ]

    # First attempt: a miss, which fills the cache after verifying the digest.
    assert (
        execute_callable(
            [*common, "--result-uri", "mem://result"],
            transfer=first,
            environment=environment,
        )
        == 0
    )
    cached = cache_root / f"{artifact.sha256}.pkl"
    assert cached.is_file()

    # The retry: the origin has lost the object entirely (a spot reclaim
    # between attempts), and the run still succeeds from the cached bytes --
    # which the normal digest + signature verification still checks.
    empty = _MemoryTransfer({})
    assert (
        execute_callable(
            [*common, "--result-uri", "mem://result"],
            transfer=empty,
            environment=environment,
        )
        == 0
    )

    # Corrupt cache bytes are refused by the existing integrity path, not
    # silently served.
    cached.write_bytes(b"tampered")
    with pytest.raises(Exception):
        execute_callable(
            [*common],
            transfer=_MemoryTransfer({}),
            environment=environment,
        )
