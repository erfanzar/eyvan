# Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Integrity and confinement behavior of the native artifact stores."""

from __future__ import annotations

import subprocess
import typing as tp
from pathlib import Path

import pytest

from eyvan.artifacts import GCSArtifactStore, LocalArtifactStore
from eyvan.runtime.callables import dump_callable


def _add(left: int, right: int = 1) -> int:
    return left + right


class FakeGcloudRunner:
    """In-memory ``gcloud storage`` double used by the GCS store tests."""

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
            self.objects[uri] = Path(command[-2]).read_bytes()
            return subprocess.CompletedProcess(command, 0, stdout=b"", stderr=b"")
        if command[2] == "cat":
            return subprocess.CompletedProcess(command, 0, stdout=self.objects[uri], stderr=b"")
        raise AssertionError(command)


def test_local_store_round_trips_a_put_within_the_root(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")
    artifact = dump_callable(_add)

    reference = store.put(artifact)

    assert store.read(reference.uri) == artifact.payload


def test_local_read_is_confined_to_the_store_root(tmp_path) -> None:
    root = tmp_path / "artifacts"
    store = LocalArtifactStore(root)
    store.put(dump_callable(_add))
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"foreign-bytes")

    with pytest.raises(ValueError, match="escapes the store root"):
        store.read(str(outside))
    with pytest.raises(ValueError, match="escapes the store root"):
        store.read(outside.as_uri())
    with pytest.raises(ValueError, match="escapes the store root"):
        store.read(str(root / "callables" / ".." / ".." / "outside.bin"))


def test_local_read_rejects_symlinks_escaping_the_root(tmp_path) -> None:
    root = tmp_path / "artifacts"
    store = LocalArtifactStore(root)
    store.put(dump_callable(_add))
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"foreign-bytes")
    (root / "callables" / "link.bin").symlink_to(outside)

    with pytest.raises(ValueError, match="escapes the store root"):
        store.read(str(root / "callables" / "link.bin"))


def test_local_read_still_rejects_gcs_uris(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path / "artifacts")

    with pytest.raises(ValueError, match="cannot read GCS URIs"):
        store.read("gs://bucket/object.pkl")


def test_gcs_put_uploads_once_and_verifies_a_preexisting_object() -> None:
    runner = FakeGcloudRunner()
    store = GCSArtifactStore("gs://artifacts/eyvan", runner=runner)
    artifact = dump_callable(_add)

    first = store.put(artifact)
    second = store.put(artifact)

    assert second == first
    assert sum(command[2] == "cp" for command in runner.calls) == 1
    assert sum(command[2] == "cat" for command in runner.calls) == 1
    assert store.read(first.uri) == artifact.payload


def test_gcs_put_rejects_a_corrupted_preexisting_object() -> None:
    runner = FakeGcloudRunner()
    store = GCSArtifactStore("gs://artifacts/eyvan", runner=runner)
    artifact = dump_callable(_add)
    uri = f"gs://artifacts/eyvan/callables/{artifact.sha256}.pkl"
    runner.objects[uri] = b"tampered-payload"

    with pytest.raises(ValueError, match="invalid digest"):
        store.put(artifact)
    assert not any(command[2] == "cp" for command in runner.calls)
