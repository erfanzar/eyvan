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
"""Execute a trusted callable in an Eyvan OCI workload image.

This file is run directly from the read-only OCI bridge mount.  It has no
Eyvan, Ray, or eRay import: its adjacent ``callable_protocol.py`` is the same
protocol source imported by the native worker.
"""

from __future__ import annotations

import os
import typing as tp

if tp.TYPE_CHECKING or __package__:
    from .callable_protocol import GcsObjectTransfer, UriArtifactTransfer, execute_callable
else:
    from callable_protocol import GcsObjectTransfer, UriArtifactTransfer, execute_callable


class _GcsBlob(tp.Protocol):
    """Small portion of the Storage blob API needed by the OCI runner."""

    def download_as_bytes(self) -> bytes:
        """Read this object as bytes."""

        ...

    def upload_from_string(self, data: bytes, *, content_type: str) -> None:
        """Write bytes to this object."""

        ...


class _GcsBucket(tp.Protocol):
    """Small portion of the Storage bucket API needed by the OCI runner."""

    def blob(self, blob_name: str) -> _GcsBlob:
        """Return a named object handle."""

        ...


class _GcsClient(tp.Protocol):
    """Small portion of the Storage client API needed by the OCI runner."""

    def bucket(self, bucket_name: str) -> _GcsBucket:
        """Return a named bucket handle."""

        ...


class GoogleCloudStorageTransfer(GcsObjectTransfer):
    """Use the TPU VM's ADC identity without mounting host credentials."""

    def __init__(self) -> None:
        try:
            from google.cloud import storage
        except ImportError as exc:
            raise RuntimeError(
                "Eyvan OCI GCS callable artifacts require google-cloud-storage in the workload image"
            ) from exc
        self._client = tp.cast(_GcsClient, storage.Client())

    def read_object(self, bucket_name: str, object_name: str) -> bytes:
        """Download one callable artifact through Application Default Credentials."""

        payload = self._client.bucket(bucket_name).blob(object_name).download_as_bytes()
        if not isinstance(payload, bytes):
            raise RuntimeError("GCS callable artifact download returned non-bytes data")
        return payload

    def write_object(self, bucket_name: str, object_name: str, payload: bytes) -> None:
        """Upload one result artifact through Application Default Credentials."""

        self._client.bucket(bucket_name).blob(object_name).upload_from_string(
            payload,
            content_type="application/octet-stream",
        )


def main(argv: tp.Sequence[str] | None = None) -> int:
    """Run a signed callable artifact inside the OCI workload runtime."""

    return execute_callable(
        argv,
        transfer=UriArtifactTransfer(GoogleCloudStorageTransfer(), os.environ),
        environment=os.environ,
    )


if __name__ == "__main__":
    raise SystemExit(main())
