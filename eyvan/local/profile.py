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
"""Owner-only paths and secrets used by a single-machine Eyvan profile."""

from __future__ import annotations

import os
import secrets
import stat
from pathlib import Path

DEFAULT_LOCAL_CONTROL_PLANE_PATH = Path("~/.eyvan/control-plane.json")
DEFAULT_LOCAL_ARTIFACT_TOKEN_KEY_PATH = Path("~/.eyvan/artifact-token.key")


class LocalProfileSecretError(RuntimeError):
    """A local Eyvan secret path is missing, malformed, or unsafe."""


class LocalArtifactTokenKeyStore:
    """Create and load the stable artifact-transfer HMAC key for one local profile."""

    def __init__(self, path: Path | str = DEFAULT_LOCAL_ARTIFACT_TOKEN_KEY_PATH):
        self.path = Path(path).expanduser()

    def load_or_create(self) -> bytes:
        """Return one owner-only 32-byte key, creating it exactly once when absent."""

        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        try:
            return self._load()
        except FileNotFoundError:
            return self._create_or_load()

    def _load(self) -> bytes:
        """Load a previously created regular owner-only key file."""

        try:
            metadata = self.path.lstat()
        except FileNotFoundError:
            raise
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise LocalProfileSecretError(f"local artifact token key {self.path} must be a regular file")
        mode = stat.S_IMODE(metadata.st_mode)
        if mode & 0o077:
            raise LocalProfileSecretError(f"local artifact token key {self.path} must not be group- or world-readable")
        try:
            descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            raise
        except OSError as exc:
            raise LocalProfileSecretError(f"could not open local artifact token key {self.path}: {exc}") from exc
        try:
            chunks: list[bytes] = []
            size = 0
            while chunk := os.read(descriptor, 4096):
                size += len(chunk)
                if size > 32:
                    raise LocalProfileSecretError(f"local artifact token key {self.path} must contain exactly 32 bytes")
                chunks.append(chunk)
        finally:
            os.close(descriptor)
        key = b"".join(chunks)
        if len(key) != 32:
            raise LocalProfileSecretError(f"local artifact token key {self.path} must contain exactly 32 bytes")
        return key

    def _create_or_load(self) -> bytes:
        """Atomically create the key, accepting a concurrent creator's key instead."""

        key = secrets.token_bytes(32)
        try:
            descriptor = os.open(
                self.path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
            )
        except FileExistsError:
            return self._load()
        except OSError as exc:
            raise LocalProfileSecretError(f"could not create local artifact token key {self.path}: {exc}") from exc
        try:
            offset = 0
            while offset < len(key):
                written = os.write(descriptor, key[offset:])
                if written <= 0:
                    raise LocalProfileSecretError(f"could not persist local artifact token key {self.path}")
                offset += written
            os.fsync(descriptor)
        except OSError as exc:
            raise LocalProfileSecretError(f"could not persist local artifact token key {self.path}: {exc}") from exc
        finally:
            os.close(descriptor)
        self._fsync_parent()
        return key

    def _fsync_parent(self) -> None:
        """Persist the directory entry created for a new key file."""

        try:
            descriptor = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
        except OSError as exc:
            message = f"could not sync local Eyvan profile directory {self.path.parent}: {exc}"
            raise LocalProfileSecretError(message) from exc
        try:
            os.fsync(descriptor)
        except OSError as exc:
            message = f"could not sync local Eyvan profile directory {self.path.parent}: {exc}"
            raise LocalProfileSecretError(message) from exc
        finally:
            os.close(descriptor)


__all__ = (
    "DEFAULT_LOCAL_ARTIFACT_TOKEN_KEY_PATH",
    "DEFAULT_LOCAL_CONTROL_PLANE_PATH",
    "LocalArtifactTokenKeyStore",
    "LocalProfileSecretError",
)
