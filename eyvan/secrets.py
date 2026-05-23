# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Reading secret values from an operator-owned store, by reference.

Today the only way to pass a credential to a workload is
``WorkloadDefinition.environment`` -- a persisted ``dict[str, str]`` validated
for identifier names and NUL bytes and nothing else. So the documented advice
that secrets are "never persisted in specs" is contradicted by the only
mechanism available for passing one.

This is the reference side of the fix. Values live in a store the operator
already runs; a spec carries a *reference*, and the reference is resolved at
dispatch by something holding the agent's identity rather than the submitter's.

Selected by URI scheme, exactly like ``artifact_store_from_uri``, so adding a
backend is adding a scheme rather than editing a dispatch table that every
caller has to know about.
"""

from __future__ import annotations

import subprocess
import typing as tp
from pathlib import Path
from urllib.parse import urlsplit


class SecretUnavailable(RuntimeError):
    """Raised when a secret reference cannot be resolved.

    Distinct from "the secret is empty": a store that cannot be reached and a
    secret whose value is the empty string are different incidents, and only one
    of them means a workload should not start.
    """


class SecretStore(tp.Protocol):
    """Read one secret value by reference."""

    def read(self, reference: str) -> bytes: ...


class GcpSecretManagerStore:
    """Read from Google Secret Manager via gcloud.

    Through the CLI rather than the client library for the same reason the TPU
    provider does: the agent already authenticates gcloud, and adding a second
    credential path is a second thing to get wrong on a host whose identity is
    the whole security boundary.
    """

    def __init__(self, runner: tp.Callable[[tuple[str, ...]], subprocess.CompletedProcess[str]] | None = None):
        self._runner = runner or self._default_runner

    @staticmethod
    def _default_runner(command: tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)

    def read(self, reference: str) -> bytes:
        """Resolve ``gcp-secret://project/name[/version]``."""

        parsed = urlsplit(reference)
        project = parsed.netloc
        parts = [part for part in parsed.path.split("/") if part]
        if not project or not parts:
            raise SecretUnavailable(f"{reference!r} is not a gcp-secret://project/name reference")
        name, version = parts[0], (parts[1] if len(parts) > 1 else "latest")
        result = self._runner(
            (
                "gcloud",
                "secrets",
                "versions",
                "access",
                version,
                f"--secret={name}",
                f"--project={project}",
                "--format=get(payload.data)",
            )
        )
        if result.returncode != 0:
            # The stderr is included because the useful half is almost always
            # "permission denied on secret X for service account Y", which names
            # the exact grant an operator has to add.
            raise SecretUnavailable(f"could not read {reference!r}: {result.stderr.strip()}")
        return result.stdout.encode()


class FileSecretStore:
    """Read from a file the operator placed on the host.

    The local-development and Kubernetes-projected-volume case: a mounted
    Secret is a file, so the same reader serves both.
    """

    def read(self, reference: str) -> bytes:
        parsed = urlsplit(reference)
        path = Path(f"{parsed.netloc}{parsed.path}" if parsed.netloc else parsed.path)
        try:
            return path.read_bytes()
        except OSError as exc:
            raise SecretUnavailable(f"could not read {reference!r}: {exc}") from exc


_SCHEMES: dict[str, tp.Callable[[], SecretStore]] = {
    "gcp-secret": GcpSecretManagerStore,
    "file": FileSecretStore,
}


def secret_store_from_reference(reference: str) -> SecretStore:
    """Select the backend one reference names.

    Refuses an unknown scheme rather than defaulting to the file reader: a typo
    in ``gcp-secret`` would otherwise be read as a relative path, and the
    resulting error would name a missing file rather than a bad scheme.
    """

    scheme = urlsplit(reference).scheme
    factory = _SCHEMES.get(scheme)
    if factory is None:
        known = ", ".join(sorted(_SCHEMES))
        raise SecretUnavailable(f"{reference!r} uses unknown secret scheme {scheme!r}; known schemes: {known}")
    return factory()


def read_secret(reference: str) -> bytes:
    """Resolve one secret reference through the backend its scheme names."""

    return secret_store_from_reference(reference).read(reference)


__all__ = (
    "FileSecretStore",
    "GcpSecretManagerStore",
    "SecretStore",
    "SecretUnavailable",
    "read_secret",
    "secret_store_from_reference",
)
