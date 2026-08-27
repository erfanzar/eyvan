# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Serving managed artifacts from the region that needs them.

A multi-region fleet pays for every artifact read twice: once in egress and once
in the latency of pulling a workspace across a continent before a rank can start.
The bytes are identical in every region, so the second pull is pure waste.

What makes this cheap to do safely is that managed artifacts are already
**content-addressed** -- ``callable_uri(sha256)`` and ``workspace_uri(sha256)``
name an object by the digest of its contents, and :mod:`eyvan.artifacts.managed`
verifies that digest on read. A mirror is therefore immutable by construction:
there is no version skew to reconcile, no invalidation to get wrong, and a
corrupted copy fails the existing check rather than being served. This is a
cache in the one situation where a cache needs no coherence protocol.

Mirrors are filled on demand rather than pre-seeded. Most artifacts are read in
one region, and copying every object everywhere would trade an egress bill for a
storage bill plus the writes to produce it.
"""

from __future__ import annotations

import logging
import typing as tp
from dataclasses import dataclass

from eyvan.artifacts.managed import ManagedArtifactObjectMetadata, ManagedArtifactStore

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RegionMirroredArtifactStore:
    """Read through a region's mirror, write through to the origin.

    ``mirror`` is the store for the region this control plane serves, and
    ``origin`` is the shared one every region can reach. With no mirror
    configured the composition is not used at all, so a single-region fleet runs
    exactly the code it ran before.
    """

    origin: ManagedArtifactStore
    mirror: ManagedArtifactStore
    region: str

    def put(self, uri: str, payload: bytes, *, org_id: str, project_id: str) -> str:
        """Write to the origin, which is the copy every region can reach.

        Not written to the mirror here. A written artifact is not necessarily a
        read one -- a result uploaded by a finished job may never be fetched
        again -- and populating on write would fill the mirror with exactly the
        objects nothing asks for.
        """

        return self.origin.put(uri, payload, org_id=org_id, project_id=project_id)

    def get(self, uri: str, *, org_id: str, project_id: str) -> bytes:
        """Serve from the mirror, filling it from the origin on a miss.

        A mirror failure is logged and falls back to the origin rather than
        propagating. The mirror is an optimisation, and a region whose bucket is
        misconfigured should run slower, not fail to place work -- which is what
        raising here would do to every rank in that region at once.
        """

        try:
            if self.mirror.exists(uri, org_id=org_id, project_id=project_id):
                return self.mirror.get(uri, org_id=org_id, project_id=project_id)
        except Exception:
            logger.warning("artifact mirror in %s could not serve %s; using the origin", self.region, uri)
            return self.origin.get(uri, org_id=org_id, project_id=project_id)

        payload = self.origin.get(uri, org_id=org_id, project_id=project_id)
        try:
            self.mirror.put(uri, payload, org_id=org_id, project_id=project_id)
        except Exception:
            # The read succeeded; only the next one will be slower. Failing the
            # caller because a cache write failed would turn a cost optimisation
            # into an availability risk.
            logger.warning("artifact mirror in %s could not store %s; serving from the origin", self.region, uri)
        return payload

    def exists(self, uri: str, *, org_id: str, project_id: str) -> bool:
        """Ask the origin, which is authoritative.

        Deliberately not answered from the mirror. A mirror that has not been
        filled yet would report an artifact absent when it exists, and callers
        use this to decide whether an upload is needed -- so a mirror miss would
        become a redundant upload, or worse, a "missing" artifact for a job whose
        inputs are perfectly present.
        """

        return self.origin.exists(uri, org_id=org_id, project_id=project_id)

    def delete(self, uri: str, *, org_id: str, project_id: str) -> bool:
        """Delete from the origin *and* every mirror holding a copy.

        The half that keeps mirrors from quietly defeating retention. A deleted
        artifact whose copies survive is still stored, still billed, and still
        readable by anything that reaches that region -- so the collector's work
        would be undone by the cache it never knew about.

        The origin's answer is the return value: it is the authority on whether
        an object existed. A mirror failure is logged rather than raised, because
        the collector is at-least-once and a raise would stall the whole sweep on
        one region's bucket.
        """

        try:
            self.mirror.delete(uri, org_id=org_id, project_id=project_id)
        except Exception:
            logger.warning("artifact mirror in %s could not delete %s; it will be retried", self.region, uri)
        return self.origin.delete(uri, org_id=org_id, project_id=project_id)

    def stat(self, uri: str, *, org_id: str, project_id: str) -> ManagedArtifactObjectMetadata | None:
        """Describe the origin's object, for the same reason ``exists`` does."""

        return self.origin.stat(uri, org_id=org_id, project_id=project_id)


def mirrored_store(
    origin: ManagedArtifactStore,
    mirror: ManagedArtifactStore | None,
    *,
    region: str | None,
) -> ManagedArtifactStore:
    """Return ``origin`` unchanged unless a mirror is configured for ``region``.

    Composed rather than switched on inside the store: a fleet with no mirror
    configured runs the original object, so the behaviour it has today cannot
    change by accident.
    """

    if mirror is None or not region:
        return origin
    return tp.cast(ManagedArtifactStore, RegionMirroredArtifactStore(origin=origin, mirror=mirror, region=region))


__all__ = ("RegionMirroredArtifactStore", "mirrored_store")
