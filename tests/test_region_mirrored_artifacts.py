# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Serving artifacts from the region that needs them.

A multi-region fleet pulls every workspace across a continent before a rank can
start, and pays for it twice -- in egress and in the latency ahead of the job.
The bytes are identical everywhere, so the second pull is waste.

The three properties worth testing are the ones that decide whether the cache is
safe rather than whether it is fast: a miss fills, a mirror failure degrades to
the origin instead of failing the job, and a deletion reaches the copies. That
last one is what stops the mirror quietly defeating retention -- a deleted
artifact whose regional copies survive is still stored, still billed, and still
readable.
"""

from __future__ import annotations

import pytest

from eyvan.artifacts.mirrored import RegionMirroredArtifactStore, mirrored_store


class FakeStore:
    """The protocol, in a dict, counting what it was asked to do."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.reads = 0
        self.writes = 0

    def put(self, uri: str, payload: bytes, *, org_id: str, project_id: str) -> str:
        del org_id, project_id
        self.writes += 1
        self.objects[uri] = payload
        return uri

    def get(self, uri: str, *, org_id: str, project_id: str) -> bytes:
        del org_id, project_id
        self.reads += 1
        return self.objects[uri]

    def exists(self, uri: str, *, org_id: str, project_id: str) -> bool:
        del org_id, project_id
        return uri in self.objects

    def delete(self, uri: str, *, org_id: str, project_id: str) -> bool:
        del org_id, project_id
        return self.objects.pop(uri, None) is not None

    def stat(self, uri: str, *, org_id: str, project_id: str):
        del org_id, project_id
        return None


class BrokenStore(FakeStore):
    """A region whose bucket is misconfigured, which must not stop the fleet."""

    def get(self, uri: str, *, org_id: str, project_id: str) -> bytes:
        raise OSError("regional bucket is unreachable")

    def put(self, uri: str, payload: bytes, *, org_id: str, project_id: str) -> str:
        raise OSError("regional bucket is unreachable")

    def exists(self, uri: str, *, org_id: str, project_id: str) -> bool:
        raise OSError("regional bucket is unreachable")

    def delete(self, uri: str, *, org_id: str, project_id: str) -> bool:
        raise OSError("regional bucket is unreachable")


def _scoped(store) -> dict:
    return {"org_id": "acme", "project_id": "research"}


def test_a_miss_fills_the_mirror_and_the_next_read_stops_crossing_regions() -> None:
    """The whole point, stated as the origin not being read twice."""

    origin, mirror = FakeStore(), FakeStore()
    origin.put("eyvan://workspaces/abc", b"payload", **_scoped(origin))
    store = RegionMirroredArtifactStore(origin=origin, mirror=mirror, region="us-east5")

    first = store.get("eyvan://workspaces/abc", **_scoped(origin))
    origin_reads_after_fill = origin.reads
    second = store.get("eyvan://workspaces/abc", **_scoped(origin))

    assert first == second == b"payload"
    assert mirror.objects["eyvan://workspaces/abc"] == b"payload"
    assert origin.reads == origin_reads_after_fill, "the second read must not cross regions"


def test_a_broken_mirror_serves_from_the_origin_rather_than_failing_the_job() -> None:
    """A misconfigured region should run slower, not stop placing work.

    Raising here would fail every rank in that region at once, turning a cost
    optimisation into an availability risk -- a strictly worse position than not
    having the mirror at all.
    """

    origin, mirror = FakeStore(), BrokenStore()
    origin.put("eyvan://workspaces/abc", b"payload", **_scoped(origin))
    store = RegionMirroredArtifactStore(origin=origin, mirror=mirror, region="us-east5")

    assert store.get("eyvan://workspaces/abc", **_scoped(origin)) == b"payload"


def test_a_mirror_that_cannot_be_written_still_returns_the_bytes() -> None:
    """The read succeeded; only the next one is slower."""

    origin = FakeStore()
    origin.put("eyvan://workspaces/abc", b"payload", **_scoped(origin))

    class WriteOnlyBroken(FakeStore):
        def put(self, uri, payload, *, org_id, project_id):
            raise OSError("regional bucket is full")

    store = RegionMirroredArtifactStore(origin=origin, mirror=WriteOnlyBroken(), region="us-east5")

    assert store.get("eyvan://workspaces/abc", **_scoped(origin)) == b"payload"


def test_deleting_an_artifact_removes_the_regional_copy_too() -> None:
    """Otherwise retention is defeated by a cache it never knew about.

    A collected artifact whose mirror survives is still stored, still billed and
    still readable by anything reaching that region.
    """

    origin, mirror = FakeStore(), FakeStore()
    origin.put("eyvan://results/abc", b"payload", **_scoped(origin))
    mirror.put("eyvan://results/abc", b"payload", **_scoped(mirror))
    store = RegionMirroredArtifactStore(origin=origin, mirror=mirror, region="us-east5")

    removed = store.delete("eyvan://results/abc", **_scoped(origin))

    assert removed is True
    assert "eyvan://results/abc" not in mirror.objects
    assert "eyvan://results/abc" not in origin.objects


def test_existence_is_answered_by_the_origin_not_the_mirror() -> None:
    """A cold mirror must not report a present artifact as missing.

    Callers use this to decide whether an upload is needed, so answering from an
    unfilled mirror would turn every first read in a region into a redundant
    upload -- or report a job's inputs missing when they are perfectly present.
    """

    origin, mirror = FakeStore(), FakeStore()
    origin.put("eyvan://workspaces/abc", b"payload", **_scoped(origin))
    store = RegionMirroredArtifactStore(origin=origin, mirror=mirror, region="us-east5")

    assert store.exists("eyvan://workspaces/abc", **_scoped(origin)) is True


def test_a_fleet_with_no_mirror_configured_is_left_exactly_as_it_was() -> None:
    """The single-region path must not acquire a wrapper it has no use for."""

    origin = FakeStore()

    assert mirrored_store(origin, None, region="us-east5") is origin
    assert mirrored_store(origin, FakeStore(), region=None) is origin


@pytest.mark.parametrize("uri", ["eyvan://workspaces/abc", "eyvan://callables/def"])
def test_writes_go_to_the_origin_every_region_can_reach(uri: str) -> None:
    """A written artifact is not necessarily a read one.

    Populating on write would fill each region with the objects nothing there
    ever asks for -- results uploaded by a finished job being the common case.
    """

    origin, mirror = FakeStore(), FakeStore()
    store = RegionMirroredArtifactStore(origin=origin, mirror=mirror, region="us-east5")

    store.put(uri, b"payload", **_scoped(origin))

    assert origin.objects[uri] == b"payload"
    assert uri not in mirror.objects
