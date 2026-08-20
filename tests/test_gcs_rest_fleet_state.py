# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The API path for fleet state must behave exactly like the subprocess one.

Every fleet-state mutation is a compare-and-swap, and the subprocess form spends
three interpreter startups on it. Releases serialise on this document, so that
cost is what left a hundred cells in ``deleting`` for hours with the provider
already empty.

Replacing it is only safe if the two paths are indistinguishable to a caller:
the same generation semantics, the same ``ConflictError`` on a lost race, and a
fallback rather than a failure when the API cannot be reached at all.
"""

from __future__ import annotations

import pytest

from eyvan.capacity.fleet_state import ConflictError, GcsBackend
from eyvan.capacity.gcs_rest import GcsPreconditionFailed, GcsRestError, split_uri


def test_split_uri_requires_a_bucket_and_an_object() -> None:
    assert split_uri("gs://bucket/a/b.json") == ("bucket", "a/b.json")
    for bad in ("gs://bucket", "gs://bucket/", "https://bucket/x", "gs:///x"):
        with pytest.raises(ValueError):
            split_uri(bad)


class _FakeRest:
    """A REST object whose reads and writes are decided by the test."""

    def __init__(self, *, body=None, generation=0, on_write=None):
        self.body = body
        self.generation = generation
        self.on_write = on_write
        self.writes: list[tuple[dict, int]] = []

    def read(self):
        return self.body, self.generation

    def write(self, document, generation):
        self.writes.append((document, generation))
        if self.on_write is not None:
            raise self.on_write


def _runner_that_must_not_run(_args):
    raise AssertionError("the subprocess path ran while the API path was available")


def test_an_absent_object_reads_as_an_empty_document_at_generation_zero() -> None:
    """Generation zero is GCS's own spelling for 'create only if still absent'."""

    backend = GcsBackend("gs://bucket/state.json", runner=_runner_that_must_not_run, rest=_FakeRest())

    document, generation = backend._read_fast()

    assert generation == 0
    assert document["cells"] == {}


def test_a_lost_race_is_reported_as_a_conflict_exactly_as_the_subprocess_path_does() -> None:
    """The caller re-reads and reapplies; that is the compare-and-swap."""

    rest = _FakeRest(on_write=GcsPreconditionFailed("changed"))
    backend = GcsBackend("gs://bucket/state.json", runner=_runner_that_must_not_run, rest=rest)

    with pytest.raises(ConflictError):
        backend._write_fast({"schema": "eyvan.fleet-state", "version": 1, "cells": {}}, 7)


def test_an_unreachable_api_falls_back_instead_of_failing_the_mutation() -> None:
    """An API that cannot be reached is not a conflict and must not look like one."""

    calls: list[tuple[str, ...]] = []

    def runner(args):
        calls.append(tuple(args))
        raise RuntimeError("subprocess path reached")

    rest = _FakeRest(on_write=GcsRestError("no route"))
    backend = GcsBackend("gs://bucket/state.json", runner=runner, rest=rest)

    with pytest.raises(RuntimeError, match="subprocess path reached"):
        backend._write_fast({"schema": "eyvan.fleet-state", "version": 1, "cells": {}}, 7)
    assert calls, "the fallback was never attempted"
    # And the failed reader is dropped, so one outage does not cost a failed
    # round trip on every later mutation.
    assert backend._rest is None
