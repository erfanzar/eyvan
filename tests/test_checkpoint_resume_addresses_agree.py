# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Where a checkpoint is written, and where a resume reads it from.

Found on a live 2x v5p-16 on 2026-08-02, and only findable there. A batch
training job was preempted by a production job; every step of the protocol
worked -- plan, checkpoint hook, stop, capacity handed over, victim requeued as
attempt 2 carrying its checkpoint URI. Then attempt 2 logged::

    RESUME: reading gs://.../preempt-demo
    RESUME: gs://.../preempt-demo is absent; starting from step 0

The hook had written ``gs://.../preempt-demo/rank-0`` and ``/rank-1``. The write
path rank-suffixes through ``_rank_checkpoint_uri``; the resume path passed the
bare base URI. Nothing raised: the workload saw "no checkpoint", restarted from
zero, and 252 completed training steps were silently discarded while every
record said the resume had succeeded.

The two addresses have to be derived the same way, so these tests assert they
agree rather than asserting either one's shape.
"""

from __future__ import annotations

import pytest

from eyvan.worker.runtime.native_slice import _RESUME_CHECKPOINT_ENV
from eyvan.worker.runtime.systemd_transient import _rank_checkpoint_uri

BASE = "gs://bucket/eyvan/checkpoints/job"


def _resume_env(executor, spec: dict, rank: int) -> dict[str, str]:
    return dict(executor._ranked_spec(spec, rank)["environment"])


@pytest.mark.parametrize("rank", [0, 1, 3])
def test_a_rank_resumes_from_the_address_its_hook_wrote(rank: int) -> None:
    """The property that broke: write address and read address must match.

    Asserted as agreement between the two derivations rather than against a
    literal, so changing the layout cannot leave one side behind again.
    """

    from eyvan.worker.runtime.native_slice import NativeSliceExecutor

    executor = NativeSliceExecutor.__new__(NativeSliceExecutor)
    executor._workers = (None,) * 4  # type: ignore[attr-defined]
    executor.jax_cluster_detection = _detection()  # type: ignore[attr-defined]
    spec = {"entrypoint": "python train.py", "environment": {_RESUME_CHECKPOINT_ENV: BASE}}

    resume = _resume_env(executor, spec, rank)[_RESUME_CHECKPOINT_ENV]

    assert resume == _rank_checkpoint_uri(BASE, rank)
    assert resume.endswith(f"/rank-{rank}")


def _detection():
    from eyvan.worker.runtime.native_slice import JaxClusterDetectionMode

    return JaxClusterDetectionMode.GCE_TPU


def test_two_ranks_never_resume_from_the_same_object() -> None:
    """Each rank checkpointed its own state; sharing one would corrupt a resume."""

    from eyvan.worker.runtime.native_slice import NativeSliceExecutor

    executor = NativeSliceExecutor.__new__(NativeSliceExecutor)
    executor._workers = (None,) * 2  # type: ignore[attr-defined]
    executor.jax_cluster_detection = _detection()  # type: ignore[attr-defined]
    spec = {"entrypoint": "python train.py", "environment": {_RESUME_CHECKPOINT_ENV: BASE}}

    first = _resume_env(executor, spec, 0)[_RESUME_CHECKPOINT_ENV]
    second = _resume_env(executor, spec, 1)[_RESUME_CHECKPOINT_ENV]

    assert first != second


def test_a_first_attempt_is_given_no_resume_address() -> None:
    """An empty resume URI must stay empty, not become ``/rank-0``.

    A workload starting fresh would otherwise be pointed at an object that does
    not exist and cannot: there is nothing to resume from.
    """

    from eyvan.worker.runtime.native_slice import NativeSliceExecutor

    executor = NativeSliceExecutor.__new__(NativeSliceExecutor)
    executor._workers = (None,) * 2  # type: ignore[attr-defined]
    executor.jax_cluster_detection = _detection()  # type: ignore[attr-defined]
    spec = {"entrypoint": "python train.py", "environment": {_RESUME_CHECKPOINT_ENV: ""}}

    assert _resume_env(executor, spec, 0)[_RESUME_CHECKPOINT_ENV] == ""


def test_a_workload_with_no_resume_variable_is_left_alone() -> None:
    """Most workloads never checkpoint; the fix must not invent the variable."""

    from eyvan.worker.runtime.native_slice import NativeSliceExecutor

    executor = NativeSliceExecutor.__new__(NativeSliceExecutor)
    executor._workers = (None,) * 2  # type: ignore[attr-defined]
    executor.jax_cluster_detection = _detection()  # type: ignore[attr-defined]
    spec = {"entrypoint": "python train.py", "environment": {}}

    assert _RESUME_CHECKPOINT_ENV not in _resume_env(executor, spec, 0)


def test_a_trailing_slash_does_not_produce_a_double_separator() -> None:
    """Operators write both forms; both must resolve to the same object."""

    assert _rank_checkpoint_uri(f"{BASE}/", 0) == _rank_checkpoint_uri(BASE, 0)
