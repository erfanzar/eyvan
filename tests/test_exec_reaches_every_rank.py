# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""`eyvan workload exec` could not reach a rank of a multi-host slice.

Two gates stand between the command and a host. The *admission* gate is
``AgentCapabilities.supports_exec``, which the GCP driver declared a literal
``False`` under a comment promising operator control -- fixed separately. The
*implementation* gate is ``ExecCapableExecutionDriver``, and this is that half.

``exec_in_workload`` existed once, on the local systemd driver, so a single-host
cell could exec while a TPU slice could not: the slice is driven over each host's
native worker HTTP API, and that API exposed no exec route. Measured on a live
v5p-16 on 2026-08-13, where the refusal read *"execution backend 'eyvan-native'
cannot exec beside a workload"* with a trainer running on both hosts.

The fan-out reports an unreachable rank in place rather than returning the hosts
it could reach, which is the same rule ``process_status`` and the structured-log
fan-out follow, and the rule P2 qualified against the same day.
"""

from __future__ import annotations

import typing as tp

import httpx
import pytest

from eyvan.worker.runtime.native_slice import NativeSliceExecutor
from eyvan.worker.runtime.registry import ExecCapableExecutionDriver


class _Worker:
    """One host's native-worker client, answering or refusing to."""

    def __init__(self, name: str, *, error: Exception | None = None) -> None:
        self.name = name
        self.error = error
        self.calls: list[tuple[str, tuple[str, ...], float]] = []

    def exec_in_workload(self, remote_id: str, command: tp.Sequence[str], *, timeout_s: float = 30.0) -> dict:
        self.calls.append((remote_id, tuple(command), timeout_s))
        if self.error is not None:
            raise self.error
        return {"host": self.name, "exit_code": 0}

    def close(self) -> None:
        return None


def _slice(*workers: _Worker) -> NativeSliceExecutor:
    """Build a slice executor over stub hosts without touching the network."""

    executor = NativeSliceExecutor.__new__(NativeSliceExecutor)
    executor.endpoints = tuple(f"http://{worker.name}:8080" for worker in workers)
    executor._workers = tuple(workers)
    return executor


def test_the_slice_executor_now_satisfies_the_exec_protocol() -> None:
    """The gate the agent checks before it will exec at all."""

    assert issubclass(NativeSliceExecutor, ExecCapableExecutionDriver)


def test_exec_runs_on_every_host_of_the_slice() -> None:
    """One rank cannot distinguish a hung collective from a slow step."""

    workers = (_Worker("w0"), _Worker("w1"))
    result = _slice(*workers).exec_in_workload("rem-1", ["py-spy", "dump"], timeout_s=12.0)

    assert [rank["rank"] for rank in result["ranks"]] == [0, 1]
    assert [rank["result"]["host"] for rank in result["ranks"]] == ["w0", "w1"]
    for worker in workers:
        assert worker.calls == [("rem-1", ("py-spy", "dump"), 12.0)]


@pytest.mark.parametrize(
    "error",
    [httpx.ConnectError("refused"), RuntimeError("boom")],
    ids=["unreachable", "failed"],
)
def test_a_rank_that_cannot_answer_is_reported_in_place(error: Exception) -> None:
    """The rule P2 qualified, applied to exec.

    Returning only the hosts that answered would show a complete-looking result
    for a half-dead slice -- and exec is used precisely when one rank is
    suspected of differing from the rest, so dropping the suspect one is the
    worst possible answer.
    """

    result = _slice(_Worker("w0"), _Worker("w1", error=error)).exec_in_workload("rem-1", ["true"])

    assert [rank["rank"] for rank in result["ranks"]] == [0, 1]
    assert result["ranks"][0]["result"]["exit_code"] == 0
    assert result["ranks"][1]["result"]["observed"] is False
    assert result["ranks"][1]["endpoint"] == "http://w1:8080"


def test_the_worker_exposes_an_exec_route_for_a_slice_to_call() -> None:
    """The missing link: the slice had nothing to fan out to."""

    from eyvan.worker.runtime.native_remote import NativeRemoteExecutor

    assert hasattr(NativeRemoteExecutor, "exec_in_workload")


def test_the_gang_executor_reaches_every_rank_too() -> None:
    """A multi-cell group has the same property and the same failure mode."""

    from eyvan.worker.runtime.native_gang import NativeGangExecutor

    assert issubclass(NativeGangExecutor, ExecCapableExecutionDriver)
