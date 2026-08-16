# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""`eyvan quickstart` tells you the one next thing, and never spends money for you.

A first-run command has two ways to fail. It can be useless -- reporting state
without saying what to do about it -- or it can be dangerous, provisioning a
control plane and allocating accelerators because somebody typed a friendly
word. These tests hold both lines.
"""

from __future__ import annotations

from click.testing import CliRunner

from eyvan.records import CommandExecution, WorkloadDefinition, WorkloadRecord, WorkloadState
from eyvan.sdk import cli
from eyvan.sdk.commands.quickstart import HELLO_COMMAND


def _finished() -> dict:
    """One workload record the follower can read as done."""

    return WorkloadRecord(
        id="wl-1",
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(name="quickstart", execution=CommandExecution(command=HELLO_COMMAND)),
        state=WorkloadState.SUCCEEDED,
        created_by="alice",
    ).model_dump(mode="json")


POOL = {
    "id": "pool-a",
    "fleet_name": "research-fleet",
    "node_type": "v5p-workers",
    "provider": "gcp",
    "region": "us-east5",
    "zone": "us-east5-a",
    "accelerator_type": "v5p-8",
    "capacity_class": "spot",
    "enabled": True,
    "allocatable": {"cpu": 0.0, "memory_gib": 0.0, "gpu": 0, "tpu": 8},
}


class _Plane:
    """A control plane scripted per route, recording everything asked of it."""

    def __init__(self, **routes) -> None:
        self.routes = routes
        self.calls: list[tuple[str, str]] = []
        self.unreachable = False

    def request(self, method, path, *, params=None, json=None, headers=None):
        del params, json, headers
        self.calls.append((method, path))
        if self.unreachable:
            raise ConnectionError("no control plane here")
        if path == "/api/v1/health":
            return self.routes.get("health", {"status": "ok"})
        if path == "/api/v1/fleets":
            return self.routes.get("fleets", ["research-fleet"])
        if path == "/api/v1/capacity-pools":
            return self.routes.get("pools", [POOL])
        if path == "/api/v1/workloads/estimate":
            return self.routes.get(
                "estimate",
                {"candidates": [{"pool_id": "pool-a", "estimated_cost": 0.42, "price_source": "pool:pool-a"}]},
            )
        if method == "POST":
            return self.routes.get("submitted", _finished())
        if path.endswith("/log-page"):
            return {"attempt": 1, "records": [], "next_cursor": "", "unavailable": False, "detail": ""}
        if path.startswith("/api/v1/workloads/"):
            return _finished()
        return {}

    def request_text(self, method, path, *, params=None, headers=None):
        del method, path, params, headers
        return ""

    def replace_identity_headers(self, headers) -> None:
        del headers


def _submissions(plane: _Plane) -> list[str]:
    """Return the calls that actually create a workload, not the ones that read."""

    return [path for method, path in plane.calls if method == "POST" and "node-types" in path]


def _invoke(monkeypatch, plane: _Plane, *args: str):
    monkeypatch.setattr(cli, "EyvanClient", lambda *_a, **_k: plane)
    return CliRunner().invoke(
        cli.main, ["--user", "alice", "--org", "acme", "--project", "research", "quickstart", *args]
    )


# --------------------------------------------------------------------------- the guide


def test_no_control_plane_prints_the_command_that_makes_one(monkeypatch) -> None:
    """The first wall anyone hits, and the one with no obvious next move."""

    plane = _Plane()
    plane.unreachable = True

    result = _invoke(monkeypatch, plane)

    assert result.exit_code == 0, result.output
    assert "eyvan head up" in result.output
    assert "eyvan-server" in result.output, "a laptop should not be told to provision a VM"


def test_no_fleet_prints_the_command_that_applies_one(monkeypatch) -> None:
    plane = _Plane(fleets=[])

    result = _invoke(monkeypatch, plane)

    assert result.exit_code == 0, result.output
    assert "eyvan up" in result.output
    assert "/api/v1/capacity-pools" not in [path for _method, path in plane.calls], "stop at the first missing step"


def test_a_fleet_with_no_pools_says_so_rather_than_offering_a_job(monkeypatch) -> None:
    plane = _Plane(pools=[])

    result = _invoke(monkeypatch, plane)

    assert result.exit_code == 0, result.output
    assert "declares no pools" in result.output
    assert HELLO_COMMAND not in result.output


def test_disabled_pools_are_not_capacity(monkeypatch) -> None:
    """A disabled pool provisions nothing, so a job offered against it waits forever."""

    plane = _Plane(pools=[{**POOL, "enabled": False}])

    result = _invoke(monkeypatch, plane)

    assert "disabled" in result.output
    assert HELLO_COMMAND not in result.output


def test_a_ready_fleet_is_offered_a_first_job_with_its_cost(monkeypatch) -> None:
    """The end of the path: the exact command, and what it will cost."""

    plane = _Plane()

    result = _invoke(monkeypatch, plane)

    assert result.exit_code == 0, result.output
    assert "eyvan run --tpu v5p-8" in result.output
    assert "$0.42" in result.output


def test_an_unpriced_pool_says_unknown_rather_than_zero(monkeypatch) -> None:
    """A confident $0.00 is the number somebody budgets against."""

    plane = _Plane(estimate={"candidates": [{"pool_id": "pool-a", "estimated_cost": 0.0, "price_source": None}]})

    result = _invoke(monkeypatch, plane)

    assert "declares no price" in result.output
    assert "$0.00" not in result.output


def test_an_estimate_matching_nothing_is_reported_as_that(monkeypatch) -> None:
    """Distinct from free, and the more useful thing to see before submitting."""

    plane = _Plane(estimate={"candidates": []})

    result = _invoke(monkeypatch, plane)

    assert "no pool in this fleet matches" in result.output


# --------------------------------------------------------------------------- the money


def test_nothing_is_submitted_without_saying_yes(monkeypatch) -> None:
    """The line this command must not cross. Real hardware, real billing."""

    plane = _Plane()

    result = _invoke(monkeypatch, plane)

    assert result.exit_code == 0, result.output
    # The estimate is also a POST and writes nothing; a submission is the one
    # that names a node type, and that is the call this must not make.
    assert not _submissions(plane), "quickstart must not submit unasked"
    assert "--yes" in result.output


def test_confirming_submits_and_streams_the_first_job(monkeypatch) -> None:
    """With consent it does the whole thing, including waiting for the answer."""

    plane = _Plane()
    monkeypatch.setattr("eyvan.sdk.follow.time.sleep", lambda _seconds: None)

    result = _invoke(monkeypatch, plane, "--yes")

    assert _submissions(plane), result.output


def test_an_explicit_shape_must_exist_in_the_fleet(monkeypatch) -> None:
    """Offering a job on a shape no pool provides wastes the user's next minute."""

    plane = _Plane()

    result = _invoke(monkeypatch, plane, "--tpu", "v6e-256")

    assert result.exit_code != 0
    assert "v5p-8" in result.output, "say what the fleet does provide"
