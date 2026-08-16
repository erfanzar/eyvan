# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""What a run cost, and what it would have cost on demand.

The sentence a spot-first scheduler exists to say. It is also the easiest place
in the whole system to lie: an unpriced pool settles to zero, and a plausible
on-demand comparison could be produced from any discount constant. Both would
read as a fact and end up in somebody's budget, so most of these tests are
about saying *unknown* out loud.
"""

from __future__ import annotations

from click.testing import CliRunner

from eyvan.sdk import cli
from eyvan.sdk.spend import render, spend_of

SPOT_POOL = {
    "id": "pool-spot",
    "accelerator_type": "v5p-8",
    "capacity_class": "spot",
    "unit_prices": {"tpu": 1.2},
}
ON_DEMAND_POOL = {
    "id": "pool-od",
    "accelerator_type": "v5p-8",
    "capacity_class": "on-demand",
    "unit_prices": {"tpu": 4.0},
}


def _lease(cell_id: str, *, settled: float = 0.0, hours: float = 0.0, reserved: float = 0.0) -> dict:
    return {
        "id": f"lease-{cell_id}",
        "cell_id": cell_id,
        "settled_cost": settled,
        "reserved_cost": reserved,
        "accelerator_hours_settled": {"tpu": hours} if hours else {},
    }


# --------------------------------------------------------------------------- the arithmetic


def test_hours_and_cost_are_summed_across_every_lease() -> None:
    """A multislice run holds one lease per slice, and the bill is all of them."""

    spend = spend_of(
        [_lease("cell-a", settled=4.8, hours=4.0), _lease("cell-b", settled=4.8, hours=4.0)],
        priced_cells={"cell-a", "cell-b"},
    )

    assert spend.hours == 8.0
    assert spend.settled == 9.6
    assert spend.leases == 2
    assert spend.priced


def test_a_lease_from_an_unpriced_pool_makes_the_total_untrustworthy() -> None:
    """Half a bill presented as the whole one is worse than no bill."""

    spend = spend_of([_lease("cell-a", settled=4.8, hours=4.0), _lease("cell-b", hours=4.0)], priced_cells={"cell-a"})

    assert spend.hours == 8.0
    assert not spend.priced


def test_measurement_survives_when_pricing_does_not() -> None:
    """Accelerator-hours are measured, so they are known even when money is not."""

    spend = spend_of([_lease("cell-a", hours=16.0)], priced_cells=set())

    assert spend.hours == 16.0
    assert not spend.priced


def test_a_workload_that_never_leased_says_so() -> None:
    lines = render(
        "wl-1", "failed", spend_of([]), shape=None, capacity_class=None, on_demand_hourly=None, on_demand_source="n/a"
    )

    assert any("never held a lease" in line for line in lines)


# --------------------------------------------------------------------------- what it prints


def test_an_unpriced_run_reports_unknown_rather_than_zero() -> None:
    """$0.00 from an unpriced pool is the absence of a price, not of a bill."""

    lines = render(
        "wl-1",
        "succeeded",
        spend_of([_lease("cell-a", hours=16.0)], priced_cells=set()),
        shape="v5p-8",
        capacity_class="spot",
        on_demand_hourly=None,
        on_demand_source="no on-demand pool declares a price",
    )

    assert any("cost        unknown" in line for line in lines)
    assert not any("$0.00" in line for line in lines)


def test_a_priced_run_shows_what_it_cost_and_what_it_saved() -> None:
    """The whole point, when the fleet actually declares both prices."""

    lines = render(
        "wl-1",
        "succeeded",
        spend_of([_lease("cell-a", settled=9.6, hours=8.0)], priced_cells={"cell-a"}),
        shape="v5p-8",
        capacity_class="spot",
        on_demand_hourly=4.0,
        on_demand_source="declared by pool pool-od",
    )
    rendered = "\n".join(lines)

    assert "cost        $9.60" in rendered
    assert "on demand   $32.00" in rendered
    assert "saved       $22.40" in rendered
    assert "70% less" in rendered


def test_no_saving_is_claimed_when_the_run_itself_is_unpriced() -> None:
    """Subtracting a real price from an unknown one produces a fictional saving."""

    lines = render(
        "wl-1",
        "succeeded",
        spend_of([_lease("cell-a", hours=8.0)], priced_cells=set()),
        shape="v5p-8",
        capacity_class="spot",
        on_demand_hourly=4.0,
        on_demand_source="declared by pool pool-od",
    )
    rendered = "\n".join(lines)

    assert "on demand   $32.00" in rendered
    assert "saved" not in rendered


# --------------------------------------------------------------------------- the command


class _Plane:
    def __init__(self, *, pools: list[dict], leases: list[dict], workload: dict) -> None:
        self.pools = pools
        self.leases = leases
        self.workload = workload

    def request(self, method, path, *, params=None, json=None, headers=None):
        del method, json, headers, params
        if path == "/api/v1/capacity-pools":
            return self.pools
        if path == "/api/v1/leases":
            return self.leases
        return self.workload

    def request_text(self, method, path, *, params=None, headers=None):
        del method, path, params, headers
        return ""

    def replace_identity_headers(self, headers) -> None:
        del headers


def _workload(pool_id: str) -> dict:
    return {
        "id": "wl-1",
        "state": "succeeded",
        "spec": {"metadata": {"eyvan_pool": pool_id}},
    }


def _invoke(monkeypatch, plane: _Plane, *args: str):
    monkeypatch.setattr(cli, "EyvanClient", lambda *_a, **_k: plane)
    return CliRunner().invoke(
        cli.main, ["--user", "alice", "--org", "acme", "--project", "research", "workload", "cost", "wl-1", *args]
    )


def test_the_command_compares_against_a_declared_on_demand_pool(monkeypatch) -> None:
    """The one source of an honest comparison: a price somebody wrote down."""

    plane = _Plane(
        pools=[SPOT_POOL, ON_DEMAND_POOL],
        leases=[_lease("cell-a", settled=9.6, hours=8.0)],
        workload=_workload("pool-spot"),
    )

    result = _invoke(monkeypatch, plane)

    assert result.exit_code == 0, result.output
    assert "$32.00" in result.output
    assert "pool-od" in result.output


def test_without_an_on_demand_pool_the_comparison_says_how_to_get_one(monkeypatch) -> None:
    """A spot-first fleet declares no on-demand pool, which is most fleets."""

    plane = _Plane(pools=[SPOT_POOL], leases=[_lease("cell-a", settled=9.6, hours=8.0)], workload=_workload("pool-spot"))

    result = _invoke(monkeypatch, plane)

    assert result.exit_code == 0, result.output
    assert "--on-demand-price" in result.output


def test_a_supplied_price_is_used_and_attributed(monkeypatch) -> None:
    """An operator's own number, labelled as theirs rather than as the fleet's."""

    plane = _Plane(pools=[SPOT_POOL], leases=[_lease("cell-a", settled=9.6, hours=8.0)], workload=_workload("pool-spot"))

    result = _invoke(monkeypatch, plane, "--on-demand-price", "4.0")

    assert result.exit_code == 0, result.output
    assert "$32.00" in result.output
    assert "--on-demand-price" in result.output


def test_the_shape_the_run_used_is_named(monkeypatch) -> None:
    plane = _Plane(pools=[SPOT_POOL], leases=[_lease("cell-a", hours=8.0)], workload=_workload("pool-spot"))

    result = _invoke(monkeypatch, plane)

    assert "v5p-8" in result.output
    assert "spot" in result.output
