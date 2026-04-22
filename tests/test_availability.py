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
from datetime import datetime, timedelta

from eyvan._compat import UTC
from eyvan.core import CapacityBelief, CapacityPoolKey, ProvisionOutcome
from eyvan.core.availability import p95_latency
from eyvan.service.models import CapacityBeliefState, CellRecord, CellState
from eyvan.service.models import ResourceVector as ApiResourceVector
from eyvan.service.scheduling import to_core_cell


def belief() -> CapacityBelief:
    return CapacityBelief(CapacityPoolKey("gcp", "project-a", "us-central2-b", "v5p-16", "spot"))


def test_success_updates_probability_and_latency() -> None:
    now = datetime.now(UTC)

    updated = belief().observe(ProvisionOutcome.SUCCESS, now=now, latency_s=90)

    assert updated.success_probability > 0.5
    assert updated.provisioning_latency_p95_s == 90
    assert updated.cooldown_until is None


def test_stockout_applies_exponential_cooldown() -> None:
    now = datetime.now(UTC)

    first = belief().observe(ProvisionOutcome.STOCKOUT, now=now)
    second = first.observe(ProvisionOutcome.STOCKOUT, now=now)

    assert first.cooldown_until == now + timedelta(seconds=60)
    assert second.cooldown_until == now + timedelta(seconds=120)
    assert second.success_probability < first.success_probability


def test_probe_requires_value_budget_uncertainty_and_expired_cooldown() -> None:
    now = datetime.now(UTC)
    available = belief()
    cooling = available.observe(ProvisionOutcome.STOCKOUT, now=now)

    assert available.should_probe(
        blocked_value_usd=100,
        probe_cost_usd=5,
        probe_budget_remaining_usd=10,
        now=now,
    )
    assert not cooling.should_probe(
        blocked_value_usd=100,
        probe_cost_usd=5,
        probe_budget_remaining_usd=10,
        now=now,
    )
    assert not available.should_probe(
        blocked_value_usd=4,
        probe_cost_usd=5,
        probe_budget_remaining_usd=10,
        now=now,
    )


def test_p95_latency_uses_the_shared_nearest_rank_rule() -> None:
    assert p95_latency(()) is None
    assert p95_latency((90,)) == 90
    # ceil(20 * 0.95) - 1 == 18, i.e. the 19th smallest sample.
    assert p95_latency(tuple(float(value) for value in range(1, 21))) == 19


def test_belief_latency_p95_matches_the_shared_helper() -> None:
    latencies = (120.0, 30.0, 60.0, 45.0)
    updated = belief()
    for latency in latencies:
        updated = updated.observe(ProvisionOutcome.SUCCESS, latency_s=latency)

    assert updated.provisioning_latency_p95_s == p95_latency(latencies) == 120.0


def test_service_adapter_computes_the_same_p95() -> None:
    record = CellRecord(
        id="cell",
        provider="local",
        region="local",
        state=CellState.READY,
        capacity=ApiResourceVector(cpu=8),
        allocatable=ApiResourceVector(cpu=8),
        availability=CapacityBeliefState(provision_latencies_s=tuple(float(value) for value in range(1, 21))),
    )

    cell = to_core_cell(record)

    assert cell.startup_latency_seconds == 19
