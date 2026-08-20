# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import json
import subprocess
from collections.abc import Iterable
from datetime import datetime

import pytest

from eyvan._internal.compat import UTC
from eyvan._internal.json_types import JsonValue
from eyvan.capacity.providers.gcp.accelerator_catalog import AcceleratorCatalogError
from eyvan.capacity.providers.gcp.quota_catalog import QuotaCatalogError
from eyvan.capacity.providers.gcp.quota_discovery import GcloudQuotaError, GcpTpuQuotaDiscovery
from eyvan.capacity.providers.gcp.quota_usage import TpuQuotaUsage


def _result(payload: JsonValue) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(["gcloud"], 0, json.dumps(payload), "")


class FakeUsageDiscovery:
    """Return a complete zero-usage view for deterministic quota discovery tests."""

    locations = (
        "us-central2-b",
        "us-east5-a",
        "us-east5-b",
        "us-west1-a",
    )

    def discover(self, *, project: str, metrics: Iterable[str]) -> tuple[TpuQuotaUsage, ...]:
        assert project == "project-a"
        requested_metrics = tuple(metrics)
        observed_at = datetime(2026, 7, 15, tzinfo=UTC)
        return tuple(
            TpuQuotaUsage(metric=metric, location=location, used_quota_units=0, observed_at=observed_at)
            for metric in requested_metrics
            for location in self.locations
        )


class OfflineQuotaCatalog:
    """A quota reader that always defers to the runner these tests supply.

    Without it every test here mints a real access token and calls Google:
    ``GcpTpuQuotaDiscovery`` builds the API readers by default, which is right
    for a CLI and wrong for a unit test. The suite took sixteen seconds for
    seven cases and its result depended on the machine's gcloud login.
    """

    def limits(self, project: str) -> list[dict[str, object]]:
        raise QuotaCatalogError("offline in tests")


class OfflineAcceleratorCatalog:
    """The same, for the per-zone accelerator listing."""

    def types_for_zones(self, project: str, zones) -> dict[str, tuple[str, ...]]:
        raise AcceleratorCatalogError("offline in tests")


def _offline(**kwargs) -> GcpTpuQuotaDiscovery:
    """Build discovery that reaches no network, whatever else the caller sets."""

    kwargs.setdefault("catalog", OfflineAcceleratorCatalog())
    kwargs.setdefault("quota_catalog", OfflineQuotaCatalog())
    return GcpTpuQuotaDiscovery(**kwargs)


def _discovery(runner) -> GcpTpuQuotaDiscovery:
    """Build discovery with a deterministic read-only Cloud Monitoring source."""

    return _offline(runner=runner, usage_discovery=FakeUsageDiscovery())


def test_quota_discovery_filters_spot_metrics_and_exact_accelerator_types() -> None:
    quotas = [
        {
            "metric": "tpu.googleapis.com/tpu-v5p-preemptible",
            "dimensionsInfos": [
                {
                    "dimensions": {"zone": "us-east5-a"},
                    "details": {"value": "32"},
                }
            ],
        },
        {
            "metric": "tpu.googleapis.com/tpu-v6e-preemptible",
            "dimensions": ["zone"],
            "dimensionsInfos": [
                {
                    "applicableLocations": ["us-east5-b"],
                    "details": {"value": "-1"},
                }
            ],
        },
        {
            "metric": "tpu.googleapis.com/tpu-v5p",
            "dimensionsInfos": [
                {
                    "dimensions": {"zone": "us-east5-a"},
                    "details": {"value": "64"},
                }
            ],
        },
        {
            "metric": "tpu.googleapis.com/tpu-v5s-litepod-serving-preemptible",
            "dimensionsInfos": [
                {
                    "dimensions": {"zone": "us-east5-a"},
                    "details": {"value": "128"},
                }
            ],
        },
    ]
    calls: list[tuple[str, ...]] = []

    def runner(command):
        calls.append(tuple(command))
        if command[:4] == ("beta", "quotas", "info", "list"):
            return _result(quotas)
        zone = next(value.removeprefix("--zone=") for value in command if value.startswith("--zone="))
        values = {
            "us-east5-a": [{"type": "v5p-16"}, {"type": "v5p-32"}, {"type": "v6e-8"}],
            "us-east5-b": [{"type": "v6e-8"}],
        }
        return _result(values[zone])

    records = _discovery(runner).discover(
        project="project-a",
        accelerator_types=("v5p-16", "v6e-8"),
    )

    assert [(item.zone, item.accelerator_type, item.quota_tpus) for item in records] == [
        ("us-east5-a", "v5p-16", 32),
        ("us-east5-b", "v6e-8", None),
    ]
    assert all(item.preemptible for item in records)
    assert len(calls) == 3


def test_quota_discovery_supports_zone_generation_filters_without_querying_other_zones() -> None:
    calls: list[tuple[str, ...]] = []

    def runner(command):
        calls.append(tuple(command))
        if command[:4] == ("beta", "quotas", "info", "list"):
            return _result(
                [
                    {
                        "metric": "tpu.googleapis.com/tpu-v5p-preemptible",
                        "dimensionsInfos": [
                            {"dimensions": {"zone": "us-east5-a"}, "details": {"value": "16"}},
                            {"dimensions": {"zone": "us-east5-b"}, "details": {"value": "16"}},
                        ],
                    }
                ]
            )
        return _result([{"type": "v5p-16"}, {"type": "v6e-8"}])

    records = _discovery(runner).discover(
        project="project-a",
        zones=("us-east5-b",),
        generations=("v5p",),
    )

    assert [(item.zone, item.accelerator_type) for item in records] == [("us-east5-b", "v5p-16")]
    accelerator_calls = [call for call in calls if call[:4] == ("compute", "tpus", "accelerator-types", "list")]
    assert len(accelerator_calls) == 1
    assert "--zone=us-east5-b" in accelerator_calls[0]


def test_quota_discovery_normalizes_v4_pod_spot_quota_to_v4_accelerator_types() -> None:
    def runner(command):
        if command[:4] == ("beta", "quotas", "info", "list"):
            return _result(
                [
                    {
                        "metric": "tpu.googleapis.com/tpu-v4s-pod-preemptible",
                        "dimensionsInfos": [
                            {"dimensions": {"zone": "us-central2-b"}, "details": {"value": "32"}},
                        ],
                    }
                ]
            )
        return _result([{"type": "v4-8"}])

    records = _discovery(runner).discover(
        project="project-a",
        zones=("us-central2-b",),
        generations=("v4",),
    )

    assert [(item.zone, item.accelerator_type, item.generation, item.quota_tpus) for item in records] == [
        ("us-central2-b", "v4-8", "v4", 32),
    ]


def test_quota_discovery_skips_an_inaccessible_zone_when_another_zone_is_available() -> None:
    calls: list[tuple[str, ...]] = []

    def runner(command):
        calls.append(tuple(command))
        if command[:4] == ("beta", "quotas", "info", "list"):
            return _result(
                [
                    {
                        "metric": "tpu.googleapis.com/tpu-v5p-preemptible",
                        "dimensionsInfos": [
                            {"dimensions": {"zone": "us-east5-a"}, "details": {"value": "16"}},
                            {"dimensions": {"zone": "us-west1-a"}, "details": {"value": "16"}},
                        ],
                    }
                ]
            )
        zone = next(value.removeprefix("--zone=") for value in command if value.startswith("--zone="))
        if zone == "us-west1-a":
            return subprocess.CompletedProcess(
                ["gcloud"],
                1,
                "",
                "PERMISSION_DENIED: Location us-west1-a is not found or access is unauthorized.",
            )
        return _result([{"type": "v5p-16"}])

    records = _discovery(runner).discover(project="project-a", generations=("v5p",))

    assert [(item.zone, item.accelerator_type, item.quota_tpus) for item in records] == [
        ("us-east5-a", "v5p-16", 16),
    ]
    accelerator_zones = {
        next(value.removeprefix("--zone=") for value in command if value.startswith("--zone="))
        for command in calls
        if command[:4] == ("compute", "tpus", "accelerator-types", "list")
    }
    assert accelerator_zones == {"us-east5-a", "us-west1-a"}


def test_quota_discovery_aggregates_failures_when_no_quota_matching_zone_is_accessible() -> None:
    def runner(command):
        if command[:4] == ("beta", "quotas", "info", "list"):
            return _result(
                [
                    {
                        "metric": "tpu.googleapis.com/tpu-v5p-preemptible",
                        "dimensionsInfos": [
                            {"dimensions": {"zone": "us-east5-a"}, "details": {"value": "16"}},
                            {"dimensions": {"zone": "us-west1-a"}, "details": {"value": "16"}},
                        ],
                    }
                ]
            )
        zone = next(value.removeprefix("--zone=") for value in command if value.startswith("--zone="))
        return subprocess.CompletedProcess(
            ["gcloud"],
            1,
            "",
            f"PERMISSION_DENIED: Location {zone} is not found or access is unauthorized.",
        )

    with pytest.raises(GcloudQuotaError) as raised:
        _discovery(runner).discover(project="project-a", generations=("v5p",))

    message = str(raised.value)
    assert "any quota-matching zone" in message
    assert "active gcloud account" in message
    assert "us-east5-a" in message
    assert "us-west1-a" in message


def test_quota_discovery_uses_minimum_region_and_zone_headroom_not_the_quota_ceiling() -> None:
    quotas = [
        {
            "metric": "tpu.googleapis.com/tpu-v5p-preemptible",
            "quotaId": "TPUV5PPreemptiblePerProjectPerRegionForTPUAPI",
            "dimensions": ["region"],
            "dimensionsInfos": [
                {"dimensions": {"region": "us-east5"}, "details": {"value": "2048"}},
            ],
        },
        {
            "metric": "tpu.googleapis.com/tpu-v5p-preemptible",
            "quotaId": "TPUV5PPreemptiblePerProjectPerZoneForTPUAPI",
            "dimensions": ["zone"],
            "dimensionsInfos": [
                {"dimensions": {"zone": "us-east5-a"}, "details": {"value": "2048"}},
            ],
        },
    ]

    def runner(command):
        if command[:4] == ("beta", "quotas", "info", "list"):
            return _result(quotas)
        return _result([{"type": "v5p-16"}])

    class UsageDiscovery:
        def discover(self, *, project: str, metrics: Iterable[str]) -> tuple[TpuQuotaUsage, ...]:
            assert project == "project-a"
            assert tuple(metrics) == ("tpu.googleapis.com/tpu-v5p-preemptible",)
            observed_at = datetime(2026, 7, 15, tzinfo=UTC)
            return (
                TpuQuotaUsage(
                    metric="tpu.googleapis.com/tpu-v5p-preemptible",
                    location="us-east5",
                    used_quota_units=2048,
                    observed_at=observed_at,
                ),
                TpuQuotaUsage(
                    metric="tpu.googleapis.com/tpu-v5p-preemptible",
                    location="us-east5-a",
                    used_quota_units=0,
                    observed_at=observed_at,
                ),
            )

    records = _offline(runner=runner, usage_discovery=UsageDiscovery()).discover(
        project="project-a", generations=("v5p",)
    )

    assert len(records) == 1
    assert records[0].quota_tpus == 2048
    assert records[0].available_tpus == 0


def test_quota_discovery_preserves_missing_monitoring_usage_as_unknown() -> None:
    quotas = [
        {
            "metric": "tpu.googleapis.com/tpu-v5p-preemptible",
            "dimensions": ["zone"],
            "dimensionsInfos": [
                {"dimensions": {"zone": "us-east5-a"}, "details": {"value": "2048"}},
            ],
        },
    ]

    def runner(command):
        if command[:4] == ("beta", "quotas", "info", "list"):
            return _result(quotas)
        return _result([{"type": "v5p-16"}])

    class EmptyUsageDiscovery:
        def discover(self, *, project: str, metrics: Iterable[str]) -> tuple[TpuQuotaUsage, ...]:
            assert project == "project-a"
            assert tuple(metrics) == ("tpu.googleapis.com/tpu-v5p-preemptible",)
            return ()

    records = _offline(runner=runner, usage_discovery=EmptyUsageDiscovery()).discover(
        project="project-a", generations=("v5p",)
    )

    assert len(records) == 1
    assert records[0].quota_tpus == 2048
    assert records[0].available_tpus is None
