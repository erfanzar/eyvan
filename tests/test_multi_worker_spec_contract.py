# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The multi-worker contract, enforced where a spec is built.

``WorkloadDefinition`` carried this contract in a ``@model_validator``. At some
point the validator's body was left stranded *after* a ``return`` inside the
``capacity_class_portfolio`` property, so every line of it became unreachable
and nothing enforced any of it.

The multislice ceiling survived elsewhere (``CellRecord``, and the callable
submit path), so the hole was bounded rather than total. But a direct HTTP or
CLI submit with ``worker_count > 1`` and a GPU gang, a non-GCP provider, or more
than one cell group was accepted at the spec boundary and only failed much
later, when a cell was built -- the worst place to learn about it, because by
then capacity has been requested.

Ruff's selected rules (``A,B,E,F,I,NPY,RUF,UP,W``) contain no unreachable-code
check, which is why this survived review and CI.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from eyvan.records import (
    MAX_GCP_TPU_MULTISLICE_WORKERS,
    WORKER_COUNT_LABEL,
    CommandExecution,
    WorkloadDefinition,
)


def _spec(**overrides: object) -> WorkloadDefinition:
    """Build a valid multi-worker TPU spec, with one thing changed."""

    fields: dict[str, object] = {
        "name": "train",
        "execution": CommandExecution(command="python train.py"),
        "gang": {"replicas": 1, "per_replica": {"cpu": 1, "tpu": 8}},
        "worker_count": 2,
        "provider": "gcp",
        "required_executor": "eyvan-native",
    }
    fields.update(overrides)
    return WorkloadDefinition(**fields)  # type: ignore[arg-type]


def test_a_valid_multi_worker_spec_is_accepted() -> None:
    """The contract must not reject the shape it exists to describe."""

    assert _spec().worker_count == 2


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"cell_group_count": 2}, "exactly one physical cell"),
        ({"provider": "aws"}, "requires the GCP provider"),
        ({"required_executor": "ray-jobs"}, "requires the eyvan-native executor"),
        ({"gang": {"replicas": 1, "per_replica": {"cpu": 1, "gpu": 2}}}, "requires a TPU resource request"),
        ({"worker_count": MAX_GCP_TPU_MULTISLICE_WORKERS + 1}, "must not exceed"),
    ],
    ids=["multi-cell-group", "non-gcp-provider", "wrong-executor", "gpu-gang", "over-ceiling"],
)
def test_each_invalid_multi_worker_shape_is_refused(overrides: dict, message: str) -> None:
    """Every clause of the contract, one test case each.

    A multislice is one physical GCP TPU allocation driven by the native
    executor. Each of these shapes describes something that cannot be built, and
    accepting one means discovering it only when capacity is already requested.
    """

    with pytest.raises(ValidationError, match=message):
        _spec(**overrides)


def test_a_declared_worker_count_label_must_agree_with_the_field() -> None:
    """Two sources for one number must not be allowed to disagree.

    ``required_labels`` steers placement while ``worker_count`` sizes the
    allocation; a spec whose label says 4 and whose field says 2 would be
    scheduled onto cells it does not fit.
    """

    with pytest.raises(ValidationError, match=WORKER_COUNT_LABEL):
        _spec(required_labels={WORKER_COUNT_LABEL: "4"})


def test_a_matching_worker_count_label_is_accepted() -> None:
    """Agreement is the normal case and must stay quiet."""

    assert _spec(required_labels={WORKER_COUNT_LABEL: "2"}).worker_count == 2


def test_a_single_worker_spec_is_not_held_to_the_multislice_contract() -> None:
    """Ordinary single-cell workloads must remain unconstrained.

    A GPU job on a non-GCP provider is a perfectly good workload; the contract
    only governs ``worker_count > 1``.
    """

    spec = WorkloadDefinition(
        name="train",
        execution=CommandExecution(command="python train.py"),
        gang={"replicas": 1, "per_replica": {"cpu": 1, "gpu": 2}},
        provider="manual",
        required_executor="ray-jobs",
    )

    assert spec.worker_count == 1


def test_the_capacity_class_portfolio_property_still_works() -> None:
    """The property the dead code was hiding behind must be unaffected."""

    from eyvan.records import CapacityClass

    spec = _spec(capacity_class=CapacityClass.SPOT, additional_capacity_classes=(CapacityClass.ON_DEMAND,))

    assert spec.capacity_class_portfolio == (CapacityClass.SPOT, CapacityClass.ON_DEMAND)
