# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Kueue coexistence: the precondition that makes it safe, made checkable.

Two admission controllers over the same nodes fight twice, and only one of the
two fights is survivable.

**Kueue vetoing a cell Eyvan already leased** is safe but wasteful: Eyvan's
placement is overridden and the cell sits WAITING.

**Kueue *evicting* an admitted Workload** is not. Eviction destroys a pod with no
checkpoint hook, bypassing Eyvan's entire ``PreemptionPlan`` /
``CHECKPOINT_AND_STOP`` / ``grace_period_seconds`` machinery -- a training job
loses hours of progress and nothing in Eyvan's ledger records why.

So the boundary is:

    **Eyvan delegates *capacity* admission to Kueue and retains *workload*
    admission. A ClusterQueue Eyvan uses must be exclusively Eyvan's, with
    preemption disabled.**

The value of this module is that the precondition is *checkable*. An invisible
correctness hazard becomes a refusal at the point of use:
``KubernetesCapacityDriver`` reads the ClusterQueue before creating a Job and
refuses a configuration that could evict Eyvan's work. The driver is where the
check belongs because it runs in-cluster with a ServiceAccount; ``eyvan doctor``
reports the same hazard but can only warn, since it runs on an operator's
machine with no cluster access.
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass

from eyvan._internal.json_types import JsonObject, is_json_object
from eyvan.capacity.providers.kubernetes.workers import CELL_LABEL

#: Preemption policies that leave Eyvan's workloads safe. ``Never`` is the only
#: setting under which Kueue will not destroy a running pod out from under the
#: control plane that thinks it owns it.
SAFE_PREEMPTION = frozenset({"Never", ""})


@dataclass(frozen=True, slots=True)
class KueueFinding:
    """One reason a ClusterQueue is unsafe for Eyvan to use."""

    cluster_queue: str
    problem: str
    remedy: str


def inspect_cluster_queue(payload: JsonObject | None, name: str) -> tuple[KueueFinding, ...]:
    """Report why one ClusterQueue is unsafe, if it is.

    A missing ClusterQueue is a finding, not an empty result: a pool configured
    against a queue that does not exist will suspend every Job forever, and
    reporting "no problems" for that would be actively misleading.
    """

    if payload is None:
        return (
            KueueFinding(
                cluster_queue=name,
                problem="the ClusterQueue does not exist",
                remedy=f"create ClusterQueue {name!r}, or remove queue_name from the pool's provider_options",
            ),
        )
    spec = payload.get("spec") if is_json_object(payload.get("spec")) else {}
    assert isinstance(spec, dict)
    preemption = spec.get("preemption") if is_json_object(spec.get("preemption")) else {}
    assert isinstance(preemption, dict)
    findings: list[KueueFinding] = []
    within = str(preemption.get("withinClusterQueue", ""))
    cohort = str(preemption.get("reclaimWithinCohort", ""))
    if within not in SAFE_PREEMPTION:
        findings.append(
            KueueFinding(
                cluster_queue=name,
                problem=f"preemption.withinClusterQueue is {within!r}",
                remedy="set it to 'Never'; Kueue eviction destroys pods with no checkpoint hook",
            )
        )
    if cohort not in SAFE_PREEMPTION:
        findings.append(
            KueueFinding(
                cluster_queue=name,
                problem=f"preemption.reclaimWithinCohort is {cohort!r}",
                remedy="set it to 'Never'; another cohort's queue must not reclaim Eyvan's capacity",
            )
        )
    return tuple(findings)


def foreign_workloads(workloads: tp.Sequence[JsonObject]) -> tuple[str, ...]:
    """Return the names of Workloads in the queue that are not Eyvan's.

    Identified by the absence of the cell label rather than by a positive marker
    of foreignness: anything Eyvan created carries the label, so absence is the
    only test that cannot be spoofed into looking like Eyvan's own work.
    """

    foreign: list[str] = []
    for workload in workloads:
        if not is_json_object(workload):
            continue
        metadata = workload.get("metadata") if is_json_object(workload.get("metadata")) else {}
        assert isinstance(metadata, dict)
        labels = metadata.get("labels") if is_json_object(metadata.get("labels")) else {}
        assert isinstance(labels, dict)
        if not labels.get(CELL_LABEL):
            foreign.append(str(metadata.get("name", "<unnamed>")))
    return tuple(sorted(foreign))


__all__ = ("SAFE_PREEMPTION", "KueueFinding", "foreign_workloads", "inspect_cluster_queue")
