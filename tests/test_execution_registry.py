# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Which JAX cluster-detection mode a cell gets, and why it matters.

Eyvan supports two ways for the hosts of one workload to find each other. GCE
metadata detection lets JAX discover the topology itself. Explicit detection has
Eyvan supply the coordinator address.

The choice is not cosmetic: on the explicit path Eyvan reuses the worker's
*management* endpoint as the coordinator address, and for a public-routed cell
that is an external IP -- so the hosts coordinate over the public internet on a
port no firewall rule permits.
"""

from __future__ import annotations

import pytest

from eyvan.capacity import CapacityStatus
from eyvan.capacity.drivers.base import CellObservation
from eyvan.worker.runtime.registry import _uses_gce_tpu_cluster_detection


def _observation(metadata: dict) -> CellObservation:
    return CellObservation(cell_id="cell-a", state=CapacityStatus.HEALTHY, generation=1, metadata=metadata)


def test_a_single_slice_multi_host_tpu_uses_gce_metadata_detection() -> None:
    """The cause of every pods=1 inference failure on 2026-08-02.

    A single ``v5p-16`` is two hosts. Asking "is this multislice?" put it on the
    explicit path, where the coordinator address is the worker's *management*
    endpoint -- the address the control plane reaches the worker on, which for a
    public-routed cell is external. The hosts then tried to coordinate over the
    public internet on port 12345, which no firewall rule permits, and every
    rank died in ``RegisterTask`` with DEADLINE_EXCEEDED having never joined --
    while sitting healthy on one subnet with ``default-allow-internal`` covering
    everything they actually needed.

    Two addresses were conflated: management (control plane to worker, may be
    public) and coordination (host to host, must be internal). GCE metadata
    gives JAX the second directly, which is why the pods=2 job succeeded the
    same day: only multislice reached that branch.
    """

    assert _uses_gce_tpu_cluster_detection(_observation({"tpu_config": {"node_count": 1}})) is True


def test_a_multislice_tpu_still_uses_gce_metadata_detection() -> None:
    """The path that already worked on hardware must keep working."""

    assert _uses_gce_tpu_cluster_detection(_observation({"tpu_config": {"node_count": 2}})) is True


def test_a_non_gce_tpu_cell_still_needs_explicit_addresses() -> None:
    """A manual multi-host backend has no metadata service to discover.

    Explicit detection is not the bug; using it for a Cloud TPU was.
    """

    assert _uses_gce_tpu_cluster_detection(_observation({})) is False


def test_a_malformed_node_count_is_still_refused() -> None:
    """Widening which cells qualify must not weaken validation of the record."""

    with pytest.raises(ValueError, match="node count"):
        _uses_gce_tpu_cluster_detection(_observation({"tpu_config": {"node_count": 0}}))
