# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Tests for Eyvan-managed JAX distributed initialization.

These exercise ``_multihost_site`` -- the module ``native_bootstrap`` actually
stages onto TPU hosts. They used to exercise a thin ``runtime/multihost.py``
facade that nothing in production imported, so the scrub-before-initialize
ordering was behaviourally covered only in the copy nobody ran; the shipped copy
lived in a generated shell heredoc and could be asserted only as a string.
"""

from __future__ import annotations

import os
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from eyvan.worker.runtime import _multihost_site
from eyvan.worker.runtime._multihost_site import initialize_worker_runtime as initialize_from_environment


class _FakeDistributed:
    def __init__(self) -> None:
        self.initialize_calls: list[dict[str, str | int | None]] = []
        #: Topology variables still present at the moment JAX was initialized.
        #: Captured rather than checked afterwards, because *afterwards* both a
        #: correct and a too-late scrub look identical -- and a too-late scrub is
        #: exactly the bug: JAX has already read the override by then.
        self.topology_at_initialize: dict[str, str] = {}

    @staticmethod
    def is_initialized() -> bool:
        return False

    def initialize(
        self,
        *,
        cluster_detection_method: str,
        coordinator_address: str | None = None,
        num_processes: int | None = None,
        process_id: int | None = None,
        initialization_timeout: int | None = None,
        heartbeat_timeout_seconds: int | None = None,
    ) -> None:
        self.initialization_timeout = initialization_timeout
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self.topology_at_initialize = {
            name: value
            for name, value in os.environ.items()
            if name in {"GCE_METADATA_IP", "JAX_LOCAL_DEVICE_IDS", "MEGASCALE_NUM_SLICES", "TPU_SKIP_MDS_QUERY"}
        }
        self.initialize_calls.append(
            {
                "coordinator_address": coordinator_address,
                "num_processes": num_processes,
                "process_id": process_id,
                "cluster_detection_method": cluster_detection_method,
            }
        )


class _FakeJax(ModuleType):
    def __init__(self, distributed: _FakeDistributed) -> None:
        super().__init__("jax")
        self.distributed = distributed


def test_initialize_from_environment_disables_platform_cluster_detection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distributed = _FakeDistributed()
    monkeypatch.setitem(sys.modules, "jax", _FakeJax(distributed))
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("JAX_COORDINATOR_ADDRESS", "10.12.0.10:12345")
    monkeypatch.setenv("JAX_PROCESS_COUNT", "4")
    monkeypatch.setenv("JAX_PROCESS_ID", "3")

    initialize_from_environment()

    assert distributed.initialize_calls == [
        {
            "coordinator_address": "10.12.0.10:12345",
            "num_processes": 4,
            "process_id": 3,
            "cluster_detection_method": "deactivate",
        }
    ]


def test_standalone_multihost_module_runs_without_importing_eyvan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distributed = _FakeDistributed()
    monkeypatch.setitem(sys.modules, "jax", _FakeJax(distributed))
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("JAX_COORDINATOR_ADDRESS", "10.12.0.10:12345")
    monkeypatch.setenv("JAX_PROCESS_COUNT", "4")
    monkeypatch.setenv("JAX_PROCESS_ID", "1")
    module_path = Path(_multihost_site.__file__)
    spec = spec_from_file_location("eyvan_multihost", module_path)
    assert spec is not None and spec.loader is not None
    standalone = module_from_spec(spec)
    spec.loader.exec_module(standalone)

    standalone.initialize_from_environment()

    assert distributed.initialize_calls[0]["cluster_detection_method"] == "deactivate"


def test_initialize_from_gce_tpu_metadata_leaves_topology_arguments_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distributed = _FakeDistributed()
    monkeypatch.setitem(sys.modules, "jax", _FakeJax(distributed))
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("EYVAN_JAX_CLUSTER_DETECTION", "gcetpu")
    monkeypatch.setenv("GCE_METADATA_IP", "attacker.invalid")
    monkeypatch.setenv("JAX_LOCAL_DEVICE_IDS", "0")
    monkeypatch.setenv("MEGASCALE_NUM_SLICES", "99")
    monkeypatch.setenv("TPU_SKIP_MDS_QUERY", "1")

    initialize_from_environment()

    assert distributed.initialize_calls == [
        {
            "coordinator_address": None,
            "num_processes": None,
            "process_id": None,
            "cluster_detection_method": "gcetpu",
        }
    ]
    assert "GCE_METADATA_IP" not in os.environ
    assert "JAX_LOCAL_DEVICE_IDS" not in os.environ
    assert "MEGASCALE_NUM_SLICES" not in os.environ
    assert "TPU_SKIP_MDS_QUERY" not in os.environ


def test_initialize_from_gce_tpu_metadata_removes_explicit_process_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    distributed = _FakeDistributed()
    monkeypatch.setitem(sys.modules, "jax", _FakeJax(distributed))
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("EYVAN_JAX_CLUSTER_DETECTION", "gcetpu")
    monkeypatch.setenv("JAX_PROCESS_ID", "0")

    initialize_from_environment()

    assert "JAX_PROCESS_ID" not in os.environ
    assert distributed.initialize_calls[0]["cluster_detection_method"] == "gcetpu"


def test_gce_topology_overrides_are_scrubbed_before_jax_reads_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Order, not outcome: after the call both orderings look the same.

    Under ``gcetpu`` detection JAX reads topology from GCE metadata during
    ``initialize``. An inherited override removed *after* that has already been
    read, so asserting only the final environment cannot tell a correct scrub
    from a useless one.
    """

    distributed = _FakeDistributed()
    monkeypatch.setitem(sys.modules, "jax", _FakeJax(distributed))
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("EYVAN_JAX_CLUSTER_DETECTION", "gcetpu")
    monkeypatch.setenv("GCE_METADATA_IP", "attacker.invalid")
    monkeypatch.setenv("MEGASCALE_NUM_SLICES", "99")

    _multihost_site.initialize_worker_runtime()

    assert distributed.initialize_calls, "JAX must actually have been initialized"
    assert distributed.topology_at_initialize == {}, "the scrub ran too late to matter"


def test_the_heartbeat_deadline_survives_a_model_load(monkeypatch) -> None:
    """JAX's 100-second default kills a host that is busy, not hung.

    This module runs from ``sitecustomize``, so JAX is initialized at interpreter
    startup and the workload's own work happens afterwards -- weight download,
    conversion, first compile -- none of which heartbeats.

    On 2026-08-01 an eSurge job on a two-host v5p-16 died with
    "DEADLINE_EXCEEDED: the JAX distributed service detected fatal errors" while
    one host was still loading a 0.6B model. Both ranks were killed and the
    surviving message named neither the cause nor the host.
    """

    distributed = _FakeDistributed()
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("EYVAN_JAX_CLUSTER_DETECTION", "gcetpu")
    monkeypatch.delenv("EYVAN_JAX_HEARTBEAT_TIMEOUT_S", raising=False)
    monkeypatch.setitem(sys.modules, "jax", SimpleNamespace(distributed=distributed))

    initialize_from_environment()

    assert distributed.heartbeat_timeout_seconds == _multihost_site.DEFAULT_HEARTBEAT_TIMEOUT_S
    assert distributed.heartbeat_timeout_seconds >= 600, "a cold model pull must not read as a dead host"


def test_an_operator_can_lengthen_the_deadlines(monkeypatch) -> None:
    """A larger model or a slower link is a setting, not a rebuild."""

    distributed = _FakeDistributed()
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("EYVAN_JAX_CLUSTER_DETECTION", "gcetpu")
    monkeypatch.setenv("EYVAN_JAX_HEARTBEAT_TIMEOUT_S", "1800")
    monkeypatch.setenv("EYVAN_JAX_INIT_TIMEOUT_S", "1200")
    monkeypatch.setitem(sys.modules, "jax", SimpleNamespace(distributed=distributed))

    initialize_from_environment()

    assert distributed.heartbeat_timeout_seconds == 1800
    assert distributed.initialization_timeout == 1200


def test_a_nonsense_deadline_is_refused_rather_than_silently_defaulted(monkeypatch) -> None:
    """Ignoring it would hand back a default the operator thought they replaced."""

    distributed = _FakeDistributed()
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("EYVAN_JAX_CLUSTER_DETECTION", "gcetpu")
    monkeypatch.setenv("EYVAN_JAX_HEARTBEAT_TIMEOUT_S", "not-a-number")
    monkeypatch.setitem(sys.modules, "jax", SimpleNamespace(distributed=distributed))

    with pytest.raises(RuntimeError, match="integer"):
        initialize_from_environment()


def test_a_zero_deadline_is_refused(monkeypatch) -> None:
    """Zero would mean "declare every host dead immediately"."""

    distributed = _FakeDistributed()
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("EYVAN_JAX_CLUSTER_DETECTION", "gcetpu")
    monkeypatch.setenv("EYVAN_JAX_HEARTBEAT_TIMEOUT_S", "0")
    monkeypatch.setitem(sys.modules, "jax", SimpleNamespace(distributed=distributed))

    with pytest.raises(RuntimeError, match="positive"):
        initialize_from_environment()


def test_a_subprocess_does_not_rejoin_and_kill_the_cluster(monkeypatch) -> None:
    """The cause of a v5p inference job dying with no host having died.

    ``sitecustomize`` runs in every interpreter that inherits ``PYTHONPATH``, and
    the workload environment carries both that path and ``EYVAN_JAX_MULTIHOST=1``.
    Any Python subprocess -- ``gcloud`` is one -- therefore joined the
    coordination service as a duplicate task carrying its parent's
    ``JAX_PROCESS_ID``, then exited when its work was done. The service saw a
    task die and tore down every rank: "another task died", no host dead.

    Ray never hits this: it injects no sitecustomize, so user code initializes
    once, itself.
    """

    distributed = _FakeDistributed()
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("EYVAN_JAX_CLUSTER_DETECTION", "gcetpu")
    monkeypatch.setitem(sys.modules, "jax", SimpleNamespace(distributed=distributed))

    initialize_from_environment()

    assert len(distributed.initialize_calls) == 1
    # A child inherits a copy of this environment; the marker must already be
    # gone from it, or that child joins the cluster and its exit kills the job.
    assert "EYVAN_JAX_MULTIHOST" not in os.environ


def test_the_marker_is_consumed_even_for_explicit_detection(monkeypatch) -> None:
    """Both detection paths spawn subprocesses; neither may leak the marker."""

    distributed = _FakeDistributed()
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("EYVAN_JAX_CLUSTER_DETECTION", "explicit")
    monkeypatch.setenv("JAX_COORDINATOR_ADDRESS", "10.0.0.1:8476")
    monkeypatch.setenv("JAX_PROCESS_COUNT", "2")
    monkeypatch.setenv("JAX_PROCESS_ID", "0")
    monkeypatch.setitem(sys.modules, "jax", SimpleNamespace(distributed=distributed))

    initialize_from_environment()

    assert "EYVAN_JAX_MULTIHOST" not in os.environ


def test_a_second_call_in_the_same_process_is_a_no_op(monkeypatch) -> None:
    """Consuming the marker must not make a re-entrant call misbehave."""

    distributed = _FakeDistributed()
    monkeypatch.setenv("EYVAN_JAX_MULTIHOST", "1")
    monkeypatch.setenv("EYVAN_JAX_CLUSTER_DETECTION", "gcetpu")
    monkeypatch.setitem(sys.modules, "jax", SimpleNamespace(distributed=distributed))

    initialize_from_environment()
    initialize_from_environment()

    assert len(distributed.initialize_calls) == 1
