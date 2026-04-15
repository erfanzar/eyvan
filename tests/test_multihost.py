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
from types import ModuleType

import pytest

from eyvan.runtime import _multihost_site
from eyvan.runtime._multihost_site import initialize_worker_runtime as initialize_from_environment


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
    ) -> None:
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
