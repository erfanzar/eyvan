# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Focused tests for the live native v5p-16 acceptance harness helpers."""

from __future__ import annotations

import ast
import importlib.util
import sys
import textwrap
from pathlib import Path
from types import ModuleType

import pytest

from eyvan.drivers.base import CapacityStatus, CellObservation, MutationContext


def _load_live_harness() -> ModuleType:
    """Load the standalone acceptance script without invoking its main entrypoint."""

    script = Path(__file__).parents[3] / "scripts" / "eyvan_native_v5p16_live.py"
    spec = importlib.util.spec_from_file_location("eyvan_native_v5p16_live_test", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


def _context() -> MutationContext:
    return MutationContext(idempotency_key="release-1", generation=2, fencing_token="cell-1-g2")


def test_release_capacity_retries_with_the_same_mutation_context(monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _load_live_harness()
    contexts: list[MutationContext] = []

    class Driver:
        attempts = 0

        def release(self, cell_id: str, context: MutationContext) -> CellObservation:
            assert cell_id == "cell-1"
            self.attempts += 1
            contexts.append(context)
            if self.attempts == 1:
                raise RuntimeError("temporary delete failure")
            return CellObservation(cell_id=cell_id, state=CapacityStatus.RELEASED, generation=context.generation)

    sleeps: list[float] = []
    monkeypatch.setattr(harness.time, "sleep", sleeps.append)
    released = harness._release_capacity_with_retry(Driver(), "cell-1", _context(), timeout_s=30)

    assert released.state is CapacityStatus.RELEASED
    assert len(contexts) == 2
    assert contexts == [_context(), _context()]
    assert sleeps


def test_release_capacity_surfaces_an_exhausted_cleanup_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _load_live_harness()

    class Driver:
        def release(self, cell_id: str, context: MutationContext) -> None:
            assert cell_id == "cell-1"
            assert context == _context()
            raise RuntimeError("persistent delete failure")

    clock = iter((0.0, 0.0))
    monkeypatch.setattr(harness.time, "monotonic", lambda: next(clock))

    with pytest.raises(RuntimeError, match="did not finish within 0s") as exc_info:
        harness._release_capacity_with_retry(Driver(), "cell-1", _context(), timeout_s=0)

    assert isinstance(exc_info.value.__cause__, RuntimeError)


def test_live_harness_loads_native_runtime_from_the_same_fleet_yaml(tmp_path: Path) -> None:
    harness = _load_live_harness()
    fleet_path = tmp_path / "v5p16.yaml"
    fleet_path.write_text(
        textwrap.dedent(
            """\
            version: 1
            name: v5p-live
            provider:
              type: gcp
              project: project-a
              region: us-east5
              zone: us-east5-a
            agent_endpoint: http://agent.internal:8090
            executor: eyvan-native
            capacity_class: spot
            pools:
              v5p-16:
                label: v5p-16
                resources: {CPU: 32, TPU: 8}
                min_workers: 0
                max_workers: 0
                provider_options:
                  service_account: worker@project-a.iam.gserviceaccount.com
                  bootstrap_egress: private-google-access
                runtime:
                  env:
                    HF_HOME: /var/lib/eyvan/workload/hf
                  container:
                    image: registry.example.com/easydel:latest
                    privileged: true
                  artifacts:
                    - source: gs://runtime-artifacts/easydel.whl
                      destination: /opt/eyvan-runtime/easydel.whl
                  init_commands:
                    - /opt/eyvan-native/venv/bin/python -m pip install /opt/eyvan-runtime/easydel.whl
            """
        ),
        encoding="utf-8",
    )

    pool = harness.V5p16LivePool.from_fleet(fleet_path, "v5p-16")
    desired = pool.desired

    assert desired["project"] == "project-a"
    assert "node_count" not in desired["provider_options"]
    assert pool.runtime["env"]["HF_HOME"] == "/var/lib/eyvan/workload/hf"
    assert pool.runtime["artifacts"][0]["destination"] == "/opt/eyvan-runtime/easydel.whl"
    assert pool.runtime["container"]["image"] == "registry.example.com/easydel:latest"
    assert "startup_script" not in desired["provider_options"]
    assert pool.tpu_per_worker == 8


def test_live_harness_reports_one_worker_one_pod_without_slice_language() -> None:
    harness = _load_live_harness()
    result = harness.V5p16LivePool(
        name="v5p-16",
        accelerator_type="v5p-16",
        desired={},
        runtime={},
        tpu_per_worker=8,
    ).success_result()

    assert result == {
        "result": "passed",
        "pods": 1,
        "pool": "v5p-16",
        "type": "v5p-16",
        "workers": 1,
    }
    assert "slices" not in result


def test_oci_smoke_has_no_eyvan_runtime_import() -> None:
    """OCI images only need JAX and EasyDeL; bootstrap supplies multihost setup."""

    script = Path(__file__).parents[3] / "scripts" / "eyvan_easydel_tpu_smoke.py"
    module = ast.parse(script.read_text(encoding="utf-8"))
    imported_modules = {
        alias.name for node in ast.walk(module) if isinstance(node, ast.Import) for alias in node.names
    } | {node.module or "" for node in ast.walk(module) if isinstance(node, ast.ImportFrom)}

    assert not any(name == "eyvan" or name.startswith("eyvan.") for name in imported_modules)
