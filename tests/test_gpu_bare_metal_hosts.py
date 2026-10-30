# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""An operator-owned 8x H200 node is a shape the catalog knows, and both drivers say so.

Before this, ``describe_cell_accelerator`` returned ``None`` for every H200 name
and ``onprem-ssh`` never published ``cell_accelerator`` at all, so a bare-metal
GPU node was neither GPU-coordinated in a gang nor something a readiness probe
could hold to a device count. The row added here states only what NVIDIA
publishes -- eight 141 GB H200s on an HGX board -- and leaves the host CPU and
memory as an explicit ``null``, because no vendor publishes them and admission
for non-GCP providers never reads them from the catalog.

Nothing here is hardware-qualified. No Eyvan workload has run on an H200.
"""

from __future__ import annotations

import typing as tp
from types import SimpleNamespace

import pytest
import yaml

from eyvan.capacity.drivers.base import (
    CELL_ACCELERATOR_METADATA_KEY,
    MutationContext,
    describe_cell_accelerator,
    read_cell_accelerator,
)
from eyvan.capacity.drivers.manual import ManualCapacityDriver
from eyvan.capacity.drivers.onprem_ssh import OnPremSshCapacityDriver
from eyvan.capacity.fleet_state import FleetStateStore, LocalBackend
from eyvan.capacity.hardware_catalog import (
    CATALOG_PATH,
    AcceleratorShape,
    HardwareCatalogError,
    hardware_catalog,
    load_hardware_catalog,
    validate_accelerator_write_boundary,
)
from eyvan.worker.runtime.registry import ExecutionDriverResolver, NativeGangCell
from tests.test_provider_abi_onprem import OnPremSshConformanceHarness

H200 = "hgx-h200-8g"


def _context(key: str) -> MutationContext:
    return MutationContext(idempotency_key=key, generation=1, fencing_token="fence-1")


def _bare_metal_entry(**overrides: object) -> dict[str, object]:
    entry: dict[str, object] = {
        "accelerator_type": "hgx-h200-8g",
        "kind": "gpu",
        "generation": "hgx-h200",
        "chips": 8,
        "quota_units": 8,
        "hosts": 1,
        "chips_per_host": 8,
        "host_cpu": None,
        "host_memory_gib": None,
        "capacity_backend": "onprem-ssh",
        "multislice_capable": False,
        "gpu_model": "nvidia-h200-141gb",
        "machine_type": None,
        "source_url": "https://www.nvidia.com/en-us/data-center/h200/",
        "source_version": "Retrieved 2026-10-30",
    }
    entry.update(overrides)
    return entry


# ------------------------------------------------------------------ the catalog row


def test_the_catalog_describes_an_eight_gpu_h200_node() -> None:
    """Literals, from NVIDIA's H200 page: 141 GB per GPU, HGX boards of eight."""

    shape = hardware_catalog().require(H200)

    assert shape.kind == "gpu"
    assert shape.devices_per_host == 8
    assert shape.schedulable_gpu == 8
    assert shape.schedulable_tpu == 0
    assert (shape.hosts, shape.chips_per_host) == (1, 8)
    assert shape.gpu_model == "nvidia-h200-141gb"
    assert shape.capacity_backend == "onprem-ssh"
    assert shape.source_url == "https://www.nvidia.com/en-us/data-center/h200/"


def test_the_h200_row_leaves_the_host_it_cannot_ground_unstated() -> None:
    """No vendor publishes an HGX server's CPU or memory; the row must not invent one."""

    shape = hardware_catalog().require(H200)

    assert shape.host_cpu is None
    assert shape.host_memory_gib is None
    assert shape.machine_type is None


def test_the_h200_row_is_not_marked_qualified() -> None:
    """The catalog's own warning must still cover the new row, not be edited around it."""

    text = CATALOG_PATH.read_text(encoding="utf-8")

    assert "NOTHING BELOW IS HARDWARE-QUALIFIED FOR GPU" in text
    assert "NOT QUALIFIED. No Eyvan workload has run on an H200." in text


def test_a_cell_declaring_the_h200_type_is_described_with_its_device_count() -> None:
    """The thing every H200 name returned ``None`` for before this row existed."""

    described = describe_cell_accelerator(accelerator_type=H200)

    assert described is not None
    assert described.gpu_devices_per_host == 8
    assert described.gpu_model == "nvidia-h200-141gb"
    assert read_cell_accelerator({CELL_ACCELERATOR_METADATA_KEY: described.as_metadata()}) == described


@pytest.mark.parametrize("name", ["h200", "nvidia-h200-141gb", "a3-ultragpu-8g"])
def test_other_h200_spellings_stay_unknown(name: str) -> None:
    """Keyed by the platform, not by the model or a GCP machine type nobody transcribed."""

    assert describe_cell_accelerator(accelerator_type=name) is None


# ------------------------------------------------------------------ the schema rule


def test_an_operator_row_may_state_its_host_when_it_can() -> None:
    """Unstated is allowed, not required: a row that can ground its host still says so."""

    shape = AcceleratorShape(**_bare_metal_entry(host_cpu=192, host_memory_gib=2048, machine_type="vendor-box"))

    assert (shape.host_cpu, shape.host_memory_gib, shape.machine_type) == (192, 2048, "vendor-box")


@pytest.mark.parametrize("backend", ["gcp-compute", "compute", "gcp"])
@pytest.mark.parametrize("field_name", ["host_cpu", "host_memory_gib"])
def test_a_provider_gpu_row_still_cannot_leave_its_host_unstated(backend: str, field_name: str) -> None:
    """The exception is for hosts nobody publishes, not for VMs whose table was skipped."""

    entry = _bare_metal_entry(
        capacity_backend=backend,
        machine_type="a3-highgpu-8g",
        host_cpu=208,
        host_memory_gib=1872,
    )
    entry[field_name] = None

    with pytest.raises(HardwareCatalogError, match=field_name):
        AcceleratorShape(**tp.cast(dict[str, tp.Any], entry))


def test_a_provider_gpu_row_still_names_its_machine_type() -> None:
    with pytest.raises(HardwareCatalogError, match="machine_type"):
        AcceleratorShape(**_bare_metal_entry(capacity_backend="gcp-compute", host_cpu=208, host_memory_gib=1872))


def test_a_tpu_row_cannot_borrow_the_operator_exception() -> None:
    with pytest.raises(HardwareCatalogError, match="host_cpu"):
        AcceleratorShape(
            accelerator_type="v5p-16",
            kind="tpu",
            generation="v5p",
            chips=8,
            quota_units=16,
            host_cpu=None,
            host_memory_gib=None,
            capacity_backend="onprem-ssh",
            multislice_capable=True,
            source_url="https://cloud.google.com/tpu/docs/v5p",
            source_version="Last updated 2026-08-04 UTC",
        )


def test_an_omitted_host_key_is_refused_even_on_an_operator_row(tmp_path) -> None:
    """Explicit ``null`` only, so a typo cannot quietly produce an unstated host."""

    document = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    entry = _bare_metal_entry(accelerator_type="hgx-test-8g")
    entry.pop("host_cpu")
    document["shapes"].append(entry)
    path = tmp_path / "catalog.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")

    with pytest.raises(HardwareCatalogError, match="host_cpu"):
        load_hardware_catalog(path)


def test_an_on_prem_h200_pool_still_states_its_own_resource_vector() -> None:
    """Admission for a non-GCP provider never reads the catalog's host numbers."""

    complete = SimpleNamespace(cpu=192.0, memory_gib=1800.0, gpu=8, tpu=0)
    validate_accelerator_write_boundary(
        provider="onprem-ssh",
        capacity_backend="onprem-ssh",
        accelerator_type=H200,
        capacity=complete,
        allocatable=complete,
        boundary="pool",
    )
    memoryless = SimpleNamespace(cpu=192.0, memory_gib=0.0, gpu=8, tpu=0)
    with pytest.raises(ValueError, match="positive cpu and memory_gib"):
        validate_accelerator_write_boundary(
            provider="onprem-ssh",
            capacity_backend="onprem-ssh",
            accelerator_type=H200,
            capacity=memoryless,
            allocatable=memoryless,
            boundary="pool",
        )


# ------------------------------------------------------------------ what drivers publish


def _onprem(tmp_path, accelerator_type: str) -> tuple[OnPremSshCapacityDriver, dict]:
    harness = OnPremSshConformanceHarness(tmp_path)
    driver = tp.cast(OnPremSshCapacityDriver, harness.new_driver())
    desired = harness.desired_capacity("cell-a")
    desired["accelerator_type"] = accelerator_type
    return driver, desired


def test_an_on_prem_h200_cell_publishes_its_accelerator(tmp_path) -> None:
    """``onprem-ssh`` never published this key; an H200 rack was invisible to the GPU path."""

    driver, desired = _onprem(tmp_path, H200)

    applied = driver.apply_capacity("cell-a", desired, _context("apply"))
    observed = driver.observe("cell-a")

    for observation in (applied, observed):
        assert observation.metadata[CELL_ACCELERATOR_METADATA_KEY] == {
            "kind": "gpu",
            "accelerator_type": H200,
            "catalog_revision": hardware_catalog().revision,
            "devices_per_host": 8,
            "gpu_model": "nvidia-h200-141gb",
        }


def test_an_on_prem_cell_of_an_uncatalogued_type_stays_unknown(tmp_path) -> None:
    """``gpu`` names no row, so the cell is exactly as unknown as it was before."""

    driver, desired = _onprem(tmp_path, "gpu")

    driver.apply_capacity("cell-a", desired, _context("apply"))

    assert CELL_ACCELERATOR_METADATA_KEY not in driver.observe("cell-a").metadata


def test_a_manual_h200_cell_publishes_its_accelerator(tmp_path) -> None:
    driver = ManualCapacityDriver(FleetStateStore(LocalBackend(tmp_path / "fleet.json")))
    driver.apply_capacity(
        "cell-a",
        {
            "provider": "manual",
            "capacity_backend": "manual",
            "executor": "eyvan-native",
            "accelerator_type": H200,
            "capacity_class": "on_demand",
            "region": "on-prem",
            "hosts": ["http://10.0.0.20:8100"],
        },
        _context("apply"),
    )

    accelerator = read_cell_accelerator(driver.observe("cell-a").metadata)

    assert accelerator is not None and accelerator.gpu_devices_per_host == 8


def test_two_h200_cells_on_two_nodes_form_a_gpu_coordinated_gang(tmp_path) -> None:
    """The multi-cell coordination the missing row used to switch off."""

    cells = []
    for index, host in enumerate(("10.0.0.20", "10.0.0.21")):
        (tmp_path / f"rack-{index}").mkdir()
        driver, desired = _onprem(tmp_path / f"rack-{index}", H200)
        desired["provider_options"]["hosts"] = [f"eyvan@{host}"]
        cell_id = f"cell-{index}"
        driver.apply_capacity(cell_id, desired, _context(f"apply-{index}"))
        cells.append(NativeGangCell(cell_id=cell_id, observation=driver.observe(cell_id), context=_context("s")))
    endpoints = {cell.cell_id: tuple(cell.observation.metadata["native_worker_endpoints"]) for cell in cells}

    coordinated = ExecutionDriverResolver._gang_gpu_accelerators(cells, endpoints)

    assert {cell_id: accelerator.gpu_devices_per_host for cell_id, accelerator in coordinated.items()} == {
        "cell-0": 8,
        "cell-1": 8,
    }
