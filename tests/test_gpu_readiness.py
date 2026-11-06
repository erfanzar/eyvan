# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""A GPU cell is ready only when its host shows the GPUs its type declares.

The case this exists for was observed on 2026-08-13: an ``nvidia-tesla-p100``
cell whose driver could not open its GPU ran an Eyvan callable to completion and
reported success. Its readiness was the worker's ``/healthz``, which says the
HTTP server is up and nothing about the machine. These tests hold the whole
chain -- the probe, the worker route, the agent-side driver and the resolver --
to refusing that host with a reason an operator can act on, while leaving TPU,
CPU and undeclared cells on the readiness they had before.
"""

from __future__ import annotations

import functools
import os
import pwd
import secrets
import struct
import typing as tp
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from eyvan.capacity.drivers.base import MutationContext
from eyvan.capacity.drivers.manual import ManualCapacityDriver
from eyvan.capacity.fleet_state import FleetStateStore, LocalBackend
from eyvan.worker.runtime import native_remote, native_worker
from eyvan.worker.runtime.host_accelerators import (
    _granted_read_write,
    check_gpu_readiness,
    evaluate_gpu_readiness,
    inaccessible_nvidia_devices,
    observe_nvidia_devices,
)
from eyvan.worker.runtime.native_remote import NativeRemoteExecutor
from eyvan.worker.runtime.native_slice import NativeSliceExecutor
from eyvan.worker.runtime.registry import ExecutionDriverResolver

H200 = "hgx-h200-8g"
EIGHT = tuple(f"nvidia{index}" for index in range(8))
#: Generated per run, so no credential-looking literal lives in the repository.
WORKER_TOKEN = secrets.token_hex(16)
ROOT_TOKEN = secrets.token_hex(20)


def _dev(tmp_path: Path, *names: str) -> Path:
    root = tmp_path / "dev"
    root.mkdir(parents=True)
    for name in names:
        (root / name).touch(mode=0o666)
        os.chmod(root / name, 0o666)
    return root


# ------------------------------------------------------------------ the verdict


def test_an_eight_gpu_host_with_eight_nodes_is_ready(tmp_path: Path) -> None:
    verdict = evaluate_gpu_readiness(observe_nvidia_devices(_dev(tmp_path, *EIGHT, "nvidiactl")), expected_devices=8)

    assert verdict.ready
    assert verdict.observed.indices == tuple(range(8))


def test_the_p100_host_is_refused_as_a_driver_that_cannot_drive_its_gpu(tmp_path: Path) -> None:
    """Control node, no numbered node: the signature of the host that reported success."""

    verdict = evaluate_gpu_readiness(observe_nvidia_devices(_dev(tmp_path, "nvidiactl")), expected_devices=1)

    assert not verdict.ready
    assert "driver is loaded" in verdict.detail
    assert "exposes no GPU device node" in verdict.detail


def test_a_host_with_no_driver_at_all_is_refused_as_such(tmp_path: Path) -> None:
    verdict = evaluate_gpu_readiness(observe_nvidia_devices(_dev(tmp_path, "null")), expected_devices=8)

    assert not verdict.ready
    assert "driver is not loaded" in verdict.detail


def test_a_host_missing_one_gpu_is_refused_with_the_count_and_what_is_present(tmp_path: Path) -> None:
    """Seven of eight: a GPU fell off the bus, and the job would otherwise be admitted."""

    verdict = evaluate_gpu_readiness(
        observe_nvidia_devices(_dev(tmp_path, *EIGHT[:3], *EIGHT[4:], "nvidiactl")), expected_devices=8
    )

    assert not verdict.ready
    assert "7 of the 8" in verdict.detail
    assert "/dev/nvidia{0,1,2,4,5,6,7}" in verdict.detail


def test_more_devices_than_declared_is_ready(tmp_path: Path) -> None:
    """A worker owning a slice of a larger host sees every node on it."""

    assert evaluate_gpu_readiness(observe_nvidia_devices(_dev(tmp_path, *EIGHT)), expected_devices=4).ready


def test_a_named_device_with_no_node_is_refused(tmp_path: Path) -> None:
    verdict = evaluate_gpu_readiness(
        observe_nvidia_devices(_dev(tmp_path, *EIGHT[:6], "nvidiactl")),
        expected_devices=None,
        required_indices=(4, 5, 6, 7),
    )

    assert not verdict.ready
    assert "/dev/nvidia6, /dev/nvidia7" in verdict.detail


def test_a_cell_that_declares_no_gpu_is_never_refused_for_having_none(tmp_path: Path) -> None:
    """A CPU host with no NVIDIA nodes is simply a CPU host."""

    for expected in (None, 0):
        assert evaluate_gpu_readiness(
            observe_nvidia_devices(_dev(tmp_path / str(expected), "null")), expected_devices=expected
        ).ready


def test_a_negative_expectation_is_refused() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        evaluate_gpu_readiness(observe_nvidia_devices(Path("/nonexistent")), expected_devices=-1)


# ------------------------------------------------------------------ device access


def _acl(*entries: tuple[int, int, int]) -> bytes:
    return struct.pack("<I", 2) + b"".join(struct.pack("<HHI", *entry) for entry in entries)


USER_OBJ, USER, GROUP_OBJ, GROUP, MASK, OTHER = 0x01, 0x02, 0x04, 0x08, 0x10, 0x20


@pytest.mark.parametrize(
    ("mode", "acl", "expected"),
    [
        (0o666, None, True),  # Ubuntu's default for NVIDIA nodes.
        (0o660, None, False),  # root:root 0660, the case the GCP bootstrap ACLs exist for.
        (0o660, _acl((USER_OBJ, 6, 0), (USER, 6, 1001), (GROUP_OBJ, 6, 0), (MASK, 6, 0), (OTHER, 0, 0)), True),
        # A named entry is still limited by the mask.
        (0o640, _acl((USER_OBJ, 6, 0), (USER, 6, 1001), (GROUP_OBJ, 6, 0), (MASK, 4, 0), (OTHER, 0, 0)), False),
        # Group membership through a named group entry.
        (0o660, _acl((USER_OBJ, 6, 0), (GROUP_OBJ, 0, 0), (GROUP, 6, 44), (MASK, 6, 0), (OTHER, 0, 0)), True),
    ],
)
def test_device_access_follows_mode_bits_and_posix_acls(mode: int, acl: bytes | None, expected: bool) -> None:
    granted = _granted_read_write(mode, 0, 0, uid=1001, gids=frozenset({1001, 44}), acl=acl)

    assert granted is expected


def test_an_owner_without_write_is_refused() -> None:
    assert _granted_read_write(0o466, 1001, 0, uid=1001, gids=frozenset(), acl=None) is False


def test_a_node_the_workload_user_cannot_open_is_named(tmp_path: Path) -> None:
    me = pwd.getpwuid(os.getuid()).pw_name
    root = _dev(tmp_path, "nvidia0", "nvidia1", "nvidiactl", "nvidia-modeset")
    os.chmod(root / "nvidia1", 0o444)

    assert inaccessible_nvidia_devices(me, root, acl_reader=lambda _path: None) == (str(root / "nvidia1"),)


def test_an_unknown_workload_user_cannot_open_anything(tmp_path: Path) -> None:
    root = _dev(tmp_path, "nvidia0", "nvidiactl")

    blocked = inaccessible_nvidia_devices("eyvan-no-such-user-xyz", root, acl_reader=lambda _path: None)

    assert blocked == (str(root / "nvidia0"), str(root / "nvidiactl"))


def test_readiness_names_the_udev_rule_when_access_is_missing(tmp_path: Path) -> None:
    root = _dev(tmp_path, *EIGHT, "nvidiactl")

    verdict = check_gpu_readiness(8, workload_user="eyvan-no-such-user-xyz", dev_root=root)

    assert not verdict.ready
    assert "cannot open" in verdict.detail
    assert "docs/runtimes.md" in verdict.detail


def test_access_is_not_checked_on_the_container_path(tmp_path: Path) -> None:
    """Docker opens the devices as root; there is no unprivileged identity to check."""

    root = _dev(tmp_path, *EIGHT, "nvidiactl")

    assert check_gpu_readiness(8, workload_user=None, dev_root=root).ready


# ------------------------------------------------------------------ the worker route


def _worker(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *nodes: str) -> TestClient:
    root = _dev(tmp_path, *nodes)
    monkeypatch.setattr(
        native_worker,
        "check_gpu_readiness",
        functools.partial(check_gpu_readiness, dev_root=root),
    )
    app = native_worker.create_native_worker_app(state_dir=tmp_path / "state", secret=WORKER_TOKEN)
    return TestClient(app)


def test_the_worker_reports_a_short_host_as_not_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _worker(tmp_path, monkeypatch, "nvidiactl")

    response = client.get(
        "/api/v1/accelerators",
        params={"expected_gpu_devices": 8},
        headers={"Authorization": f"Bearer {WORKER_TOKEN}"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["ready"] is False
    assert body["observed_devices"] == 0
    assert body["control_node_present"] is True
    assert body["expected_devices"] == 8


def test_the_worker_reports_a_full_host_as_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _worker(tmp_path, monkeypatch, *EIGHT, "nvidiactl")

    body = client.get(
        "/api/v1/accelerators",
        params={"expected_gpu_devices": 8},
        headers={"Authorization": f"Bearer {WORKER_TOKEN}"},
    ).json()

    assert body["ready"] is True
    assert body["observed_devices"] == 8


def test_a_sliced_worker_requires_its_own_gpus_even_on_a_full_count_host(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Eight nodes, but not the one this slice owns: the catalog count is met,
    # the slice is not.
    root = _dev(tmp_path, "nvidia0", "nvidia1", "nvidia2", "nvidia3", "nvidia4", "nvidia5", "nvidia6", "nvidia8")
    monkeypatch.setattr(
        native_worker,
        "check_gpu_readiness",
        functools.partial(check_gpu_readiness, dev_root=root),
    )
    token = secrets.token_hex(16)
    app = native_worker.create_native_worker_app(
        state_dir=tmp_path / "state", secret=token, systemd_transient=True, gpu_devices=(6, 7)
    )

    body = (
        TestClient(app)
        .get(
            "/api/v1/accelerators",
            params={"expected_gpu_devices": 8},
            headers={"Authorization": f"Bearer {token}"},
        )
        .json()
    )

    assert body["ready"] is False
    assert "/dev/nvidia7" in body["detail"]


def test_the_accelerator_route_requires_the_worker_credential(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _worker(tmp_path, monkeypatch, *EIGHT)

    assert client.get("/api/v1/accelerators").status_code == 401


# ------------------------------------------------------------------ the agent side


def _remote(handler: tp.Callable[[httpx.Request], httpx.Response]) -> NativeRemoteExecutor:
    client = httpx.Client(base_url="http://10.0.0.20:8100", transport=httpx.MockTransport(handler))
    return NativeRemoteExecutor("http://10.0.0.20:8100", secret="s", client=client)


def _report(ready: bool, detail: str) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "ready": ready,
            "detail": detail,
            "expected_devices": 8,
            "observed_devices": 8 if ready else 7,
            "control_node_present": True,
        },
    )


def test_the_remote_driver_passes_the_count_and_returns_the_reason() -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.params.get("expected_gpu_devices"))
        return _report(False, "host exposes 7 of the 8 GPU device nodes")

    assert _remote(handler).gpu_readiness(8) == (False, "host exposes 7 of the 8 GPU device nodes")
    assert seen == ["8"]


def test_a_worker_that_predates_the_route_keeps_its_old_readiness() -> None:
    """An agent upgraded before its workers must not take the fleet out of service."""

    ready, detail = _remote(lambda _request: httpx.Response(404)).gpu_readiness(8)

    assert ready
    assert "predates /api/v1/accelerators" in detail and "not checked" in detail


def test_a_worker_that_cannot_answer_is_not_read_as_healthy() -> None:
    """Could-not-check must not become checked-and-fine for a cell that declared GPUs."""

    assert not _remote(lambda _request: httpx.Response(503)).gpu_readiness(8)[0]


def test_a_multi_host_cell_names_the_host_that_is_short() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "10.0.0.21":
            return _report(False, "host exposes 7 of the 8 GPU device nodes")
        return _report(True, "8 GPU device nodes present")

    endpoints = ("http://10.0.0.20:8100", "http://10.0.0.21:8100")
    clients = [httpx.Client(base_url=endpoint, transport=httpx.MockTransport(handler)) for endpoint in endpoints]
    executor = NativeSliceExecutor(endpoints, secret="s", clients=clients)

    ready, detail = executor.gpu_readiness(8)

    assert not ready
    assert detail.startswith("http://10.0.0.21:8100: host exposes 7 of the 8")


def _manual_observation(tmp_path: Path, accelerator_type: str):
    driver = ManualCapacityDriver(FleetStateStore(LocalBackend(tmp_path / "fleet.json")))
    driver.apply_capacity(
        "cell-a",
        {
            "provider": "manual",
            "capacity_backend": "manual",
            "executor": "eyvan-native",
            "accelerator_type": accelerator_type,
            "capacity_class": "on_demand",
            "region": "on-prem",
            "hosts": ["http://10.0.0.20:8100"],
        },
        MutationContext(idempotency_key="apply", generation=1, fencing_token="fence-1"),
    )
    return driver.observe("cell-a")


def _resolver_against(
    monkeypatch: pytest.MonkeyPatch, accelerators: tp.Callable[[httpx.Request], httpx.Response]
) -> tuple[ExecutionDriverResolver, list[str]]:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if request.url.path == "/api/v1/health":
            return httpx.Response(200, json={"status": "ok", "runtime": "native-worker"})
        return accelerators(request)

    real_client = httpx.Client

    def client_factory(*_args: object, **kwargs: tp.Any) -> httpx.Client:
        return real_client(base_url=kwargs["base_url"], transport=httpx.MockTransport(handler))

    monkeypatch.setattr(native_remote.httpx, "Client", client_factory)
    return ExecutionDriverResolver(backend="eyvan-native", native_worker_token=ROOT_TOKEN), paths


def test_an_h200_cell_whose_host_is_short_is_not_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End to end on the agent side: the catalog count reaches the worker and the reason comes back."""

    observation = _manual_observation(tmp_path, H200)
    seen: list[str | None] = []

    def accelerators(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.params.get("expected_gpu_devices"))
        return _report(False, "host exposes 7 of the 8 GPU device nodes")

    resolver, _paths = _resolver_against(monkeypatch, accelerators)
    try:
        ready, detail = resolver.readiness("cell-a", observation)
    finally:
        resolver.close()

    assert not ready
    assert f"GPU readiness failed for {H200}" in detail
    assert "7 of the 8" in detail
    assert seen == ["8"]


def test_an_h200_cell_whose_host_is_whole_is_ready(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    observation = _manual_observation(tmp_path, H200)
    resolver, paths = _resolver_against(monkeypatch, lambda _request: _report(True, "8 GPU device nodes present"))
    try:
        assert resolver.readiness("cell-a", observation) == (True, "execution runtime is ready")
    finally:
        resolver.close()

    assert "/api/v1/accelerators" in paths


@pytest.mark.parametrize("accelerator_type", ["v5p-8", "gpu", "cpu"])
def test_cells_without_a_catalog_gpu_shape_never_ask_about_gpus(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, accelerator_type: str
) -> None:
    """The readiness path every TPU cell depends on is unchanged, and unknown stays unknown."""

    observation = _manual_observation(tmp_path, accelerator_type)

    def accelerators(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("a cell with no catalog GPU shape must not be held to a GPU count")

    resolver, paths = _resolver_against(monkeypatch, accelerators)
    try:
        assert resolver.readiness("cell-a", observation) == (True, "execution runtime is ready")
    finally:
        resolver.close()

    assert "/api/v1/accelerators" not in paths


# ------------------------------------------------------------------ GPU slices


def test_a_slice_is_not_failed_by_another_cells_missing_gpu(tmp_path: Path) -> None:
    """One GPU gone from the host must not take every slice on it out of service."""

    root = _dev(tmp_path, "nvidia0", "nvidia1", "nvidia2", "nvidia3", "nvidia5", "nvidia6", "nvidia7", "nvidiactl")

    verdict = check_gpu_readiness(4, required_indices=(0, 1, 2, 3), dev_root=root)

    assert verdict.ready, verdict.detail


def test_a_slice_smaller_than_its_cell_declares_stays_ready_and_names_the_setting(tmp_path: Path) -> None:
    """Existing sliced cells keep working after an upgrade; the detail says what to set."""

    root = _dev(tmp_path, *EIGHT, "nvidiactl")

    verdict = check_gpu_readiness(8, required_indices=(4, 5, 6, 7), dev_root=root)

    assert verdict.ready
    assert "gpu_devices_per_host" in verdict.detail


def test_device_access_is_checked_only_on_the_slices_own_gpus(tmp_path: Path) -> None:
    me = pwd.getpwuid(os.getuid()).pw_name
    root = _dev(tmp_path, *EIGHT, "nvidiactl")
    os.chmod(root / "nvidia7", 0o444)  # another cell's GPU

    assert inaccessible_nvidia_devices(me, root, acl_reader=lambda _path: None, indices={0, 1}) == ()
    assert check_gpu_readiness(
        2, required_indices=(0, 1), workload_user=me, dev_root=root, acl_reader=lambda _path: None
    ).ready


def _proc(tmp_path: Path, minors_by_bus: dict[str, int]) -> Path:
    root = tmp_path / "proc"
    for bus, minor in minors_by_bus.items():
        (root / bus).mkdir(parents=True)
        (root / bus / "information").write_text(f"Model: \t GPU\nDevice Minor: \t {minor}\n")
    return root


def test_a_slice_is_refused_where_nvml_and_dev_numbering_differ(tmp_path: Path) -> None:
    root = _dev(tmp_path, "nvidia0", "nvidia1", "nvidiactl")
    proc = _proc(tmp_path, {"0000:08:00.0": 1, "0000:7e:00.0": 0})

    verdict = check_gpu_readiness(1, required_indices=(0,), dev_root=root, proc_root=proc)

    assert not verdict.ready
    assert "NVML index 0 is /dev/nvidia1" in verdict.detail


def test_a_slice_is_ready_where_the_numbering_agrees_or_cannot_be_read(tmp_path: Path) -> None:
    root = _dev(tmp_path, "nvidia0", "nvidia1", "nvidiactl")
    agreeing = _proc(tmp_path, {"0000:08:00.0": 0, "0000:7e:00.0": 1})

    assert check_gpu_readiness(1, required_indices=(1,), dev_root=root, proc_root=agreeing).ready
    assert check_gpu_readiness(1, required_indices=(1,), dev_root=root, proc_root=tmp_path / "absent").ready
