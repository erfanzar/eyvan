# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""One host's GPUs split between several cells by a per-worker device list.

A bare-metal eight-GPU host carved into 1/1/2/4 slices runs one native worker
per slice, each with its own port and state directory, and each told which host
GPUs are its own. These tests pin what that list does on both execution paths:
the container is given only those devices, a native unit is denied every other
one, the workload sees the slice as ``0..n-1``, and nothing a submission carries
can name a GPU outside it. None of it has run on a GPU; what is pinned is the
argv and environment Eyvan hands Docker and systemd.
"""

from __future__ import annotations

import csv
import secrets
from pathlib import Path

import pytest
from test_container_executor import FakeDockerSystemd
from test_systemd_transient import FakeSystemd, _environment_from_launch

from eyvan.capacity.drivers.base import MutationContext
from eyvan.records import NativeContainerConfig
from eyvan.worker.distributed_environment import GpuRankLayout, plan_gpu_devices
from eyvan.worker.runtime.container import DockerContainerExecutor
from eyvan.worker.runtime.native_worker import create_native_worker_app, parse_gpu_devices
from eyvan.worker.runtime.systemd_transient import SystemdTransientServiceExecutor, validate_gpu_devices

_IMAGE = "registry.example/easydel:latest"
#: Generated per run, so no credential-looking literal lives in the repository.
WORKER_TOKEN = secrets.token_hex(16)


def _context(key: str = "submit-1") -> MutationContext:
    return MutationContext(idempotency_key=key, generation=1, fencing_token="token-1")


def _spec(
    *,
    submission_id: str = "workload-1",
    environment: dict[str, str] | None = None,
    resources: dict[str, int] | None = None,
) -> dict:
    return {
        "entrypoint": "python train.py",
        "submission_id": submission_id,
        "environment": environment or {"EXPERIMENT": "slice"},
        "gang": {"replicas": 1, "per_replica": resources or {"gpu": 4}, "required_topology": {}},
        "metadata": {"eyvan_workload_id": submission_id},
    }


def _native(tmp_path: Path, systemd: FakeSystemd, devices: tuple[int, ...] | None = (6, 4, 7, 5), **kwargs):
    return SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        use_exit_type_cgroup=True,
        workload_user="eyvan-workload",
        workload_group="eyvan-workload",
        workload_directory="/var/lib/eyvan/workload",
        gpu_devices=devices,
        **kwargs,
    )


def _fake_dev(tmp_path: Path, names: tuple[str, ...] = ("nvidiactl", "nvidia-uvm")) -> Path:
    root = tmp_path / "dev"
    root.mkdir(parents=True, exist_ok=True)
    for name in names:
        (root / name).touch()
    return root


def _container(
    tmp_path: Path,
    daemon: FakeDockerSystemd,
    devices: tuple[int, ...] | None = (4, 5, 6, 7),
    *,
    device_root: Path | None = None,
    privileged: bool = False,
):
    return DockerContainerExecutor(
        tmp_path,
        container=NativeContainerConfig(image=_IMAGE, privileged=privileged),
        command_runner=daemon,
        boot_id_reader=lambda: "boot-a",
        use_exit_type_cgroup=True,
        gpu_devices=devices,
        device_root=device_root if device_root is not None else _fake_dev(tmp_path),
    )


def _docker_run(daemon: FakeDockerSystemd) -> tuple[str, ...]:
    launch = next(command for command in daemon.commands if command[0] == "systemd-run")
    return launch[launch.index("docker") :]


def _container_environment(docker: tuple[str, ...]) -> dict[str, str]:
    env_path = Path(docker[docker.index("--env-file") + 1])
    return dict(line.split("=", 1) for line in env_path.read_text(encoding="utf-8").splitlines() if line)


# --- the list itself ---------------------------------------------------------


def test_device_lists_are_sorted_and_refused_when_malformed() -> None:
    assert validate_gpu_devices([7, 4, 6, 5]) == (4, 5, 6, 7)
    assert parse_gpu_devices("4,5,6,7") == (4, 5, 6, 7)
    assert parse_gpu_devices("0") == (0,)
    for bad in ([], [-1], [1, 1], [True], ["4"], [1.0]):
        with pytest.raises(ValueError):
            validate_gpu_devices(bad)
    for bad in ("", "4,", ",4", "4, 5", "4;5", "-1", "all", "4,4", "GPU-0f2c", "٤"):
        with pytest.raises(ValueError):
            parse_gpu_devices(bad)


# --- container path ----------------------------------------------------------


def test_container_refuses_privileged_beside_a_gpu_slice(tmp_path: Path) -> None:
    """--privileged exposes every GPU, so the slice's cell-relative indices would pick others."""

    with pytest.raises(ValueError, match="privileged cannot be combined with a GPU device slice"):
        _container(tmp_path, FakeDockerSystemd(images={_IMAGE}), privileged=True)


def test_container_privileged_without_a_slice_is_still_accepted(tmp_path: Path) -> None:
    _container(tmp_path, FakeDockerSystemd(images={_IMAGE}), devices=None, privileged=True)


def test_container_names_only_control_nodes_that_exist(tmp_path: Path) -> None:
    """Before nvidia_uvm loads there is no /dev/nvidia-uvm, and naming it would fail docker create."""

    daemon = FakeDockerSystemd(images={_IMAGE})
    executor = _container(tmp_path, daemon, device_root=_fake_dev(tmp_path, ("nvidiactl",)))

    executor.submit(_spec(), _context())

    docker = _docker_run(daemon)
    devices = [docker[i + 1] for i, part in enumerate(docker) if part == "--device"]
    assert devices == ["/dev/nvidia4", "/dev/nvidia5", "/dev/nvidia6", "/dev/nvidia7", "/dev/nvidiactl"]


def test_container_requests_only_the_cells_devices_as_one_csv_quoted_argument(tmp_path: Path) -> None:
    """Docker reads ``--gpus`` as CSV, so the comma-bearing value is quoted.

    The assertion parses the argv element the way Docker's CLI does (one CSV
    record) rather than comparing a string, because what matters is that
    Docker sees one ``device=`` field naming all four devices.
    """

    daemon = FakeDockerSystemd(images={_IMAGE})
    executor = _container(tmp_path, daemon)

    executor.submit(_spec(), _context())

    docker = _docker_run(daemon)
    value = docker[docker.index("--gpus") + 1]
    assert value == '"device=4,5,6,7"'
    assert next(csv.reader([value])) == ["device=4,5,6,7"]
    assert docker.count("--gpus") == 1
    # The slice's device nodes are also named one by one, so a host `systemctl daemon-reload`
    # cannot take them away from the running container (see gpu_device_arguments).
    devices = [docker[i + 1] for i, part in enumerate(docker) if part == "--device"]
    assert devices == [
        "/dev/nvidia4",
        "/dev/nvidia5",
        "/dev/nvidia6",
        "/dev/nvidia7",
        "/dev/nvidiactl",
        "/dev/nvidia-uvm",
    ]
    environment = _container_environment(docker)
    # Inside the container the four devices are the only ones, numbered 0..3.
    assert environment["CUDA_VISIBLE_DEVICES"] == "0,1,2,3"
    assert environment["CUDA_DEVICE_ORDER"] == "PCI_BUS_ID"
    assert "NVIDIA_VISIBLE_DEVICES" not in environment


def test_container_single_device_slice_and_unsliced_cell(tmp_path: Path) -> None:
    single = FakeDockerSystemd(images={_IMAGE})
    _container(tmp_path / "single", single, devices=(3,)).submit(_spec(resources={"gpu": 1}), _context())
    docker = _docker_run(single)
    assert docker[docker.index("--gpus") + 1] == '"device=3"'
    assert next(csv.reader([docker[docker.index("--gpus") + 1]])) == ["device=3"]

    unsliced = FakeDockerSystemd(images={_IMAGE})
    _container(tmp_path / "unsliced", unsliced, devices=None).submit(_spec(), _context())
    docker = _docker_run(unsliced)
    assert docker[docker.index("--gpus") + 1] == "all"
    assert "--device" not in docker
    assert "CUDA_VISIBLE_DEVICES" not in _container_environment(docker)


def test_container_drops_a_gang_plans_nvidia_visibility_but_keeps_its_cuda_narrowing(tmp_path: Path) -> None:
    """A coordinated rank's NVIDIA_VISIBLE_DEVICES would read as host indices to the hook."""

    daemon = FakeDockerSystemd(images={_IMAGE})
    executor = _container(tmp_path, daemon)

    executor.submit(
        _spec(environment={"CUDA_VISIBLE_DEVICES": "2,3", "NVIDIA_VISIBLE_DEVICES": "2,3"}),
        _context(),
    )

    environment = _container_environment(_docker_run(daemon))
    assert environment["CUDA_VISIBLE_DEVICES"] == "2,3"
    assert "NVIDIA_VISIBLE_DEVICES" not in environment


@pytest.mark.parametrize(
    "environment",
    [
        {"CUDA_VISIBLE_DEVICES": "4,5,6,7"},
        {"CUDA_VISIBLE_DEVICES": "0,1,2,3,4"},
        {"NVIDIA_VISIBLE_DEVICES": "all"},
        {"CUDA_VISIBLE_DEVICES": "GPU-8d5e"},
        {"CUDA_VISIBLE_DEVICES": "0,0"},
        {"CUDA_VISIBLE_DEVICES": "-1"},
    ],
)
def test_container_refuses_visibility_outside_the_cell_before_any_durable_record(
    tmp_path: Path, environment: dict[str, str]
) -> None:
    daemon = FakeDockerSystemd(images={_IMAGE})
    executor = _container(tmp_path, daemon)

    with pytest.raises(ValueError, match="names a device outside this cell: the cell owns host GPUs 4,5,6,7"):
        executor.submit(_spec(environment=environment), _context())

    assert not any(command[0] == "systemd-run" for command in daemon.commands)
    assert list(executor.jobs_dir.iterdir()) == []


# --- native path -------------------------------------------------------------


def test_native_unit_is_confined_to_the_cells_device_nodes(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = _native(tmp_path, systemd)

    executor.submit(_spec(), _context())

    launch = systemd.commands[0]
    assert "--property=DevicePolicy=closed" in launch
    allowed = [option.removeprefix("--property=DeviceAllow=") for option in launch if "DeviceAllow=" in option]
    assert allowed == [
        "/dev/nvidiactl rw",
        "/dev/nvidia-modeset rw",
        "char-nvidia-uvm rw",
        "/dev/nvidia4 rw",
        "/dev/nvidia5 rw",
        "/dev/nvidia6 rw",
        "/dev/nvidia7 rw",
    ]
    # The confinement precedes the command; systemd-run reads properties only
    # before the first positional argument.
    assert launch.index("--property=DevicePolicy=closed") < launch.index("/bin/sh")
    environment = _environment_from_launch(launch)
    assert environment["CUDA_VISIBLE_DEVICES"] == "0,1,2,3"
    assert environment["CUDA_DEVICE_ORDER"] == "PCI_BUS_ID"
    assert environment["EXPERIMENT"] == "slice"


def test_native_unsliced_unit_keeps_its_device_access(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = _native(tmp_path, systemd, devices=None)

    executor.submit(_spec(environment={"CUDA_VISIBLE_DEVICES": "7"}), _context())

    launch = systemd.commands[0]
    assert not any("DevicePolicy" in option or "DeviceAllow" in option for option in launch)
    assert _environment_from_launch(launch)["CUDA_VISIBLE_DEVICES"] == "7"


def test_native_job_may_narrow_but_not_escape_its_slice(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = _native(tmp_path, systemd, devices=(4, 5))

    executor.submit(_spec(submission_id="narrow", environment={"CUDA_VISIBLE_DEVICES": "1"}), _context("a"))
    assert _environment_from_launch(systemd.commands[0])["CUDA_VISIBLE_DEVICES"] == "1"

    with pytest.raises(ValueError, match=r"numbers them 0\.\.1"):
        executor.submit(_spec(submission_id="escape", environment={"CUDA_VISIBLE_DEVICES": "4"}), _context("b"))
    assert not executor._record_path("escape").exists()


def test_runtime_environment_visibility_is_refused_beside_a_device_list(tmp_path: Path) -> None:
    """The pre-slicing workaround named host indices; beside a list they are wrong."""

    with pytest.raises(ValueError, match="runtime environment sets CUDA_VISIBLE_DEVICES"):
        _native(tmp_path, FakeSystemd(), base_environment={"CUDA_VISIBLE_DEVICES": "4,5,6,7"})


# --- GPU-coordinated gangs ---------------------------------------------------


def test_a_gang_plan_that_counts_the_cells_gpus_splits_the_slice(tmp_path: Path) -> None:
    """``plan_gpu_devices`` numbers a cell's GPUs 0..n-1, which is the slice's numbering.

    Two rank-process ranks on a four-GPU slice get ``0,1`` and ``2,3`` -- host
    GPUs 4,5 and 6,7 -- with no translation, because the confinement already
    renumbered the slice from zero. A plan that counted the host's eight GPUs
    instead names devices the slice does not have and is refused.
    """

    plan = plan_gpu_devices([4, 4], rank_hosts=["node", "node"])
    systemd = FakeSystemd()
    executor = _native(tmp_path, systemd, devices=(4, 5, 6, 7))
    for rank in range(plan.rank_count):
        rank_environment = plan.rank_environment(rank, rendezvous_host="node", layout=GpuRankLayout.RANK_PROCESS)
        executor.submit(
            _spec(submission_id=f"rank-{rank}", environment=rank_environment),
            _context(f"rank-{rank}"),
        )
    assert [_environment_from_launch(launch)["CUDA_VISIBLE_DEVICES"] for launch in systemd.commands] == [
        "0,1",
        "2,3",
    ]

    host_plan = plan_gpu_devices([8, 8], rank_hosts=["node", "node"])
    second = host_plan.rank_environment(1, rendezvous_host="node", layout=GpuRankLayout.RANK_PROCESS)
    assert second["CUDA_VISIBLE_DEVICES"] == "4,5,6,7"
    with pytest.raises(ValueError, match="names a device outside this cell"):
        executor.submit(_spec(submission_id="host-plan", environment=second), _context("host-plan"))


# --- worker configuration ----------------------------------------------------


def test_worker_cli_reads_the_flag_or_its_environment_default(monkeypatch, tmp_path: Path) -> None:
    from eyvan.worker.runtime import native_worker

    captured: list[dict] = []
    monkeypatch.setattr("uvicorn.run", lambda *args, **kwargs: None)
    monkeypatch.setattr(native_worker, "create_native_worker_app", lambda **kwargs: captured.append(kwargs))
    base = ["--secret", WORKER_TOKEN, "--state-dir", str(tmp_path), "--systemd-transient"]

    assert native_worker.main([*base, "--gpu-devices", "7,6"]) == 0
    assert captured[-1]["gpu_devices"] == (6, 7)

    monkeypatch.setenv("EYVAN_NATIVE_WORKER_GPU_DEVICES", "2,3")
    assert native_worker.main(base) == 0
    assert captured[-1]["gpu_devices"] == (2, 3)

    monkeypatch.setenv("EYVAN_NATIVE_WORKER_GPU_DEVICES", "")
    assert native_worker.main(base) == 0
    assert "gpu_devices" not in captured[-1]


@pytest.mark.parametrize(
    ("arguments", "runtime_env", "message"),
    [
        (["--gpu-devices", "4,4"], None, "must not name a device twice"),
        (["--gpu-devices", "4 5"], None, "comma-separated GPU indices"),
        (["--gpu-devices", "4"], {"CUDA_VISIBLE_DEVICES": "4"}, "which --gpu-devices owns"),
    ],
)
def test_worker_cli_names_a_bad_device_list(monkeypatch, tmp_path: Path, capsys, arguments, runtime_env, message):
    import json

    from eyvan.worker.runtime import native_worker

    monkeypatch.setattr("uvicorn.run", lambda *args, **kwargs: None)
    extra: list[str] = []
    if runtime_env is not None:
        env_file = tmp_path / "runtime-env.json"
        env_file.write_text(json.dumps(runtime_env), encoding="utf-8")
        extra = ["--runtime-env-file", str(env_file)]
    with pytest.raises(SystemExit) as error:
        native_worker.main(
            ["--secret", WORKER_TOKEN, "--state-dir", str(tmp_path), "--systemd-transient", *extra, *arguments]
        )
    assert error.value.code == 2
    assert message in capsys.readouterr().err


def test_worker_cli_refuses_a_device_list_without_the_systemd_executor(monkeypatch, tmp_path: Path, capsys) -> None:
    from eyvan.worker.runtime import native_worker

    monkeypatch.setattr("uvicorn.run", lambda *args, **kwargs: None)
    with pytest.raises(SystemExit):
        native_worker.main(["--secret", WORKER_TOKEN, "--state-dir", str(tmp_path), "--gpu-devices", "0"])
    assert "--gpu-devices requires --systemd-transient" in capsys.readouterr().err
    with pytest.raises(ValueError, match="GPU devices require systemd_transient"):
        create_native_worker_app(state_dir=tmp_path, secret=WORKER_TOKEN, gpu_devices=(0,))


def test_exec_beside_a_sliced_workload_is_confined_like_the_workload(tmp_path: Path) -> None:
    """The diagnostic inherits ``CUDA_VISIBLE_DEVICES=0..n-1``; unconfined, that is another cell's GPUs."""

    systemd = FakeSystemd()
    executor = _native(tmp_path, systemd, devices=(2,))
    remote_id = executor.submit(_spec(resources={"gpu": 1}), _context())

    executor.exec_in_workload(remote_id, ["nvidia-smi"], timeout_s=10)

    invocation = next(command for command in systemd.commands if "--pipe" in command)
    assert "--property=DevicePolicy=closed" in invocation
    assert "--property=DeviceAllow=/dev/nvidia2 rw" in invocation
    assert invocation.index("--property=DevicePolicy=closed") < invocation.index("--")
