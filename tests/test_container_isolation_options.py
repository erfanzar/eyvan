# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""A container off the host network, publishing chosen ports, run as a chosen user, locked down.

What these protect: a service a workload starts (a debugger, a dashboard, a
cluster runtime's own ports) listens only inside the container unless the
operator published it; a published port lands on loopback unless an address
is written down; and a container block that states none of this is launched
and serialized exactly as before.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from eyvan.records import NativeContainerConfig, NativeContainerMount, NativeContainerPort, NativeContainerTmpfs
from tests.test_container_executor import FakeDockerSystemd, _context, _docker_run, _executor, _option_value, _spec

_IMAGE = "registry.example/server:1"


def _options(docker: tuple[str, ...], option: str) -> list[str]:
    return [docker[index + 1] for index, part in enumerate(docker) if part == option]


def test_an_unstated_block_keeps_the_host_network_and_the_images_user(tmp_path: Path) -> None:
    daemon = FakeDockerSystemd(images={_IMAGE})
    _executor(tmp_path, daemon, runtime=NativeContainerConfig(image=_IMAGE)).submit(_spec(), _context())

    docker = _docker_run(daemon)
    assert _option_value(docker, "--network") == "host"
    for option in ("--publish", "--user", "--read-only", "--tmpfs", "--cap-drop", "--security-opt", "--init"):
        assert option not in docker
    # Serialized exactly as before the options existed, so an older worker still accepts the block.
    assert NativeContainerConfig(image=_IMAGE).model_dump() == {
        "image": _IMAGE,
        "pull_policy": "if-not-present",
        "working_dir": None,
        "privileged": False,
        "mounts": (),
    }


def test_a_bridge_container_publishes_only_its_ports_on_loopback_and_runs_locked_down(tmp_path: Path) -> None:
    runtime = NativeContainerConfig(
        image=_IMAGE,
        network="bridge",
        ports=(
            NativeContainerPort(container_port=8000, host_port=8003),
            NativeContainerPort(container_port=9000, host_port=9003, host_ip="::1", protocol="udp"),
        ),
        user="997:997",
        read_only=True,
        tmpfs=(NativeContainerTmpfs(target="/tmp", size="16g"), NativeContainerTmpfs(target="/run/scratch")),
        drop_capabilities=True,
        init=True,
        shm_size="16g",
        mounts=(NativeContainerMount(source="/data/server", target="/var/lib/server", read_only=False),),
    )
    daemon = FakeDockerSystemd(images={_IMAGE})
    _executor(tmp_path, daemon, runtime=runtime).submit(_spec(resources={"gpu": 1}), _context())

    docker = _docker_run(daemon)
    assert _option_value(docker, "--network") == "bridge"
    assert _options(docker, "--publish") == ["127.0.0.1:8003:8000/tcp", "[::1]:9003:9000/udp"]
    assert _option_value(docker, "--user") == "997:997"
    assert "--read-only" in docker and "--init" in docker
    assert _options(docker, "--tmpfs") == ["/tmp:rw,nosuid,nodev,size=16g", "/run/scratch:rw,nosuid,nodev"]
    assert _option_value(docker, "--cap-drop") == "ALL"
    assert _option_value(docker, "--security-opt") == "no-new-privileges"
    assert _option_value(docker, "--shm-size") == "16g" and "--ipc" not in docker
    assert "type=bind,src=/data/server,dst=/var/lib/server" in _options(docker, "--mount")
    # The stated options round-trip through the worker's runtime file.
    assert NativeContainerConfig.model_validate(runtime.model_dump()) == runtime


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        ({"ports": ({"container_port": 80, "host_port": 8080},)}, "only from network: bridge"),
        ({"network": "host", "ports": ({"container_port": 80, "host_port": 8080},)}, "only from network: bridge"),
        (
            {
                "network": "bridge",
                "ports": ({"container_port": 80, "host_port": 8080}, {"container_port": 81, "host_port": 8080}),
            },
            "same host address and port twice",
        ),
        (
            {"network": "bridge", "ports": ({"container_port": 80, "host_port": 8080, "host_ip": "localhost"},)},
            "IP address",
        ),
        ({"network": "bridge", "ports": ({"container_port": 0, "host_port": 8080},)}, "greater than or equal to 1"),
        ({"user": "server"}, "numeric UID"),
        ({"user": "1000:"}, "numeric UID"),
        ({"tmpfs": ({"target": "tmp"},)}, "absolute non-root path"),
        ({"tmpfs": ({"target": "/tmp", "size": "lots"},)}, "tmpfs size"),
        ({"tmpfs": ({"target": "/tmp"}, {"target": "/tmp"})}, "tmpfs targets must be unique"),
        ({"tmpfs": ({"target": "/data"},), "mounts": ({"source": "/data", "target": "/data"},)}, "tmpfs targets"),
        ({"drop_capabilities": True, "privileged": True}, "cannot be combined with privileged"),
    ],
)
def test_the_block_refuses_what_could_not_mean_what_it_says(fields: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        NativeContainerConfig.model_validate({"image": _IMAGE, **fields})
