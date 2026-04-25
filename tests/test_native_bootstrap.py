# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import base64
import json
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from eyvan.drivers.base import CapacityStatus, CellObservation
from eyvan.runtime import ConfiguredExecutionResolver, ExecutionRuntimeRegistry
from eyvan.runtime.native_bootstrap import build_native_worker_startup_script, derive_native_worker_token
from eyvan.runtime.native_slice import JaxClusterDetectionMode, NativeSliceExecutor
from eyvan.service.models import NativeContainerConfig, NativeContainerMount


def test_compute_bootstrap_fetches_a_cell_scoped_token_from_secret_manager_and_runs_fleet_setup() -> None:
    token = derive_native_worker_token("agent-global-secret", "cell-uid")
    secret_resource = "projects/project-a/secrets/eyvan-native-cell-uid/versions/latest"
    script = build_native_worker_startup_script(
        package="gs://artifacts/eyvan/eyvan.whl",
        port=8123,
        state_dir="/var/lib/eyvan/worker",
        worker_secret_resource=secret_resource,
        wheelhouse="gs://artifacts/eyvan/wheelhouse.zip",
        before_start="echo setup-runtime",
    )

    assert token is not None
    assert "agent-global-secret" not in script
    assert token not in script
    assert secret_resource in script
    assert "gcloud secrets versions access" in script
    assert script.index("echo setup-runtime") < script.index("gcloud storage cp")
    assert "gcloud storage cp gs://artifacts/eyvan/eyvan.whl /var/lib/eyvan/bootstrap/eyvan.whl" in script
    assert "/opt/eyvan-native/venv/bin/python -m pip install" in script
    assert "/var/lib/eyvan/bootstrap/eyvan.whl" in script
    assert "eyvan-worker.whl" not in script
    assert "gcloud storage cp gs://artifacts/eyvan/wheelhouse.zip" in script
    assert "--no-index --find-links /var/lib/eyvan/bootstrap/wheels" in script
    assert "eyvan.runtime.native_worker --host 0.0.0.0 --port 8123" in script
    assert "--secret-file /run/eyvan-native/worker-secret" in script
    assert "--systemd-transient" in script
    assert "EYVAN_SYSTEMD_EXIT_TYPE_CGROUP" in script
    assert "EnvironmentFile=/etc/eyvan-native/systemd-runtime.env" in script
    assert "useradd --system --gid eyvan-workload" in script
    assert "Environment=VIRTUAL_ENV=/opt/eyvan-native/venv" in script
    assert "Environment=PATH=/opt/eyvan-native/venv/bin:/snap/bin:" in script
    assert "Environment=EYVAN_NATIVE_WORKLOAD_USER=eyvan-workload" in script
    assert "runuser -u eyvan-workload -- /opt/eyvan-native/venv/bin/python -c 'import sys'" in script
    assert "virtual environment is not executable by the workload user" in script
    assert "runuser -u eyvan-workload -- test -r" in script
    assert "/dev/vfio/devices/*" in script
    assert '[ -c "${device}" ] || continue' in script
    assert "setfacl -m u:eyvan-workload:rw" in script
    assert 'command apt-get -o DPkg::Lock::Timeout=300 "$@"' in script
    assert "eyvan_apt_get install -y --no-install-recommends acl" in script
    assert "python3 -m ensurepip --version" in script
    assert "eyvan_apt_get install -y --no-install-recommends python3-venv" in script
    assert "rm -rf /opt/eyvan-native/venv" in script
    assert "systemctl enable eyvan-native-worker.service" in script
    assert "systemctl restart eyvan-native-worker.service" in script
    assert "/opt/eyvan-native/multihost/sitecustomize.py" in script
    assert "import eyvan.runtime._multihost_site as module" in script
    assert "/opt/eyvan-native/multihost/eyvan_multihost.py" in script
    assert "import eyvan.distributed_environment as module" in script
    assert "/opt/eyvan-native/multihost/eyvan_distributed_environment.py" in script
    # The generated sitecustomize delegates to one staged function rather than
    # re-implementing the scrub-then-initialize ordering inline. Ordering written
    # in a heredoc can only be asserted as a *string*; ordering written in Python
    # is covered behaviourally in ``test_multihost``, which is where it belongs.
    assert "from eyvan_multihost import initialize_worker_runtime" in script
    assert "initialize_worker_runtime()" in script
    assert "remove_gce_tpu_topology_overrides(os.environ)" not in script
    assert "def _initialize_eyvan_multihost" not in script


def test_compute_bootstrap_apt_helper_executes_with_lock_timeout_and_preserves_failures(tmp_path: Path) -> None:
    apt_args = tmp_path / "apt-args"
    fake_apt = tmp_path / "apt-get"
    fake_apt.write_text('#!/bin/sh\nprintf \'%s\\n\' "$@" > "$EYVAN_APT_ARGS"\nexit "${EYVAN_APT_EXIT_CODE:-0}"\n')
    fake_apt.chmod(0o755)
    script = build_native_worker_startup_script(
        package="eyvan==0.1.0",
        port=8123,
        state_dir="/var/lib/eyvan/worker",
        worker_secret_resource="projects/project-a/secrets/eyvan-native-cell/versions/latest",
        before_start="eyvan_apt_get install -y docker.io\nexit 0",
    )
    environment = {
        "EYVAN_APT_ARGS": str(apt_args),
        "EYVAN_APT_EXIT_CODE": "0",
        "PATH": str(tmp_path),
    }
    bash = shutil.which("bash")
    assert bash is not None

    syntax = subprocess.run([bash, "-n"], input=script, text=True, capture_output=True, check=False)
    succeeded = subprocess.run([bash], input=script, text=True, capture_output=True, env=environment, check=False)

    assert syntax.returncode == 0, syntax.stderr
    assert succeeded.returncode == 0, succeeded.stderr
    assert apt_args.read_text().splitlines() == [
        "-o",
        "DPkg::Lock::Timeout=300",
        "install",
        "-y",
        "docker.io",
    ]

    environment["EYVAN_APT_EXIT_CODE"] = "23"
    failed = subprocess.run([bash], input=script, text=True, capture_output=True, env=environment, check=False)
    missing = subprocess.run(
        [bash],
        input=script,
        text=True,
        capture_output=True,
        env={**environment, "PATH": str(tmp_path / "missing")},
        check=False,
    )

    assert failed.returncode == 23
    assert missing.returncode == 127
    assert "bootstrap requires apt-get" in missing.stderr


def test_compute_bootstrap_writes_the_declarative_runtime_environment_for_workloads() -> None:
    script = build_native_worker_startup_script(
        package="gs://artifacts/eyvan/eyvan.whl",
        port=8123,
        state_dir="/var/lib/eyvan/worker",
        worker_secret_resource="projects/project-a/secrets/eyvan-native-cell/versions/latest",
        runtime_env={
            "HF_HOME": "/var/lib/eyvan/workload/hf",
            "JAX_COMPILATION_CACHE_DIR": "/var/lib/eyvan/workload/jax-cache",
        },
    )

    assert "/etc/eyvan-native/workload-runtime.json" in script
    assert "--runtime-env-file /etc/eyvan-native/workload-runtime.json" in script
    assert script.index("workload-runtime.json") < script.index("eyvan.runtime.native_worker --host 0.0.0.0")


def test_compute_bootstrap_writes_a_typed_oci_runtime_for_the_native_worker() -> None:
    container = NativeContainerConfig(
        image="us-docker.pkg.dev/project-a/easydel/runtime@sha256:" + "a" * 64,
        pull_policy="always",
        working_dir="/workspace",
        privileged=True,
        mounts=(NativeContainerMount(source="/mnt/datasets", target="/datasets"),),
    )
    script = build_native_worker_startup_script(
        package="gs://artifacts/eyvan/eyvan.whl",
        port=8123,
        state_dir="/var/lib/eyvan/worker",
        worker_secret_resource="projects/project-a/secrets/eyvan-native-cell/versions/latest",
        runtime_container=container,
        container_artifact_mounts=("/opt/eyvan-runtime/easydel.whl",),
    )

    payload_line = next(line for line in script.splitlines() if "container-runtime.json" in line and "base64" in line)
    encoded_payload = shlex.split(payload_line)[2]
    payload = json.loads(base64.b64decode(encoded_payload))

    assert payload == {
        "artifact_mounts": ["/opt/eyvan-runtime/easydel.whl"],
        "container": container.model_dump(mode="json"),
    }
    assert "--container-runtime-file /etc/eyvan-native/container-runtime.json" in script
    assert "docker version >/dev/null 2>&1" in script
    assert script.index("container-runtime.json") < script.index("eyvan.runtime.native_worker --host 0.0.0.0")


def test_compute_bootstrap_rejects_container_artifacts_without_an_oci_runtime() -> None:
    with pytest.raises(ValueError, match="require a container runtime"):
        build_native_worker_startup_script(
            package="gs://artifacts/eyvan/eyvan.whl",
            port=8123,
            state_dir="/var/lib/eyvan/worker",
            worker_secret_resource="projects/project-a/secrets/eyvan-native-cell/versions/latest",
            container_artifact_mounts=("/opt/eyvan-runtime/easydel.whl",),
        )


def test_compute_bootstrap_rejects_an_environment_that_cannot_be_loaded_by_a_native_worker() -> None:
    with pytest.raises(ValueError, match="environment"):
        build_native_worker_startup_script(
            package="gs://artifacts/eyvan/eyvan.whl",
            port=8123,
            state_dir="/var/lib/eyvan/worker",
            worker_secret_resource="projects/project-a/secrets/eyvan-native-cell/versions/latest",
            runtime_env={"NOT-AN-ENVIRONMENT-NAME": "value"},
        )


def test_compute_bootstrap_rejects_a_gcs_package_without_a_wheel_filename() -> None:
    with pytest.raises(ValueError, match=r"must name a .whl file"):
        build_native_worker_startup_script(
            package="gs://artifacts/eyvan/eyvan",
            port=8123,
            state_dir="/var/lib/eyvan/worker",
            worker_secret_resource="projects/project-a/secrets/eyvan-native-cell/versions/latest",
        )


def test_native_resolver_derives_vm_endpoint_and_cell_token_without_importing_ray(tmp_path: Path, monkeypatch) -> None:
    captured: list[dict] = []
    registry = ExecutionRuntimeRegistry()

    class FakeNativeDriver:
        def health(self) -> bool:
            return True

    def factory(**kwargs):
        captured.append(kwargs)
        return FakeNativeDriver()

    registry.register("eyvan-native", factory)
    resolver = ConfiguredExecutionResolver(
        "eyvan-native",
        registry=registry,
        state_dir=tmp_path,
        native_worker_port=8123,
        native_worker_token="agent-global-secret",
    )
    observation = CellObservation(
        "cell-a",
        CapacityStatus.HEALTHY,
        1,
        metadata={"head_ip": "10.20.0.7", "management_uid": "cell-uid", "provider_generation": 1},
    )
    monkeypatch.delitem(sys.modules, "eyvan.runtime.ray_jobs", raising=False)
    monkeypatch.delitem(sys.modules, "ray", raising=False)

    driver = resolver.resolve("cell-a", observation)

    assert isinstance(driver, FakeNativeDriver)
    assert captured == [
        {
            "endpoint": "http://10.20.0.7:8123",
            "state_dir": tmp_path,
            "token": derive_native_worker_token("agent-global-secret", "cell-uid", provider_generation=1),
        }
    ]
    assert "eyvan.runtime.ray_jobs" not in sys.modules
    assert "ray" not in sys.modules


def test_native_resolver_rotates_its_client_when_a_recreated_cell_reuses_an_ip(tmp_path: Path) -> None:
    captured: list[dict] = []
    registry = ExecutionRuntimeRegistry()

    class FakeNativeDriver:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    registry.register("eyvan-native", lambda **kwargs: captured.append(kwargs) or FakeNativeDriver())
    resolver = ConfiguredExecutionResolver(
        "eyvan-native",
        registry=registry,
        state_dir=tmp_path,
        native_worker_token="agent-global-secret",
    )
    first = CellObservation(
        "cell-a",
        CapacityStatus.HEALTHY,
        1,
        execution_endpoint="http://10.20.0.7:8100",
        metadata={"management_uid": "cell-uid", "provider_generation": 1},
    )
    replacement = CellObservation(
        "cell-a",
        CapacityStatus.HEALTHY,
        2,
        execution_endpoint="http://10.20.0.7:8100",
        metadata={"management_uid": "cell-uid", "provider_generation": 2},
    )

    first_driver = resolver.resolve("cell-a", first)
    replacement_driver = resolver.resolve("cell-a", replacement)

    assert first_driver is not replacement_driver
    assert len(captured) == 2
    assert captured[0]["token"] != captured[1]["token"]
    assert first_driver.closed is True

    resolver.close_cell("cell-a")

    assert replacement_driver.closed is True


def test_native_resolver_uses_gce_topology_for_a_multi_node_tpu_qr(tmp_path: Path) -> None:
    resolver = ConfiguredExecutionResolver(
        "eyvan-native",
        state_dir=tmp_path,
        native_worker_token="agent-global-secret",
    )
    observation = CellObservation(
        "cell-a",
        CapacityStatus.HEALTHY,
        1,
        metadata={
            "management_uid": "cell-uid",
            "provider_generation": 1,
            "native_worker_endpoints": ["http://worker-a:8100", "http://worker-b:8100"],
            "tpu_config": {"node_count": 2},
        },
    )
    try:
        driver = resolver.resolve("cell-a", observation)

        assert isinstance(driver, NativeSliceExecutor)
        assert driver.jax_cluster_detection is JaxClusterDetectionMode.GCE_TPU
    finally:
        resolver.close()


def test_native_worker_module_can_be_imported_without_ray() -> None:
    code = "\n".join(
        (
            "import sys",
            "import eyvan.runtime.native_remote",
            "import eyvan.runtime.native_worker",
            "assert 'ray' not in sys.modules",
            "assert 'eyvan.runtime.ray_jobs' not in sys.modules",
        )
    )

    # A clean interpreter catches accidental transitive imports rather than
    # merely inspecting the current test process.
    import subprocess

    subprocess.run((sys.executable, "-c", code), check=True)


def test_a_rebuilt_wheel_replaces_the_installed_package() -> None:
    """Found live 2026-08-01 on a v5p-16 host, and it fails silently.

    The bootstrap writes ``sitecustomize.py`` from a heredoc on every run, but
    installs the package with ``pip install --upgrade <path>``, which
    short-circuits when that exact version is already present. The wheel *URI* is
    content-addressed; its filename and version are not. So a rebuilt wheel at an
    unchanged version leaves the old ``eyvan.runtime._multihost_site`` staged as
    ``eyvan_multihost.py`` underneath a freshly written sitecustomize that imports
    a symbol it does not have.

    Python swallows sitecustomize exceptions, so the only trace is a one-line
    "Error in sitecustomize" and a host whose distributed-topology scrub and
    coordinator initialization never ran -- a silently half-configured JAX
    cluster, which is the failure mode this staging exists to prevent.
    """

    script = build_native_worker_startup_script(
        package="gs://artifacts/eyvan/runtime/wheels/abc/eyvan-0.1.0-py3-none-any.whl",
        port=8123,
        state_dir="/var/lib/eyvan/worker",
        worker_secret_resource="projects/p/secrets/s/versions/latest",
    )

    assert "--force-reinstall" in script


def test_the_forced_reinstall_does_not_rebuild_the_dependency_tree() -> None:
    """Every bootstrap pays this cost, so it must stay bounded to one package."""

    script = build_native_worker_startup_script(
        package="gs://artifacts/eyvan/runtime/wheels/abc/eyvan-0.1.0-py3-none-any.whl",
        port=8123,
        state_dir="/var/lib/eyvan/worker",
        worker_secret_resource="projects/p/secrets/s/versions/latest",
    )

    forced = [line for line in script.splitlines() if "--force-reinstall" in line]
    assert forced, "the forced reinstall must be present for this to mean anything"
    assert all("--no-deps" in line for line in forced)
