# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Bootstrap rendering for a native Eyvan worker on a Compute Engine VM."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
import shlex
import typing as tp
from pathlib import PurePosixPath

from eyvan._internal.json_types import JsonObject, model_json
from eyvan.records import NativeContainerConfig, NativeRuntimeConfig

_ENVIRONMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_WORKLOAD_RUNTIME_ENVIRONMENT = "/etc/eyvan-native/workload-runtime.json"
_WORKLOAD_CONTAINER_RUNTIME = "/etc/eyvan-native/container-runtime.json"


def _required(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"native worker {name} must be a non-empty string")
    if "\x00" in normalized:
        raise ValueError(f"native worker {name} must not contain NUL bytes")
    return normalized


def _runtime_environment(value: tp.Mapping[str, str] | None) -> dict[str, str]:
    """Validate the persistent workload environment passed to the worker."""

    if value is None:
        return {}
    if not isinstance(value, tp.Mapping):
        raise ValueError("native worker runtime environment must be a mapping")
    environment: dict[str, str] = {}
    for name, item in value.items():
        if not isinstance(name, str) or not _ENVIRONMENT_NAME.fullmatch(name):
            raise ValueError("native worker runtime environment names must be valid shell identifiers")
        if not isinstance(item, str):
            raise ValueError(f"native worker runtime environment variable {name!r} must have a string value")
        if "\x00" in item:
            raise ValueError(f"native worker runtime environment variable {name!r} must not contain NUL bytes")
        environment[name] = item
    return environment


def _container_artifact_mounts(value: tp.Sequence[str] | None) -> tuple[str, ...]:
    """Validate staged host paths automatically exposed to an OCI workload."""

    if value is None:
        return ()
    mounts: list[str] = []
    for item in value:
        if not isinstance(item, str) or item != item.strip() or "\x00" in item:
            raise ValueError("native worker container artifact mounts must be trimmed NUL-free paths")
        path = PurePosixPath(item)
        if not path.is_absolute() or ".." in path.parts or str(path) == "/" or str(path).startswith("//"):
            raise ValueError("native worker container artifact mounts must be absolute non-root paths without '..'")
        mounts.append(str(path))
    if len(mounts) != len(set(mounts)):
        raise ValueError("native worker container artifact mounts must be unique")
    return tuple(mounts)


def _container_runtime_payload(
    container: NativeContainerConfig | None,
    artifact_mounts: tp.Sequence[str] | None,
) -> JsonObject | None:
    """Return the JSON handoff consumed by the native worker's OCI executor."""

    mounts = _container_artifact_mounts(artifact_mounts)
    if container is None:
        if mounts:
            raise ValueError("native worker container artifact mounts require a container runtime")
        return None
    if not isinstance(container, NativeContainerConfig):
        raise ValueError("native worker container runtime must be a NativeContainerConfig")
    return {"artifact_mounts": list(mounts), "container": model_json(container)}


def render_native_runtime_setup(runtime: NativeRuntimeConfig) -> str | None:
    """Render trusted fleet setup after the Eyvan worker venv is ready.

    Artifacts are staged before commands so a command can install a wheel or
    invoke a repository script. This is capacity bootstrap configuration, not
    workload input, and therefore runs on every host in a native TPU slice.
    """

    if not isinstance(runtime, NativeRuntimeConfig):
        raise ValueError("native runtime setup requires a NativeRuntimeConfig")
    if not runtime.artifacts and not runtime.init_commands:
        return None
    lines = ["# Eyvan declarative native runtime setup."]
    for name, value in sorted(runtime.env.items()):
        lines.append(f"export {name}={shlex.quote(value)}")
    for artifact in runtime.artifacts:
        source = shlex.quote(artifact.source)
        destination = shlex.quote(artifact.destination)
        if artifact.recursive:
            lines.extend(
                (
                    f"install -d -m 0755 {destination}",
                    f"gcloud storage cp --recursive {source} {destination}",
                )
            )
        else:
            parent = shlex.quote(str(PurePosixPath(artifact.destination).parent))
            lines.extend(
                (
                    f"install -d -m 0755 {parent}",
                    f"gcloud storage cp {source} {destination}",
                )
            )
    lines.extend(command.rstrip() for command in runtime.init_commands)
    return "\n".join(lines)


def _sha256(value: str | None, name: str, *, required: bool) -> str | None:
    """Validate a lowercase SHA-256 used by a bootstrap integrity check."""

    if value is None:
        if required:
            raise ValueError(f"native worker {name} SHA-256 is required")
        return None
    normalized = _required(value, f"{name} SHA-256")
    if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
        raise ValueError(f"native worker {name} SHA-256 must be 64 lowercase hexadecimal characters")
    return normalized


def _content_address_from_uri(uri: str) -> str | None:
    """Infer the digest from ``.../<sha256>/<filename>`` staging layouts."""

    segments = uri.split("#", 1)[0].rstrip("/").split("/")
    if len(segments) < 2:
        return None
    candidate = segments[-2]
    return candidate if re.fullmatch(r"[0-9a-f]{64}", candidate) else None


def _gcs_source(uri: str, generation: str | None, name: str) -> str:
    """Return a generation-qualified GCS URI when the operator supplied one."""

    if generation is None:
        return uri
    normalized = _required(generation, f"{name} GCS generation")
    if not uri.startswith("gs://"):
        raise ValueError(f"native worker {name} generation is supported only for gs:// artifacts")
    if "#" in uri:
        raise ValueError(f"native worker {name} URI must not include a generation fragment twice")
    if re.fullmatch(r"[1-9][0-9]*", normalized) is None:
        raise ValueError(f"native worker {name} GCS generation must be a positive decimal integer")
    return f"{uri}#{normalized}"


def _verify_sha256(path: str, digest: str, name: str) -> tuple[str, ...]:
    """Render a fail-closed SHA-256 check before an artifact is consumed."""

    record = shlex.quote(f"{digest}  {path}")
    return (
        "command -v sha256sum >/dev/null 2>&1 || "
        f"{{ echo 'Eyvan {name} verification requires sha256sum' >&2; exit 1; }}",
        f"printf '%s\n' {record} | sha256sum --check --strict -",
    )


def _install_command(
    package: str,
    *,
    python: str,
    package_sha256: str | None = None,
    package_generation: str | None = None,
    wheelhouse: str | None = None,
    wheelhouse_sha256: str | None = None,
    wheelhouse_generation: str | None = None,
) -> str:
    """Return an install command with verified wheel and wheelhouse content."""

    quoted = shlex.quote(package)
    wheelhouse_lines: tuple[str, ...] = ()
    pip_flags = "--no-cache-dir --upgrade"
    if wheelhouse is not None:
        expected_wheelhouse = _sha256(wheelhouse_sha256, "wheelhouse", required=True)
        assert expected_wheelhouse is not None
        wheelhouse_source = _gcs_source(wheelhouse, wheelhouse_generation, "wheelhouse")
        wheelhouse_path = "/var/lib/eyvan/bootstrap/wheelhouse.zip"
        wheelhouse_lines = (
            f"gcloud storage cp {shlex.quote(wheelhouse_source)} {wheelhouse_path}",
            *_verify_sha256(wheelhouse_path, expected_wheelhouse, "wheelhouse"),
            "rm -rf /var/lib/eyvan/bootstrap/wheels",
            "mkdir -p /var/lib/eyvan/bootstrap/wheels",
            f"{python} -m zipfile -e {wheelhouse_path} /var/lib/eyvan/bootstrap/wheels",
        )
        pip_flags = "--no-cache-dir --no-index --find-links /var/lib/eyvan/bootstrap/wheels --upgrade"
    elif wheelhouse_sha256 is not None or wheelhouse_generation is not None:
        raise ValueError("native worker wheelhouse digest/generation requires a wheelhouse URI")
    if package.startswith("gs://"):
        wheel_name = package.split("#", 1)[0].rsplit("/", 1)[-1]
        if not wheel_name.endswith(".whl") or wheel_name == ".whl":
            raise ValueError("native worker GCS package must name a .whl file")
        expected_package = _sha256(
            package_sha256 or _content_address_from_uri(package),
            "package",
            required=True,
        )
        assert expected_package is not None
        package_source = _gcs_source(package, package_generation, "package")
        wheel_path = f"/var/lib/eyvan/bootstrap/{wheel_name}"
        quoted_wheel_path = shlex.quote(wheel_path)
        # `--force-reinstall`, because a verified/content-addressed object may
        # retain the same wheel filename and version. Ordinary --upgrade would
        # silently keep an earlier build already installed on the VM.
        return "\n".join(
            (
                "mkdir -p /var/lib/eyvan/bootstrap",
                f"gcloud storage cp {shlex.quote(package_source)} {quoted_wheel_path}",
                *_verify_sha256(wheel_path, expected_package, "package"),
                *wheelhouse_lines,
                f"{python} -m pip install {pip_flags} --force-reinstall --no-deps {quoted_wheel_path}",
                # Dependencies are resolved separately so --force-reinstall does
                # not rebuild the entire tree on every bootstrap.
                f"{python} -m pip install {pip_flags} {quoted_wheel_path}",
            )
        )
    if package_sha256 is not None or package_generation is not None:
        raise ValueError("native worker package digest/generation requires a gs:// wheel URI")
    return "\n".join((*wheelhouse_lines, f"{python} -m pip install {pip_flags} {quoted}"))


def derive_native_worker_token(
    shared_secret: str | None,
    management_uid: str,
    *,
    provider_generation: int = 1,
) -> str | None:
    """Derive a VM-scoped worker credential without exposing the agent secret."""

    if shared_secret is None:
        return None
    shared_secret = _required(shared_secret, "shared secret")
    management_uid = _required(management_uid, "management UID")
    if isinstance(provider_generation, bool) or not isinstance(provider_generation, int) or provider_generation < 1:
        raise ValueError("native worker provider generation must be a positive integer")
    return hmac.new(
        shared_secret.encode(),
        f"eyvan-native-worker:{management_uid}:r{provider_generation}".encode(),
        hashlib.sha256,
    ).hexdigest()


def build_native_worker_startup_script(
    *,
    package: str,
    port: int,
    state_dir: str,
    worker_secret_resource: str,
    package_sha256: str | None = None,
    package_generation: str | None = None,
    wheelhouse: str | None = None,
    wheelhouse_sha256: str | None = None,
    wheelhouse_generation: str | None = None,
    before_start: str | None = None,
    runtime_setup: str | None = None,
    runtime_env: tp.Mapping[str, str] | None = None,
    runtime_container: NativeContainerConfig | None = None,
    container_artifact_mounts: tp.Sequence[str] | None = None,
) -> str:
    """Render an idempotent systemd bootstrap for one native worker.

    ``before_start`` preserves the legacy provider setup hook and runs before
    Eyvan is installed. ``runtime_setup`` is generated from the typed fleet
    runtime and runs after the worker virtual environment exists, allowing an
    artifact to be installed without placing bootstrap details in job
    submission. ``runtime_container`` is a typed OCI handoff; its Docker
    engine preflight intentionally runs after setup so fleet init commands can
    install or configure the engine.
    """

    package = _required(package, "package")
    state_dir = _required(state_dir, "state directory")
    worker_secret_resource = _required(worker_secret_resource, "secret resource")
    if wheelhouse is not None:
        wheelhouse = _required(wheelhouse, "wheelhouse")
    if port <= 0 or port > 65535:
        raise ValueError("native worker port must be between 1 and 65535")
    if before_start is not None and not before_start.strip():
        raise ValueError("native worker setup script must be non-empty when set")
    if runtime_setup is not None and not runtime_setup.strip():
        raise ValueError("native worker runtime setup must be non-empty when set")
    runtime_env = _runtime_environment(runtime_env)
    container_payload = _container_runtime_payload(runtime_container, container_artifact_mounts)
    runtime_environment_lines: tuple[str, ...] = ()
    if runtime_env:
        encoded_environment = base64.b64encode(
            json.dumps(runtime_env, sort_keys=True, separators=(",", ":")).encode()
        ).decode()
        quoted_environment_path = shlex.quote(_WORKLOAD_RUNTIME_ENVIRONMENT)
        runtime_environment_lines = (
            f"printf '%s' {shlex.quote(encoded_environment)} | base64 --decode > {quoted_environment_path}",
            f"chmod 0600 {quoted_environment_path}",
        )
    runtime_container_lines: tuple[str, ...] = ()
    if container_payload is not None:
        encoded_container = base64.b64encode(
            json.dumps(container_payload, sort_keys=True, separators=(",", ":")).encode()
        ).decode()
        quoted_container_path = shlex.quote(_WORKLOAD_CONTAINER_RUNTIME)
        runtime_container_lines = (
            f"printf '%s' {shlex.quote(encoded_container)} | base64 --decode > {quoted_container_path}",
            f"chmod 0600 {quoted_container_path}",
        )
    runtime_setup_lines: tuple[str, ...] = ()
    if runtime_setup is not None:
        runtime_setup_lines = ("", "# Declarative fleet runtime setup.", runtime_setup.rstrip())
    container_preflight_lines: tuple[str, ...] = ()
    if container_payload is not None:
        container_preflight_lines = (
            "if ! command -v docker >/dev/null 2>&1 || ! docker version >/dev/null 2>&1; then",
            "  echo 'Eyvan OCI runtime requires a running Docker-compatible engine; "
            "install and start it in runtime.init_commands' >&2",
            "  exit 1",
            "fi",
        )

    venv_dir = "/opt/eyvan-native/venv"
    venv_python = f"{venv_dir}/bin/python"
    runtime_dir = "/run/eyvan-native"
    secret_path = f"{runtime_dir}/worker-secret"
    workload_user = "eyvan-workload"
    workload_group = "eyvan-workload"
    workload_directory = "/var/lib/eyvan/workload"
    multihost_site_directory = "/opt/eyvan-native/multihost"
    systemd_runtime_environment = "/etc/eyvan-native/systemd-runtime.env"
    environment = [
        "Environment=PYTHONUNBUFFERED=1",
        f"Environment=PATH={venv_dir}/bin:/snap/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        f"Environment=VIRTUAL_ENV={venv_dir}",
        f"Environment=EYVAN_NATIVE_WORKLOAD_USER={workload_user}",
        f"Environment=EYVAN_NATIVE_WORKLOAD_GROUP={workload_group}",
        f"Environment=EYVAN_NATIVE_WORKLOAD_DIRECTORY={workload_directory}",
        f"Environment=XDG_CACHE_HOME={workload_directory}/.cache",
        f"EnvironmentFile={systemd_runtime_environment}",
    ]
    command_parts = [
        f"{venv_python} -m eyvan.worker.runtime.native_worker",
        "--host 0.0.0.0",
        f"--port {port}",
        f"--state-dir {shlex.quote(state_dir)}",
        f"--secret-file {shlex.quote(secret_path)}",
    ]
    if runtime_env:
        command_parts.extend(("--runtime-env-file", shlex.quote(_WORKLOAD_RUNTIME_ENVIRONMENT)))
    if container_payload is not None:
        command_parts.extend(("--container-runtime-file", shlex.quote(_WORKLOAD_CONTAINER_RUNTIME)))
    command_parts.append("--systemd-transient")
    command = " ".join(command_parts)
    fetch_secret = " ".join(
        (
            "umask 077;",
            "gcloud secrets versions access",
            shlex.quote(worker_secret_resource),
            ">",
            shlex.quote(secret_path),
        )
    )
    unit = "\n".join(
        (
            "[Unit]",
            "Description=Eyvan native worker",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=simple",
            "RuntimeDirectory=eyvan-native",
            "RuntimeDirectoryMode=0700",
            *environment,
            f"ExecStartPre=/bin/sh -ec {shlex.quote(fetch_secret)}",
            f"ExecStart={command}",
            "KillMode=control-group",
            "Restart=always",
            "RestartSec=2",
            "",
            "[Install]",
            "WantedBy=multi-user.target",
        )
    )
    lines = [
        "#!/bin/bash",
        "set -euo pipefail",
        "eyvan_apt_get() {",
        "  if ! command -v apt-get >/dev/null 2>&1; then",
        "    echo 'Eyvan bootstrap requires apt-get for this operation' >&2",
        "    return 127",
        "  fi",
        '  command apt-get -o DPkg::Lock::Timeout=300 "$@"',
        "}",
    ]
    if before_start:
        lines.extend(("", "# Fleet-provided setup runs before the Eyvan worker service.", before_start.rstrip()))
    lines.extend(
        (
            "",
            "export DEBIAN_FRONTEND=noninteractive",
            "mkdir -p /var/lib/eyvan/bootstrap",
            "if ! command -v gcloud >/dev/null 2>&1; then",
            "  echo 'Eyvan native worker requires gcloud in the VM image' >&2",
            "  exit 1",
            "fi",
            "if ! command -v systemd-run >/dev/null 2>&1 || ! command -v systemctl >/dev/null 2>&1; then",
            "  echo 'Eyvan native worker requires systemd-run and systemctl in the VM image' >&2",
            "  exit 1",
            "fi",
            "if ! command -v runuser >/dev/null 2>&1; then",
            "  echo 'Eyvan native worker requires runuser in the VM image' >&2",
            "  exit 1",
            "fi",
            "systemd_version=$(systemctl --version | awk 'NR == 1 { print $2 }')",
            "systemd_exit_type_cgroup=0",
            'if [ "${systemd_version}" -ge 250 ] 2>/dev/null; then',
            "  systemd_exit_type_cgroup=1",
            "fi",
            "install -d -m 0755 /etc/eyvan-native",
            "printf 'EYVAN_SYSTEMD_EXIT_TYPE_CGROUP=%s\\n' \"${systemd_exit_type_cgroup}\" "
            "> /etc/eyvan-native/systemd-runtime.env",
            "chmod 0644 /etc/eyvan-native/systemd-runtime.env",
            *runtime_environment_lines,
            *runtime_container_lines,
            f"if ! {venv_python} -m pip --version >/dev/null 2>&1; then",
            f"  rm -rf {venv_dir}",
            "  if ! python3 -m ensurepip --version >/dev/null 2>&1; then",
            "    if ! command -v apt-get >/dev/null 2>&1; then",
            "      echo 'Eyvan native worker requires Python venv support and apt-get is unavailable' >&2",
            "      exit 1",
            "    fi",
            "    eyvan_apt_get update",
            "    eyvan_apt_get install -y --no-install-recommends python3-venv",
            "  fi",
            "  if ! python3 -m ensurepip --version >/dev/null 2>&1; then",
            "    echo 'Eyvan native worker could not install Python venv support' >&2",
            "    exit 1",
            "  fi",
            f"  python3 -m venv --system-site-packages {venv_dir}",
            "fi",
            _install_command(
                package,
                python=venv_python,
                package_sha256=package_sha256,
                package_generation=package_generation,
                wheelhouse=wheelhouse,
                wheelhouse_sha256=wheelhouse_sha256,
                wheelhouse_generation=wheelhouse_generation,
            ),
            f"chmod -R a+rX {venv_dir}",
            f"install -d -m 0755 {multihost_site_directory}",
            f'multihost_module=$({venv_python} -c "import eyvan.worker.runtime._multihost_site as module; '
            'print(module.__file__)")',
            f'install -m 0644 "${{multihost_module}}" {multihost_site_directory}/eyvan_multihost.py',
            f'distributed_environment_module=$({venv_python} -c "import eyvan.worker.distributed_environment as module; '
            'print(module.__file__)")',
            f'install -m 0644 "${{distributed_environment_module}}" '
            f"{multihost_site_directory}/eyvan_distributed_environment.py",
            f"cat >{multihost_site_directory}/sitecustomize.py <<'EYVAN_MULTIHOST_SITECUSTOMIZE'",
            # Scrub-then-initialize lives in eyvan_multihost.initialize_worker_runtime,
            # not here: ordering expressed in a heredoc can only be asserted as a
            # string, and the order is the whole correctness property.
            "from eyvan_multihost import initialize_worker_runtime",
            "",
            "initialize_worker_runtime()",
            "EYVAN_MULTIHOST_SITECUSTOMIZE",
            f"chmod 0644 {multihost_site_directory}/sitecustomize.py",
            f"if ! getent group {workload_group} >/dev/null 2>&1; then",
            f"  groupadd --system {workload_group}",
            "fi",
            f"if ! id -u {workload_user} >/dev/null 2>&1; then",
            f"  useradd --system --gid {workload_group} --home-dir {workload_directory} --create-home "
            f"--shell /usr/sbin/nologin {workload_user}",
            "fi",
            f"if ! runuser -u {workload_user} -- {venv_python} -c 'import sys' >/dev/null 2>&1; then",
            "  echo 'Eyvan native worker virtual environment is not executable by the workload user; "
            "install its interpreter under a shared path such as /opt/eyvan-native/python' >&2",
            "  exit 1",
            "fi",
            f"install -d -o {workload_user} -g {workload_group} -m 0750 {workload_directory} "
            f"{workload_directory}/.cache",
            "for device in /dev/vfio/* /dev/vfio/devices/* /dev/accel* /dev/nvidiactl /dev/nvidia-uvm "
            "/dev/nvidia[0-9]*; do",
            '  [ -c "${device}" ] || continue',
            "  device_group=$(stat -c '%G' \"${device}\")",
            '  if [ "${device_group}" != root ] && getent group "${device_group}" >/dev/null 2>&1; then',
            f'    usermod -a -G "${{device_group}}" {workload_user}',
            "  fi",
            "done",
            "needs_device_acl=0",
            "for device in /dev/vfio/* /dev/vfio/devices/* /dev/accel* /dev/nvidiactl /dev/nvidia-uvm "
            "/dev/nvidia[0-9]*; do",
            '  [ -c "${device}" ] || continue',
            f'  if ! runuser -u {workload_user} -- test -r "${{device}}" -a -w "${{device}}"; then',
            "    needs_device_acl=1",
            "    break",
            "  fi",
            "done",
            'if [ "${needs_device_acl}" -eq 1 ]; then',
            "  if ! command -v setfacl >/dev/null 2>&1; then",
            "    if ! command -v apt-get >/dev/null 2>&1; then",
            "      echo 'Eyvan native worker requires setfacl to grant workload access to accelerator devices' >&2",
            "      exit 1",
            "    fi",
            "    eyvan_apt_get update",
            "    eyvan_apt_get install -y --no-install-recommends acl",
            "  fi",
            "  for device in /dev/vfio/* /dev/vfio/devices/* /dev/accel* /dev/nvidiactl /dev/nvidia-uvm "
            "/dev/nvidia[0-9]*; do",
            '    [ -c "${device}" ] || continue',
            f'    setfacl -m u:{workload_user}:rw "${{device}}"',
            "  done",
            "fi",
            "for device in /dev/vfio/* /dev/vfio/devices/* /dev/accel* /dev/nvidiactl /dev/nvidia-uvm "
            "/dev/nvidia[0-9]*; do",
            '  [ -c "${device}" ] || continue',
            f'  if ! runuser -u {workload_user} -- test -r "${{device}}" -a -w "${{device}}"; then',
            '    echo "Eyvan workload user cannot access accelerator device ${device}" >&2',
            "    exit 1",
            "  fi",
            "done",
            f"install -d -m 0700 {shlex.quote(state_dir)}",
            *runtime_setup_lines,
            *container_preflight_lines,
            "cat >/etc/systemd/system/eyvan-native-worker.service <<'EYVAN_NATIVE_WORKER_UNIT'",
            unit,
            "EYVAN_NATIVE_WORKER_UNIT",
            "systemctl daemon-reload",
            "systemctl enable eyvan-native-worker.service",
            "systemctl restart eyvan-native-worker.service",
            "",
        )
    )
    return "\n".join(lines)


__all__ = ("build_native_worker_startup_script", "derive_native_worker_token", "render_native_runtime_setup")
