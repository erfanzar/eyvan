# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Local-environment preflight checks for ``eyvan doctor``.

These checks ask about *this machine* rather than a running control plane, so
they still answer when nothing has been started: gcloud authentication, the
Secret Manager bind permission the native bootstrap needs, the artifact
store, the GCP native runtime profile, and ownership of the control-plane
port.

Status discipline, chosen to match the checks already living in doctor:
genuine blockers are ``fail``; an unanswerable question whose answer the
fleet will establish later is ``warn``; a check that does not apply to this
machine at all (no gcloud, no profile, a remote endpoint) is ``pass`` with
``not applicable:`` in the detail, because doctor's ``ok`` treats ``skip`` as
not-ok and a GCP-less laptop is not a sick one. Unknown stays unknown: it is
never reported as allowed.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import socket
import subprocess
from urllib.parse import urlsplit

import httpx

from eyvan._internal.subprocess_env import provider_cli_env
from eyvan.capacity.providers.gcp.compute_instances import (
    GcloudError,
    GcloudNativeWorkerSecretStore,
)
from eyvan.capacity.providers.gcp.runtime.profile import (
    DEFAULT_GCP_NATIVE_RUNTIME_PROFILE_PATH,
    GcpNativeRuntimeProfileError,
    GcpNativeRuntimeProfileStore,
)
from eyvan.config import EyvanSettings

#: One check result as the doctor service consumes it:
#: (name, status, detail, remedy). Kept positional so the service maps them
#: onto CheckResult without a schema both sides must agree on separately.
PreflightOutcome = tuple[str, str, str, str]


def gcloud_run(argv: tuple[str, ...]) -> tuple[bool, str]:
    """Run one gcloud command; return (succeeded, one-line diagnostic).

    The diagnostic is the first output line, never the whole stream: a
    ``print-access-token`` success carries the token on stdout, and a
    diagnostic that leaked it would hand the credential to every log reader.
    The seam is module-level so tests can replace it without subprocesses.
    """

    if shutil.which("gcloud") is None:
        return False, "gcloud is not installed or not on PATH"
    try:
        result = subprocess.run(
            ("gcloud", *argv), capture_output=True, text=True, timeout=30, check=False, env=provider_cli_env()
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return False, f"gcloud {' '.join(argv)} could not run: {error}"
    if result.returncode != 0:
        lines = (result.stderr or result.stdout or "").strip().splitlines()
        return False, lines[0] if lines else f"gcloud {' '.join(argv)} exited {result.returncode}"
    return True, (result.stdout or "").strip()


def _gcp_project() -> str | None:
    ok, output = gcloud_run(("config", "get-value", "project"))
    return output if ok and output and output != "(unset)" else None


def check_gcloud_authentication() -> PreflightOutcome:
    """Preflight the active gcloud identity."""

    name = "gcloud-authentication"
    if shutil.which("gcloud") is None:
        return (
            name,
            "pass",
            "not applicable: gcloud is not installed; only GCP fleets need it",
            "",
        )
    ok, output = gcloud_run(("auth", "print-access-token"))
    if ok:
        return (name, "pass", f"an access token is available for {_gcp_project() or 'the active account'}", "")
    return (
        name,
        "fail",
        f"no usable gcloud credential: {output}",
        "run `gcloud auth application-default login` (API access) or `gcloud auth login` (CLI access)",
    )


def check_secret_manager_bind() -> PreflightOutcome:
    """Preflight the IAM permission the native bootstrap cannot proceed without."""

    name = "secret-manager-bind-permission"
    if shutil.which("gcloud") is None:
        return (name, "pass", "not applicable: gcloud is not installed; only GCP fleets need it", "")
    project = _gcp_project()
    if not project:
        return (name, "pass", "not applicable: no gcloud project is selected", "")
    try:
        allowed, denial = GcloudNativeWorkerSecretStore().caller_bind_state(project=project)
    except GcloudError as error:
        return (name, "fail", str(error), "")
    if allowed:
        return (name, "pass", f"the active account can bind cell secrets in {project}", "")
    if denial is not None:
        return (name, "fail", denial, "")
    # Unknown, stated as unknown. No remedy field on purpose: a warn with a
    # remedy becomes `next_step`, and this uncertainty must not elbow out a
    # concrete actionable finding behind it. The fleet re-verifies before
    # creating anything, so the detail says that rather than crying wolf.
    return (
        name,
        "warn",
        f"could not determine whether the active account can bind cell secrets in {project} "
        "(no token, or the Secret Manager API was unreachable); the fleet verifies this before "
        "creating anything",
        "",
    )


def check_artifact_store(settings: EyvanSettings) -> PreflightOutcome:
    """Preflight the configured artifact store: gs:// reachability or local writability."""

    name = "artifact-store"
    uri = settings.artifact_store_uri
    parsed = urlsplit(uri)
    if parsed.scheme == "gs":
        if shutil.which("gcloud") is None:
            return (
                name,
                "warn",
                f"cannot verify configured artifact bucket gs://{parsed.netloc}: gcloud is not installed",
                "install gcloud to run the bucket preflight, or verify access with the runtime's storage credentials",
            )
        ok, output = gcloud_run(("storage", "buckets", "describe", f"gs://{parsed.netloc}", "--format=value(name)"))
        if ok:
            return (name, "pass", f"gs://{parsed.netloc} is reachable", "")
        return (
            name,
            "fail",
            f"artifact bucket gs://{parsed.netloc} is not reachable: {output}",
            "create it (`gcloud storage buckets create gs://NAME`) or grant the active account storage.objects access",
        )
    if parsed.scheme not in {"", "file"}:
        return (name, "pass", f"not applicable: artifact store scheme {parsed.scheme!r} is not locally checkable", "")
    local = pathlib.Path(parsed.path).expanduser()
    if local.is_dir() and os.access(local, os.W_OK):
        return (name, "pass", f"{local} exists and is writable", "")
    # The plane creates the store directory on startup; what the preflight
    # actually guards is that creation being possible at all.
    parent = local
    while not parent.exists() and parent != parent.parent:
        parent = parent.parent
    if parent.is_dir() and os.access(parent, os.W_OK):
        return (
            name,
            "pass",
            f"{local} does not exist yet and its parent is writable; the plane creates it on startup",
            "",
        )
    return (
        name,
        "fail",
        f"local artifact store {local} is missing and cannot be created: {parent} is not writable",
        f"create it (`mkdir -p {local}`) or point EYVAN_ARTIFACT_STORE_URI elsewhere",
    )


def check_runtime_profile() -> PreflightOutcome:
    """Preflight the GCP native runtime profile: present, parseable, safe."""

    name = "runtime-profile"
    path = DEFAULT_GCP_NATIVE_RUNTIME_PROFILE_PATH
    if not path.expanduser().exists():
        return (
            name,
            "pass",
            "not applicable: no GCP native runtime profile; run `eyvan runtime init` when you stand up a "
            "GCP-native fleet",
            "",
        )
    try:
        profile = GcpNativeRuntimeProfileStore(path).load()
    except (GcpNativeRuntimeProfileError, ValueError, OSError) as error:
        return (
            name,
            "fail",
            f"the runtime profile at {path} is unreadable or unsafe: {error}",
            "re-run `eyvan runtime init` to rewrite it; the profile pins the artifact root and secrets",
        )
    return (
        name,
        "pass",
        f"profile for project {profile.project_id} with artifact root {profile.artifact_root_uri}",
        "",
    )


def check_control_plane_port(base_url: str) -> PreflightOutcome:
    """Preflight the port a local plane would bind: free, serving, or squatted."""

    name = "control-plane-port"
    if not base_url:
        return (name, "pass", "not applicable: no control-plane URL is configured, so there is no local port", "")
    parsed = urlsplit(base_url)
    host = parsed.hostname or ""
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError:
        return (name, "pass", f"not applicable: {base_url!r} does not carry a usable port", "")
    if host not in {"127.0.0.1", "localhost", "::1"}:
        return (
            name,
            "pass",
            f"not applicable: the configured endpoint {host} is remote; its port is the remote host's concern",
            "",
        )
    family = socket.AF_INET6 if host == "::1" else socket.AF_INET
    probe = socket.socket(family, socket.SOCK_STREAM)
    probe.settimeout(2)
    try:
        reachable = probe.connect_ex((host, port)) == 0
    except OSError:
        reachable = False
    finally:
        probe.close()
    if not reachable:
        return (name, "pass", f"{host}:{port} is free for a control plane to bind", "")
    authority_host = f"[{host}]" if ":" in host else host
    try:
        health = httpx.get(f"{parsed.scheme or 'http'}://{authority_host}:{port}/api/v1/health", timeout=3.0)
        serving = health.status_code == 200 and '"controller"' in health.text
    except (OSError, httpx.HTTPError):
        serving = False
    if serving:
        return (name, "pass", f"an Eyvan control plane already answers on {host}:{port}", "")
    return (
        name,
        "fail",
        f"{host}:{port} is occupied by a process that is not an Eyvan control plane",
        f"free the port (find the holder with `lsof -i :{port}`), point the client at the real plane with "
        f"`eyvan use FLEET`, or start the server on another port with `eyvan-server --port PORT`",
    )
