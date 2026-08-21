# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Coverage for interpreter-steering scrubbing at provider CLI boundaries.

The behavior under test exists because of a live outage on 2026-08-22: a client
process set a checkout-relative ``PYTHONPATH``, the cell agent inherited it,
and every ``gcloud`` it spawned crashed at import time when macOS TCC refused
the CLI's embedded Python a directory listing the invoking terminal was
allowed. Cells that were ACTIVE and billing never reached READY.
"""

from __future__ import annotations

import subprocess
import typing as tp

from eyvan._internal.subprocess_env import INTERPRETER_STEERING_ENV_NAMES, provider_cli_env


def test_provider_cli_env_strips_interpreter_steering_and_keeps_the_rest() -> None:
    base = {
        "PATH": "/usr/bin",
        "HOME": "/home/operator",
        "PYTHONPATH": "libs/easydel:libs/eyvan",
        "PYTHONHOME": "/opt/custom-python",
        "PYTHONSTARTUP": "/home/operator/.pythonrc",
        "PYTHONNOUSERSITE": "1",
        "PYTHONUSERBASE": "/home/operator/.local",
        "PYTHONEXECUTABLE": "/usr/bin/python3",
        "__PYVENV_LAUNCHER__": "/usr/bin/python3",
        "CLOUDSDK_PYTHON": "/usr/bin/python3",  # operator-chosen gcloud interpreter: respected
        "EYVAN_URL": "http://127.0.0.1:8080",
    }

    environment = provider_cli_env(base)

    for name in INTERPRETER_STEERING_ENV_NAMES:
        assert name not in environment
    assert environment == {
        "PATH": "/usr/bin",
        "HOME": "/home/operator",
        "CLOUDSDK_PYTHON": "/usr/bin/python3",
        "EYVAN_URL": "http://127.0.0.1:8080",
    }
    # The caller's mapping is never mutated, and two calls never share a dict.
    assert base["PYTHONPATH"] == "libs/easydel:libs/eyvan"
    assert provider_cli_env(base) is not environment


def test_provider_cli_env_defaults_to_the_process_environment(monkeypatch) -> None:
    monkeypatch.setenv("PYTHONPATH", "libs/easydel")
    monkeypatch.setenv("EYVAN_PROJECT", "default")

    environment = provider_cli_env()

    assert "PYTHONPATH" not in environment
    assert environment["EYVAN_PROJECT"] == "default"


def _captured_env_run(
    captured: dict[str, tp.Any],
) -> tp.Callable[..., subprocess.CompletedProcess[str]]:
    """Return a subprocess.run stand-in that records the environment it was given."""

    def run(*args: tp.Any, **kwargs: tp.Any) -> subprocess.CompletedProcess[str]:
        captured["env"] = kwargs.get("env")
        return subprocess.CompletedProcess(args[0], 0, "", "")

    return run


def test_gcloud_runners_never_inherit_pythonpath(monkeypatch) -> None:
    """Every module-level gcloud boundary must scrub the caller's interpreter steering."""

    monkeypatch.setenv("PYTHONPATH", "libs/easydel:libs/eyvan")

    from eyvan.capacity import fleet_state
    from eyvan.capacity.providers.gcp import compute_instances, native_worker_ingress, queued_resources

    for runner in (
        queued_resources._runner,
        compute_instances._runner,
        native_worker_ingress._gcloud_runner,
        fleet_state._gcloud,
    ):
        captured: dict[str, tp.Any] = {}
        monkeypatch.setattr(subprocess, "run", _captured_env_run(captured))
        runner(("version",))
        env = captured.get("env")
        assert env is not None, "the runner must pass an explicit scrubbed environment"
        assert "PYTHONPATH" not in env


def test_gcs_artifact_store_default_runner_scrubs_interpreter_steering(monkeypatch, tmp_path) -> None:
    """The SDK's own callable upload runs gcloud from the submitting process."""

    from eyvan.artifacts import GCSArtifactStore

    store = GCSArtifactStore("gs://eyvan-artifacts")
    captured: dict[str, tp.Any] = {}
    monkeypatch.setattr(subprocess, "run", _captured_env_run(captured))

    store.read("gs://eyvan-artifacts/callables/x.pkl")

    env = captured.get("env")
    assert env is not None
    assert "PYTHONPATH" not in env
