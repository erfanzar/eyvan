# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Adding a package to one job without rebuilding an image.

The half of a per-job runtime environment Eyvan did not have. A workload could
already carry its working tree and its variables; its dependencies could only be
whatever the host image happened to hold, so adding one library meant building
an image and rolling a fleet -- which is exactly the friction people reach for
Ray to avoid.

Most of what is tested here is the failure side, because the two ways this goes
wrong are both silent. An install that fails and is ignored produces an
ImportError minutes later, on an accelerator, blamed on the user's code. And a
clean virtualenv on a TPU host hides `jax` and `libtpu`, so a job that asked for
one small package would find its accelerator stack gone.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from eyvan.records import CallableExecution, CommandExecution, WorkloadDefinition
from eyvan.worker.runtime import dependency_entry

# --------------------------------------------------------------------------- what a job may declare


def _spec(*dependencies: str, callable_execution: bool = False) -> WorkloadDefinition:
    execution = (
        CallableExecution(artifact_uri="file:///tmp/x.pkl", sha256="0" * 64, python="3.13")
        if callable_execution
        else CommandExecution(command="python train.py")
    )
    return WorkloadDefinition(name="run", execution=execution, dependencies=dependencies)


def test_a_job_may_name_the_packages_it_needs() -> None:
    assert _spec("jax>=0.4.30", "flax").dependencies == ("jax>=0.4.30", "flax")


@pytest.mark.parametrize("option", ["-r requirements.txt", "--index-url https://pypi.example/simple", "-e ."])
def test_an_installer_option_is_not_a_dependency(option: str) -> None:
    """Both of these are honoured by pip and neither is a package.

    ``-r`` names a file on the *host*, which nothing shipped there, and
    ``--index-url`` points the install at somewhere the submitter chose. A
    dependency list that quietly accepts either is not a dependency list.
    """

    with pytest.raises(ValueError, match="installer option"):
        _spec(option)


def test_a_callable_carries_dependencies_too() -> None:
    """The first draft refused this, and the reasoning was wrong.

    The wrapper `exec`s, so the callable worker starts *under* the prepared
    environment: the interpreter is still the host's, with the job's packages
    layered over it, which is exactly what the pickled function's imports
    resolve against. A callable is the case that needs this most -- its
    dependencies are whatever the pickle happens to reference.
    """

    assert _spec("flax", callable_execution=True).dependencies == ("flax",)


# --------------------------------------------------------------------------- the environment that gets built


def test_the_same_packages_in_a_different_order_are_the_same_environment() -> None:
    """Order is not part of what was asked for, and two keys means two installs."""

    assert dependency_entry.environment_key(["flax", "jax"]) == dependency_entry.environment_key(["jax", "flax"])
    assert dependency_entry.environment_key(["jax"]) != dependency_entry.environment_key(["jax", "flax"])


def test_the_environment_can_see_the_host_accelerator_stack() -> None:
    """The decision this whole feature stands on.

    A clean virtualenv on a TPU host has no ``jax`` and no ``libtpu``, so a job
    that asked for one unrelated package would install a second, CPU-only JAX
    over a working one and fail on its first device call.
    """

    commands = dependency_entry._install_commands(Path("/tmp/env"), ["flax"])

    assert any("--system-site-packages" in command for command in commands)


def _fake_install(environment: Path) -> list[list[str]]:
    """Stand in for `uv venv`, which is a command that creates the directory."""

    return [[sys.executable, "-c", f"import pathlib; pathlib.Path({str(environment)!r}).mkdir(parents=True)"]]


def test_a_failed_install_fails_the_job_rather_than_starting_it(tmp_path, monkeypatch) -> None:
    """Running anyway produces an ImportError minutes later on an accelerator."""

    monkeypatch.setattr(
        dependency_entry,
        "_install_commands",
        lambda environment, requirements: [[sys.executable, "-c", "import sys; sys.exit(7)"]],
    )

    with pytest.raises(RuntimeError, match="failed"):
        dependency_entry.prepare(["nonexistent-package"], root=tmp_path)


def test_a_crashed_install_is_rebuilt_rather_than_trusted(tmp_path, monkeypatch) -> None:
    """The marker exists so a half-populated directory is not mistaken for one.

    A preemption during install leaves a virtualenv with some of the packages
    in it, which is indistinguishable from a good one by anything except the
    marker that is written last.
    """

    partial = tmp_path / dependency_entry.environment_key(["flax"])
    partial.mkdir(parents=True)
    (partial / "leftover").write_text("from an attempt that died")
    built: list[Path] = []

    def record(environment: Path, requirements):
        built.append(environment)
        return _fake_install(environment)

    monkeypatch.setattr(dependency_entry, "_install_commands", record)

    dependency_entry.prepare(["flax"], root=tmp_path)

    assert built == [partial]
    assert not (partial / "leftover").exists(), "the dead attempt's directory was reused"
    assert (partial / dependency_entry.COMPLETE_MARKER).exists()


def test_a_complete_environment_is_reused_rather_than_rebuilt(tmp_path, monkeypatch) -> None:
    """Eight ranks on one host declare the same packages; one of them installs."""

    calls: list[Path] = []
    monkeypatch.setattr(
        dependency_entry,
        "_install_commands",
        lambda environment, requirements: (calls.append(environment), _fake_install(environment))[1],
    )

    first = dependency_entry.prepare(["flax"], root=tmp_path)
    second = dependency_entry.prepare(["flax"], root=tmp_path)

    assert first == second
    assert len(calls) == 1


def test_activation_prepends_rather_than_replacing_the_path() -> None:
    """An accelerator host keeps vendor binaries outside any virtual environment."""

    activated = dependency_entry.activate(Path("/envs/abc"), {"PATH": "/usr/bin"})

    assert activated["PATH"] == f"/envs/abc/bin{os.pathsep}/usr/bin"
    assert activated["VIRTUAL_ENV"] == "/envs/abc"


# --------------------------------------------------------------------------- how it reaches the host


def _entrypoint(*dependencies: str, command: str = "python train.py") -> str:
    from eyvan.server.controller.workload_commands import _ControllerWorkloadCommands

    return _ControllerWorkloadCommands._dependency_entrypoint(dependencies, command)


def test_a_job_declaring_nothing_runs_exactly_what_was_written() -> None:
    """No wrapper, no cache lookup, no second process for a job that asked for none."""

    assert _entrypoint() == "python train.py"


def test_a_version_specifier_survives_the_shell() -> None:
    """`jax>=0.4` unquoted is a redirect that truncates a file called `=0.4`."""

    wrapped = _entrypoint("jax>=0.4.30")

    assert "'jax>=0.4.30'" in wrapped
    assert wrapped.endswith(" python train.py")
    assert "eyvan.worker.runtime.dependency_entry" in wrapped


def test_the_wrapper_runs_before_the_workspace_entry() -> None:
    """Order matters: the workspace entry is itself `python -m`.

    It resolves through PATH, and the packages have to be in place by then --
    including `eyvan` itself, which the entry imports. Installing after would
    put the environment on PATH for nothing, because the command has already
    been exec'd by then.
    """

    from eyvan.server.controller.workload_commands import _ControllerWorkloadCommands

    wrapped = _ControllerWorkloadCommands._dependency_entrypoint(
        ("flax",),
        "python -m eyvan.worker.runtime.workspace_entry --workspace-uri file:///w -- python train.py",
    )

    assert wrapped.index("dependency_entry") < wrapped.index("workspace_entry")


# --------------------------------------------------------------------------- what the CLI sends


def test_a_requirements_file_is_read_here_rather_than_on_the_host(tmp_path) -> None:
    """The whole reason the flag exists rather than passing `-r` through.

    That path names a file on the submitter's machine. The host has never seen
    it, so pip would either fail or read something else with the same name.
    """

    from eyvan.sdk.commands.workloads import _requirements

    listing = tmp_path / "requirements.txt"
    listing.write_text("# training\nflax==0.8.0\n\njax>=0.4.30  # pinned upstream\n", encoding="utf-8")

    assert _requirements(("optax",), listing) == ("optax", "flax==0.8.0", "jax>=0.4.30")


def test_a_requirements_file_that_redirects_the_install_is_refused(tmp_path) -> None:
    """`--index-url` inside the file does on the host what passing it would."""

    import click

    from eyvan.sdk.commands.workloads import _requirements

    listing = tmp_path / "requirements.txt"
    listing.write_text("--index-url https://pypi.example/simple\nflax\n", encoding="utf-8")

    with pytest.raises(click.UsageError, match="installer option"):
        _requirements((), listing)


def test_the_same_package_named_twice_is_installed_once(tmp_path) -> None:
    from eyvan.sdk.commands.workloads import _requirements

    listing = tmp_path / "requirements.txt"
    listing.write_text("flax\n", encoding="utf-8")

    assert _requirements(("flax", "optax"), listing) == ("flax", "optax")


def test_a_callable_dispatch_installs_before_the_worker_starts() -> None:
    """The wrapper is outermost on this path too.

    The callable worker resolves `python` through PATH and imports `eyvan`, so
    the environment has to exist before it runs -- and the pickled function's
    own imports resolve after that, inside the same process.
    """

    from eyvan.server.controller.workload_commands import _ControllerWorkloadCommands

    wrapped = _ControllerWorkloadCommands._dependency_entrypoint(
        ("flax",),
        "python -m eyvan.worker.runtime.callable_worker --sha256 abc --python 3.13",
    )

    assert wrapped.index("dependency_entry") < wrapped.index("callable_worker")


# --------------------------------------------------------------------------- where the packages land


def test_an_unwritable_host_path_falls_back_to_the_working_directory(tmp_path, monkeypatch, capsys) -> None:
    """A laptop's cell agent cannot write /var/lib, and the job still has to run.

    Failing here would mean `--with` worked only on a root-owned cell, which is
    every fleet except the one somebody tries first.
    """

    monkeypatch.delenv("EYVAN_RUNTIME_ENV_DIR", raising=False)
    monkeypatch.setattr(dependency_entry, "DEFAULT_CACHE_ROOT", Path("/proc/eyvan-cannot-write-here"))
    monkeypatch.chdir(tmp_path)

    assert dependency_entry.cache_root() == tmp_path / dependency_entry.FALLBACK_CACHE_DIR


def test_the_fallback_says_which_later_runs_reuse_the_install(tmp_path, monkeypatch, capsys) -> None:
    """The consequence, not just the fact.

    A silent fallback turns into minutes of reinstall on every attempt with no
    stated cause -- and the earlier wording claimed the install was never reused,
    which is wrong for the ordinary case of running twice from one directory.
    """

    monkeypatch.delenv("EYVAN_RUNTIME_ENV_DIR", raising=False)
    monkeypatch.setattr(dependency_entry, "DEFAULT_CACHE_ROOT", Path("/proc/eyvan-cannot-write-here"))
    monkeypatch.chdir(tmp_path)

    dependency_entry.cache_root()
    reported = capsys.readouterr().err

    assert "is not writable" in reported
    assert dependency_entry.FALLBACK_CACHE_DIR in reported
    assert "same" in reported and "directory" in reported


def test_the_fallback_directory_is_ignored_by_the_repository() -> None:
    """It is written into whatever directory a job was submitted from.

    For anybody submitting out of a checkout -- which is how the examples read
    -- an un-ignored cache dirties `git status` with an installed virtualenv.
    """

    ignored = (Path(__file__).resolve().parents[1] / ".gitignore").read_text(encoding="utf-8")

    assert f"{dependency_entry.FALLBACK_CACHE_DIR}/" in ignored


def test_an_explicit_runtime_env_dir_wins_over_both(tmp_path, monkeypatch) -> None:
    """The declared path is the one a fleet points at a fast shared disk."""

    monkeypatch.setenv("EYVAN_RUNTIME_ENV_DIR", str(tmp_path / "declared"))

    assert dependency_entry.cache_root() == tmp_path / "declared"
