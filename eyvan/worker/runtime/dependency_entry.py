# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Install a job's declared packages, then exec the job.

The half of a per-job runtime environment that Eyvan did not have. A workload
could already ship its working tree and its environment variables; its
*dependencies* could only be whatever the host image happened to carry, so
adding one package meant rebuilding an image and rolling a fleet -- which is how
"quick experiment" turns into an afternoon. Ray calls this ``runtime_env``, and
it is the reason people reach for Ray on a cluster they otherwise do not need.

Three decisions are load-bearing:

**The environment sees the host's site-packages.** A clean virtualenv on a TPU
host has no ``jax`` and no ``libtpu``, so every accelerator job would install a
second, CPU-only JAX over the top of a working one and fail on the first device
call. The job's packages are layered *over* what the host provides, never
instead of it.

**One environment per distinct requirement set, per host.** Eight ranks on one
host declare the same packages; installing eight times is eight times the work
and a race over the same files. The set is content-addressed, and the marker
file is written only after a successful install, so a crashed attempt leaves a
directory that is rebuilt rather than a half-populated one that is trusted.

**A failed install fails the job.** Running the command anyway produces an
``ImportError`` several minutes later, on an accelerator, attributed to the
user's code rather than to the install that did not happen.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import os
import shutil
import subprocess
import sys
import typing as tp
from pathlib import Path

#: Where environments are cached when the runtime does not say. A host path, so
#: a second attempt or a second rank on the same machine reuses the first one's
#: work; the per-attempt working directory would defeat exactly that.
DEFAULT_CACHE_ROOT = Path("/var/lib/eyvan/runtime-envs")

#: Where environments go when the host path is not writable -- a laptop, or any
#: cell whose agent does not run as root. Named rather than inlined because it
#: appears in a working tree somebody is version-controlling, and the .gitignore
#: that keeps it out of their commits has to spell the same thing.
FALLBACK_CACHE_DIR = ".eyvan-runtime-envs"

#: Written only after an install succeeds. Its absence is what makes a partial
#: environment rebuild instead of being trusted.
COMPLETE_MARKER = ".eyvan-complete"


def environment_key(requirements: tp.Sequence[str]) -> str:
    """Return a stable identity for one set of requirements.

    Sorted and de-duplicated first, because the order somebody typed them in is
    not part of what they asked for -- and two identical requests that hash
    differently mean two environments and two installs.
    """

    unique = "\n".join(sorted({item.strip() for item in requirements if item.strip()}))
    return hashlib.sha256(unique.encode()).hexdigest()[:32]


def cache_root() -> Path:
    """Return a writable directory for cached environments.

    Falls back to the working directory when the host path is not writable,
    which is the ordinary case for a developer running a local cell. The
    fallback is reported rather than silent: it changes *which* later runs reuse
    this install -- only ones starting from the same directory, so a retry that
    materialises a fresh workspace installs again -- and a job whose minutes go
    to a reinstall should say where they went.
    """

    configured = os.environ.get("EYVAN_RUNTIME_ENV_DIR")
    if configured:
        return Path(configured)
    try:
        DEFAULT_CACHE_ROOT.mkdir(parents=True, exist_ok=True)
        probe = DEFAULT_CACHE_ROOT / ".writable"
        probe.touch()
        probe.unlink()
        return DEFAULT_CACHE_ROOT
    except OSError as error:
        print(
            f"eyvan: {DEFAULT_CACHE_ROOT} is not writable ({error}); caching this job's packages "
            f"in ./{FALLBACK_CACHE_DIR} instead, so only a later run from this same "
            "directory reuses them",
            file=sys.stderr,
        )
        return Path.cwd() / FALLBACK_CACHE_DIR


def _install_commands(environment: Path, requirements: tp.Sequence[str]) -> list[list[str]]:
    """Return the commands that build ``environment``, preferring uv."""

    uv = shutil.which("uv")
    if uv:
        return [
            # `--system-site-packages` is not an optimization. Without it the
            # host's accelerator stack -- jax, libtpu, the vendor's drivers --
            # is invisible, and the job installs a second CPU-only copy over a
            # working one.
            [uv, "venv", "--system-site-packages", str(environment)],
            [uv, "pip", "install", "--python", str(environment / "bin" / "python"), *requirements],
        ]
    return [
        [sys.executable, "-m", "venv", "--system-site-packages", str(environment)],
        [str(environment / "bin" / "python"), "-m", "pip", "install", *requirements],
    ]


def prepare(requirements: tp.Sequence[str], *, root: Path | None = None) -> Path:
    """Return an environment holding ``requirements``, building it if needed.

    Raises:
        RuntimeError: If the environment could not be built. The caller must
            fail the job rather than run it: a command that starts without the
            packages it declared fails minutes later, on an accelerator, and
            blames the user's code.
    """

    base = root if root is not None else cache_root()
    base.mkdir(parents=True, exist_ok=True)
    environment = base / environment_key(requirements)
    marker = environment / COMPLETE_MARKER
    if marker.exists():
        return environment

    # Held across the whole build so ranks sharing a host queue rather than
    # write over each other. The marker is re-checked after the lock is taken:
    # by then the rank that was building may have finished.
    lock_path = base / f"{environment.name}.lock"
    with lock_path.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if marker.exists():
            return environment
        if environment.exists():
            # An earlier attempt died mid-install. Its directory is unusable and
            # indistinguishable from a good one except by the missing marker.
            shutil.rmtree(environment, ignore_errors=True)
        for command in _install_commands(environment, requirements):
            completed = subprocess.run(command, capture_output=True, text=True, check=False)
            if completed.returncode != 0:
                shutil.rmtree(environment, ignore_errors=True)
                detail = (completed.stderr or completed.stdout or "").strip()
                raise RuntimeError(f"installing {', '.join(requirements)} failed:\n{detail}")
        marker.write_text("\n".join(requirements), encoding="utf-8")
    return environment


def activate(environment: Path, base: tp.Mapping[str, str] | None = None) -> dict[str, str]:
    """Return the environment variables that put ``environment`` first.

    Prepended rather than replacing ``PATH``: the job still needs the host's
    tooling, and an accelerator host keeps vendor binaries outside any virtual
    environment.
    """

    source = dict(os.environ if base is None else base)
    binaries = str(environment / "bin")
    source["VIRTUAL_ENV"] = str(environment)
    source["PATH"] = f"{binaries}{os.pathsep}{source.get('PATH', '')}".rstrip(os.pathsep)
    return source


def main(argv: tp.Sequence[str] | None = None) -> int:
    """Install the declared packages, then exec the command that follows."""

    parser = argparse.ArgumentParser(prog="eyvan-dependency-entry")
    parser.add_argument("--require", action="append", default=[], metavar="REQUIREMENT")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    arguments = parser.parse_args(list(argv) if argv is not None else None)

    command = list(arguments.command)
    # argparse hands the wrapper its own end-of-options marker back inside the
    # remainder; consume exactly that one. Any later `--` belongs to the
    # workload's command line -- `python tool.py -- --fast` means something --
    # and must be exec'd verbatim.
    if command and command[0] == "--":
        del command[0]
    if not command:
        parser.error("a command to execute is required")
    if not arguments.require:
        parser.error("at least one --require is needed; without one this wrapper does nothing")

    try:
        environment = prepare(arguments.require)
    except RuntimeError as error:
        # Non-zero before the job starts. The alternative is an ImportError
        # minutes later on an accelerator, blamed on the user's code.
        print(f"eyvan: {error}", file=sys.stderr)
        return 1

    os.environ.update(activate(environment))
    # `exec` rather than spawn, for the reason the workspace entry does it: the
    # workload must keep the PID its supervisor started, or signals, exit status
    # and process introspection describe a wrapper instead of the job.
    os.execvp(command[0], command)
    raise AssertionError("execvp returned")  # pragma: no cover - unreachable


if __name__ == "__main__":  # pragma: no cover - module entrypoint
    sys.exit(main())
