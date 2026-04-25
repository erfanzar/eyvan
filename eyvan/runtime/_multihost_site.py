# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Standalone JAX distributed initialization for native worker Python paths.

This module is *staged* onto TPU hosts by ``native_bootstrap`` as
``eyvan_multihost.py``, next to ``eyvan_distributed_environment.py``, and driven
from a generated ``sitecustomize.py``. It is therefore imported under two
different names -- as part of the ``eyvan`` package here, and as a bare module
there -- which is why the one cross-module import below is written to work either
way rather than assuming the package layout.
"""

from __future__ import annotations

import os
import typing as tp


def _scrub_gce_topology_overrides(environment: tp.MutableMapping[str, str]) -> None:
    """Drop topology values that must come from GCE TPU metadata discovery.

    The import is tried under the staged module name first because that is the
    context that matters: on a TPU host there is no ``eyvan`` package on the
    site path, only the two files the bootstrap installed.
    """

    try:
        from eyvan_distributed_environment import (  # type: ignore[import-not-found]
            remove_gce_tpu_topology_overrides,
        )
    except ImportError:
        from eyvan.distributed_environment import remove_gce_tpu_topology_overrides
    remove_gce_tpu_topology_overrides(environment)


#: JAX declares a task dead if it misses this many seconds of heartbeats, and
#: its own default is 100. That is far too short for the work that happens
#: *after* initialization on a real ML host: this module runs from
#: ``sitecustomize``, so JAX is initialized at interpreter startup and the user's
#: code then downloads weights, converts them, and compiles -- none of which
#: heartbeats. On 2026-08-01 an eSurge inference job on a two-host v5p-16 died
#: with "DEADLINE_EXCEEDED: the JAX distributed service detected fatal errors"
#: while one host was still loading a 0.6B model. Both ranks were killed, and
#: the surviving message named neither the cause nor the host.
#:
#: Eyvan makes this call on the workload's behalf, so the default is Eyvan's to
#: choose. Fifteen minutes accommodates a cold model pull and a first compile;
#: a genuinely hung host is still detected, just not a busy one.
DEFAULT_HEARTBEAT_TIMEOUT_S = 900

#: Skew between hosts *reaching* initialization: image pull, container start,
#: interpreter boot. JAX defaults to 300.
DEFAULT_INITIALIZATION_TIMEOUT_S = 600


def _timeout(name: str, default: int) -> int:
    """Return a positive integer timeout from the environment, or the default."""

    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer number of seconds") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be positive")
    return value


def initialize_worker_runtime(environment: tp.MutableMapping[str, str] | None = None) -> None:
    """Prepare the environment and initialize JAX, in that order.

    The order is the point and it used to live in a generated shell heredoc,
    where it could only ever be asserted as a *string*. Under ``gcetpu``
    detection, JAX reads topology from GCE metadata, so any inherited override
    must be removed *before* initialization -- afterwards it has already been
    read. Here it is one Python function a test can actually run.
    """

    target = os.environ if environment is None else environment
    if target.get("EYVAN_JAX_CLUSTER_DETECTION") == "gcetpu":
        _scrub_gce_topology_overrides(target)
    initialize_from_environment()


def _consume_multihost_marker() -> None:
    """Stop this process's children from re-joining the JAX cluster.

    ``sitecustomize`` runs in *every* Python interpreter that inherits
    ``PYTHONPATH``, and the workload's environment carries both that path and
    ``EYVAN_JAX_MULTIHOST=1``. So any Python subprocess the workload spawns --
    and ``gcloud`` is itself a Python program -- would load this module, see the
    marker, and call ``jax.distributed.initialize()`` carrying its *parent's*
    ``JAX_PROCESS_ID``. It joins the coordination service as a duplicate task,
    finishes its work, and exits. The service sees a task die and tears the
    whole cluster down: every rank reports "another task died" and no host has
    died at all.

    Ray never hits this because it injects no ``sitecustomize``; user code calls
    ``initialize()`` once, itself, at a time it chooses.

    Removing the marker from ``os.environ`` after initializing is what breaks the
    inheritance: this process is already joined, and every interpreter it starts
    from here on sees no marker and returns immediately. The ``PYTHONPATH`` entry
    is left alone deliberately -- staged sibling modules are still importable,
    and without the marker this module is a no-op anyway.

    Also removes the earlier symptom: a child with no JAX installed printed
    "Error in sitecustomize ... ModuleNotFoundError: No module named 'jax'",
    because the import is reached only past this guard.
    """

    os.environ.pop("EYVAN_JAX_MULTIHOST", None)


def initialize_from_environment() -> None:
    """Initialize JAX from Eyvan's explicit multi-host environment."""

    if os.getenv("EYVAN_JAX_MULTIHOST") != "1":
        return
    _consume_multihost_marker()
    detection_method = os.getenv("EYVAN_JAX_CLUSTER_DETECTION", "explicit")
    if detection_method == "gcetpu":
        import jax

        if not jax.distributed.is_initialized():
            jax.distributed.initialize(
                cluster_detection_method="gcetpu",
                initialization_timeout=_timeout("EYVAN_JAX_INIT_TIMEOUT_S", DEFAULT_INITIALIZATION_TIMEOUT_S),
                heartbeat_timeout_seconds=_timeout("EYVAN_JAX_HEARTBEAT_TIMEOUT_S", DEFAULT_HEARTBEAT_TIMEOUT_S),
            )
        return
    if detection_method != "explicit":
        raise RuntimeError(f"Eyvan does not support JAX cluster detection method {detection_method!r}")
    coordinator_address = os.getenv("JAX_COORDINATOR_ADDRESS")
    process_count = os.getenv("JAX_PROCESS_COUNT")
    process_id = os.getenv("JAX_PROCESS_ID")
    if not coordinator_address or process_count is None or process_id is None:
        raise RuntimeError("Eyvan multi-host JAX workload is missing distributed process configuration")
    try:
        num_processes = int(process_count)
        rank = int(process_id)
    except ValueError as exc:
        raise RuntimeError("Eyvan multi-host JAX process configuration must be integers") from exc
    if num_processes < 2 or rank < 0 or rank >= num_processes:
        raise RuntimeError("Eyvan multi-host JAX process configuration is invalid")
    import jax

    if not jax.distributed.is_initialized():
        jax.distributed.initialize(
            coordinator_address=coordinator_address,
            num_processes=num_processes,
            process_id=rank,
            cluster_detection_method="deactivate",
            initialization_timeout=_timeout("EYVAN_JAX_INIT_TIMEOUT_S", DEFAULT_INITIALIZATION_TIMEOUT_S),
            heartbeat_timeout_seconds=_timeout("EYVAN_JAX_HEARTBEAT_TIMEOUT_S", DEFAULT_HEARTBEAT_TIMEOUT_S),
        )


__all__ = (
    "DEFAULT_HEARTBEAT_TIMEOUT_S",
    "DEFAULT_INITIALIZATION_TIMEOUT_S",
    "initialize_from_environment",
    "initialize_worker_runtime",
)
