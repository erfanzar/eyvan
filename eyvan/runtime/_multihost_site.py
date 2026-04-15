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


def initialize_from_environment() -> None:
    """Initialize JAX from Eyvan's explicit multi-host environment."""

    if os.getenv("EYVAN_JAX_MULTIHOST") != "1":
        return
    detection_method = os.getenv("EYVAN_JAX_CLUSTER_DETECTION", "explicit")
    if detection_method == "gcetpu":
        import jax

        if not jax.distributed.is_initialized():
            jax.distributed.initialize(cluster_detection_method="gcetpu")
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
        )


__all__ = ("initialize_from_environment", "initialize_worker_runtime")
