# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Registry of capacity backends, mirroring the execution-runtime registry.

Execution backends have always been registered
(:mod:`eyvan.runtime.registry`), while capacity backends were selected by a
hard-coded ``if/elif/else`` in the cell agent with eager driver imports. That
asymmetry meant "add a backend" was a *registration* on one axis and an *edit to
a conditional* on the other, and it is the thing that made a second capacity
substrate awkward to introduce.

Every factory imports its driver lazily inside the function body, for the same
reason the execution registry does: selecting one backend must never import
another's dependencies. That discipline is what keeps a GCP-free deployment from
importing the GCP driver, and it is why the Ray adapter stays out of a native
install.
"""

from __future__ import annotations

import typing as tp

from eyvan.drivers.base import CapacityDriver

if tp.TYPE_CHECKING:
    from eyvan.config import EyvanSettings
    from eyvan.persistence.fleet import FleetStateStore

#: A factory receives the requested backend name so one factory can serve
#: several aliases that differ only by mode (as the GCP driver does).
CapacityBackendFactory = tp.Callable[..., CapacityDriver]


def _normalize(name: str) -> str:
    return name.strip().lower().replace("_", "-")


def _local_factory(name: str, *, state: FleetStateStore, settings: EyvanSettings) -> CapacityDriver:
    del name, settings
    from eyvan.drivers.local import LocalCapacityDriver

    return LocalCapacityDriver(state)


def _manual_factory(name: str, *, state: FleetStateStore, settings: EyvanSettings) -> CapacityDriver:
    del name
    from eyvan.drivers.manual import ManualCapacityDriver

    return ManualCapacityDriver(state, native_worker_port=settings.native_worker_port)


def _kubernetes_factory(name: str, *, state: FleetStateStore, settings: EyvanSettings) -> CapacityDriver:
    """Build the Kubernetes driver for either of its two registered names.

    One factory, two names -- exactly as the three GCP modes share one -- because
    ``kubernetes`` and ``k8s`` are the same backend and a second factory would be
    a second place to keep the defaults in sync.
    """

    del name
    from eyvan.drivers.kubernetes import KubernetesCapacityDriver
    from eyvan.providers.kubernetes.api import KubernetesApi

    return KubernetesCapacityDriver(
        state,
        api=KubernetesApi(),
        namespace=settings.kubernetes_namespace,
        image=settings.kubernetes_worker_image,
        shared_secret=settings.native_worker_root_secret,
    )


def _gcp_factory(name: str, *, state: FleetStateStore, settings: EyvanSettings) -> CapacityDriver:
    from eyvan.drivers.gcp import GcpCapacityDriver
    from eyvan.providers.gcp.native_worker_ingress import GcloudNativeWorkerPublicIngress

    native_worker_public_ingress = (
        GcloudNativeWorkerPublicIngress(network_tag=settings.native_worker_public_ingress_tag)
        if settings.native_worker_public_ingress_tag is not None
        else None
    )
    return GcpCapacityDriver(
        state,
        execution_backend=settings.execution_backend,
        mode=name,
        native_worker_port=settings.native_worker_port,
        native_worker_package=settings.native_worker_package,
        native_worker_wheelhouse=settings.native_worker_wheelhouse,
        native_worker_state_dir=settings.native_worker_state_dir,
        native_worker_shared_secret=settings.native_worker_root_secret,
        native_worker_artifact_root_uri=settings.native_worker_artifact_root_uri,
        native_worker_public_ingress=native_worker_public_ingress,
    )


class CapacityBackendRegistry:
    """Name-to-factory registry for capacity backends."""

    def __init__(self) -> None:
        self._factories: dict[str, CapacityBackendFactory] = {}

    def register(self, name: str, factory: CapacityBackendFactory, *, replace: bool = False) -> None:
        """Register one capacity backend under a normalized name."""

        key = _normalize(name)
        if not key:
            raise ValueError("capacity backend name must be non-empty")
        if not replace and key in self._factories:
            raise ValueError(f"capacity backend {key!r} is already registered")
        self._factories[key] = factory

    def create(self, name: str, *, state: FleetStateStore, settings: EyvanSettings) -> CapacityDriver:
        """Build the driver registered under ``name``."""

        key = _normalize(name)
        factory = self._factories.get(key)
        if factory is None:
            raise ValueError(f"unknown Eyvan capacity backend {name!r}; choose one of: {', '.join(self.names())}")
        return factory(key, state=state, settings=settings)

    def names(self) -> tuple[str, ...]:
        """Return every registered backend name in stable order."""

        return tuple(sorted(self._factories))


DEFAULT_CAPACITY_BACKENDS = CapacityBackendRegistry()
DEFAULT_CAPACITY_BACKENDS.register("local", _local_factory)
DEFAULT_CAPACITY_BACKENDS.register("manual", _manual_factory)
DEFAULT_CAPACITY_BACKENDS.register("gcp", _gcp_factory)
DEFAULT_CAPACITY_BACKENDS.register("gcp-tpu", _gcp_factory)
DEFAULT_CAPACITY_BACKENDS.register("gcp-compute", _gcp_factory)
DEFAULT_CAPACITY_BACKENDS.register("kubernetes", _kubernetes_factory)
DEFAULT_CAPACITY_BACKENDS.register("k8s", _kubernetes_factory)


__all__ = (
    "DEFAULT_CAPACITY_BACKENDS",
    "CapacityBackendFactory",
    "CapacityBackendRegistry",
)
