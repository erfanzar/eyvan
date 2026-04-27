# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Eyvan global fleet control plane."""

from __future__ import annotations

import typing as tp

if tp.TYPE_CHECKING:
    from eyvan.capacity.fleet_config import EyvanFleetConfig
    from eyvan.config import EyvanSettings
    from eyvan.sdk import AsyncEyvanClient, ClientIdentity, EyvanCallableSubmitter, EyvanClient
    from eyvan.sdk.client.jobs import CpuConfig, GpuConfig
    from eyvan.sdk.execution import (
        EyvanJob,
        EyvanJobError,
        EyvanPlacementError,
        EyvanUnavailableError,
        TpuConfig,
        execute,
        init,
    )

__all__ = (
    "AsyncEyvanClient",
    "ClientIdentity",
    "CpuConfig",
    "EyvanCallableSubmitter",
    "EyvanClient",
    "EyvanFleetConfig",
    "EyvanJob",
    "EyvanJobError",
    "EyvanPlacementError",
    "EyvanSettings",
    "EyvanUnavailableError",
    "GpuConfig",
    "TpuConfig",
    "execute",
    "init",
)
from eyvan._version import __version__

_Export = tp.TypeVar("_Export")


def _cache_export(name: str, value: _Export) -> _Export:
    """Cache one explicitly imported lazy package export."""

    globals()[name] = value
    return value


def __getattr__(name: str):
    """Load public SDK exports only when callers request them.

    Native worker subprocesses import ``eyvan.worker.runtime.local_worker`` directly.
    Keeping the package root light prevents that critical path from importing
    the client, API, and their optional observability dependencies first.
    """

    if name == "AsyncEyvanClient":
        from eyvan.sdk import AsyncEyvanClient

        return _cache_export(name, AsyncEyvanClient)
    if name == "ClientIdentity":
        from eyvan.sdk import ClientIdentity

        return _cache_export(name, ClientIdentity)
    if name == "EyvanCallableSubmitter":
        from eyvan.sdk import EyvanCallableSubmitter

        return _cache_export(name, EyvanCallableSubmitter)
    if name == "EyvanClient":
        from eyvan.sdk import EyvanClient

        return _cache_export(name, EyvanClient)
    if name == "EyvanFleetConfig":
        from eyvan.capacity.fleet_config import EyvanFleetConfig

        return _cache_export(name, EyvanFleetConfig)
    if name == "EyvanJob":
        from eyvan.sdk.execution import EyvanJob

        return _cache_export(name, EyvanJob)
    if name == "EyvanJobError":
        from eyvan.sdk.execution import EyvanJobError

        return _cache_export(name, EyvanJobError)
    if name == "EyvanPlacementError":
        from eyvan.sdk.execution import EyvanPlacementError

        return _cache_export(name, EyvanPlacementError)
    if name == "EyvanUnavailableError":
        from eyvan.sdk.execution import EyvanUnavailableError

        return _cache_export(name, EyvanUnavailableError)
    if name == "EyvanSettings":
        from eyvan.config import EyvanSettings

        return _cache_export(name, EyvanSettings)
    if name == "TpuConfig":
        from eyvan.sdk.execution import TpuConfig

        return _cache_export(name, TpuConfig)
    if name == "GpuConfig":
        from eyvan.sdk.client.jobs import GpuConfig

        return _cache_export(name, GpuConfig)
    if name == "CpuConfig":
        from eyvan.sdk.client.jobs import CpuConfig

        return _cache_export(name, CpuConfig)
    if name == "execute":
        from eyvan.sdk.execution import execute

        return _cache_export(name, execute)
    if name == "init":
        from eyvan.sdk.execution import init

        return _cache_export(name, init)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Expose lazily loaded SDK names to interactive tooling."""

    return sorted({*globals(), *__all__})
