# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Backend-neutral workload runtime contracts and built-in executors."""

from __future__ import annotations

import typing as tp

if tp.TYPE_CHECKING:
    from eyvan.runtime.callables import CallableArtifact, dump_callable, load_callable
    from eyvan.runtime.container import DockerContainerExecutor
    from eyvan.runtime.local_process import LocalProcessExecutor
    from eyvan.runtime.native_gang import NativeGangExecutor, NativeGangMember
    from eyvan.runtime.native_remote import NativeRemoteExecutor
    from eyvan.runtime.native_slice import NativeSliceExecutor
    from eyvan.runtime.native_worker import create_native_worker_app
    from eyvan.runtime.registry import (
        ConfiguredExecutionResolver,
        ExecutionDriverResolver,
        ExecutionRuntimeRegistry,
        available_execution_drivers,
        create_execution_driver,
    )
    from eyvan.runtime.systemd_transient import SystemdTransientServiceExecutor

__all__ = (
    "CallableArtifact",
    "ConfiguredExecutionResolver",
    "DockerContainerExecutor",
    "ExecutionDriverResolver",
    "ExecutionRuntimeRegistry",
    "LocalProcessExecutor",
    "NativeGangExecutor",
    "NativeGangMember",
    "NativeRemoteExecutor",
    "NativeSliceExecutor",
    "SystemdTransientServiceExecutor",
    "available_execution_drivers",
    "create_execution_driver",
    "create_native_worker_app",
    "dump_callable",
    "load_callable",
)

_Export = tp.TypeVar("_Export")


def _cache_export(name: str, value: _Export) -> _Export:
    """Cache one explicitly imported lazy runtime export."""

    globals()[name] = value
    return value


def __getattr__(name: str):
    """Load an execution surface only when code explicitly requests it."""

    if name in {"CallableArtifact", "dump_callable", "load_callable"}:
        import eyvan.runtime.callables as callables

        if name == "CallableArtifact":
            return _cache_export(name, callables.CallableArtifact)
        if name == "dump_callable":
            return _cache_export(name, callables.dump_callable)
        return _cache_export(name, callables.load_callable)
    if name == "DockerContainerExecutor":
        from eyvan.runtime.container import DockerContainerExecutor

        return _cache_export(name, DockerContainerExecutor)
    if name == "LocalProcessExecutor":
        from eyvan.runtime.local_process import LocalProcessExecutor

        return _cache_export(name, LocalProcessExecutor)
    if name in {"NativeGangExecutor", "NativeGangMember"}:
        import eyvan.runtime.native_gang as native_gang

        if name == "NativeGangExecutor":
            return _cache_export(name, native_gang.NativeGangExecutor)
        return _cache_export(name, native_gang.NativeGangMember)
    if name == "NativeRemoteExecutor":
        from eyvan.runtime.native_remote import NativeRemoteExecutor

        return _cache_export(name, NativeRemoteExecutor)
    if name == "NativeSliceExecutor":
        from eyvan.runtime.native_slice import NativeSliceExecutor

        return _cache_export(name, NativeSliceExecutor)
    if name == "SystemdTransientServiceExecutor":
        from eyvan.runtime.systemd_transient import SystemdTransientServiceExecutor

        return _cache_export(name, SystemdTransientServiceExecutor)
    if name in {
        "ConfiguredExecutionResolver",
        "ExecutionDriverResolver",
        "ExecutionRuntimeRegistry",
        "available_execution_drivers",
        "create_execution_driver",
    }:
        import eyvan.runtime.registry as registry

        if name == "ConfiguredExecutionResolver":
            return _cache_export(name, registry.ConfiguredExecutionResolver)
        if name == "ExecutionDriverResolver":
            return _cache_export(name, registry.ExecutionDriverResolver)
        if name == "ExecutionRuntimeRegistry":
            return _cache_export(name, registry.ExecutionRuntimeRegistry)
        if name == "available_execution_drivers":
            return _cache_export(name, registry.available_execution_drivers)
        return _cache_export(name, registry.create_execution_driver)
    if name == "create_native_worker_app":
        from eyvan.runtime.native_worker import create_native_worker_app

        return _cache_export(name, create_native_worker_app)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Expose lazy exports to interactive tooling."""

    return sorted({*globals(), *__all__})
