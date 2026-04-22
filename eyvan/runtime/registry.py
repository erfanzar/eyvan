# Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Backend-neutral execution-driver selection."""

from __future__ import annotations

import hashlib
import logging
import os
import typing as tp
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

from eyvan._json import JsonObject, JsonValue, is_json_object
from eyvan._network import format_http_ip_endpoint
from eyvan.drivers.base import (
    CellObservation,
    ExecutionDriver,
    MutationContext,
    UnsupportedCapabilityError,
    canonical_execution_backend,
)

ExecutorFactory = tp.Callable[..., ExecutionDriver]
logger = logging.getLogger(__name__)


def _uses_gce_tpu_cluster_detection(observation: CellObservation) -> bool:
    """Return whether JAX should derive this cell's cluster from GCE metadata.

    True for *any* GCE TPU, not only a multislice. The question that matters is
    "can JAX discover this topology itself", and for a Cloud TPU it always can --
    a single ``v5p-16`` is two hosts and is discovered exactly like a multislice.

    Asking "is this multislice?" instead put single-slice multi-host TPUs on the
    ``EXPLICIT`` path, where Eyvan supplies the coordinator address by reusing
    the worker's *management* endpoint. That endpoint is the address the control
    plane reaches the worker on, which for a public-routed cell is an external
    IP -- so the hosts tried to coordinate over the public internet on port
    12345, which no firewall rule permits. Every rank died in
    ``RegisterTask`` with DEADLINE_EXCEEDED, having never joined, while the
    hosts sat healthy on the same subnet with ``default-allow-internal``
    covering everything they actually needed.

    Two addresses were being conflated: the *management* endpoint (control plane
    to worker, may have to be public) and the *coordination* address (host to
    host, must be internal). GCE metadata gives JAX the second one directly.

    Live on 2026-08-02: every ``pods=1`` inference run failed this way, while the
    ``pods=2`` job succeeded -- because only multislice reached this branch.

    ``EXPLICIT`` remains correct for multi-host backends that are not GCE TPUs,
    where Eyvan genuinely does have to supply the addresses.
    """

    raw_config = observation.metadata.get("tpu_config")
    if raw_config is None:
        return False
    if not is_json_object(raw_config):
        raise ValueError("eyvan-native execution observation has invalid TPU provider configuration")
    # Validated but deliberately not branched on. A malformed record must still
    # be refused, while the *value* no longer selects the detection mode: JAX can
    # discover a Cloud TPU from metadata whether it is one slice or several, and
    # branching on it is precisely what put two-host single slices on the
    # explicit path with an external coordinator address.
    node_count = raw_config.get("node_count", 1)
    if isinstance(node_count, bool) or not isinstance(node_count, int) or node_count < 1:
        raise ValueError("eyvan-native execution observation has an invalid TPU node count")
    return True


@tp.runtime_checkable
class ClosableExecutionDriver(tp.Protocol):
    """Optional cleanup capability implemented by stateful drivers."""

    def close(self) -> None: ...


@tp.runtime_checkable
class HealthCheckedExecutionDriver(tp.Protocol):
    """Optional readiness capability implemented by remote drivers."""

    def health(self) -> bool: ...


@tp.runtime_checkable
class IntrospectableExecutionDriver(tp.Protocol):
    """Optional live-process introspection.

    A protocol rather than a ``getattr(executor, "process_status", None)`` probe:
    this module already establishes ``ClosableExecutionDriver`` and
    ``HealthCheckedExecutionDriver`` as the idiomatic seam, and a third ad-hoc
    probe would make the pattern look optional.
    """

    def process_status(self, remote_id: str) -> JsonObject: ...


@tp.runtime_checkable
class ExecCapableExecutionDriver(tp.Protocol):
    """Optional diagnostic exec beside a running workload.

    The *implementation* gate. ``AgentCapabilities.supports_exec`` is the
    operator-facing *admission* gate, and the two answer different questions:
    "may anyone do this here" versus "can this backend do it at all".
    Disagreement between them is a configuration error the agent reports at
    startup, with a message distinct from "backend cannot".
    """

    def exec_in_workload(self, remote_id: str, command: tp.Sequence[str], *, timeout_s: float) -> JsonObject: ...


@dataclass(frozen=True, slots=True)
class NativeGangCell:
    """One cell selected for a native multi-cell workload group."""

    cell_id: str
    observation: CellObservation
    context: MutationContext

    def __post_init__(self) -> None:
        if not isinstance(self.cell_id, str) or not self.cell_id.strip():
            raise ValueError("native gang cell ID must be non-empty and trimmed")
        if self.observation.cell_id != self.cell_id:
            raise ValueError("native gang observation does not belong to its declared cell")


def _worker_endpoints(
    value: JsonValue,
    *,
    field_name: str,
    require_trimmed: bool,
    allow_empty: bool,
) -> tuple[str, ...]:
    """Validate a provider metadata value as unique native worker endpoints."""

    if not isinstance(value, (list, tuple)) or isinstance(value, (str, bytes)):
        raise ValueError(f"{field_name} has invalid worker endpoints")
    endpoints: list[str] = []
    for endpoint in value:
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise ValueError(f"{field_name} has invalid worker endpoints")
        if require_trimmed and endpoint != endpoint.strip():
            raise ValueError(f"{field_name} has invalid worker endpoints")
        endpoints.append(endpoint.strip())
    if not endpoints:
        if allow_empty:
            return ()
        raise ValueError(f"{field_name} has invalid worker endpoints")
    if len(endpoints) != len(set(endpoints)):
        raise ValueError(f"{field_name} has invalid worker endpoints")
    return tuple(endpoints)


def _normalize_name(name: str) -> str:
    return canonical_execution_backend(name)


def _native_local_factory(
    *,
    endpoint: str | None = None,
    state_dir: str | os.PathLike[str] | None = None,
    token: str | None = None,
) -> ExecutionDriver:
    del token
    if endpoint not in {None, "", "local"}:
        raise ValueError("native-local execution does not accept a remote endpoint")
    from eyvan.runtime.local_process import LocalProcessExecutor

    return LocalProcessExecutor(state_dir=state_dir)


def _eyvan_native_factory(
    *,
    endpoint: str | None = None,
    state_dir: str | os.PathLike[str] | None = None,
    token: str | None = None,
) -> ExecutionDriver:
    del state_dir
    if endpoint is None or not endpoint.strip():
        raise ValueError("eyvan-native execution requires a native worker endpoint")
    if not isinstance(token, str) or not token.strip():
        raise ValueError("eyvan-native execution requires a native worker bearer secret")
    from eyvan.runtime.native_remote import NativeRemoteExecutor

    return NativeRemoteExecutor(endpoint, secret=token)


def _ray_jobs_factory(
    *,
    endpoint: str | None = None,
    state_dir: str | os.PathLike[str] | None = None,
    token: str | None = None,
) -> ExecutionDriver:
    del state_dir, token
    if endpoint is None or not endpoint.strip():
        raise ValueError("ray-jobs execution requires a Ray dashboard endpoint")
    # Ray support is transitional and optional. Importing Eyvan or selecting
    # the native runtime must never import the Ray adapter.
    from eyvan.runtime.ray_jobs import RayJobsExecutor

    try:
        return RayJobsExecutor(endpoint)
    except ModuleNotFoundError as exc:
        if exc.name == "ray" or (exc.name is not None and exc.name.startswith("ray.")):
            raise RuntimeError("ray-jobs execution requires the 'eyvan[ray]' extra") from exc
        raise


class ExecutionRuntimeRegistry:
    """Registry of execution-driver factories keyed by stable backend name."""

    def __init__(self) -> None:
        self._factories: dict[str, ExecutorFactory] = {}
        self._lock = RLock()

    def register(self, name: str, factory: ExecutorFactory, *, replace: bool = False) -> None:
        """Register one execution backend factory."""

        normalized = _normalize_name(name)
        if not normalized:
            raise ValueError("execution backend name must be non-empty")
        with self._lock:
            if normalized in self._factories and not replace:
                raise ValueError(f"execution backend {normalized!r} is already registered")
            self._factories[normalized] = factory

    def create(
        self,
        name: str,
        *,
        endpoint: str | None = None,
        state_dir: str | os.PathLike[str] | None = None,
        token: str | None = None,
    ) -> ExecutionDriver:
        """Construct the selected execution driver without importing others."""

        normalized = _normalize_name(name)
        with self._lock:
            factory = self._factories.get(normalized)
        if factory is None:
            choices = ", ".join(self.names())
            raise ValueError(f"unknown execution backend {name!r}; available backends: {choices}")
        if token is None:
            return factory(endpoint=endpoint, state_dir=state_dir)
        return factory(endpoint=endpoint, state_dir=state_dir, token=token)

    def names(self) -> tuple[str, ...]:
        """Return registered canonical backend names."""

        with self._lock:
            return tuple(sorted(self._factories))


DEFAULT_EXECUTION_RUNTIMES = ExecutionRuntimeRegistry()
DEFAULT_EXECUTION_RUNTIMES.register("native-local", _native_local_factory)
DEFAULT_EXECUTION_RUNTIMES.register("eyvan-native", _eyvan_native_factory)
DEFAULT_EXECUTION_RUNTIMES.register("ray-jobs", _ray_jobs_factory)


class ExecutionDriverResolver:
    """Resolve and cache drivers for one cell-agent process."""

    def __init__(
        self,
        backend: str = "eyvan-native",
        *,
        registry: ExecutionRuntimeRegistry | None = None,
        state_dir: str | os.PathLike[str] | None = None,
        ray_jobs_port: int = 8265,
        native_worker_port: int = 8100,
        native_worker_token: str | None = None,
    ) -> None:
        if ray_jobs_port <= 0 or ray_jobs_port > 65535:
            raise ValueError("Ray Jobs port must be between 1 and 65535")
        if native_worker_port <= 0 or native_worker_port > 65535:
            raise ValueError("native worker port must be between 1 and 65535")
        self.backend = _normalize_name(backend)
        if self.backend == "eyvan-native" and (
            not isinstance(native_worker_token, str) or not native_worker_token.strip()
        ):
            raise ValueError("eyvan-native execution requires EYVAN_NATIVE_WORKER_ROOT_SECRET")
        self.registry = registry or DEFAULT_EXECUTION_RUNTIMES
        if self.backend not in self.registry.names():
            choices = ", ".join(self.registry.names())
            raise ValueError(f"unknown execution backend {backend!r}; available backends: {choices}")
        self.state_dir = Path(state_dir).expanduser() if state_dir is not None else None
        self.ray_jobs_port = ray_jobs_port
        self.native_worker_port = native_worker_port
        self.native_worker_token = native_worker_token
        self._drivers: dict[tuple[str, ...], ExecutionDriver] = {}
        self._lock = RLock()

    @staticmethod
    def _close_driver(driver: ExecutionDriver) -> None:
        if not isinstance(driver, ClosableExecutionDriver):
            return
        try:
            driver.close()
        except Exception:
            logger.warning("failed to close an Eyvan execution driver", exc_info=True)

    def close_cell(self, cell_id: str) -> None:
        """Dispose every cached driver for one deleted execution cell."""

        with self._lock:
            stale_keys = [key for key in self._drivers if key[1] == cell_id]
            stale_drivers = [self._drivers.pop(key) for key in stale_keys]
        for driver in stale_drivers:
            self._close_driver(driver)

    def close(self) -> None:
        """Dispose all cached execution drivers owned by this agent."""

        with self._lock:
            drivers, self._drivers = list(self._drivers.values()), {}
        for driver in drivers:
            self._close_driver(driver)

    def resolve(self, cell_id: str, observation: CellObservation) -> ExecutionDriver:
        """Return the configured driver for one observed execution cell."""

        if not cell_id or cell_id != cell_id.strip():
            raise ValueError("cell ID must be non-empty and trimmed")
        if observation.cell_id != cell_id:
            raise ValueError(
                f"execution observation belongs to cell {observation.cell_id!r}, not requested cell {cell_id!r}"
            )
        endpoint = observation.execution_endpoint
        if self.backend == "ray-jobs" and not endpoint:
            head_ip = observation.metadata.get("head_ip")
            if isinstance(head_ip, str) and head_ip.strip():
                try:
                    endpoint = format_http_ip_endpoint(head_ip, self.ray_jobs_port)
                except ValueError as exc:
                    raise ValueError("ray-jobs execution observation has an invalid head IP address") from exc
        elif self.backend == "eyvan-native" and not endpoint:
            head_ip = observation.metadata.get("head_ip")
            if isinstance(head_ip, str) and head_ip.strip():
                try:
                    endpoint = format_http_ip_endpoint(head_ip, self.native_worker_port)
                except ValueError as exc:
                    raise ValueError("eyvan-native execution observation has an invalid head IP address") from exc
        token = None
        native_endpoints: tuple[str, ...] = ()
        if self.backend == "eyvan-native":
            token = self._native_worker_token(observation)
            raw_endpoints = observation.metadata.get("native_worker_endpoints")
            if raw_endpoints is not None:
                native_endpoints = _worker_endpoints(
                    raw_endpoints,
                    field_name="eyvan-native execution observation",
                    require_trimmed=False,
                    allow_empty=True,
                )
        token_fingerprint = hashlib.sha256(token.encode()).hexdigest()[:24] if token is not None else ""
        gce_tpu_cluster = self.backend == "eyvan-native" and _uses_gce_tpu_cluster_detection(observation)
        endpoint_fingerprint = "\x1f".join(native_endpoints) if len(native_endpoints) > 1 else str(endpoint or "")
        endpoint_fingerprint = f"{endpoint_fingerprint}\x1e{int(gce_tpu_cluster)}"
        key = (self.backend, cell_id, endpoint_fingerprint, token_fingerprint)
        with self._lock:
            driver = self._drivers.get(key)
            if driver is None:
                state_dir = self.state_dir
                if state_dir is not None and self.backend == "native-local":
                    state_dir = state_dir / "executions" / hashlib.sha256(cell_id.encode()).hexdigest()[:24]
                if self.backend == "eyvan-native" and len(native_endpoints) > 1:
                    from eyvan.runtime.native_slice import JaxClusterDetectionMode, NativeSliceExecutor

                    assert token is not None
                    driver = NativeSliceExecutor(
                        native_endpoints,
                        secret=token,
                        jax_cluster_detection=(
                            JaxClusterDetectionMode.GCE_TPU if gce_tpu_cluster else JaxClusterDetectionMode.EXPLICIT
                        ),
                    )
                else:
                    driver = self.registry.create(self.backend, endpoint=endpoint, state_dir=state_dir, token=token)
                self._drivers[key] = driver
            stale_keys = [driver_key for driver_key in self._drivers if driver_key[1] == cell_id and driver_key != key]
            stale_drivers = [self._drivers.pop(driver_key) for driver_key in stale_keys]
        for stale_driver in stale_drivers:
            self._close_driver(stale_driver)
        return driver

    def resolve_gang(self, cells: tp.Sequence[NativeGangCell]):
        """Resolve one native executor spanning independently fenced execution cells."""

        if self.backend != "eyvan-native":
            raise UnsupportedCapabilityError(
                f"execution backend {self.backend!r} does not support native multi-cell gangs"
            )
        ordered = tuple(sorted(cells, key=lambda cell: cell.cell_id))
        if len(ordered) < 2:
            raise ValueError("native gang execution requires at least two cells")
        if len({cell.cell_id for cell in ordered}) != len(ordered):
            raise ValueError("native gang execution cell IDs must be unique")

        from eyvan.runtime.native_gang import NativeGangExecutor, NativeGangMember

        members = []
        for cell in ordered:
            raw_endpoints = cell.observation.metadata.get("native_worker_endpoints")
            if raw_endpoints is None:
                endpoint = cell.observation.execution_endpoint
                if not isinstance(endpoint, str) or not endpoint.strip():
                    raise ValueError(f"native gang cell {cell.cell_id!r} has no execution endpoint")
                endpoints = (endpoint.strip(),)
            else:
                endpoints = _worker_endpoints(
                    raw_endpoints,
                    field_name=f"native gang cell {cell.cell_id!r}",
                    require_trimmed=True,
                    allow_empty=False,
                )
            members.append(
                NativeGangMember(
                    cell_id=cell.cell_id,
                    endpoints=endpoints,
                    secret=self._native_worker_token(cell.observation),
                    context=cell.context,
                )
            )
        return NativeGangExecutor(members)

    def _native_worker_token(self, observation: CellObservation) -> str:
        """Derive the per-cell worker token from the agent's shared secret."""

        if self.native_worker_token is None:
            raise ValueError("eyvan-native execution requires EYVAN_NATIVE_WORKER_ROOT_SECRET")
        management_uid = observation.metadata.get("management_uid")
        if not isinstance(management_uid, str) or not management_uid.strip():
            raise ValueError("eyvan-native execution observation is missing its management UID")
        provider_generation = observation.metadata.get("provider_generation", 1)
        if isinstance(provider_generation, bool) or not isinstance(provider_generation, int) or provider_generation < 1:
            raise ValueError("eyvan-native execution observation has an invalid provider generation")
        from eyvan.runtime.native_bootstrap import derive_native_worker_token

        token = derive_native_worker_token(
            self.native_worker_token,
            management_uid,
            provider_generation=provider_generation,
        )
        if token is None:
            raise ValueError("eyvan-native execution could not derive a native worker token")
        return token

    def readiness(self, cell_id: str, observation: CellObservation) -> tuple[bool, str]:
        """Probe the selected runtime before provider capacity becomes schedulable."""

        try:
            driver = self.resolve(cell_id, observation)
            if not isinstance(driver, HealthCheckedExecutionDriver):
                return True, "execution runtime is ready"
            if driver.health():
                return True, "execution runtime is ready"
            return False, f"{self.backend} execution runtime is not ready"
        except Exception as exc:
            return False, f"{self.backend} execution runtime is not ready: {exc}"


ConfiguredExecutionResolver = ExecutionDriverResolver


def create_execution_driver(
    name: str,
    *,
    endpoint: str | None = None,
    state_dir: str | os.PathLike[str] | None = None,
    token: str | None = None,
) -> ExecutionDriver:
    """Construct one driver using the built-in runtime registry."""

    return DEFAULT_EXECUTION_RUNTIMES.create(name, endpoint=endpoint, state_dir=state_dir, token=token)


def available_execution_drivers() -> tuple[str, ...]:
    """Return canonical built-in execution backend names."""

    return DEFAULT_EXECUTION_RUNTIMES.names()


__all__ = (
    "DEFAULT_EXECUTION_RUNTIMES",
    "ConfiguredExecutionResolver",
    "ExecutionDriverResolver",
    "ExecutionRuntimeRegistry",
    "NativeGangCell",
    "available_execution_drivers",
    "create_execution_driver",
)
