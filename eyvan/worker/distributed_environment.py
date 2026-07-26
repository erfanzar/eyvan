# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Environment ownership for Eyvan-managed distributed runtimes.

Two families of topology variable live here and they are deliberately kept
apart.

:data:`DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES` is the TPU/JAX family. It is
reserved at *submission* time -- ``records.WorkloadDefinition`` refuses a
workload that names any of them -- as well as stripped at dispatch.

:data:`GPU_TOPOLOGY_ENVIRONMENT_NAMES` is the GPU family, and it is only
stripped at dispatch, on the path that then replaces it. The difference is not
an oversight. Reserving these names at submission would start refusing workloads
that a running control plane accepts today -- ``CUDA_VISIBLE_DEVICES`` in a
submitted environment is ordinary and harmless on every non-GPU-coordinated
path -- and a rolling upgrade that rejects a previously valid submission is a
worse failure than the one being prevented. What matters is that a *coordinated*
rank never inherits one, and that is a property of the dispatch path, which owns
the value it writes. A workload that is not GPU-coordinated sees exactly what it
sees today.

**Nothing here is hardware-qualified.** No GPU has executed an Eyvan workload.
The variables below are the ones a GPU runtime reads; that they are computed and
delivered correctly is host-testable, and is all that is claimed.
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass

DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES = frozenset(
    {
        "EYVAN_DISTRIBUTED_PROCESS_COUNT",
        "EYVAN_DISTRIBUTED_PROCESS_ID",
        "EYVAN_JAX_CLUSTER_DETECTION",
        "EYVAN_JAX_MULTIHOST",
        "EYVAN_REPLICA_COUNT",
        "EYVAN_REPLICA_INDEX",
        "GCE_METADATA_IP",
        "JAX_COORDINATOR_ADDRESS",
        "JAX_COORDINATOR_BIND_ADDRESS",
        "JAX_COORDINATOR_PORT",
        "JAX_LOCAL_DEVICE_IDS",
        "JAX_PARTITION_INDEX",
        "JAX_PROCESS_COUNT",
        "JAX_PROCESS_ID",
        "JAX_PROCESS_INDEX",
        "JAX_SLICE_INDEX",
        "MEGASCALE_COORDINATOR_ADDRESS",
        "MEGASCALE_NUM_SLICES",
        "MEGASCALE_SLICE_ID",
        "TPU_PROCESS_ADDRESSES",
        "TPU_SKIP_MDS_QUERY",
        "TPU_WORKER_HOSTNAMES",
        "TPU_WORKER_ID",
    }
)

#: Device-visibility and rendezvous variables a GPU runtime reads. A rank that
#: inherited any of these from a submission would compute on a device another
#: rank owns, or join a rendezvous the control plane never created -- so the
#: GPU-coordinated dispatch path removes every one of them and then writes back
#: only those it can state without guessing (see :func:`gpu_rank_environment`).
#:
#: Deliberately *not* every ``NCCL_`` name: ``NCCL_DEBUG``, ``NCCL_SOCKET_IFNAME``
#: and the transport tunables are an operator's to set and say nothing about
#: which rank a process is. Only ``NCCL_COMM_ID``, which names a rendezvous, is
#: taken.
GPU_TOPOLOGY_ENVIRONMENT_NAMES = frozenset(
    {
        "CUDA_DEVICE_ORDER",
        "CUDA_VISIBLE_DEVICES",
        "GPU_DEVICE_ORDINAL",
        "GROUP_RANK",
        "HIP_VISIBLE_DEVICES",
        "LOCAL_RANK",
        "LOCAL_WORLD_SIZE",
        "MASTER_ADDR",
        "MASTER_PORT",
        "NCCL_COMM_ID",
        "NODE_RANK",
        "NVIDIA_VISIBLE_DEVICES",
        "RANK",
        "ROCR_VISIBLE_DEVICES",
        "WORLD_SIZE",
    }
)

# These host identity and launch-path values are assigned by the native runtime.
# The complete ``EYVAN_`` namespace is reserved below as well: controller
# dispatch, callable/workspace transfer, checkpoint/profile collection, Docker,
# and future runtime features must be able to inject their own variables without
# a secret mount being silently overwritten after its value is materialized.
RUNTIME_OWNED_ENVIRONMENT_NAMES = frozenset(
    {
        "HOME",
        "LOGNAME",
        "PATH",
        "USER",
        "VIRTUAL_ENV",
        "XDG_CACHE_HOME",
    }
    | DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES
)


def is_distributed_topology_environment_name(name: str) -> bool:
    """Return whether Eyvan, JAX, or GCP owns a topology variable."""

    return name in DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES or name.startswith(("EYVAN_JAX_", "MEGASCALE_"))


def is_runtime_owned_environment_name(name: str) -> bool:
    """Return whether a secret mount would collide with runtime-owned state.

    Eyvan owns its entire environment namespace rather than an enumerated
    snapshot. This keeps a newly injected checkpoint, callable, source, or
    container variable from becoming an overwrite of an already-materialized
    secret in older submission code.
    """

    return (
        name in RUNTIME_OWNED_ENVIRONMENT_NAMES
        or name.startswith("EYVAN_")
        or is_distributed_topology_environment_name(name)
    )


def without_distributed_topology_overrides(environment: tp.Mapping[str, str]) -> dict[str, str]:
    """Copy an environment without caller-controlled topology variables."""

    return {name: value for name, value in environment.items() if not is_distributed_topology_environment_name(name)}


def is_gpu_topology_environment_name(name: str) -> bool:
    """Return whether Eyvan owns a GPU device-visibility or rendezvous variable."""

    return name in GPU_TOPOLOGY_ENVIRONMENT_NAMES


def without_gpu_topology_overrides(environment: tp.Mapping[str, str]) -> dict[str, str]:
    """Copy an environment without caller-controlled GPU topology variables.

    Applied only where the caller is about to be given Eyvan's own values. A
    submission that carried ``CUDA_VISIBLE_DEVICES=0`` on every rank would put
    every rank of a gang on device zero of its host and leave the rest idle;
    stripping first is what makes the assignment below authoritative rather than
    a suggestion the submission can override.
    """

    return {name: value for name, value in environment.items() if not is_gpu_topology_environment_name(name)}


#: Rendezvous port for GPU collectives. Deliberately not the JAX coordinator
#: port: a workload is free to initialize JAX *and* a torch process group, and
#: two services asking for one port is a failure that would look like a network
#: fault. Eyvan owns both numbers, so it can simply not collide with itself.
DEFAULT_GPU_RENDEZVOUS_PORT = 12346


@dataclass(frozen=True, slots=True)
class GpuDeviceAssignment:
    """The devices one rank of a GPU gang may use, host-local and gang-wide.

    ``host_devices`` are the indices the rank's process sees, which is what
    ``CUDA_VISIBLE_DEVICES`` takes. ``global_devices`` are the same devices'
    positions in the gang-wide device order, which is what makes "exact and
    non-overlapping" checkable: two ranks on different hosts both see local
    index ``0``, and only the global ordinals can say whether that is one device
    or two.

    Attributes:
        rank: The gang rank this assignment belongs to.
        host_devices: Host-local device indices, ascending from zero.
        global_devices: Gang-wide device ordinals, ascending and contiguous.
    """

    rank: int
    host_devices: tuple[int, ...]
    global_devices: tuple[int, ...]

    def __post_init__(self) -> None:
        if self.rank < 0:
            raise ValueError("a GPU device assignment rank cannot be negative")
        if not self.host_devices or len(self.host_devices) != len(self.global_devices):
            raise ValueError("a GPU device assignment must map every host device to one global device")

    @property
    def device_count(self) -> int:
        """Return how many devices this rank owns."""

        return len(self.host_devices)

    @property
    def global_device_offset(self) -> int:
        """Return this rank's first gang-wide device ordinal."""

        return self.global_devices[0]


@dataclass(frozen=True, slots=True)
class GpuDevicePlan:
    """Every rank's device assignment for one GPU gang.

    Built by :func:`plan_gpu_devices` and never by hand, so the non-overlap
    property is established once rather than re-argued at each call site.
    """

    assignments: tuple[GpuDeviceAssignment, ...]

    @property
    def host_count(self) -> int:
        """Return how many hosts this plan covers."""

        return len(self.assignments)

    @property
    def device_count(self) -> int:
        """Return how many devices this plan covers in total."""

        return sum(assignment.device_count for assignment in self.assignments)

    def rank_environment(
        self,
        rank: int,
        *,
        rendezvous_host: str,
        rendezvous_port: int = DEFAULT_GPU_RENDEZVOUS_PORT,
        gpu_model: str | None = None,
    ) -> dict[str, str]:
        """Return the GPU coordination environment Eyvan owns for one rank.

        What is written back is only what Eyvan can state without guessing the
        workload's launcher. Device visibility, the device counts, the host
        index and the rendezvous endpoint all follow from the placement Eyvan
        made. ``RANK`` and ``LOCAL_RANK`` do not: they identify a *process*
        within a host-local launcher that Eyvan does not run, so they are
        stripped and left unset rather than filled with the host's index, which
        would be wrong for every process but the first.

        Args:
            rank: The gang rank to render.
            rendezvous_host: Host part of the collective rendezvous endpoint.
            rendezvous_port: Port part of that endpoint.
            gpu_model: The attached accelerator, when the catalog records one.

        Returns:
            A mapping of environment variable names to values.

        Raises:
            IndexError: If ``rank`` is not in this plan.
            ValueError: If the rendezvous host is empty.
        """

        if not rendezvous_host or rendezvous_host != rendezvous_host.strip():
            raise ValueError("a GPU rendezvous host must be non-empty and trimmed")
        assignment = self.assignments[rank]
        visible = ",".join(str(index) for index in assignment.host_devices)
        environment = {
            # PCI bus order rather than CUDA's default "fastest first", so the
            # index a rank is given means the same device on every host of a
            # heterogeneous gang and matches what nvidia-smi reports.
            "CUDA_DEVICE_ORDER": "PCI_BUS_ID",
            "CUDA_VISIBLE_DEVICES": visible,
            "NVIDIA_VISIBLE_DEVICES": visible,
            "LOCAL_WORLD_SIZE": str(assignment.device_count),
            "NODE_RANK": str(assignment.rank),
            "WORLD_SIZE": str(self.device_count),
            "MASTER_ADDR": rendezvous_host,
            "MASTER_PORT": str(rendezvous_port),
            "EYVAN_GPU_HOST_COUNT": str(self.host_count),
            "EYVAN_GPU_HOST_INDEX": str(assignment.rank),
            "EYVAN_GPU_LOCAL_DEVICE_COUNT": str(assignment.device_count),
            "EYVAN_GPU_GLOBAL_DEVICE_COUNT": str(self.device_count),
            "EYVAN_GPU_GLOBAL_DEVICE_OFFSET": str(assignment.global_device_offset),
        }
        if gpu_model is not None:
            environment["EYVAN_GPU_MODEL"] = gpu_model
        return environment


def plan_gpu_devices(devices_per_rank: tp.Sequence[int]) -> GpuDevicePlan:
    """Assign every gang rank an exact, non-overlapping set of GPU devices.

    One rank per host, which is the shape Eyvan launches: the native worker runs
    one process per endpoint, and a host-local launcher inside it fans out to
    the devices this function made visible. Hosts may hold different numbers of
    devices -- a single-GPU host and an eight-GPU host in one gang is the
    heterogeneous case -- so the gang-wide ordinals are accumulated rather than
    assumed uniform.

    Args:
        devices_per_rank: Device count of each rank's host, in rank order.

    Returns:
        The plan covering every rank.

    Raises:
        ValueError: If no rank was given, or any host's device count is not a
            positive integer. A host whose device count is unknown is refused
            rather than treated as zero: an unobservable device count is not
            evidence of an empty host, and a zero would silently hand the rank
            an empty ``CUDA_VISIBLE_DEVICES``, which means "no GPU at all".
    """

    counts = tuple(devices_per_rank)
    if not counts:
        raise ValueError("a GPU device plan requires at least one rank")
    for rank, count in enumerate(counts):
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise ValueError(f"rank {rank} must declare a positive integer GPU device count, not {count!r}")
    assignments: list[GpuDeviceAssignment] = []
    offset = 0
    for rank, count in enumerate(counts):
        assignments.append(
            GpuDeviceAssignment(
                rank=rank,
                host_devices=tuple(range(count)),
                global_devices=tuple(range(offset, offset + count)),
            )
        )
        offset += count
    return GpuDevicePlan(assignments=tuple(assignments))


def remove_gce_tpu_topology_overrides(environment: tp.MutableMapping[str, str]) -> None:
    """Remove values that must come from GCE TPU metadata discovery."""

    for name in DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES:
        if not name.startswith("EYVAN_") and name != "JAX_COORDINATOR_PORT":
            environment.pop(name, None)


__all__ = (
    "DEFAULT_GPU_RENDEZVOUS_PORT",
    "DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES",
    "GPU_TOPOLOGY_ENVIRONMENT_NAMES",
    "RUNTIME_OWNED_ENVIRONMENT_NAMES",
    "GpuDeviceAssignment",
    "GpuDevicePlan",
    "is_distributed_topology_environment_name",
    "is_gpu_topology_environment_name",
    "is_runtime_owned_environment_name",
    "plan_gpu_devices",
    "remove_gce_tpu_topology_overrides",
    "without_distributed_topology_overrides",
    "without_gpu_topology_overrides",
)
