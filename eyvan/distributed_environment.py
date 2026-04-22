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
"""Environment ownership for Eyvan-managed distributed runtimes."""

from __future__ import annotations

import typing as tp

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


def is_distributed_topology_environment_name(name: str) -> bool:
    """Return whether Eyvan, JAX, or GCP owns a topology variable."""

    return name in DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES or name.startswith(("EYVAN_JAX_", "MEGASCALE_"))


def without_distributed_topology_overrides(environment: tp.Mapping[str, str]) -> dict[str, str]:
    """Copy an environment without caller-controlled topology variables."""

    return {name: value for name, value in environment.items() if not is_distributed_topology_environment_name(name)}


def remove_gce_tpu_topology_overrides(environment: tp.MutableMapping[str, str]) -> None:
    """Remove values that must come from GCE TPU metadata discovery."""

    for name in DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES:
        if not name.startswith("EYVAN_") and name != "JAX_COORDINATOR_PORT":
            environment.pop(name, None)


__all__ = (
    "DISTRIBUTED_TOPOLOGY_ENVIRONMENT_NAMES",
    "is_distributed_topology_environment_name",
    "remove_gce_tpu_topology_overrides",
    "without_distributed_topology_overrides",
)
