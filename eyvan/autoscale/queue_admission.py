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
"""Shared local recording for provider-proven GCP TPU queue denials."""

from __future__ import annotations

from eyvan.autoscale.cache import AutoscaleConfigCache
from eyvan.drivers.base import CapacityProviderFailureCode
from eyvan.service.models import (
    AutoscaleBlockReason,
    CapacityClass,
    CellRecord,
    GcpQueueAdmissionDenialTarget,
    WorkloadRecord,
)


class QueueAdmissionDenialRecorder:
    """Persist exact Spot denials without changing workload or CLI outcomes."""

    def __init__(self, cache: AutoscaleConfigCache) -> None:
        self._cache = cache

    def record_workload(self, workload: WorkloadRecord) -> None:
        """Record the immutable target carried by a terminal denied workload."""

        if workload.autoscale_block_reason is not AutoscaleBlockReason.QUEUE_ADMISSION_DENIED:
            return
        self._record_target(workload.queue_admission_denial)

    def record_cell(self, cell: CellRecord) -> None:
        """Backfill an exact denial from a cell reported by fleet status."""

        if cell.provider_failure_code is not CapacityProviderFailureCode.QUEUE_ADMISSION_DENIED:
            return
        if cell.provider != "gcp":
            return
        project = cell.provider_project
        zone = cell.zone
        accelerator_type = cell.accelerator_type
        if project is None or zone is None or accelerator_type is None:
            return
        self._record_target(
            GcpQueueAdmissionDenialTarget(
                provider_project=project,
                zone=zone,
                accelerator_type=accelerator_type,
                capacity_class=cell.capacity_class,
            )
        )

    def _record_target(self, target: GcpQueueAdmissionDenialTarget | None) -> None:
        """Write one cache-compatible Spot target while treating the cache as optional."""

        if target is None or target.capacity_class is not CapacityClass.SPOT:
            return
        try:
            self._cache.record_queue_admission_denial(
                project=target.provider_project,
                zone=target.zone,
                accelerator_type=target.accelerator_type,
                capacity_class=target.capacity_class,
            )
        except (OSError, ValueError):
            # The provider's durable denial must remain visible when a local cache is unavailable or malformed.
            return


__all__ = ("QueueAdmissionDenialRecorder",)
