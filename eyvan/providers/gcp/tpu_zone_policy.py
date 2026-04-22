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
"""Documented deny-only TPU location policy for queued-resource autoscale."""

from __future__ import annotations

from dataclasses import dataclass

from eyvan.providers.gcp.tpu_types import accelerator_generation


@dataclass(frozen=True, slots=True)
class DocumentedTpuZoneExclusion:
    """One documented TPU generation and zone excluded from queue admission."""

    generation: str
    zone: str
    source_url: str


# Source: https://cloud.google.com/tpu/docs/regions-zones
# Verified 2026-07-14. The TPU locations table lists TPU7x for
# us-central1-c and v6e for us-central1-b, but does not list v6e in
# us-central1-c. Do not add positive availability assertions here; dynamic
# provider discovery remains the primary source for eligible types.
DOCUMENTED_TPU_QUEUE_ADMISSION_EXCLUSIONS = (
    DocumentedTpuZoneExclusion(
        generation="v6e",
        zone="us-central1-c",
        source_url="https://cloud.google.com/tpu/docs/regions-zones",
    ),
)


def documented_tpu_zone_exclusion(*, zone: str, accelerator_type: str) -> DocumentedTpuZoneExclusion | None:
    """Return a documented deny-only exclusion for one TPU type, if any."""

    generation = accelerator_generation(accelerator_type)
    if generation is None:
        return None
    for exclusion in DOCUMENTED_TPU_QUEUE_ADMISSION_EXCLUSIONS:
        if exclusion.zone == zone and exclusion.generation == generation:
            return exclusion
    return None


__all__ = (
    "DOCUMENTED_TPU_QUEUE_ADMISSION_EXCLUSIONS",
    "DocumentedTpuZoneExclusion",
    "documented_tpu_zone_exclusion",
)
