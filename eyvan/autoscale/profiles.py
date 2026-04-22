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
"""GCP TPU worker-profile policy used by Eyvan autoscale generation."""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass

from eyvan.autoscale.models import AutoscaleSkipReason, TpuProfile
from eyvan.providers.gcp.tpu_types import parse_tpu_accelerator_type

_NATIVE_TPU_GENERATIONS = frozenset({"v4", "v5e", "v5litepod", "v5p", "v6e"})
_TPU_QUOTA_UNITS_PER_RESOURCE = {"v4": 2, "v5p": 2}
# This type is deliberately compiled by the fleet schema as an integrated
# Compute Engine TPU VM. The dynamic Cloud TPU quota catalog must not emit it
# as a queued-resource autoscale worker.
_DYNAMIC_QUEUED_ACCELERATOR_EXCLUSIONS = frozenset({"v5p-8"})


@dataclass(frozen=True, slots=True)
class TpuProfileResolution:
    """The canonical queued-resource policy decision for one GCP type."""

    profile: TpuProfile | None
    skip_reason: AutoscaleSkipReason | None

    def __post_init__(self) -> None:
        if (self.profile is None) == (self.skip_reason is None):
            raise ValueError("a TPU profile resolution requires exactly one outcome")


class QueuedTpuProfileCatalog:
    """Derive Eyvan worker profiles from GCP's live accelerator catalog.

    The catalog deliberately has no accelerator-size ceiling. GCP remains the
    authority for the types available in a zone; this policy only excludes
    backend-incompatible types and translates valid types into Eyvan's typed
    worker resource vector.
    """

    def resolve(self, accelerator_type: str) -> TpuProfileResolution:
        """Classify one discovered GCP accelerator type for queued-resource use."""

        if not isinstance(accelerator_type, str):
            return TpuProfileResolution(None, AutoscaleSkipReason.UNSUPPORTED_ACCELERATOR_TYPE)
        normalized = accelerator_type.strip().lower()
        if normalized in _DYNAMIC_QUEUED_ACCELERATOR_EXCLUSIONS:
            return TpuProfileResolution(None, AutoscaleSkipReason.COMPUTE_BACKED_TPU_VM)
        parsed = parse_tpu_accelerator_type(normalized)
        if parsed is None or parsed.generation not in _NATIVE_TPU_GENERATIONS:
            return TpuProfileResolution(None, AutoscaleSkipReason.UNSUPPORTED_ACCELERATOR_TYPE)
        divisor = _TPU_QUOTA_UNITS_PER_RESOURCE.get(parsed.generation, 1)
        if parsed.size % divisor:
            return TpuProfileResolution(None, AutoscaleSkipReason.UNSUPPORTED_ACCELERATOR_TYPE)
        tpu = parsed.size // divisor
        if tpu <= 0:
            return TpuProfileResolution(None, AutoscaleSkipReason.UNSUPPORTED_ACCELERATOR_TYPE)
        return TpuProfileResolution(TpuProfile(accelerator_type=normalized, cpu=32, tpu=tpu), None)


QUEUED_TPU_PROFILE_CATALOG = QueuedTpuProfileCatalog()


def tpu_profile_from_accelerator_type(accelerator_type: str) -> TpuProfile | None:
    """Build Eyvan's typed resource vector for one discovered TPU slice type.

    Cloud TPU quota units use the public accelerator suffix. v4 and v5p use
    two quota units per schedulable TPU resource while v5e, LitePod, and v6e
    use one. Every native TPU worker reserves the same conservative CPU vector
    until host-specific discovery is available.
    """

    return QUEUED_TPU_PROFILE_CATALOG.resolve(accelerator_type).profile


def resolve_queued_tpu_profile(accelerator_type: str) -> TpuProfileResolution:
    """Return the typed policy decision for one discovered accelerator type."""

    return QUEUED_TPU_PROFILE_CATALOG.resolve(accelerator_type)


# Compatibility catalog for direct Python callers. The CLI derives profiles
# from the accessible GCP accelerator catalog instead of this tuple.
DEFAULT_TPU_PROFILES: tuple[TpuProfile, ...] = (
    TpuProfile("v5p-16", cpu=32, tpu=8),
    TpuProfile("v5p-32", cpu=32, tpu=16),
    TpuProfile("v6e-8", cpu=32, tpu=8),
)


def select_profiles(
    *,
    generations: tp.Iterable[str] = (),
    accelerator_types: tp.Iterable[str] = (),
    profiles: tp.Iterable[TpuProfile] = DEFAULT_TPU_PROFILES,
) -> tuple[TpuProfile, ...]:
    """Select catalog profiles by generation and/or exact accelerator type."""

    requested_generations = {value.strip().lower() for value in generations if value.strip()}
    requested_types = {value.strip().lower() for value in accelerator_types if value.strip()}
    selected = tuple(
        profile
        for profile in profiles
        if (not requested_generations or profile.generation in requested_generations)
        and (not requested_types or profile.accelerator_type in requested_types)
    )
    if not selected:
        selector = ", ".join(sorted({*requested_generations, *requested_types})) or "the default catalog"
        raise ValueError(f"no Eyvan TPU profiles match {selector}")
    return selected


def unsupported_profile_reason(accelerator_type: str) -> str:
    """Return the stable user-facing explanation for an excluded catalog type."""

    resolution = resolve_queued_tpu_profile(accelerator_type)
    if resolution.skip_reason is None:
        raise ValueError(f"queued-resource worker profile exists for {accelerator_type!r}")
    return resolution.skip_reason.value


__all__ = (
    "DEFAULT_TPU_PROFILES",
    "QUEUED_TPU_PROFILE_CATALOG",
    "QueuedTpuProfileCatalog",
    "TpuProfile",
    "TpuProfileResolution",
    "resolve_queued_tpu_profile",
    "select_profiles",
    "tpu_profile_from_accelerator_type",
    "unsupported_profile_reason",
)
