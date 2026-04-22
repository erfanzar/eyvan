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
"""Generate, persist, and discover Spot TPU fleet configurations for Eyvan."""

from eyvan.autoscale.cache import (
    AutoscaleConfigCache,
    AutoscaleDefaultsRepository,
    GeneratedAutoscaleCache,
    QueueAdmissionDenialCache,
)
from eyvan.autoscale.compiler import generate_autoscale_config, generate_zone_autoscale_config, render_autoscale_yaml
from eyvan.autoscale.models import (
    DEFAULT_AUTOSCALE_CACHE_ROOT,
    DEFAULT_AUTOSCALE_DEFAULTS_PATH,
    AutoscaleDefaults,
    AutoscaleGenerationRequest,
    AutoscaleRequest,
    AutoscaleSkipReason,
    AutoscaleZoneRequest,
    CachedAutoscaleConfig,
    GeneratedAutoscaleConfig,
    GeneratedZoneAutoscaleConfig,
    GenerationQuotaSnapshot,
    QueueAdmissionDenial,
    QueueAdmissionDenialKey,
    QuotaSnapshot,
    SkippedAutoscaleCandidate,
    TpuProfile,
    validate_gcp_project_id,
)
from eyvan.autoscale.profiles import (
    DEFAULT_TPU_PROFILES,
    QUEUED_TPU_PROFILE_CATALOG,
    QueuedTpuProfileCatalog,
    TpuProfileResolution,
    resolve_queued_tpu_profile,
    select_profiles,
    tpu_profile_from_accelerator_type,
)
from eyvan.autoscale.queue_admission import QueueAdmissionDenialRecorder
from eyvan.autoscale.service import AutoscaleGenerationResult, AutoscaleGenerationService, TpuQuotaDiscovery

__all__ = (
    "DEFAULT_AUTOSCALE_CACHE_ROOT",
    "DEFAULT_AUTOSCALE_DEFAULTS_PATH",
    "DEFAULT_TPU_PROFILES",
    "QUEUED_TPU_PROFILE_CATALOG",
    "AutoscaleConfigCache",
    "AutoscaleDefaults",
    "AutoscaleDefaultsRepository",
    "AutoscaleGenerationRequest",
    "AutoscaleGenerationResult",
    "AutoscaleGenerationService",
    "AutoscaleRequest",
    "AutoscaleSkipReason",
    "AutoscaleZoneRequest",
    "CachedAutoscaleConfig",
    "GeneratedAutoscaleCache",
    "GeneratedAutoscaleConfig",
    "GeneratedZoneAutoscaleConfig",
    "GenerationQuotaSnapshot",
    "QueueAdmissionDenial",
    "QueueAdmissionDenialCache",
    "QueueAdmissionDenialKey",
    "QueueAdmissionDenialRecorder",
    "QueuedTpuProfileCatalog",
    "QuotaSnapshot",
    "SkippedAutoscaleCandidate",
    "TpuProfile",
    "TpuProfileResolution",
    "TpuQuotaDiscovery",
    "generate_autoscale_config",
    "generate_zone_autoscale_config",
    "render_autoscale_yaml",
    "resolve_queued_tpu_profile",
    "select_profiles",
    "tpu_profile_from_accelerator_type",
    "validate_gcp_project_id",
)
