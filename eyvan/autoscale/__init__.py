# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

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
