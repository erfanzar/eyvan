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
"""Google Cloud capacity primitives."""

from eyvan.providers.gcp.compute_instances import (
    ComputeInstance,
    ComputeInstanceSpec,
    ComputeProviderConfig,
    GcloudComputeProvider,
    GcloudDefaultComputeServiceAccountResolver,
    GcloudNativeWorkerArtifactAccessStore,
    GcloudNativeWorkerSecretStore,
    GcpComputeInstanceStatus,
    GcsArtifactBucket,
    compute_instance_name,
    native_worker_secret_id,
    normalize_compute_config,
)
from eyvan.providers.gcp.native_worker_ingress import (
    FirewallNetworkReference,
    FirewallRuleConflictError,
    GcloudFirewallRuleError,
    GcloudNativeWorkerPublicIngress,
    HttpPublicIpv4Resolver,
    NativeWorkerEndpointMode,
    NativeWorkerIngressError,
    NativeWorkerPublicIngress,
    PublicIpv4ResolutionError,
    endpoint_mode_from_metadata,
)
from eyvan.providers.gcp.queued_resources import (
    GcloudTpuProvider,
    GcpQueuedResourceState,
    GcpTpuNodeState,
    ProviderObservation,
    QueuedResource,
    QueuedResourceSpec,
    TpuProviderConfig,
)
from eyvan.providers.gcp.quota_discovery import GcloudQuotaError, GcpTpuQuotaDiscovery, TpuQuotaAvailability
from eyvan.providers.gcp.quota_usage import GcpQuotaUsageError, GcpTpuQuotaUsageDiscovery, TpuQuotaUsage

__all__ = (
    "ComputeInstance",
    "ComputeInstanceSpec",
    "ComputeProviderConfig",
    "FirewallNetworkReference",
    "FirewallRuleConflictError",
    "GcloudComputeProvider",
    "GcloudDefaultComputeServiceAccountResolver",
    "GcloudFirewallRuleError",
    "GcloudNativeWorkerArtifactAccessStore",
    "GcloudNativeWorkerPublicIngress",
    "GcloudNativeWorkerSecretStore",
    "GcloudQuotaError",
    "GcloudTpuProvider",
    "GcpComputeInstanceStatus",
    "GcpQueuedResourceState",
    "GcpQuotaUsageError",
    "GcpTpuNodeState",
    "GcpTpuQuotaDiscovery",
    "GcpTpuQuotaUsageDiscovery",
    "GcsArtifactBucket",
    "HttpPublicIpv4Resolver",
    "NativeWorkerEndpointMode",
    "NativeWorkerIngressError",
    "NativeWorkerPublicIngress",
    "ProviderObservation",
    "PublicIpv4ResolutionError",
    "QueuedResource",
    "QueuedResourceSpec",
    "TpuProviderConfig",
    "TpuQuotaAvailability",
    "TpuQuotaUsage",
    "compute_instance_name",
    "endpoint_mode_from_metadata",
    "native_worker_secret_id",
    "normalize_compute_config",
)
