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
"""Settings coverage for Eyvan's owner-only native GCP runtime profile."""

from __future__ import annotations

from eyvan.agent.service import CellAgentService
from eyvan.client.endpoint import ControlPlaneEndpoint
from eyvan.config import EyvanSettings
from eyvan.drivers.gcp import GcpCapacityDriver
from eyvan.persistence.fleet import FleetStateStore, LocalBackend
from eyvan.providers.gcp import GcloudNativeWorkerPublicIngress, GcsArtifactBucket
from eyvan.providers.gcp.runtime.profile import GcpNativeRuntimeProfile


def _profile(
    *,
    wheel_uri: str | None = (f"gs://eyvan-artifacts/eyvan/runtime/wheels/{'a' * 64}/eyvan-0.1.0-py3-none-any.whl"),
) -> GcpNativeRuntimeProfile:
    """Return a valid profile without reading or writing a local secret file."""

    return GcpNativeRuntimeProfile(
        project_id="eyvan-project",
        artifact_root_uri="gs://eyvan-artifacts/eyvan",
        agent_shared_secret="agent-secret-abcdefghijklmnopqrstuvwxyz",
        native_worker_root_secret="worker-secret-abcdefghijklmnopqrstuvwxyz",
        native_worker_wheel_uri=wheel_uri,
    )


def test_gcp_native_runtime_profile_maps_to_api_and_agent_settings() -> None:
    profile = _profile()

    settings = EyvanSettings.from_gcp_native_runtime_profile(profile)

    assert settings.agent_shared_secret == profile.agent_shared_secret
    assert settings.native_worker_root_secret == profile.native_worker_root_secret
    assert settings.fleet_state_uri == "gs://eyvan-artifacts/eyvan/control-plane/fleet-state.json"
    assert settings.artifact_store_uri == profile.artifact_root_uri
    assert settings.native_worker_package == profile.native_worker_wheel_uri
    assert settings.native_worker_artifact_root_uri == profile.artifact_root_uri
    assert settings.native_worker_public_ingress_tag == profile.native_worker_public_ingress_tag
    assert settings.capacity_backend == "gcp"
    assert settings.execution_backend == "eyvan-native"
    assert profile.agent_shared_secret not in repr(settings)
    assert profile.native_worker_root_secret not in repr(settings)


def test_gcp_native_runtime_profile_allows_staging_before_a_wheel_exists() -> None:
    settings = EyvanSettings.from_gcp_native_runtime_profile(_profile(wheel_uri=None))

    assert settings.native_worker_package is None


def test_profile_backed_cell_agent_passes_the_gcs_artifact_root_to_native_gcp_drivers(tmp_path) -> None:
    profile = _profile()
    service = CellAgentService(
        state=FleetStateStore(LocalBackend(tmp_path / "fleet.json")),
        settings=EyvanSettings.from_gcp_native_runtime_profile(profile),
    )

    assert isinstance(service.capacity, GcpCapacityDriver)
    expected = GcsArtifactBucket.from_artifact_root_uri(profile.artifact_root_uri)
    assert service.capacity.tpu.native_worker_artifact_bucket == expected
    assert service.capacity.compute.native_worker_artifact_bucket == expected
    assert isinstance(service.capacity.tpu.native_worker_public_ingress, GcloudNativeWorkerPublicIngress)
    assert service.capacity.tpu.native_worker_public_ingress.network_tag == profile.native_worker_public_ingress_tag
    assert isinstance(service.capacity.compute.native_worker_public_ingress, GcloudNativeWorkerPublicIngress)
    assert service.capacity.compute.native_worker_public_ingress.network_tag == profile.native_worker_public_ingress_tag


def test_the_profile_path_honours_the_callable_trust_allow_list(monkeypatch) -> None:
    """Found live 2026-08-01: `EYVAN_TRUSTED_CALLABLE_PROJECTS` was inert here.

    A runtime profile describes one operator's *GCP* runtime -- buckets, secrets,
    a wheel. It has no notion of which projects may submit pickled Python, and it
    should not: that is an operator policy decision, exactly like
    ``fleet_state_uri``, which this constructor already reads from the
    environment for the same reason.

    Dropping it meant the whole `eyvan runtime` path refused every callable
    submission outside the loopback development project, with the documented
    environment variable set and no other way to grant trust.
    """

    monkeypatch.setenv("EYVAN_TRUSTED_CALLABLE_PROJECTS", "acme/research, acme/eval")

    settings = EyvanSettings.from_gcp_native_runtime_profile(_profile())

    assert settings.trusted_callable_projects == ("acme/research", "acme/eval")


def test_callable_trust_stays_empty_when_the_operator_grants_nothing(monkeypatch) -> None:
    """Least privilege: an unset variable must not widen the allow-list."""

    monkeypatch.delenv("EYVAN_TRUSTED_CALLABLE_PROJECTS", raising=False)

    assert EyvanSettings.from_gcp_native_runtime_profile(_profile()).trusted_callable_projects == ()


def test_the_capabilities_probe_tolerates_an_agent_that_is_restarting() -> None:
    """This probe runs *while* an agent is being replaced, so refusal is normal.

    Regression for a self-inflicted break on 2026-08-01: routing the agent client
    through one transport seam turned "connection refused" from a bare
    `httpx.ConnectError` into a typed `CellAgentUnavailableError`. This handler
    listed only httpx errors, so the typed one escaped and aborted
    `eyvan runtime restart` with exactly the condition the probe exists to
    tolerate -- the agent it was about to start not being up yet.
    """

    import httpx

    from eyvan.providers.gcp.runtime.supervisor import HttpCellAgentCapabilitiesProbe

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("[Errno 61] Connection refused", request=request)

    probe = HttpCellAgentCapabilitiesProbe()
    endpoint = ControlPlaneEndpoint.parse("http://127.0.0.1:8090")

    original = httpx.Client

    class _RefusingClient(original):  # type: ignore[misc,valid-type]
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            super().__init__(*args, **kwargs)

    httpx.Client = _RefusingClient  # type: ignore[misc]
    try:
        assert probe(endpoint, "secret") is None
    finally:
        httpx.Client = original  # type: ignore[misc]
