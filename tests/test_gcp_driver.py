# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import typing as tp

import pytest

from eyvan._internal.json_types import JsonObject, is_json_object
from eyvan.capacity.drivers.base import CapacityStatus, MutationContext, StaleMutationError
from eyvan.capacity.drivers.gcp import GcpTpuCapacityDriver
from eyvan.capacity.fleet_state import FleetCellRecord, FleetStateStore, LocalBackend
from eyvan.capacity.providers.gcp import ProviderObservation

_SHA256 = "a" * 64


def metadata_object(record: FleetCellRecord, name: str) -> JsonObject:
    value = record.metadata[name]
    assert is_json_object(value)
    return value


def context(key: str, generation: int = 1, token: str = "leader-1") -> MutationContext:
    return MutationContext(idempotency_key=key, generation=generation, fencing_token=token)


class FakeProvider:
    def __init__(
        self,
        *,
        events: list[str] | None = None,
        on_ensure: tp.Callable[[FleetCellRecord], None] | None = None,
    ):
        self.reports = [ProviderObservation(CapacityStatus.WAITING, "capacity requested")]
        self.ensured: list[FleetCellRecord] = []
        self.deleted: list[tuple[str, str, str]] = []
        self.deleted_ownership_uids: list[str | None] = []
        self.events = events
        self.on_ensure = on_ensure

    def ensure(self, record: FleetCellRecord) -> ProviderObservation:
        if self.events is not None:
            self.events.append("provider")
        self.ensured.append(record)
        callback = self.on_ensure
        self.on_ensure = None
        if callback is not None:
            callback(record)
        return self.reports.pop(0) if len(self.reports) > 1 else self.reports[0]

    def delete(
        self,
        qr_id: str,
        *,
        project: str,
        zone: str,
        management_uid: str | None = None,
    ) -> None:
        self.deleted.append((qr_id, project, zone))
        self.deleted_ownership_uids.append(management_uid)


class FakeNativeWorkerSecrets:
    def __init__(self, *, on_ensure: tp.Callable[[], None] | None = None) -> None:
        self.ensured: list[dict] = []
        self.deleted: list[dict] = []
        self.on_ensure = on_ensure

    def ensure(self, **kwargs) -> str:
        self.ensured.append(kwargs)
        callback = self.on_ensure
        self.on_ensure = None
        if callback is not None:
            callback()
        return f"projects/{kwargs['project']}/secrets/eyvan-native-{kwargs['management_uid']}/versions/latest"

    def delete(self, **kwargs) -> None:
        self.deleted.append(kwargs)


class FakeNativeWorkerServiceAccountResolver:
    def __init__(self, service_account: str = "123456789012-compute@developer.gserviceaccount.com") -> None:
        self.service_account = service_account
        self.projects: list[str] = []

    def resolve(self, *, project: str) -> str:
        self.projects.append(project)
        return self.service_account


class FakeNativeWorkerPublicIngress:
    def __init__(self, *, events: list[str] | None = None, error: RuntimeError | None = None) -> None:
        self.events = events
        self.error = error
        self.calls: list[tuple[str, str | None, int]] = []

    def ensure(self, *, project: str, network: str | None, port: int) -> str:
        if self.events is not None:
            self.events.append("ingress")
        self.calls.append((project, network, port))
        if self.error is not None:
            raise self.error
        return "eyvan-nw-profile-managed"


def make_driver(tmp_path, provider: FakeProvider | None = None):
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    provider = provider or FakeProvider()
    return state, provider, GcpTpuCapacityDriver(state, provider=provider, execution_backend="native-local")


def desired() -> dict[str, str]:
    return {
        "project": "project-a",
        "zone": "us-central2-b",
        "accelerator_type": "v5p-16",
        "capacity_class": "spot",
        "executor": "native-local",
    }


def remove_cell_record(state: FleetStateStore, cell_id: str) -> None:
    """Simulate an external state writer removing one registered cell."""

    def erase(document: JsonObject) -> None:
        cells = document["cells"]
        assert is_json_object(cells)
        cells.pop(cell_id)

    state.backend.update(erase)


class DropCellOnNextReadState(FleetStateStore):
    """Inject the loss window between durable registration and bootstrap."""

    def __init__(self, backend: LocalBackend, cell_id: str) -> None:
        super().__init__(backend)
        self.cell_id = cell_id
        self.drop_next_read = False

    def get(self, name: str) -> FleetCellRecord | None:
        record = super().get(name)
        if self.drop_next_read and name == self.cell_id:
            self.drop_next_read = False
            remove_cell_record(self, name)
            return None
        return record


def test_apply_capacity_is_eyvan_owned_and_idempotent(tmp_path) -> None:
    state, provider, driver = make_driver(tmp_path)

    first = driver.apply_capacity("cell-a", desired(), context("request-1"))
    second = driver.apply_capacity("cell-a", desired(), context("request-1"))

    record = state.get("cell-a")
    assert first.state is CapacityStatus.WAITING
    assert second.state is CapacityStatus.WAITING
    assert len(provider.ensured) == 2
    assert record is not None
    assert (record.project, record.zone, record.accelerator_type) == (
        "project-a",
        "us-central2-b",
        "v5p-16",
    )
    assert record.qr_id == "cell-a-r1"
    assert record.fencing_token == "leader-1"
    assert metadata_object(record, "tpu_config")["internal_ips"] is False
    management_uid = record.metadata["management_uid"]
    assert isinstance(management_uid, str)
    assert len(management_uid) == 32
    assert all(item.metadata["management_uid"] == management_uid for item in provider.ensured)


def test_apply_capacity_persists_normalized_tpu_provider_options(tmp_path) -> None:
    state, _provider, driver = make_driver(tmp_path)
    configured = {
        **desired(),
        "provider_options": {
            "runtime_version": "v2-alpha-tpuv5",
            "network": "research",
            "subnetwork": "tpu-workers",
            "internal_ips": True,
            "service_account": "tpu-runtime@project-a.iam.gserviceaccount.com",
            "scopes": ["cloud-platform"],
            "tags": ["eyvan-worker"],
            "metadata": {"enable-oslogin": "TRUE"},
        },
    }

    observation = driver.apply_capacity("cell", configured, context("create"))

    record = state.get("cell")
    assert record is not None
    assert record.runtime_version == "v2-alpha-tpuv5"
    assert record.metadata["tpu_config"] == {
        "runtime_version": "v2-alpha-tpuv5",
        "network": "research",
        "subnetwork": "tpu-workers",
        "internal_ips": True,
        "service_account": "tpu-runtime@project-a.iam.gserviceaccount.com",
        "scopes": ["cloud-platform"],
        "tags": ["eyvan-worker"],
        "metadata": {"enable-oslogin": "TRUE"},
    }
    assert observation.metadata["tpu_config"] == record.metadata["tpu_config"]


def test_reconcile_of_an_already_missing_cell_returns_absent_without_provider_side_effects(tmp_path) -> None:
    state, provider, driver = make_driver(tmp_path)

    observation = driver.reconcile("missing", context("reconcile"))

    assert observation.state is CapacityStatus.ABSENT
    assert provider.ensured == []
    assert state.cell_tombstone("missing") is None


def test_native_queued_tpu_bootstraps_every_host_with_one_cell_scoped_secret(tmp_path) -> None:
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    provider = FakeProvider()
    secrets = FakeNativeWorkerSecrets()
    accounts = FakeNativeWorkerServiceAccountResolver()
    driver = GcpTpuCapacityDriver(
        state,
        provider=provider,
        execution_backend="eyvan-native",
        native_worker_port=8123,
        native_worker_package="gs://artifacts/eyvan/eyvan.whl",
        native_worker_package_sha256=_SHA256,
        native_worker_wheelhouse="gs://artifacts/eyvan/wheelhouse.zip",
        native_worker_wheelhouse_sha256=_SHA256,
        native_worker_shared_secret="agent-global-secret",
        native_worker_secrets=secrets,
        native_worker_service_account_resolver=accounts,
    )
    native_desired = {
        **desired(),
        "executor": "eyvan-native",
        "provider_options": {
            "node_count": 2,
            "service_account": "eyvan-worker@project-a.iam.gserviceaccount.com",
            "bootstrap_egress": "private-google-access",
        },
        "runtime": {
            "env": {"HF_HOME": "/var/lib/eyvan/workload/hf"},
            "artifacts": [
                {
                    "source": "gs://artifacts/runtime/easydel.whl",
                    "destination": "/opt/eyvan-runtime/easydel.whl",
                }
            ],
            "init_commands": ["/opt/eyvan-native/venv/bin/python -m pip install /opt/eyvan-runtime/easydel.whl"],
            "container": {
                "image": f"us-docker.pkg.dev/project-a/easydel/runtime@sha256:{'a' * 64}",
                "pull_policy": "if-not-present",
                "privileged": True,
            },
        },
    }

    driver.apply_capacity("cell", native_desired, context("create"))

    record = state.get("cell")
    assert record is not None
    tpu_config = metadata_object(record, "tpu_config")
    native_runtime = metadata_object(record, "native_runtime")
    script = tpu_config["startup_script"]
    assert isinstance(script, str)
    assert tpu_config["node_count"] == 2
    assert record.metadata["execution_backend"] == "eyvan-native"
    assert record.metadata["native_worker_port"] == 8123
    assert native_runtime["env"] == {"HF_HOME": "/var/lib/eyvan/workload/hf"}
    assert native_runtime["container"] == native_desired["runtime"]["container"] | {
        "working_dir": None,
        "mounts": [],
    }
    assert "agent-global-secret" not in script
    assert "--systemd-transient" in script
    assert "--runtime-env-file /etc/eyvan-native/workload-runtime.json" in script
    assert "--container-runtime-file /etc/eyvan-native/container-runtime.json" in script
    assert "docker version >/dev/null 2>&1" in script
    assert "gcloud storage cp gs://artifacts/runtime/easydel.whl /opt/eyvan-runtime/easydel.whl" in script
    assert "/opt/eyvan-native/venv/bin/python -m pip install /opt/eyvan-runtime/easydel.whl" in script
    assert secrets.ensured[0]["management_uid"] == record.metadata["management_uid"]
    assert secrets.ensured[0]["reader_service_account"] == "eyvan-worker@project-a.iam.gserviceaccount.com"
    assert accounts.projects == []
    assert metadata_object(provider.ensured[-1], "tpu_config")["startup_script"] == script

    driver.release("cell", context("release", generation=2, token="leader-2"))

    assert secrets.deleted == [{"project": "project-a", "management_uid": record.metadata["management_uid"]}]


def test_native_queued_tpu_uses_the_default_compute_service_account_when_omitted(tmp_path) -> None:
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    secrets = FakeNativeWorkerSecrets()
    accounts = FakeNativeWorkerServiceAccountResolver()
    driver = GcpTpuCapacityDriver(
        state,
        provider=FakeProvider(),
        execution_backend="eyvan-native",
        native_worker_package="gs://artifacts/eyvan/eyvan.whl",
        native_worker_package_sha256=_SHA256,
        native_worker_wheelhouse="gs://artifacts/eyvan/wheelhouse.zip",
        native_worker_wheelhouse_sha256=_SHA256,
        native_worker_shared_secret="agent-global-secret",
        native_worker_secrets=secrets,
        native_worker_service_account_resolver=accounts,
    )

    driver.apply_capacity(
        "cell",
        {
            **desired(),
            "executor": "eyvan-native",
            "provider_options": {"bootstrap_egress": "private-google-access"},
        },
        context("create"),
    )

    record = state.get("cell")
    assert record is not None
    assert metadata_object(record, "tpu_config")["service_account"] is None
    assert accounts.projects == ["project-a"]
    assert secrets.ensured[0]["reader_service_account"] == accounts.service_account


def test_native_tpu_bootstrap_fences_a_missing_record_before_provider_capacity(tmp_path) -> None:
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))

    provider = FakeProvider()
    secrets = FakeNativeWorkerSecrets(on_ensure=lambda: remove_cell_record(state, "cell"))
    driver = GcpTpuCapacityDriver(
        state,
        provider=provider,
        execution_backend="eyvan-native",
        native_worker_package="gs://artifacts/eyvan/eyvan.whl",
        native_worker_package_sha256=_SHA256,
        native_worker_shared_secret="agent-global-secret",
        native_worker_secrets=secrets,
        native_worker_service_account_resolver=FakeNativeWorkerServiceAccountResolver(),
    )
    native_desired = {
        **desired(),
        "executor": "eyvan-native",
        "provider_options": {"bootstrap_egress": "public-internet"},
    }

    with pytest.raises(StaleMutationError, match="removed before native worker bootstrap could be persisted"):
        driver.apply_capacity("cell", native_desired, context("create"))

    assert provider.ensured == []
    assert state.get("cell") is None
    assert state.cell_tombstone("cell") == {"generation": 1, "fencing_token": "leader-1"}
    assert secrets.deleted == [{"project": "project-a", "management_uid": secrets.ensured[0]["management_uid"]}]
    with pytest.raises(StaleMutationError, match="not newer"):
        driver.apply_capacity("cell", native_desired, context("replay"))

    replacement = driver.apply_capacity("cell", native_desired, context("replacement", generation=2, token="leader-2"))

    assert replacement.state is CapacityStatus.WAITING
    assert len(provider.ensured) == 1


def test_native_tpu_bootstrap_fences_record_loss_before_setup_begins(tmp_path) -> None:
    state = DropCellOnNextReadState(LocalBackend(tmp_path / "fleet.json"), "cell")
    state.drop_next_read = True
    provider = FakeProvider()
    secrets = FakeNativeWorkerSecrets()
    driver = GcpTpuCapacityDriver(
        state,
        provider=provider,
        execution_backend="eyvan-native",
        native_worker_package="gs://artifacts/eyvan/eyvan.whl",
        native_worker_package_sha256=_SHA256,
        native_worker_shared_secret="agent-global-secret",
        native_worker_secrets=secrets,
        native_worker_service_account_resolver=FakeNativeWorkerServiceAccountResolver(),
    )
    native_desired = {
        **desired(),
        "executor": "eyvan-native",
        "provider_options": {"bootstrap_egress": "public-internet"},
    }

    with pytest.raises(StaleMutationError, match="capacity reconciliation could begin"):
        driver.apply_capacity("cell", native_desired, context("create"))

    assert provider.ensured == []
    assert secrets.ensured == []
    assert state.cell_tombstone("cell") == {"generation": 1, "fencing_token": "leader-1"}
    with pytest.raises(StaleMutationError, match="not newer"):
        driver.apply_capacity("cell", native_desired, context("replay"))


def test_provider_observation_fences_record_loss_and_deletes_owned_capacity(tmp_path) -> None:
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    provider = FakeProvider(on_ensure=lambda _record: remove_cell_record(state, "cell"))
    driver = GcpTpuCapacityDriver(state, provider=provider, execution_backend="native-local")

    with pytest.raises(StaleMutationError, match="provider observation could be persisted"):
        driver.apply_capacity("cell", desired(), context("create"))

    assert len(provider.ensured) == 1
    assert provider.deleted == [("cell-r1", "project-a", "us-central2-b")]
    assert state.cell_tombstone("cell") == {"generation": 1, "fencing_token": "leader-1"}
    with pytest.raises(StaleMutationError, match="not newer"):
        driver.apply_capacity("cell", desired(), context("replay"))


def test_native_tpu_bootstrap_never_resurrects_a_released_record(tmp_path) -> None:
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))

    def release_cell() -> None:
        state.tombstone_record("cell", 2, "release", lambda _record: True)

    provider = FakeProvider()
    secrets = FakeNativeWorkerSecrets(on_ensure=release_cell)
    driver = GcpTpuCapacityDriver(
        state,
        provider=provider,
        execution_backend="eyvan-native",
        native_worker_package="gs://artifacts/eyvan/eyvan.whl",
        native_worker_package_sha256=_SHA256,
        native_worker_shared_secret="agent-global-secret",
        native_worker_secrets=secrets,
        native_worker_service_account_resolver=FakeNativeWorkerServiceAccountResolver(),
    )
    native_desired = {
        **desired(),
        "executor": "eyvan-native",
        "provider_options": {"bootstrap_egress": "public-internet"},
    }

    with pytest.raises(StaleMutationError, match="not newer"):
        driver.apply_capacity("cell", native_desired, context("create"))

    assert provider.ensured == []
    assert state.get("cell") is None
    assert state.cell_tombstone("cell") == {"generation": 2, "fencing_token": "release"}
    with pytest.raises(StaleMutationError, match="not newer"):
        driver.apply_capacity("cell", native_desired, context("replay"))

    replacement = driver.apply_capacity("cell", native_desired, context("replacement", generation=3, token="leader-3"))

    assert replacement.state is CapacityStatus.WAITING
    assert len(provider.ensured) == 1


def test_native_tpu_recreation_fences_record_loss_and_cleans_up_owned_resources(tmp_path) -> None:
    class RemoveCellOnSecondSecret(FakeNativeWorkerSecrets):
        def __init__(self) -> None:
            super().__init__()
            self.ensure_count = 0

        def ensure(self, **kwargs) -> str:
            secret = super().ensure(**kwargs)
            self.ensure_count += 1
            if self.ensure_count == 2:
                remove_cell_record(state, "cell")
            return secret

    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    provider = FakeProvider()
    provider.reports = [
        ProviderObservation(CapacityStatus.WAITING),
        ProviderObservation(CapacityStatus.NEEDS_RECREATE, "queued resource failed"),
        ProviderObservation(CapacityStatus.NEEDS_RECREATE, "queued resource remains failed"),
    ]
    secrets = RemoveCellOnSecondSecret()
    driver = GcpTpuCapacityDriver(
        state,
        provider=provider,
        execution_backend="eyvan-native",
        native_worker_package="gs://artifacts/eyvan/eyvan.whl",
        native_worker_package_sha256=_SHA256,
        native_worker_shared_secret="agent-global-secret",
        native_worker_secrets=secrets,
        native_worker_service_account_resolver=FakeNativeWorkerServiceAccountResolver(),
    )
    native_desired = {
        **desired(),
        "executor": "eyvan-native",
        "provider_options": {"bootstrap_egress": "public-internet"},
    }
    driver.apply_capacity("cell", native_desired, context("create"))
    driver.reconcile("cell", context("reconcile"))

    with pytest.raises(StaleMutationError, match="native worker recreation could be persisted"):
        driver.reconcile("cell", context("reconcile"))

    assert len(provider.ensured) == 3
    assert provider.deleted == [
        ("cell-r1", "project-a", "us-central2-b"),
        ("cell-r2", "project-a", "us-central2-b"),
    ]
    assert state.cell_tombstone("cell") == {"generation": 1, "fencing_token": "leader-1"}
    assert secrets.deleted == [{"project": "project-a", "management_uid": secrets.ensured[0]["management_uid"]}]
    with pytest.raises(StaleMutationError, match="not newer"):
        driver.apply_capacity("cell", native_desired, context("replay"))


def test_profile_managed_public_tpu_ingress_precedes_capacity_and_refreshes_on_reconcile(tmp_path) -> None:
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    events: list[str] = []
    ingress = FakeNativeWorkerPublicIngress(events=events)
    provider = FakeProvider(events=events)
    driver = GcpTpuCapacityDriver(
        state,
        provider=provider,
        execution_backend="eyvan-native",
        native_worker_package="gs://artifacts/eyvan/eyvan.whl",
        native_worker_package_sha256=_SHA256,
        native_worker_shared_secret="agent-global-secret",
        native_worker_secrets=FakeNativeWorkerSecrets(),
        native_worker_service_account_resolver=FakeNativeWorkerServiceAccountResolver(),
        native_worker_public_ingress=ingress,
    )

    driver.apply_capacity(
        "public-cell",
        {
            **desired(),
            "executor": "eyvan-native",
            "provider_options": {"bootstrap_egress": "public-internet"},
        },
        context("create"),
    )

    record = state.get("public-cell")
    assert record is not None
    assert events == ["ingress", "provider"]
    assert ingress.calls == [("project-a", None, 8100)]
    assert metadata_object(record, "tpu_config")["tags"] == ["eyvan-nw-profile-managed"]
    assert record.metadata["native_worker_endpoint_mode"] == "profile-managed-public"

    driver.reconcile("public-cell", context("reconcile"))

    assert ingress.calls == [("project-a", None, 8100), ("project-a", None, 8100)]


def test_private_tpu_workers_never_receive_the_profile_managed_public_tag(tmp_path) -> None:
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    ingress = FakeNativeWorkerPublicIngress()
    driver = GcpTpuCapacityDriver(
        state,
        provider=FakeProvider(),
        execution_backend="eyvan-native",
        native_worker_package="gs://artifacts/eyvan/eyvan.whl",
        native_worker_package_sha256=_SHA256,
        native_worker_wheelhouse="gs://artifacts/eyvan/wheelhouse.zip",
        native_worker_wheelhouse_sha256=_SHA256,
        native_worker_shared_secret="agent-global-secret",
        native_worker_secrets=FakeNativeWorkerSecrets(),
        native_worker_service_account_resolver=FakeNativeWorkerServiceAccountResolver(),
        native_worker_public_ingress=ingress,
    )

    driver.apply_capacity(
        "private-cell",
        {
            **desired(),
            "executor": "eyvan-native",
            "provider_options": {"internal_ips": True, "bootstrap_egress": "private-google-access"},
        },
        context("create"),
    )

    record = state.get("private-cell")
    assert record is not None
    assert ingress.calls == []
    assert metadata_object(record, "tpu_config")["tags"] == []
    assert "native_worker_endpoint_mode" not in record.metadata


def test_public_tpu_capacity_is_not_requested_when_profile_ingress_cannot_be_configured(tmp_path) -> None:
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    provider = FakeProvider()
    ingress = FakeNativeWorkerPublicIngress(error=RuntimeError("local public IPv4 unavailable"))
    driver = GcpTpuCapacityDriver(
        state,
        provider=provider,
        execution_backend="eyvan-native",
        native_worker_package="gs://artifacts/eyvan/eyvan.whl",
        native_worker_package_sha256=_SHA256,
        native_worker_shared_secret="agent-global-secret",
        native_worker_secrets=FakeNativeWorkerSecrets(),
        native_worker_service_account_resolver=FakeNativeWorkerServiceAccountResolver(),
        native_worker_public_ingress=ingress,
    )

    with pytest.raises(RuntimeError, match="local public IPv4 unavailable"):
        driver.apply_capacity(
            "public-cell",
            {
                **desired(),
                "executor": "eyvan-native",
                "provider_options": {"bootstrap_egress": "public-internet"},
            },
            context("create"),
        )

    assert ingress.calls == [("project-a", None, 8100)]
    assert provider.ensured == []


def test_reconcile_persists_native_worker_endpoints_and_observed_tpu_vm_count(tmp_path) -> None:
    provider = FakeProvider()
    provider.reports = [
        ProviderObservation(CapacityStatus.WAITING),
        ProviderObservation(
            CapacityStatus.HEALTHY,
            head_ip="10.0.0.2",
            execution_endpoint="http://10.0.0.2:8100",
            worker_endpoints=("http://10.0.0.2:8100", "http://10.0.0.3:8100"),
            observed_tpu_vms=2,
            runtime_process_count=2,
        ),
    ]
    _state, _, driver = make_driver(tmp_path, provider)
    driver.apply_capacity("cell", desired(), context("create"))

    observation = driver.reconcile("cell", context("reconcile"))

    assert observation.metadata["native_worker_endpoints"] == [
        "http://10.0.0.2:8100",
        "http://10.0.0.3:8100",
    ]
    assert observation.observed_tpu_vms == 2
    assert observation.metadata["observed_tpu_vms"] == 2
    assert observation.runtime_process_count == 2
    assert observation.metadata["runtime_process_count"] == 2

    provider.reports = [ProviderObservation(CapacityStatus.UNAVAILABLE, "one host disappeared")]
    lost = driver.reconcile("cell", context("reconcile-after-loss"))
    assert lost.runtime_process_count == 2
    assert lost.observed_tpu_vms is None


@pytest.mark.parametrize(
    "provider_options",
    [
        {"unknown": True},
        {"internal_ips": "false"},
        {"metadata": {"eyvan-uid": "foreign"}},
    ],
)
def test_apply_capacity_rejects_invalid_tpu_provider_options_without_persisting(tmp_path, provider_options) -> None:
    state, _provider, driver = make_driver(tmp_path)

    with pytest.raises(ValueError):
        driver.apply_capacity("cell", {**desired(), "provider_options": provider_options}, context("create"))

    assert state.get("cell") is None


def test_apply_capacity_rejects_native_runtime_for_a_non_native_executor(tmp_path) -> None:
    state, _provider, driver = make_driver(tmp_path)

    with pytest.raises(ValueError, match="native runtime configuration requires execution backend eyvan-native"):
        driver.apply_capacity(
            "cell",
            {**desired(), "runtime": {"env": {"HF_HOME": "/var/lib/eyvan/workload/hf"}}},
            context("create"),
        )

    assert state.get("cell") is None


def test_reconcile_persists_provider_observation(tmp_path) -> None:
    provider = FakeProvider()
    provider.reports = [
        ProviderObservation(CapacityStatus.WAITING),
        ProviderObservation(CapacityStatus.HEALTHY, head_ip="10.0.0.2"),
    ]
    state, _, driver = make_driver(tmp_path, provider)
    driver.apply_capacity("cell", desired(), context("create"))

    observation = driver.reconcile("cell", context("reconcile"))

    assert observation.state is CapacityStatus.HEALTHY
    assert observation.metadata["head_ip"] == "10.0.0.2"
    assert state.get("cell").head_ip == "10.0.0.2"


def test_reconcile_never_recreates_capacity_after_the_agent_is_draining(tmp_path) -> None:
    provider = FakeProvider()
    provider.reports = [
        ProviderObservation(CapacityStatus.WAITING),
        ProviderObservation(CapacityStatus.NEEDS_RECREATE, "queued resource failed"),
    ]
    state, _, driver = make_driver(tmp_path, provider)
    driver.apply_capacity("cell", desired(), context("create"))

    def start_drain(record: FleetCellRecord) -> None:
        record.desired_state = "down"
        record.state = CapacityStatus.NEEDS_RECREATE

    state.mutate_record("cell", start_drain)
    observation = driver.reconcile("cell", context("reconcile"))

    assert observation.state is CapacityStatus.NEEDS_RECREATE
    assert observation.detail == "capacity desired state is down"
    assert len(provider.ensured) == 1
    assert provider.deleted == []


def test_reconcile_durably_requeues_terminal_capacity(tmp_path) -> None:
    provider = FakeProvider()
    provider.reports = [
        ProviderObservation(CapacityStatus.WAITING),
        ProviderObservation(CapacityStatus.NEEDS_RECREATE, "queued resource failed"),
        ProviderObservation(CapacityStatus.NEEDS_RECREATE, "queued resource still failed"),
        ProviderObservation(CapacityStatus.WAITING, "replacement requested"),
    ]
    state, _, driver = make_driver(tmp_path, provider)
    driver.apply_capacity("cell", desired(), context("create"))

    failed = driver.reconcile("cell", context("reconcile"))
    observation = driver.reconcile("cell", context("reconcile"))

    record = state.get("cell")
    assert failed.state is CapacityStatus.NEEDS_RECREATE
    assert observation.state is CapacityStatus.WAITING
    assert provider.deleted == [("cell-r1", "project-a", "us-central2-b")]
    assert record.qr_id == "cell-r2"
    assert record.provider_generation == 2
    assert "recreate_intent" not in record.metadata


def test_reconcile_resumes_a_crashed_capacity_recreate_intent(tmp_path) -> None:
    state, provider, driver = make_driver(tmp_path)
    driver.apply_capacity("cell", desired(), context("create"))

    def interrupted(record):
        record.state = CapacityStatus.RECREATING
        record.provider_generation = 2
        record.qr_id = "cell-r2"
        record.metadata["recreate_intent"] = {
            "previous_qr_id": "cell-r1",
            "target_qr_id": "cell-r2",
            "cell_generation": 1,
        }

    state.mutate_record("cell", interrupted)

    observation = driver.reconcile("cell", context("reconcile"))

    assert observation.state is CapacityStatus.WAITING
    assert provider.deleted == [("cell-r1", "project-a", "us-central2-b")]
    assert "recreate_intent" not in state.get("cell").metadata


def test_release_during_recreate_deletes_source_and_target_capacity(tmp_path) -> None:
    state, provider, driver = make_driver(tmp_path)
    driver.apply_capacity("cell", desired(), context("create"))

    def interrupted(record):
        record.state = CapacityStatus.RECREATING
        record.provider_generation = 2
        record.qr_id = "cell-r2"
        record.metadata["recreate_intent"] = {
            "previous_qr_id": "cell-r1",
            "target_qr_id": "cell-r2",
            "cell_generation": 1,
        }

    state.mutate_record("cell", interrupted)

    driver.release("cell", context("release", generation=2, token="leader-2"))

    assert provider.deleted == [
        ("cell-r1", "project-a", "us-central2-b"),
        ("cell-r2", "project-a", "us-central2-b"),
    ]
    assert state.get("cell") is None


def test_apply_capacity_rejects_stale_leader_and_wrong_executor(tmp_path) -> None:
    _, _, driver = make_driver(tmp_path)
    driver.apply_capacity("cell", desired(), context("new", generation=2, token="leader-2"))

    with pytest.raises(StaleMutationError, match="older"):
        driver.drain("cell", context("old", generation=1, token="leader-1"))
    with pytest.raises(StaleMutationError, match="fencing"):
        driver.drain("cell", context("split-brain", generation=2, token="other"))
    with pytest.raises(ValueError, match="provides 'native-local'"):
        driver.apply_capacity("other", {**desired(), "executor": "ray-jobs"}, context("wrong"))


def test_release_deletes_current_queued_resource_and_is_replay_safe(tmp_path) -> None:
    state, provider, driver = make_driver(tmp_path)
    driver.apply_capacity("cell", desired(), context("create"))
    management_uid = state.get("cell").metadata["management_uid"]
    release = context("release", generation=2, token="leader-2")

    first = driver.release("cell", release)
    second = driver.release("cell", release)

    assert first.state is CapacityStatus.RELEASED
    assert second.state is CapacityStatus.RELEASED
    assert provider.deleted == [("cell-r1", "project-a", "us-central2-b")]
    assert provider.deleted_ownership_uids == [management_uid]
    assert state.get("cell") is None
    assert state.cell_tombstone("cell") == {"generation": 2, "fencing_token": "leader-2"}


def test_release_does_not_remove_newer_owner_after_provider_call(tmp_path) -> None:
    state, provider, driver = make_driver(tmp_path)
    driver.apply_capacity("cell", desired(), context("create"))

    def steal(qr_id, *, project, zone, **_ownership):
        del qr_id, project, zone

        def mutate(record):
            record.generation = 3
            record.fencing_token = "leader-3"

        state.mutate_record("cell", mutate)

    provider.delete = steal

    with pytest.raises(StaleMutationError, match="changed ownership"):
        driver.release("cell", context("release", generation=2, token="leader-2"))

    assert state.get("cell").generation == 3


def test_release_with_a_consumed_key_still_deletes_live_capacity(tmp_path) -> None:
    state, provider, driver = make_driver(tmp_path)
    driver.apply_capacity("cell", desired(), context("create"))
    # A colliding idempotency key is consumed by another mutation while the
    # record still shows live capacity.
    driver.reconcile("cell", context("release", generation=2, token="leader-2"))

    observation = driver.release("cell", context("release", generation=2, token="leader-2"))

    assert observation.state is CapacityStatus.RELEASED
    assert provider.deleted == [("cell-r1", "project-a", "us-central2-b")]
    assert state.get("cell") is None
    assert state.cell_tombstone("cell") == {"generation": 2, "fencing_token": "leader-2"}


def test_replayed_release_on_a_deleting_record_still_deletes_capacity(tmp_path) -> None:
    state, provider, driver = make_driver(tmp_path)
    driver.apply_capacity("cell", desired(), context("create"))

    def crashed_release(record: FleetCellRecord) -> None:
        record.desired_state = "down"
        record.state = CapacityStatus.DELETING
        record.generation = 2
        record.fencing_token = "leader-2"
        record.idempotency_keys = [*record.idempotency_keys, "release"]

    state.mutate_record("cell", crashed_release)

    observation = driver.release("cell", context("release", generation=2, token="leader-2"))

    assert observation.state is CapacityStatus.RELEASED
    assert provider.deleted == [("cell-r1", "project-a", "us-central2-b")]
    assert state.get("cell") is None


def test_observation_metadata_never_shadows_record_fields(tmp_path) -> None:
    state, _, driver = make_driver(tmp_path)
    driver.apply_capacity("cell", desired(), context("create"))

    def poison(record: FleetCellRecord) -> None:
        record.metadata["project"] = "poisoned-project"
        record.metadata["zone"] = "poisoned-zone"
        record.metadata["resource_id"] = "poisoned-resource"
        record.metadata["head_ip"] = "203.0.113.99"

    state.mutate_record("cell", poison)

    metadata = driver.observe("cell").metadata
    assert metadata["project"] == "project-a"
    assert metadata["zone"] == "us-central2-b"
    assert metadata["resource_id"] == "cell-r1"
    assert metadata["qr_id"] == "cell-r1"
    assert metadata["head_ip"] is None


def test_drain_fences_absent_incarnation_and_newer_generation_can_recreate(tmp_path) -> None:
    _state, _, driver = make_driver(tmp_path)
    mutation = context("drain", generation=1, token="cell-1")

    observation = driver.drain("cell", mutation)

    assert observation.state is CapacityStatus.DRAINING
    with pytest.raises(StaleMutationError, match="not newer"):
        driver.apply_capacity("cell", desired(), context("late", generation=1, token="cell-1"))
    recreated = driver.apply_capacity("cell", desired(), context("new", generation=2, token="cell-2"))
    assert recreated.generation == 2
