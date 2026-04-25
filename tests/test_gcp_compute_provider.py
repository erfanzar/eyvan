# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import json
import subprocess
from dataclasses import replace

import pytest

from eyvan.capacity import CapacityStatus
from eyvan.persistence.fleet import FleetCellRecord
from eyvan.providers.gcp.compute_instances import (
    ComputeInstanceSpec,
    GcloudComputeProvider,
    GcloudDefaultComputeServiceAccountResolver,
    GcloudNativeWorkerArtifactAccessStore,
    GcloudNativeWorkerSecretStore,
    GcsArtifactBucket,
    compute_instance_name,
    native_worker_secret_id,
    normalize_compute_config,
)
from eyvan.providers.gcp.queued_resources import GcloudError

UID = "0123456789abcdef0123456789abcdef"


def result(returncode: int = 0, *, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(["gcloud"], returncode, stdout, stderr)


def instance_payload(
    *,
    uid: str = UID,
    status: str = "RUNNING",
    machine_type: str = "g2-standard-8",
    accelerator_type: str | None = "nvidia-l4",
    accelerator_count: int = 1,
    provisioning_model: str = "SPOT",
    preemptible: bool = True,
    instance_termination_action: str = "DELETE",
    maintenance_policy: str = "TERMINATE",
) -> dict:
    accelerators = (
        []
        if accelerator_type is None
        else [{"acceleratorType": accelerator_type, "acceleratorCount": accelerator_count}]
    )
    return {
        "name": "cell-a-2e7d2c03-r1",
        "status": status,
        "machineType": f"zones/us-central1-a/machineTypes/{machine_type}",
        "guestAccelerators": accelerators,
        "labels": {"eyvan-cell": "cell-a", "eyvan-uid": uid},
        "metadata": {"items": [{"key": "eyvan-uid", "value": uid}]},
        "networkInterfaces": [{"networkIP": "10.20.0.7"}],
        "scheduling": {
            "provisioningModel": provisioning_model,
            "preemptible": preemptible,
            "instanceTerminationAction": instance_termination_action,
            "onHostMaintenance": maintenance_policy,
        },
    }


def spec(*, gpu: bool = True, capacity_class: str = "spot") -> ComputeInstanceSpec:
    return ComputeInstanceSpec(
        name="cell-a-2e7d2c03-r1",
        project="project-a",
        zone="us-central1-a",
        machine_type="g2-standard-8" if gpu else "c4-standard-8",
        capacity_class=capacity_class,
        gpu_type="nvidia-l4" if gpu else None,
        gpu_count=1 if gpu else 0,
        image_family="ubuntu-accelerator-2404-amd64-with-nvidia-580" if gpu else "ubuntu-2404-lts-amd64",
        image_project="ubuntu-os-accelerator-images" if gpu else "ubuntu-os-cloud",
        labels={"eyvan-cell": "cell-a", "eyvan-uid": UID},
        metadata={"eyvan-uid": UID},
    )


def test_compute_describe_rejects_non_object_gcloud_json() -> None:
    provider = GcloudComputeProvider(runner=lambda _args: result(stdout="[]"))

    with pytest.raises(GcloudError, match="did not return a JSON object"):
        provider.describe("cell-a", project="project-a", zone="us-central1-a")


@pytest.mark.parametrize(
    ("status", "message"),
    [(None, "did not report a status"), ("FUTURE_STATUS", "unsupported status")],
)
def test_compute_describe_rejects_missing_or_unsupported_statuses(
    status: str | None,
    message: str,
) -> None:
    payload = instance_payload()
    if status is None:
        payload.pop("status")
    else:
        payload["status"] = status
    provider = GcloudComputeProvider(runner=lambda _args: result(stdout=json.dumps(payload)))

    with pytest.raises(GcloudError, match=message):
        provider.describe("cell-a", project="project-a", zone="us-central1-a")


def tpu_spec(*, capacity_class: str = "spot") -> ComputeInstanceSpec:
    return ComputeInstanceSpec(
        name="cell-a-2e7d2c03-r1",
        project="project-a",
        zone="us-east5-a",
        machine_type="ct5p-hightpu-4t",
        capacity_class=capacity_class,
        tpu_type="ct5p",
        tpu_count=4,
        image_family="ubuntu-accel-2204-amd64-tpu-v5e-v5p-v6e",
        image_project="ubuntu-os-accelerator-images",
        labels={"eyvan-cell": "cell-a", "eyvan-uid": UID},
        metadata={"eyvan-uid": UID},
    )


def record() -> FleetCellRecord:
    config = normalize_compute_config(
        {
            "machine_type": "g2-standard-8",
            "gpu_type": "nvidia-l4",
            "gpu_count": 1,
        }
    )
    return FleetCellRecord(
        name="cell-a",
        project="project-a",
        zone="us-central1-a",
        accelerator_type="nvidia-l4",
        qr_id="cell-a-2e7d2c03-r1",
        metadata={"management_uid": UID, "compute_config": config},
    )


def test_compute_config_normalizes_cpu_gpu_tpu_and_rejects_ambiguous_images() -> None:
    cpu = normalize_compute_config(
        {"provider_options": {"machine_type": "c4-standard-8", "startup_script": "#!/bin/sh\necho ready\n"}}
    )
    gpu = normalize_compute_config(
        {"provider_options": {"machine_type": "g2-standard-8", "gpu_type": "nvidia-l4", "gpu_count": 1}}
    )
    tpu = normalize_compute_config(
        {"provider_options": {"machine_type": "ct5p-hightpu-4t", "tpu_type": "ct5p", "tpu_count": 4}}
    )

    assert (cpu["gpu_type"], cpu["gpu_count"], cpu["tpu_type"], cpu["tpu_count"], cpu["image_family"]) == (
        None,
        0,
        None,
        0,
        "ubuntu-2404-lts-amd64",
    )
    assert cpu["startup_script"].endswith("\n")
    assert (gpu["gpu_type"], gpu["gpu_count"], gpu["image_project"]) == (
        "nvidia-l4",
        1,
        "ubuntu-os-accelerator-images",
    )
    assert (tpu["tpu_type"], tpu["tpu_count"], tpu["image_family"], tpu["image_project"]) == (
        "ct5p",
        4,
        "ubuntu-accel-2204-amd64-tpu-v5e-v5p-v6e",
        "ubuntu-os-accelerator-images",
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        normalize_compute_config({"machine_type": "c4-standard-8", "image": "image-a", "image_family": "family-a"})
    with pytest.raises(ValueError, match="provided together"):
        normalize_compute_config({"machine_type": "g2-standard-8", "gpu_type": "nvidia-l4"})


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"tpu_type": "ct5p"}, "provided together"),
        ({"tpu_count": 4}, "provided together"),
        ({"tpu_count": True}, "non-negative integer"),
        (
            {"gpu_type": "nvidia-l4", "gpu_count": 1, "tpu_type": "ct5p", "tpu_count": 4},
            "mutually exclusive",
        ),
        ({"tpu_type": "ct5p", "tpu_count": 4, "maintenance_policy": "MIGRATE"}, "require.*TERMINATE"),
    ],
)
def test_compute_config_rejects_invalid_tpu_options(options: dict, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        normalize_compute_config({"machine_type": "ct5p-hightpu-4t", **options})


def test_compute_instance_spec_rejects_mixed_gpu_and_tpu_accelerators() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        replace(spec(), tpu_type="ct5p", tpu_count=4)


def test_compute_args_are_deterministic_for_cpu_gpu_tpu_and_spot() -> None:
    gpu_args = spec().create_args()
    cpu_args = spec(gpu=False, capacity_class="on-demand").create_args()
    attached_args = replace(spec(), machine_type="n1-standard-8").create_args()
    tpu_args = tpu_spec().create_args()

    assert gpu_args[:4] == ["compute", "instances", "create", "cell-a-2e7d2c03-r1"]
    assert "--accelerator" not in gpu_args
    assert attached_args[attached_args.index("--accelerator") + 1] == "type=nvidia-l4,count=1"
    assert gpu_args[gpu_args.index("--provisioning-model") + 1] == "SPOT"
    assert gpu_args[gpu_args.index("--instance-termination-action") + 1] == "DELETE"
    assert gpu_args[gpu_args.index("--maintenance-policy") + 1] == "TERMINATE"
    assert "--no-address" in gpu_args
    assert "--accelerator" not in cpu_args
    assert "--provisioning-model" not in cpu_args
    assert "--accelerator" not in tpu_args
    assert tpu_args[tpu_args.index("--machine-type") + 1] == "ct5p-hightpu-4t"
    assert tpu_args[tpu_args.index("--image-family") + 1] == "ubuntu-accel-2204-amd64-tpu-v5e-v5p-v6e"
    assert tpu_args[tpu_args.index("--image-project") + 1] == "ubuntu-os-accelerator-images"
    assert tpu_args[tpu_args.index("--provisioning-model") + 1] == "SPOT"
    assert tpu_args[tpu_args.index("--instance-termination-action") + 1] == "DELETE"
    assert tpu_args[tpu_args.index("--maintenance-policy") + 1] == "TERMINATE"
    assert not any("eray" in value.lower() for value in gpu_args)


def test_compute_instance_names_are_valid_stable_and_generation_scoped() -> None:
    first = compute_instance_name("GPU_Cell/With Invalid Characters", 1)
    replay = compute_instance_name("GPU_Cell/With Invalid Characters", 1)
    replacement = compute_instance_name("GPU_Cell/With Invalid Characters", 2)

    assert first == replay
    assert first != replacement
    assert len(first) <= 63
    assert first[0].isalpha() and first[-1].isalnum()
    assert set(first) <= set("abcdefghijklmnopqrstuvwxyz0123456789-")


def test_default_compute_transport_bounds_gcloud_calls(monkeypatch) -> None:
    calls: list[dict] = []

    def run(command, **kwargs):
        calls.append(kwargs)
        return subprocess.CompletedProcess(command, 1, "", "NOT_FOUND")

    monkeypatch.delenv("EYVAN_GCLOUD_COMPUTE_TIMEOUT_S", raising=False)
    monkeypatch.setattr(subprocess, "run", run)

    assert GcloudComputeProvider().describe("cell-a", project="project-a", zone="us-central1-a") is None
    assert calls[0]["timeout"] == 600


def test_default_compute_service_account_resolver_uses_the_project_number() -> None:
    calls: list[tuple[str, ...]] = []

    def runner(args):
        calls.append(tuple(args))
        return result(stdout="123456789012\n")

    account = GcloudDefaultComputeServiceAccountResolver(runner=runner).resolve(project="project-a")

    assert account == "123456789012-compute@developer.gserviceaccount.com"
    assert calls == [("projects", "describe", "project-a", "--format=value(projectNumber)")]


@pytest.mark.parametrize("stdout", ["", "not-a-number\n", "012345\n"])
def test_default_compute_service_account_resolver_rejects_an_invalid_project_number(stdout: str) -> None:
    with pytest.raises(GcloudError, match="default Compute Engine service account"):
        GcloudDefaultComputeServiceAccountResolver(runner=lambda _args: result(stdout=stdout)).resolve(
            project="project-a"
        )


def test_native_worker_secret_store_scopes_a_rotated_token_to_the_vm_service_account() -> None:
    calls: list[tuple[str, ...]] = []
    stored_value = ""

    def runner(args):
        nonlocal stored_value
        command = tuple(args)
        calls.append(command)
        if command[:2] == ("secrets", "describe"):
            return result(1, stderr="NOT_FOUND")
        if command[:2] == ("secrets", "create"):
            return result()
        if command[:3] == ("secrets", "versions", "add"):
            with open(command[command.index("--data-file") + 1], encoding="utf-8") as stream:
                stored_value = stream.read()
            return result()
        if command[:2] == ("secrets", "add-iam-policy-binding"):
            return result()
        raise AssertionError(command)

    store = GcloudNativeWorkerSecretStore(runner=runner)
    resource = store.ensure(
        project="project-a",
        management_uid=UID,
        value="worker-token",
        reader_service_account="eyvan-worker@project-a.iam.gserviceaccount.com",
    )

    secret_id = native_worker_secret_id(UID)
    assert resource == f"projects/project-a/secrets/{secret_id}/versions/latest"
    assert stored_value == "worker-token"
    assert all("worker-token" not in " ".join(command) for command in calls)
    binding = calls[-1]
    assert binding[binding.index("--member") + 1] == "serviceAccount:eyvan-worker@project-a.iam.gserviceaccount.com"
    assert binding[binding.index("--role") + 1] == "roles/secretmanager.secretAccessor"


def test_native_worker_secret_store_refuses_a_foreign_secret() -> None:
    def runner(args):
        assert tuple(args[:2]) == ("secrets", "describe")
        return result(stdout=json.dumps({"labels": {"eyvan-managed": "true", "eyvan-uid": "foreign"}}))

    with pytest.raises(GcloudError, match="not owned"):
        GcloudNativeWorkerSecretStore(runner=runner).ensure(
            project="project-a",
            management_uid=UID,
            value="worker-token",
            reader_service_account="eyvan-worker@project-a.iam.gserviceaccount.com",
        )


def test_native_worker_artifact_access_store_grants_only_read_and_create_roles() -> None:
    calls: list[tuple[str, ...]] = []

    def runner(args):
        calls.append(tuple(args))
        return result()

    GcloudNativeWorkerArtifactAccessStore(runner=runner).ensure(
        project="project-a",
        artifact_bucket=GcsArtifactBucket.from_artifact_root_uri("gs://eyvan-artifacts/eyvan"),
        reader_service_account="123456789012-compute@developer.gserviceaccount.com",
    )

    assert calls == [
        (
            "storage",
            "buckets",
            "add-iam-policy-binding",
            "gs://eyvan-artifacts",
            "--member",
            "serviceAccount:123456789012-compute@developer.gserviceaccount.com",
            "--role",
            "roles/storage.objectViewer",
            "--condition=None",
            "--project=project-a",
            "--quiet",
        ),
        (
            "storage",
            "buckets",
            "add-iam-policy-binding",
            "gs://eyvan-artifacts",
            "--member",
            "serviceAccount:123456789012-compute@developer.gserviceaccount.com",
            "--role",
            "roles/storage.objectCreator",
            "--condition=None",
            "--project=project-a",
            "--quiet",
        ),
    ]
    assert all("storage.objectAdmin" not in command for command in calls)


def test_native_worker_artifact_access_store_stops_before_provisioning_on_iam_failure() -> None:
    calls: list[tuple[str, ...]] = []

    def runner(args):
        calls.append(tuple(args))
        return result(1, stderr="PERMISSION_DENIED")

    with pytest.raises(GcloudError, match="could not configure native worker artifact access") as error:
        GcloudNativeWorkerArtifactAccessStore(runner=runner).ensure(
            project="project-a",
            artifact_bucket=GcsArtifactBucket.from_artifact_root_uri("gs://eyvan-artifacts/eyvan"),
            reader_service_account="123456789012-compute@developer.gserviceaccount.com",
        )

    assert "PERMISSION_DENIED" not in str(error.value)
    assert len(calls) == 1
    assert calls[0][calls[0].index("--role") + 1] == "roles/storage.objectViewer"


def test_provider_observes_only_owned_running_gpu() -> None:
    calls: list[tuple[str, ...]] = []

    def runner(args):
        calls.append(tuple(args))
        return result(stdout=json.dumps(instance_payload()))

    observation = GcloudComputeProvider(runner=runner).ensure(record())

    assert observation.state is CapacityStatus.HEALTHY
    assert observation.head_ip == "10.20.0.7"
    assert len(calls) == 1


def test_provider_discovers_native_worker_endpoint_only_for_owned_native_cells() -> None:
    calls: list[tuple[str, ...]] = []

    def runner(args):
        calls.append(tuple(args))
        return result(stdout=json.dumps(instance_payload()))

    native_record = record()
    native_record.metadata.update({"execution_backend": "eyvan-native", "native_worker_port": 8123})

    observation = GcloudComputeProvider(runner=runner).ensure(native_record)

    assert observation.head_ip == "10.20.0.7"
    assert observation.execution_endpoint == "http://10.20.0.7:8123"
    assert len(calls) == 1


def test_profile_managed_public_compute_workers_publish_nat_not_internal_endpoints() -> None:
    payload = instance_payload()
    payload["networkInterfaces"] = [
        {
            "networkIP": "10.20.0.7",
            "accessConfigs": [{"natIP": "34.1.2.3"}],
        }
    ]

    native_record = record()
    native_record.metadata.update(
        {
            "execution_backend": "eyvan-native",
            "native_worker_port": 8123,
            "native_worker_endpoint_mode": "profile-managed-public",
        }
    )

    observation = GcloudComputeProvider(runner=lambda _args: result(stdout=json.dumps(payload))).ensure(native_record)

    assert observation.state is CapacityStatus.HEALTHY
    assert observation.head_ip == "34.1.2.3"
    assert observation.execution_endpoint == "http://34.1.2.3:8123"


def test_profile_managed_public_compute_workers_wait_without_a_nat_ip() -> None:
    payload = instance_payload()
    payload["networkInterfaces"] = [{"networkIP": "10.20.0.7"}]
    native_record = record()
    native_record.metadata.update(
        {
            "execution_backend": "eyvan-native",
            "native_worker_port": 8123,
            "native_worker_endpoint_mode": "profile-managed-public",
        }
    )

    observation = GcloudComputeProvider(runner=lambda _args: result(stdout=json.dumps(payload))).ensure(native_record)

    assert observation.state is CapacityStatus.ACTIVE
    assert observation.head_ip is None
    assert observation.execution_endpoint is None


@pytest.mark.parametrize("status", ["TERMINATED", "SUSPENDED"])
def test_terminal_compute_instance_requires_recreation(status: str) -> None:
    def runner(_args):
        return result(stdout=json.dumps(instance_payload(status=status)))

    observation = GcloudComputeProvider(runner=runner).ensure(record())

    assert observation.state is CapacityStatus.NEEDS_RECREATE
    assert status in observation.detail


def test_missing_previously_observed_compute_instance_requires_fenced_recreation() -> None:
    calls: list[tuple[str, ...]] = []

    def runner(args):
        calls.append(tuple(args))
        return result(1, stderr="NOT_FOUND")

    persisted = record()
    persisted.state = CapacityStatus.HEALTHY

    observation = GcloudComputeProvider(runner=runner).ensure(persisted)

    assert observation.state is CapacityStatus.NEEDS_RECREATE
    assert calls and all("create" not in call for call in calls)


def test_spot_instance_validation_accepts_the_modern_gce_shape_without_preemptible() -> None:
    payload = instance_payload()
    payload["scheduling"].pop("preemptible")

    instance = GcloudComputeProvider._parse_instance(payload)

    GcloudComputeProvider._validate_instance(instance, spec())


def test_foreign_or_drifted_compute_instance_is_never_adopted() -> None:
    foreign = GcloudComputeProvider._parse_instance(instance_payload(uid="foreign"))
    with pytest.raises(GcloudError, match="not owned"):
        GcloudComputeProvider._validate_instance(foreign, spec())

    wrong_shape = GcloudComputeProvider._parse_instance(instance_payload(machine_type="a2-highgpu-1g"))
    with pytest.raises(GcloudError, match="expected 'g2-standard-8'"):
        GcloudComputeProvider._validate_instance(wrong_shape, spec())


def test_compute_tpu_validation_requires_exact_guest_accelerator_shape() -> None:
    payload = instance_payload(
        machine_type="ct5p-hightpu-4t",
        accelerator_type="ct5p",
        accelerator_count=4,
    )
    instance = GcloudComputeProvider._parse_instance(payload)

    assert instance.accelerators == (("ct5p", 4),)
    GcloudComputeProvider._validate_instance(instance, tpu_spec())

    drifted = GcloudComputeProvider._parse_instance({**payload, "guestAccelerators": []})
    with pytest.raises(GcloudError, match=r"expected.*ct5p.*4"):
        GcloudComputeProvider._validate_instance(drifted, tpu_spec())


@pytest.mark.parametrize(
    "scheduling",
    [
        {"provisioning_model": "STANDARD", "preemptible": False},
        {"instance_termination_action": "STOP"},
        {"maintenance_policy": "MIGRATE"},
    ],
)
def test_spot_instance_validation_rejects_scheduling_drift(scheduling: dict) -> None:
    instance = GcloudComputeProvider._parse_instance(instance_payload(**scheduling))

    with pytest.raises(GcloudError, match="expected Spot"):
        GcloudComputeProvider._validate_instance(instance, spec())


def test_create_error_recovers_only_when_owned_instance_landed() -> None:
    descriptions = 0

    def runner(args):
        nonlocal descriptions
        if "describe" in args:
            descriptions += 1
            if descriptions == 1:
                return result(1, stderr="NOT_FOUND")
            return result(stdout=json.dumps(instance_payload()))
        if "create" in args:
            return result(1, stderr="deadline exceeded after request was accepted")
        raise AssertionError(tuple(args))

    created = GcloudComputeProvider(runner=runner).create(spec())

    assert created.name == "cell-a-2e7d2c03-r1"
    assert descriptions == 2


def test_startup_script_uses_a_file_instead_of_command_line_metadata() -> None:
    descriptions = 0
    observed_script = ""

    def runner(args):
        nonlocal descriptions, observed_script
        if "describe" in args:
            descriptions += 1
            if descriptions == 1:
                return result(1, stderr="NOT_FOUND")
            return result(stdout=json.dumps(instance_payload()))
        file_arg = args[args.index("--metadata-from-file") + 1]
        with open(file_arg.split("=", 1)[1], encoding="utf-8") as stream:
            observed_script = stream.read()
        return result()

    provider = GcloudComputeProvider(runner=runner)
    provider.create(replace(spec(), startup_script="#!/bin/sh\necho ready\n"))

    assert observed_script == "#!/bin/sh\necho ready\n"


def test_delete_is_idempotent_and_refuses_foreign_replacement() -> None:
    def absent(_args):
        return result(1, stderr="NOT_FOUND")

    GcloudComputeProvider(runner=absent).delete(
        "cell-a-2e7d2c03-r1", project="project-a", zone="us-central1-a", management_uid=UID
    )

    def foreign(_args):
        return result(stdout=json.dumps(instance_payload(uid="foreign")))

    with pytest.raises(GcloudError, match="refusing to delete"):
        GcloudComputeProvider(runner=foreign).delete(
            "cell-a-2e7d2c03-r1", project="project-a", zone="us-central1-a", management_uid=UID
        )


def test_successful_delete_rechecks_absence() -> None:
    descriptions = 0

    def runner(args):
        nonlocal descriptions
        if "describe" in args:
            descriptions += 1
            if descriptions == 1:
                return result(stdout=json.dumps(instance_payload()))
            return result(1, stderr="NOT_FOUND")
        if "delete" in args:
            return result()
        raise AssertionError(tuple(args))

    GcloudComputeProvider(runner=runner, delete_wait_timeout_s=0).delete(
        "cell-a-2e7d2c03-r1", project="project-a", zone="us-central1-a", management_uid=UID
    )

    assert descriptions == 2


@pytest.mark.parametrize("present_after_error", [False, True])
def test_delete_ambiguous_failure_rechecks_presence(present_after_error: bool) -> None:
    descriptions = 0

    def runner(args):
        nonlocal descriptions
        if "describe" in args:
            descriptions += 1
            if descriptions == 1 or present_after_error:
                return result(stdout=json.dumps(instance_payload()))
            return result(1, stderr="NOT_FOUND")
        if "delete" in args:
            return result(1, stderr="connection closed before response")
        raise AssertionError(tuple(args))

    provider = GcloudComputeProvider(runner=runner, delete_wait_timeout_s=0)
    if present_after_error:
        with pytest.raises(GcloudError, match="connection closed"):
            provider.delete(
                "cell-a-2e7d2c03-r1",
                project="project-a",
                zone="us-central1-a",
                management_uid=UID,
            )
    else:
        provider.delete(
            "cell-a-2e7d2c03-r1",
            project="project-a",
            zone="us-central1-a",
            management_uid=UID,
        )
