# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Kubernetes-backed capacity: one cell is one Indexed Job.

The manifest builders are the primary test surface because they are where the
design decisions live, and every one of them is a decision that fails silently if
it is wrong: a non-deterministic name breaks idempotent create, a wrong
completion mode breaks rank ordering, a token in the wrong field leaks a
credential to anyone with ``get`` in the namespace.

The driver tests use a fake API rather than a cluster. That is a real limit and
it is stated: this proves the *control-plane* logic -- fencing, generation
bumping, leak detection, endpoint publication -- and proves nothing about whether
a real kube-scheduler places the pods.
"""

from __future__ import annotations

import json
import typing as tp

import pytest

from eyvan._json import JsonObject
from eyvan.capacity import CapacityStatus
from eyvan.drivers.base import CapacityProviderError, MutationContext, StaleMutationError
from eyvan.drivers.kubernetes import MAX_WORKERS, KubernetesCapacityDriver
from eyvan.persistence.fleet import FleetStateStore, LocalBackend
from eyvan.providers.kubernetes.api import JobStatus, capacity_state, interpret_job
from eyvan.providers.kubernetes.kueue import foreign_workloads, inspect_cluster_queue
from eyvan.providers.kubernetes.workers import (
    CELL_LABEL,
    FENCE_ANNOTATION,
    GENERATION_LABEL,
    MAX_OBJECT_NAME,
    fence_fingerprint,
    headless_service_manifest,
    job_manifest,
    object_name,
    secret_manifest,
    worker_endpoints,
)

_TOKEN = "fencing-token-abcdef"


def _job(**overrides: object) -> JsonObject:
    arguments: dict[str, tp.Any] = {
        "cell_id": "cell-a",
        "namespace": "eyvan",
        "image": "registry.test/worker:1",
        "worker_count": 2,
        "provider_generation": 3,
        "fencing_token": _TOKEN,
        "resources": {"google.com/tpu": "4"},
    }
    arguments.update(overrides)
    return job_manifest(**arguments)


# --- manifest builders ------------------------------------------------------


def test_object_names_are_deterministic_which_is_what_makes_create_idempotent() -> None:
    assert object_name("eyvan", "cell-a") == object_name("eyvan", "cell-a")
    assert object_name("eyvan", "cell-a") != object_name("eyvan", "cell-b")


def test_a_long_cell_id_is_truncated_to_a_valid_dns_label() -> None:
    """Cell ids reach 88 characters; a DNS-1123 label may not exceed 63."""

    name = object_name("eyvan", "c" * 120)

    assert len(name) <= MAX_OBJECT_NAME
    assert name == name.lower()
    assert not name.startswith("-") and not name.endswith("-")


def test_two_long_cell_ids_sharing_a_prefix_do_not_collide() -> None:
    """Truncation alone would map every long id with the same prefix to one name."""

    first = object_name("eyvan", "c" * 100 + "-alpha")
    second = object_name("eyvan", "c" * 100 + "-beta")

    assert first != second


def test_a_cell_id_with_illegal_characters_becomes_a_valid_label() -> None:
    name = object_name("eyvan", "Cell_A.1/x")

    assert all(character.isalnum() or character == "-" for character in name)


def test_the_job_is_indexed_with_parallelism_matching_the_worker_count() -> None:
    """Indexed completion is what gives each pod a stable rank and DNS name."""

    manifest = _job(worker_count=4)

    spec = manifest["spec"]
    assert spec["completionMode"] == "Indexed"
    assert spec["parallelism"] == 4
    assert spec["completions"] == 4


def test_the_job_never_retries_itself() -> None:
    """A cell is a capacity unit, not a retry unit: Eyvan's budgets decide reruns."""

    assert _job()["spec"]["backoffLimit"] == 0


def test_the_fencing_token_appears_nowhere_in_the_job_manifest() -> None:
    """Annotations are readable by anyone with ``get`` in the namespace."""

    serialized = json.dumps(_job())

    assert _TOKEN not in serialized
    assert fence_fingerprint(_TOKEN) in serialized


def test_the_fence_fingerprint_is_not_reversible_to_the_token() -> None:
    fingerprint = fence_fingerprint(_TOKEN)

    assert _TOKEN not in fingerprint
    assert fingerprint != fence_fingerprint("a-different-token")


def test_the_job_carries_its_cell_and_generation_as_labels() -> None:
    manifest = _job(provider_generation=7)

    labels = manifest["metadata"]["labels"]
    assert labels[CELL_LABEL] == "cell-a"
    assert labels[GENERATION_LABEL] == "7"
    assert manifest["metadata"]["annotations"][FENCE_ANNOTATION] == fence_fingerprint(_TOKEN)


def test_pods_carry_the_same_labels_so_the_headless_service_selects_them() -> None:
    manifest = _job()

    assert manifest["spec"]["template"]["metadata"]["labels"][CELL_LABEL] == "cell-a"
    assert headless_service_manifest("cell-a", "eyvan")["spec"]["selector"][CELL_LABEL] == "cell-a"


def test_the_pod_subdomain_matches_the_service_name() -> None:
    """Stable DNS is `<job>-<index>.<subdomain>.<ns>.svc`; a mismatch resolves nothing."""

    manifest = _job()
    service = headless_service_manifest("cell-a", "eyvan")

    assert manifest["spec"]["template"]["spec"]["subdomain"] == service["metadata"]["name"]


def test_the_service_is_headless_and_publishes_before_readiness() -> None:
    spec = headless_service_manifest("cell-a", "eyvan")["spec"]

    # The literal string "None", not a JSON null: that is Kubernetes' own spelling
    # for a headless Service, and a null here would allocate a cluster IP and give
    # every pod the same address.
    assert spec["clusterIP"] == "None"
    assert spec["publishNotReadyAddresses"] is True


def test_worker_endpoints_are_index_ordered_because_index_is_rank() -> None:
    """NativeSliceExecutor derives per-rank checkpoint URIs from position."""

    endpoints = worker_endpoints("cell-a", "eyvan", 3)

    assert len(endpoints) == 3
    assert endpoints[0].startswith("http://eyvan-cell-a-0.")
    assert endpoints[2].startswith("http://eyvan-cell-a-2.")
    assert endpoints == worker_endpoints("cell-a", "eyvan", 3)


def test_a_zero_worker_cell_is_refused() -> None:
    with pytest.raises(ValueError):
        worker_endpoints("cell-a", "eyvan", 0)
    with pytest.raises(ValueError):
        _job(worker_count=0)


def test_the_worker_index_comes_from_the_completion_index_annotation() -> None:
    """That annotation is what Kubernetes sets per Indexed pod; nothing else is the rank."""

    container = _job()["spec"]["template"]["spec"]["containers"][0]
    index_env = next(item for item in container["env"] if item["name"] == "EYVAN_WORKER_INDEX")

    assert "job-completion-index" in index_env["valueFrom"]["fieldRef"]["fieldPath"]


def test_a_secret_backed_token_is_mounted_never_passed_as_an_env_value() -> None:
    manifest = _job(secret_name="eyvan-token-cell-a")

    container = manifest["spec"]["template"]["spec"]["containers"][0]
    assert container["volumeMounts"][0]["readOnly"] is True
    assert manifest["spec"]["template"]["spec"]["volumes"][0]["secret"]["secretName"] == "eyvan-token-cell-a"
    assert all("token" not in str(item.get("value", "")).lower() for item in container["env"])


def test_the_secret_manifest_holds_the_token_and_nothing_else_does() -> None:
    secret = secret_manifest("cell-a", "eyvan", token="derived-token")

    assert secret["stringData"]["token"] == "derived-token"
    assert "derived-token" not in json.dumps(_job())


def test_kueue_is_opt_in_and_only_then_is_the_job_suspended() -> None:
    """Kueue is not installed in most clusters; requiring it would make this untestable."""

    assert "suspend" not in _job()["spec"]
    assert _job(queue_name="research")["spec"]["suspend"] is True
    assert _job(queue_name="research")["metadata"]["labels"]["kueue.x-k8s.io/queue-name"] == "research"


# --- status interpretation --------------------------------------------------


def test_an_absent_job_is_a_legitimate_observation() -> None:
    assert interpret_job(None).exists is False
    assert capacity_state(interpret_job(None))[0] is CapacityStatus.ABSENT


def test_a_suspended_job_is_waiting_so_the_queue_timeout_applies() -> None:
    """It is queued behind admission; nothing is being built yet."""

    status = interpret_job({"spec": {"suspend": True, "parallelism": 2}, "status": {}})

    assert capacity_state(status)[0] is CapacityStatus.WAITING


def test_a_partially_ready_job_is_provisioning() -> None:
    status = interpret_job({"spec": {"parallelism": 4}, "status": {"ready": 2}})

    assert capacity_state(status)[0] is CapacityStatus.PROVISIONING


def test_a_fully_ready_job_is_healthy() -> None:
    status = interpret_job({"spec": {"parallelism": 4}, "status": {"ready": 4}})

    assert capacity_state(status)[0] is CapacityStatus.HEALTHY


def test_a_failed_job_needs_recreation_because_nothing_will_retry_it() -> None:
    status = interpret_job({"spec": {"parallelism": 2}, "status": {"failed": 1}})

    assert capacity_state(status)[0] is CapacityStatus.NEEDS_RECREATE


def test_kueue_inadmissible_is_waiting_and_eviction_is_unavailable() -> None:
    """Different states because only one of them destroyed something."""

    inadmissible = interpret_job(
        {"spec": {"parallelism": 2}, "status": {"conditions": [{"reason": "Inadmissible", "status": "True"}]}}
    )
    evicted = interpret_job(
        {"spec": {"parallelism": 2}, "status": {"conditions": [{"reason": "Evicted", "status": "True"}]}}
    )

    assert capacity_state(inadmissible)[0] is CapacityStatus.WAITING
    assert capacity_state(evicted)[0] is CapacityStatus.UNAVAILABLE
    assert "kueue" in capacity_state(evicted)[1]


def test_a_false_kueue_condition_is_not_treated_as_active() -> None:
    status = interpret_job(
        {"spec": {"parallelism": 1}, "status": {"ready": 1, "conditions": [{"reason": "Evicted", "status": "False"}]}}
    )

    assert capacity_state(status)[0] is CapacityStatus.HEALTHY


def test_malformed_status_counts_do_not_crash_interpretation() -> None:
    """A field the API server never sends must not take down reconciliation."""

    status = interpret_job({"spec": {"parallelism": "two"}, "status": {"ready": None, "failed": True}})

    assert status.parallelism == 0
    assert status.ready == 0
    assert status.failed == 0


# --- Kueue preconditions -----------------------------------------------------


def test_a_missing_cluster_queue_is_a_finding_not_a_clean_bill() -> None:
    """A pool pointed at a nonexistent queue suspends every Job forever."""

    findings = inspect_cluster_queue(None, "research")

    assert len(findings) == 1
    assert "does not exist" in findings[0].problem


def test_a_queue_with_preemption_disabled_is_safe() -> None:
    payload = {"spec": {"preemption": {"withinClusterQueue": "Never", "reclaimWithinCohort": "Never"}}}

    assert inspect_cluster_queue(payload, "research") == ()


def test_within_queue_preemption_is_reported() -> None:
    """Eviction destroys a pod with no checkpoint hook, bypassing PreemptionPlan."""

    payload = {"spec": {"preemption": {"withinClusterQueue": "LowerPriority"}}}

    findings = inspect_cluster_queue(payload, "research")

    assert len(findings) == 1
    assert "withinClusterQueue" in findings[0].problem
    assert "Never" in findings[0].remedy


def test_cohort_reclaim_is_reported_separately() -> None:
    payload = {"spec": {"preemption": {"reclaimWithinCohort": "Any"}}}

    findings = inspect_cluster_queue(payload, "research")

    assert len(findings) == 1
    assert "reclaimWithinCohort" in findings[0].problem


def test_a_workload_without_the_cell_label_is_foreign() -> None:
    """Absence of the label is the only test that cannot be spoofed."""

    workloads = [
        {"metadata": {"name": "eyvan-cell-a", "labels": {CELL_LABEL: "cell-a"}}},
        {"metadata": {"name": "someone-elses-job", "labels": {"app": "batch"}}},
        {"metadata": {"name": "unlabelled"}},
    ]

    assert foreign_workloads(workloads) == ("someone-elses-job", "unlabelled")


# --- driver lifecycle --------------------------------------------------------


class _FakeApi:
    """Record every API call and answer from an in-memory Job table."""

    def __init__(self) -> None:
        self.jobs: dict[str, JsonObject] = {}
        self.created: list[tuple[str, JsonObject]] = []
        self.deleted: list[str] = []

    def get_job(self, namespace: str, name: str) -> JsonObject | None:
        return self.jobs.get(f"{namespace}/{name}")

    def create(self, path: str, manifest: JsonObject) -> bool:
        self.created.append((path, manifest))
        if path.endswith("/jobs"):
            metadata = manifest["metadata"]
            key = f"{metadata['namespace']}/{metadata['name']}"
            if key in self.jobs:
                return False
            spec = manifest["spec"]
            self.jobs[key] = {
                "metadata": metadata,
                "spec": spec,
                "status": {"ready": spec["parallelism"]},
            }
        return True

    def delete(self, path: str) -> None:
        self.deleted.append(path)
        for key in list(self.jobs):
            if path.endswith(key.split("/", 1)[1]) and "/jobs/" in path:
                del self.jobs[key]


def _driver(tmp_path, api: _FakeApi) -> KubernetesCapacityDriver:
    return KubernetesCapacityDriver(
        FleetStateStore(LocalBackend(tmp_path / "fleet.json")),
        api=api,
        namespace="eyvan",
        shared_secret="agent-secret",
    )


def _desired(**overrides: object) -> dict[str, tp.Any]:
    payload: dict[str, tp.Any] = {
        "provider": "kubernetes",
        "capacity_backend": "kubernetes",
        "executor": "eyvan-native",
        "accelerator_type": "tpu",
        "capacity_class": "on-demand",
        "region": "us-east5",
        "provider_options": {"worker_count": 2, "resources": {"google.com/tpu": "4"}},
    }
    payload.update(overrides)
    return payload


def _context(generation: int = 1, key: str = "apply-1") -> MutationContext:
    return MutationContext(idempotency_key=key, generation=generation, fencing_token=_TOKEN)


def test_apply_creates_the_service_before_the_job(tmp_path) -> None:
    """Pods whose DNS name does not resolve at start have nothing to connect to."""

    api = _FakeApi()
    _driver(tmp_path, api).apply_capacity("cell-a", _desired(), _context())

    paths = [path for path, _manifest in api.created]
    assert paths.index("/api/v1/namespaces/eyvan/services") < paths.index("/apis/batch/v1/namespaces/eyvan/jobs")


def test_apply_creates_the_secret_before_the_job(tmp_path) -> None:
    """A pod that starts and fails to mount burns the backoffLimit-0 budget."""

    api = _FakeApi()
    _driver(tmp_path, api).apply_capacity("cell-a", _desired(), _context())

    paths = [path for path, _manifest in api.created]
    assert paths.index("/api/v1/namespaces/eyvan/secrets") < paths.index("/apis/batch/v1/namespaces/eyvan/jobs")


def test_a_ready_cell_publishes_one_endpoint_per_worker(tmp_path) -> None:
    api = _FakeApi()
    observation = _driver(tmp_path, api).apply_capacity("cell-a", _desired(), _context())

    assert observation.state is CapacityStatus.HEALTHY
    assert len(observation.metadata["native_worker_endpoints"]) == 2


def test_endpoints_are_withheld_until_every_worker_is_ready(tmp_path) -> None:
    """Publishing early makes the readiness probe resolve a nonexistent DNS name."""

    api = _FakeApi()
    driver = _driver(tmp_path, api)
    driver.apply_capacity("cell-a", _desired(), _context())
    for job in api.jobs.values():
        job["status"] = {"ready": 1}

    observation = driver.observe("cell-a")

    assert observation.state is CapacityStatus.PROVISIONING
    assert observation.metadata["native_worker_endpoints"] == []


def test_the_management_uid_is_derived_and_includes_the_namespace(tmp_path) -> None:
    """One cell_id in two clusters must not collide on a derived worker token."""

    api = _FakeApi()
    first = _driver(tmp_path, api).apply_capacity("cell-a", _desired(), _context())
    other = KubernetesCapacityDriver(
        FleetStateStore(LocalBackend(tmp_path / "other.json")),
        api=_FakeApi(),
        namespace="other-namespace",
        shared_secret="agent-secret",
    ).apply_capacity("cell-a", _desired(), _context())

    assert first.metadata["management_uid"] != other.metadata["management_uid"]


def test_reapplying_the_same_request_does_not_bump_the_generation(tmp_path) -> None:
    """A replayed outbox command must not invalidate every running worker's token."""

    api = _FakeApi()
    driver = _driver(tmp_path, api)
    first = driver.apply_capacity("cell-a", _desired(), _context())

    second = driver.apply_capacity("cell-a", _desired(), _context())

    assert second.metadata["provider_generation"] == first.metadata["provider_generation"]


def test_a_stale_generation_is_refused(tmp_path) -> None:
    api = _FakeApi()
    driver = _driver(tmp_path, api)
    driver.apply_capacity("cell-a", _desired(), _context(generation=5, key="apply-5"))

    with pytest.raises(StaleMutationError):
        driver.apply_capacity("cell-a", _desired(), _context(generation=2, key="apply-2"))


def test_a_job_from_a_previous_incarnation_is_not_adopted(tmp_path) -> None:
    """Its workers hold a token derived from a generation this record moved past."""

    api = _FakeApi()
    driver = _driver(tmp_path, api)
    driver.apply_capacity("cell-a", _desired(), _context())
    for job in api.jobs.values():
        job["metadata"]["annotations"][FENCE_ANNOTATION] = fence_fingerprint("someone-elses-token")

    observation = driver.observe("cell-a")

    assert observation.state is CapacityStatus.NEEDS_RECREATE
    assert "previous incarnation" in observation.detail


def test_release_deletes_the_job_first(tmp_path) -> None:
    """Deleting the Secret first strands running pods on a missing volume."""

    api = _FakeApi()
    driver = _driver(tmp_path, api)
    driver.apply_capacity("cell-a", _desired(), _context())

    driver.release("cell-a", _context())

    assert "/jobs/" in api.deleted[0]
    assert any("/secrets/" in path for path in api.deleted)
    assert any("/services/" in path for path in api.deleted)


def test_release_tombstones_the_record(tmp_path) -> None:
    api = _FakeApi()
    driver = _driver(tmp_path, api)
    driver.apply_capacity("cell-a", _desired(), _context())

    observation = driver.release("cell-a", _context())

    assert observation.state is CapacityStatus.RELEASED
    assert driver.observe("cell-a").state is CapacityStatus.ABSENT


def test_drain_does_not_delete_the_job(tmp_path) -> None:
    """Leased workloads keep their pods; that is what distinguishes drain from release."""

    api = _FakeApi()
    driver = _driver(tmp_path, api)
    driver.apply_capacity("cell-a", _desired(), _context())

    driver.drain("cell-a", _context())

    assert api.deleted == []
    assert api.jobs


def test_an_unsupported_capacity_class_is_refused(tmp_path) -> None:
    api = _FakeApi()

    with pytest.raises(ValueError, match="capacity class"):
        _driver(tmp_path, api).apply_capacity("cell-a", _desired(capacity_class="preemptible-ish"), _context())


def test_an_oversized_worker_count_is_refused(tmp_path) -> None:
    api = _FakeApi()
    desired = _desired(provider_options={"worker_count": MAX_WORKERS + 1})

    with pytest.raises(ValueError, match="at most"):
        _driver(tmp_path, api).apply_capacity("cell-a", desired, _context())


def test_the_k8s_alias_is_accepted_as_the_same_backend(tmp_path) -> None:
    api = _FakeApi()

    observation = _driver(tmp_path, api).apply_capacity("cell-a", _desired(capacity_backend="k8s"), _context())

    assert observation.state is CapacityStatus.HEALTHY


def test_a_provider_error_carries_its_retryability() -> None:
    """An unreachable API server is the ordinary state of a cluster during upgrade."""

    from eyvan.drivers.base import CapacityProviderFailureCode

    unavailable = CapacityProviderError("down", code=CapacityProviderFailureCode.PROVIDER_UNAVAILABLE)
    misconfigured = CapacityProviderError("no namespace", code=CapacityProviderFailureCode.CONFIGURATION)

    assert unavailable.retryable is True
    assert misconfigured.retryable is False


def test_job_status_defaults_are_not_silently_healthy() -> None:
    """A default-constructed status must never read as a ready cell."""

    assert capacity_state(JobStatus(exists=True))[0] is not CapacityStatus.HEALTHY
