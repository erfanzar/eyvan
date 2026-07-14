# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import base64
import http.client
import io
import json
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, replace
from types import TracebackType

import pytest

from eyvan._internal.compat import StrEnum
from eyvan._internal.json_types import JsonObject
from eyvan.capacity.drivers.base import (
    CapacityStatus,
    CellObservation,
    ExecutionWorkloadStatus,
    MutationContext,
    StaleMutationError,
)
from eyvan.worker.runtime import callable_worker
from eyvan.worker.runtime.callable_worker import main as callable_worker_main
from eyvan.worker.runtime.callables import dump_callable, load_callable
from eyvan.worker.runtime.ray_jobs import RayJobsExecutor
from eyvan.worker.runtime.results import dump_result, load_result


def _increment(value: int) -> int:
    return value + 1


def test_callable_artifact_checks_digest_signature_and_python() -> None:
    artifact = dump_callable(_increment, signing_key=b"secret")

    loaded = load_callable(artifact, signing_key=b"secret")

    assert loaded(4) == 5
    with pytest.raises(ValueError, match="digest"):
        load_callable(replace(artifact, payload=artifact.payload + b"broken"), signing_key=b"secret")
    with pytest.raises(ValueError, match="signature"):
        load_callable(artifact, signing_key=b"wrong")
    with pytest.raises(ValueError, match="Python"):
        load_callable(replace(artifact, python="0.0"), signing_key=b"secret")


def test_callable_result_round_trips_arbitrary_python_and_checks_integrity() -> None:
    value = {"tuple": (1, 2), "bytes": b"payload"}
    artifact = dump_result(value, signing_key=b"secret")

    assert load_result(artifact, signing_key=b"secret") == value
    with pytest.raises(ValueError, match="digest"):
        load_result(artifact + b"broken", signing_key=b"secret")
    with pytest.raises(ValueError, match="signature"):
        load_result(artifact, signing_key=b"wrong")


def test_callable_worker_commits_result_artifact(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    callable_artifact = dump_callable(_increment)
    callable_path = tmp_path / "callable.pkl"
    result_path = tmp_path / "result.bin"
    callable_path.write_bytes(callable_artifact.payload)
    monkeypatch.setenv("EYVAN_CALL_ARGS_JSON", "[4]")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "eyvan-callable-worker",
            "--uri",
            str(callable_path),
            "--sha256",
            callable_artifact.sha256,
            "--python",
            callable_artifact.python,
            "--result-uri",
            str(result_path),
        ],
    )

    callable_worker_main()

    assert load_result(result_path.read_bytes()) == 5


@pytest.mark.parametrize(
    ("environment", "writes_result"),
    [
        ({"EYVAN_REPLICA_COUNT": "2", "EYVAN_REPLICA_INDEX": "0"}, True),
        ({"EYVAN_REPLICA_COUNT": "2", "EYVAN_REPLICA_INDEX": "1"}, False),
        (
            {
                "EYVAN_DISTRIBUTED_PROCESS_COUNT": "2",
                "EYVAN_DISTRIBUTED_PROCESS_ID": "0",
                "EYVAN_REPLICA_COUNT": "2",
                "EYVAN_REPLICA_INDEX": "0",
            },
            True,
        ),
        (
            {
                "EYVAN_DISTRIBUTED_PROCESS_COUNT": "2",
                "EYVAN_DISTRIBUTED_PROCESS_ID": "1",
                "EYVAN_REPLICA_COUNT": "2",
                "EYVAN_REPLICA_INDEX": "1",
            },
            False,
        ),
    ],
)
def test_callable_worker_only_the_leader_writes_a_gang_result(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    environment: dict[str, str],
    writes_result: bool,
) -> None:
    callable_artifact = dump_callable(_increment)
    callable_path = tmp_path / "callable.pkl"
    result_path = tmp_path / "result.bin"
    callable_path.write_bytes(callable_artifact.payload)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("EYVAN_CALL_ARGS_JSON", "[4]")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "eyvan-callable-worker",
            "--uri",
            str(callable_path),
            "--sha256",
            callable_artifact.sha256,
            "--python",
            callable_artifact.python,
            "--result-uri",
            str(result_path),
        ],
    )

    callable_worker_main()

    assert result_path.exists() is writes_result
    if writes_result:
        assert load_result(result_path.read_bytes()) == 5


@pytest.mark.parametrize(
    "environment",
    [
        {"EYVAN_REPLICA_COUNT": "2"},
        {"EYVAN_REPLICA_COUNT": "2", "EYVAN_REPLICA_INDEX": "2"},
        {
            "EYVAN_DISTRIBUTED_PROCESS_COUNT": "2",
            "EYVAN_DISTRIBUTED_PROCESS_ID": "1",
            "EYVAN_REPLICA_COUNT": "2",
            "EYVAN_REPLICA_INDEX": "0",
        },
    ],
)
def test_callable_worker_rejects_invalid_gang_result_ownership(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    environment: dict[str, str],
) -> None:
    callable_artifact = dump_callable(_increment)
    callable_path = tmp_path / "callable.pkl"
    result_path = tmp_path / "result.bin"
    callable_path.write_bytes(callable_artifact.payload)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("EYVAN_CALL_ARGS_JSON", "[4]")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "eyvan-callable-worker",
            "--uri",
            str(callable_path),
            "--sha256",
            callable_artifact.sha256,
            "--python",
            callable_artifact.python,
            "--result-uri",
            str(result_path),
        ],
    )

    with pytest.raises(RuntimeError, match="Eyvan callable"):
        callable_worker_main()

    assert not result_path.exists()


class FakeHTTPResponse:
    def __init__(self, payload: bytes = b""):
        self.payload = payload

    def __enter__(self) -> FakeHTTPResponse:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


class IncompleteHTTPResponse(FakeHTTPResponse):
    def read(self) -> bytes:
        raise http.client.IncompleteRead(b"partial")


def test_callable_worker_transfers_managed_artifacts_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = dump_callable(_increment)
    uploaded: list[urllib.request.Request] = []
    source_attempts = [0]
    sleeps: list[float] = []

    def urlopen(request: str | urllib.request.Request, *, timeout: int) -> FakeHTTPResponse:
        assert timeout == 60
        if isinstance(request, urllib.request.Request) and request.get_method() == "GET":
            assert request.full_url == "http://control-plane/callable"
            assert request.get_header("X-eyvan-artifact-token") == "read-token"
            source_attempts[0] += 1
            if source_attempts[0] == 1:
                raise urllib.error.HTTPError(
                    request.full_url,
                    503,
                    "temporarily unavailable",
                    {"Retry-After": "0"},
                    io.BytesIO(),
                )
            return FakeHTTPResponse(artifact.payload)
        assert isinstance(request, urllib.request.Request)
        uploaded.append(request)
        if len(uploaded) == 1:
            raise urllib.error.URLError("connection reset after write")
        if len(uploaded) == 2:
            return IncompleteHTTPResponse()
        return FakeHTTPResponse()

    monkeypatch.setattr(callable_worker.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(callable_worker.time, "sleep", sleeps.append)
    monkeypatch.setenv("EYVAN_MANAGED_CALLABLE_URL", "http://control-plane/callable")
    monkeypatch.setenv("EYVAN_MANAGED_CALLABLE_TOKEN", "read-token")
    monkeypatch.setenv("EYVAN_MANAGED_RESULT_URL", "http://control-plane/result")
    monkeypatch.setenv("EYVAN_MANAGED_RESULT_TOKEN", "write-token")
    monkeypatch.setenv("EYVAN_ALLOW_INSECURE_ARTIFACT_HTTP", "1")
    monkeypatch.setenv("EYVAN_CALL_ARGS_JSON", "[4]")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "eyvan-callable-worker",
            "--uri-env",
            "EYVAN_MANAGED_CALLABLE_URL",
            "--uri-token-env",
            "EYVAN_MANAGED_CALLABLE_TOKEN",
            "--sha256",
            artifact.sha256,
            "--python",
            artifact.python,
            "--result-uri-env",
            "EYVAN_MANAGED_RESULT_URL",
            "--result-token-env",
            "EYVAN_MANAGED_RESULT_TOKEN",
        ],
    )

    callable_worker_main()

    assert source_attempts == [2]
    assert len(uploaded) == 3
    assert uploaded[2].full_url == "http://control-plane/result"
    assert uploaded[2].get_method() == "PUT"
    assert uploaded[2].get_header("X-eyvan-artifact-token") == "write-token"
    assert load_result(uploaded[2].data or b"") == 5
    assert sleeps == [0.0, 0.25, 0.5]


def test_callable_worker_rejects_insecure_managed_artifact_http(monkeypatch: pytest.MonkeyPatch) -> None:
    artifact = dump_callable(_increment)
    monkeypatch.setenv("EYVAN_MANAGED_CALLABLE_URL", "http://control-plane/callable?token=read-token")
    monkeypatch.delenv("EYVAN_ALLOW_INSECURE_ARTIFACT_HTTP", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "eyvan-callable-worker",
            "--uri-env",
            "EYVAN_MANAGED_CALLABLE_URL",
            "--sha256",
            artifact.sha256,
            "--python",
            artifact.python,
        ],
    )

    with pytest.raises(ValueError, match="HTTP callable artifacts require"):
        callable_worker_main()


class FakeRayJobStatus(StrEnum):
    """The current Ray states plus one future value used for rejection coverage."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    QUEUED = "QUEUED"


@dataclass(slots=True)
class FakeJob:
    submission_id: str
    metadata: dict[str, str] | None = None
    status: FakeRayJobStatus = FakeRayJobStatus.RUNNING
    message: str | None = None


@dataclass(frozen=True, slots=True)
class FakeSubmission:
    entrypoint: str
    submission_id: str
    runtime_env: JsonObject
    metadata: dict[str, str]
    entrypoint_num_cpus: int
    entrypoint_num_gpus: int
    entrypoint_memory: int
    entrypoint_resources: dict[str, float]


class FakeJobClient:
    def __init__(self) -> None:
        self.submissions: list[FakeSubmission] = []
        self.jobs: list[FakeJob] = []
        self.stopped: list[str] = []
        self.race_on_submit = False
        self.list_error: RuntimeError | None = None

    def get_version(self) -> str:
        return "2.56.1"

    def submit_job(
        self,
        *,
        entrypoint: str,
        runtime_env: JsonObject,
        metadata: dict[str, str],
        submission_id: str,
        entrypoint_num_cpus: int,
        entrypoint_num_gpus: int,
        entrypoint_memory: int,
        entrypoint_resources: dict[str, float],
    ) -> str:
        self.submissions.append(
            FakeSubmission(
                entrypoint=entrypoint,
                submission_id=submission_id,
                runtime_env=runtime_env,
                metadata=metadata,
                entrypoint_num_cpus=entrypoint_num_cpus,
                entrypoint_num_gpus=entrypoint_num_gpus,
                entrypoint_memory=entrypoint_memory,
                entrypoint_resources=entrypoint_resources,
            )
        )
        self.jobs.append(FakeJob(submission_id, metadata))
        if self.race_on_submit:
            raise RuntimeError("submission ID already exists")
        return submission_id

    def list_jobs(self) -> list[FakeJob]:
        if self.list_error is not None:
            raise self.list_error
        return self.jobs

    def get_job_info(self, submission_id: str) -> FakeJob:
        for job in self.jobs:
            if job.submission_id == submission_id:
                return job
        raise RuntimeError(f"job {submission_id!r} is absent")

    def stop_job(self, submission_id: str) -> bool:
        self.stopped.append(submission_id)
        self.get_job_info(submission_id).status = FakeRayJobStatus.STOPPED
        return True

    def get_job_logs(self, submission_id: str) -> str:
        self.get_job_info(submission_id)
        return ""


def _gang() -> JsonObject:
    return {
        "replicas": 2,
        "per_replica": {
            "cpu": 4,
            "memory_gib": 16,
            "gpu": 1,
            "tpu": 2,
            "custom": {"accelerator_type:v6e": 1, "fabric_bandwidth": 0.5},
        },
        "placement_strategy": "STRICT_SPREAD",
    }


def _fence(context: MutationContext) -> dict[str, str]:
    return {
        "eyvan_generation": str(context.generation),
        "eyvan_fencing_token": context.fencing_token,
    }


def test_ray_jobs_submission_wraps_every_gang_bundle_and_persists_fence() -> None:
    client = FakeJobClient()
    executor = RayJobsExecutor("http://cell:8265", client=client)
    context = MutationContext(idempotency_key="workload-1", generation=7, fencing_token="token-7")

    assert executor.health()

    remote_id = executor.submit(
        {
            "entrypoint": "python train.py --steps 10",
            "gang": _gang(),
            "runtime_env": {"env_vars": {"EXPERIMENT": "test"}},
            "metadata": {"project": "research"},
        },
        context,
    )

    assert remote_id == "workload-1"
    expected_bundles = [
        {
            "CPU": 4.0,
            "GPU": 1,
            "TPU": 2,
            "accelerator_type:v6e": 1.0,
            "fabric_bandwidth": 0.5,
            "memory": 16 * 1024**3,
        }
    ] * 2
    request = client.submissions[0]
    assert request.submission_id == "workload-1"
    assert request.entrypoint_num_cpus == 0
    assert request.entrypoint_num_gpus == 0
    assert request.entrypoint_memory == 0
    assert request.entrypoint_resources == {}
    assert request.metadata == {
        "project": "research",
        "eyvan_generation": "7",
        "eyvan_fencing_token": "token-7",
        "eyvan_resource_bundles": json.dumps(expected_bundles, sort_keys=True, separators=(",", ":")),
        "eyvan_placement_strategy": "STRICT_SPREAD",
    }
    assert request.runtime_env["env_vars"] == {
        "EXPERIMENT": "test",
        "EYVAN_RESOURCE_BUNDLES": request.metadata["eyvan_resource_bundles"],
        "EYVAN_PLACEMENT_STRATEGY": "STRICT_SPREAD",
    }
    command = shlex.split(request.entrypoint)
    assert command[:5] == ["python", "-m", "eyvan.worker.runtime.ray_jobs", "run-gang", command[4]]
    payload = json.loads(base64.urlsafe_b64decode(command[4]).decode())
    assert payload == {
        "entrypoint": "python train.py --steps 10",
        "bundles": expected_bundles,
        "strategy": "STRICT_SPREAD",
    }


@pytest.mark.parametrize(
    ("ray_status", "expected_status"),
    [
        (FakeRayJobStatus.PENDING, ExecutionWorkloadStatus.PENDING),
        (FakeRayJobStatus.RUNNING, ExecutionWorkloadStatus.RUNNING),
        (FakeRayJobStatus.STOPPED, ExecutionWorkloadStatus.STOPPED),
        (FakeRayJobStatus.SUCCEEDED, ExecutionWorkloadStatus.SUCCEEDED),
        (FakeRayJobStatus.FAILED, ExecutionWorkloadStatus.FAILED),
    ],
)
def test_ray_jobs_status_maps_each_concrete_ray_status(
    ray_status: FakeRayJobStatus,
    expected_status: ExecutionWorkloadStatus,
) -> None:
    client = FakeJobClient()
    client.jobs.append(
        FakeJob(
            "workload-1",
            _fence(MutationContext(idempotency_key="workload-1", generation=7, fencing_token="token-7")),
            status=ray_status,
            message="Ray status detail",
        )
    )

    observation = RayJobsExecutor("http://cell:8265", client=client).status("workload-1")

    assert observation.status is expected_status
    assert observation.as_result() == {
        "remote_id": "workload-1",
        "status": expected_status,
        "message": "Ray status detail",
    }
    assert dict(observation) == observation.as_result()


def test_ray_jobs_status_rejects_an_unrecognised_future_ray_status() -> None:
    client = FakeJobClient()
    client.jobs.append(FakeJob("workload-1", status=FakeRayJobStatus.QUEUED))

    with pytest.raises(RuntimeError, match="unsupported Ray job status 'QUEUED'"):
        RayJobsExecutor("http://cell:8265", client=client).status("workload-1")


def test_ray_jobs_status_reports_not_found_only_after_the_sdk_list_confirms_absence() -> None:
    observation = RayJobsExecutor("http://cell:8265", client=FakeJobClient()).status("absent")

    assert observation.status is ExecutionWorkloadStatus.NOT_FOUND
    assert observation.message == "Ray job is absent"


def test_ray_jobs_status_propagates_an_sdk_list_failure() -> None:
    client = FakeJobClient()
    client.list_error = RuntimeError("Ray dashboard is unavailable")

    with pytest.raises(RuntimeError, match="dashboard is unavailable"):
        RayJobsExecutor("http://cell:8265", client=client).status("workload-1")


def test_execution_workload_indeterminate_status_has_an_explicit_wire_value() -> None:
    assert ExecutionWorkloadStatus.INDETERMINATE.value == "INDETERMINATE"


def test_ray_jobs_adapter_import_does_not_import_optional_ray() -> None:
    code = "\n".join(
        (
            "import sys",
            "import eyvan.worker.runtime.ray_jobs",
            "assert 'ray' not in sys.modules",
        )
    )

    subprocess.run((sys.executable, "-c", code), check=True)


def test_ray_jobs_duplicate_submit_requires_the_persisted_fence() -> None:
    context = MutationContext(idempotency_key="workload-1", generation=7, fencing_token="token-7")
    client = FakeJobClient()
    client.jobs.append(FakeJob("workload-1", _fence(context)))
    executor = RayJobsExecutor("http://cell:8265", client=client)

    assert executor.submit({"entrypoint": "python train.py", "gang": _gang()}, context) == "workload-1"
    assert client.submissions == []


@pytest.mark.parametrize(
    "metadata,match",
    [
        ({}, "no persisted"),
        ({"eyvan_generation": "6", "eyvan_fencing_token": "token-7"}, "does not own"),
        ({"eyvan_generation": "7", "eyvan_fencing_token": "other"}, "fencing token"),
    ],
)
def test_ray_jobs_duplicate_submit_rejects_missing_or_stale_fences(metadata: dict, match: str) -> None:
    context = MutationContext(idempotency_key="workload-1", generation=7, fencing_token="token-7")
    client = FakeJobClient()
    client.jobs.append(FakeJob("workload-1", metadata))
    executor = RayJobsExecutor("http://cell:8265", client=client)

    with pytest.raises(StaleMutationError, match=match):
        executor.submit({"entrypoint": "python train.py", "gang": _gang()}, context)

    assert client.submissions == []


def test_ray_jobs_duplicate_submit_race_requires_an_idempotent_retry() -> None:
    context = MutationContext(idempotency_key="workload-1", generation=7, fencing_token="token-7")
    client = FakeJobClient()
    client.race_on_submit = True
    executor = RayJobsExecutor("http://cell:8265", client=client)

    with pytest.raises(RuntimeError, match="submission ID already exists"):
        executor.submit({"entrypoint": "python train.py", "gang": _gang()}, context)

    client.race_on_submit = False
    assert executor.submit({"entrypoint": "python train.py", "gang": _gang()}, context) == "workload-1"
    assert len(client.submissions) == 1


def test_ray_jobs_stop_and_checkpoint_validate_source_fence_before_mutation() -> None:
    context = MutationContext(idempotency_key="checkpoint-1", generation=7, fencing_token="token-7")
    client = FakeJobClient()
    client.jobs.append(FakeJob("workload-1", _fence(context)))
    executor = RayJobsExecutor("http://cell:8265", client=client)

    executor.stop("workload-1", context)
    checkpoint_id = executor.checkpoint(
        "workload-1",
        hook="python checkpoint.py",
        output_uri="gs://checkpoints/workload-1",
        context=context,
    )

    assert client.stopped == ["workload-1"]
    assert checkpoint_id == "checkpoint-1"
    assert client.submissions[0].metadata == {
        "eyvan_operation": "checkpoint",
        "eyvan_source_job_id": "workload-1",
        **_fence(context),
    }


def test_ray_jobs_retire_waits_for_terminal_and_treats_absence_as_terminal() -> None:
    context = MutationContext(idempotency_key="supersede", generation=7, fencing_token="token-7")
    client = FakeJobClient()
    client.jobs.append(FakeJob("workload-1", _fence(context)))
    executor = RayJobsExecutor("http://cell:8265", client=client)

    executor.retire("workload-1", context)
    executor.retire("already-absent", context)

    assert client.stopped == ["workload-1"]
    assert client.get_job_info("workload-1").status is FakeRayJobStatus.STOPPED


@pytest.mark.parametrize("operation", ["stop", "checkpoint"])
def test_ray_jobs_mutations_reject_a_source_without_the_matching_fence(operation: str) -> None:
    context = MutationContext(idempotency_key="mutation-1", generation=8, fencing_token="token-8")
    client = FakeJobClient()
    client.jobs.append(
        FakeJob(
            "workload-1",
            {"eyvan_generation": "7", "eyvan_fencing_token": "token-7"},
        )
    )
    executor = RayJobsExecutor("http://cell:8265", client=client)

    with pytest.raises(StaleMutationError, match="does not own"):
        if operation == "stop":
            executor.stop("workload-1", context)
        else:
            executor.checkpoint("workload-1", hook="checkpoint", output_uri=None, context=context)

    assert client.stopped == []
    assert client.submissions == []


@pytest.mark.parametrize(
    "gang,match",
    [
        (None, "gang resource contract"),
        ({"replicas": 0, "per_replica": {"cpu": 1}}, "at least one"),
        ({"replicas": 1, "per_replica": {"custom": {"TPU": 1}}}, "collides"),
    ],
)
def test_ray_jobs_rejects_invalid_gang_contracts(gang: dict | None, match: str) -> None:
    executor = RayJobsExecutor("http://cell:8265", client=FakeJobClient())
    context = MutationContext(idempotency_key="workload-1", generation=1, fencing_token="token")

    spec = {"entrypoint": "python train.py"}
    if gang is not None:
        spec["gang"] = gang
    with pytest.raises(ValueError, match=match):
        executor.submit(spec, context)


def test_ray_jobs_rejects_empty_entrypoint() -> None:
    executor = RayJobsExecutor("http://cell:8265", client=FakeJobClient())
    context = MutationContext(idempotency_key="workload-1", generation=1, fencing_token="token")

    with pytest.raises(ValueError, match="entrypoint"):
        executor.submit({}, context)


def test_resolver_raises_instead_of_asserting_when_worker_token_derivation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import eyvan.worker.runtime.native_bootstrap as native_bootstrap
    from eyvan.worker.runtime.registry import ExecutionDriverResolver

    monkeypatch.setattr(
        native_bootstrap,
        "derive_native_worker_token",
        lambda *args, **kwargs: None,
    )
    resolver = ExecutionDriverResolver(
        backend="eyvan-native",
        native_worker_token="root-secret",
    )
    observation = CellObservation(
        "cell-a",
        CapacityStatus.HEALTHY,
        1,
        execution_endpoint="http://worker",
        metadata={"management_uid": "uid-1"},
    )

    with pytest.raises(ValueError, match="could not derive a native worker token"):
        resolver.resolve("cell-a", observation)
