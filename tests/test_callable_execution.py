# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import inspect
import subprocess
import typing as tp
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

import eyvan
from eyvan.artifacts import GCSArtifactStore, LocalArtifactStore
from eyvan.sdk import EyvanCallableSubmitter
from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
from eyvan.sdk.execution import (
    CallableSubmission,
    ExecutionContext,
    EyvanJob,
    EyvanJobError,
    EyvanPlacementError,
    JobCompletion,
    TpuConfig,
    execute,
    init,
)
from eyvan.server.service.models import (
    CapacityClass,
    CommandExecution,
    WorkloadDefinition,
    WorkloadRecord,
    WorkloadState,
)
from eyvan.worker.runtime.callables import CallableArtifact, dump_callable, load_callable

T = tp.TypeVar("T")


@pytest.fixture(autouse=True)
def _isolated_home(monkeypatch, tmp_path) -> None:
    """Keep default SDK setup independent from a developer's private GCP runtime profile."""

    monkeypatch.setenv("HOME", str(tmp_path / "home"))


def _add(left: int, right: int = 1) -> int:
    return left + right


class FakeJobBackend:
    def __init__(self, completion: JobCompletion | None = None):
        self.completion = completion or JobCompletion(state=WorkloadState.SUCCEEDED, succeeded=True)
        self.waited: list[tuple[str, float | None]] = []
        self.cancelled: list[str] = []

    def wait(self, job_id: str, timeout: float | None = None) -> JobCompletion:
        self.waited.append((job_id, timeout))
        return self.completion

    def status(self, job_id: str) -> WorkloadState:
        return self.completion.state

    def logs(self, job_id: str) -> str:
        return f"logs:{job_id}"

    def cancel(self, job_id: str) -> WorkloadRecord:
        self.cancelled.append(job_id)
        return WorkloadRecord(
            id=job_id,
            org_id="acme",
            project_id="models",
            spec=WorkloadDefinition(name="cancel", execution=CommandExecution(command="true")),
            state=WorkloadState.CANCELLED,
            created_by="tester",
        )


class FakeSubmitter(tp.Generic[T]):
    def __init__(self, result: T):
        self.result = result
        self.requests: list[CallableSubmission] = []
        self.backend = FakeJobBackend()
        self.read_uris: list[str] = []

    def submit_callable(self, request: CallableSubmission) -> EyvanJob[T]:
        self.requests.append(request)

        def read_result(uri: str) -> T:
            self.read_uris.append(uri)
            return self.result

        return EyvanJob(
            id=f"job-{len(self.requests)}",
            result_uri=request.result_uri,
            backend=self.backend,
            result_reader=read_result,
        )


def test_tpu_config_is_frozen_and_validated() -> None:
    config = TpuConfig(type=" v5p-512 ", pods=2)

    assert config == TpuConfig(type="v5p-512", pods=2)
    assert config.capacity_class is CapacityClass.SPOT
    assert config.max_retries == 3
    assert TpuConfig("v5p-512", 2, 7).max_retries == 7
    with pytest.raises(FrozenInstanceError):
        config.pods = 1  # type: ignore[misc]
    with pytest.raises(ValueError, match="TPU type"):
        TpuConfig(type="v5p 512")
    with pytest.raises(ValueError, match="positive integer"):
        TpuConfig(type="v5p-512", pods=0)
    with pytest.raises(ValueError, match="positive integer"):
        TpuConfig(type="v5p-512", pods=True)
    with pytest.raises(ValueError, match="max_retries"):
        TpuConfig(type="v5p-512", max_retries=-1)
    with pytest.raises(ValueError, match="max_retries"):
        TpuConfig(type="v5p-512", max_retries=True)
    with pytest.raises(ValueError, match="capacity_class"):
        TpuConfig(type="v5p-512", capacity_class="spot")  # type: ignore[arg-type]


def test_callable_surface_is_exported_from_top_level_package() -> None:
    assert eyvan.TpuConfig is TpuConfig
    assert eyvan.execute is execute
    assert eyvan.init is init
    assert eyvan.EyvanJob is EyvanJob


def test_init_uses_environment_fallbacks_and_explicit_artifact_uri(tmp_path) -> None:
    context = init(
        artifact_uri=str(tmp_path / "explicit"),
        environ={
            "EYVAN_FLEET": "research",
            "EYVAN_ORG": "acme",
            "EYVAN_PROJECT": "models",
            "EYVAN_ARTIFACT_URI": str(tmp_path / "ignored"),
        },
    )

    assert context.fleet == "research"
    assert context.org_id == "acme"
    assert context.project_id == "models"
    assert isinstance(context.artifact_store, LocalArtifactStore)
    assert context.artifact_store.root == (tmp_path / "explicit").resolve()
    assert context.signing_key is None
    assert isinstance(context.submitter, EyvanCallableSubmitter)


@pytest.mark.parametrize(
    ("signing_key", "environment"),
    (
        (b"caller-secret", {}),
        (None, {"EYVAN_CALLABLE_SIGNING_KEY": "caller-secret"}),
    ),
)
def test_init_rejects_caller_signing_keys_for_the_default_remote_submitter(
    tmp_path,
    monkeypatch,
    signing_key: bytes | None,
    environment: dict[str, str],
) -> None:
    """A caller secret must not be forwarded to an HTTP/native worker."""

    def unexpected_daemon_start(endpoint: str | ControlPlaneEndpoint) -> tp.NoReturn:
        del endpoint
        pytest.fail("a rejected signing key must not start the local control plane")

    monkeypatch.setattr("eyvan.sdk.execution._context", None)
    monkeypatch.setattr("eyvan.sdk.execution.ensure_loopback_control_plane", unexpected_daemon_start)
    environ = {
        "EYVAN_CONFIG_FILE": str(tmp_path / "no-context.json"),
        "EYVAN_FLEET": "research",
        "EYVAN_ORG": "acme",
        "EYVAN_PROJECT": "models",
        **environment,
    }

    with pytest.raises(ValueError, match="remote callable signing keys are not supported"):
        init(
            artifact_uri=str(tmp_path / "artifacts"),
            signing_key=signing_key,
            environ=environ,
        )


def test_init_allows_a_signing_key_for_an_explicit_custom_submitter(tmp_path, monkeypatch) -> None:
    """Custom submitters own their transport and may implement their own signing protocol."""

    submitter = FakeSubmitter(result=None)
    monkeypatch.setattr("eyvan.sdk.execution._context", None)

    context = init(
        artifact_store=LocalArtifactStore(tmp_path / "artifacts"),
        submitter=submitter,
        signing_key=b"custom-submitter-secret",
        environ={
            "EYVAN_CONFIG_FILE": str(tmp_path / "no-context.json"),
            "EYVAN_FLEET": "research",
            "EYVAN_ORG": "acme",
            "EYVAN_PROJECT": "models",
        },
    )

    assert context.submitter is submitter
    assert context.signing_key == b"custom-submitter-secret"


def test_init_raises_a_runtime_error_when_the_default_client_is_unavailable(tmp_path, monkeypatch) -> None:
    """The default submitter path fails explicitly instead of tripping an assert."""

    monkeypatch.setattr("eyvan.sdk.execution._context", None)
    monkeypatch.setattr("eyvan.sdk.execution.ensure_loopback_control_plane", lambda _endpoint: None)
    monkeypatch.setattr("eyvan.sdk.client.sync.EyvanClient", lambda **_kwargs: None)

    with pytest.raises(RuntimeError, match="requires the default SDK client"):
        init(
            artifact_uri=str(tmp_path / "artifacts"),
            environ={
                "EYVAN_CONFIG_FILE": str(tmp_path / "no-context.json"),
                "EYVAN_FLEET": "research",
                "EYVAN_ORG": "acme",
                "EYVAN_PROJECT": "models",
            },
        )


def test_decorator_is_lazy_preserves_metadata_and_submits_bound_callable(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path)
    submitter = FakeSubmitter(result=9)
    context = ExecutionContext(
        fleet="research",
        org_id="acme",
        project_id="models",
        artifact_store=store,
        submitter=submitter,
        signing_key=b"secret",
    )

    @execute(TpuConfig(type="v5p-512"), context=context)
    def add(left: int, right: int = 1) -> int:
        """Add two values remotely."""

        return left + right

    assert add.__name__ == "add"
    assert add.__doc__ == "Add two values remotely."
    assert inspect.signature(add) == inspect.signature(add.__wrapped__)
    assert list((tmp_path / "callables").glob("*")) == []
    assert submitter.requests == []

    job = add.remote(4, right=5)

    assert job.id == "job-1"
    assert job.status() is WorkloadState.SUCCEEDED
    assert job.logs() == "logs:job-1"
    assert job.cancel().id == "job-1"
    request = submitter.requests[0]
    assert request.fleet == "research"
    assert request.org_id == "acme"
    assert request.project_id == "models"
    assert request.tpu == TpuConfig(type="v5p-512")
    assert request.result_uri.endswith(f"/results/{request.invocation_id}.result")
    payload = store.read(request.artifact.uri)
    bound = load_callable(
        CallableArtifact(
            payload=payload,
            sha256=request.artifact.sha256,
            python=request.artifact.python,
            format_version=request.artifact.format_version,
            signature=request.artifact.signature,
        ),
        signing_key=b"secret",
    )
    assert tuple(inspect.signature(bound).parameters) == ()
    assert bound() == 9
    assert job.result(timeout=3) == 9
    assert submitter.backend.waited == [("job-1", 3)]
    assert submitter.read_uris == [request.result_uri]

    assert add(3, 6) == 9
    assert submitter.backend.waited[-1] == ("job-2", None)


def test_multi_pod_is_serialized_and_forwarded_to_the_pool_submitter(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path)
    submitter = FakeSubmitter(result=None)
    context = ExecutionContext(
        fleet="research",
        org_id="acme",
        project_id="models",
        artifact_store=store,
        submitter=submitter,
    )

    @execute(TpuConfig(type="v5p-512", pods=2), context=context)
    def train() -> None:
        pass

    job = train.submit()

    assert job.id == "job-1"
    assert len(submitter.requests) == 1
    assert submitter.requests[0].tpu == TpuConfig(type="v5p-512", pods=2)
    assert (tmp_path / "callables").exists()
    assert (tmp_path / "results").exists()


def test_decorator_uses_context_defaults_for_six_independent_pods(tmp_path, monkeypatch) -> None:
    store = LocalArtifactStore(tmp_path)
    submitter = FakeSubmitter(result=None)
    monkeypatch.setattr("eyvan.sdk.execution._context", None)
    init(
        artifact_store=store,
        submitter=submitter,
        environ={
            "EYVAN_CONFIG_FILE": str(tmp_path / "no-context.json"),
            "EYVAN_FLEET": "research",
            "EYVAN_ORG": "acme",
            "EYVAN_PROJECT": "models",
        },
    )

    @execute(TpuConfig(type="v5p-16", pods=6))
    def train() -> None:
        pass

    job = train.submit()

    assert job.id == "job-1"
    request = submitter.requests[0]
    assert (request.fleet, request.org_id, request.project_id) == ("research", "acme", "models")
    assert request.tpu == TpuConfig(type="v5p-16", pods=6)
    assert not hasattr(request.tpu, "slices")
    assert not hasattr(request.tpu, "node_count")


def test_job_failure_never_reads_result() -> None:
    backend = FakeJobBackend(JobCompletion(state=WorkloadState.FAILED, succeeded=False, message="worker exited"))
    read_uris: list[str] = []
    job = EyvanJob(
        id="job-1",
        result_uri="gs://bucket/results/job-1.result",
        backend=backend,
        result_reader=lambda uri: read_uris.append(uri),
    )

    with pytest.raises(EyvanJobError, match="worker exited"):
        job.result()

    assert read_uris == []


def test_job_result_uses_progress_waiter_without_making_plain_wait_noisy() -> None:
    completion = JobCompletion(state=WorkloadState.SUCCEEDED, succeeded=True)
    backend = FakeJobBackend(completion)
    progress_waits: list[tuple[str, float | None]] = []

    def wait_with_progress(job_id: str, timeout: float | None) -> JobCompletion:
        progress_waits.append((job_id, timeout))
        return completion

    job = EyvanJob(
        id="job-progress",
        result_uri="gs://bucket/results/job-progress.result",
        backend=backend,
        result_reader=lambda _uri: 7,
        progress_waiter=wait_with_progress,
    )

    assert job.wait(timeout=1) == completion
    assert backend.waited == [("job-progress", 1)]
    assert progress_waits == []

    assert job.result(timeout=2) == 7
    assert progress_waits == [("job-progress", 2)]


def test_empty_placement_inventory_does_not_claim_the_fleet_has_no_tpu_pools() -> None:
    error = EyvanPlacementError(
        fleet="research",
        requested_accelerator_type="v5p-16",
        available_accelerator_types=(),
    )

    assert str(error) == (
        "Eyvan cannot run TPU 'v5p-16' in selected fleet 'research'. "
        "That fleet has no compatible enabled TPU pools for this request. "
        "Select another fleet with `eyvan use FLEET`, or apply one with `eyvan up CONFIG`."
    )


def test_local_store_is_content_addressed_and_idempotent(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path)
    artifact = dump_callable(_add)

    first = store.put(artifact)
    second = store.put(artifact)

    assert first == second
    assert store.read(first.uri) == artifact.payload
    assert list((tmp_path / "callables").glob("*.pkl")) == [tmp_path / "callables" / f"{artifact.sha256}.pkl"]
    assert store.allocate_result_uri("call-one") != store.allocate_result_uri("call-two")


class FakeGcloudRunner:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.calls: list[list[str]] = []

    def __call__(
        self,
        args: tp.Sequence[str],
        *,
        check: bool,
        capture_output: bool,
    ) -> subprocess.CompletedProcess[bytes]:
        command = list(args)
        self.calls.append(command)
        uri = command[-1]
        if command[2:4] == ["objects", "describe"]:
            return subprocess.CompletedProcess(command, 0 if uri in self.objects else 1, stdout=b"", stderr=b"")
        if command[2] == "cp":
            self.objects[uri] = Path(command[-2]).read_bytes()
            return subprocess.CompletedProcess(command, 0, stdout=b"", stderr=b"")
        if command[2] == "cat":
            return subprocess.CompletedProcess(command, 0, stdout=self.objects[uri], stderr=b"")
        raise AssertionError(command)


def test_gcs_store_uploads_each_digest_once_and_can_read_results() -> None:
    runner = FakeGcloudRunner()
    store = GCSArtifactStore("gs://artifacts/eyvan", runner=runner)
    artifact = dump_callable(_add)

    first = store.put(artifact)
    second = store.put(artifact)

    assert first == second
    assert store.read(first.uri) == artifact.payload
    assert sum(command[2] == "cp" for command in runner.calls) == 1
    assert "--no-clobber" in next(command for command in runner.calls if command[2] == "cp")
    assert store.allocate_result_uri("call-one") == "gs://artifacts/eyvan/results/call-one.result"
