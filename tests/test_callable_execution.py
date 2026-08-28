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

import httpx
import pytest

import eyvan
from eyvan.artifacts import GCSArtifactStore, LocalArtifactStore
from eyvan.records import (
    CapacityClass,
    CommandExecution,
    WorkloadDefinition,
    WorkloadRecord,
    WorkloadState,
)
from eyvan.sdk import EyvanCallableSubmitter
from eyvan.sdk.client.callables import _CallableProgressRenderer
from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
from eyvan.sdk.client.transport import EyvanHTTPError
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
from eyvan.sdk.job_receipt import JobLifecycleState, JobLifecycleUpdate, JobTerminalOutput
from eyvan.server.local.daemon import (
    LocalDaemonError,
    LocalDaemonPaths,
    LocalDaemonRecord,
    LocalDaemonSupervisor,
    LocalDaemonTarget,
    process_is_alive,
    read_owner_only_record,
    write_owner_only_record,
)
from eyvan.worker.runtime.callables import CallableArtifact, dump_callable, load_callable

T = tp.TypeVar("T")


@pytest.fixture(autouse=True)
def _isolated_home(monkeypatch, tmp_path):
    """Keep default SDK setup independent from a developer's private GCP runtime profile."""

    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    yield home
    _stop_local_daemons_recorded_under(home)


def _stop_local_daemons_recorded_under(home: Path) -> None:
    """Stop planes this test recorded, and fail on any process left behind.

    A daemon started under the isolated home outlives the test process, keeps
    the loopback port bound with a credential from a home pytest is about to
    delete, and the next real ``eyvan up`` on that port answers 401 from a
    plane nobody owns. The PID records are owner-only by construction, so
    anything readable here was written during this test run. What the
    supervisor can prove it owns, it stops; what it cannot prove (a foreign
    command under a recycled PID) it refuses to signal, and the liveness
    check then names the leak instead of silently passing.
    """

    for record_path in sorted(home.glob(".eyvan/*.pid")):
        record = read_owner_only_record(record_path)
        if record is None:
            continue
        try:
            LocalDaemonSupervisor().stop(record.endpoint)
        except LocalDaemonError:
            pass
        if process_is_alive(record.pid):
            pytest.fail(
                f"a local Eyvan daemon (pid {record.pid}, {record.endpoint.url}) outlived the test; "
                "record it for teardown or patch ensure_loopback_control_plane out"
            )


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


def test_isolated_home_stops_recorded_daemons_and_flags_foreign_survivors(tmp_path) -> None:
    """The teardown must stop what it owns and name what it cannot prove it owns."""

    home = tmp_path / "home"
    endpoint = ControlPlaneEndpoint.parse("http://127.0.0.1:8080")
    target = LocalDaemonTarget.from_endpoint(endpoint)
    assert target is not None
    # The real record path for the endpoint, because stop() recomputes it.
    record_path = LocalDaemonPaths().record_path(target)
    record_path.parent.mkdir(parents=True, exist_ok=True)
    sleeper = subprocess.Popen(["sleep", "60"])
    try:
        write_owner_only_record(record_path, LocalDaemonRecord(endpoint=endpoint, pid=sleeper.pid))
        assert read_owner_only_record(record_path) is not None
        # A foreign command under a recorded PID must be refused, not killed,
        # and the refusal must surface as a failure that names the survivor.
        with pytest.raises(pytest.fail.Exception, match="outlived the test"):
            _stop_local_daemons_recorded_under(home)
    finally:
        sleeper.terminate()
        sleeper.wait()
    assert not process_is_alive(sleeper.pid)
    # A dead PID is stale metadata, not a leak: the record is cleared and no
    # failure is raised.
    _stop_local_daemons_recorded_under(home)
    assert not record_path.exists()


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
        "Select a fleet with `eyvan use FLEET`, list them with `eyvan capacity fleets`, "
        "or apply one with `eyvan up CONFIG`."
    )


def test_the_refusal_names_the_fleets_that_do_provide_the_shape() -> None:
    """The commonest way to reach this error is typing a zone where a fleet
    name belongs -- `asia-east1-c` for `research-asia-east1-c`. Telling somebody
    to "select another fleet" without saying which leaves them exactly as stuck,
    holding a name they already believe is a fleet."""

    error = EyvanPlacementError(
        fleet="asia-east1-c",
        requested_accelerator_type="v6e-16",
        available_accelerator_types=(),
        fleets_providing_requested_type=("research-asia-east1-c",),
        available_fleets=("laptop", "research-asia-east1-c", "research-us-east5-a"),
    )

    assert "Fleets providing 'v6e-16': research-asia-east1-c." in str(error)


def test_when_no_fleet_provides_the_shape_it_says_so_and_lists_what_exists() -> None:
    """ "No fleet has this" and "you named the wrong one" are different problems;
    only one of them is fixed by picking from a list."""

    error = EyvanPlacementError(
        fleet="research",
        requested_accelerator_type="v7-4096",
        available_accelerator_types=(),
        available_fleets=("laptop", "research"),
    )

    message = str(error)
    assert "No fleet provides it." in message
    assert "Fleets in this project: laptop, research." in message


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


# --------------------------------------------------------------------------- connecting, explicitly


def test_init_takes_the_address_positionally(monkeypatch) -> None:
    """`ray.init("...")` is the shape people arrive with.

    Keyword-only meant the one argument anybody passes was also the one that
    read least like the tool it replaces.
    """

    monkeypatch.delenv("EYVAN_URL", raising=False)
    submitter = FakeSubmitter("done")

    context = init("http://10.0.0.5:8080", submitter=submitter, artifact_uri="gs://bucket/root")

    assert context.base_url == "http://10.0.0.5:8080"


def test_init_reports_which_plane_it_resolved(monkeypatch) -> None:
    """It picks from argument, EYVAN_URL, saved context, this node, then the
    default -- and the caller could not previously tell which one won."""

    monkeypatch.setenv("EYVAN_URL", "http://10.0.0.9:8080")
    submitter = FakeSubmitter("done")

    context = init(submitter=submitter, artifact_uri="gs://bucket/root")

    assert context.base_url == "http://10.0.0.9:8080"


def test_the_address_and_the_keyword_may_not_disagree(monkeypatch) -> None:
    """One of them would silently win, and the loser is somebody's production
    plane."""

    monkeypatch.delenv("EYVAN_URL", raising=False)

    with pytest.raises(ValueError, match="address or base_url"):
        init(
            "http://10.0.0.5:8080",
            base_url="http://10.0.0.6:8080",
            submitter=FakeSubmitter("done"),
            artifact_uri="gs://bucket/root",
        )


def test_the_same_address_twice_is_not_a_conflict(monkeypatch) -> None:
    """Passing the same value both ways is redundant, not contradictory, and
    refusing it would fail code that is merely repetitive."""

    monkeypatch.delenv("EYVAN_URL", raising=False)

    context = init(
        "http://10.0.0.5:8080",
        base_url="http://10.0.0.5:8080",
        submitter=FakeSubmitter("done"),
        artifact_uri="gs://bucket/root",
    )

    assert context.base_url == "http://10.0.0.5:8080"


def test_a_blocking_wait_tails_its_job_output_like_ray() -> None:
    """The synchronous wait streams produced lines as they land, then the verdict."""

    lines: list[str] = []
    output = JobTerminalOutput(writer=lines.append)
    pages = {
        "": {
            "records": [{"rank": 0, "message": "hello from the cell\n"}],
            "next_cursor": "c1",
            "expected_ranks": 1,
        },
        "c1": {
            "records": [{"rank": 0, "message": "second line"}],
            "next_cursor": "c2",
            "expected_ranks": 1,
        },
    }
    served: list[str] = []

    not_found = EyvanHTTPError(
        response=httpx.Response(404, request=httpx.Request("GET", "http://plane/api")),
        detail="no progress",
    )

    class _Client:
        def get_workload_progress(self, workload_id: str):
            raise not_found

        def workload_log_page(self, workload_id: str, *, after: str, limit: int):
            served.append(after)
            return pages.get(after, {"records": [], "next_cursor": after})

    def record(state: WorkloadState) -> WorkloadRecord:
        return WorkloadRecord(
            id="wl-stream",
            org_id="acme",
            project_id="models",
            spec=WorkloadDefinition(name="stream", execution=CommandExecution(command="true")),
            state=state,
            created_by="tester",
        )

    renderer = _CallableProgressRenderer(_Client(), output, None)  # type: ignore[arg-type]
    renderer(record(WorkloadState.RUNNING))
    renderer(record(WorkloadState.SUCCEEDED))

    assert "hello from the cell" in lines
    assert "second line" in lines
    # Every page was read, and the verdict renders after the output it follows.
    assert served == ["", "c1", "c2"]
    assert any(line.startswith("Eyvan") for line in lines)
    assert lines.index("  job completed") > lines.index("second line")


def test_a_worker_count_change_never_reprints_the_state_heading() -> None:
    """A cell draining before SUCCEEDED is progress, not a new status block."""

    rendered: list[tuple[str, ...]] = []
    output = JobTerminalOutput(writer=lambda line: None)
    output.lifecycle = lambda update: rendered.append(update.plain_lines)  # type: ignore[method-assign]
    output.lifecycle_detail = lambda update: rendered.append(update.detail_lines)  # type: ignore[method-assign]
    renderer = _CallableProgressRenderer(client=None, output=output, queue_admission_recorder=None)  # type: ignore[arg-type]

    renderer._emit(JobLifecycleUpdate(JobLifecycleState.RUNNING, "v6e-8 x 1 worker", "1/1 workers ready"))
    renderer._emit(JobLifecycleUpdate(JobLifecycleState.RUNNING, "v6e-8 x 1 worker", "0/1 workers ready"))

    headings = [lines for lines in rendered if lines and lines[0].startswith("Eyvan")]
    assert len(headings) == 1
    assert rendered[-1] == ("  0/1 workers ready",)
