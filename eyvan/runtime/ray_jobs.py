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
"""Optional Ray Jobs compatibility adapter for Eyvan cell agents."""

from __future__ import annotations

import base64
import json
import math
import os
import shlex
import subprocess
import sys
import time
import typing as tp
from dataclasses import dataclass, field

from eyvan._json import JsonObject, JsonValue, is_json_object
from eyvan.drivers.base import (
    ExecutionWorkloadObservation,
    ExecutionWorkloadStatus,
    MutationContext,
    StaleMutationError,
)

if tp.TYPE_CHECKING:
    from ray.job_submission import JobDetails, JobStatus


_GIB = 1024**3
_RAY_RESOURCE_NAMES = frozenset({"cpu", "gpu", "memory", "tpu"})
_PLACEMENT_STRATEGIES = frozenset({"PACK", "SPREAD", "STRICT_PACK", "STRICT_SPREAD"})
_RAY_JOB_STATUS_MAP: tp.Final[dict[str, ExecutionWorkloadStatus]] = {
    "PENDING": ExecutionWorkloadStatus.PENDING,
    "RUNNING": ExecutionWorkloadStatus.RUNNING,
    "STOPPED": ExecutionWorkloadStatus.STOPPED,
    "SUCCEEDED": ExecutionWorkloadStatus.SUCCEEDED,
    "FAILED": ExecutionWorkloadStatus.FAILED,
}
_TERMINAL_JOB_STATES = frozenset(
    {
        ExecutionWorkloadStatus.SUCCEEDED,
        ExecutionWorkloadStatus.STOPPED,
        ExecutionWorkloadStatus.FAILED,
    }
)


class RayJobsClient(tp.Protocol):
    """The public Ray Jobs SDK surface used by this adapter."""

    def get_version(self) -> str: ...

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
    ) -> str: ...

    def list_jobs(self) -> list[JobDetails]: ...

    def get_job_info(self, job_id: str) -> JobDetails: ...

    def stop_job(self, job_id: str) -> bool: ...

    def get_job_logs(self, job_id: str) -> str: ...


@dataclass(frozen=True, slots=True)
class _RayJobSubmission:
    """The strict subset of Ray's dynamic submission request that Eyvan emits."""

    entrypoint: str
    submission_id: str
    runtime_env: JsonObject
    metadata: dict[str, str]
    entrypoint_num_cpus: int = 0
    entrypoint_num_gpus: int = 0
    entrypoint_memory: int = 0
    entrypoint_resources: dict[str, float] = field(default_factory=dict)

    def submit(self, client: RayJobsClient) -> str:
        """Submit this request through the typed public Ray Jobs API."""

        return client.submit_job(
            entrypoint=self.entrypoint,
            submission_id=self.submission_id,
            runtime_env=self.runtime_env,
            metadata=self.metadata,
            entrypoint_num_cpus=self.entrypoint_num_cpus,
            entrypoint_num_gpus=self.entrypoint_num_gpus,
            entrypoint_memory=self.entrypoint_memory,
            entrypoint_resources=self.entrypoint_resources,
        )


def _optional_json_object(value: JsonValue | None, name: str) -> JsonObject:
    """Return an optional decoded JSON object as a mutable copy."""

    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return dict(value)


def _string_mapping(value: JsonValue | None, name: str) -> dict[str, str]:
    """Validate a JSON object whose keys and values are strings."""

    result = _optional_json_object(value, name)
    if not all(isinstance(key, str) and isinstance(item, str) for key, item in result.items()):
        raise ValueError(f"{name} must map strings to strings")
    return {key: item for key, item in result.items() if isinstance(item, str)}


def _quantity(value: JsonValue | None, name: str) -> float:
    """Validate a finite non-negative resource quantity."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"gang resource {name!r} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"gang resource {name!r} must be finite and non-negative")
    return result


def _integer_quantity(value: JsonValue | None, name: str) -> int:
    """Validate a finite non-negative integral resource quantity."""

    result = _quantity(value, name)
    if not result.is_integer():
        raise ValueError(f"gang resource {name!r} must be an integer")
    return int(result)


def _gang_contract(spec: tp.Mapping[str, JsonValue]) -> tuple[list[dict[str, float | int]], str]:
    """Build the Ray placement-group bundles from an Eyvan workload spec."""

    gang = spec.get("gang")
    if not isinstance(gang, dict):
        raise ValueError("Ray Jobs workloads require a gang resource contract")
    replicas = _integer_quantity(gang.get("replicas"), "replicas")
    if replicas < 1:
        raise ValueError("gang replicas must be at least one")
    resources = gang.get("per_replica")
    if not isinstance(resources, dict):
        raise ValueError("gang per_replica resources must be a mapping")

    bundle: dict[str, float | int] = {}
    cpu = _quantity(resources.get("cpu", 0), "cpu")
    memory_gib = _quantity(resources.get("memory_gib", 0), "memory_gib")
    gpu = _integer_quantity(resources.get("gpu", 0), "gpu")
    tpu = _integer_quantity(resources.get("tpu", 0), "tpu")
    if cpu:
        bundle["CPU"] = cpu
    if memory_gib:
        bundle["memory"] = int(memory_gib * _GIB)
    if gpu:
        bundle["GPU"] = gpu
    if tpu:
        bundle["TPU"] = tpu

    custom = _optional_json_object(resources.get("custom"), "gang custom resources")
    for raw_name, raw_value in custom.items():
        name = raw_name.strip()
        if not name:
            raise ValueError("gang custom resource names must be non-empty")
        if name.lower() in _RAY_RESOURCE_NAMES:
            raise ValueError(f"gang custom resource {name!r} collides with a built-in Ray resource")
        value = _quantity(raw_value, name)
        if value:
            bundle[name] = value
    if not bundle:
        raise ValueError("gang per_replica must request at least one resource")

    raw_strategy = gang.get("placement_strategy", "PACK")
    if not isinstance(raw_strategy, str):
        raise ValueError("gang placement_strategy must be a string")
    strategy = raw_strategy.upper()
    if strategy not in _PLACEMENT_STRATEGIES:
        choices = ", ".join(sorted(_PLACEMENT_STRATEGIES))
        raise ValueError(f"gang placement_strategy must be one of {choices}")
    return [dict(bundle) for _ in range(replicas)], strategy


def _encode_wrapper(entrypoint: str, bundles: list[dict[str, float | int]], strategy: str) -> str:
    """Encode the placement-group wrapper payload as an internal command."""

    payload: JsonObject = {
        "entrypoint": entrypoint,
        "bundles": [dict(bundle) for bundle in bundles],
        "strategy": strategy,
    }
    token = base64.urlsafe_b64encode(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).decode()
    return shlex.join(("python", "-m", "eyvan.runtime.ray_jobs", "run-gang", token))


def _run_replica(entrypoint: str, replica_index: int, replica_count: int) -> None:
    """Run one rank inside its placement-group bundle."""

    env = {
        **os.environ,
        "EYVAN_REPLICA_INDEX": str(replica_index),
        "EYVAN_REPLICA_COUNT": str(replica_count),
    }
    subprocess.run(("/bin/sh", "-lc", entrypoint), check=True, env=env)


def _payload_bundles(payload: JsonObject) -> tuple[str, list[dict[str, float | int]], str]:
    """Validate the JSON wrapper payload before passing it into Ray."""

    entrypoint = payload.get("entrypoint")
    if not isinstance(entrypoint, str) or not entrypoint.strip():
        raise ValueError("invalid Eyvan gang wrapper entrypoint")
    strategy = payload.get("strategy")
    if not isinstance(strategy, str) or strategy not in _PLACEMENT_STRATEGIES:
        raise ValueError("invalid Eyvan gang wrapper placement strategy")
    raw_bundles = payload.get("bundles")
    if not isinstance(raw_bundles, list) or not raw_bundles:
        raise ValueError("invalid Eyvan gang wrapper bundles")
    bundles: list[dict[str, float | int]] = []
    for raw_bundle in raw_bundles:
        if not isinstance(raw_bundle, dict) or not raw_bundle:
            raise ValueError("invalid Eyvan gang wrapper bundle")
        bundle: dict[str, float | int] = {}
        for name, value in raw_bundle.items():
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError("invalid Eyvan gang wrapper resource quantity")
            if not math.isfinite(float(value)) or value < 0:
                raise ValueError("invalid Eyvan gang wrapper resource quantity")
            bundle[name] = value
        bundles.append(bundle)
    return entrypoint, bundles, strategy


def _run_gang(payload: JsonObject) -> None:
    """Create one Ray placement group and run a rank in every bundle."""

    import ray
    from ray.util.placement_group import placement_group, remove_placement_group
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

    entrypoint, bundles, strategy = _payload_bundles(payload)
    if not ray.is_initialized():
        ray.init(address="auto")
    group = placement_group(bundles, strategy=strategy)
    try:
        ray.get(group.ready(), timeout=float(os.getenv("EYVAN_GANG_READY_TIMEOUT_S", "600")))
        replica = ray.remote(_run_replica)
        tasks = []
        for index, bundle in enumerate(bundles):
            custom = {name: value for name, value in bundle.items() if name not in {"CPU", "GPU", "memory"}}
            tasks.append(
                replica.options(
                    num_cpus=bundle.get("CPU", 0),
                    num_gpus=bundle.get("GPU", 0),
                    memory=bundle.get("memory", 0),
                    resources=custom,
                    scheduling_strategy=PlacementGroupSchedulingStrategy(
                        placement_group=group,
                        placement_group_bundle_index=index,
                        placement_group_capture_child_tasks=True,
                    ),
                ).remote(entrypoint, index, len(bundles))
            )
        ray.get(tasks)
    finally:
        remove_placement_group(group)


class RayJobsExecutor:
    """Idempotent, fenced whole-gang operations against a Ray dashboard."""

    def __init__(self, dashboard_url: str, *, client: RayJobsClient | None = None):
        if client is None:
            from ray.job_submission import JobSubmissionClient

            client = JobSubmissionClient(dashboard_url)
        self.dashboard_url = dashboard_url
        self.client = client

    def health(self) -> bool:
        """Return whether the Ray Jobs server accepts API requests."""

        return bool(self.client.get_version())

    @staticmethod
    def _metadata(job: JobDetails) -> dict[str, str]:
        """Read the metadata persisted on one Ray job submission."""

        return job.metadata or {}

    @staticmethod
    def _status_from_ray(status: JobStatus) -> ExecutionWorkloadStatus:
        """Map a concrete Ray ``JobStatus`` without guessing unknown states."""

        try:
            return _RAY_JOB_STATUS_MAP[status.value]
        except KeyError as exc:
            raise RuntimeError(f"unsupported Ray job status {status.value!r}") from exc

    @classmethod
    def _status(cls, job: JobDetails) -> ExecutionWorkloadStatus:
        """Map the direct ``status`` field on one Ray job submission."""

        return cls._status_from_ray(job.status)

    @classmethod
    def _observation(cls, remote_id: str, job: JobDetails) -> ExecutionWorkloadObservation:
        """Translate one typed Ray job record into Eyvan's stable status shape."""

        return ExecutionWorkloadObservation(
            remote_id=remote_id,
            status=cls._status(job),
            message=job.message or "",
        )

    def _find_job(self, submission_id: str) -> JobDetails | None:
        """Find a submitted job from Ray's typed list response."""

        for job in self.client.list_jobs():
            if job.submission_id != submission_id:
                continue
            return job
        return None

    def _validate_fence(self, remote_id: str, job: JobDetails, context: MutationContext) -> None:
        """Require the persisted Eyvan generation and fencing token."""

        metadata = self._metadata(job)
        persisted_generation = metadata.get("eyvan_generation")
        persisted_token = metadata.get("eyvan_fencing_token")
        if persisted_generation is None or persisted_token is None:
            raise StaleMutationError(f"Ray job {remote_id!r} has no persisted Eyvan fence")
        try:
            generation = int(persisted_generation)
        except ValueError as exc:
            raise StaleMutationError(f"Ray job {remote_id!r} has an invalid persisted generation") from exc
        if generation != context.generation:
            raise StaleMutationError(
                f"generation {context.generation} does not own Ray job {remote_id!r} generation {generation}"
            )
        if persisted_token != context.fencing_token:
            raise StaleMutationError(f"fencing token does not own Ray job {remote_id!r} generation {generation}")

    def _submit_job(self, request: _RayJobSubmission, context: MutationContext) -> str:
        """Return an existing fenced submission or submit one new Ray job."""

        existing = self._find_job(request.submission_id)
        if existing is not None:
            self._validate_fence(request.submission_id, existing, context)
            return request.submission_id
        return request.submit(self.client)

    def submit(self, spec: tp.Mapping[str, JsonValue], context: MutationContext) -> str:
        """Submit a fenced placement-group workload through Ray Jobs."""

        entrypoint = spec.get("entrypoint")
        if not isinstance(entrypoint, str) or not entrypoint.strip():
            raise ValueError("Ray Jobs workloads require a non-empty entrypoint")
        bundles, strategy = _gang_contract(spec)
        bundles_json = json.dumps(bundles, sort_keys=True, separators=(",", ":"))
        raw_submission_id = spec.get("submission_id")
        if raw_submission_id is None:
            submission_id = context.idempotency_key
        elif isinstance(raw_submission_id, str) and raw_submission_id.strip():
            submission_id = raw_submission_id.strip()
        else:
            raise ValueError("Ray Jobs submission_id must be a non-empty string")

        backend_options = _optional_json_object(spec.get("backend_options"), "backend_options")
        ray_options = _optional_json_object(backend_options.get("ray_jobs"), "backend_options.ray_jobs")
        runtime_env = _optional_json_object(ray_options.get("runtime_env", spec.get("runtime_env")), "Ray runtime_env")
        environment = _string_mapping(spec.get("environment"), "workload environment")
        env_vars = _string_mapping(runtime_env.get("env_vars"), "Ray runtime_env env_vars")
        runtime_env["env_vars"] = {
            **env_vars,
            **environment,
            "EYVAN_RESOURCE_BUNDLES": bundles_json,
            "EYVAN_PLACEMENT_STRATEGY": strategy,
        }
        metadata = {
            **_string_mapping(spec.get("metadata"), "workload metadata"),
            "eyvan_generation": str(context.generation),
            "eyvan_fencing_token": context.fencing_token,
            "eyvan_resource_bundles": bundles_json,
            "eyvan_placement_strategy": strategy,
        }
        request = _RayJobSubmission(
            entrypoint=_encode_wrapper(entrypoint, bundles, strategy),
            submission_id=submission_id,
            runtime_env=runtime_env,
            metadata=metadata,
        )
        return self._submit_job(request, context)

    def status(self, remote_id: str) -> ExecutionWorkloadObservation:
        """Return the current Ray status or confirmed absence of a submission."""

        job = self._find_job(remote_id)
        if job is None:
            return ExecutionWorkloadObservation(
                remote_id=remote_id,
                status=ExecutionWorkloadStatus.NOT_FOUND,
                message="Ray job is absent",
            )
        return self._observation(remote_id, job)

    def stop(self, remote_id: str, context: MutationContext) -> None:
        """Stop a live Ray job only when this generation owns its fence."""

        job = self._find_job(remote_id)
        if job is None:
            return
        self._validate_fence(remote_id, job, context)
        if self._status(job) in _TERMINAL_JOB_STATES:
            return
        self.client.stop_job(remote_id)

    def retire(self, remote_id: str, context: MutationContext) -> None:
        """Stop a superseded attempt and wait until it cannot consume resources."""

        timeout_s = max(float(os.getenv("EYVAN_SUPERSEDE_WAIT_TIMEOUT_S", "300")), 1)
        poll_s = max(float(os.getenv("EYVAN_SUPERSEDE_POLL_S", "1")), 0.05)
        deadline = time.monotonic() + timeout_s
        job = self._find_job(remote_id)
        if job is None:
            return
        self._validate_fence(remote_id, job, context)
        if self._status(job) not in _TERMINAL_JOB_STATES:
            self.client.stop_job(remote_id)
        while True:
            job = self._find_job(remote_id)
            if job is None:
                return
            self._validate_fence(remote_id, job, context)
            if self._status(job) in _TERMINAL_JOB_STATES:
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(f"superseded Ray job {remote_id!r} did not stop within {timeout_s:g}s")
            time.sleep(poll_s)

    def checkpoint(
        self,
        remote_id: str,
        *,
        hook: str,
        output_uri: str | None,
        context: MutationContext,
    ) -> str:
        """Run an application checkpoint hook after validating source ownership."""

        self._validate_fence(remote_id, self.client.get_job_info(remote_id), context)
        request = _RayJobSubmission(
            entrypoint=hook,
            submission_id=context.idempotency_key,
            runtime_env={
                "env_vars": {
                    "EYVAN_SOURCE_JOB_ID": remote_id,
                    "EYVAN_CHECKPOINT_URI": output_uri or "",
                }
            },
            metadata={
                "eyvan_operation": "checkpoint",
                "eyvan_source_job_id": remote_id,
                "eyvan_generation": str(context.generation),
                "eyvan_fencing_token": context.fencing_token,
            },
        )
        return self._submit_job(request, context)

    def logs(self, remote_id: str) -> str:
        """Return the Ray Jobs log stream for one submission."""

        return self.client.get_job_logs(remote_id)


def main(argv: tp.Sequence[str] | None = None) -> int:
    """Execute the internal placement-group wrapper inside a Ray Job."""

    args = list(argv if argv is not None else sys.argv[1:])
    if len(args) != 2 or args[0] != "run-gang":
        raise SystemExit("usage: python -m eyvan.runtime.ray_jobs run-gang PAYLOAD")
    payload = json.loads(base64.urlsafe_b64decode(args[1]).decode())
    if not is_json_object(payload):
        raise ValueError("invalid Eyvan gang wrapper payload")
    _run_gang(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
