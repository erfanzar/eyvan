# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Contracts for native execution across independently fenced TPU cells."""

from __future__ import annotations

import json
import typing as tp

import httpx
import pytest

from eyvan._internal.json_types import JsonObject, JsonValue, is_json_object
from eyvan.capacity.drivers.base import ExecutionWorkloadStatus, MutationContext
from eyvan.worker.runtime.native_gang import NativeGangExecutor, NativeGangMember


def _context(*, key: str, generation: int, token: str) -> MutationContext:
    return MutationContext(idempotency_key=key, generation=generation, fencing_token=token)


def _client(endpoint: str, handler: tp.Callable[[httpx.Request], httpx.Response]) -> httpx.Client:
    return httpx.Client(base_url=endpoint, transport=httpx.MockTransport(handler))


def _json_object(value: JsonValue) -> JsonObject:
    assert is_json_object(value)
    return value


def _json_string(value: JsonValue) -> str:
    assert isinstance(value, str)
    return value


def _payload(request: httpx.Request) -> JsonObject:
    value = json.loads(request.content)
    assert is_json_object(value)
    return value


def _spec(*, submission_id: str = "gang-job", environment: dict[str, str] | None = None) -> JsonObject:
    environment_document: JsonObject = dict(environment or {})
    document: JsonObject = {
        "entrypoint": "python train.py --steps 4",
        "submission_id": submission_id,
        "environment": environment_document,
        "metadata": {"eyvan_workload_id": submission_id},
    }
    return document


def _members() -> tuple[NativeGangMember, NativeGangMember]:
    return (
        NativeGangMember(
            cell_id="cell-b",
            endpoints=("http://10.12.0.20:8100", "http://10.12.0.21:8100"),
            secret="worker-secret-b",
            context=_context(key="submit-b", generation=7, token="fence-b"),
        ),
        NativeGangMember(
            cell_id="cell-a",
            endpoints=("http://10.12.0.10:8100", "http://10.12.0.11:8100"),
            secret="worker-secret-a",
            context=_context(key="submit-a", generation=7, token="fence-a"),
        ),
    )


def test_native_gang_submit_uses_global_ranks_and_per_cell_credentials() -> None:
    submissions: list[tuple[str, httpx.Request, JsonObject]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/api/v1/workloads"
        submissions.append((request.url.host, request, _payload(request)))
        return httpx.Response(200, json={"remote_id": "gang-job"})

    endpoints = (
        "http://10.12.0.10:8100",
        "http://10.12.0.11:8100",
        "http://10.12.0.20:8100",
        "http://10.12.0.21:8100",
    )
    clients = [_client(endpoint, handler) for endpoint in endpoints]
    original_environment = {"USER_FLAG": "kept", "PYTHONPATH": "/workspace"}
    workload = _spec(environment=original_environment)
    try:
        executor = NativeGangExecutor(_members(), clients=clients)

        assert executor.submit(workload) == "gang-job"

        assert workload["environment"] == original_environment
        assert [host for host, _, _ in submissions] == ["10.12.0.10", "10.12.0.11", "10.12.0.20", "10.12.0.21"]
        coordinator_addresses = {
            _json_string(_json_object(_json_object(payload["spec"])["environment"])["JAX_COORDINATOR_ADDRESS"])
            for _, _, payload in submissions
        }
        assert coordinator_addresses == {"10.12.0.10:12345"}
        for rank, (host, request, payload) in enumerate(submissions):
            expected_cell = "cell-a" if host.startswith("10.12.0.1") else "cell-b"
            expected_secret = "worker-secret-a" if expected_cell == "cell-a" else "worker-secret-b"
            expected_mutation = {
                "idempotency_key": "submit-a" if expected_cell == "cell-a" else "submit-b",
                "generation": 7,
                "fencing_token": "fence-a" if expected_cell == "cell-a" else "fence-b",
            }
            assert request.headers["Authorization"] == f"Bearer {expected_secret}"
            assert payload["mutation"] == expected_mutation
            environment = _json_object(_json_object(payload["spec"])["environment"])
            assert environment["USER_FLAG"] == "kept"
            assert environment["EYVAN_DISTRIBUTED_PROCESS_COUNT"] == "4"
            assert environment["EYVAN_DISTRIBUTED_PROCESS_ID"] == str(rank)
            assert environment["EYVAN_REPLICA_COUNT"] == "4"
            assert environment["EYVAN_REPLICA_INDEX"] == str(rank)
            assert environment["JAX_PROCESS_COUNT"] == "4"
            assert environment["JAX_PROCESS_ID"] == str(rank)
            assert environment["JAX_PROCESS_INDEX"] == str(rank)
            assert environment["JAX_PARTITION_INDEX"] == ("0" if expected_cell == "cell-a" else "1")
            assert environment["JAX_COORDINATOR_PORT"] == "12345"
            assert environment["EYVAN_GANG_CELL_ID"] == expected_cell
            assert environment["EYVAN_GANG_CELL_INDEX"] == ("0" if expected_cell == "cell-a" else "1")
            assert environment["PYTHONPATH"].endswith("/workspace")
            assert environment["PYTHONPATH"] != "/workspace"
    finally:
        for client in clients:
            client.close()


def test_native_gang_retires_every_attempted_worker_with_its_cell_fence() -> None:
    calls: list[tuple[str, str, httpx.Request]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        calls.append((host, request.url.path, request))
        if request.url.path == "/api/v1/workloads":
            if host == "10.12.0.20":
                # A server-side failure can be ambiguous: the worker might
                # have persisted the launch before its reply reached Eyvan.
                return httpx.Response(503, json={"detail": "worker unavailable"})
            return httpx.Response(200, json={"remote_id": "gang-partial"})
        assert request.url.path == "/api/v1/workloads/gang-partial/retire"
        assert request.method == "POST"
        return httpx.Response(200, json={})

    endpoints = (
        "http://10.12.0.10:8100",
        "http://10.12.0.11:8100",
        "http://10.12.0.20:8100",
        "http://10.12.0.21:8100",
    )
    clients = [_client(endpoint, handler) for endpoint in endpoints]
    try:
        executor = NativeGangExecutor(_members(), clients=clients)

        with pytest.raises(httpx.HTTPStatusError):
            executor.submit(_spec(submission_id="gang-partial"))

        assert [(host, path) for host, path, _ in calls] == [
            ("10.12.0.10", "/api/v1/workloads"),
            ("10.12.0.11", "/api/v1/workloads"),
            ("10.12.0.20", "/api/v1/workloads"),
            ("10.12.0.20", "/api/v1/workloads/gang-partial/retire"),
            ("10.12.0.11", "/api/v1/workloads/gang-partial/retire"),
            ("10.12.0.10", "/api/v1/workloads/gang-partial/retire"),
        ]
        for host, path, request in calls:
            expected_cell = "cell-a" if host.startswith("10.12.0.1") else "cell-b"
            expected_secret = "worker-secret-a" if expected_cell == "cell-a" else "worker-secret-b"
            expected_mutation = {
                "idempotency_key": "submit-a" if expected_cell == "cell-a" else "submit-b",
                "generation": 7,
                "fencing_token": "fence-a" if expected_cell == "cell-a" else "fence-b",
            }
            assert request.headers["Authorization"] == f"Bearer {expected_secret}"
            if path.endswith("/retire"):
                assert _payload(request)["mutation"] == expected_mutation
    finally:
        for client in clients:
            client.close()


def test_native_gang_surfaces_retire_failures_and_still_retires_remaining_ranks() -> None:
    retire_attempts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if request.url.path == "/api/v1/workloads":
            if host == "10.12.0.21":
                return httpx.Response(503, json={"detail": "worker unavailable"})
            return httpx.Response(200, json={"remote_id": "gang-partial"})
        assert request.url.path == "/api/v1/workloads/gang-partial/retire"
        retire_attempts.append(host)
        if host == "10.12.0.20":
            return httpx.Response(500, json={"detail": "retire exploded"})
        return httpx.Response(200, json={})

    endpoints = (
        "http://10.12.0.10:8100",
        "http://10.12.0.11:8100",
        "http://10.12.0.20:8100",
        "http://10.12.0.21:8100",
    )
    clients = [_client(endpoint, handler) for endpoint in endpoints]
    try:
        executor = NativeGangExecutor(_members(), clients=clients)

        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            executor.submit(_spec(submission_id="gang-partial"))

        message = str(excinfo.value)
        assert "partial-launch cleanup could not retire" in message
        assert "rank 2 (http://10.12.0.20:8100): HTTPStatusError" in message
        # One failing retire must not stop cleanup of the other attempted ranks.
        assert retire_attempts == ["10.12.0.21", "10.12.0.20", "10.12.0.11", "10.12.0.10"]
    finally:
        for client in clients:
            client.close()


def test_native_gang_strips_caller_distributed_topology_overrides() -> None:
    submissions: list[JsonObject] = []

    def handler(request: httpx.Request) -> httpx.Response:
        submissions.append(_payload(request))
        return httpx.Response(200, json={"remote_id": "gang-job"})

    endpoints = (
        "http://10.12.0.10:8100",
        "http://10.12.0.11:8100",
        "http://10.12.0.20:8100",
        "http://10.12.0.21:8100",
    )
    clients = [_client(endpoint, handler) for endpoint in endpoints]
    workload = _spec(
        environment={
            "GCE_METADATA_IP": "attacker.invalid",
            "JAX_COORDINATOR_ADDRESS": "stale:12345",
            "JAX_COORDINATOR_BIND_ADDRESS": "127.0.0.1:12345",
            "JAX_LOCAL_DEVICE_IDS": "0",
            "JAX_PARTITION_INDEX": "9",
            "JAX_PROCESS_COUNT": "99",
            "JAX_PROCESS_ID": "98",
            "JAX_SLICE_INDEX": "9",
            "MEGASCALE_NUM_SLICES": "99",
            "TPU_SKIP_MDS_QUERY": "1",
            "USER_FLAG": "kept",
        }
    )
    try:
        executor = NativeGangExecutor(_members(), clients=clients)

        assert executor.submit(workload) == "gang-job"

        assert len(submissions) == 4
        for payload in submissions:
            environment = _json_object(_json_object(payload["spec"])["environment"])
            assert environment["USER_FLAG"] == "kept"
            assert environment["JAX_COORDINATOR_ADDRESS"] == "10.12.0.10:12345"
            assert environment["JAX_PROCESS_COUNT"] == "4"
            assert environment["JAX_PROCESS_ID"] != "98"
            assert environment["JAX_PARTITION_INDEX"] != "9"
            assert "GCE_METADATA_IP" not in environment
            assert "JAX_COORDINATOR_BIND_ADDRESS" not in environment
            assert "JAX_LOCAL_DEVICE_IDS" not in environment
            assert "JAX_SLICE_INDEX" not in environment
            assert "MEGASCALE_NUM_SLICES" not in environment
            assert "TPU_SKIP_MDS_QUERY" not in environment
    finally:
        for client in clients:
            client.close()


def test_native_gang_aggregates_status_and_rejects_uncoordinated_checkpoints() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/workloads/gang-job":
            state = "RUNNING" if request.url.host == "10.12.0.10" else "SUCCEEDED"
            return httpx.Response(200, json={"remote_id": "gang-job", "status": state})
        if request.url.path == "/api/v1/workloads/gang-job/logs":
            return httpx.Response(200, text=f"logs from {request.url.host}")
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    endpoints = (
        "http://10.12.0.10:8100",
        "http://10.12.0.11:8100",
        "http://10.12.0.20:8100",
        "http://10.12.0.21:8100",
    )
    clients = [_client(endpoint, handler) for endpoint in endpoints]
    try:
        executor = NativeGangExecutor(_members(), clients=clients)

        report = executor.status("gang-job")

        assert report["status"] is ExecutionWorkloadStatus.RUNNING
        assert [worker["rank"] for worker in report["workers"]] == [0, 1, 2, 3]
        assert [worker["cell_id"] for worker in report["workers"]] == ["cell-a", "cell-a", "cell-b", "cell-b"]
        assert report["workers"][0]["status"] is ExecutionWorkloadStatus.RUNNING
        assert all(worker["status"] is ExecutionWorkloadStatus.SUCCEEDED for worker in report["workers"][1:])
        logs = executor.logs("gang-job")
        assert "rank 0 cell cell-a" in logs
        assert "rank 3 cell cell-b" in logs
        with pytest.raises(NotImplementedError, match="multi-cell checkpoint"):
            executor.checkpoint(
                "gang-job",
                hook="python checkpoint.py",
                output_uri="gs://bucket/checkpoint",
                context=_context(key="checkpoint", generation=1, token="cell-a-token"),
            )
    finally:
        for client in clients:
            client.close()


def test_native_gang_marks_a_malformed_worker_observation_indeterminate() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path != "/api/v1/workloads/gang-job":
            raise AssertionError(f"unexpected request: {request.method} {request.url.path}")
        if request.url.host == "10.12.0.20":
            return httpx.Response(200, json={"remote_id": "gang-job", "message": "status was omitted"})
        return httpx.Response(200, json={"remote_id": "gang-job", "status": "RUNNING"})

    endpoints = (
        "http://10.12.0.10:8100",
        "http://10.12.0.11:8100",
        "http://10.12.0.20:8100",
        "http://10.12.0.21:8100",
    )
    clients = [_client(endpoint, handler) for endpoint in endpoints]
    try:
        report = NativeGangExecutor(_members(), clients=clients).status("gang-job")

        assert report["status"] is ExecutionWorkloadStatus.INDETERMINATE
        assert report["workers"][2]["status"] is ExecutionWorkloadStatus.INDETERMINATE
        assert "omitted status" in report["workers"][2]["message"]
    finally:
        for client in clients:
            client.close()


def test_native_gang_reports_a_transport_loss_as_lost() -> None:
    def healthy_worker(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"remote_id": "gang-job", "status": "RUNNING"})

    def unreachable_worker(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    endpoints = (
        "http://10.12.0.10:8100",
        "http://10.12.0.11:8100",
        "http://10.12.0.20:8100",
        "http://10.12.0.21:8100",
    )
    clients = [
        _client(endpoints[0], healthy_worker),
        _client(endpoints[1], healthy_worker),
        _client(endpoints[2], unreachable_worker),
        _client(endpoints[3], healthy_worker),
    ]
    try:
        report = NativeGangExecutor(_members(), clients=clients).status("gang-job")

        assert report["status"] is ExecutionWorkloadStatus.LOST
        assert report["workers"][2]["status"] is ExecutionWorkloadStatus.LOST
        assert "connection refused" in report["workers"][2]["message"]
    finally:
        for client in clients:
            client.close()
