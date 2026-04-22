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
"""Cell-agent contracts for native multi-cell workload gangs."""

from __future__ import annotations

import json
import typing as tp

import httpx
import pytest
from fastapi.testclient import TestClient

from eyvan._json import JsonObject, JsonValue, is_json_object
from eyvan.agent.app import create_agent_app
from eyvan.agent.client import CellAgentClient
from eyvan.agent.models import (
    GangWorkloadMember,
    GangWorkloadMutationRequest,
    GangWorkloadRequest,
    MutationModel,
    WorkloadRequest,
)
from eyvan.agent.service import CellAgentService, InvalidWorkloadRequestError
from eyvan.config import EyvanSettings
from eyvan.drivers.base import CapacityStatus, CellCapabilities, CellObservation, ExecutionWorkloadStatus
from eyvan.persistence.fleet import FleetStateStore, LocalBackend
from eyvan.runtime.native_gang import NativeGangWorkloadResult
from eyvan.runtime.registry import NativeGangCell

GangCallValue: tp.TypeAlias = JsonObject | str | None


class GangCapacity:
    def capabilities(self):
        return CellCapabilities(
            provider="fake",
            executor="eyvan-native",
            accelerator_types=("tpu",),
            capacity_classes=("spot",),
        )

    def observe(self, cell_id: str):
        return CellObservation(
            cell_id=cell_id,
            state=CapacityStatus.HEALTHY,
            generation=3,
            execution_endpoint=f"http://{cell_id}:8100",
            metadata={"agent_endpoint": "https://agent.example.com"},
        )

    def reconcile(self, cell_id, _context):
        return self.observe(cell_id)

    def release(self, cell_id, _context):
        return self.observe(cell_id)


class GangExecutor:
    def __init__(self, calls: list[tuple[str, GangCallValue]]):
        self.calls = calls

    def submit(self, spec: tp.Mapping[str, JsonValue]) -> str:
        self.calls.append(("submit", dict(spec)))
        submission_id = spec["submission_id"]
        assert isinstance(submission_id, str)
        return submission_id

    def status(self, remote_id: str) -> NativeGangWorkloadResult:
        self.calls.append(("status", remote_id))
        return {
            "remote_id": remote_id,
            "status": ExecutionWorkloadStatus.RUNNING,
            "message": "all ranks running",
            "workers": [],
        }

    def stop(self, remote_id: str) -> None:
        self.calls.append(("stop", remote_id))

    def retire(self, remote_id: str) -> None:
        self.calls.append(("retire", remote_id))

    def logs(self, remote_id: str) -> str:
        self.calls.append(("logs", remote_id))
        return f"logs for {remote_id}"

    def close(self) -> None:
        self.calls.append(("close", None))


class GangResolver:
    def __init__(self) -> None:
        self.cells: list[tuple[NativeGangCell, ...]] = []
        self.calls: list[tuple[str, GangCallValue]] = []

    def resolve_gang(self, cells: tp.Sequence[NativeGangCell]) -> GangExecutor:
        self.cells.append(tuple(cells))
        return GangExecutor(self.calls)


class MismatchedGangExecutor(GangExecutor):
    def submit(self, spec: tp.Mapping[str, JsonValue]) -> str:
        self.calls.append(("submit", dict(spec)))
        return "unexpected-remote-id"


class MismatchedGangResolver(GangResolver):
    def resolve_gang(self, cells: tp.Sequence[NativeGangCell]) -> MismatchedGangExecutor:
        self.cells.append(tuple(cells))
        return MismatchedGangExecutor(self.calls)


def _member(cell_id: str, *, key: str, token: str, endpoint: str = "https://agent.example.com") -> GangWorkloadMember:
    return GangWorkloadMember(
        cell_id=cell_id,
        agent_endpoint=endpoint,
        mutation=MutationModel(idempotency_key=key, generation=7, fencing_token=token),
    )


def _request(*, members: tuple[GangWorkloadMember, ...] | None = None) -> GangWorkloadRequest:
    return GangWorkloadRequest(
        members=members
        or (
            _member("cell-a", key="submit-a", token="fence-a"),
            _member("cell-b", key="submit-b", token="fence-b"),
        ),
        spec={
            "submission_id": "workload-1-a7",
            "metadata": {"eyvan_workload_id": "workload-1"},
            "environment": {},
        },
    )


def _service(tmp_path):
    resolver = GangResolver()
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    service = CellAgentService(
        capacity=GangCapacity(),  # type: ignore[arg-type]
        state=state,
        executors=resolver,  # type: ignore[arg-type]
    )
    return service, state, resolver


def test_native_gang_agent_api_submits_observes_stops_retires_and_logs(tmp_path) -> None:
    service, state, resolver = _service(tmp_path)
    request = _request()
    app = create_agent_app(service, EyvanSettings(agent_shared_secret="agent-secret"))

    with TestClient(app) as client:
        headers = {"Authorization": "Bearer agent-secret"}
        submitted = client.post(
            "/api/v1/cells/cell-a/workload-gangs",
            headers=headers,
            json=request.model_dump(mode="json"),
        )
        assert submitted.status_code == 200
        assert submitted.json() == {
            "remote_id": "workload-1-a7",
            "status": "SUBMITTED",
            "message": "",
            "checkpoint_uri": None,
        }

        status = client.get(
            "/api/v1/cells/cell-a/workload-gangs/workload-1/workload-1-a7",
            headers=headers,
        )
        assert status.status_code == 200
        assert status.json()["status"] == "RUNNING"

        action_members = tuple(
            member.model_copy(
                update={"mutation": member.mutation.model_copy(update={"idempotency_key": f"stop-{member.cell_id}"})}
            )
            for member in request.members
        )
        action = GangWorkloadMutationRequest(members=action_members)
        stopped = client.post(
            "/api/v1/cells/cell-a/workload-gangs/workload-1/workload-1-a7/stop",
            headers=headers,
            json=action.model_dump(mode="json"),
        )
        assert stopped.status_code == 200
        assert stopped.json()["status"] == "STOPPING"

        retired = client.post(
            "/api/v1/cells/cell-a/workload-gangs/workload-1/workload-1-a7/retire",
            headers=headers,
            json=action.model_dump(mode="json"),
        )
        assert retired.status_code == 200
        assert retired.json()["status"] == "STOPPED"

        logs = client.get(
            "/api/v1/cells/cell-a/workload-gangs/workload-1/workload-1-a7/logs",
            headers=headers,
        )
        assert logs.status_code == 200
        assert logs.text == "logs for workload-1-a7"

    fence = state.workload_fence("workload-1")
    assert fence is not None
    assert fence["kind"] == "gang"
    assert fence["coordinator_cell_id"] == "cell-a"
    members = fence["members"]
    assert isinstance(members, list)
    assert all(is_json_object(member) for member in members)
    assert [member["cell_id"] for member in members if is_json_object(member)] == ["cell-a", "cell-b"]
    assert [[cell.cell_id for cell in cells] for cells in resolver.cells] == [
        ["cell-a", "cell-b"],
        ["cell-a", "cell-b"],
        ["cell-a", "cell-b"],
        ["cell-a", "cell-b"],
        ["cell-a", "cell-b"],
    ]
    assert [name for name, _ in resolver.calls] == [
        "submit",
        "close",
        "status",
        "close",
        "stop",
        "close",
        "retire",
        "close",
        "logs",
        "close",
    ]
    stop_cells = resolver.cells[2]
    assert [cell.context.idempotency_key for cell in stop_cells] == ["stop-cell-a", "stop-cell-b"]


def test_native_gang_rejects_cross_agent_members_before_submit(tmp_path) -> None:
    service, _state, resolver = _service(tmp_path)
    app = create_agent_app(service, EyvanSettings(), allow_unauthenticated=True)
    payload = _request().model_dump(mode="json")
    payload["members"][1]["agent_endpoint"] = "https://other-agent.example.com"

    with TestClient(app) as client:
        response = client.post("/api/v1/cells/cell-a/workload-gangs", json=payload)

    assert response.status_code == 422
    assert "same agent_endpoint" in response.text
    assert resolver.cells == []


def test_native_gang_rejects_a_stale_member_fence_without_touching_workers(tmp_path) -> None:
    service, _state, resolver = _service(tmp_path)
    request = _request()
    service.submit_gang("cell-a", request)
    stale_members = (
        request.members[0],
        request.members[1].model_copy(
            update={"mutation": request.members[1].mutation.model_copy(update={"fencing_token": "wrong"})}
        ),
    )

    with pytest.raises(Exception, match="does not own every member fence"):
        service.stop_gang(
            "cell-a",
            "workload-1",
            "workload-1-a7",
            GangWorkloadMutationRequest(members=stale_members),
        )

    assert [name for name, _ in resolver.calls] == ["submit", "close"]


def test_cell_agent_maps_invalid_workload_submissions_to_422(tmp_path) -> None:
    service, _state, _resolver = _service(tmp_path)
    app = create_agent_app(service, EyvanSettings(agent_shared_secret="agent-secret"))
    mutation = {"idempotency_key": "submit-1", "generation": 1, "fencing_token": "cell:1"}

    with TestClient(app) as client:
        headers = {"Authorization": "Bearer agent-secret"}
        missing_workload_id = client.post(
            "/api/v1/cells/cell-a/workloads",
            headers=headers,
            json={"mutation": mutation, "spec": {"submission_id": "workload-1", "metadata": {}}},
        )
        assert missing_workload_id.status_code == 422
        assert "eyvan_workload_id" in missing_workload_id.json()["detail"]

        bad_submission_id = client.post(
            "/api/v1/cells/cell-a/workloads",
            headers=headers,
            json={
                "mutation": mutation,
                "spec": {"submission_id": 123, "metadata": {"eyvan_workload_id": "workload-1"}},
            },
        )
        assert bad_submission_id.status_code == 422
        assert "submission_id" in bad_submission_id.json()["detail"]

    with pytest.raises(InvalidWorkloadRequestError, match="eyvan_workload_id") as excinfo:
        service.submit(
            "cell-a",
            WorkloadRequest(
                mutation=MutationModel(idempotency_key="submit-1", generation=1, fencing_token="cell:1"),
                spec={"submission_id": "workload-1", "metadata": {}},
            ),
        )
    assert isinstance(excinfo.value, ValueError)
    assert excinfo.value.status_code == 422


def test_native_gang_maps_invalid_submissions_to_422(tmp_path) -> None:
    service, _state, resolver = _service(tmp_path)
    app = create_agent_app(service, EyvanSettings(agent_shared_secret="agent-secret"))

    with TestClient(app) as client:
        headers = {"Authorization": "Bearer agent-secret"}
        missing_workload_id = _request().model_dump(mode="json")
        missing_workload_id["spec"]["metadata"] = {}
        response = client.post("/api/v1/cells/cell-a/workload-gangs", headers=headers, json=missing_workload_id)
        assert response.status_code == 422
        assert "eyvan_workload_id" in response.json()["detail"]

        blank_submission_id = _request().model_dump(mode="json")
        blank_submission_id["spec"]["submission_id"] = "  "
        response = client.post("/api/v1/cells/cell-a/workload-gangs", headers=headers, json=blank_submission_id)
        assert response.status_code == 422
        assert "submission_id" in response.json()["detail"]

        response = client.post(
            "/api/v1/cells/cell-b/workload-gangs",
            headers=headers,
            json=_request().model_dump(mode="json"),
        )
        assert response.status_code == 422
        assert "lexicographically first member cell" in response.json()["detail"]

    assert resolver.cells == []


def test_native_gang_retires_a_launched_gang_when_the_remote_id_mismatches(tmp_path) -> None:
    resolver = MismatchedGangResolver()
    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    service = CellAgentService(
        capacity=GangCapacity(),  # type: ignore[arg-type]
        state=state,
        executors=resolver,  # type: ignore[arg-type]
    )

    with pytest.raises(
        RuntimeError,
        match=r"unexpected remote ID 'unexpected-remote-id'.*the launched gang was retired",
    ):
        service.submit_gang("cell-a", _request())

    assert [name for name, _ in resolver.calls] == ["submit", "retire", "close"]
    assert resolver.calls[1] == ("retire", "unexpected-remote-id")


def test_cell_agent_client_uses_group_routes() -> None:
    observed: list[tuple[str, str, JsonObject | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        decoded = json.loads(request.content) if request.content else None
        assert decoded is None or is_json_object(decoded)
        payload = decoded if is_json_object(decoded) else None
        observed.append((request.method, request.url.path, payload))
        if request.method == "POST" and request.url.path == "/api/v1/cells/cell-a/workload-gangs":
            return httpx.Response(
                200,
                json={"remote_id": "workload-1-a7", "status": ExecutionWorkloadStatus.SUBMITTED.value},
            )
        if request.method == "GET":
            return httpx.Response(
                200,
                json={"remote_id": "workload-1-a7", "status": ExecutionWorkloadStatus.RUNNING.value},
            )
        if request.method == "POST" and request.url.path.endswith("/stop"):
            return httpx.Response(
                200,
                json={"remote_id": "workload-1-a7", "status": ExecutionWorkloadStatus.STOPPING.value},
            )
        if request.method == "POST" and request.url.path.endswith("/retire"):
            return httpx.Response(
                200,
                json={"remote_id": "workload-1-a7", "status": ExecutionWorkloadStatus.STOPPED.value},
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    http_client = httpx.Client(base_url="http://agent", transport=httpx.MockTransport(handler))
    client = CellAgentClient("http://agent", client=http_client)
    request = _request()
    action = GangWorkloadMutationRequest(members=request.members)
    try:
        assert client.submit_gang("cell-a", request).status is ExecutionWorkloadStatus.SUBMITTED
        assert client.gang_workload("cell-a", "workload-1", "workload-1-a7").status is ExecutionWorkloadStatus.RUNNING
        assert (
            client.stop_gang("cell-a", "workload-1", "workload-1-a7", action).status is ExecutionWorkloadStatus.STOPPING
        )
        assert (
            client.retire_gang("cell-a", "workload-1", "workload-1-a7", action).status is ExecutionWorkloadStatus.STOPPED
        )
    finally:
        client.close()

    assert [path for _method, path, _payload in observed] == [
        "/api/v1/cells/cell-a/workload-gangs",
        "/api/v1/cells/cell-a/workload-gangs/workload-1/workload-1-a7",
        "/api/v1/cells/cell-a/workload-gangs/workload-1/workload-1-a7/stop",
        "/api/v1/cells/cell-a/workload-gangs/workload-1/workload-1-a7/retire",
    ]
