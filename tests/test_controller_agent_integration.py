from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from eyvan.agent.app import create_agent_app
from eyvan.agent.client import CellAgentClient
from eyvan.agent.service import CellAgentService
from eyvan.config import EyvanSettings
from eyvan.controller import EyvanController
from eyvan.drivers.base import CapacityStatus, CellCapabilities, CellObservation
from eyvan.persistence import InMemoryRepository
from eyvan.persistence.fleet import FleetStateStore, LocalBackend
from eyvan.service import ControlPlaneService
from eyvan.service.models import CellRecord, CellState, OutboxState, Principal, ResourceVector, Role, RoleGrant
from eyvan.service.scheduling import CoreSchedulingService


class _RecordingCapacity:
    def __init__(self) -> None:
        self.applied: list[tuple[str, dict]] = []

    def capabilities(self) -> CellCapabilities:
        return CellCapabilities(
            provider="test",
            executor="native-local",
            accelerator_types=("tpu",),
            capacity_classes=("spot",),
        )

    def observe(self, cell_id: str) -> CellObservation:
        return CellObservation(cell_id=cell_id, state=CapacityStatus.HEALTHY, generation=1)

    def reconcile(self, cell_id: str, context) -> CellObservation:
        return CellObservation(cell_id=cell_id, state=CapacityStatus.HEALTHY, generation=context.generation)

    def apply_capacity(self, cell_id: str, desired, context) -> CellObservation:
        self.applied.append((cell_id, dict(desired)))
        return CellObservation(cell_id=cell_id, state=CapacityStatus.HEALTHY, generation=context.generation)

    def drain(self, cell_id: str, context) -> CellObservation:
        return CellObservation(cell_id=cell_id, state=CapacityStatus.DRAINING, generation=context.generation)

    def release(self, cell_id: str, context) -> CellObservation:
        return CellObservation(cell_id=cell_id, state=CapacityStatus.RELEASED, generation=context.generation)


class _UnusedExecutors:
    def resolve(self, _cell_id, _observation):
        raise AssertionError("this test does not dispatch a workload")


class _BlockingSchedulerController(EyvanController):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.reconciliation_entered = asyncio.Event()
        self.release_reconciliation = asyncio.Event()

    async def reconcile_capacity_pools_once(self) -> dict[str, int]:
        self.reconciliation_entered.set()
        await self.release_reconciliation.wait()
        return {"launched": 0, "drained": 0, "deleted": 0}


@pytest.mark.asyncio
async def test_outbox_delivery_continues_while_leader_reconciliation_is_blocked(tmp_path) -> None:
    """A slow leader cycle must not starve durable agent mutations."""

    secret = "test-secret"
    capacity = _RecordingCapacity()
    fleet_state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))
    agent_settings = EyvanSettings(agent_shared_secret=secret)
    agent_service = CellAgentService(
        capacity=capacity,  # type: ignore[arg-type]
        state=fleet_state,
        executors=_UnusedExecutors(),  # type: ignore[arg-type]
        settings=agent_settings,
    )
    transport = TestClient(
        create_agent_app(agent_service, agent_settings),
        headers={"Authorization": f"Bearer {secret}"},
    )
    agent = CellAgentClient("http://agent", client=transport)

    repository = InMemoryRepository()
    service = ControlPlaneService(repository, scheduler=CoreSchedulingService())
    controller = _BlockingSchedulerController(
        repository,
        service,
        settings=EyvanSettings(scheduler_interval_s=0.01, agent_shared_secret=secret),
        agent_factory=lambda _cell: agent,  # type: ignore[arg-type]
    )
    admin = Principal(subject="admin", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    await service.upsert_cell(
        admin,
        CellRecord(
            id="cell-live",
            provider="gcp",
            provider_project="project-a",
            region="us-east5",
            zone="us-east5-a",
            capacity=ResourceVector(cpu=0, memory_gib=0, tpu=4),
            allocatable=ResourceVector(cpu=0, memory_gib=0, tpu=4),
            accelerator_type="v5p-8",
            capacity_class="spot",
            endpoint="http://agent",
        ),
        idempotency_key="cell-live",
    )

    stop = asyncio.Event()
    task = asyncio.create_task(controller.run(stop))
    try:
        await asyncio.wait_for(controller.reconciliation_entered.wait(), timeout=1)

        async def wait_for_capacity_update() -> None:
            while True:
                async with repository.transaction() as transaction:
                    cell = await transaction.get_cell("cell-live")
                    commands = await transaction.list_outbox()
                if cell is not None and cell.state is CellState.READY and commands[0].state is OutboxState.SUCCEEDED:
                    return
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_capacity_update(), timeout=1)
    finally:
        controller.release_reconciliation.set()
        stop.set()
        await task
        await controller.close()
        agent.close()

    assert capacity.applied == [
        (
            "cell-live",
            {
                "cell_id": "cell-live",
                "provider": "gcp",
                "project": "project-a",
                "region": "us-east5",
                "zone": "us-east5-a",
                "accelerator_type": "v5p-8",
                "capacity_class": "spot",
                "executor": "eyvan-native",
                "agent_endpoint": "http://agent",
                "provider_options": {},
                "runtime": {"env": {}, "init_commands": [], "artifacts": [], "vm_image": None, "container": None},
            },
        )
    ]
