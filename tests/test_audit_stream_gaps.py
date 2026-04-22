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
"""Audit-stream completeness when transactions commit out of sequence order.

``AuditEventRow.sequence`` is a BIGSERIAL allocated at *flush*, while rows become
visible at *commit*. Two overlapping transactions can therefore commit out of
sequence order. A follower that only ever advances ``sequence > cursor`` would
skip the lower one permanently -- and because the in-memory backend assigns
sequences under a single lock, the guarantee was strictly weaker on the
production backend than on the development one, with nothing covering it.
"""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from eyvan.api.app import create_app
from eyvan.config import EyvanSettings
from eyvan.persistence import InMemoryRepository
from eyvan.service import ControlPlaneService
from eyvan.service.models import AuditEvent


def _headers() -> dict[str, str]:
    return {
        "X-Eyvan-User": "alice",
        "X-Eyvan-Org": "acme",
        "X-Eyvan-Project": "research",
        "X-Eyvan-Roles": "operator",
    }


def _event(event_id: str, sequence: int) -> AuditEvent:
    return AuditEvent(
        id=event_id,
        actor="alice",
        org_id="acme",
        project_id="research",
        entity_type="workload",
        entity_id="wl-1",
        action="workload.submitted",
        sequence=sequence,
    )


def _stream(client: TestClient, *, after: int) -> list[dict]:
    response = client.get(
        "/api/v1/events/stream",
        params={"org_id": "acme", "project_id": "research", "after_sequence": after, "follow": False},
        headers=_headers(),
    )
    assert response.status_code == 200
    return [json.loads(block.split("data: ", 1)[1]) for block in response.text.split("\n\n") if "data: " in block]


def _client(repository: InMemoryRepository) -> TestClient:
    return TestClient(
        create_app(
            settings=EyvanSettings(dev_auth_enabled=True, controller_enabled=False),
            repository=repository,
            service=ControlPlaneService(repository),
        )
    )


async def test_a_late_committing_lower_sequence_event_is_still_delivered() -> None:
    """The gap: sequence 100 lands after 101 was already streamed."""

    repository = InMemoryRepository()
    async with repository.transaction() as transaction:
        await transaction.append_event(_event("evt-101", 101))

    with _client(repository) as client:
        first = _stream(client, after=0)
        assert [event["id"] for event in first] == ["evt-101"]

        # The straggler commits now, below the cursor the follower already reached.
        async with repository.transaction() as transaction:
            await transaction.append_event(_event("evt-100", 100))

        second = _stream(client, after=101)

    assert "evt-100" in [event["id"] for event in second], (
        "a follower that only advances its cursor would lose this event permanently"
    )


async def test_lookback_does_not_redeliver_within_one_stream() -> None:
    """Re-scanning behind the cursor must not duplicate what was already sent."""

    repository = InMemoryRepository()
    async with repository.transaction() as transaction:
        for index in range(1, 6):
            await transaction.append_event(_event(f"evt-{index}", index))

    with _client(repository) as client:
        delivered = _stream(client, after=0)

    ids = [event["id"] for event in delivered]
    assert ids == sorted(ids, key=lambda value: int(value.split("-")[1]))
    assert len(ids) == len(set(ids)), "lookback re-reads rows; the follower must dedupe them"
