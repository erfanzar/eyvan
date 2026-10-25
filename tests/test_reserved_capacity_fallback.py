# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""An unnamed TPU capacity class runs on a fleet's reservation; a named one is exact."""

import json

import httpx
import pytest

from eyvan.artifacts import LocalArtifactStore, StoredCallableArtifact
from eyvan.records import CallableExecution, CapacityClass, WorkloadDefinition, WorkloadRecord
from eyvan.sdk.client.callables import EyvanCallableSubmitter
from eyvan.sdk.client.jobs import CallableSubmission, EyvanPlacementError, TpuConfig
from eyvan.sdk.client.sync import EyvanClient
from eyvan.sdk.client.transport import EyvanHTTPError
from eyvan.sdk.job_receipt import JobTerminalOutput

FLEET = "tpu-shared"


def _pool(capacity_class: str, *, fleet: str = FLEET, enabled: bool = True) -> dict[str, object]:
    return {
        "fleet": fleet,
        "pool": f"{fleet}-v5p-128",
        "accelerator_type": "v5p-128",
        "capacity_class": capacity_class,
        "region": "europe-west4",
        "zone": "europe-west4-b",
        "min_cells": 0,
        "max_cells": 1,
        "enabled": enabled,
    }


def _run(tmp_path, config: TpuConfig, pools: list[dict[str, object]]):
    """Submit against a plane that accepts only the classes ``pools`` offer for FLEET."""

    submissions: list[tuple[str, str]] = []
    listings: list[httpx.Request] = []
    notes: list[str] = []
    offered = {pool["capacity_class"] for pool in pools if pool["fleet"] == FLEET and pool["enabled"]}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/capacity":
            listings.append(request)
            return httpx.Response(200, json={"pools": pools})
        assert request.url.path == f"/api/v1/fleets/{FLEET}/callables"
        payload = json.loads(request.content)
        submissions.append((payload["capacity_class"], request.headers["Idempotency-Key"]))
        if payload["capacity_class"] not in offered:
            detail = (
                f"enabled TPU pool for 'v5p-128' in fleet {FLEET!r} does not offer "
                f"{payload['capacity_class']!r} capacity; available: {', '.join(sorted(map(str, offered)))}"
            )
            return httpx.Response(409, json={"detail": detail})
        workload = WorkloadRecord(
            id="submitted",
            org_id="acme",
            project_id="research",
            created_by="alice",
            spec=WorkloadDefinition(name="probe", execution=CallableExecution.model_validate(payload["execution"])),
        )
        return httpx.Response(201, json=workload.model_dump(mode="json"))

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://plane") as raw:
        submitter = EyvanCallableSubmitter(
            EyvanClient(base_url="http://plane", client=raw),
            LocalArtifactStore(tmp_path),
            terminal_output=JobTerminalOutput(writer=notes.append),
        )
        request = CallableSubmission(
            invocation_id="call-1",
            fleet=FLEET,
            org_id="acme",
            project_id="research",
            callable_name="probe",
            tpu=config,
            artifact=StoredCallableArtifact("gs://trusted/probe", "a" * 64, "3.12", 1),
            result_uri="gs://trusted/result",
        )
        try:
            job = submitter.submit_callable(request)
        except EyvanHTTPError as error:
            return error, submissions, listings, notes
    return job, submissions, listings, notes


def test_an_unnamed_class_defaults_to_spot_and_remembers_it_was_unnamed() -> None:
    unnamed = TpuConfig("v5p-128", 1)
    named = TpuConfig("v5p-128", 1, capacity_class=CapacityClass.SPOT)

    assert unnamed.capacity_class is CapacityClass.SPOT
    assert unnamed.capacity_class_explicit is False
    assert named.capacity_class_explicit is True
    assert TpuConfig("v5p-128", capacity_class=CapacityClass.RESERVED).capacity_class_explicit is True
    # Equality is about the request, not how it was spelled.
    assert unnamed == named


def test_an_unnamed_class_runs_on_a_reserved_only_fleet(tmp_path) -> None:
    job, submissions, listings, notes = _run(tmp_path, TpuConfig("v5p-128", 1), [_pool("reserved")])

    assert not isinstance(job, EyvanHTTPError)
    assert job.id == "submitted"
    assert [klass for klass, _ in submissions] == ["spot", "reserved"]
    # The refused key is not replayed with a different class.
    assert submissions[0][1] == "call-1"
    assert submissions[1][1] != submissions[0][1]
    assert len(listings) == 1
    assert listings[0].url.params["accelerator_type"] == "v5p-128"
    assert any("only as reserved" in line and "capacity_class" in line for line in notes)


def test_a_named_spot_request_is_never_substituted(tmp_path) -> None:
    error, submissions, listings, _ = _run(
        tmp_path, TpuConfig("v5p-128", 1, capacity_class=CapacityClass.SPOT), [_pool("reserved")]
    )

    assert isinstance(error, EyvanHTTPError)
    assert error.status_code == 409
    assert [klass for klass, _ in submissions] == ["spot"]
    assert listings == []


@pytest.mark.parametrize(
    "pools",
    [
        # On-demand would start billing for something nobody asked for.
        [_pool("on_demand")],
        # Reserved on another fleet is not this fleet's reservation.
        [_pool("reserved", fleet="elsewhere"), _pool("on_demand")],
        # A disabled reserved pool offers nothing.
        [_pool("reserved", enabled=False), _pool("on_demand")],
    ],
)
def test_an_unnamed_class_is_not_substituted_with_anything_but_this_fleets_reservation(tmp_path, pools) -> None:
    error, submissions, _, _ = _run(tmp_path, TpuConfig("v5p-128", 1), pools)

    assert isinstance(error, EyvanHTTPError)
    assert [klass for klass, _ in submissions] == ["spot"]


def _no_pool(fleet: str) -> httpx.Response:
    return httpx.Response(
        409,
        json={
            "detail": f"fleet {fleet!r} has no enabled pool for 'v5p-128'",
            "code": "no_matching_capacity_pool",
            "context": {
                "fleet": fleet,
                "requested_accelerator_type": "v5p-128",
                "available_enabled_accelerator_types": [],
                "fleets_providing_requested_type": [FLEET],
                "available_fleets": [FLEET],
            },
        },
    )


def _submit_to(tmp_path, fleet: str, pools: list[dict[str, object]]):
    """Submit naming ``fleet``; the plane knows only FLEET and accepts reserved there."""

    posted: list[tuple[str, str]] = []
    notes: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/capacity":
            return httpx.Response(200, json={"pools": pools})
        named = request.url.path.removeprefix("/api/v1/fleets/").removesuffix("/callables")
        posted.append((named, request.headers["Idempotency-Key"]))
        if named != FLEET:
            return _no_pool(named)
        payload = json.loads(request.content)
        workload = WorkloadRecord(
            id="submitted",
            org_id="acme",
            project_id="research",
            created_by="alice",
            spec=WorkloadDefinition(name="probe", execution=CallableExecution.model_validate(payload["execution"])),
        )
        return httpx.Response(201, json=workload.model_dump(mode="json"))

    with httpx.Client(transport=httpx.MockTransport(handler), base_url="http://plane") as raw:
        submitter = EyvanCallableSubmitter(
            EyvanClient(base_url="http://plane", client=raw),
            LocalArtifactStore(tmp_path),
            terminal_output=JobTerminalOutput(writer=notes.append),
        )
        request = CallableSubmission(
            invocation_id="call-1",
            fleet=fleet,
            org_id="acme",
            project_id="research",
            callable_name="probe",
            tpu=TpuConfig("v5p-128", 1, capacity_class=CapacityClass.RESERVED),
            artifact=StoredCallableArtifact("gs://trusted/probe", "a" * 64, "3.12", 1),
            result_uri="gs://trusted/result",
        )
        try:
            return submitter.submit_callable(request), posted, notes
        except EyvanPlacementError as error:
            return error, posted, notes


def test_a_pool_id_named_as_the_fleet_is_submitted_to_its_fleet(tmp_path) -> None:
    pool_id = f"{FLEET}-v5p-128"
    job, posted, notes = _submit_to(tmp_path, pool_id, [_pool("reserved")])

    assert not isinstance(job, EyvanPlacementError)
    assert job.id == "submitted"
    assert [fleet for fleet, _ in posted] == [pool_id, FLEET]
    assert posted[1][1] != posted[0][1]
    assert any(f"is a pool of fleet {FLEET}" in line for line in notes)


@pytest.mark.parametrize(
    "pools",
    [
        # Not a pool id at all.
        [_pool("reserved")],
        # Ambiguous: the same id under two fleets is not guessed between.
        [
            {**_pool("reserved"), "pool": f"{FLEET}-typo"},
            {**_pool("reserved", fleet="other"), "pool": f"{FLEET}-typo"},
        ],
    ],
)
def test_an_unknown_or_ambiguous_name_keeps_the_placement_error(tmp_path, pools) -> None:
    error, posted, _ = _submit_to(tmp_path, f"{FLEET}-typo", pools)

    assert isinstance(error, EyvanPlacementError)
    assert [fleet for fleet, _ in posted] == [f"{FLEET}-typo"]


def test_capacity_class_is_importable_from_the_package_root() -> None:
    import eyvan
    from eyvan import CapacityClass as Exported

    assert Exported is CapacityClass
    assert "CapacityClass" in eyvan.__all__
    assert eyvan.TpuConfig("v5p-8", capacity_class=Exported.RESERVED).capacity_class is CapacityClass.RESERVED


def test_a_spot_fleet_is_submitted_once_as_before(tmp_path) -> None:
    job, submissions, listings, notes = _run(tmp_path, TpuConfig("v5p-128", 1), [_pool("spot"), _pool("reserved")])

    assert not isinstance(job, EyvanHTTPError)
    assert submissions == [("spot", "call-1")]
    assert listings == []
    assert not any("only as reserved" in line for line in notes)
