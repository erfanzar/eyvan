# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The credential a running job would need to submit its own children.

In-job submission has no credential at all today: ``GET /workloads/{id}/children``
exists with no ``POST`` counterpart, so a launcher that wants to fan out has to
be handed a human's API token -- scoped to everything that human can do, for as
long as they can do it.

This is the token layer for the real thing. Two properties carry the weight.

**Audience separation.** A stolen artifact token must not verify as a submit
capability even if a claim check were buggy, because the version prefix is
inside the signed material rather than merely inspected afterwards.

**Revocation without expiry.** The capability is non-expiring on purpose -- an
expiring one would inherit the seven-day artifact TTL and break exactly the
long-running jobs that fan out -- and is revoked by the generation instead. A
retry or a preemption resume bumps it, killing a leaked attempt-1 token the
instant attempt 2 exists.
"""

from __future__ import annotations

import pytest

from eyvan.artifacts._capability_tokens import InvalidCapabilityToken, encode_segment, sign_claims
from eyvan.artifacts.managed import ArtifactAction, ArtifactTokenSigner, InvalidArtifactToken, result_uri
from eyvan.artifacts.workload_capability import (
    SUBMIT_CHILDREN,
    InvalidWorkloadToken,
    WorkloadCapabilitySigner,
)

SECRET = b"k" * 32

#: A token minted by ``ArtifactTokenSigner`` *before* the shared primitives were
#: extracted. Frozen here because a refactor of a signing path is exactly where
#: a silent wire change hides: every test would still pass while every token
#: already issued to a running job stopped working.
FROZEN_ARTIFACT_TOKEN = (
    "v1.eyJhY3Rpb24iOiJ3cml0ZSIsImV4cGlyZXNfYXQiOm51bGwsImlzc3VlZF9hdCI6MTc4NTc5OTIyMywia2luZCI6InJlc3VsdHMiLCJvY"
    "mplY3RfaWQiOiJ3bC0xLWcxIiwib3JnX2lkIjoiYWNtZSIsInByb2plY3RfaWQiOiJyZXNlYXJjaCIsInZlcnNpb24iOjEsIndvcmtsb2FkX2"
    "lkIjoid2wtMSJ9.N1QUSVxrOi6bAzI6E4HFwXcW-PxAm0Jlh5Wb-VecRsU"
)


def _signer(**kwargs) -> WorkloadCapabilitySigner:
    return WorkloadCapabilitySigner(SECRET, **kwargs)


def _artifact_signer() -> ArtifactTokenSigner:
    return ArtifactTokenSigner(SECRET, default_ttl_seconds=3600, max_ttl_seconds=3600)


# --------------------------------------------------------------------------- the refactor


def test_a_token_issued_before_the_refactor_still_verifies() -> None:
    """Extracting a signing path must not change one byte of the wire format.

    Tokens are already live inside running jobs. A refactor that re-encoded them
    would pass every behavioural test -- issue and verify would still agree with
    each other -- and break every credential already in flight.
    """

    claims = _artifact_signer().verify(
        FROZEN_ARTIFACT_TOKEN,
        ArtifactAction.WRITE,
        result_uri("wl-1-g1"),
        org_id="acme",
        project_id="research",
        workload_id="wl-1",
    )

    assert claims.action is ArtifactAction.WRITE
    assert claims.workload_id == "wl-1"


def test_the_artifact_signer_still_rejects_a_tampered_token() -> None:
    """The refactor must not have loosened verification while preserving the format."""

    tampered = FROZEN_ARTIFACT_TOKEN[:-4] + "AAAA"

    with pytest.raises(InvalidArtifactToken):
        _artifact_signer().verify(
            tampered,
            ArtifactAction.WRITE,
            result_uri("wl-1-g1"),
            org_id="acme",
            project_id="research",
            workload_id="wl-1",
        )


# --------------------------------------------------------------------------- audience


def test_an_artifact_token_does_not_verify_as_a_submit_capability() -> None:
    """The reason the version prefix lives inside the signed material.

    Both capabilities are HMACed with the same deployment secret. If the prefix
    were merely a label checked after verification, a bug in one claim check
    would let a token minted to upload a result authorize submitting workloads.
    """

    with pytest.raises(InvalidCapabilityToken):
        _signer().verify(FROZEN_ARTIFACT_TOKEN, workload_id="wl-1", generation=1)


def test_a_submit_capability_does_not_verify_as_an_artifact_token() -> None:
    """The same separation in the other direction."""

    capability = _signer().issue(workload_id="wl-1", org_id="acme", project_id="research", generation=1)

    with pytest.raises(InvalidArtifactToken):
        _artifact_signer().verify(
            capability,
            ArtifactAction.WRITE,
            result_uri("wl-1-g1"),
            org_id="acme",
            project_id="research",
            workload_id="wl-1",
        )


# --------------------------------------------------------------------------- issuing


def test_a_capability_verifies_for_the_workload_and_generation_it_names() -> None:
    """The happy path everything else here is a deviation from."""

    capability = _signer().issue(workload_id="wl-1", org_id="acme", project_id="research", generation=3)

    claims = _signer().verify(capability, workload_id="wl-1", generation=3)

    assert claims.workload_id == "wl-1"
    assert claims.action == SUBMIT_CHILDREN
    assert (claims.org_id, claims.project_id) == ("acme", "research")


def test_a_capability_for_another_workload_is_refused() -> None:
    """Otherwise any job in the fleet could submit as any other."""

    capability = _signer().issue(workload_id="wl-1", org_id="acme", project_id="research", generation=1)

    with pytest.raises(InvalidWorkloadToken, match="different workload"):
        _signer().verify(capability, workload_id="wl-2", generation=1)


def test_a_superseded_attempts_capability_is_refused() -> None:
    """The revocation, and the reason no expiry is needed.

    A retry or a preemption resume bumps the generation, so a token leaked from
    attempt 1 stops working the instant attempt 2 exists -- with no revocation
    list to maintain and nothing to expire.
    """

    capability = _signer().issue(workload_id="wl-1", org_id="acme", project_id="research", generation=1)

    with pytest.raises(InvalidWorkloadToken, match="superseded"):
        _signer().verify(capability, workload_id="wl-1", generation=2)


def test_a_capability_signed_with_another_secret_is_refused() -> None:
    """The check that makes every other check meaningful."""

    other = WorkloadCapabilitySigner(b"z" * 32)
    capability = other.issue(workload_id="wl-1", org_id="acme", project_id="research", generation=1)

    with pytest.raises(InvalidCapabilityToken):
        _signer().verify(capability, workload_id="wl-1", generation=1)


def test_a_short_secret_is_refused_at_construction() -> None:
    """A signing key too short to be one is a configuration error, not a runtime one."""

    with pytest.raises(ValueError, match="at least 32 bytes"):
        WorkloadCapabilitySigner(b"tooshort")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"workload_id": "", "org_id": "acme", "project_id": "research", "generation": 1},
        {"workload_id": "wl-1", "org_id": "", "project_id": "research", "generation": 1},
        {"workload_id": "wl-1", "org_id": "acme", "project_id": "research", "generation": 0},
        {"workload_id": "wl-1", "org_id": "acme", "project_id": "research", "generation": True},
    ],
    ids=["no-workload", "no-org", "zero-generation", "bool-generation"],
)
def test_an_unscoped_capability_cannot_be_issued(kwargs: dict) -> None:
    """A capability missing its scope would verify against whatever it was asked about."""

    with pytest.raises(ValueError):
        _signer().issue(**kwargs)


# --------------------------------------------------------------------------- claim shape


def test_a_capability_carrying_an_expiry_is_refused() -> None:
    """These do not expire, and a token claiming otherwise is not one of ours.

    Honoring an expiry here would mean two revocation models -- one that a
    caller can influence by choosing a claim -- for the same capability.
    """

    forged = sign_claims(
        SECRET,
        "eyvw1",
        {
            "action": SUBMIT_CHILDREN,
            "expires_at": 99_999_999_999,
            "generation": 1,
            "issued_at": 0,
            "org_id": "acme",
            "project_id": "research",
            "version": 1,
            "workload_id": "wl-1",
        },
    )

    with pytest.raises(InvalidWorkloadToken, match="do not expire"):
        _signer().verify(forged, workload_id="wl-1", generation=1)


def test_an_unknown_claim_is_refused_rather_than_ignored() -> None:
    """A token this build only partly understands is how a capability widens.

    Honoring the claims it recognizes and ignoring the rest means a future
    narrowing claim -- a scope, a rank restriction -- is silently dropped by
    every older replica during a rolling upgrade.
    """

    forged = sign_claims(
        SECRET,
        "eyvw1",
        {
            "action": SUBMIT_CHILDREN,
            "expires_at": None,
            "generation": 1,
            "issued_at": 0,
            "org_id": "acme",
            "project_id": "research",
            "version": 1,
            "workload_id": "wl-1",
            "only_rank": 0,
        },
    )

    with pytest.raises(InvalidWorkloadToken, match="unexpected claims"):
        _signer().verify(forged, workload_id="wl-1", generation=1)


def test_a_capability_granting_another_action_is_refused() -> None:
    """The action is named so a second one cannot arrive by accident."""

    forged = sign_claims(
        SECRET,
        "eyvw1",
        {
            "action": "do_anything",
            "expires_at": None,
            "generation": 1,
            "issued_at": 0,
            "org_id": "acme",
            "project_id": "research",
            "version": 1,
            "workload_id": "wl-1",
        },
    )

    with pytest.raises(InvalidWorkloadToken, match="does not grant"):
        _signer().verify(forged, workload_id="wl-1", generation=1)


def test_a_malformed_token_is_refused_without_leaking_why() -> None:
    """Every rejection reads the same to a caller probing the endpoint."""

    for token in ("", "not-a-token", "eyvw1.notbase64.sig", f"eyvw1.{encode_segment(b'[]')}.sig"):
        with pytest.raises(InvalidCapabilityToken):
            _signer().verify(token, workload_id="wl-1", generation=1)


def test_claims_are_canonically_ordered_before_signing() -> None:
    """The signature covers exact bytes, so key order is part of the format.

    Every claim literal in this file already happens to be alphabetical, so
    dropping ``sort_keys`` changes nothing about them -- and the frozen vector
    stays green. The failure it would hide arrives the first time a caller
    builds claims in a different order: two encoders disagree, and a token
    signed by one verifies nowhere.
    """

    ordered = {"action": SUBMIT_CHILDREN, "workload_id": "wl-1", "zzz": 1}
    shuffled = {"zzz": 1, "workload_id": "wl-1", "action": SUBMIT_CHILDREN}

    assert sign_claims(SECRET, "eyvw1", ordered) == sign_claims(SECRET, "eyvw1", shuffled)


def test_a_verified_capability_round_trips_through_the_shared_encoding() -> None:
    """Issue and verify must agree across the extraction, not merely inside it."""

    from eyvan.artifacts._capability_tokens import verify_signature

    capability = _signer().issue(workload_id="wl-9", org_id="acme", project_id="research", generation=7)
    raw = verify_signature(SECRET, "eyvw1", capability)

    assert raw["workload_id"] == "wl-9"
    assert raw["generation"] == 7
    assert raw["expires_at"] is None


# --------------------------------------------------------------------------- the route


def _plane(tmp_path):
    from fastapi.testclient import TestClient

    from eyvan.config import EyvanSettings
    from eyvan.server.api.app import create_app

    app = create_app(
        settings=EyvanSettings(
            dev_auth_enabled=True,
            artifact_token_key="k" * 32,
            artifact_store_uri=str(tmp_path / "artifacts"),
        )
    )
    return TestClient(app), app


def _submit_parent(client, *, can_submit_children: bool = True) -> dict:
    response = client.post(
        "/api/v1/workloads",
        json={
            "org_id": "acme",
            "project_id": "research",
            "spec": {
                "name": "launcher",
                "execution": {"kind": "command", "command": "python launch.py"},
                "can_submit_children": can_submit_children,
            },
        },
        headers={
            "X-Eyvan-User": "alice",
            "X-Eyvan-Roles": "operator",
            "X-Eyvan-Org": "acme",
            "X-Eyvan-Project": "research",
            "Idempotency-Key": "parent",
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def _child_spec() -> dict:
    return {"spec": {"name": "step", "execution": {"kind": "command", "command": "python step.py"}}}


def test_a_running_job_submits_a_child_on_its_own_authority(tmp_path) -> None:
    """The capability that did not exist: fanning out without a human's token."""

    client, app = _plane(tmp_path)
    parent = _submit_parent(client)
    capability = app.state.workload_capability.issue(
        workload_id=parent["id"], org_id="acme", project_id="research", generation=parent["generation"]
    )

    response = client.post(
        f"/api/v1/workloads/{parent['id']}/children",
        json=_child_spec(),
        headers={"X-Eyvan-Workload-Token": capability, "Idempotency-Key": "step-1"},
    )

    assert response.status_code == 201, response.text
    assert response.json()["parent_workload_id"] == parent["id"]


def test_a_child_belongs_to_its_parents_owner_not_to_the_workload(tmp_path) -> None:
    """The detail that decides whether fan-out escapes quota.

    ``QuotaRecord`` supports a per-principal scope and fair share leaves on
    ``created_by``. A synthetic ``workload:wl-x`` subject would create a fresh
    leaf and a fresh quota scope, so a launcher could outrun the limit its owner
    is subject to simply by submitting children.
    """

    client, app = _plane(tmp_path)
    parent = _submit_parent(client)
    capability = app.state.workload_capability.issue(
        workload_id=parent["id"], org_id="acme", project_id="research", generation=parent["generation"]
    )

    child = client.post(
        f"/api/v1/workloads/{parent['id']}/children",
        json=_child_spec(),
        headers={"X-Eyvan-Workload-Token": capability, "Idempotency-Key": "step-1"},
    ).json()

    assert child["created_by"] == parent["created_by"] == "alice"


def test_a_workload_without_the_opt_in_cannot_submit(tmp_path) -> None:
    """A job that does not fan out should not hold a usable credential."""

    client, app = _plane(tmp_path)
    parent = _submit_parent(client, can_submit_children=False)
    capability = app.state.workload_capability.issue(
        workload_id=parent["id"], org_id="acme", project_id="research", generation=parent["generation"]
    )

    response = client.post(
        f"/api/v1/workloads/{parent['id']}/children",
        json=_child_spec(),
        headers={"X-Eyvan-Workload-Token": capability, "Idempotency-Key": "step-1"},
    )

    assert response.status_code == 403


def test_a_capability_from_a_superseded_attempt_is_refused_at_the_route(tmp_path) -> None:
    """The revocation, end to end rather than at the signer alone."""

    client, app = _plane(tmp_path)
    parent = _submit_parent(client)
    stale = app.state.workload_capability.issue(
        workload_id=parent["id"], org_id="acme", project_id="research", generation=parent["generation"] + 1
    )

    response = client.post(
        f"/api/v1/workloads/{parent['id']}/children",
        json=_child_spec(),
        headers={"X-Eyvan-Workload-Token": stale, "Idempotency-Key": "step-1"},
    )

    assert response.status_code == 403


def test_one_jobs_capability_cannot_submit_under_another(tmp_path) -> None:
    """Otherwise any running job could place work in any other job's tree."""

    client, app = _plane(tmp_path)
    parent = _submit_parent(client)
    other = app.state.workload_capability.issue(
        workload_id="wl-someone-else", org_id="acme", project_id="research", generation=1
    )

    response = client.post(
        f"/api/v1/workloads/{parent['id']}/children",
        json=_child_spec(),
        headers={"X-Eyvan-Workload-Token": other, "Idempotency-Key": "step-1"},
    )

    assert response.status_code == 403


def test_sibling_launchers_do_not_collide_on_a_natural_key(tmp_path) -> None:
    """A launcher's natural key is a step number, and every launcher has step 1.

    Two parents both submitting "step-1" must get two children. Without scoping
    the key to the parent, the second call replays the first one's record and
    returns a handle to a job the caller never submitted.
    """

    client, app = _plane(tmp_path)
    first = _submit_parent(client)
    second = client.post(
        "/api/v1/workloads",
        json={
            "org_id": "acme",
            "project_id": "research",
            "spec": {
                "name": "launcher-2",
                "execution": {"kind": "command", "command": "python launch.py"},
                "can_submit_children": True,
            },
        },
        headers={
            "X-Eyvan-User": "alice",
            "X-Eyvan-Roles": "operator",
            "X-Eyvan-Org": "acme",
            "X-Eyvan-Project": "research",
            "Idempotency-Key": "parent-2",
        },
    ).json()

    children = []
    for parent in (first, second):
        capability = app.state.workload_capability.issue(
            workload_id=parent["id"], org_id="acme", project_id="research", generation=parent["generation"]
        )
        response = client.post(
            f"/api/v1/workloads/{parent['id']}/children",
            json=_child_spec(),
            headers={"X-Eyvan-Workload-Token": capability, "Idempotency-Key": "step-1"},
        )
        assert response.status_code == 201, response.text
        children.append(response.json())

    assert children[0]["id"] != children[1]["id"]
    assert children[0]["parent_workload_id"] != children[1]["parent_workload_id"]


def test_a_retried_submission_returns_the_same_child(tmp_path) -> None:
    """A launcher that crashes mid-fan-out re-runs its loop."""

    client, app = _plane(tmp_path)
    parent = _submit_parent(client)
    capability = app.state.workload_capability.issue(
        workload_id=parent["id"], org_id="acme", project_id="research", generation=parent["generation"]
    )
    headers = {"X-Eyvan-Workload-Token": capability, "Idempotency-Key": "step-1"}

    first = client.post(f"/api/v1/workloads/{parent['id']}/children", json=_child_spec(), headers=headers)
    second = client.post(f"/api/v1/workloads/{parent['id']}/children", json=_child_spec(), headers=headers)

    assert first.json()["id"] == second.json()["id"]


def test_a_forged_capability_is_refused(tmp_path) -> None:
    """The signature check at the boundary, not only in the signer's own tests."""

    client, _app = _plane(tmp_path)
    parent = _submit_parent(client)

    response = client.post(
        f"/api/v1/workloads/{parent['id']}/children",
        json=_child_spec(),
        headers={"X-Eyvan-Workload-Token": "eyvw1.forged.signature", "Idempotency-Key": "step-1"},
    )

    assert response.status_code == 403


def test_a_token_stops_working_when_the_parent_is_retried(tmp_path) -> None:
    """Generation pinning, on a parent whose generation has actually moved.

    Every other test here uses a parent at generation 1, where pinning to the
    live generation and hardcoding 1 are indistinguishable. This is the case
    that separates them, and it is the case the design exists for: a retry
    replaces the attempt, and the credential the dead attempt was holding must
    stop working immediately rather than at some expiry.
    """

    import asyncio

    client, app = _plane(tmp_path)
    parent = _submit_parent(client)
    stale = app.state.workload_capability.issue(
        workload_id=parent["id"], org_id="acme", project_id="research", generation=1
    )

    async def retry() -> None:
        async with app.state.service.repository.transaction() as transaction:
            stored = await transaction.get_workload(parent["id"])
            await transaction.put_workload(stored.model_copy(update={"generation": 2}))

    asyncio.run(retry())

    refused = client.post(
        f"/api/v1/workloads/{parent['id']}/children",
        json=_child_spec(),
        headers={"X-Eyvan-Workload-Token": stale, "Idempotency-Key": "step-1"},
    )
    assert refused.status_code == 403, "a credential from the dead attempt still worked"

    reissued = app.state.workload_capability.issue(
        workload_id=parent["id"], org_id="acme", project_id="research", generation=2
    )
    accepted = client.post(
        f"/api/v1/workloads/{parent['id']}/children",
        json=_child_spec(),
        headers={"X-Eyvan-Workload-Token": reissued, "Idempotency-Key": "step-1"},
    )
    assert accepted.status_code == 201, accepted.text
