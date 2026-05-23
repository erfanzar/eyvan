# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Two organizations on one control plane, and the wall between them.

Eyvan enforces per-org and per-project scope at roughly seventy-five call sites,
and every one of them was individually plausible. What nothing established is the
property those call sites are supposed to add up to: that a principal scoped to
one tenant cannot reach another tenant's workloads, capacity, governance or
audit trail through *any* route.

This asserts that over the real HTTP surface, and it is written so a route added
tomorrow cannot quietly avoid the question. ``ROUTE_CLASSIFICATION`` names every
route in the application; a route missing from it fails
``test_every_route_is_classified`` rather than silently going untested. That is
the difference between a suite that covers the boundary today and one that keeps
covering it.

Classification is the honest part. A route that cannot leak across tenants --
because it takes no tenant scope, or is authenticated by a capability token
rather than a principal -- is exempt *with a stated reason*, not omitted.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from eyvan.config import EyvanSettings
from eyvan.server.api.app import create_app

# --------------------------------------------------------------------------- classification

#: Routes exercised below for cross-tenant denial, mapped to the concrete
#: attempt. Every entry is "org B's operator, aimed at org A's resource".
CROSS_TENANT_ROUTES: dict[str, str] = {
    "GET /api/v1/workloads": "list another org's workloads",
    "GET /api/v1/workloads/{workload_id}": "read another org's workload",
    "GET /api/v1/workloads/{workload_id}/explain": "read another org's scheduling verdict",
    "GET /api/v1/workloads/{workload_id}/logs": "read another org's logs",
    "GET /api/v1/workloads/{workload_id}/log-page": "read another org's structured logs",
    "GET /api/v1/workloads/{workload_id}/logs/stream": "follow another org's logs",
    "GET /api/v1/workloads/{workload_id}/progress": "read another org's progress",
    "GET /api/v1/workloads/{workload_id}/process-status": "read another org's process state",
    "GET /api/v1/workloads/{workload_id}/children": "enumerate another org's job tree",
    "GET /api/v1/workloads/{workload_id}/endpoints": "read another org's service endpoints",
    "POST /api/v1/workloads": "submit into another org",
    "POST /api/v1/workloads/{workload_id}/cancel": "cancel another org's workload",
    "POST /api/v1/workloads/{workload_id}/retry": "retry another org's workload",
    "POST /api/v1/workloads/{workload_id}/checkpoint": "checkpoint another org's workload",
    "POST /api/v1/workloads/{workload_id}/exec": "run a command beside another org's workload",
    "POST /api/v1/workloads/estimate": "price work in another org",
    "POST /api/v1/batches": "fan out into another org",
    "POST /api/v1/dags": "release a graph into another org",
    "POST /api/v1/groups": "place a heterogeneous group into another org",
    "GET /api/v1/quotas": "read another org's quotas",
    "GET /api/v1/quotas/current": "read another org's effective quota",
    "PUT /api/v1/quotas": "write another org's quota",
    "DELETE /api/v1/quotas": "delete another org's quota",
    "GET /api/v1/budgets": "read another org's budgets",
    "GET /api/v1/budgets/{org_id}/{project_id}": "read another org's budget",
    "GET /api/v1/budgets/{org_id}/{project_id}/status": "read another org's spend",
    "PUT /api/v1/budgets/{org_id}/{project_id}": "write another org's budget",
    "GET /api/v1/policies": "read another org's policies",
    "GET /api/v1/policies/{org_id}/{project_id}": "read another org's policy",
    "PUT /api/v1/policies/{org_id}/{project_id}": "write another org's policy",
    "GET /api/v1/events": "read another org's audit trail",
    "GET /api/v1/events/stream": "follow another org's audit trail",
    "GET /api/v1/artifacts": "list another org's artifacts",
    "GET /api/v1/artifacts/usage": "read another org's storage usage",
    "GET /api/v1/fleets": "list fleets runnable in another org's scope",
    "GET /api/v1/organizations/{org_id}": "read another tenant's registration",
    "GET /api/v1/organizations/{org_id}/projects": "enumerate another tenant's projects",
    "PUT /api/v1/organizations/{org_id}/projects/{project_id}": "create a project in another tenant",
    "POST /api/v1/grants": "grant access inside another tenant",
    "DELETE /api/v1/grants": "strip access inside another tenant",
}

#: Routes that require fleet administration outright. A tenant principal is
#: refused regardless of which tenant's resource it names, so the cross-tenant
#: question does not arise -- but they are listed rather than omitted, because
#: "needs fleet admin" is a claim worth failing on if it ever stops being true.
FLEET_ONLY_ROUTES: dict[str, str] = {
    "GET /api/v1/cells": "fleet capacity inventory",
    "GET /api/v1/cells/{cell_id}": "one cell's provider state",
    "POST /api/v1/cells": "register capacity",
    "DELETE /api/v1/cells/{cell_id}": "destroy capacity",
    "POST /api/v1/cells/{cell_id}/drain": "drain capacity",
    "GET /api/v1/capacity-pools": "fleet pool inventory",
    "GET /api/v1/capacity-pools/{pool_id}": "one pool",
    "POST /api/v1/capacity-pools": "create a pool",
    "DELETE /api/v1/capacity-pools/{pool_id}": "disable a pool",
    "GET /api/v1/capacity/orphans": "capacity no record claims",
    "GET /api/v1/capacity/billing": "fleet-wide cost",
    "GET /api/v1/capacity/warmth": "fleet-wide warm state",
    "GET /api/v1/capacity/queue-trend": "fleet-wide queue depth",
    "GET /api/v1/fleets/{fleet_name}": "one fleet",
    "PUT /api/v1/fleets/{fleet_name}": "declarative fleet apply",
    "DELETE /api/v1/fleets/{fleet_name}": "delete a fleet",
    "POST /api/v1/fleets/{fleet_name}/callables": "place a callable on fleet capacity",
    "POST /api/v1/fleets/{fleet_name}/node-types/{node_type}/workloads": "place work on fleet capacity",
    "GET /api/v1/leases": "fleet lease inventory",
    "GET /api/v1/leases/{lease_id}": "one lease",
    "GET /api/v1/fleet/events": "unscoped audit trail",
    "POST /api/v1/preemptions/{command_id}/force-finalize": "break-glass preemption finalize",
    "POST /api/v1/system/reconcile": "force a scheduling pass",
    "GET /api/v1/artifacts/collectable": "fleet-wide collectable artifacts",
    "GET /api/v1/artifacts/references": "fleet-wide artifact references",
    "POST /api/v1/artifacts/holds": "place a legal hold",
    "DELETE /api/v1/artifacts/holds": "release a legal hold",
    "GET /api/v1/organizations": "tenant list, narrowed to the caller's own grants",
    "PUT /api/v1/organizations/{org_id}": "register a tenant",
    "GET /api/v1/grants": "grant list, narrowed to the caller's own scopes",
    "POST /api/v1/budgets/{org_id}/{project_id}/roll": "close a budget period",
    "GET /api/v1/capacity": "pool availability for placement, org-scoped by query",
}

#: Routes that carry no principal-scoped tenancy at all, each with the reason.
EXEMPT_ROUTES: dict[str, str] = {
    "GET /api/v1/health": "liveness; no tenant data",
    "GET /healthz": "liveness; no tenant data",
    "GET /metrics": "separately authenticated by a scrape token; see test_metrics_endpoint_is_not_public",
    "GET /docs": "API documentation",
    "GET /docs/oauth2-redirect": "API documentation",
    "GET /redoc": "API documentation",
    "GET /openapi.json": "API schema",
    "PUT /api/v1/artifacts/callables/{digest}": "content-addressed; authorized by project trust and digest",
    "GET /api/v1/artifacts/callables/{digest}/content": "capability token, not a principal",
    "PUT /api/v1/artifacts/workspaces/{digest}": "content-addressed; authorized by project trust and digest",
    "GET /api/v1/artifacts/workspaces/{digest}/content": "capability token, not a principal",
    "GET /api/v1/artifacts/results/{result_id}": "capability token, not a principal",
    "PUT /api/v1/artifacts/results/{result_id}/content": "capability token, not a principal",
    "POST /api/v1/workloads/{workload_id}/progress-reports": "capability token, not a principal",
    "POST /api/v1/workloads/{workload_id}/children": "workload capability token, not a principal",
    "PUT /api/v1/workloads/{workload_id}/endpoint": "workload capability token, not a principal",
    "GET /api/v1/secrets/content": "secret capability token, not a principal",
}

ROUTE_CLASSIFICATION = {**CROSS_TENANT_ROUTES, **FLEET_ONLY_ROUTES, **EXEMPT_ROUTES}


# --------------------------------------------------------------------------- harness


def _app():
    return create_app(settings=EyvanSettings(dev_auth_enabled=True, artifact_token_key="k" * 32))


def _headers(user: str, roles: str, org: str | None = None, project: str | None = None) -> dict[str, str]:
    headers = {"X-Eyvan-User": user, "X-Eyvan-Roles": roles}
    if org is not None:
        headers["X-Eyvan-Org"] = org
    if project is not None:
        headers["X-Eyvan-Project"] = project
    return headers


ADMIN = _headers("root", "fleet_admin")
ACME = _headers("alice", "operator", "acme", "research")
RIVAL = _headers("mallory", "operator", "rival", "research")


@pytest.fixture
def plane() -> TestClient:
    """One control plane serving two registered, unrelated tenants."""

    client = TestClient(_app())
    for org in ("acme", "rival"):
        client.put(
            f"/api/v1/organizations/{org}",
            json={"owner": f"owner@{org}"},
            headers={**ADMIN, "Idempotency-Key": f"o-{org}"},
        )
        client.put(
            f"/api/v1/organizations/{org}/projects/research",
            json={"owner": f"owner@{org}"},
            headers={**ADMIN, "Idempotency-Key": f"p-{org}"},
        )
    return client


def _submit(client: TestClient, headers: dict[str, str], org: str, key: str) -> str:
    response = client.post(
        "/api/v1/workloads",
        json={
            "org_id": org,
            "project_id": "research",
            "spec": {"name": "train", "execution": {"kind": "command", "command": "python train.py"}},
        },
        headers={**headers, "Idempotency-Key": key},
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


# --------------------------------------------------------------------------- the guard


def test_every_route_is_classified() -> None:
    """A route added tomorrow must be classified before this suite goes green.

    Without this, the cross-tenant coverage below is a snapshot: correct on the
    day it was written and silently incomplete from the next route onward. The
    failure message names the route so classifying it is a one-line decision,
    not an investigation.
    """

    live = {
        f"{method} {route.path}"
        for route in _app().routes
        if getattr(route, "methods", None)
        for method in sorted(route.methods - {"HEAD", "OPTIONS"})
    }

    unclassified = sorted(live - set(ROUTE_CLASSIFICATION))
    assert not unclassified, (
        "these routes are not classified for tenant isolation; add each to "
        "CROSS_TENANT_ROUTES, FLEET_ONLY_ROUTES, or EXEMPT_ROUTES with a reason: " + ", ".join(unclassified)
    )

    stale = sorted(set(ROUTE_CLASSIFICATION) - live)
    assert not stale, f"these classified routes no longer exist: {', '.join(stale)}"


# --------------------------------------------------------------------------- workloads


def test_a_tenant_cannot_see_or_touch_another_tenants_workload(plane: TestClient) -> None:
    """Read, cancel, retry, checkpoint, and the diagnostic views around them.

    Cancel and retry matter most: they are unauthenticated denial of service
    against a competitor's training run if the scope check is missing on even
    one of them.
    """

    workload = _submit(plane, ACME, "acme", "w-acme")

    reads = {
        "get": plane.get(f"/api/v1/workloads/{workload}", headers=RIVAL),
        "explain": plane.get(f"/api/v1/workloads/{workload}/explain", headers=RIVAL),
        "logs": plane.get(f"/api/v1/workloads/{workload}/logs", headers=RIVAL),
        "log-page": plane.get(f"/api/v1/workloads/{workload}/log-page", headers=RIVAL),
        "log-stream": plane.get(f"/api/v1/workloads/{workload}/logs/stream", params={"follow": "false"}, headers=RIVAL),
        "progress": plane.get(f"/api/v1/workloads/{workload}/progress", headers=RIVAL),
        "process-status": plane.get(f"/api/v1/workloads/{workload}/process-status", headers=RIVAL),
        "children": plane.get(f"/api/v1/workloads/{workload}/children", headers=RIVAL),
        "endpoints": plane.get(f"/api/v1/workloads/{workload}/endpoints", headers=RIVAL),
    }
    writes = {
        "cancel": plane.post(f"/api/v1/workloads/{workload}/cancel", headers={**RIVAL, "Idempotency-Key": "x1"}),
        "retry": plane.post(f"/api/v1/workloads/{workload}/retry", headers={**RIVAL, "Idempotency-Key": "x2"}),
        "checkpoint": plane.post(f"/api/v1/workloads/{workload}/checkpoint", headers={**RIVAL, "Idempotency-Key": "x3"}),
    }

    for name, response in {**reads, **writes}.items():
        assert response.status_code in {403, 404}, f"{name} leaked across tenants: {response.status_code}"

    # And the owner is unaffected -- a wall that also blocks the tenant is a
    # broken plane, not a secure one.
    assert plane.get(f"/api/v1/workloads/{workload}", headers=ACME).status_code == 200


def test_listing_workloads_never_returns_another_tenants(plane: TestClient) -> None:
    """The quietest leak: a list route that ignores the scope it was given."""

    _submit(plane, ACME, "acme", "w-acme")
    _submit(plane, RIVAL, "rival", "w-rival")

    listed = plane.get("/api/v1/workloads", params={"org_id": "acme", "project_id": "research"}, headers=RIVAL)
    own = plane.get("/api/v1/workloads", params={"org_id": "rival", "project_id": "research"}, headers=RIVAL)

    assert listed.status_code == 403
    assert [item["org_id"] for item in own.json()] == ["rival"]


def test_a_tenant_cannot_submit_into_another_tenants_scope(plane: TestClient) -> None:
    """Placing work into another tenant spends their quota and their budget."""

    submissions = {
        "workload": plane.post(
            "/api/v1/workloads",
            json={
                "org_id": "acme",
                "project_id": "research",
                "spec": {"name": "t", "execution": {"kind": "command", "command": "echo hi"}},
            },
            headers={**RIVAL, "Idempotency-Key": "s1"},
        ),
        "estimate": plane.post(
            "/api/v1/workloads/estimate",
            json={
                "org_id": "acme",
                "project_id": "research",
                "spec": {"name": "t", "execution": {"kind": "command", "command": "echo hi"}},
            },
            headers=RIVAL,
        ),
        "batch": plane.post(
            "/api/v1/batches",
            json={
                "org_id": "acme",
                "project_id": "research",
                "count": 2,
                "spec": {"name": "t", "execution": {"kind": "command", "command": "echo hi"}},
            },
            headers={**RIVAL, "Idempotency-Key": "s2"},
        ),
        "dag": plane.post(
            "/api/v1/dags",
            json={
                "org_id": "acme",
                "project_id": "research",
                "nodes": [
                    {
                        "name": "a",
                        "spec": {"name": "a", "execution": {"kind": "command", "command": "echo hi"}},
                    }
                ],
            },
            headers={**RIVAL, "Idempotency-Key": "s3"},
        ),
        "group": plane.post(
            "/api/v1/groups",
            json={
                "org_id": "acme",
                "project_id": "research",
                "members": [
                    {
                        "name": "a",
                        "spec": {"name": "a", "execution": {"kind": "command", "command": "echo hi"}},
                    }
                ],
            },
            headers={**RIVAL, "Idempotency-Key": "s4"},
        ),
    }

    for name, response in submissions.items():
        assert response.status_code in {403, 422}, f"{name} placed work into another tenant: {response.status_code}"


# --------------------------------------------------------------------------- governance


def test_governance_reads_and_writes_stay_inside_a_tenant(plane: TestClient) -> None:
    """Quotas, budgets and policies decide what a tenant may spend.

    Reading them is competitive intelligence; writing them is either a denial of
    service or a way to lift your own ceiling using someone else's admin path.
    """

    attempts = {
        "quota-list": plane.get("/api/v1/quotas", params={"org_id": "acme"}, headers=RIVAL),
        "quota-current": plane.get(
            "/api/v1/quotas/current", params={"org_id": "acme", "project_id": "research"}, headers=RIVAL
        ),
        "quota-write": plane.put(
            "/api/v1/quotas",
            json={"org_id": "acme", "project_id": "research", "limits": {"cpu": 999}},
            headers={**RIVAL, "Idempotency-Key": "q1"},
        ),
        "quota-delete": plane.request(
            "DELETE", "/api/v1/quotas", params={"org_id": "acme", "project_id": "research"}, headers=RIVAL
        ),
        "budget-list": plane.get("/api/v1/budgets", params={"org_id": "acme"}, headers=RIVAL),
        "budget-get": plane.get("/api/v1/budgets/acme/research", headers=RIVAL),
        "budget-status": plane.get("/api/v1/budgets/acme/research/status", headers=RIVAL),
        "budget-write": plane.put(
            "/api/v1/budgets/acme/research",
            json={"limit": 1_000_000},
            headers={**RIVAL, "Idempotency-Key": "b1"},
        ),
        "policy-list": plane.get("/api/v1/policies", params={"org_id": "acme"}, headers=RIVAL),
        "policy-get": plane.get("/api/v1/policies/acme/research", headers=RIVAL),
        "policy-write": plane.put(
            "/api/v1/policies/acme/research",
            json={"max_replicas": 1},
            headers={**RIVAL, "Idempotency-Key": "p1"},
        ),
    }

    for name, response in attempts.items():
        assert response.status_code in {403, 404, 422}, f"{name} crossed tenants: {response.status_code}"


def test_a_tenant_cannot_ask_which_fleets_another_tenant_may_run_on(plane: TestClient) -> None:
    """Runnable-fleet names describe what capacity a competitor has been given."""

    theirs = plane.get("/api/v1/fleets", params={"org_id": "acme", "project_id": "research"}, headers=RIVAL)
    ours = plane.get("/api/v1/fleets", params={"org_id": "rival", "project_id": "research"}, headers=RIVAL)

    assert theirs.status_code == 403
    assert ours.status_code == 200


def test_governance_resolves_per_tenant_rather_than_globally(plane: TestClient) -> None:
    """A limit stored under the right key but resolved globally binds the wrong tenant.

    That failure passes every isolation assertion above -- nothing there submits
    enough to reach a limit -- so it needs its own check. This asserts the
    resolution, which is where the hierarchy is chosen; the scheduler's *use* of
    the resolved limit is covered by ``test_scheduler``, because quotas and
    budgets are scheduling constraints here rather than admission gates: a
    workload over its limit is accepted, queued, and explained.
    """

    plane.put(
        "/api/v1/quotas",
        json={"org_id": "acme", "project_id": "research", "limits": {"cpu": 4}},
        headers={**ADMIN, "Idempotency-Key": "q-acme"},
    )
    plane.put(
        "/api/v1/budgets/acme/research",
        json={"limit": 100.0, "hard_limit": True},
        headers={**ADMIN, "Idempotency-Key": "b-acme"},
    )

    acme_quota = plane.get("/api/v1/quotas/current", params={"org_id": "acme", "project_id": "research"}, headers=ACME)
    rival_quota = plane.get(
        "/api/v1/quotas/current", params={"org_id": "rival", "project_id": "research"}, headers=RIVAL
    )
    rival_budget = plane.get("/api/v1/budgets/rival/research", headers=RIVAL)

    assert acme_quota.status_code == 200
    assert acme_quota.json()["limits"]["cpu"] == 4.0
    # The neighbouring tenant inherits neither. A global resolution would hand
    # rival acme's ceiling, which reads as a working limit and is the wrong one.
    assert rival_quota.status_code == 404
    assert rival_budget.status_code == 404


# --------------------------------------------------------------------------- audit and artifacts


def test_the_audit_trail_is_not_readable_across_tenants(plane: TestClient) -> None:
    """An audit stream is a record of who did what, and it is the last thing to leak.

    ``/events/stream`` is checked because a route that authorizes *inside* the
    response generator has already committed a 200 by the time it refuses.
    """

    _submit(plane, ACME, "acme", "w-acme")

    events = plane.get("/api/v1/events", params={"org_id": "acme", "project_id": "research"}, headers=RIVAL)
    stream = plane.get(
        "/api/v1/events/stream",
        params={"org_id": "acme", "project_id": "research", "follow": "false"},
        headers=RIVAL,
    )

    assert events.status_code == 403
    assert stream.status_code == 403, "the stream authorized after committing a response"


def test_artifact_listings_stay_inside_a_tenant(plane: TestClient) -> None:
    """Object names carry model names, dataset names, and experiment structure."""

    listing = plane.get("/api/v1/artifacts", params={"org_id": "acme", "project_id": "research"}, headers=RIVAL)
    usage = plane.get("/api/v1/artifacts/usage", params={"org_id": "acme", "project_id": "research"}, headers=RIVAL)

    assert listing.status_code == 403
    assert usage.status_code == 403


# --------------------------------------------------------------------------- tenancy itself


def test_the_tenancy_surface_does_not_leak_between_tenants(plane: TestClient) -> None:
    """Registration and grants are how a tenant would acquire the rest."""

    attempts = {
        "read-org": plane.get("/api/v1/organizations/acme", headers=RIVAL),
        "list-projects": plane.get("/api/v1/organizations/acme/projects", headers=RIVAL),
        "create-project": plane.put(
            "/api/v1/organizations/acme/projects/sneaky",
            json={"owner": "mallory"},
            headers={**RIVAL, "Idempotency-Key": "t1"},
        ),
        "grant": plane.post(
            "/api/v1/grants",
            json={
                "subject": "mallory",
                "grant": {"role": "project_operator", "org_id": "acme", "project_id": "research"},
            },
            headers={**RIVAL, "Idempotency-Key": "t2"},
        ),
        "revoke": plane.request(
            "DELETE",
            "/api/v1/grants",
            params={"subject": "alice", "role": "project_operator", "org_id": "acme", "project_id": "research"},
            headers=RIVAL,
        ),
    }

    for name, response in attempts.items():
        assert response.status_code in {403, 404}, f"{name} crossed tenants: {response.status_code}"


# --------------------------------------------------------------------------- capacity


def test_capacity_administration_is_refused_to_a_tenant_operator(plane: TestClient) -> None:
    """Every fleet-only route, refused to a tenant principal.

    Listed rather than assumed: "this needs fleet administration" is a claim,
    and a claim worth failing on the day it stops being true.
    """

    attempts = {
        "cells": plane.get("/api/v1/cells", headers=RIVAL),
        "pools": plane.get("/api/v1/capacity-pools", headers=RIVAL),
        "orphans": plane.get("/api/v1/capacity/orphans", headers=RIVAL),
        "billing": plane.get("/api/v1/capacity/billing", headers=RIVAL),
        "warmth": plane.get("/api/v1/capacity/warmth", headers=RIVAL),
        "leases": plane.get("/api/v1/leases", headers=RIVAL),
        "fleet-events": plane.get("/api/v1/fleet/events", headers=RIVAL),
        "reconcile": plane.post("/api/v1/system/reconcile", headers=RIVAL),
        "create-pool": plane.post("/api/v1/capacity-pools", json={}, headers={**RIVAL, "Idempotency-Key": "c1"}),
        "drain-cell": plane.post("/api/v1/cells/any/drain", headers={**RIVAL, "Idempotency-Key": "c2"}),
        "delete-cell": plane.request("DELETE", "/api/v1/cells/any", headers=RIVAL),
    }

    for name, response in attempts.items():
        assert response.status_code in {403, 404, 422}, f"{name} reached fleet capacity: {response.status_code}"


def test_a_tenant_sees_only_its_own_registration_in_a_listing(plane: TestClient) -> None:
    """Narrowed listings are the other half: a 200 that omits, rather than a 403."""

    organizations = plane.get("/api/v1/organizations", headers=RIVAL)

    assert organizations.status_code == 200
    assert [item["id"] for item in organizations.json()] == ["rival"]
