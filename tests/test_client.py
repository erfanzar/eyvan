# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import httpx
import pytest

import eyvan
from eyvan.client import EyvanUnavailableError as ClientUnavailableError
from eyvan.client.endpoint import ControlPlaneEndpoint
from eyvan.client.idempotency import IdempotencyKeyFactory
from eyvan.client.identity import local_development_subject, resolve_identity
from eyvan.client.retry import RetryPolicy
from eyvan.client.transport import AsyncApiTransport, EyvanHTTPError, EyvanUnavailableError, SyncApiTransport
from eyvan.execution import EyvanUnavailableError as ExecutionUnavailableError


def test_endpoint_normalizes_only_absolute_http_urls() -> None:
    endpoint = ControlPlaneEndpoint.parse(" https://eyvan.example.com/ ")

    assert endpoint.url == "https://eyvan.example.com"
    assert ControlPlaneEndpoint("http://127.0.0.1:8080/").is_loopback
    with pytest.raises(ValueError, match="absolute HTTP"):
        ControlPlaneEndpoint.parse("eyvan.example.com")
    with pytest.raises(ValueError, match="credentials"):
        ControlPlaneEndpoint.parse("https://token@eyvan.example.com")


def test_unavailable_error_is_exported_through_public_sdk_surfaces() -> None:
    assert eyvan.EyvanUnavailableError is EyvanUnavailableError
    assert ClientUnavailableError is EyvanUnavailableError
    assert ExecutionUnavailableError is EyvanUnavailableError


def test_identity_resolution_preserves_bearer_precedence_and_dev_scope() -> None:
    bearer_identity = resolve_identity(
        token="token",
        dev_user="alice",
        dev_org="acme",
        dev_project="research",
        dev_roles="operator, viewer",
    )
    dev_identity = resolve_identity(
        dev_user="alice",
        dev_org="acme",
        dev_project="research",
        dev_roles="operator, viewer",
    )

    assert bearer_identity.headers() == {"Authorization": "Bearer token"}
    assert dev_identity.headers() == {
        "X-Eyvan-User": "alice",
        "X-Eyvan-Roles": "operator,viewer",
        "X-Eyvan-Org": "acme",
        "X-Eyvan-Project": "research",
    }


def test_local_development_subject_is_stable_and_never_empty() -> None:
    assert local_development_subject(lambda: " erfan ") == "erfan"
    assert local_development_subject(lambda: "") == "local-user"

    def unavailable() -> str:
        raise OSError("no login database")

    assert local_development_subject(unavailable) == "local-user"


def test_idempotency_factory_preserves_supplied_key_and_scopes_generated_keys() -> None:
    factory = IdempotencyKeyFactory(prefix="cli")

    assert factory.create("up", "request-key") == "request-key"
    assert factory.create("up").startswith("cli-up-")
    assert factory.create(supplied="sdk-key") == "sdk-key"


def test_retry_policy_matches_retry_after_contract() -> None:
    policy = RetryPolicy()

    assert policy.delay(status_code=503, headers={"Retry-After": "6"}, attempt=0) == 5
    assert policy.delay(status_code=503, headers={"Retry-After": "invalid"}, attempt=2) == 0.4
    assert policy.delay(status_code=500, headers={"Retry-After": "0"}, attempt=0) is None
    assert policy.delay(status_code=503, headers={}, attempt=0) is None


def test_sync_transport_retries_one_mutation_with_the_same_key() -> None:
    keys: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        keys.append(request.headers["Idempotency-Key"])
        if len(keys) == 1:
            return httpx.Response(503, headers={"Retry-After": "0"})
        return httpx.Response(201, json={"accepted": True})

    raw = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://test")
    transport = SyncApiTransport(client=raw)

    response = transport.mutate("POST", "/api/v1/workloads", json={"accepted": True})

    assert response.status_code == 201
    assert keys[0] == keys[1]
    assert keys[0].startswith("sdk-")
    transport.close()


def test_sync_transport_preserves_structured_api_error_detail() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            request=request,
            json={"detail": "callable workloads are not trusted for project default/default"},
        )

    raw = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://test")
    transport = SyncApiTransport(client=raw)

    with pytest.raises(EyvanHTTPError) as raised:
        transport.mutate("PUT", "/api/v1/artifacts/callables/digest", content=b"callable")

    assert raised.value.status_code == 403
    assert raised.value.detail == "callable workloads are not trusted for project default/default"
    assert str(raised.value) == (
        "Eyvan API returned 403 Forbidden: callable workloads are not trusted for project default/default"
    )
    assert raised.value.__cause__ is None
    transport.close()


def test_sync_transport_replaces_connection_traceback_with_actionable_eyvan_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    raw = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://127.0.0.1:8080")
    transport = SyncApiTransport("http://127.0.0.1:8080", client=raw)

    with pytest.raises(EyvanUnavailableError) as raised:
        transport.request_checked("GET", "/api/v1/workloads/workload-one")

    assert raised.value.endpoint == ControlPlaneEndpoint("http://127.0.0.1:8080")
    assert raised.value.method == "GET"
    assert str(raised.value) == (
        "Eyvan control plane is unavailable at http://127.0.0.1:8080. "
        "Start it with 'eyvan-server --log-level info' and keep it running while jobs are active."
    )
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
    transport.close()


def test_sync_mutation_replaces_connection_traceback_with_eyvan_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    raw = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://eyvan.example.com")
    transport = SyncApiTransport("https://eyvan.example.com", client=raw)

    with pytest.raises(EyvanUnavailableError) as raised:
        transport.mutate("POST", "/api/v1/workloads", json={"name": "train"})

    assert raised.value.endpoint == ControlPlaneEndpoint("https://eyvan.example.com")
    assert raised.value.method == "POST"
    assert str(raised.value) == (
        "Eyvan control plane is unavailable at https://eyvan.example.com. "
        "Check the configured control-plane URL and network access."
    )
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
    transport.close()


@pytest.mark.asyncio
async def test_async_transport_applies_identity_to_an_injected_client() -> None:
    seen_headers: list[httpx.Headers] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(request.headers)
        return httpx.Response(200, json={"ok": True})

    raw = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://test")
    transport = AsyncApiTransport(
        client=raw,
        identity=resolve_identity(dev_user="alice", dev_org="acme", dev_project="research"),
    )

    response = await transport.request_checked("GET", "/api/v1/workloads")

    assert response.status_code == 200
    assert seen_headers[0]["X-Eyvan-User"] == "alice"
    assert seen_headers[0]["X-Eyvan-Org"] == "acme"
    await transport.aclose()


@pytest.mark.asyncio
async def test_async_transport_replaces_connection_traceback_with_actionable_eyvan_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    raw = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://127.0.0.1:8080")
    transport = AsyncApiTransport("http://127.0.0.1:8080", client=raw)

    with pytest.raises(EyvanUnavailableError) as raised:
        await transport.request_checked("GET", "/api/v1/workloads/workload-one")

    assert raised.value.endpoint == ControlPlaneEndpoint("http://127.0.0.1:8080")
    assert raised.value.method == "GET"
    assert str(raised.value) == (
        "Eyvan control plane is unavailable at http://127.0.0.1:8080. "
        "Start it with 'eyvan-server --log-level info' and keep it running while jobs are active."
    )
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
    await transport.aclose()
