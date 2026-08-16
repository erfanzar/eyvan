# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Synchronous and asynchronous HTTP transports for Eyvan API clients."""

from __future__ import annotations

import asyncio
import json
import time

import httpx

from eyvan._internal.client_version import CLIENT_VERSION_HEADER, client_version
from eyvan._internal.json_types import JsonObject, is_json_object
from eyvan.sdk.client.endpoint import DEFAULT_CONTROL_PLANE_URL, ControlPlaneEndpoint
from eyvan.sdk.client.idempotency import IdempotencyKeyFactory
from eyvan.sdk.client.identity import ClientIdentity
from eyvan.sdk.client.retry import RetryPolicy
from eyvan.sdk.client.types import HeaderMap, QueryParams


class EyvanHTTPError(httpx.HTTPStatusError):
    """An unsuccessful Eyvan response with its structured API detail preserved."""

    def __init__(self, response: httpx.Response, detail: str) -> None:
        self.status_code = response.status_code
        self.detail = detail
        message = f"Eyvan API returned {response.status_code} {response.reason_phrase}"
        if detail:
            message = f"{message}: {detail}"
        if response.status_code == 422 and "extra_forbidden" in detail:
            # Name what this is. Eyvan's records are extra="forbid" deliberately,
            # so a rejected unknown field means this client sent something the
            # control plane's models do not carry -- a version skew, not a
            # malformed request. Reported as a bare pydantic dump it reads like a
            # bug in the payload: `eyvan up` failed exactly this way against a
            # head one commit behind, with nothing naming the cause.
            message = (
                f"{message}\n\nThis is a version skew: the control plane rejected a field this client sent. "
                "Compare builds with 'eyvan capacity builds', then bring the plane up to this source with "
                "'eyvan runtime deploy'."
            )
        super().__init__(message, request=response.request, response=response)


class EyvanUnavailableError(ConnectionError):
    """A request that could not reach the configured Eyvan control plane."""

    def __init__(self, endpoint: ControlPlaneEndpoint, method: str) -> None:
        self.endpoint = endpoint
        self.method = method.upper()
        message = f"Eyvan control plane is unavailable at {endpoint.url}."
        if endpoint.is_loopback:
            message = (
                f"{message} Start it with 'eyvan-server --log-level info' and keep it running while jobs are active."
            )
        else:
            message = f"{message} Check the configured control-plane URL and network access."
        super().__init__(message)


def _response_detail(response: httpx.Response) -> str:
    """Extract a stable message from an Eyvan JSON error response."""

    try:
        payload = response.json()
    except ValueError:
        return response.text.strip()
    if is_json_object(payload) and "detail" in payload:
        detail = payload["detail"]
        return detail if isinstance(detail, str) else json.dumps(detail, separators=(",", ":"), sort_keys=True)
    return response.text.strip()


def raise_for_status(response: httpx.Response) -> httpx.Response:
    """Raise a typed Eyvan error for unsuccessful responses and return successes."""

    if not response.is_success:
        raise EyvanHTTPError(response, _response_detail(response))
    return response


class SyncApiTransport:
    """Typed synchronous HTTP boundary with mutation retry handling."""

    def __init__(
        self,
        endpoint: str | ControlPlaneEndpoint = DEFAULT_CONTROL_PLANE_URL,
        *,
        identity: ClientIdentity | None = None,
        headers: HeaderMap | None = None,
        client: httpx.Client | None = None,
        trust_env: bool = True,
        retry_policy: RetryPolicy = RetryPolicy(),
        idempotency_factory: IdempotencyKeyFactory = IdempotencyKeyFactory(),
    ) -> None:
        self.endpoint = ControlPlaneEndpoint.parse(endpoint)
        self.retry_policy = retry_policy
        self.idempotency_factory = idempotency_factory
        client_headers = {
            CLIENT_VERSION_HEADER: client_version(),
            **(identity or ClientIdentity()).headers(),
            **dict(headers or {}),
        }
        if client is None:
            client = httpx.Client(
                base_url=self.endpoint.url,
                headers=client_headers,
                timeout=30,
                trust_env=trust_env,
            )
            self._own_client = True
            self._injected_headers: HeaderMap = {}
        else:
            self._own_client = False
            self._injected_headers = client_headers
        self._client = client

    @property
    def client(self) -> httpx.Client:
        """Return the underlying HTTPX client for compatibility integrations."""

        return self._client

    def request(
        self,
        method: str,
        path: str,
        *,
        params: QueryParams | None = None,
        json: JsonObject | None = None,
        content: bytes | None = None,
        headers: HeaderMap | None = None,
    ) -> httpx.Response:
        """Send one request without applying status handling or retries."""

        try:
            return self._client.request(
                method,
                path,
                params=params,
                json=json,
                content=content,
                headers=self._request_headers(headers),
            )
        except httpx.TransportError:
            raise EyvanUnavailableError(self.endpoint, method) from None

    def _request_headers(self, headers: HeaderMap | None) -> HeaderMap | None:
        """Merge identity headers onto caller headers without mutating injected clients."""

        merged = {**self._injected_headers, **dict(headers or {})}
        return merged or None

    def request_checked(
        self,
        method: str,
        path: str,
        *,
        params: QueryParams | None = None,
        json: JsonObject | None = None,
        content: bytes | None = None,
        headers: HeaderMap | None = None,
    ) -> httpx.Response:
        """Send one request and require a successful response."""

        return raise_for_status(self.request(method, path, params=params, json=json, content=content, headers=headers))

    def mutate(
        self,
        method: str,
        path: str,
        *,
        idempotency_key: str | None = None,
        params: QueryParams | None = None,
        json: JsonObject | None = None,
        content: bytes | None = None,
        headers: HeaderMap | None = None,
    ) -> httpx.Response:
        """Send one mutation, retaining its idempotency key across retries."""

        request_headers = {
            **dict(headers or {}),
            "Idempotency-Key": self.idempotency_factory.create(supplied=idempotency_key),
        }
        for attempt in range(self.retry_policy.max_attempts):
            try:
                response = self.request(
                    method,
                    path,
                    params=params,
                    json=json,
                    content=content,
                    headers=request_headers,
                )
            except EyvanUnavailableError:
                if attempt == self.retry_policy.max_attempts - 1:
                    raise
                time.sleep(self.retry_policy.backoff(attempt))
                continue
            delay = self.retry_policy.delay(
                status_code=response.status_code,
                headers=response.headers,
                attempt=attempt,
            )
            if delay is None or attempt == self.retry_policy.max_attempts - 1:
                return raise_for_status(response)
            time.sleep(delay)
        raise AssertionError("retry policy must allow at least one attempt")

    def close(self) -> None:
        """Close the underlying synchronous HTTP client when owned by this transport."""

        if self._own_client:
            self._client.close()


class AsyncApiTransport:
    """Typed asynchronous HTTP boundary with mutation retry handling."""

    def __init__(
        self,
        endpoint: str | ControlPlaneEndpoint = DEFAULT_CONTROL_PLANE_URL,
        *,
        identity: ClientIdentity | None = None,
        headers: HeaderMap | None = None,
        client: httpx.AsyncClient | None = None,
        trust_env: bool = True,
        retry_policy: RetryPolicy = RetryPolicy(),
        idempotency_factory: IdempotencyKeyFactory = IdempotencyKeyFactory(),
    ) -> None:
        self.endpoint = ControlPlaneEndpoint.parse(endpoint)
        self.retry_policy = retry_policy
        self.idempotency_factory = idempotency_factory
        client_headers = {
            CLIENT_VERSION_HEADER: client_version(),
            **(identity or ClientIdentity()).headers(),
            **dict(headers or {}),
        }
        if client is None:
            client = httpx.AsyncClient(
                base_url=self.endpoint.url,
                headers=client_headers,
                timeout=30,
                trust_env=trust_env,
            )
            self._own_client = True
            self._injected_headers: HeaderMap = {}
        else:
            self._own_client = False
            self._injected_headers = client_headers
        self._client = client

    @property
    def client(self) -> httpx.AsyncClient:
        """Return the underlying async HTTPX client for compatibility integrations."""

        return self._client

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: QueryParams | None = None,
        json: JsonObject | None = None,
        content: bytes | None = None,
        headers: HeaderMap | None = None,
    ) -> httpx.Response:
        """Send one request without applying status handling or retries."""

        try:
            return await self._client.request(
                method,
                path,
                params=params,
                json=json,
                content=content,
                headers=self._request_headers(headers),
            )
        except httpx.TransportError:
            raise EyvanUnavailableError(self.endpoint, method) from None

    def _request_headers(self, headers: HeaderMap | None) -> HeaderMap | None:
        """Merge identity headers onto caller headers without mutating injected clients."""

        merged = {**self._injected_headers, **dict(headers or {})}
        return merged or None

    async def request_checked(
        self,
        method: str,
        path: str,
        *,
        params: QueryParams | None = None,
        json: JsonObject | None = None,
        content: bytes | None = None,
        headers: HeaderMap | None = None,
    ) -> httpx.Response:
        """Send one request and require a successful response."""

        response = await self.request(method, path, params=params, json=json, content=content, headers=headers)
        return raise_for_status(response)

    async def mutate(
        self,
        method: str,
        path: str,
        *,
        idempotency_key: str | None = None,
        params: QueryParams | None = None,
        json: JsonObject | None = None,
        content: bytes | None = None,
        headers: HeaderMap | None = None,
    ) -> httpx.Response:
        """Send one mutation, retaining its idempotency key across retries."""

        request_headers = {
            **dict(headers or {}),
            "Idempotency-Key": self.idempotency_factory.create(supplied=idempotency_key),
        }
        for attempt in range(self.retry_policy.max_attempts):
            try:
                response = await self.request(
                    method,
                    path,
                    params=params,
                    json=json,
                    content=content,
                    headers=request_headers,
                )
            except EyvanUnavailableError:
                if attempt == self.retry_policy.max_attempts - 1:
                    raise
                await asyncio.sleep(self.retry_policy.backoff(attempt))
                continue
            delay = self.retry_policy.delay(
                status_code=response.status_code,
                headers=response.headers,
                attempt=attempt,
            )
            if delay is None or attempt == self.retry_policy.max_attempts - 1:
                return raise_for_status(response)
            await asyncio.sleep(delay)
        raise AssertionError("retry policy must allow at least one attempt")

    async def aclose(self) -> None:
        """Close the underlying asynchronous HTTP client when owned by this transport."""

        if self._own_client:
            await self._client.aclose()


__all__ = (
    "AsyncApiTransport",
    "EyvanHTTPError",
    "EyvanUnavailableError",
    "SyncApiTransport",
    "raise_for_status",
)
