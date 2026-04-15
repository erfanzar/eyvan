"""Click-facing adapter over Eyvan's shared synchronous HTTP transport."""

from __future__ import annotations

import json
import typing as tp

import click
import httpx

from eyvan._json import JsonObject, JsonValue, is_json_object, is_json_value
from eyvan.client.endpoint import ControlPlaneEndpoint
from eyvan.client.transport import EyvanUnavailableError, SyncApiTransport
from eyvan.client.types import HeaderMap, QueryParams

_MUTATING_METHODS = frozenset({"DELETE", "PATCH", "POST", "PUT"})


class EyvanApiError(click.ClickException):
    """An HTTP API failure with its status available to Click commands."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        super().__init__(f"{status_code}: {detail}")


class EyvanClient:
    """JSON CLI adapter that delegates HTTP behavior to ``SyncApiTransport``."""

    def __init__(self, base_url: str, *, headers: HeaderMap):
        endpoint = ControlPlaneEndpoint.parse(base_url)
        self._transport = SyncApiTransport(
            endpoint,
            headers=headers,
            trust_env=not endpoint.is_loopback,
        )

    @staticmethod
    def _error_detail(response: httpx.Response) -> str:
        detail = response.text
        try:
            payload = response.json()
        except ValueError:
            return detail
        if is_json_object(payload):
            api_detail = payload.get("detail")
            if api_detail is not None:
                return str(api_detail)
        return detail

    @staticmethod
    def _json(response: httpx.Response) -> JsonValue:
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            excerpt = " ".join(response.text.split())[:200]
            raise EyvanApiError(
                response.status_code,
                f"control plane returned a non-JSON response body: {excerpt!r}",
            ) from exc
        if not is_json_value(payload):
            raise EyvanApiError(response.status_code, "control plane returned a non-JSON response value")
        return payload

    def _response(
        self,
        method: str,
        path: str,
        *,
        params: QueryParams | None,
        json: JsonObject | None,
        headers: HeaderMap | None,
        content: bytes | None = None,
    ) -> httpx.Response:
        request_headers = dict(headers or {})
        idempotency_key = request_headers.pop("Idempotency-Key", None)
        try:
            if method.upper() in _MUTATING_METHODS:
                return self._transport.mutate(
                    method,
                    path,
                    idempotency_key=idempotency_key,
                    params=params,
                    json=json,
                    content=content,
                    headers=request_headers,
                )
            return self._transport.request_checked(
                method,
                path,
                params=params,
                json=json,
                headers=request_headers,
            )
        except httpx.HTTPStatusError as exc:
            response = exc.response
            raise EyvanApiError(response.status_code, self._error_detail(response)) from exc
        except EyvanUnavailableError as exc:
            raise click.ClickException(str(exc)) from None

    def request(
        self,
        method: str,
        path: str,
        *,
        params: QueryParams | None = None,
        json: JsonObject | None = None,
        content: bytes | None = None,
        headers: HeaderMap | None = None,
    ) -> JsonValue:
        """Send one JSON API request through the shared transport.

        ``content`` carries a raw body for the content-addressed artifact routes,
        which take bytes rather than JSON.
        """

        return self._json(self._response(method, path, params=params, json=json, content=content, headers=headers))

    def replace_identity_headers(self, headers: HeaderMap) -> None:
        """Replace only Eyvan authentication headers on the persistent HTTP client."""

        identity_headers = (
            "Authorization",
            "X-Eyvan-Local-Credential",
            "X-Eyvan-Org",
            "X-Eyvan-Project",
            "X-Eyvan-Roles",
            "X-Eyvan-User",
        )
        for name in identity_headers:
            self._transport.client.headers.pop(name, None)
        self._transport.client.headers.update(headers)

    def request_text(
        self,
        method: str,
        path: str,
        *,
        params: QueryParams | None = None,
        headers: HeaderMap | None = None,
    ) -> str:
        """Send one API request and return its response body."""

        return self._response(method, path, params=params, json=None, headers=headers).text

    def stream_lines(
        self,
        method: str,
        path: str,
        *,
        params: QueryParams | None = None,
        headers: HeaderMap | None = None,
    ) -> tp.Iterator[str]:
        """Yield response lines as they arrive, for server-sent event streams.

        A generator rather than a callback: the caller decides when to stop, and
        the ``with`` block closes the connection on the way out however the
        iteration ended -- including a ``KeyboardInterrupt``, which is how anyone
        actually exits ``--follow``.
        """

        client = self._transport.client
        request_headers = dict(headers or {})
        try:
            with client.stream(
                method,
                path,
                params=dict(params or {}),
                headers=request_headers,
                timeout=None,
            ) as response:
                if response.status_code >= 400:
                    response.read()
                    raise EyvanApiError(response.status_code, self._error_detail(response))
                yield from response.iter_lines()
        except httpx.HTTPError as exc:
            raise click.ClickException(f"event stream failed: {exc}") from exc


__all__ = ("EyvanApiError", "EyvanClient")
