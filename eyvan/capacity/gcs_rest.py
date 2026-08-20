# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Reading and conditionally writing one GCS object without spawning gcloud.

Every fleet-state mutation is a compare-and-swap against a GCS object, and the
existing backend does it with three ``gcloud`` invocations -- describe, cat,
then a conditional copy. Each is about 1.4 seconds of Python interpreter startup
before a byte crosses the network, so a mutation costs several seconds of CPU
and almost no I/O.

That is the dominant cost in draining a fleet: releases serialise on this
document, and a hundred cells release one CAS at a time. Cells sat in
``deleting`` for hours on 2026-08-20 with the provider already empty.

Same conditional semantics, over the JSON API: a media download reports the
object's generation in ``x-goog-generation``, and an upload takes
``ifGenerationMatch``. The credential comes from the path already established
for the other API readers here -- one ``gcloud auth print-access-token``,
inheriting whatever login the operator has.

**The subprocess backend stays.** It works where a raw HTTPS call may not, and a
caller that cannot mint a token falls back to it rather than failing.
"""

from __future__ import annotations

import json
import typing as tp
from dataclasses import dataclass, field

from eyvan._internal.json_types import JsonObject


def _default_token() -> str:
    """Mint a token via the shared gcloud helper, imported on use.

    Deferred because the helper lives under ``capacity.providers.gcp``, which
    imports this module's package -- taking it at module scope makes
    ``fleet_state`` a partially initialised import for anything that reaches it
    through a provider.
    """

    from eyvan.capacity.providers.gcp.accelerator_catalog import gcloud_access_token

    return gcloud_access_token()


#: Bound on one request. Generous because a conditional write that times out is
#: indeterminate and costs a reconciliation to resolve, tight enough that a hung
#: socket cannot hold a fleet lease open.
REQUEST_TIMEOUT_S = 30.0

_OBJECT = "https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object}"
_UPLOAD = "https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o"


class GcsRestError(RuntimeError):
    """The object could not be read or written over the GCS JSON API."""


class GcsPreconditionFailed(GcsRestError):
    """The object changed since it was read, so the conditional write was refused."""


def split_uri(uri: str) -> tuple[str, str]:
    """Split ``gs://bucket/path/to/object`` into its bucket and object name."""

    if not uri.startswith("gs://") or uri.endswith("/"):
        raise ValueError("fleet state URI must be a gs:// object path")
    bucket, _, name = uri[len("gs://") :].partition("/")
    if not bucket or not name:
        raise ValueError("fleet state URI must name a bucket and an object")
    return bucket, name


@dataclass
class GcsRestObject:
    """One GCS object, read and conditionally written over the JSON API.

    Attributes:
        uri: The ``gs://`` object path.
        token_source: Where the bearer credential comes from. Injected so a test
            never shells out and a caller can supply its own.
    """

    uri: str
    token_source: tp.Callable[[], str] = _default_token
    _token: str | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.bucket, self.name = split_uri(self.uri)

    def _headers(self) -> dict[str, str]:
        token = self._token or self.token_source()
        self._token = token
        return {"Authorization": f"Bearer {token}"}

    def read(self) -> tuple[JsonObject | None, int]:
        """Return the object's parsed body and generation, or ``(None, 0)`` if absent.

        Generation zero is GCS's own spelling for "this object does not exist",
        and is what ``ifGenerationMatch`` takes to mean "create only if still
        absent" -- so the missing case needs no separate signalling.
        """

        from urllib.parse import quote

        import httpx

        url = _OBJECT.format(bucket=self.bucket, object=quote(self.name, safe=""))
        try:
            with httpx.Client(timeout=REQUEST_TIMEOUT_S, trust_env=False) as client:
                response = client.get(url, params={"alt": "media"}, headers=self._headers())
        except httpx.HTTPError as exc:
            raise GcsRestError(f"could not read {self.uri}: {exc}") from exc
        if response.status_code == 404:
            return None, 0
        if response.status_code != 200:
            raise GcsRestError(f"reading {self.uri} answered {response.status_code}")
        generation = response.headers.get("x-goog-generation")
        if generation is None or not generation.isdigit():
            # Without a generation there is nothing to compare and swap against,
            # and writing unconditionally would overwrite a concurrent mutation.
            raise GcsRestError(f"{self.uri} was read without a generation header")
        try:
            body = response.json()
        except ValueError as exc:
            raise GcsRestError(f"{self.uri} is not valid JSON") from exc
        if not isinstance(body, dict):
            raise GcsRestError(f"{self.uri} is not a JSON object")
        return tp.cast("JsonObject", body), int(generation)

    def write(self, document: JsonObject, generation: int) -> None:
        """Replace the object only while it is still at ``generation``.

        Raises:
            GcsPreconditionFailed: Something else wrote first. The caller re-reads
                and reapplies its mutation; that is the compare-and-swap.
            GcsRestError: The write could not be attempted or its outcome is not
                known.
        """

        import httpx

        payload = json.dumps(document, indent=2, sort_keys=True).encode()
        try:
            with httpx.Client(timeout=REQUEST_TIMEOUT_S, trust_env=False) as client:
                response = client.post(
                    _UPLOAD.format(bucket=self.bucket),
                    params={
                        "uploadType": "media",
                        "name": self.name,
                        "ifGenerationMatch": str(generation),
                    },
                    headers={**self._headers(), "Content-Type": "application/json"},
                    content=payload,
                )
        except httpx.HTTPError as exc:
            raise GcsRestError(f"could not write {self.uri}: {exc}") from exc
        if response.status_code == 412:
            raise GcsPreconditionFailed(f"{self.uri} changed since it was read at generation {generation}")
        if response.status_code not in {200, 201}:
            raise GcsRestError(f"writing {self.uri} answered {response.status_code}")


__all__ = ("REQUEST_TIMEOUT_S", "GcsPreconditionFailed", "GcsRestError", "GcsRestObject", "split_uri")
