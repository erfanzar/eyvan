# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Turning a dispatch's secret capabilities into values, on the cell.

The control plane hands a cell one short-lived capability per declared mount and
the address to redeem it at. This redeems them, and it runs on the **agent**
rather than inside the job for two reasons.

The value can then be written into the launch environment file the agent already
writes at ``0600``, so it never appears on a command line, in a unit definition,
or in any durable record -- whereas a job that fetched its own secrets would have
to be handed the capability, and a capability that reaches the job is a
capability the job can log.

And it means every execution kind is covered by one implementation. Wrapping the
entrypoint would have needed a shim per kind: callable, command, workspace,
container.

Standard library only, like its neighbours here. This runs on a TPU worker where
the agent's dependencies are the ones that had to be installed, and one JSON
fetch is not worth adding to that list.
"""

from __future__ import annotations

import http.client
import time
import typing as tp
import urllib.error
import urllib.request

from eyvan.worker.runtime.http_retry import HTTP_ATTEMPTS, RETRYABLE_HTTP_STATUS, retry_delay

#: Written by the control plane at dispatch, one triple per mount.
COUNT_ENV = "EYVAN_SECRET_COUNT"

_TIMEOUT_S = 30.0
_MAX_SECRET_BYTES = 1024 * 1024


class SecretMaterializationError(RuntimeError):
    """Raised when a declared secret could not be resolved for a launch."""


def secret_environment_keys(environment: tp.Mapping[str, str]) -> tuple[str, ...]:
    """Return the scaffolding variables that must not survive into the job.

    The capabilities in particular. A job receives the *value* it asked for and
    not the means to fetch it again -- otherwise a leaked process environment
    would hand an attacker a working credential for the rest of the attempt,
    rather than one secret.
    """

    keys = [key for key in environment if key.startswith("EYVAN_SECRET_")]
    return tuple(sorted(keys))


def materialize_secrets(
    environment: tp.Mapping[str, str],
    *,
    fetch: tp.Callable[[str, str], bytes] | None = None,
) -> dict[str, str]:
    """Resolve every declared mount and return the variables the job should see.

    Raises rather than skipping a mount it could not resolve. A job that starts
    without the credential it declared fails later, further from the cause, and
    often as an authentication error against someone else's service -- so the
    launch is refused here, where the reason is still in hand.
    """

    count = _count(environment)
    if not count:
        return {}
    resolve = fetch or _fetch
    resolved: dict[str, str] = {}
    for index in range(count):
        name = environment.get(f"EYVAN_SECRET_{index}_NAME", "")
        url = environment.get(f"EYVAN_SECRET_{index}_URL", "")
        token = environment.get(f"EYVAN_SECRET_{index}_TOKEN", "")
        if not name or not url or not token:
            raise SecretMaterializationError(f"secret mount {index} is missing its name, address, or capability")
        try:
            value = resolve(url, token)
        except SecretMaterializationError:
            raise
        except Exception as exc:
            raise SecretMaterializationError(f"secret mount {name!r} could not be fetched: {exc}") from exc
        try:
            resolved[name] = value.decode()
        except UnicodeDecodeError:
            # A binary secret in an environment variable would be silently
            # mangled by the env-file encoding. Refusing names the real
            # constraint instead of delivering something subtly wrong.
            #
            # ``from None``: the decode error's message quotes the offending
            # bytes, and those bytes are the secret.
            raise SecretMaterializationError(
                f"secret mount {name!r} is not text and cannot be an environment variable"
            ) from None
    return resolved


def _count(environment: tp.Mapping[str, str]) -> int:
    raw = environment.get(COUNT_ENV, "")
    if not raw:
        return 0
    try:
        count = int(raw)
    except ValueError as exc:
        raise SecretMaterializationError(f"{COUNT_ENV}={raw!r} is not a count") from exc
    if count < 0:
        raise SecretMaterializationError(f"{COUNT_ENV}={raw!r} is not a count")
    return count


def _fetch(url: str, token: str) -> bytes:
    """Redeem one capability, retrying only what is worth retrying.

    A 403 is the control plane saying this attempt is over; retrying it delays a
    launch that is already doomed and adds refusals to the audit trail that look
    like an attack. The shared policy already distinguishes the two.
    """

    request = urllib.request.Request(url, method="GET", headers={"X-Eyvan-Secret-Token": token})
    last: Exception | None = None
    for attempt in range(HTTP_ATTEMPTS):
        try:
            with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:
                return response.read(_MAX_SECRET_BYTES + 1)[:_MAX_SECRET_BYTES]
        except urllib.error.HTTPError as exc:
            retryable = exc.code in RETRYABLE_HTTP_STATUS
            delay = retry_delay(exc, attempt)
            # The body is deliberately not read: on the one route in the API that
            # returns a secret, an error body is the only thing standing between
            # a failure message and a value.
            exc.close()
            if not retryable or attempt == HTTP_ATTEMPTS - 1:
                raise SecretMaterializationError(f"secret fetch refused with HTTP {exc.code}") from exc
            last = exc
        except (urllib.error.URLError, http.client.HTTPException, TimeoutError, ConnectionError, OSError) as exc:
            last = exc
            if attempt == HTTP_ATTEMPTS - 1:
                raise SecretMaterializationError(f"secret fetch could not reach the control plane: {exc}") from exc
            delay = retry_delay(None, attempt)
        time.sleep(delay)
    raise SecretMaterializationError(f"secret fetch failed: {last}")


__all__ = (
    "COUNT_ENV",
    "SecretMaterializationError",
    "materialize_secrets",
    "secret_environment_keys",
)
