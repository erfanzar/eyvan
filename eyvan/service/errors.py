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
"""Service-layer errors mapped to transport-specific responses by adapters."""

from __future__ import annotations

import typing as tp

from eyvan._compat import StrEnum
from eyvan.accelerators import sort_accelerator_types


class ServiceErrorCode(StrEnum):
    """Stable machine-readable codes for structured service failures."""

    NO_MATCHING_CAPACITY_POOL = "no_matching_capacity_pool"


class EyvanServiceError(Exception):
    """Base class for expected control-plane failures."""


class NotFoundError(EyvanServiceError):
    """Requested control-plane entity does not exist or is not visible."""


class ConflictError(EyvanServiceError):
    """Request conflicts with durable state or prior idempotent input."""


class AuthorizationError(EyvanServiceError):
    """Verified principal lacks the required scoped permission."""


class InputError(EyvanServiceError):
    """Request is structurally valid but violates a domain precondition."""


class NoMatchingCapacityPoolError(NotFoundError):
    """No enabled pool in one fleet provides the requested accelerator type."""

    code = ServiceErrorCode.NO_MATCHING_CAPACITY_POOL

    def __init__(
        self,
        *,
        fleet: str,
        requested_accelerator_type: str,
        available_enabled_accelerator_types: tp.Iterable[str],
    ) -> None:
        self.fleet = fleet
        self.requested_accelerator_type = requested_accelerator_type
        self.available_enabled_accelerator_types = sort_accelerator_types(available_enabled_accelerator_types)
        super().__init__(
            f"no enabled capacity pool in fleet {fleet!r} matches accelerator type {requested_accelerator_type!r}"
        )


__all__ = (
    "AuthorizationError",
    "ConflictError",
    "EyvanServiceError",
    "InputError",
    "NoMatchingCapacityPoolError",
    "NotFoundError",
    "ServiceErrorCode",
)
