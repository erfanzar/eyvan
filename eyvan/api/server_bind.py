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
"""Typed socket-exposure policy for the Eyvan API server."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass

from eyvan._compat import StrEnum

LOOPBACK_APP_FACTORY = "eyvan.api.app:create_loopback_app"
NETWORK_APP_FACTORY = "eyvan.api.app:create_network_app"


class ServerExposure(StrEnum):
    """Network exposure inferred from a literal server bind address."""

    LOOPBACK = "loopback"
    NETWORK = "network"


@dataclass(frozen=True, slots=True)
class ServerBindPolicy:
    """Bind address and its security-relevant exposure classification."""

    host: str
    exposure: ServerExposure

    @classmethod
    def from_host(cls, host: str) -> ServerBindPolicy:
        """Classify a literal IP bind address, treating hostnames as network-exposed."""

        normalized = host.strip()
        if not normalized:
            raise ValueError("server bind host must be a non-empty string")
        try:
            address = ipaddress.ip_address(normalized)
        except ValueError:
            exposure = ServerExposure.NETWORK
        else:
            exposure = ServerExposure.LOOPBACK if address.is_loopback else ServerExposure.NETWORK
        return cls(host=normalized, exposure=exposure)

    @property
    def is_loopback(self) -> bool:
        """Return whether this policy binds only to a literal loopback address."""

        return self.exposure is ServerExposure.LOOPBACK

    @property
    def application_factory(self) -> str:
        """Return the reload-safe Uvicorn factory matching this exposure."""

        return LOOPBACK_APP_FACTORY if self.is_loopback else NETWORK_APP_FACTORY


__all__ = (
    "LOOPBACK_APP_FACTORY",
    "NETWORK_APP_FACTORY",
    "ServerBindPolicy",
    "ServerExposure",
)
