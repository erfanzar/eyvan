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
"""Validated network-address formatting shared by provider and runtime boundaries."""

from __future__ import annotations

import ipaddress


def normalize_ip_address(value: str) -> str:
    """Return the canonical spelling of a trimmed IPv4 or IPv6 literal."""

    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError("IP address must be a non-empty trimmed string")
    try:
        return ipaddress.ip_address(value).compressed
    except ValueError as exc:
        raise ValueError("value is not an IPv4 or IPv6 address") from exc


def format_http_ip_endpoint(address: str, port: int) -> str:
    """Build an HTTP origin from an IP literal and TCP port."""

    normalized = normalize_ip_address(address)
    if isinstance(port, bool) or not isinstance(port, int) or port <= 0 or port > 65535:
        raise ValueError("endpoint port must be an integer between 1 and 65535")
    host = f"[{normalized}]" if ":" in normalized else normalized
    return f"http://{host}:{port}"


__all__ = ("format_http_ip_endpoint", "normalize_ip_address")
