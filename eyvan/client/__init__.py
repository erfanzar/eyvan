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
"""Shared transport primitives for Eyvan SDK and command clients."""

from __future__ import annotations

from eyvan.client.endpoint import ControlPlaneEndpoint
from eyvan.client.idempotency import IdempotencyKeyFactory
from eyvan.client.identity import ClientIdentity, resolve_control_plane_identity, resolve_identity
from eyvan.client.retry import RetryPolicy
from eyvan.client.transport import AsyncApiTransport, EyvanHTTPError, EyvanUnavailableError, SyncApiTransport

__all__ = (
    "AsyncApiTransport",
    "ClientIdentity",
    "ControlPlaneEndpoint",
    "EyvanHTTPError",
    "EyvanUnavailableError",
    "IdempotencyKeyFactory",
    "RetryPolicy",
    "SyncApiTransport",
    "resolve_control_plane_identity",
    "resolve_identity",
)
