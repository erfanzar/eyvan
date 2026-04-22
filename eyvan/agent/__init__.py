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
"""Lazy public entry point for the out-of-process Eyvan cell agent."""

from __future__ import annotations

import typing as tp

if tp.TYPE_CHECKING:
    from fastapi import FastAPI

    from eyvan.agent.service import CellAgentService
    from eyvan.config import EyvanSettings


def create_agent_app(
    service: CellAgentService | None = None,
    settings: EyvanSettings | None = None,
) -> FastAPI:
    """Import and build the cell-agent application on demand."""

    from eyvan.agent.app import create_agent_app as build_agent_app

    return build_agent_app(service=service, settings=settings)


__all__ = ("create_agent_app",)
