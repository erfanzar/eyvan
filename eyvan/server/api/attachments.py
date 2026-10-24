# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Fleet-admin-only attachment bridge for a local loopback development plane."""

from __future__ import annotations

import asyncio
import subprocess
import typing as tp
from pathlib import Path

import yaml
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi import Path as ApiPath

from eyvan.config import EyvanSettings
from eyvan.records import Permission, Principal
from eyvan.server.api.auth import authenticated_principal
from eyvan.server.api.server_bind import ServerBindPolicy
from eyvan.server.local.attach_job import JOB_ID_PATTERN, AttachmentJobs, AttachmentRequest, run_plan
from eyvan.server.service import ControlPlaneService

_Principal = tp.Annotated[Principal, Depends(authenticated_principal)]
_JobId = tp.Annotated[str, ApiPath(pattern=JOB_ID_PATTERN)]


def _summary(plan: dict[str, tp.Any]) -> dict[str, tp.Any]:
    """Extract display facts from the CLI's fleet YAML, without inventing defaults."""

    try:
        fleet = yaml.safe_load(plan.get("fleet_yaml", ""))
        pools = fleet.get("pools", {})
        pool = next(iter(pools.values()), {})
        return {
            "shape": pool.get("accelerator_type"),
            "hosts": len(plan.get("endpoints", [])),
            "capacity_class": fleet.get("capacity_class"),
            "endpoint_mode": pool.get("provider_options", {}).get("endpoint_mode"),
        }
    except (yaml.YAMLError, AttributeError, TypeError):
        return {"shape": None, "hosts": len(plan.get("endpoints", [])), "capacity_class": None, "endpoint_mode": None}


def register_attachment_routes(
    app: FastAPI, *, settings: EyvanSettings, service: ControlPlaneService, server_bind: ServerBindPolicy
) -> None:
    """Register local CLI operations, always guarded before disk or subprocess I/O."""

    def authorize(principal: Principal) -> None:
        service.authorizer.require(principal, Permission.MANAGE_FLEET)
        if not server_bind.is_loopback or not settings.dev_auth_enabled:
            raise HTTPException(
                status_code=403,
                detail=(
                    "Attachments require a local loopback development plane. Run `eyvan up` on this machine, "
                    "or use `eyvan gcp connect` on the plane's host."
                ),
            )

    def plane_url(request: Request) -> str:
        # The ASGI listening socket is authoritative, never an untrusted Host or
        # forwarded-host header that could redirect a privileged CLI operation.
        server = request.scope.get("server")
        if server is None:
            raise HTTPException(status_code=503, detail="Cannot resolve the local plane's listening port.")
        host = f"[{server_bind.host}]" if ":" in server_bind.host else server_bind.host
        return f"{'https' if settings.tls_enabled else 'http'}://{host}:{server[1]}"

    def jobs(request: Request) -> AttachmentJobs:
        # A test/embedding seam, not a new environment configuration surface.
        directory: Path | None = getattr(request.app.state, "attachments_directory", None)
        return AttachmentJobs(directory)

    def payload(request: Request, body: AttachmentRequest, principal: Principal) -> dict[str, tp.Any]:
        return {"request": body.model_dump(), "url": plane_url(request), "user": principal.subject}

    @app.post("/api/v1/attachments/plan", tags=["attachments"])
    async def attachment_plan(request: Request, body: AttachmentRequest, principal: _Principal) -> dict[str, tp.Any]:
        """Preview an existing TPU/SSH attachment without mutating the host or fleet."""

        authorize(principal)
        try:
            plan = await asyncio.to_thread(run_plan, payload(request, body, principal))
        except subprocess.TimeoutExpired:
            raise HTTPException(
                status_code=504, detail="Attachment preview timed out after 120 seconds; check SSH access."
            ) from None
        except (OSError, ValueError) as error:
            detail = str(error) if isinstance(error, ValueError) else "Could not start the local attachment CLI."
            raise HTTPException(status_code=502, detail=detail) from None
        return {**plan, "summary": _summary(plan)}

    @app.post("/api/v1/attachments", status_code=202, tags=["attachments"])
    async def start_attachment(request: Request, body: AttachmentRequest, principal: _Principal) -> dict[str, tp.Any]:
        """Start a durable detached CLI job that survives local plane redeployment."""

        authorize(principal)
        return await asyncio.to_thread(jobs(request).start, payload(request, body, principal))

    @app.get("/api/v1/attachments", tags=["attachments"])
    async def recent_attachments(request: Request, principal: _Principal) -> dict[str, tp.Any]:
        """List recent local attachment jobs with their progress and terminal result."""

        authorize(principal)
        return {"jobs": await asyncio.to_thread(jobs(request).recent, url=plane_url(request))}

    @app.get("/api/v1/attachments/{attachment_id}", tags=["attachments"])
    async def get_attachment(request: Request, attachment_id: _JobId, principal: _Principal) -> dict[str, tp.Any]:
        """Read durable progress even after the application process has restarted."""

        authorize(principal)
        try:
            return await asyncio.to_thread(jobs(request).get, attachment_id)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Attachment job not found on this machine.") from None
