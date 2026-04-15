"""Lazy public entry point for the Eyvan FastAPI application."""

from __future__ import annotations

import typing as tp

if tp.TYPE_CHECKING:
    from fastapi import FastAPI
    from prometheus_client import CollectorRegistry

    from eyvan.api.auth import TokenVerifier
    from eyvan.api.server_bind import ServerBindPolicy
    from eyvan.artifact_transfers import ManagedArtifactAccess
    from eyvan.config import EyvanSettings
    from eyvan.observability import EyvanMetrics
    from eyvan.persistence.base import Repository
    from eyvan.platform import EyvanPlatformConfig
    from eyvan.service.control_plane import ControlPlaneService


def create_app(
    *,
    settings: EyvanSettings | None = None,
    repository: Repository | None = None,
    service: ControlPlaneService | None = None,
    verifier: TokenVerifier | None = None,
    metrics: EyvanMetrics | None = None,
    metrics_registry: CollectorRegistry | None = None,
    artifact_access: ManagedArtifactAccess | None = None,
    server_bind: ServerBindPolicy | None = None,
    automatic_loopback_auth: bool = False,
    platform_config: EyvanPlatformConfig | None = None,
) -> FastAPI:
    """Import and build the control-plane application on demand.

    This signature must stay identical to :func:`eyvan.api.app.create_app`; it is
    the only way the factory is reachable without importing FastAPI eagerly.
    """

    from eyvan.api.app import create_app as build_app

    return build_app(
        settings=settings,
        repository=repository,
        service=service,
        verifier=verifier,
        metrics=metrics,
        metrics_registry=metrics_registry,
        artifact_access=artifact_access,
        server_bind=server_bind,
        automatic_loopback_auth=automatic_loopback_auth,
        platform_config=platform_config,
    )


__all__ = ("create_app",)
