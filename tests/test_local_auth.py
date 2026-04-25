# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Security coverage for Eyvan's automatic loopback development credential."""

from __future__ import annotations

import stat
from pathlib import Path

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from eyvan.config import EyvanSettings
from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
from eyvan.sdk.client.identity import resolve_control_plane_identity
from eyvan.server.api.auth import RequestAuthenticator
from eyvan.server.local.auth import (
    LOCAL_DEVELOPMENT_CREDENTIAL_HEADER,
    LocalDevelopmentCredential,
    LocalDevelopmentCredentialError,
    LocalDevelopmentCredentialStore,
)


def _identity_app(authenticator: RequestAuthenticator) -> FastAPI:
    """Expose one endpoint protected by an injected request authenticator."""

    app = FastAPI()

    @app.get("/identity")
    async def identity(request: Request) -> dict[str, str | list[str]]:
        principal = await authenticator.authenticate(request)
        return {
            "subject": principal.subject,
            "roles": [grant.role.value for grant in principal.grants],
        }

    return app


def _development_headers(*, credential: str | None = None) -> dict[str, str]:
    """Build development-role headers, optionally with the local credential."""

    headers = {
        "X-Eyvan-User": "alice",
        "X-Eyvan-Org": "acme",
        "X-Eyvan-Project": "research",
        "X-Eyvan-Roles": "fleet_admin",
    }
    if credential is not None:
        headers[LOCAL_DEVELOPMENT_CREDENTIAL_HEADER] = credential
    return headers


def test_local_development_credential_store_is_stable_and_owner_only(tmp_path: Path) -> None:
    path = tmp_path / "profile" / "local-development-credential"
    store = LocalDevelopmentCredentialStore(path)

    created = store.load_or_create()
    loaded = store.load()

    assert loaded == created
    assert created.matches(created.value)
    assert not created.matches("forged")
    assert stat.S_IMODE(path.stat().st_mode) & 0o077 == 0
    assert stat.S_IMODE(path.parent.stat().st_mode) & 0o077 == 0


def test_local_development_credential_store_rejects_a_readable_secret(tmp_path: Path) -> None:
    path = tmp_path / "profile" / "local-development-credential"
    store = LocalDevelopmentCredentialStore(path)
    store.load_or_create()
    path.chmod(0o644)

    with pytest.raises(LocalDevelopmentCredentialError, match="must not be group- or world-readable"):
        store.load()


def test_local_development_credential_store_rejects_an_exposed_profile_directory(tmp_path: Path) -> None:
    path = tmp_path / "profile" / "local-development-credential"
    store = LocalDevelopmentCredentialStore(path)
    store.load_or_create()
    path.parent.chmod(0o755)

    with pytest.raises(LocalDevelopmentCredentialError, match="must not be group- or world-accessible"):
        store.load()


def test_loopback_client_identity_automatically_attaches_the_owner_credential(tmp_path: Path) -> None:
    credential_path = tmp_path / "profile" / "local-development-credential"
    credential = LocalDevelopmentCredentialStore(credential_path).load_or_create()

    loopback = resolve_control_plane_identity(
        ControlPlaneEndpoint("http://127.0.0.1:8080"),
        dev_user="alice",
        dev_org="acme",
        dev_project="research",
        dev_roles="fleet_admin",
        local_development_credential_path=credential_path,
    )
    remote = resolve_control_plane_identity(
        ControlPlaneEndpoint("https://eyvan.example.com"),
        dev_user="alice",
        dev_org="acme",
        dev_project="research",
        dev_roles="fleet_admin",
        local_development_credential_path=credential_path,
    )
    bearer = resolve_control_plane_identity(
        ControlPlaneEndpoint("http://127.0.0.1:8080"),
        token="oidc-token",
        local_development_credential_path=credential_path,
    )

    assert loopback.headers()[LOCAL_DEVELOPMENT_CREDENTIAL_HEADER] == credential.value
    assert LOCAL_DEVELOPMENT_CREDENTIAL_HEADER not in remote.headers()
    assert bearer.headers() == {"Authorization": "Bearer oidc-token"}


def test_loopback_client_identity_never_creates_a_server_credential(tmp_path: Path) -> None:
    credential_path = tmp_path / "profile" / "local-development-credential"

    identity = resolve_control_plane_identity(
        ControlPlaneEndpoint("http://127.0.0.1:8080"),
        dev_user="alice",
        local_development_credential_path=credential_path,
    )

    assert LOCAL_DEVELOPMENT_CREDENTIAL_HEADER not in identity.headers()
    assert not credential_path.parent.exists()


def test_automatic_loopback_auth_rejects_forged_role_headers_without_credential(tmp_path: Path) -> None:
    credential = LocalDevelopmentCredentialStore(tmp_path / "profile" / "local-development-credential").load_or_create()
    app = _identity_app(
        RequestAuthenticator(
            EyvanSettings(dev_auth_enabled=True),
            local_development_credential=credential,
        )
    )

    with TestClient(app) as client:
        missing = client.get("/identity", headers=_development_headers())
        forged = client.get("/identity", headers=_development_headers(credential="forged"))
        accepted = client.get("/identity", headers=_development_headers(credential=credential.value))

    assert missing.status_code == 401
    assert missing.json() == {"detail": "local Eyvan development credential is required"}
    assert forged.status_code == 401
    assert forged.json() == {"detail": "local Eyvan development credential is required"}
    assert accepted.status_code == 200
    assert accepted.json() == {"subject": "alice", "roles": ["fleet_admin"]}


def test_explicit_development_auth_keeps_header_only_behavior() -> None:
    app = _identity_app(RequestAuthenticator(EyvanSettings(dev_auth_enabled=True)))

    with TestClient(app) as client:
        response = client.get("/identity", headers=_development_headers())

    assert response.status_code == 200
    assert response.json() == {"subject": "alice", "roles": ["fleet_admin"]}


def test_local_development_credential_rejects_a_noncanonical_value() -> None:
    with pytest.raises(LocalDevelopmentCredentialError, match="malformed"):
        LocalDevelopmentCredential("not-a-secret")
