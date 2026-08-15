# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""HTTP identity adapters with dev headers and an injectable OIDC verifier."""

from __future__ import annotations

import asyncio
import secrets
import typing as tp

import httpx
import jwt
from fastapi import HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field

from eyvan._internal.json_types import JsonValue
from eyvan.config import EyvanSettings
from eyvan.records import Principal, Role, RoleGrant
from eyvan.server.local.auth import LOCAL_DEVELOPMENT_CREDENTIAL_HEADER, LocalDevelopmentCredential


class _OIDCDiscovery(BaseModel):
    """Discovery fields required to configure JWT verification."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    issuer: str = Field(min_length=1)
    jwks_uri: str = Field(min_length=1)


class _OIDCClaims(BaseModel):
    """Validated identity claims consumed by Eyvan authorization."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    sub: str = Field(min_length=1)
    name: str | None = None
    eyvan_grants: tuple[RoleGrant, ...] = ()


class TokenVerifier(tp.Protocol):
    """Verify an OIDC bearer token and return normalized Eyvan identity."""

    async def verify(self, token: str) -> Principal: ...


class OIDCTokenVerifier:
    """PyJWT-backed verifier using an issuer's JWKS discovery endpoint."""

    def __init__(self, *, issuer: str, audience: str, jwks_url: str | None = None):
        self.issuer = issuer.rstrip("/")
        self.audience = audience
        self._jwks_url = jwks_url
        self._jwks: jwt.PyJWKClient | None = None
        self._discovery_lock = asyncio.Lock()

    async def _jwks_client(self) -> jwt.PyJWKClient:
        if self._jwks is not None:
            return self._jwks
        async with self._discovery_lock:
            if self._jwks is not None:
                return self._jwks
            jwks_url = self._jwks_url
            if jwks_url is None:
                discovery_url = f"{self.issuer}/.well-known/openid-configuration"
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.get(discovery_url)
                    response.raise_for_status()
                    discovery = _OIDCDiscovery.model_validate_json(response.content)
                if discovery.issuer.rstrip("/") != self.issuer:
                    raise ValueError("OIDC discovery issuer does not match configured issuer")
                jwks_url = discovery.jwks_uri
            self._jwks = jwt.PyJWKClient(jwks_url)
            return self._jwks

    async def verify(self, token: str) -> Principal:
        jwks = await self._jwks_client()

        def decode() -> _OIDCClaims:
            key = jwks.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                key.key,
                algorithms=["RS256", "ES256"],
                audience=self.audience,
                issuer=self.issuer,
                options={"require": ["exp"]},
            )
            return _OIDCClaims.model_validate(claims)

        claims = await asyncio.to_thread(decode)
        return Principal(subject=claims.sub, display_name=claims.name, grants=claims.eyvan_grants)


def principal_from_claims(claims: tp.Mapping[str, JsonValue]) -> Principal:
    """Normalize OIDC claims with explicit ``eyvan_grants`` entries."""

    validated = _OIDCClaims.model_validate(dict(claims))
    return Principal(subject=validated.sub, display_name=validated.name, grants=validated.eyvan_grants)


_DEV_ROLE_ALIASES = {
    "admin": Role.PROJECT_ADMIN,
    "operator": Role.PROJECT_OPERATOR,
    "submitter": Role.PROJECT_SUBMITTER,
    "viewer": Role.PROJECT_VIEWER,
}


#: The single principal a bootstrap token authenticates. It is one named admin,
#: not a role a caller may choose: a bootstrap credential that let the holder
#: pick their own authority would be a general bypass wearing a narrow name.
BOOTSTRAP_SUBJECT = "eyvan-bootstrap"


def bootstrap_principal() -> Principal:
    """Return the one principal a bootstrap token authenticates.

    A head created by ``eyvan head up`` has no identity provider, so every
    authenticated request was refused with 401 and the fleet could not be
    created at all. This closes that gap with possession of a generated secret
    rather than a trusted header: unlike development auth, the caller cannot
    name themselves or their roles, so it is safe on a network bind in a way
    ``EYVAN_DEV_AUTH_ENABLED`` is not.
    """

    return Principal(
        subject=BOOTSTRAP_SUBJECT,
        grants=(RoleGrant(role=Role.FLEET_ADMIN, org_id=None, project_id=None),),
    )


class RequestAuthenticator:
    """Authenticate bearer tokens, or explicit headers when dev auth is enabled."""

    def __init__(
        self,
        settings: EyvanSettings,
        *,
        verifier: TokenVerifier | None = None,
        local_development_credential: LocalDevelopmentCredential | None = None,
    ):
        self.settings = settings
        self.verifier = verifier
        self.local_development_credential = local_development_credential
        self.bootstrap_token = settings.bootstrap_token or None

    async def authenticate(self, request: Request) -> Principal:
        authorization = request.headers.get("Authorization", "")
        if authorization:
            scheme, _, token = authorization.partition(" ")
            if scheme.lower() != "bearer" or not token:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid Authorization header")
            if self.bootstrap_token is not None:
                if secrets.compare_digest(token, self.bootstrap_token):
                    return bootstrap_principal()
                if self.verifier is None:
                    # A credential was presented and it is wrong. Reporting "no
                    # identity provider" would call a rejected token a server
                    # fault and tell an unauthenticated caller how the plane is
                    # configured.
                    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid bearer token")
            if self.verifier is None:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=(
                        "no identity provider is configured. Set EYVAN_OIDC_ISSUER, or run "
                        "`eyvan head up --bootstrap-identity` and send its token as a bearer token"
                    ),
                )
            try:
                return await self.verifier.verify(token)
            except Exception as exc:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid bearer token") from exc
        if not self.settings.dev_auth_enabled:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="authentication required")
        self._require_local_development_credential(request)
        return self._dev_principal(request)

    def _require_local_development_credential(self, request: Request) -> None:
        """Require the owner-only credential when automatic loopback auth is active."""

        credential = self.local_development_credential
        if credential is None:
            return
        if not credential.matches(request.headers.get(LOCAL_DEVELOPMENT_CREDENTIAL_HEADER)):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="local Eyvan development credential is required",
            )

    @staticmethod
    def _dev_principal(request: Request) -> Principal:
        subject = request.headers.get("X-Eyvan-User")
        if not subject:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="X-Eyvan-User is required")
        org_id = request.headers.get("X-Eyvan-Org")
        project_id = request.headers.get("X-Eyvan-Project")
        role_names = [part.strip().lower() for part in request.headers.get("X-Eyvan-Roles", "viewer").split(",")]
        grants: list[RoleGrant] = []
        try:
            for name in role_names:
                role = _DEV_ROLE_ALIASES[name] if name in _DEV_ROLE_ALIASES else Role(name)
                grants.append(RoleGrant(role=role, org_id=org_id, project_id=project_id))
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
        return Principal(subject=subject, grants=tuple(grants))


async def authenticated_principal(request: Request) -> Principal:
    """Resolve the caller, then union the grants this plane issued itself.

    Authentication says who; authority comes from both sources. The OIDC
    ``eyvan_grants`` claim used to be the only one, so a deployment whose
    identity provider could not be taught a bespoke claim had no way to
    authorize anyone, and revoking access meant an IdP change rather than an
    API call.

    Read per request rather than cached: a revocation that takes effect on the
    next request is what an operator assumes when they run ``eyvan grant
    revoke``, and a cache would make that true only eventually.
    """

    authenticator: RequestAuthenticator = request.app.state.authenticator
    principal = await authenticator.authenticate(request)
    resolver = getattr(request.app.state, "grant_resolver", None)
    if resolver is None:
        return principal
    durable = await resolver(principal.subject)
    if not durable:
        return principal
    # Union, order-preserving, claim first. Duplicates are dropped because
    # holding a role twice means nothing, and the authorizer already treats
    # grants as a set of independent statements.
    merged: list[RoleGrant] = []
    for grant in (*principal.grants, *durable):
        if grant not in merged:
            merged.append(grant)
    return principal.model_copy(update={"grants": tuple(merged)})
