# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Team-mode HTTP routes: sign-in, invites, personal API tokens, users.

Every route is registered on every plane so the route inventory and the API
reference stay stable, but each answers 404 unless ``EYVAN_TEAM_MODE`` is on,
except ``/api/v1/team/status`` which tells the dashboard which mode it is in.

Secrets never travel in a URL path or query: uvicorn's access log records
paths, so invite tokens are POSTed and session tokens live in an HttpOnly cookie.
"""

from __future__ import annotations

import typing as tp

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field

from eyvan.config import EyvanSettings
from eyvan.identity.team_secrets import TEAM_SESSION_COOKIE, SignInThrottle
from eyvan.records import ApiTokenRecord, InviteRecord, Principal, Role, RoleGrant, UserRecord
from eyvan.server.api.auth import authenticated_principal

if tp.TYPE_CHECKING:
    from eyvan.server.service import ControlPlaneService

_Principal = tp.Annotated[Principal, Depends(authenticated_principal)]


def require_team_mode(request: Request) -> None:
    """Answer 404 for every team route unless this plane runs team mode."""

    if not getattr(request.app.state, "team_mode", False):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="team mode is not enabled")


#: Module level, not a closure: with postponed annotations FastAPI resolves
#: dependency annotations against module globals, and a local alias is invisible.
_On = tp.Annotated[None, Depends(require_team_mode)]


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LoginBody(_Body):
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=1, max_length=1024)
    #: Set by `eyvan login`: return a named personal API token in the body
    #: instead of setting a browser session cookie.
    token_name: str | None = Field(default=None, min_length=1, max_length=64)


class InviteBody(_Body):
    email: str = Field(min_length=3, max_length=320)
    subject: str = Field(min_length=1, max_length=64)
    role: Role | None = None
    org_id: str | None = Field(default=None, max_length=128)
    project_id: str | None = Field(default=None, max_length=128)


class InviteTokenBody(_Body):
    token: str = Field(min_length=8, max_length=128)


class AcceptBody(InviteTokenBody):
    password: str = Field(min_length=1, max_length=1024)
    display_name: str = Field(default="", max_length=128)


class TokenBody(_Body):
    name: str = Field(min_length=1, max_length=64)
    expires_in_days: int | None = Field(default=None, ge=1, le=3650)


def user_document(user: UserRecord) -> dict[str, tp.Any]:
    """Public view of an account; never the password hash."""

    return {
        "subject": user.subject,
        "email": user.email,
        "display_name": user.display_name,
        "disabled": user.disabled,
        "created_at": user.created_at.isoformat(),
        "created_by": user.created_by,
    }


def token_document(record: ApiTokenRecord) -> dict[str, tp.Any]:
    """Public view of a token; never its hash."""

    return {
        "id": record.id,
        "name": record.name,
        "prefix": record.prefix,
        "created_at": record.created_at.isoformat(),
        "expires_at": None if record.expires_at is None else record.expires_at.isoformat(),
        "last_used_at": None if record.last_used_at is None else record.last_used_at.isoformat(),
        "revoked_at": None if record.revoked_at is None else record.revoked_at.isoformat(),
    }


def invite_document(invite: InviteRecord) -> dict[str, tp.Any]:
    return {
        "id": invite.id,
        "email": invite.email,
        "subject": invite.subject,
        "roles": [grant.model_dump(mode="json") for grant in invite.grants],
        "created_by": invite.created_by,
        "created_at": invite.created_at.isoformat(),
        "expires_at": invite.expires_at.isoformat(),
        "accepted_at": None if invite.accepted_at is None else invite.accepted_at.isoformat(),
        "revoked": invite.revoked,
    }


def register_team_routes(app: FastAPI, *, settings: EyvanSettings, service: ControlPlaneService) -> None:
    """Register the team account routes; all but ``status`` answer 404 when team mode is off."""

    throttle = SignInThrottle()
    app.state.team_sign_in_throttle = throttle
    app.state.team_mode = settings.team_mode
    # Secure whenever the browser reaches this plane over TLS: either the
    # advertised URL is https, or this process terminates TLS itself.
    secure_cookie = settings.public_url.lower().startswith("https://") or settings.tls_enabled
    ttl = settings.dashboard_session_ttl_s

    def set_session(response: Response, raw: str) -> None:
        response.set_cookie(
            key=TEAM_SESSION_COOKIE,
            value=raw,
            max_age=ttl,
            httponly=True,
            secure=secure_cookie,
            samesite="strict",
            path="/",
        )

    def bad_input(error: ValueError) -> HTTPException:
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error))

    @app.get("/api/v1/team/status", tags=["team"])
    async def team_status() -> dict[str, tp.Any]:
        """Report whether this plane runs team accounts (unauthenticated)."""

        return {"team_mode": settings.team_mode}

    @app.post("/api/v1/team/login", tags=["team"])
    async def team_login(body: LoginBody, response: Response, _on: _On) -> dict[str, tp.Any]:
        """Sign in with email and password; sets an HttpOnly session cookie."""

        key = body.email.strip().lower()
        if throttle.blocked(key):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="too many failed sign-ins; wait a minute and try again",
                headers={"Retry-After": "60"},
            )
        result = await service.password_login(key, body.password, session_ttl_s=ttl, token_name=body.token_name)
        if result is None:
            throttle.failed(key)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="wrong email or password")
        throttle.succeeded(key)
        user, record, raw = result
        expires_at = record.expires_at.isoformat() if record.expires_at else None
        if body.token_name is not None:
            # A CLI credential is returned once, in the body; no cookie is set.
            return {
                "user": user_document(user),
                "expires_at": expires_at,
                "token": raw,
                "record": token_document(record),
            }
        set_session(response, raw)
        return {"user": user_document(user), "expires_at": expires_at}

    @app.post("/api/v1/team/logout", tags=["team"])
    async def team_logout(request: Request, response: Response, _on: _On) -> dict[str, tp.Any]:
        """End the cookie session (and a bearer session token, if that is what was sent)."""

        cookie = request.cookies.get(TEAM_SESSION_COOKIE)
        if cookie:
            await service.end_session(cookie)
        response.delete_cookie(key=TEAM_SESSION_COOKIE, path="/", httponly=True, secure=secure_cookie, samesite="strict")
        return {"signed_out": True}

    @app.get("/api/v1/team/me", tags=["team"])
    async def team_me(principal: _Principal, _on: _On) -> dict[str, tp.Any]:
        """Return who the caller is and the grants this plane resolved for them."""

        user = await service.get_team_user(principal.subject)
        return {
            "subject": principal.subject,
            "display_name": principal.display_name,
            "user": None if user is None else user_document(user),
            "grants": [grant.model_dump(mode="json") for grant in principal.grants],
            "fleet_admin": any(grant.role is Role.FLEET_ADMIN for grant in principal.grants),
        }

    @app.post("/api/v1/team/tokens", status_code=201, tags=["team"])
    async def create_team_token(body: TokenBody, principal: _Principal, _on: _On) -> dict[str, tp.Any]:
        """Mint a personal API token; the raw value is returned only here."""

        record, raw = await service.create_api_token(principal, name=body.name, expires_in_days=body.expires_in_days)
        return {"token": raw, "record": token_document(record)}

    @app.get("/api/v1/team/tokens", tags=["team"])
    async def list_team_tokens(principal: _Principal, _on: _On) -> dict[str, tp.Any]:
        return {"tokens": [token_document(item) for item in await service.list_api_tokens(principal)]}

    @app.delete("/api/v1/team/tokens/{token_id}", tags=["team"])
    async def revoke_team_token(token_id: str, principal: _Principal, _on: _On) -> dict[str, tp.Any]:
        return token_document(await service.revoke_api_token(principal, token_id))

    @app.post("/api/v1/team/invites", status_code=201, tags=["team"])
    async def create_team_invite(body: InviteBody, principal: _Principal, _on: _On) -> dict[str, tp.Any]:
        """Invite someone; the link is returned once and should be sent to them privately."""

        grants: tuple[RoleGrant, ...] = ()
        if body.role is not None:
            try:
                grants = (RoleGrant(role=body.role, org_id=body.org_id, project_id=body.project_id),)
            except ValueError as error:
                raise bad_input(error) from None
        invite, raw = await service.invite_user(principal, email=body.email, subject=body.subject, grants=grants)
        return {
            "invite": invite_document(invite),
            "url": f"{settings.public_url.rstrip('/')}/#/accept-invite/{raw}",
            "token": raw,
        }

    @app.get("/api/v1/team/invites", tags=["team"])
    async def list_team_invites(principal: _Principal, _on: _On) -> dict[str, tp.Any]:
        return {"invites": [invite_document(item) for item in await service.list_invites(principal)]}

    @app.delete("/api/v1/team/invites/{invite_id}", tags=["team"])
    async def revoke_team_invite(invite_id: str, principal: _Principal, _on: _On) -> dict[str, tp.Any]:
        return invite_document(await service.revoke_invite(principal, invite_id))

    @app.post("/api/v1/team/invites/preview", tags=["team"])
    async def preview_team_invite(body: InviteTokenBody, _on: _On) -> dict[str, tp.Any]:
        """Show who an invite is for (unauthenticated: the token is the authority)."""

        invite = await service.preview_invite(body.token)
        if invite is None:
            raise HTTPException(status_code=404, detail="this invite is invalid, expired or already used")
        return {"email": invite.email, "subject": invite.subject, "expires_at": invite.expires_at.isoformat()}

    @app.post("/api/v1/team/invites/accept", tags=["team"])
    async def accept_team_invite(body: AcceptBody, response: Response, _on: _On) -> dict[str, tp.Any]:
        """Create the account from an invite, set its password, and sign in."""

        try:
            user, record, raw = await service.accept_invite(
                body.token, password=body.password, display_name=body.display_name, session_ttl_s=ttl
            )
        except ValueError as error:
            raise bad_input(error) from None
        set_session(response, raw)
        return {"user": user_document(user), "expires_at": record.expires_at.isoformat() if record.expires_at else None}

    @app.get("/api/v1/team/users", tags=["team"])
    async def list_team_users(principal: _Principal, _on: _On) -> dict[str, tp.Any]:
        return {"users": [user_document(item) for item in await service.list_team_users(principal)]}

    @app.post("/api/v1/team/users/{subject}/disable", tags=["team"])
    async def disable_team_user(subject: str, principal: _Principal, _on: _On) -> dict[str, tp.Any]:
        return user_document(await service.set_user_disabled(principal, subject, disabled=True))

    @app.post("/api/v1/team/users/{subject}/enable", tags=["team"])
    async def enable_team_user(subject: str, principal: _Principal, _on: _On) -> dict[str, tp.Any]:
        return user_document(await service.set_user_disabled(principal, subject, disabled=False))


__all__ = ("TEAM_SESSION_COOKIE", "register_team_routes")
