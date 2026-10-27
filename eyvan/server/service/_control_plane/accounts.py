# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Team-mode accounts: invites, password sign-in, personal API tokens.

Authority is never stored on the account. An invite carries the role grants an
administrator chose; accepting it issues ordinary durable ``RoleGrantRecord``s,
so the rest of the plane authorizes team users exactly as it authorizes anyone
else. Only hashes of invite and token secrets are persisted, and a raw secret
is returned exactly once, to the caller it was minted for.
"""

from __future__ import annotations

import asyncio
import secrets
from datetime import timedelta

from eyvan.identity.team_secrets import hash_password, is_team_token, new_secret, token_hash, verify_password
from eyvan.records import ApiTokenRecord, InviteRecord, UserRecord
from eyvan.server.service._control_plane.shared import (
    AuditEvent,
    AuthorizationError,
    ConflictError,
    InputError,
    NotFoundError,
    Permission,
    Principal,
    RoleGrant,
    RoleGrantRecord,
    _new_id,
    utcnow,
)

#: How long an unaccepted invite stays usable.
INVITE_TTL = timedelta(days=7)
#: Bound on how often a successful bearer request rewrites ``last_used_at``.
_TOUCH_INTERVAL = timedelta(minutes=1)


def _id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(8)}"


def _event(actor: str, entity_type: str, entity_id: str, action: str, payload: dict) -> AuditEvent:
    return AuditEvent(
        id=_new_id("evt"),
        actor=actor,
        entity_type=entity_type,
        entity_id=entity_id,
        action=action,
        payload=payload,
    )


class AccountsMixin:
    """Transactional team-account operations."""

    # ------------------------------------------------------------ authority

    def _require_account_administration(self, principal: Principal) -> None:
        if not self.authorizer.allows(principal, Permission.MANAGE_FLEET):  # type: ignore[attr-defined]
            raise AuthorizationError(f"{principal.subject!r} may not administer team accounts")

    # -------------------------------------------------------------- invites

    async def invite_user(
        self,
        principal: Principal,
        *,
        email: str,
        subject: str,
        grants: tuple[RoleGrant, ...] = (),
        ttl: timedelta = INVITE_TTL,
    ) -> tuple[InviteRecord, str]:
        """Create a single-use invite; return it and its raw token (shown once).

        The inviter must be able to issue every grant the invite carries: an
        invite is a deferred grant, so it may not be a way around grant rules.
        """

        if grants:
            for grant in grants:
                self._require_grant_administration(principal, grant)  # type: ignore[attr-defined]
        else:
            self._require_account_administration(principal)
        email = email.strip().lower()
        raw, digest = new_secret("eyvinv_")
        now = utcnow()
        try:
            invite = InviteRecord(
                id=_id("inv"),
                email=email,
                subject=subject,
                grants=grants,
                token_hash=digest,
                created_by=principal.subject,
                created_at=now,
                expires_at=now + ttl,
            )
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            if await tx.get_user(subject) is not None:
                raise ConflictError(f"user {subject!r} already exists")
            if await tx.get_user_by_email(email) is not None:
                raise ConflictError(f"an account for {email} already exists")
            for pending in await tx.list_invites():
                if pending.open_at(now) and (pending.subject == subject or pending.email == email):
                    raise ConflictError(f"an open invite for {email} already exists; revoke it first")
            await tx.put_invite(invite)
            await tx.append_event(
                _event(
                    principal.subject,
                    "invite",
                    invite.id,
                    "invite.created",
                    {"email": email, "subject": subject, "roles": [grant.role.value for grant in grants]},
                )
            )
        return invite, raw

    async def list_invites(self, principal: Principal) -> list[InviteRecord]:
        self._require_account_administration(principal)
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            return await tx.list_invites()

    async def revoke_invite(self, principal: Principal, invite_id: str) -> InviteRecord:
        self._require_account_administration(principal)
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            invite = await tx.get_invite(invite_id)
            if invite is None:
                raise NotFoundError(f"invite {invite_id!r} not found")
            if invite.revoked or invite.accepted_at is not None:
                return invite
            revoked = invite.model_copy(update={"revoked": True})
            await tx.put_invite(revoked)
            await tx.append_event(_event(principal.subject, "invite", invite_id, "invite.revoked", {}))
            return revoked

    async def preview_invite(self, raw: str) -> InviteRecord | None:
        """Return the still-open invite a raw token names, or ``None`` (no detail on why)."""

        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            invite = await tx.get_invite_by_token_hash(token_hash(raw))
        return invite if invite is not None and invite.open_at(utcnow()) else None

    async def accept_invite(
        self, raw: str, *, password: str, display_name: str = "", session_ttl_s: int
    ) -> tuple[UserRecord, ApiTokenRecord, str]:
        """Create the account, issue the invite's grants, and sign the user in."""

        encoded = await asyncio.to_thread(hash_password, password)
        digest = token_hash(raw)
        now = utcnow()
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            invite = await tx.get_invite_by_token_hash(digest)
            if invite is None or not invite.open_at(now):
                raise NotFoundError("this invite is invalid, expired or already used")
            if await tx.get_user(invite.subject) is not None or await tx.get_user_by_email(invite.email) is not None:
                raise ConflictError("an account for this invite already exists")
            user = UserRecord(
                subject=invite.subject,
                email=invite.email,
                display_name=display_name.strip()[:128],
                password_hash=encoded,
                created_by=invite.created_by,
                created_at=now,
                updated_at=now,
            )
            await tx.put_user(user)
            await tx.put_invite(invite.model_copy(update={"accepted_at": now}))
            for grant in invite.grants:
                await tx.put_role_grant(
                    RoleGrantRecord(
                        subject=user.subject,
                        grant=grant,
                        reason=f"invite {invite.id}",
                        granted_by=invite.created_by,
                        granted_at=now,
                    )
                )
            record, session = self._mint_token(user.subject, name="dashboard", kind="session", ttl_s=session_ttl_s)
            await tx.put_api_token(record)
            await tx.append_event(
                _event(user.subject, "user", user.subject, "user.created", {"invite": invite.id, "email": user.email})
            )
        return user, record, session

    # --------------------------------------------------------------- sign-in

    @staticmethod
    def _mint_token(subject: str, *, name: str, kind: str, ttl_s: float | None) -> tuple[ApiTokenRecord, str]:
        raw, digest = new_secret()
        now = utcnow()
        record = ApiTokenRecord(
            id=_id("tok"),
            subject=subject,
            name=name,
            kind=kind,  # type: ignore[arg-type]
            token_hash=digest,
            prefix=raw[:10],
            created_at=now,
            expires_at=None if ttl_s is None else now + timedelta(seconds=ttl_s),
        )
        return record, raw

    async def password_login(
        self, email: str, password: str, *, session_ttl_s: int, token_name: str | None = None
    ) -> tuple[UserRecord, ApiTokenRecord, str] | None:
        """Return a new credential for valid credentials, else ``None``.

        Without ``token_name`` this is a dashboard session (expires with the
        session TTL); with it, a named personal ``api`` token for a CLI or script.
        A missing account, a disabled one and a wrong password look identical to
        the caller and take the same time.
        """

        # ``email`` is whatever the person typed: the address from their invite
        # or the user name they were given. Nobody is ever emailed, so the name
        # is often the only thing they remember.
        identifier = email.strip().lower()
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            if "@" in identifier:
                user = await tx.get_user_by_email(identifier)
            else:
                user = await tx.get_user(identifier)
        stored = user.password_hash if user is not None and not user.disabled else None
        if not await asyncio.to_thread(verify_password, password, stored):
            return None
        assert user is not None
        if token_name is None:
            record, raw = self._mint_token(user.subject, name="dashboard", kind="session", ttl_s=session_ttl_s)
        else:
            try:
                record, raw = self._mint_token(user.subject, name=token_name.strip(), kind="api", ttl_s=None)
            except ValueError as exc:
                raise InputError(str(exc)) from exc
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            await tx.put_api_token(record)
            await tx.append_event(
                _event(user.subject, "user", user.subject, "user.signed_in", {"kind": record.kind, "token": record.id})
            )
        return user, record, raw

    async def resolve_team_token(self, raw: str) -> Principal | None:
        """Return the principal a team bearer token authenticates, or ``None``."""

        if not is_team_token(raw):
            return None
        now = utcnow()
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            record = await tx.get_api_token_by_hash(token_hash(raw))
            if record is None or not record.active_at(now):
                return None
            user = await tx.get_user(record.subject)
            if user is None or user.disabled:
                return None
            if record.last_used_at is None or now - record.last_used_at > _TOUCH_INTERVAL:
                await tx.put_api_token(record.model_copy(update={"last_used_at": now}))
        return Principal(subject=user.subject, display_name=user.display_name or None)

    async def end_session(self, raw: str) -> None:
        """Revoke the token behind a sign-out; unknown tokens are ignored."""

        if not is_team_token(raw):
            return
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            record = await tx.get_api_token_by_hash(token_hash(raw))
            if record is not None and record.revoked_at is None:
                await tx.put_api_token(record.model_copy(update={"revoked_at": utcnow()}))

    async def get_team_user(self, subject: str) -> UserRecord | None:
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            return await tx.get_user(subject)

    # ------------------------------------------------------------ API tokens

    async def create_api_token(
        self, principal: Principal, *, name: str, expires_in_days: int | None = None
    ) -> tuple[ApiTokenRecord, str]:
        """Mint a personal token for the caller's own account."""

        if expires_in_days is not None and not 1 <= expires_in_days <= 3650:
            raise InputError("expires_in_days must be between 1 and 3650")
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            user = await tx.get_user(principal.subject)
            if user is None or user.disabled:
                raise AuthorizationError("personal API tokens need a team account; sign in with one")
            try:
                record, raw = self._mint_token(
                    user.subject,
                    name=name.strip(),
                    kind="api",
                    ttl_s=None if expires_in_days is None else expires_in_days * 86400,
                )
            except ValueError as exc:
                raise InputError(str(exc)) from exc
            await tx.put_api_token(record)
            await tx.append_event(
                _event(principal.subject, "api_token", record.id, "api_token.created", {"name": record.name})
            )
        return record, raw

    async def list_api_tokens(self, principal: Principal, *, subject: str | None = None) -> list[ApiTokenRecord]:
        """List personal (``api``) tokens: one's own, or anyone's for an administrator."""

        target = subject or principal.subject
        if target != principal.subject:
            self._require_account_administration(principal)
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            return [item for item in await tx.list_api_tokens(subject=target) if item.kind == "api"]

    async def revoke_api_token(self, principal: Principal, token_id: str) -> ApiTokenRecord:
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            record = await tx.get_api_token(token_id)
            if record is None:
                raise NotFoundError(f"token {token_id!r} not found")
            if record.subject != principal.subject:
                self._require_account_administration(principal)
            if record.revoked_at is not None:
                return record
            revoked = record.model_copy(update={"revoked_at": utcnow()})
            await tx.put_api_token(revoked)
            await tx.append_event(_event(principal.subject, "api_token", token_id, "api_token.revoked", {}))
            return revoked

    # ----------------------------------------------------------------- users

    async def list_team_users(self, principal: Principal) -> list[UserRecord]:
        self._require_account_administration(principal)
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            return await tx.list_users()

    async def set_user_disabled(self, principal: Principal, subject: str, *, disabled: bool) -> UserRecord:
        """Disable or re-enable an account; disabling revokes every token it holds."""

        self._require_account_administration(principal)
        if disabled and subject == principal.subject:
            raise InputError("you cannot disable your own account")
        now = utcnow()
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            user = await tx.get_user(subject)
            if user is None:
                raise NotFoundError(f"user {subject!r} not found")
            updated = user.model_copy(update={"disabled": disabled, "updated_at": now})
            await tx.put_user(updated)
            if disabled:
                for record in await tx.list_api_tokens(subject=subject):
                    if record.revoked_at is None:
                        await tx.put_api_token(record.model_copy(update={"revoked_at": now}))
            await tx.append_event(
                _event(principal.subject, "user", subject, "user.disabled" if disabled else "user.enabled", {})
            )
            return updated


__all__ = ("INVITE_TTL", "AccountsMixin")
