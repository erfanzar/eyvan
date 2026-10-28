# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Users' saved assistant conversations.

A conversation belongs to the person who started it. Nobody else -- a fleet
admin included -- can list, read, rename or delete it: someone else's chat is
answered with "not found", exactly like a conversation that never existed.
Messages are bounded, so the oldest turns are dropped once a chat grows past
``MAX_CONVERSATION_MESSAGES``.
"""

from __future__ import annotations

import secrets
import typing as tp

from eyvan.records import (
    MAX_CONVERSATION_MESSAGE_CHARS,
    MAX_CONVERSATION_MESSAGES,
    MAX_CONVERSATION_TRACE_ITEMS,
    AssistantConversationRecord,
    AssistantMessageRecord,
)
from eyvan.server.service._control_plane.shared import (
    InputError,
    NotFoundError,
    Principal,
    utcnow,
)

#: How many conversations one user keeps; the least recently used beyond this are dropped.
MAX_CONVERSATIONS_PER_USER = 200
_TITLE_CHARS = 80


def _title_from(text: str) -> str:
    words = " ".join(text.split())
    if not words:
        return "New chat"
    return words if len(words) <= _TITLE_CHARS else words[: _TITLE_CHARS - 1].rstrip() + "…"


def new_message_id() -> str:
    return f"msg_{secrets.token_hex(8)}"


class AssistantConversationsMixin:
    """Owner-only conversation storage for the assistant."""

    async def _owned_conversation(self, tx: tp.Any, principal: Principal, conversation_id: str) -> tp.Any:
        record = await tx.get_assistant_conversation(conversation_id)
        if record is None or record.subject != principal.subject:
            raise NotFoundError(f"conversation {conversation_id!r} not found")
        return record

    async def list_assistant_conversations(self, principal: Principal) -> list[AssistantConversationRecord]:
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            return list(await tx.list_assistant_conversations(subject=principal.subject))

    async def get_assistant_conversation(
        self, principal: Principal, conversation_id: str
    ) -> AssistantConversationRecord:
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            return tp.cast(AssistantConversationRecord, await self._owned_conversation(tx, principal, conversation_id))

    async def start_assistant_conversation(
        self,
        principal: Principal,
        *,
        first_message: str,
        org_id: str,
        project_id: str,
        provider_id: str | None,
        model: str | None,
    ) -> AssistantConversationRecord:
        """Create an empty conversation titled from the first message."""

        record = AssistantConversationRecord(
            id=f"conv_{secrets.token_hex(8)}",
            subject=principal.subject,
            title=_title_from(first_message),
            org_id=org_id,
            project_id=project_id,
            provider_id=provider_id,
            model=model,
        )
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            owned = await tx.list_assistant_conversations(subject=principal.subject)
            # Newest first: keep room for this one by dropping the least recently used.
            for stale in owned[MAX_CONVERSATIONS_PER_USER - 1 :]:
                await tx.delete_assistant_conversation(stale.id)
            await tx.put_assistant_conversation(record)
        return record

    async def append_assistant_messages(
        self,
        principal: Principal,
        conversation_id: str,
        messages: tp.Sequence[AssistantMessageRecord],
        *,
        provider_id: str | None = None,
        model: str | None = None,
    ) -> AssistantConversationRecord:
        """Add turns to one of the caller's conversations, keeping only the newest messages."""

        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            current = await self._owned_conversation(tx, principal, conversation_id)
            kept = (*current.messages, *messages)[-MAX_CONVERSATION_MESSAGES:]
            update: dict[str, tp.Any] = {"messages": kept, "updated_at": utcnow()}
            if provider_id is not None:
                update["provider_id"] = provider_id
            if model is not None:
                update["model"] = model
            updated = current.model_copy(update=update)
            AssistantConversationRecord.model_validate(updated.model_dump())
            await tx.put_assistant_conversation(updated)
            return tp.cast(AssistantConversationRecord, updated)

    async def rename_assistant_conversation(
        self, principal: Principal, conversation_id: str, *, title: str
    ) -> AssistantConversationRecord:
        cleaned = " ".join(title.split())
        if not cleaned:
            raise InputError("a conversation title cannot be empty")
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            current = await self._owned_conversation(tx, principal, conversation_id)
            try:
                updated = AssistantConversationRecord.model_validate(
                    {**current.model_dump(), "title": cleaned[:200], "updated_at": utcnow()}
                )
            except ValueError as exc:
                raise InputError(str(exc)) from None
            await tx.put_assistant_conversation(updated)
            return updated

    async def delete_assistant_conversation(self, principal: Principal, conversation_id: str) -> None:
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            await self._owned_conversation(tx, principal, conversation_id)
            await tx.delete_assistant_conversation(conversation_id)


def clip_message(text: str) -> str:
    return text if len(text) <= MAX_CONVERSATION_MESSAGE_CHARS else text[: MAX_CONVERSATION_MESSAGE_CHARS - 1] + "…"


def clip_trace(items: tp.Sequence[dict[str, tp.Any]]) -> tuple[dict[str, tp.Any], ...]:
    return tuple(items[-MAX_CONVERSATION_TRACE_ITEMS:])


__all__ = (
    "MAX_CONVERSATIONS_PER_USER",
    "AssistantConversationsMixin",
    "clip_message",
    "clip_trace",
    "new_message_id",
)
