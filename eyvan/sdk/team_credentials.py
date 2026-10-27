# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Owner-only store of personal team tokens, keyed by control-plane URL.

Separate from the context file on purpose: contexts are documented as
non-secret and may be shared or printed, while this file holds bearer
credentials. It is written atomically with mode 0600 and refused when it is
not a regular file owned by this user.
"""

from __future__ import annotations

import json
import os
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path

from eyvan._internal.workspace import create_private_directory

DEFAULT_TEAM_CREDENTIALS_PATH = Path("~/.eyvan/team-credentials.json")
_MAX_BYTES = 1024 * 1024


class TeamCredentialError(RuntimeError):
    """The credential file exists but is unsafe or unreadable."""


@dataclass(frozen=True, slots=True)
class TeamCredential:
    """One signed-in identity on one plane."""

    url: str
    token: str
    token_id: str
    subject: str
    email: str

    def __repr__(self) -> str:  # never print the token
        return f"TeamCredential(url={self.url!r}, subject={self.subject!r}, token_id={self.token_id!r})"


class TeamCredentialStore:
    """Read and atomically replace ``~/.eyvan/team-credentials.json``."""

    def __init__(self, path: Path | str = DEFAULT_TEAM_CREDENTIALS_PATH):
        self.path = Path(path).expanduser()

    def _load(self) -> dict[str, dict[str, str]]:
        try:
            descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            return {}
        except OSError as exc:
            raise TeamCredentialError(f"could not open {self.path}: {exc}") from exc
        with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
                raise TeamCredentialError(f"{self.path} is not a regular file owned by this user")
            if metadata.st_mode & 0o077:
                raise TeamCredentialError(f"{self.path} is readable by other users; run chmod 600 {self.path}")
            text = stream.read(_MAX_BYTES + 1)
        try:
            document = json.loads(text)
        except ValueError as exc:
            raise TeamCredentialError(f"{self.path} is not valid JSON") from exc
        planes = document.get("planes") if isinstance(document, dict) else None
        if not isinstance(planes, dict):
            raise TeamCredentialError(f"{self.path} has no 'planes' map")
        return {str(url): dict(entry) for url, entry in planes.items() if isinstance(entry, dict)}

    def _save(self, planes: dict[str, dict[str, str]]) -> None:
        create_private_directory(self.path.parent)
        temporary = self.path.with_name(f".{self.path.name}-{uuid.uuid4().hex}")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump({"version": 1, "planes": planes}, stream, indent=2, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def get(self, url: str) -> TeamCredential | None:
        entry = self._load().get(url.rstrip("/"))
        if entry is None:
            return None
        try:
            return TeamCredential(
                url=url.rstrip("/"),
                token=entry["token"],
                token_id=entry["token_id"],
                subject=entry["subject"],
                email=entry.get("email", ""),
            )
        except KeyError:
            return None

    def token_for(self, url: str) -> str | None:
        credential = self.get(url)
        return None if credential is None else credential.token

    def put(self, credential: TeamCredential) -> None:
        planes = self._load()
        planes[credential.url.rstrip("/")] = {
            "token": credential.token,
            "token_id": credential.token_id,
            "subject": credential.subject,
            "email": credential.email,
        }
        self._save(planes)

    def remove(self, url: str) -> bool:
        planes = self._load()
        removed = planes.pop(url.rstrip("/"), None) is not None
        if removed:
            self._save(planes)
        return removed


def team_token_or_warn(url: str, warn=None) -> str | None:  # type: ignore[no-untyped-def]
    """Return the stored token for ``url``; report (never raise) an unsafe store."""

    try:
        return TeamCredentialStore().token_for(url)
    except TeamCredentialError as error:
        if warn is not None:
            warn(str(error))
        return None


__all__ = (
    "DEFAULT_TEAM_CREDENTIALS_PATH",
    "TeamCredential",
    "TeamCredentialError",
    "TeamCredentialStore",
    "team_token_or_warn",
)
