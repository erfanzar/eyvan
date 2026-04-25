# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Typed dependencies and value objects shared by Eyvan CLI commands."""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass
from pathlib import Path

from eyvan._json import JsonObject, JsonValue
from eyvan.client.identity import ClientIdentity
from eyvan.contexts import (
    DEFAULT_ORG_ID,
    DEFAULT_PROJECT_ID,
    EyvanContext,
    EyvanContextConfig,
    FileContextStore,
)
from eyvan.fleet_config import validate_fleet_name


class ApiClient(tp.Protocol):
    """Minimal control-plane transport required by CLI commands."""

    def request(
        self,
        method: str,
        path: str,
        *,
        params: tp.Mapping[str, str | int | float | bool | None] | None = None,
        json: JsonObject | None = None,
        headers: tp.Mapping[str, str] | None = None,
    ) -> JsonValue:
        """Send a JSON request and return its decoded response."""

        ...

    def request_text(
        self,
        method: str,
        path: str,
        *,
        params: tp.Mapping[str, str | int | float | bool | None] | None = None,
        headers: tp.Mapping[str, str] | None = None,
    ) -> str:
        """Send a request and return its text response."""

        ...

    def replace_identity_headers(self, headers: tp.Mapping[str, str]) -> None:
        """Replace the authentication headers used for subsequent API requests."""

        ...


@dataclass(frozen=True, slots=True)
class ContextOverrides:
    """Explicit CLI or environment values that outrank persisted context."""

    base_url: str | None = None
    org_id: str | None = None
    project_id: str | None = None


@dataclass(frozen=True, slots=True)
class WorkloadScope:
    """Logical authorization scope for one workload submission."""

    org_id: str
    project_id: str


@dataclass(frozen=True, slots=True)
class RunTarget:
    """Resolved fleet and pool selected by a shorthand or explicit target."""

    fleet: str
    pool: str

    @classmethod
    def parse(cls, value: str, *, default_fleet: str | None = None) -> RunTarget:
        """Parse ``TYPE`` or ``FLEET/TYPE`` into a validated target."""

        parts = value.split("/")
        if len(parts) == 1 and parts[0] and default_fleet is not None:
            fleet, pool = default_fleet, parts[0]
        elif len(parts) == 2 and all(parts):
            fleet, pool = parts
        else:
            raise ValueError("run target must be TYPE or FLEET/TYPE")
        try:
            validate_fleet_name(fleet)
            validate_fleet_name(pool)
        except ValueError as exc:
            raise ValueError(f"invalid run target: {exc}") from exc
        return cls(fleet=fleet, pool=pool)


@dataclass(slots=True)
class CliSession:
    """All typed state and dependencies available to one CLI invocation."""

    client: ApiClient
    identity: ClientIdentity
    context_store: FileContextStore
    context_config: EyvanContextConfig
    base_url: str
    org_id: str | None
    project_id: str | None
    overrides: ContextOverrides

    @property
    def active_context(self) -> EyvanContext | None:
        """Return the active persisted context for this invocation."""

        return self.context_config.active

    @property
    def autoscale_cache_dir(self) -> Path | None:
        """Return the active fleet's selected autoscale cache root, if any."""

        active = self.active_context
        if active is None or active.autoscale_cache_dir is None:
            return None
        return Path(active.autoscale_cache_dir)

    def workload_scope(self, *, org_id: str | None = None, project_id: str | None = None) -> WorkloadScope:
        """Resolve explicit, active, or conventional default workload scope."""

        return WorkloadScope(
            org_id=org_id or self.org_id or DEFAULT_ORG_ID,
            project_id=project_id or self.project_id or DEFAULT_PROJECT_ID,
        )

    def workload_headers(self, scope: WorkloadScope) -> dict[str, str]:
        """Return request-local development headers for an explicitly selected scope."""

        if self.identity.token is not None or self.identity.dev_user is None:
            return {}
        return ClientIdentity(
            dev_user=self.identity.dev_user,
            dev_org=scope.org_id,
            dev_project=scope.project_id,
            dev_roles=self.identity.dev_roles,
            local_development_credential=self.identity.local_development_credential,
        ).headers()

    def replace_identity(self, identity: ClientIdentity) -> None:
        """Apply a refreshed identity to this session and its concrete API client."""

        self.client.replace_identity_headers(identity.headers())
        self.identity = identity

    def activate_fleet(
        self,
        fleet: str,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
        autoscale_cache_dir: Path | None = None,
    ) -> EyvanContext:
        """Persist ``fleet`` as active without borrowing another fleet's scope."""

        existing = self.context_config.contexts.get(fleet)
        context = EyvanContext(
            fleet=fleet,
            org_id=(
                org_id or self.overrides.org_id or (existing.org_id if existing is not None else None) or DEFAULT_ORG_ID
            ),
            project_id=(
                project_id
                or self.overrides.project_id
                or (existing.project_id if existing is not None else None)
                or DEFAULT_PROJECT_ID
            ),
            base_url=self.base_url,
            autoscale_cache_dir=(
                str(autoscale_cache_dir)
                if autoscale_cache_dir is not None
                else (existing.autoscale_cache_dir if existing is not None else None)
            ),
        )
        self.context_config = self.context_store.activate(context)
        self.org_id = context.org_id
        self.project_id = context.project_id
        return context

    def select_context(
        self,
        fleet: str,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
    ) -> EyvanContext:
        """Select a named fleet, preserving its saved endpoint and scope defaults."""

        existing = self.context_config.contexts.get(fleet)
        selected_org = (
            org_id
            or self.overrides.org_id
            or (existing.org_id if existing is not None else self.org_id)
            or DEFAULT_ORG_ID
        )
        selected_project = (
            project_id or self.overrides.project_id or (existing.project_id if existing is not None else self.project_id)
        ) or DEFAULT_PROJECT_ID
        context = EyvanContext(
            fleet=fleet,
            org_id=selected_org,
            project_id=selected_project,
            base_url=self.overrides.base_url or (existing.base_url if existing is not None else self.base_url),
            autoscale_cache_dir=existing.autoscale_cache_dir if existing is not None else None,
        )
        self.context_config = self.context_store.activate(context)
        self.base_url = context.base_url
        self.org_id = context.org_id
        self.project_id = context.project_id
        return context
