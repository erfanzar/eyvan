"""Typed startup policy for the Eyvan API server."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace

from eyvan._compat import StrEnum
from eyvan.api.server_bind import ServerBindPolicy
from eyvan.config import EyvanSettings, RepositoryBackend

_DEV_AUTH_ENVIRONMENT_VARIABLE = "EYVAN_DEV_AUTH_ENABLED"
_REPOSITORY_BACKEND_ENVIRONMENT_VARIABLE = "EYVAN_REPOSITORY_BACKEND"


class DevelopmentAuthMode(StrEnum):
    """How development-header authentication was selected for this process."""

    LOOPBACK_DEFAULT = "loopback_default"
    EXPLICIT = "explicit"
    DISABLED = "disabled"


class ServerControllerMode(StrEnum):
    """Whether this API process actively reconciles workloads and capacity."""

    ACTIVE = "active"
    API_ONLY = "api_only"


@dataclass(frozen=True, slots=True)
class ServerStartupSettings:
    """Security-relevant server settings resolved for one bind address."""

    bind: ServerBindPolicy
    settings: EyvanSettings
    development_auth_mode: DevelopmentAuthMode

    @classmethod
    def from_environment(cls, bind: ServerBindPolicy) -> ServerStartupSettings:
        """Apply zero-configuration local defaults only to an unconfigured literal loopback bind."""

        settings = EyvanSettings.from_env()
        if bind.is_loopback and _REPOSITORY_BACKEND_ENVIRONMENT_VARIABLE not in os.environ:
            settings = replace(settings, repository_backend=RepositoryBackend.LOCAL)
        if bind.is_loopback and _DEV_AUTH_ENVIRONMENT_VARIABLE not in os.environ:
            return cls(
                bind=bind,
                settings=replace(settings, dev_auth_enabled=True),
                development_auth_mode=DevelopmentAuthMode.LOOPBACK_DEFAULT,
            )
        if not bind.is_loopback and settings.repository_backend is not RepositoryBackend.POSTGRES:
            raise ValueError(
                "a network-bound Eyvan server requires EYVAN_REPOSITORY_BACKEND=postgres so every replica "
                "shares durable control-plane state"
            )
        return cls.from_settings(bind, settings)

    @classmethod
    def from_settings(cls, bind: ServerBindPolicy, settings: EyvanSettings) -> ServerStartupSettings:
        """Describe explicit settings without changing their authentication policy."""

        if settings.repository_backend is RepositoryBackend.LOCAL and not bind.is_loopback:
            raise ValueError("the local Eyvan repository may only be used with a literal loopback server bind")
        if settings.dev_auth_enabled and not bind.is_loopback:
            raise ValueError(
                "Eyvan development authentication may only be used with a literal loopback server bind; "
                "disable it or configure OIDC for a network-bound deployment"
            )
        mode = DevelopmentAuthMode.EXPLICIT if settings.dev_auth_enabled else DevelopmentAuthMode.DISABLED
        return cls(bind=bind, settings=settings, development_auth_mode=mode)

    @property
    def controller_mode(self) -> ServerControllerMode:
        """Return the controller mode exposed by the health endpoint."""

        return ServerControllerMode.ACTIVE if self.settings.controller_enabled else ServerControllerMode.API_ONLY


__all__ = ("DevelopmentAuthMode", "ServerControllerMode", "ServerStartupSettings")
