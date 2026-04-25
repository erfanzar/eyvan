# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Platform-owned Eyvan routing configuration.

This file intentionally owns infrastructure routing such as the default cell
agent. User fleet and autoscale YAML files do not need to know internal service
URLs.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from eyvan._json import JsonValue, is_json_object, is_json_value

DEFAULT_PLATFORM_CONFIG_PATH = Path("/etc/eyvan/platform.yaml")


@dataclass(frozen=True, slots=True)
class EyvanPlatformConfig:
    """Resolved platform routing defaults available to the control plane."""

    default_agent_endpoint: str | None = None

    @classmethod
    def from_document(cls, value: JsonValue) -> EyvanPlatformConfig:
        """Parse one strict platform YAML mapping."""

        if not is_json_object(value):
            raise ValueError("Eyvan platform YAML must be a mapping")
        allowed = {"version", "agents"}
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise ValueError(f"unknown Eyvan platform field {unknown[0]!r}")
        version = value.get("version", 1)
        if isinstance(version, bool) or not isinstance(version, int) or version != 1:
            raise ValueError("Eyvan platform version must be 1")
        agents = value.get("agents", {})
        if not isinstance(agents, dict):
            raise ValueError("Eyvan platform agents must be a mapping")
        unknown_agents = sorted(set(agents) - {"default"})
        if unknown_agents:
            raise ValueError(f"unknown Eyvan platform agent {unknown_agents[0]!r}")
        default = agents.get("default")
        if default is None:
            return cls()
        if not isinstance(default, dict) or set(default) != {"endpoint"}:
            raise ValueError("Eyvan platform default agent must contain only endpoint")
        endpoint = default.get("endpoint")
        if not isinstance(endpoint, str) or not endpoint.strip() or endpoint != endpoint.strip():
            raise ValueError("Eyvan platform default agent endpoint must be a non-empty trimmed string")
        return cls(default_agent_endpoint=endpoint)


def load_platform_config(path: Path | str = DEFAULT_PLATFORM_CONFIG_PATH) -> EyvanPlatformConfig:
    """Load a platform YAML file, treating an absent optional file as no default."""

    target = Path(path).expanduser()
    if not target.exists():
        return EyvanPlatformConfig()
    try:
        value = yaml.safe_load(target.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"invalid Eyvan platform configuration {target}: {exc}") from exc
    try:
        if not is_json_value(value):
            raise ValueError("Eyvan platform YAML must contain only JSON-compatible values")
        return EyvanPlatformConfig.from_document(value)
    except ValueError as exc:
        raise ValueError(f"invalid Eyvan platform configuration {target}: {exc}") from exc


__all__ = ("DEFAULT_PLATFORM_CONFIG_PATH", "EyvanPlatformConfig", "load_platform_config")
