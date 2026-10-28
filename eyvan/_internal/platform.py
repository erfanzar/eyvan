# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Platform-owned Eyvan routing configuration.

This file intentionally owns infrastructure routing such as the default cell
agent. User fleet and autoscale YAML files do not need to know internal service
URLs.
"""

from __future__ import annotations

import stat
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import yaml

from eyvan._internal.json_types import JsonValue, is_json_object, is_json_value

DEFAULT_PLATFORM_CONFIG_PATH = Path("/etc/eyvan/platform.yaml")
DEFAULT_ASSISTANT_MAX_STEPS = 6
MAX_ASSISTANT_STEPS = 20
MAX_ASSISTANT_KEY_BYTES = 4096


@dataclass(frozen=True, slots=True)
class AssistantConfig:
    """The fleet assistant's provider, owned by the plane operator.

    ``url`` is an OpenAI-compatible base (``{url}/chat/completions``). The key
    is read from ``api_key_file`` when the configuration loads and is never
    part of ``repr``, error text or any response.
    """

    url: str
    model: str
    api_key: str | None = field(default=None, repr=False)
    max_steps: int = DEFAULT_ASSISTANT_MAX_STEPS

    def __post_init__(self) -> None:
        parsed = urlsplit(self.url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "assistant url must be an http(s) URL without credentials, query or fragment; "
                "put the key in api_key_file"
            )
        if not self.model.strip() or self.model != self.model.strip():
            raise ValueError("assistant model must be a non-empty trimmed string")
        if isinstance(self.max_steps, bool) or not 1 <= self.max_steps <= MAX_ASSISTANT_STEPS:
            raise ValueError(f"assistant max_steps must be between 1 and {MAX_ASSISTANT_STEPS}")

    @classmethod
    def from_document(cls, value: JsonValue, *, base: Path | None = None) -> AssistantConfig:
        """Parse the ``assistant`` mapping; a relative key path resolves beside the YAML file."""

        if not is_json_object(value):
            raise ValueError("Eyvan platform assistant must be a mapping")
        unknown = sorted(set(value) - {"url", "model", "api_key_file", "max_steps"})
        if unknown:
            raise ValueError(f"unknown Eyvan platform assistant field {unknown[0]!r}")
        url, model = value.get("url"), value.get("model")
        if not isinstance(url, str) or not isinstance(model, str):
            raise ValueError("Eyvan platform assistant needs string url and model")
        max_steps = value.get("max_steps", DEFAULT_ASSISTANT_MAX_STEPS)
        if isinstance(max_steps, bool) or not isinstance(max_steps, int):
            raise ValueError("assistant max_steps must be an integer")
        key_file = value.get("api_key_file")
        api_key = None
        if key_file is not None:
            if not isinstance(key_file, str) or not key_file.strip():
                raise ValueError("assistant api_key_file must be a non-empty path")
            path = Path(key_file).expanduser()
            if not path.is_absolute() and base is not None:
                path = base / path
            api_key = read_assistant_key(path)
        return cls(url=url, model=model, api_key=api_key, max_steps=max_steps)


def read_assistant_key(path: Path) -> str:
    """Read one provider key from an owner-only file; errors never include its contents."""

    try:
        info = path.stat()
    except OSError as exc:
        raise ValueError(f"assistant api_key_file {path} is not readable ({type(exc).__name__})") from None
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"assistant api_key_file {path} is not a regular file")
    if info.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise ValueError(f"assistant api_key_file {path} must be readable by its owner only (chmod 600)")
    if info.st_size > MAX_ASSISTANT_KEY_BYTES:
        raise ValueError(f"assistant api_key_file {path} is larger than {MAX_ASSISTANT_KEY_BYTES} bytes")
    try:
        key = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError(f"assistant api_key_file {path} is not readable ({type(exc).__name__})") from None
    if not key or any(character.isspace() for character in key):
        raise ValueError(f"assistant api_key_file {path} must hold exactly one key")
    return key


@dataclass(frozen=True, slots=True)
class EyvanPlatformConfig:
    """Resolved platform routing defaults available to the control plane."""

    default_agent_endpoint: str | None = None
    #: The fleet assistant's provider; ``None`` keeps every assistant chat route answering 404.
    assistant: AssistantConfig | None = None

    @classmethod
    def from_document(cls, value: JsonValue, *, base: Path | None = None) -> EyvanPlatformConfig:
        """Parse one strict platform YAML mapping."""

        if not is_json_object(value):
            raise ValueError("Eyvan platform YAML must be a mapping")
        allowed = {"version", "agents", "assistant"}
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise ValueError(f"unknown Eyvan platform field {unknown[0]!r}")
        assistant = None
        if value.get("assistant") is not None:
            assistant = AssistantConfig.from_document(value["assistant"], base=base)
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
            return cls(assistant=assistant)
        if not isinstance(default, dict) or set(default) != {"endpoint"}:
            raise ValueError("Eyvan platform default agent must contain only endpoint")
        endpoint = default.get("endpoint")
        if not isinstance(endpoint, str) or not endpoint.strip() or endpoint != endpoint.strip():
            raise ValueError("Eyvan platform default agent endpoint must be a non-empty trimmed string")
        return cls(default_agent_endpoint=endpoint, assistant=assistant)


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
        return EyvanPlatformConfig.from_document(value, base=target.parent)
    except ValueError as exc:
        raise ValueError(f"invalid Eyvan platform configuration {target}: {exc}") from None


__all__ = (
    "DEFAULT_PLATFORM_CONFIG_PATH",
    "AssistantConfig",
    "EyvanPlatformConfig",
    "load_platform_config",
    "read_assistant_key",
)
