# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Small deterministic helpers shared by Eyvan CLI commands."""

from __future__ import annotations

import json
import shlex
from pathlib import Path

import click
import yaml
from pydantic import ValidationError

from eyvan._json import JsonValue
from eyvan.fleet_config import EyvanFleetConfig


class JsonRenderer:
    """Stable JSON output used by machine-readable CLI paths."""

    def emit(self, value: JsonValue) -> None:
        """Write one deterministic JSON document to stdout."""

        click.echo(json.dumps(value, indent=2, sort_keys=True))


class FleetConfigLoader:
    """Load and validate public fleet YAML documents."""

    def load(self, path: Path) -> EyvanFleetConfig:
        """Return a validated config or a Click-facing actionable error."""

        try:
            return EyvanFleetConfig.from_yaml(path)
        except (OSError, ValueError, ValidationError, yaml.YAMLError) as exc:
            raise click.ClickException(f"invalid fleet configuration {path}: {exc}") from exc


def parse_environment(values: tuple[str, ...]) -> dict[str, str]:
    """Parse repeated ``KEY=VALUE`` options without accepting ambiguous input."""

    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key or key != key.strip() or not item or item != item.strip():
            raise click.ClickException("--env values must use KEY=VALUE with non-empty trimmed strings")
        result[key] = item
    return result


def shell_command(values: tuple[str, ...]) -> str:
    """Return one safely quoted remote shell command."""

    return shlex.join(values)
