# Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
