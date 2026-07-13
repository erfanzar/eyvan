# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The monorepo proof inputs are tracked templates, not operator-local state."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest
import yaml

from eyvan.capacity.fleet_config import EyvanFleetConfig

_MONOREPO = Path(__file__).resolve().parents[3]
_CONFIGS = _MONOREPO / "configs"
_SCRIPTS = _MONOREPO / "scripts"
_VALUES = {
    "GCP_PROJECT_ID": "example-eyvan-project",
    "GCS_BUCKET": "example-eyvan-artifacts",
    "OCI_IMAGE": ("us-east5-docker.pkg.dev/example-eyvan-project/eyvan/easydel-oci-smoke@sha256:" + "0" * 64),
}


def _monorepo_file(path: Path) -> Path:
    if not path.exists():
        pytest.skip("proof inputs live in the EasyDeL monorepo, not the standalone Eyvan checkout")
    return path


def _renderer() -> ModuleType:
    path = _monorepo_file(_SCRIPTS / "render_eyvan_config.py")
    spec = importlib.util.spec_from_file_location("render_eyvan_config_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_every_gcp_proof_config_is_a_sanitized_renderable_template(tmp_path: Path) -> None:
    templates = sorted(_monorepo_file(_CONFIGS).glob("eyvan-*.yaml.in"))
    assert templates, "the source-of-truth checkout contains no reproducible Eyvan templates"
    renderer = _renderer()

    for template in templates:
        source = template.read_text(encoding="utf-8")
        parsed_template = yaml.safe_load(source)
        assert parsed_template["provider"]["project"] == "{{GCP_PROJECT_ID}}"
        assert "/Users/" not in source
        assert all(bucket == "{{GCS_BUCKET}}" for bucket in re.findall(r"gs://([^/]+)", source))
        rendered = renderer.render_template(template, overrides=_VALUES)
        destination = tmp_path / template.name.removesuffix(".in")
        destination.write_text(rendered, encoding="utf-8")
        loaded = EyvanFleetConfig.from_yaml(destination)
        assert loaded.provider.project == _VALUES["GCP_PROJECT_ID"]


def test_local_proof_config_needs_no_operator_identity() -> None:
    path = _monorepo_file(_CONFIGS / "eyvan-proof-local.yaml")
    source = path.read_text(encoding="utf-8")
    assert "{{" not in source
    assert EyvanFleetConfig.from_yaml(path).provider.type == "local"


def test_mutating_live_harnesses_require_an_explicit_acknowledgement() -> None:
    harnesses = (
        "eyvan_easydel_acceptance.py",
        "eyvan_easydel_v5p16_acceptance.py",
        "eyvan_live_v6e16_oci.py",
        "eyvan_native_v5p16_live.py",
    )
    for name in harnesses:
        source = _monorepo_file(_SCRIPTS / name).read_text(encoding="utf-8")
        assert "confirm-provider-mutations" in source or "EYVAN_LIVE_CONFIRM_PROVIDER_MUTATIONS" in source
