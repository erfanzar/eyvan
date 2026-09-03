# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The standalone repository ships its own CI, and it must not drift.

`.github/` lives at the monorepo root and is not part of the `libs/eyvan`
subtree, so the published repository had no CI at all: every check that guarded
this package ran only in a repository its users never see. These workflows live
inside the package so the subtree split carries them.
"""

from __future__ import annotations

import json
import pathlib
import re

import pytest
import yaml

_EYVAN = pathlib.Path(__file__).resolve().parents[1]
_WORKFLOWS = _EYVAN / ".github" / "workflows"


def _workflow(name: str) -> dict:
    return yaml.safe_load((_WORKFLOWS / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("name", ["ci.yaml", "image.yaml", "publish.yaml"])
def test_every_action_is_pinned_to_a_full_commit(name: str) -> None:
    """A moving action tag is an unreviewed dependency with write access."""

    text = (_WORKFLOWS / name).read_text(encoding="utf-8")
    uses = re.findall(r"^\s*-?\s*uses:\s*([^\s#]+)", text, flags=re.MULTILINE)

    assert uses
    for reference in uses:
        assert re.fullmatch(r"[^@\s]+@[0-9a-f]{40}", reference), reference


def test_the_image_workflow_reads_its_bases_from_the_release_lock() -> None:
    """A base image must only change where the policy verifier looks.

    Writing the digests into the workflow puts them somewhere `validate-policy`
    does not read, so they could drift from the lock with nothing objecting.
    """

    text = (_WORKFLOWS / "image.yaml").read_text(encoding="utf-8")

    assert "oci/release-lock.json" in text
    lock = json.loads((_EYVAN / "oci" / "release-lock.json").read_text(encoding="utf-8"))
    for role in ("control-plane", "agent", "worker"):
        assert role in text, f"the image matrix does not build {role}"
        for value in lock["images"][role]["buildArgs"].values():
            assert value not in text, "a base image digest is hard-coded instead of read from the lock"


def test_no_workflow_publishes_a_moving_tag() -> None:
    """`latest` makes "which image is running" unanswerable after the fact."""

    text = (_WORKFLOWS / "image.yaml").read_text(encoding="utf-8")

    assert ":latest" not in text
    assert "sha-${{ github.sha }}" in text


def test_publishing_carries_no_long_lived_credential() -> None:
    """Trusted publishing means a compromised workflow has nothing to steal."""

    text = (_WORKFLOWS / "publish.yaml").read_text(encoding="utf-8")
    publish = _workflow("publish.yaml")["jobs"]["publish"]

    assert "--trusted-publishing always" in text
    assert publish["permissions"]["id-token"] == "write"
    assert "PYPI" not in text and "password" not in text


def test_the_suite_runs_against_the_production_backend() -> None:
    """A green suite on SQLite says nothing about SERIALIZABLE or asyncpg."""

    python = _workflow("ci.yaml")["jobs"]["python"]

    assert re.fullmatch(r"postgres:17-alpine@sha256:[0-9a-f]{64}", python["services"]["postgres"]["image"])
    steps = json.dumps(python["steps"])
    assert "EYVAN_REQUIRE_POSTGRES" in steps


def test_every_install_is_hash_pinned() -> None:
    """The standalone repository has no lock; the hashed exports are the lock."""

    text = (_WORKFLOWS / "ci.yaml").read_text(encoding="utf-8")

    installs = [line for line in text.splitlines() if "uv pip install" in line and "-r " in line]
    assert installs
    for line in installs:
        assert "--require-hashes" in line, line


@pytest.mark.parametrize("name", ["requirements-docker.txt", "requirements-dev.txt", "requirements-build.txt"])
def test_the_exports_the_workflows_install_from_are_shipped(name: str) -> None:
    """A workflow that installs a file the subtree does not carry cannot run."""

    export = _EYVAN / name

    assert export.is_file(), f"{name} is missing"
    lines = [line for line in export.read_text(encoding="utf-8").splitlines() if line and not line.startswith("#")]
    assert any("--hash=sha256:" in line for line in lines), f"{name} carries no hashes"
