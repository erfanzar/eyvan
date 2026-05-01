# Copyright (C) 2026 Erfan Zare Chavoshi.
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Two ways ``eyvan doctor`` was not answering the question it exists for.

The fleet check called ``GET /api/v1/fleets`` with no query parameters, but the
route requires ``org_id`` and ``project_id``. It therefore returned 422 on every
invocation in every scope, so the check always failed, ``doctor`` always exited
non-zero, and whether a fleet was actually submittable was never established.

The existing doctor fake accepts ``**_kwargs`` and drops them, which is why no
test caught it -- the request looked identical with or without the parameters.
These tests record what was sent.

The second gap is drift rather than a defect: in a source checkout the runtime
profile pins one content-addressed wheel and nothing re-stages it when the
source changes, so a run silently deploys whatever was staged last. That bit
this session three separate times, so ``doctor`` now says it out loud.
"""

from __future__ import annotations

import time
import typing as tp
from pathlib import Path

import pytest
from test_doctor import _healthy, _Store

from eyvan.sdk.client.identity import ClientIdentity
from eyvan.sdk.commands.doctor import CheckStatus, DoctorCommandService
from eyvan.sdk.commands.state import CliSession, ContextOverrides
from eyvan.sdk.contexts import EyvanContextConfig


class _RecordingClient:
    """A client that keeps the query parameters each call actually sent."""

    def __init__(self, handler: tp.Callable[[str], tp.Any]) -> None:
        self._handler = handler
        self.calls: list[tuple[str, dict[str, tp.Any]]] = []

    def request(self, method: str, path: str, **kwargs: tp.Any) -> tp.Any:
        del method
        self.calls.append((path, dict(kwargs.get("params") or {})))
        return self._handler(path)


def _session(client: _RecordingClient, *, org: str = "acme", project: str = "research") -> CliSession:
    return CliSession(
        client=client,
        identity=ClientIdentity(),
        context_store=_Store(),
        context_config=EyvanContextConfig(),
        base_url="http://test",
        org_id=org,
        project_id=project,
        overrides=ContextOverrides(),
    )


def test_the_fleet_check_sends_the_scope_the_route_requires() -> None:
    """Without both parameters the route answers 422, never a fleet list."""

    client = _RecordingClient(_healthy())

    report = DoctorCommandService(_session(client)).report()

    fleet_params = next(params for path, params in client.calls if path.endswith("/fleets"))
    assert fleet_params == {"org_id": "acme", "project_id": "research"}
    assert {check.name: check.status for check in report.checks}["submittable-fleet"] is CheckStatus.PASS


def test_the_fleet_check_uses_the_session_scope_not_a_constant() -> None:
    """A different context must ask about that context's fleets."""

    client = _RecordingClient(_healthy())

    DoctorCommandService(_session(client, org="other-org", project="other-project")).report()

    fleet_params = next(params for path, params in client.calls if path.endswith("/fleets"))
    assert fleet_params == {"org_id": "other-org", "project_id": "other-project"}


def test_a_wheel_staged_after_the_source_is_reported_as_current(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No warning when nothing has changed, or the warning becomes noise."""

    profile = tmp_path / "runtime.json"
    profile.write_text("{}", encoding="utf-8")
    monkeypatch.setattr("eyvan.sdk.commands.doctor.DEFAULT_GCP_NATIVE_RUNTIME_PROFILE_PATH", str(profile))

    check = DoctorCommandService(_session(_RecordingClient(_healthy())))._staged_wheel_check()

    assert check.status is CheckStatus.PASS


def test_source_changed_since_staging_is_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The drift that silently deploys old code three times in one session."""

    profile = tmp_path / "runtime.json"
    profile.write_text("{}", encoding="utf-8")
    monkeypatch.setattr("eyvan.sdk.commands.doctor.DEFAULT_GCP_NATIVE_RUNTIME_PROFILE_PATH", str(profile))
    # Backdate the profile so the real package source is newer than the wheel.
    stale = time.time() - 86_400
    import os

    os.utime(profile, (stale, stale))

    check = DoctorCommandService(_session(_RecordingClient(_healthy())))._staged_wheel_check()

    assert check.status is CheckStatus.WARN
    assert "changed since the wheel was staged" in check.detail
    assert check.remedy is not None and "runtime init" in check.remedy


def test_no_runtime_profile_is_not_a_finding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Most deployments never stage a native GCP worker at all."""

    monkeypatch.setattr(
        "eyvan.sdk.commands.doctor.DEFAULT_GCP_NATIVE_RUNTIME_PROFILE_PATH",
        str(tmp_path / "absent.json"),
    )

    check = DoctorCommandService(_session(_RecordingClient(_healthy())))._staged_wheel_check()

    assert check.status is CheckStatus.PASS
