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

import os
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

    monkeypatch.setattr("eyvan.sdk.commands.doctor.stale_components", lambda **_kwargs: ())

    check = DoctorCommandService(_session(_RecordingClient(_healthy())))._source_drift_check()

    assert check.status is CheckStatus.PASS


def test_a_wheel_older_than_the_source_is_detected(tmp_path: Path) -> None:
    """Exercise the real drift computation, not a stub standing in for it.

    ``stale_components`` takes its inputs explicitly precisely so this can be
    asserted without depending on what the machine running it has deployed.
    """

    from eyvan.sdk.commands.deployment import WORKER_WHEEL_COMPONENT, stale_components

    profile = tmp_path / "runtime.json"
    profile.write_text("{}", encoding="utf-8")
    old = time.time() - 86_400
    os.utime(profile, (old, old))

    stale = stale_components(runtime_profile_path=profile, newest_source=time.time(), component_pids={})

    assert stale == (WORKER_WHEEL_COMPONENT,)


def test_a_wheel_newer_than_the_source_is_not_reported(tmp_path: Path) -> None:
    """A current deployment must stay silent or the warning becomes noise."""

    from eyvan.sdk.commands.deployment import stale_components

    profile = tmp_path / "runtime.json"
    profile.write_text("{}", encoding="utf-8")

    stale = stale_components(
        runtime_profile_path=profile,
        newest_source=time.time() - 86_400,
        component_pids={},
    )

    assert stale == ()


def test_a_running_component_older_than_the_source_is_detected(tmp_path: Path, monkeypatch) -> None:
    """The surface that actually bit: a daemon left behind by a partial deploy."""

    from eyvan.sdk.commands import deployment

    profile = tmp_path / "runtime.json"
    profile.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(deployment, "process_started_at", lambda pid: 0.0)

    stale = deployment.stale_components(
        runtime_profile_path=profile,
        newest_source=time.time(),
        component_pids={"cell agent (http://127.0.0.1:8090)": 4242},
    )

    assert "cell agent (http://127.0.0.1:8090)" in stale


def test_no_runtime_profile_is_not_a_finding(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Most deployments never stage a native GCP worker at all."""

    monkeypatch.setattr(
        "eyvan.sdk.commands.doctor.DEFAULT_GCP_NATIVE_RUNTIME_PROFILE_PATH",
        str(tmp_path / "absent.json"),
    )
    monkeypatch.setattr("eyvan.sdk.commands.doctor.stale_components", lambda **_kwargs: ())

    check = DoctorCommandService(_session(_RecordingClient(_healthy())))._source_drift_check()

    assert check.status is CheckStatus.PASS


def test_component_discovery_finds_the_real_pid_records(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise discovery itself, not a stub standing in for it.

    The other drift tests substitute ``_running_component_pids`` so their
    outcome does not depend on what this machine is running -- which means none
    of them would notice if discovery stopped finding anything. It already did
    once: the first version derived each record path from the control plane's
    endpoint, but the cell agent is recorded under *its own* port, so it found
    neither and a broad ``except`` turned that into a green check.
    """

    import json as _json

    from eyvan.server.local import daemon as daemon_module

    for name, port in (("control-plane-8081-abc", 8081), ("cell-agent-8090-def", 8090)):
        (tmp_path / f"{name}.pid").write_text(
            _json.dumps({"endpoint": f"http://127.0.0.1:{port}", "pid": 1000 + port}),
            encoding="utf-8",
        )
    monkeypatch.setattr(
        daemon_module.LocalDaemonPaths,
        "resolved_directory",
        property(lambda self: tmp_path),
    )

    from eyvan.sdk.commands.deployment import running_component_pids

    discovered = running_component_pids()

    assert set(discovered.values()) == {9081, 9090}
    assert any("control plane" in label for label in discovered)
    assert any("cell agent" in label for label in discovered), "the agent is recorded under its own port"
