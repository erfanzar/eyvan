# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Optional real-browser check of the team-account screens against a disposable plane.

Runs tests/browser/team_dashboard.mjs in Chromium against a real team-mode app
serving the built dashboard. Skips (never installs anything) when Node, the
dashboard's Playwright, a built dashboard or a launchable browser is missing.
"""

from __future__ import annotations

import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest
import uvicorn

from eyvan.config import EyvanSettings
from eyvan.server.api.app import create_app

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tests" / "browser" / "team_dashboard.mjs"

pytestmark = pytest.mark.slow


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_team_screens_work_in_a_real_browser(tmp_path, monkeypatch) -> None:
    node = shutil.which("node")
    if node is None or not (ROOT / "dashboard/node_modules/playwright/index.mjs").is_file():
        pytest.skip("optional browser regression requires installed dashboard Playwright and Node")
    if not (ROOT / "dashboard/dist/index.html").is_file():
        pytest.skip("optional browser regression requires a built dashboard (npm --prefix dashboard run build)")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    port = _free_port()
    settings = EyvanSettings(
        # Development headers only seed the first invite; the browser never sends them.
        dev_auth_enabled=True,
        team_mode=True,
        controller_enabled=False,
        artifact_token_key="k" * 32,
        artifact_store_uri=str(tmp_path / "artifacts"),
        public_url=f"http://127.0.0.1:{port}",
    )
    server = uvicorn.Server(
        uvicorn.Config(create_app(settings=settings), host="127.0.0.1", port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 30
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        assert server.started, "the disposable plane did not start"
        completed = subprocess.run(
            [node, str(SCRIPT), f"http://127.0.0.1:{port}"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
    finally:
        server.should_exit = True
        thread.join(timeout=10)
    # Keep the per-step PASS/FAIL lines visible under `pytest -s` for diagnosis.
    print(completed.stdout)
    if completed.returncode == 77:
        pytest.skip("optional browser regression requires a preinstalled Chromium or Chrome; no installation attempted")
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert "RESULT passed" in completed.stdout
