# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""`eyvan ask` / `size` / `requests` against a real in-process plane and a fake provider.

Only the network hop is replaced. The key point: a drafted workload is submitted
only when the user confirms it (`--yes` or an interactive yes), never otherwise.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest
from click.testing import CliRunner
from starlette.testclient import TestClient

from eyvan.config import EyvanSettings
from eyvan.sdk import cli
from eyvan.sdk.commands.http import EyvanApiError
from eyvan.server.api.app import create_app

PLANE = "http://127.0.0.1:8080"
ALICE = ["--user", "alice", "--role", "project_submitter", "--org", "acme", "--project", "r"]
ADMIN_HEADERS = {"X-Eyvan-User": "root", "X-Eyvan-Roles": "fleet_admin"}
DRAFT = {"name": "llama-sft", "command": "python train.py", "accelerator_type": "v5p-8", "tpu_chips": 4}


def _draft_script() -> list[dict]:
    call = {"id": "c1", "type": "function", "function": {"name": "draft_workload", "arguments": json.dumps(DRAFT)}}
    return [
        {"role": "assistant", "content": None, "tool_calls": [call]},
        {"role": "assistant", "content": "Here is a draft."},
    ]


@pytest.fixture
def plane(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("EYVAN_CONFIG_FILE", str(tmp_path / "contexts.json"))
    for name in ("EYVAN_URL", "EYVAN_TOKEN", "EYVAN_ORG", "EYVAN_PROJECT", "EYVAN_DEV_USER", "EYVAN_ROLE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(cli, "discover_node_context", lambda *a, **k: None)
    monkeypatch.setattr(cli, "discover_node_token_or_warn", lambda *a, **k: None)
    settings = EyvanSettings(
        dev_auth_enabled=True,
        team_mode=True,
        controller_enabled=False,
        artifact_token_key="k" * 32,
        assistant_url="http://provider.test/v1",
        assistant_model="test-model",
        assistant_api_key="sk-secret",
    )
    app = create_app(settings=settings)
    script: list[dict] = []
    app.state.assistant_transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            json={"choices": [{"message": script.pop(0) if script else {"role": "assistant", "content": "ok"}}]},
        )
    )

    class ForwardingClient:
        def __init__(self, url: str, *, headers) -> None:
            self.headers = dict(headers)
            self.http = TestClient(app, base_url=url)

        def request(self, method, path, *, params=None, json=None, headers=None, content=None):
            response = self.http.request(
                method, path, params=params, json=json, headers={**self.headers, **(headers or {})}
            )
            if response.status_code >= 400:
                raise EyvanApiError(response.status_code, response.text)
            return response.json()

    monkeypatch.setattr(cli, "EyvanClient", ForwardingClient)
    return SimpleNamespace(app=app, script=script)


def _workloads(plane) -> list:
    response = TestClient(plane.app, base_url=PLANE).get(
        "/api/v1/workloads", params={"org_id": "acme", "project_id": "r"}, headers=ADMIN_HEADERS
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_ask_shows_a_draft_and_does_not_submit_without_confirmation(plane) -> None:
    plane.script.extend(_draft_script())
    result = CliRunner().invoke(cli.main, [*ALICE, "ask", "draft", "my", "job"])
    assert result.exit_code == 0, result.output
    assert "Here is a draft." in result.output and '"name": "llama-sft"' in result.output
    assert "not submitted" in result.output
    assert _workloads(plane) == []


def test_ask_submits_the_draft_only_with_yes(plane) -> None:
    plane.script.extend(_draft_script())
    result = CliRunner().invoke(cli.main, [*ALICE, "ask", "--yes", "draft", "my", "job"])
    assert result.exit_code == 0, result.output
    [workload] = _workloads(plane)
    assert f"submitted {workload['id']}" in result.output
    assert workload["created_by"] == "alice" and workload["spec"]["name"] == "llama-sft"


def test_size_and_requests_commands(plane, tmp_path) -> None:
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "hidden_size": 4096,
                "num_hidden_layers": 32,
                "intermediate_size": 14336,
                "vocab_size": 128256,
                "num_attention_heads": 32,
                "num_key_value_heads": 8,
            }
        )
    )
    sized = CliRunner().invoke(cli.main, [*ALICE, "size", "--config", str(config), "--trainer", "sft"])
    assert sized.exit_code == 0, sized.output
    assert "8,030" in sized.output and "recommended:" in sized.output

    filed = CliRunner().invoke(cli.main, [*ALICE, "requests", "file", "capacity", "need a v5p-32"])
    assert filed.exit_code == 0, filed.output
    request_id = filed.output.split()[-1]
    listed = CliRunner().invoke(cli.main, [*ALICE, "requests", "list"])
    assert request_id in listed.output and "open" in listed.output
    refused = CliRunner().invoke(cli.main, [*ALICE, "requests", "approve", request_id])
    assert refused.exit_code != 0
    withdrawn = CliRunner().invoke(cli.main, [*ALICE, "requests", "withdraw", request_id])
    assert withdrawn.exit_code == 0, withdrawn.output
    assert f"{request_id} withdrawn" in withdrawn.output
