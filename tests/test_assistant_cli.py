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
    )
    app = create_app(settings=settings)
    created = TestClient(app, base_url=PLANE).post(
        "/api/v1/assistant/providers",
        json={"name": "test", "base_url": "http://provider.test/v1", "api_key": "sk-secret", "models": ["test-model"]},
        headers=ADMIN_HEADERS,
    )
    assert created.status_code == 201, created.text
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


ADMIN = ["--user", "root", "--role", "fleet_admin"]
CLI_KEY = "sk-cli-entered-key-0123456789"


def test_admin_manages_providers_from_the_cli_and_the_key_is_never_printed(plane) -> None:
    sent: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        reply = {"role": "assistant", "content": f"model={json.loads(request.content)['model']}"}
        return httpx.Response(200, json={"choices": [{"message": reply}]})

    plane.app.state.assistant_transport = httpx.MockTransport(record)
    runner = CliRunner()
    added = runner.invoke(
        cli.main,
        [
            *ADMIN,
            "assistant",
            "providers",
            "add",
            "beta",
            "--url",
            "http://beta.test/v1",
            "--models",
            "b-1, b-2",
            "--key-stdin",
        ],
        input=CLI_KEY + "\n",
    )
    assert added.exit_code == 0, added.output
    assert "key set" in added.output and CLI_KEY not in added.output

    refused = runner.invoke(
        cli.main, [*ALICE, "assistant", "providers", "add", "x", "--url", "http://x/v1", "--models", "m"], input=""
    )
    assert refused.exit_code != 0

    admin_list = runner.invoke(cli.main, [*ADMIN, "assistant", "providers", "list"])
    assert "beta" in admin_list.output and "key set" in admin_list.output and CLI_KEY not in admin_list.output
    user_list = runner.invoke(cli.main, [*ALICE, "assistant", "providers", "list"])
    assert "beta" in user_list.output and "http://beta.test" not in user_list.output

    asked = runner.invoke(cli.main, [*ALICE, "ask", "--provider", "BETA", "--model", "b-2", "hello"])
    assert asked.exit_code == 0, asked.output
    assert "model=b-2" in asked.output
    assert str(sent[-1].url) == "http://beta.test/v1/chat/completions"
    assert sent[-1].headers["authorization"] == f"Bearer {CLI_KEY}"
    unknown = runner.invoke(cli.main, [*ALICE, "ask", "--provider", "nope", "hello"])
    assert unknown.exit_code != 0 and "no assistant provider named 'nope'" in unknown.output

    cleared = runner.invoke(cli.main, [*ADMIN, "assistant", "providers", "update", "beta", "--clear-key", "--disable"])
    assert cleared.exit_code == 0, cleared.output
    after = runner.invoke(cli.main, [*ADMIN, "assistant", "providers", "list"])
    assert "disabled" in after.output and "no key" in after.output
    removed = runner.invoke(cli.main, [*ADMIN, "assistant", "providers", "remove", "beta"])
    assert removed.exit_code == 0, removed.output
    assert "beta" not in runner.invoke(cli.main, [*ADMIN, "assistant", "providers", "list"]).output
