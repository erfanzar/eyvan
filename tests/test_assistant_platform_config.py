# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The assistant is configured in the operator's platform file, never by a new environment knob.

The key is read from an owner-only file named by ``api_key_file``; it must never
appear in ``repr``, error text, or anything a client can read.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from eyvan._internal.platform import AssistantConfig, EyvanPlatformConfig, load_platform_config
from eyvan.config import EyvanSettings

KEY = "sk-platform-file-secret-0123456789"


def _write_key(path: Path, mode: int = 0o600, text: str = KEY) -> Path:
    path.write_text(text + "\n", encoding="utf-8")
    path.chmod(mode)
    return path


def _platform(tmp_path: Path, body: str) -> Path:
    target = tmp_path / "platform.yaml"
    target.write_text(body, encoding="utf-8")
    return target


def test_absent_section_means_no_assistant(tmp_path: Path) -> None:
    assert load_platform_config(_platform(tmp_path, "version: 1\n")).assistant is None
    assert load_platform_config(tmp_path / "missing.yaml").assistant is None


def test_relative_key_file_resolves_beside_the_yaml_and_stays_out_of_repr(tmp_path: Path) -> None:
    _write_key(tmp_path / "assistant.key")
    config = load_platform_config(
        _platform(
            tmp_path,
            "version: 1\nassistant:\n  url: https://llm.example/v1\n  model: m1\n"
            "  api_key_file: assistant.key\n  max_steps: 4\n",
        )
    )
    assert config.assistant == AssistantConfig(url="https://llm.example/v1", model="m1", api_key=KEY, max_steps=4)
    assert KEY not in repr(config)
    assert config.default_agent_endpoint is None


def test_assistant_coexists_with_the_default_agent(tmp_path: Path) -> None:
    config = load_platform_config(
        _platform(
            tmp_path,
            "version: 1\nagents:\n  default:\n    endpoint: http://agent:8090\n"
            "assistant:\n  url: http://llm.local/v1\n  model: m\n",
        )
    )
    assert config.default_agent_endpoint == "http://agent:8090"
    assert config.assistant is not None and config.assistant.api_key is None


@pytest.mark.parametrize("mode", [0o640, 0o604, 0o644])
def test_a_key_file_readable_by_others_is_refused_without_its_contents(tmp_path: Path, mode: int) -> None:
    _write_key(tmp_path / "k", mode=mode)
    target = _platform(tmp_path, "assistant:\n  url: http://llm/v1\n  model: m\n  api_key_file: k\n")
    with pytest.raises(ValueError, match="owner only") as raised:
        load_platform_config(target)
    assert KEY not in str(raised.value)


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("assistant:\n  url: http://llm/v1\n  model: m\n  api_key: inline-secret\n", "unknown Eyvan platform assistant"),
        ("assistant:\n  url: http://user:pw@llm/v1\n  model: m\n", "without credentials"),
        ("assistant:\n  url: http://llm/v1?key=x\n  model: m\n", "without credentials"),
        ("assistant:\n  url: ftp://llm/v1\n  model: m\n", "http\\(s\\)"),
        ("assistant:\n  url: http://llm/v1\n  model: ' m'\n", "trimmed"),
        ("assistant:\n  url: http://llm/v1\n  model: m\n  max_steps: 0\n", "between 1 and"),
        ("assistant:\n  url: http://llm/v1\n  model: m\n  max_steps: true\n", "integer"),
        ("assistant:\n  url: http://llm/v1\n  model: m\n  api_key_file: nope\n", "not readable"),
        ("assistant: [1]\n", "must be a mapping"),
        ("assistant:\n  model: m\n", "string url and model"),
    ],
)
def test_invalid_assistant_sections_are_refused(tmp_path: Path, body: str, message: str) -> None:
    with pytest.raises(ValueError, match=message) as raised:
        load_platform_config(_platform(tmp_path, body))
    assert "inline-secret" not in str(raised.value)


def test_a_key_file_with_more_than_one_token_is_refused(tmp_path: Path) -> None:
    _write_key(tmp_path / "k", text=f"{KEY} second")
    with pytest.raises(ValueError, match="exactly one key") as raised:
        load_platform_config(_platform(tmp_path, "assistant:\n  url: http://llm/v1\n  model: m\n  api_key_file: k\n"))
    assert KEY not in str(raised.value)


def test_settings_have_no_assistant_fields() -> None:
    names = set(EyvanSettings.__dataclass_fields__)
    assert not {name for name in names if name.startswith("assistant")}
    assert EyvanPlatformConfig().assistant is None


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.mark.slow
def test_a_real_eyvan_server_reads_the_assistant_from_the_platform_file(tmp_path: Path) -> None:
    _write_key(tmp_path / "assistant.key")
    platform = _platform(
        tmp_path,
        "version: 1\nassistant:\n  url: http://127.0.0.1:9/v1\n  model: real-process-model\n"
        "  api_key_file: assistant.key\n",
    )
    port = _free_port()
    home = tmp_path / "home"
    home.mkdir()
    environment = {key: value for key, value in os.environ.items() if not key.startswith("EYVAN_") and key != "HOME"}
    environment.update(
        HOME=str(home),
        EYVAN_PLATFORM_CONFIG_PATH=str(platform),
        EYVAN_LOCAL_STATE_PATH=str(tmp_path / "control-plane.json"),
        # Explicit header dev auth, so the probe needs no stored local credential.
        EYVAN_DEV_AUTH_ENABLED="1",
    )
    log = tmp_path / "server.log"
    with log.open("wb") as sink:
        process = subprocess.Popen(
            [sys.executable, "-m", "eyvan.server.api.app", "--host", "127.0.0.1", "--port", str(port)],
            env=environment,
            stdout=sink,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 60
            status = None
            while time.monotonic() < deadline and process.poll() is None:
                try:
                    status = httpx.get(
                        f"http://127.0.0.1:{port}/api/v1/assistant/status",
                        headers={
                            "X-Eyvan-User": "probe",
                            "X-Eyvan-Roles": "viewer",
                            "X-Eyvan-Org": "default",
                            "X-Eyvan-Project": "default",
                        },
                        timeout=2,
                    )
                    if status.status_code != 503:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.3)
            assert status is not None, log.read_text(errors="replace")[-2000:]
            assert status.status_code == 200, status.text
            assert status.json() == {"enabled": True, "model": "real-process-model"}
            assert KEY not in status.text
        finally:
            process.terminate()
            process.wait(timeout=20)
    assert KEY not in log.read_text(errors="replace")
