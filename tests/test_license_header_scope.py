# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Header maintenance must not relabel vendored dependencies or scratch files."""

import importlib.util
from pathlib import Path


def test_default_header_scan_is_limited_to_first_party_sources(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts" / "license_headers.py"
    spec = importlib.util.spec_from_file_location("license_headers_scope", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    included = ("eyvan/app.py", "tests/test_app.py", "scripts/tool.py", "examples/demo.py", "dashboard/src/App.tsx")
    excluded = (".pytest_cache/check/site-packages/dependency.py", "dashboard/node_modules/lib/index.ts", "dist/app.py")
    for name in (*included, *excluded):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# original third-party notice\n")
    assert {str(path.relative_to(tmp_path)) for path in module.default_targets(tmp_path)} == set(included)
    for name in excluded:
        assert (tmp_path / name).read_text() == "# original third-party notice\n"


def test_existing_header_preserves_following_directives_and_comments(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts" / "license_headers.py"
    spec = importlib.util.spec_from_file_location("license_headers_preservation", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for name, body in (
        ("suite.tsx", "// @vitest-environment jsdom\n\nexport {};\n"),
        ("block.tsx", "/** @vitest-environment jsdom */\n\nexport {};\n"),
        ("module.py", "# Important module comment\n# mypy: ignore-errors\n\nvalue = 1\n"),
    ):
        path = tmp_path / name
        original = module.header_for(path) + "\n" + body
        path.write_text(original)
        assert module.apply(path) is False
        assert path.read_text() == original
