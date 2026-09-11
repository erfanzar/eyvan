# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Benchmark receipts must be reproducible inputs to a fail-closed CI budget."""

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_benchmark_records_measured_samples_and_checks_placement_results(tmp_path) -> None:
    receipt = tmp_path / "receipt.json"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/benchmark_scheduler.py",
            "--cells",
            "12",
            "--iterations",
            "2",
            "--output",
            str(receipt),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    document = json.loads(receipt.read_text())
    assert document["cells"] == 12
    assert document["iterations"] == 2
    assert set(document["phases"]) == {"snapshot", "schedule", "elastic_pack"}
    for phase in document["phases"].values():
        assert len(phase["samples_ms"]) == 2
        assert phase["p99_ms"] >= phase["p50_ms"] >= 0
    assert document["violations"] == []


def test_a_budget_regression_fails_and_keeps_its_receipt(tmp_path) -> None:
    budget = tmp_path / "budgets.json"
    budget.write_text(json.dumps({"12": {"snapshot": 0, "schedule": 0, "elastic_pack": 0}}))
    receipt = tmp_path / "failed.json"
    result = subprocess.run(
        [
            sys.executable,
            "scripts/benchmark_scheduler.py",
            "--cells",
            "12",
            "--iterations",
            "2",
            "--budget-file",
            str(budget),
            "--output",
            str(receipt),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert json.loads(receipt.read_text())["violations"]
