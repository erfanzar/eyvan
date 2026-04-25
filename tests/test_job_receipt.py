# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Terminal presentation tests for durable Eyvan workload controls."""

from __future__ import annotations

import io

from eyvan.job_receipt import JobLifecycleState, JobLifecycleUpdate, JobSubmissionReceipt, JobTerminalOutput


def test_terminal_receipt_uses_a_compact_colored_panel_and_lifecycle_labels() -> None:
    stream = io.StringIO()
    output = JobTerminalOutput(stream=stream, force_terminal=True)

    output.submission(
        JobSubmissionReceipt(
            workload_id="wl_123",
            fleet="research-us-central1-c",
            resource_shape="v6e-16 x 2 workers",
            atomic=True,
        )
    )
    output.lifecycle(
        JobLifecycleUpdate(
            JobLifecycleState.BLOCKED,
            "v6e-16 x 2 workers | us-central1-c",
            "GCP denied queue admission; no queued resource was created",
            "PERMISSION_DENIED: queue admission policy rejected this request",
        )
    )

    rendered = stream.getvalue()
    assert "\x1b[" in rendered
    assert "Eyvan job submitted" in rendered
    assert "v6e-16 x 2 workers" in rendered
    assert "eyvan workload status wl_123" in rendered
    assert "eyvan workload logs wl_123" in rendered
    assert "eyvan workload cancel wl_123" in rendered
    assert "BLOCKED" in rendered
    assert "GCP denied queue admission; no queued resource was created" in rendered
