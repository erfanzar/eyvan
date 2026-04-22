# Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
