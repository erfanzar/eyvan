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
"""Tests for bounded provider diagnostics exposed to Eyvan users."""

from __future__ import annotations

import pytest

from eyvan.service.provider_diagnostics import safe_provider_diagnostic


def test_safe_provider_diagnostic_extracts_gcloud_message_from_operation_transcript() -> None:
    raw = (
        "Create request issued for: [cell-r1]\\n"
        "Waiting for operation [projects/project-a/locations/us-east5-a/operations/operation-1] to complete...\\n"
        "................................................................................................................................failed.\\n"
        "ERROR: (gcloud.compute.tpus.queued-resources.create) {\\n"
        '  "code": 7,\\n'
        '  "message": "User does not have permission to submit requests into this queue for accelerator type '
        '\\"v6e-16\\" in location us-east5-a. [EID: 0x1]"\\n'
        "}\\n"
    )

    assert safe_provider_diagnostic(raw) == (
        'User does not have permission to submit requests into this queue for accelerator type "v6e-16" '
        "in location us-east5-a. [EID: 0x1]"
    )


def test_safe_provider_diagnostic_keeps_a_single_line_queue_admission_message() -> None:
    assert safe_provider_diagnostic("GCP queued-resource admission denied") == "GCP queued-resource admission denied"


@pytest.mark.parametrize("marker", ("error:", "Error:", "ERROR:"))
def test_safe_provider_diagnostic_matches_the_error_marker_case_insensitively(marker: str) -> None:
    diagnostic = f"{marker} (gcloud.tpus.queued-resources.create) capacity request was rejected by zone policy"
    raw = f"Create request issued for: [cell-r1]\n{diagnostic}"

    assert safe_provider_diagnostic(raw) == diagnostic
