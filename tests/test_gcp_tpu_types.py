from __future__ import annotations

import pytest

from eyvan.providers.gcp.tpu_types import accelerator_generation, parse_tpu_accelerator_type


def test_parser_returns_generation_and_public_slice_size_for_valid_types() -> None:
    parsed = parse_tpu_accelerator_type("v5litepod-8")

    assert parsed is not None
    assert parsed.accelerator_type == "v5litepod-8"
    assert parsed.generation == "v5litepod"
    assert parsed.size == 8
    assert accelerator_generation("v5p-512") == "v5p"


def test_parser_rejects_noncanonical_and_incomplete_accelerator_types() -> None:
    assert parse_tpu_accelerator_type("V5P-16") is None
    assert parse_tpu_accelerator_type("v5p-0") is None
    assert parse_tpu_accelerator_type("v5p") is None
    assert accelerator_generation("not-a-tpu") is None


def _qr(raw: dict):
    from eyvan.providers.gcp.queued_resources import GcpQueuedResourceState, QueuedResource

    return QueuedResource(
        qr_id=raw.get("name", "qr-1"),
        state=GcpQueuedResourceState.ACTIVE,
        node_ids=(),
        accelerator_type=None,
        raw=raw,
    )


def _spec_labels(uid: str = "uid-1", cell: str = "cell-a"):
    from eyvan.providers.gcp.queued_resources import QueuedResourceSpec

    return QueuedResourceSpec(
        name="cell-a",
        accelerator_type="v5p-16",
        zone="us-east5-a",
        project="p",
        capacity_class="spot",
        labels={"eyvan-uid": uid, "eyvan-cell": cell},
        runtime_version="v2-alpha-tpuv5",
    )


def test_an_unlabelled_active_queued_resource_is_not_declared_foreign() -> None:
    """Cloud TPU does not echo labels or metadata back for a provisioned QR.

    Verified against a live ACTIVE v5p-16 on 2026-08-02: the queued resource
    returned no labels at any path, and the TPU node returned ``labels: null``
    and ``metadata: {}`` -- even though the startup script from that same
    metadata had demonstrably run.

    Treating that silence as "someone else owns this" stranded ACTIVE, billing
    capacity Eyvan had created itself, with the workload stuck pending. The id
    was minted by Eyvan under a fence and recorded, which is the remaining
    authority.
    """

    from eyvan.providers.gcp.queued_resources import GcloudTpuProvider

    bare = _qr({"name": "qr-1", "tpu": {"nodeSpec": [{"node": {"acceleratorType": "v5p-16"}}]}})

    GcloudTpuProvider._validate_qr(bare, _spec_labels(), recorded_qr_id="qr-1")


def test_an_unlabelled_queued_resource_we_never_recorded_is_still_refused() -> None:
    """The fallback is our own record, not "assume everything is ours"."""

    from eyvan.providers.gcp.queued_resources import GcloudError, GcloudTpuProvider

    bare = _qr({"name": "qr-1", "tpu": {"nodeSpec": [{"node": {}}]}})

    with pytest.raises(GcloudError, match="not owned"):
        GcloudTpuProvider._validate_qr(bare, _spec_labels(), recorded_qr_id="a-different-qr")


def test_a_labelled_foreign_queued_resource_is_still_refused() -> None:
    """When the provider does answer, its answer stays authoritative."""

    from eyvan.providers.gcp.queued_resources import GcloudError, GcloudTpuProvider

    foreign = _qr({"name": "qr-1", "labels": {"eyvan-uid": "someone-else", "eyvan-cell": "other"}})

    with pytest.raises(GcloudError, match="not owned"):
        GcloudTpuProvider._validate_qr(foreign, _spec_labels(), recorded_qr_id="qr-1")


def test_a_labelled_matching_queued_resource_is_accepted() -> None:
    """The path that already worked must keep working."""

    from eyvan.providers.gcp.queued_resources import GcloudTpuProvider

    ours = _qr({"name": "qr-1", "labels": {"eyvan-uid": "uid-1", "eyvan-cell": "cell-a"}})

    GcloudTpuProvider._validate_qr(ours, _spec_labels(), recorded_qr_id="qr-1")
