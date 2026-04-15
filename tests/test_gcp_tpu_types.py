from __future__ import annotations

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
