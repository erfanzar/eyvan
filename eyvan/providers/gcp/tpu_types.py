"""Neutral Cloud TPU accelerator-type parsing shared by GCP adapters."""

from __future__ import annotations

import re
from dataclasses import dataclass

_ACCELERATOR_TYPE = re.compile(r"^(?P<generation>v[0-9][a-z0-9]*?)-(?P<size>[1-9][0-9]*)$")


@dataclass(frozen=True, slots=True)
class TpuAcceleratorType:
    """The generation and public slice size encoded in a Cloud TPU type."""

    accelerator_type: str
    generation: str
    size: int


def parse_tpu_accelerator_type(value: str) -> TpuAcceleratorType | None:
    """Parse one normalized Cloud TPU accelerator type without applying policy.

    This intentionally does not decide whether a type is supported by Eyvan's
    queued-resource autoscaler. Provider discovery must still report a valid
    GCP type so the autoscale planner can explain why it was skipped.
    """

    matched = _ACCELERATOR_TYPE.fullmatch(value)
    if matched is None:
        return None
    return TpuAcceleratorType(
        accelerator_type=value,
        generation=matched.group("generation"),
        size=int(matched.group("size")),
    )


def accelerator_generation(value: str) -> str | None:
    """Return the normalized generation encoded by a valid accelerator type."""

    parsed = parse_tpu_accelerator_type(value)
    return None if parsed is None else parsed.generation


__all__ = ("TpuAcceleratorType", "accelerator_generation", "parse_tpu_accelerator_type")
