# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""One place that knows what an accelerator shape is.

Shape knowledge lived in three places that could disagree. ``fleet_config`` held
a compute-TPU dict and a queued-slice dict; ``autoscale.profiles`` derived a chip
count from the type suffix and then hardcoded ``cpu=32`` for every TPU ever made,
which its own docstring admitted was "the same conservative CPU vector until
host-specific discovery is available"; and every fleet YAML restated the answer
by hand in ``resources:``.

Three sources for one fact is not redundancy, it is three chances to be wrong in
different directions -- and the operator writing the fourth copy into YAML has no
way to know which of the three they are supposed to agree with.

**Not generated from quota discovery.** ``quota_discovery`` reads quota
*metrics*, which give a generation and a unit ceiling and never a host count.
Quota discovery stays the authority for what is *available*; this is the
authority for what a type *is*. They join on the accelerator type.

**Where the catalog stops.** ``hosts`` and ``chips_per_host`` are optional and
recorded only where this repository grounds them. Making them mandatory would
mean inventing topology for generations nothing here describes, and a wrong host
count makes Eyvan reject a slice the provider built correctly -- a worse failure
than not checking, because it looks like a provider fault.
"""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

#: Schema version this build understands. A catalog declaring anything else is
#: refused rather than partially read: a build silently ignoring fields it does
#: not know would size pools from half a description.
SUPPORTED_CATALOG_VERSION = 1

CATALOG_PATH = Path(__file__).resolve().parent / "data" / "accelerator_shapes.yaml"


class HardwareCatalogError(ValueError):
    """Raised when the catalog cannot be loaded or does not describe itself."""


@dataclass(frozen=True, slots=True)
class AcceleratorShape:
    """The dimensions of one accelerator type.

    ``chips`` is what a workload gets; ``quota_units`` is what the provider
    counts. They differ by generation -- v4 and v5p bill two units per chip,
    v5e/v6e one -- and conflating them is why a v5p-16 pool asking for 16 TPUs
    would silently request twice the hardware.
    """

    accelerator_type: str
    kind: str
    generation: str
    chips: int
    quota_units: int
    host_cpu: int
    capacity_backend: str
    multislice_capable: bool
    hosts: int | None = None
    chips_per_host: int | None = None
    host_memory_gib: float = 0.0
    machine_type: str | None = None
    tpu_type: str | None = None
    image_family: str | None = None
    image_project: str | None = None
    gpu_model: str | None = None

    def __post_init__(self) -> None:
        if self.chips <= 0 or self.quota_units <= 0:
            raise HardwareCatalogError(f"{self.accelerator_type}: chips and quota_units must be positive")
        # A whole number, because a host has whole CPUs and ``TpuProfile``
        # requires a positive integer. Storing a float here would put the
        # coercion at every call site instead of at the one boundary.
        if isinstance(self.host_cpu, bool) or not isinstance(self.host_cpu, int) or self.host_cpu <= 0:
            raise HardwareCatalogError(f"{self.accelerator_type}: host_cpu must be a positive integer")
        if (self.hosts is None) != (self.chips_per_host is None):
            raise HardwareCatalogError(
                f"{self.accelerator_type}: hosts and chips_per_host are recorded together or not at all"
            )
        if self.hosts is not None and self.chips_per_host is not None:
            # The invariant that turns a typo into a CI failure rather than a
            # pool that provisions the wrong number of machines.
            if self.chips != self.hosts * self.chips_per_host:
                raise HardwareCatalogError(
                    f"{self.accelerator_type}: {self.chips} chips is not {self.hosts} hosts "
                    f"x {self.chips_per_host} chips per host"
                )
        if self.kind == "tpu":
            # Imported here, not at module scope. ``tpu_types`` lives under the
            # GCP provider package, whose ``__init__`` reaches back into modules
            # that consume this catalog -- so a module-level import makes the
            # cycle real and its breakage depend on which module a process
            # happens to import first. The catalog stays a leaf.
            from eyvan.capacity.providers.gcp.tpu_types import parse_tpu_accelerator_type

            parsed = parse_tpu_accelerator_type(self.accelerator_type)
            if parsed is None:
                raise HardwareCatalogError(f"{self.accelerator_type}: not a valid Cloud TPU accelerator type")
            # The suffix *is* the quota unit count. Recording a different number
            # would make the catalog disagree with the provider's own naming.
            if parsed.size != self.quota_units:
                raise HardwareCatalogError(
                    f"{self.accelerator_type}: declares {self.quota_units} quota units but its name says {parsed.size}"
                )
            if parsed.generation != self.generation:
                raise HardwareCatalogError(
                    f"{self.accelerator_type}: declares generation {self.generation!r} "
                    f"but its name says {parsed.generation!r}"
                )

    @property
    def schedulable_tpu(self) -> int:
        """Return the TPU resource count Eyvan schedules against.

        Chips, not quota units. A workload asks for TPUs it can compute on.
        """

        return self.chips if self.kind == "tpu" else 0


@dataclass(frozen=True, slots=True)
class HardwareCatalog:
    """Every shape this build knows, and the revision they came from."""

    catalog_version: int
    revision: str
    shapes: tuple[AcceleratorShape, ...]

    def get(self, accelerator_type: str) -> AcceleratorShape | None:
        """Return one shape, or ``None`` for a type this catalog does not describe."""

        normalized = accelerator_type.strip().lower()
        return next((shape for shape in self.shapes if shape.accelerator_type == normalized), None)

    def require(self, accelerator_type: str) -> AcceleratorShape:
        """Return one shape or refuse, naming what the catalog does know.

        Listing the alternatives is the difference between an error an operator
        can act on and one they have to go read source for.
        """

        shape = self.get(accelerator_type)
        if shape is not None:
            return shape
        known = ", ".join(sorted(item.accelerator_type for item in self.shapes))
        raise HardwareCatalogError(
            f"accelerator type {accelerator_type!r} is not in hardware catalog revision {self.revision}; "
            f"known types: {known}"
        )

    @property
    def accelerator_types(self) -> tuple[str, ...]:
        return tuple(shape.accelerator_type for shape in self.shapes)


def _load(path: Path) -> HardwareCatalog:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise HardwareCatalogError(f"could not read the hardware catalog at {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise HardwareCatalogError(f"hardware catalog at {path} is not valid YAML: {exc}") from exc
    if not isinstance(document, dict):
        raise HardwareCatalogError(f"hardware catalog at {path} must be a mapping")

    version = document.get("catalog_version")
    if version != SUPPORTED_CATALOG_VERSION:
        raise HardwareCatalogError(
            f"hardware catalog declares schema version {version!r}; this build understands {SUPPORTED_CATALOG_VERSION}"
        )
    revision = document.get("revision")
    if not isinstance(revision, str) or not revision.strip():
        raise HardwareCatalogError("hardware catalog must declare a non-empty revision")

    entries = document.get("shapes")
    if not isinstance(entries, list) or not entries:
        raise HardwareCatalogError("hardware catalog must declare at least one shape")

    shapes: list[AcceleratorShape] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise HardwareCatalogError("every hardware catalog shape must be a mapping")
        try:
            shape = AcceleratorShape(**entry)
        except TypeError as exc:
            raise HardwareCatalogError(f"hardware catalog shape {entry.get('accelerator_type')!r}: {exc}") from exc
        if shape.accelerator_type in seen:
            # Two entries for one type is the failure this catalog exists to
            # end, so it must not be reintroducible inside the catalog itself.
            raise HardwareCatalogError(f"hardware catalog declares {shape.accelerator_type!r} more than once")
        seen.add(shape.accelerator_type)
        shapes.append(shape)
    return HardwareCatalog(catalog_version=version, revision=revision.strip(), shapes=tuple(shapes))


@lru_cache(maxsize=1)
def hardware_catalog() -> HardwareCatalog:
    """Return the catalog shipped with this build.

    Cached because it is read on every fleet parse and pool upsert, and it is
    immutable data inside the wheel.
    """

    return _load(CATALOG_PATH)


def load_hardware_catalog(path: str | Path) -> HardwareCatalog:
    """Load a catalog from an explicit path, for tests and for validating edits."""

    return _load(Path(path))


def shape_for(accelerator_type: str) -> AcceleratorShape | None:
    """Return the shipped catalog's entry for one accelerator type, if it has one."""

    return hardware_catalog().get(accelerator_type)


__all__: tp.Final = (
    "CATALOG_PATH",
    "SUPPORTED_CATALOG_VERSION",
    "AcceleratorShape",
    "HardwareCatalog",
    "HardwareCatalogError",
    "hardware_catalog",
    "load_hardware_catalog",
    "shape_for",
)
