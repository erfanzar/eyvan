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

CPU and memory are different: both participate in admission, so every built-in
shape must carry positive per-VM values plus the provider document and version
that establish them. A future GPU entry has the same requirement. Hardware the
catalog does not describe remains usable only through an explicit, complete fleet
resource vector; provider discovery alone is not evidence for host resources.
"""

from __future__ import annotations

import math
import typing as tp
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

import yaml

#: Schema version this build understands. A catalog declaring anything else is
#: refused rather than partially read: a build silently ignoring fields it does
#: not know would size pools from half a description.
SUPPORTED_CATALOG_VERSION = 2

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

    ``host_cpu`` and ``host_memory_gib`` are the resources of one TPU VM host,
    not a sum across a multi-host slice. Eyvan launches the native workload on
    every host, so admission must ensure the per-host request fits each host.
    ``source_url`` and ``source_version`` make those otherwise easy-to-guess
    numbers reviewable and make unsourced future accelerator entries invalid.
    """

    accelerator_type: str
    kind: str
    generation: str
    chips: int
    quota_units: int
    host_cpu: int
    host_memory_gib: float
    capacity_backend: str
    multislice_capable: bool
    source_url: str
    source_version: str
    hosts: int | None = None
    chips_per_host: int | None = None
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
        if (
            isinstance(self.host_memory_gib, bool)
            or not isinstance(self.host_memory_gib, int | float)
            or not math.isfinite(self.host_memory_gib)
            or self.host_memory_gib <= 0
        ):
            raise HardwareCatalogError(f"{self.accelerator_type}: host_memory_gib must be a positive finite number")
        source = urlparse(self.source_url) if isinstance(self.source_url, str) else None
        if source is None or source.scheme != "https" or not source.netloc:
            raise HardwareCatalogError(f"{self.accelerator_type}: source_url must be an absolute HTTPS URL")
        if (
            not isinstance(self.source_version, str)
            or not self.source_version.strip()
            or self.source_version != self.source_version.strip()
        ):
            raise HardwareCatalogError(f"{self.accelerator_type}: source_version must be a non-empty trimmed string")
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


class _AdmissionResourceVector(tp.Protocol):
    """The built-in resource dimensions needed at an accelerator write boundary."""

    cpu: float
    memory_gib: float
    gpu: int
    tpu: int


def _advertised_accelerator_kinds(resources: _AdmissionResourceVector) -> set[str]:
    return {kind for kind in ("gpu", "tpu") if getattr(resources, kind) > 0}


def _gcp_accelerator_kind(accelerator_type: str, shape: AcceleratorShape | None) -> str | None:
    """Classify provider names whose kind is independently knowable."""

    if shape is not None:
        return shape.kind
    from eyvan.capacity.providers.gcp.tpu_types import parse_tpu_accelerator_type

    if accelerator_type == "tpu" or parse_tpu_accelerator_type(accelerator_type) is not None:
        return "tpu"
    if accelerator_type == "gpu" or accelerator_type.startswith("nvidia-"):
        return "gpu"
    return None


def validate_accelerator_write_boundary(
    *,
    provider: str,
    capacity_backend: str | None,
    accelerator_type: str | None,
    capacity: _AdmissionResourceVector,
    allocatable: _AdmissionResourceVector,
    boundary: str,
) -> None:
    """Validate accelerator identity and admission resources before persistence.

    Durable records deliberately do not call this function: historical rows with
    incomplete vectors must remain readable so ``eyvan doctor`` can name and
    repair them. Public cell/pool requests and the typed fleet compiler do call
    it, so no new write can bypass catalog kind/count semantics.

    Known GCP shapes fix only accelerator kind and count. Positive CPU and memory
    remain documented admission overrides. An uncatalogued GCP type stays usable
    through the documented manual escape hatch, but both capacity and allocatable
    must then be complete positive host vectors.
    """

    normalized_provider = provider.strip().lower()
    normalized_backend = (capacity_backend or "").strip().lower().replace("_", "-")
    normalized_type = accelerator_type.strip().lower() if accelerator_type is not None else None
    gcp_backends = {"gcp", "gcp-tpu", "tpu", "gcp-compute", "compute"}
    if normalized_backend in gcp_backends and normalized_provider != "gcp":
        raise ValueError(f"{boundary} capacity_backend {capacity_backend!r} requires provider 'gcp', not {provider!r}")
    type_declares_accelerator = normalized_type not in {None, "cpu"}
    backend_declares_tpu = normalized_backend in {"gcp-tpu", "tpu"}
    vector_declares_accelerator = any(
        amount > 0 for resources in (capacity, allocatable) for amount in (resources.gpu, resources.tpu)
    )
    if not (type_declares_accelerator or backend_declares_tpu or vector_declares_accelerator):
        return
    if not type_declares_accelerator or normalized_type is None:
        raise ValueError(f"{boundary} accelerator resources require a non-CPU accelerator_type")

    shape = shape_for(normalized_type) if normalized_provider == "gcp" else None
    capacity_kinds = _advertised_accelerator_kinds(capacity)
    allocatable_kinds = _advertised_accelerator_kinds(allocatable)
    vectors_are_complete = (
        capacity.cpu > 0
        and capacity.memory_gib > 0
        and allocatable.cpu > 0
        and allocatable.memory_gib > 0
        and len(capacity_kinds) == 1
        and allocatable_kinds == capacity_kinds
    )
    if normalized_provider == "gcp" and shape is None and not vectors_are_complete:
        raise ValueError(
            f"{boundary} unknown GCP accelerator shape {accelerator_type!r} requires complete manual capacity and "
            "allocatable vectors with positive cpu, memory_gib, and the same one of gpu or tpu"
        )

    for field_name, resources in (("capacity", capacity), ("allocatable", allocatable)):
        if resources.cpu <= 0 or resources.memory_gib <= 0:
            raise ValueError(f"{boundary} {field_name} requires positive cpu and memory_gib for accelerator admission")
    if len(capacity_kinds) != 1 or allocatable_kinds != capacity_kinds:
        raise ValueError(f"{boundary} capacity and allocatable must advertise the same one of gpu or tpu")
    kind = next(iter(capacity_kinds))

    if normalized_provider == "gcp":
        expected_kind = _gcp_accelerator_kind(normalized_type, shape)
        if expected_kind is not None and kind != expected_kind:
            raise ValueError(
                f"{boundary} accelerator_type {accelerator_type!r} is a {expected_kind} type, "
                f"but capacity and allocatable advertise {kind}"
            )
        if normalized_backend in {"gcp-tpu", "tpu"} and kind != "tpu":
            raise ValueError(f"{boundary} capacity_backend {capacity_backend!r} requires tpu resources, not {kind}")
        if normalized_backend in {"gcp-compute", "compute"} and kind == "tpu":
            integrated_tpu = shape is not None and shape.capacity_backend in {"gcp-compute", "compute"}
            if not integrated_tpu:
                raise ValueError(
                    f"{boundary} capacity_backend {capacity_backend!r} cannot provision TPU type "
                    f"{accelerator_type!r}; use gcp-tpu/gcp or a cataloged gcp-compute TPU shape"
                )
        if shape is not None:
            expected_count = shape.chips
            for field_name, resources in (("capacity", capacity), ("allocatable", allocatable)):
                if getattr(resources, shape.kind) != expected_count:
                    raise ValueError(
                        f"{boundary} {field_name}.{shape.kind} must equal catalog count {expected_count} "
                        f"for {shape.accelerator_type!r}"
                    )


__all__: tp.Final = (
    "CATALOG_PATH",
    "SUPPORTED_CATALOG_VERSION",
    "AcceleratorShape",
    "HardwareCatalog",
    "HardwareCatalogError",
    "hardware_catalog",
    "load_hardware_catalog",
    "shape_for",
    "validate_accelerator_write_boundary",
)
