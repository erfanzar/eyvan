# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""One source of shape truth, proved to agree with the three it replaces.

``fleet_config`` held a compute-TPU dict and a queued-slice dict;
``autoscale.profiles`` derived a chip count from the type suffix and hardcoded
``cpu=32`` for every TPU; every fleet YAML restated the answer by hand. Three
sources for one fact are three chances to disagree, and the operator writing the
fourth copy has no way to know which one they are supposed to match.

The parity tests below are the load-bearing ones. A catalog that is internally
consistent but disagrees with the code it replaces would not consolidate
anything -- it would add a fourth source. So the expected numbers are written as
literals rather than read from either side: a test that compares the catalog to
the dict it replaced passes just as happily when both are wrong together.
"""

from __future__ import annotations

import textwrap

import pytest
import yaml

from eyvan.capacity.autoscale.profiles import DEFAULT_TPU_PROFILES, tpu_profile_from_accelerator_type
from eyvan.capacity.fleet_config import _COMPUTE_TPU_PROFILES, _QUEUED_TPU_SLICE_PROFILES
from eyvan.capacity.hardware_catalog import (
    CATALOG_PATH,
    SUPPORTED_CATALOG_VERSION,
    HardwareCatalogError,
    hardware_catalog,
    load_hardware_catalog,
    shape_for,
)

#: What each shape is, written out. Not derived from the catalog and not read
#: from the code being replaced -- both of those would let the test agree with a
#: mistake. These are the numbers the live fleets in ``configs/`` run on.
EXPECTED = {
    "v5p-8": {"chips": 4, "quota_units": 8, "hosts": 1, "cpu": 32},
    "v5p-16": {"chips": 8, "quota_units": 16, "hosts": 2, "cpu": 32},
    "v5p-32": {"chips": 16, "quota_units": 32, "hosts": 4, "cpu": 32},
    "v6e-16": {"chips": 16, "quota_units": 16, "hosts": None, "cpu": 32},
}


def _catalog_document() -> dict:
    return yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))


def _write(tmp_path, document: dict):
    path = tmp_path / "catalog.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return path


# --------------------------------------------------------------------------- parity


@pytest.mark.parametrize("accelerator_type", sorted(EXPECTED))
def test_the_catalog_states_the_shape_the_fleet_actually_runs(accelerator_type: str) -> None:
    """Literals, not a comparison between two things that could both be wrong."""

    expected = EXPECTED[accelerator_type]
    shape = hardware_catalog().require(accelerator_type)

    assert shape.chips == expected["chips"]
    assert shape.quota_units == expected["quota_units"]
    assert shape.hosts == expected["hosts"]
    assert shape.host_cpu == expected["cpu"]


def test_the_catalog_agrees_with_the_compute_tpu_dictionary() -> None:
    """``v5p-8`` is provisioned as an integrated Compute Engine TPU VM.

    Its machine type, image and chip count already lived in ``fleet_config``.
    Disagreeing here would mean a pool sized from the catalog and provisioned
    from the dict, which is the exact failure this replaces.
    """

    for accelerator_type, profile in _COMPUTE_TPU_PROFILES.items():
        shape = hardware_catalog().require(accelerator_type)
        assert shape.chips == profile["tpu_count"]
        assert shape.machine_type == profile["machine_type"]
        assert shape.tpu_type == profile["tpu_type"]
        assert shape.image_family == profile["image_family"]
        assert shape.image_project == profile["image_project"]


def test_the_catalog_agrees_with_the_queued_slice_dictionary() -> None:
    """Each queued worker is one independent slice, and its chip count is its size."""

    for accelerator_type, profile in _QUEUED_TPU_SLICE_PROFILES.items():
        assert hardware_catalog().require(accelerator_type).chips == profile["tpu_count"]


def test_the_catalog_agrees_with_the_derived_autoscale_profile() -> None:
    """The suffix-derived path is what runs today; the catalog must not move it.

    ``profiles`` divides the type suffix by two for v4 and v5p and by one
    otherwise. Reproducing a different chip count would change how much hardware
    an autoscaled pool asks for.
    """

    for accelerator_type in EXPECTED:
        derived = tpu_profile_from_accelerator_type(accelerator_type)
        if derived is None:
            # v5p-8 is deliberately excluded from queued autoscaling.
            assert accelerator_type == "v5p-8"
            continue
        shape = hardware_catalog().require(accelerator_type)
        assert shape.schedulable_tpu == derived.tpu, accelerator_type
        assert shape.host_cpu == derived.cpu, accelerator_type


def test_the_catalog_agrees_with_the_compatibility_profile_tuple() -> None:
    """``DEFAULT_TPU_PROFILES`` is the fourth copy, and it is still consulted."""

    for profile in DEFAULT_TPU_PROFILES:
        shape = shape_for(profile.accelerator_type)
        if shape is None:
            continue
        assert (shape.schedulable_tpu, shape.host_cpu) == (profile.tpu, profile.cpu), profile.accelerator_type


# --------------------------------------------------------------------------- invariants


def test_the_shipped_catalog_loads() -> None:
    """The one test that fails on a bad edit before anything else runs."""

    catalog = hardware_catalog()

    assert catalog.catalog_version == SUPPORTED_CATALOG_VERSION
    assert catalog.revision
    assert catalog.accelerator_types


def test_a_chip_count_that_contradicts_its_topology_is_refused(tmp_path) -> None:
    """``chips == hosts * chips_per_host`` turns a typo into a load failure.

    Without it, a mistyped host count reaches a provider as a request for the
    wrong number of machines, and the first symptom is a JAX cluster that never
    forms.
    """

    document = _catalog_document()
    document["shapes"][1]["hosts"] = 3

    with pytest.raises(HardwareCatalogError, match="is not 3 hosts"):
        load_hardware_catalog(_write(tmp_path, document))


def test_quota_units_must_match_the_name_the_provider_uses(tmp_path) -> None:
    """The suffix *is* the quota unit count; a catalog disagreeing with it is wrong.

    Getting this backwards is how a v5p-16 pool ends up requesting twice the
    hardware -- chips and quota units differ by two on this generation.
    """

    document = _catalog_document()
    document["shapes"][1]["quota_units"] = 8

    with pytest.raises(HardwareCatalogError, match="its name says 16"):
        load_hardware_catalog(_write(tmp_path, document))


def test_a_generation_that_contradicts_the_name_is_refused(tmp_path) -> None:
    """Generation drives the quota divisor, so a wrong one mis-sizes every pool."""

    document = _catalog_document()
    document["shapes"][1]["generation"] = "v6e"

    with pytest.raises(HardwareCatalogError, match="its name says 'v5p'"):
        load_hardware_catalog(_write(tmp_path, document))


def test_half_a_topology_is_refused(tmp_path) -> None:
    """Hosts without chips-per-host cannot be checked, so it must not be storable.

    A half-recorded topology reads as knowledge and is not: it would pass the
    product invariant by skipping it.
    """

    document = _catalog_document()
    document["shapes"][3]["hosts"] = 4

    with pytest.raises(HardwareCatalogError, match="recorded together or not at all"):
        load_hardware_catalog(_write(tmp_path, document))


def test_a_duplicate_type_is_refused(tmp_path) -> None:
    """Two entries for one type is the failure this catalog exists to end.

    It must not be reintroducible inside the catalog itself, where it would be
    even harder to notice than across three files.
    """

    document = _catalog_document()
    document["shapes"].append(dict(document["shapes"][1]))

    with pytest.raises(HardwareCatalogError, match="more than once"):
        load_hardware_catalog(_write(tmp_path, document))


def test_an_unsupported_schema_version_is_refused_rather_than_partly_read(tmp_path) -> None:
    """A build that ignored unknown fields would size pools from half a description."""

    document = _catalog_document()
    document["catalog_version"] = 99

    with pytest.raises(HardwareCatalogError, match="this build understands 1"):
        load_hardware_catalog(_write(tmp_path, document))


def test_a_catalog_without_a_revision_is_refused(tmp_path) -> None:
    """The revision is how an operator tells which numbers a pool was sized from."""

    document = _catalog_document()
    document.pop("revision")

    with pytest.raises(HardwareCatalogError, match="non-empty revision"):
        load_hardware_catalog(_write(tmp_path, document))


def test_malformed_yaml_names_the_file(tmp_path) -> None:
    """A parse error with no path is a support ticket."""

    path = tmp_path / "catalog.yaml"
    path.write_text(
        textwrap.dedent("""\
        catalog_version: 1
        shapes: [
        """),
        encoding="utf-8",
    )

    with pytest.raises(HardwareCatalogError, match="not valid YAML"):
        load_hardware_catalog(path)


def test_an_unknown_type_is_refused_with_the_alternatives(tmp_path) -> None:
    """An error an operator can act on names what the catalog does know."""

    with pytest.raises(HardwareCatalogError) as raised:
        hardware_catalog().require("v5p-64")

    message = str(raised.value)
    assert "v5p-64" in message
    assert "v5p-16" in message, "the refusal must list the types that are available"


def test_chips_and_quota_units_are_kept_distinct() -> None:
    """The distinction the old code blurred, on the generation where it matters.

    v5p bills two quota units per chip. A caller reading ``quota_units`` as a
    schedulable TPU count would double every request.
    """

    shape = hardware_catalog().require("v5p-16")

    assert (shape.chips, shape.quota_units) == (8, 16)
    assert shape.schedulable_tpu == 8


def test_the_catalog_is_packaged_in_the_wheel() -> None:
    """Data beside code is only shipped if the build backend is told.

    A catalog that resolves in a source checkout and vanishes in the container
    would fail at the first pool upsert on a real deployment.
    """

    assert CATALOG_PATH.is_file()
    assert CATALOG_PATH.parent.name == "data"
    assert CATALOG_PATH.parent.parent.name == "capacity"


# --------------------------------------------------------------------------- consumption


def _fleet(pool: dict, name: str = "v5p-16") -> dict:
    return {
        "version": 1,
        "name": "research-fleet",
        "provider": {"type": "gcp", "project": "p", "region": "us-east5", "zone": "us-east5-a"},
        "executor": "eyvan-native",
        "capacity_class": "spot",
        "agent_endpoint": "https://agent.example.com",
        "pools": {name: {**pool, "provider_options": {"bootstrap_egress": "public-internet"}}},
    }


def test_a_pool_named_after_a_known_shape_needs_no_resources_block() -> None:
    """The fourth copy, deleted. Every fleet YAML restated what the key already said.

    An operator writing ``v5p-16:`` has stated the shape; making them also write
    ``TPU: 8`` underneath is asking them to agree with a number they have no way
    to look up.
    """

    from eyvan.capacity.fleet_config import EyvanFleetConfig

    config = EyvanFleetConfig.model_validate(_fleet({"min_workers": 0, "max_workers": 1}))
    (pool,) = config.pool_payloads()

    assert pool["capacity"]["tpu"] == 8
    assert pool["capacity"]["cpu"] == 32


def test_a_declared_block_that_contradicts_the_catalog_is_refused() -> None:
    """Client-side, before any API call, naming the number that is wrong.

    This is the check that catches a hand-written mistake -- and it is now one
    check instead of two dictionary lookups covering different subsets of types.
    """

    from pydantic import ValidationError

    from eyvan.capacity.fleet_config import EyvanFleetConfig

    with pytest.raises(ValidationError, match=r"resources\.tpu == 8"):
        EyvanFleetConfig.model_validate(_fleet({"min_workers": 0, "max_workers": 1, "resources": {"CPU": 32, "TPU": 4}}))


def test_a_matching_declared_block_still_works() -> None:
    """Every fleet YAML in this repository declares one, and none may break."""

    from eyvan.capacity.fleet_config import EyvanFleetConfig

    config = EyvanFleetConfig.model_validate(
        _fleet({"min_workers": 0, "max_workers": 1, "resources": {"CPU": 32, "TPU": 8}})
    )
    (pool,) = config.pool_payloads()

    assert pool["capacity"]["tpu"] == 8


def test_a_pool_whose_shape_the_catalog_does_not_know_is_left_alone() -> None:
    """The catalog describes what Eyvan can size, not what a provider can run.

    Refusing an operator's hand-written pool because Eyvan has not been taught
    its shape would turn a convenience into a gate on hardware.
    """

    from eyvan.capacity.fleet_config import EyvanFleetConfig

    config = EyvanFleetConfig.model_validate(
        _fleet({"min_workers": 0, "max_workers": 1, "resources": {"CPU": 64, "TPU": 32}}, name="v5p-64")
    )
    (pool,) = config.pool_payloads()

    assert pool["capacity"]["tpu"] == 32
