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


#: Exactly what the two deleted dictionaries in ``fleet_config`` contained. Kept
#: as literals so retiring them cannot quietly change what a pool is provisioned
#: with -- the whole risk of a consolidation is that the survivor drifts.
RETIRED_COMPUTE_PROFILE = {
    "v5p-8": {
        "machine_type": "ct5p-hightpu-4t",
        "tpu_type": "ct5p",
        "tpu_count": 4,
        "image_family": "ubuntu-accel-2204-amd64-tpu-v5e-v5p-v6e",
        "image_project": "ubuntu-os-accelerator-images",
    },
}
RETIRED_QUEUED_SLICE_PROFILE = {"v5p-16": 8, "v5p-32": 16}

#: What ``DEFAULT_TPU_PROFILES`` was written out as before it was derived.
RETIRED_DEFAULT_PROFILES = (("v5p-16", 32, 8), ("v5p-32", 32, 16), ("v6e-8", 32, 8))


def test_the_catalog_carries_what_the_compute_dictionary_carried() -> None:
    """``v5p-8`` is provisioned as an integrated Compute Engine TPU VM.

    Its machine type, image and chip count used to live in a second dictionary
    that had to agree with everything else. Losing any of these on the way to
    the catalog would provision the wrong machine.
    """

    for accelerator_type, profile in RETIRED_COMPUTE_PROFILE.items():
        shape = hardware_catalog().require(accelerator_type)
        assert shape.chips == profile["tpu_count"]
        assert shape.machine_type == profile["machine_type"]
        assert shape.tpu_type == profile["tpu_type"]
        assert shape.image_family == profile["image_family"]
        assert shape.image_project == profile["image_project"]


def test_the_compiled_provider_options_still_carry_the_compute_profile() -> None:
    """The values have to survive the whole path, not merely exist in the catalog.

    ``compiled_provider_options`` is what actually reaches the provider, and it
    used to spread the dictionary in wholesale.
    """

    from eyvan.capacity.fleet_config import EyvanFleetConfig

    config = EyvanFleetConfig.model_validate(
        _fleet({"min_workers": 0, "max_workers": 1, "capacity_backend": "gcp-compute"}, name="v5p-8")
    )
    (pool,) = config.pool_payloads()
    options = pool["provider_options"]
    expected = RETIRED_COMPUTE_PROFILE["v5p-8"]

    assert options["machine_type"] == expected["machine_type"]
    assert options["tpu_type"] == expected["tpu_type"]
    assert options["tpu_count"] == expected["tpu_count"]
    assert options["image_family"] == expected["image_family"]
    assert options["image_project"] == expected["image_project"]


def test_the_catalog_carries_what_the_queued_slice_dictionary_carried() -> None:
    """Each queued worker is one independent slice, and its chip count is its size."""

    for accelerator_type, chips in RETIRED_QUEUED_SLICE_PROFILE.items():
        assert hardware_catalog().require(accelerator_type).chips == chips


def test_the_derived_compatibility_tuple_still_contains_what_it_used_to() -> None:
    """Deriving it must not silently drop an entry someone imports.

    ``v6e-8`` was in the hand-written tuple and nowhere else, which is exactly
    how a fourth copy holds the only record of something.
    """

    derived = {(profile.accelerator_type, profile.cpu, profile.tpu) for profile in DEFAULT_TPU_PROFILES}

    assert set(RETIRED_DEFAULT_PROFILES) <= derived
    # v5p-8 is compiled as an integrated Compute Engine TPU VM and excluded from
    # queued autoscaling, so offering a profile for it would be offering one the
    # autoscaler refuses to use.
    assert "v5p-8" not in {profile.accelerator_type for profile in DEFAULT_TPU_PROFILES}


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


def test_a_fractional_host_cpu_is_refused(tmp_path) -> None:
    """A host has whole CPUs, and ``TpuProfile`` requires a positive integer.

    Storing a float would move the coercion to every call site, where one of
    them would eventually truncate silently.
    """

    document = _catalog_document()
    document["shapes"][1]["host_cpu"] = 31.5

    with pytest.raises(HardwareCatalogError, match="positive integer"):
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


# --------------------------------------------------------------------------- host counts


def _tpu_runner(nodes: dict[str, tuple[str, ...]], accelerator_type: str):
    """A gcloud stand-in returning one node description per allocation."""

    import json

    from test_gcp_provider import _multislice_qr, result

    def runner(args):
        if "queued-resources" in args:
            return result(stdout=json.dumps(_multislice_qr(accelerator_type)))
        node_id = args[args.index("describe") + 1]
        return result(
            stdout=json.dumps(
                {
                    "state": "READY",
                    "acceleratorType": accelerator_type,
                    "metadata": {"eyvan-uid": "0123456789abcdef0123456789abcdef"},
                    "networkEndpoints": [{"ipAddress": ip} for ip in nodes[node_id]],
                }
            )
        )

    return runner


def test_a_slice_short_of_hosts_is_not_reported_healthy() -> None:
    """The defect this property exists for: a silently half-sized JAX cluster.

    The provider response alone cannot catch it, because a slice missing a host
    looks exactly like a smaller slice. The run starts, forms a smaller mesh
    than it asked for, and the only symptom is throughput nobody can explain.
    """

    from test_gcp_provider import _multislice_record

    from eyvan.capacity import CapacityStatus
    from eyvan.capacity.providers.gcp.queued_resources import GcloudTpuProvider

    short = {
        "cell-a-r1-node-0": ("10.2.0.4", "10.2.0.5"),
        "cell-a-r1-node-1": ("10.2.0.6",),
    }

    observation = GcloudTpuProvider(runner=_tpu_runner(short, "v5p-16")).ensure(_multislice_record())

    assert observation.state is CapacityStatus.PROVISIONING
    assert "2 hosts per slice" in observation.detail
    assert "4 are expected" in observation.detail


def test_a_complete_slice_is_healthy() -> None:
    """The check must not block the shape it exists to require."""

    from test_gcp_provider import _multislice_record

    from eyvan.capacity import CapacityStatus
    from eyvan.capacity.providers.gcp.queued_resources import GcloudTpuProvider

    complete = {
        "cell-a-r1-node-0": ("10.2.0.4", "10.2.0.5"),
        "cell-a-r1-node-1": ("10.2.0.6", "10.2.0.7"),
    }

    observation = GcloudTpuProvider(runner=_tpu_runner(complete, "v5p-16")).ensure(_multislice_record())

    assert observation.state is CapacityStatus.HEALTHY
    assert observation.observed_tpu_vms == 4


def test_a_shape_with_no_recorded_host_count_keeps_the_provider_authoritative() -> None:
    """An unknown host count must not become an outage.

    The catalog records topology only where this repository grounds it, so for
    everything else the behaviour is exactly what it was: the provider decides.
    Refusing here would make Eyvan reject slices it simply has not been taught.
    """

    from test_gcp_provider import _multislice_record

    from eyvan.capacity import CapacityStatus
    from eyvan.capacity.providers.gcp.queued_resources import GcloudTpuProvider

    assert hardware_catalog().require("v6e-16").hosts is None
    uneven = {
        "cell-a-r1-node-0": ("10.2.0.4", "10.2.0.5"),
        "cell-a-r1-node-1": ("10.2.0.6",),
    }

    observation = GcloudTpuProvider(runner=_tpu_runner(uneven, "v6e-16")).ensure(_multislice_record("v6e-16"))

    assert observation.state is CapacityStatus.HEALTHY
    assert observation.observed_tpu_vms == 3


def test_autoscale_prefers_the_catalog_over_the_suffix_derivation(monkeypatch) -> None:
    """The branch is invisible today, which is exactly why it needs a test.

    For every shape currently catalogued the derived answer and the catalog
    answer agree -- that agreement is the parity this phase was built on. So
    deleting the catalog branch changes nothing observable, and it would survive
    review as dead code right up until the first shape whose host CPU is not 32.

    ``shape_for`` is patched, not the function under test: the branch's whole job
    is to consult the catalog, so substituting the catalog is what exercises it.
    """

    from eyvan.capacity.autoscale import profiles
    from eyvan.capacity.hardware_catalog import AcceleratorShape

    unusual = AcceleratorShape(
        accelerator_type="v5p-16",
        kind="tpu",
        generation="v5p",
        chips=8,
        quota_units=16,
        host_cpu=96,
        capacity_backend="gcp-tpu",
        multislice_capable=True,
    )
    monkeypatch.setattr(profiles, "shape_for", lambda _type: unusual)

    resolved = profiles.tpu_profile_from_accelerator_type("v5p-16")

    assert resolved is not None
    assert resolved.cpu == 96, "the suffix derivation's fixed 32 won over the catalog"
    assert resolved.tpu == 8


def test_an_uncatalogued_type_keeps_the_conservative_fallback(monkeypatch) -> None:
    """An unknown shape must autoscale exactly as it did before the catalog.

    The fallback is deliberately the number that used to apply to every TPU, so
    a type Eyvan has not been taught acquires no new behaviour it did not have.
    """

    from eyvan.capacity.autoscale import profiles

    monkeypatch.setattr(profiles, "shape_for", lambda _type: None)

    resolved = profiles.tpu_profile_from_accelerator_type("v5p-512")

    assert resolved is not None
    assert (resolved.cpu, resolved.tpu) == (32, 256)
