# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""A callable that asked for a CPU can land on a CPU pool.

``eyvan`` exports ``CpuConfig``, ``@execute`` accepts it, and a test asserts the
decorator accepts every config the package ships -- while every such submission
was refused with "is not a TPU pool", after the callable had been serialized and
uploaded to the artifact store. Observed on a live fleet on 2026-08-13 against a
healthy CPU pool.

The branch that fixes it is deliberately narrow. ``cpu`` is the one accelerator
type whose kind is knowable from the wire, because ``CpuConfig`` is the only
thing that sends it; every other request takes the path it always took. Deriving
the kind for the rest -- from the catalog, say -- would refuse any TPU type the
catalog does not carry, and P1 already treats a missing catalog entry as
inconclusive rather than absent.
"""

from __future__ import annotations

import pytest

from eyvan.records import (
    CallableExecution,
    CapacityPoolRecord,
    Principal,
    ResourceVector,
    Role,
    RoleGrant,
)
from eyvan.server.persistence import InMemoryRepository
from eyvan.server.service import ControlPlaneService
from eyvan.server.service.callable_trust import CallableProjectTrustPolicy
from eyvan.server.service.errors import ConflictError


def _operator() -> Principal:
    return Principal(
        subject="operator",
        grants=(RoleGrant(role=Role.PROJECT_OPERATOR, org_id="acme", project_id="research"),),
    )


def _cpu_pool(*, pool_id: str = "cpu-pool", accelerator: ResourceVector | None = None) -> CapacityPoolRecord:
    resources = accelerator or ResourceVector(cpu=4, memory_gib=16)
    return CapacityPoolRecord(
        id=pool_id,
        fleet_name="research-fleet",
        node_type="cpu",
        provider="gcp",
        capacity_backend="gcp-compute",
        provider_project="gcp-project-a",
        region="us-central1",
        zone="us-central1-a",
        capacity=resources,
        allocatable=resources,
        accelerator_type="cpu",
        capacity_class="spot",
        executor="eyvan-native",
        agent_endpoint="http://agent:8090",
        labels={"eyvan_resource_kind": "cpu"},
        max_cells=4,
    )


async def _submit(service: ControlPlaneService, pools: list[CapacityPoolRecord], **overrides: object):
    admin = Principal(subject="system", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    for index, pool in enumerate(pools):
        await service.upsert_capacity_pool(admin, pool, idempotency_key=f"pool-{index}")
    arguments: dict[str, object] = {
        "fleet_name": "research-fleet",
        "org_id": "acme",
        "project_id": "research",
        "accelerator_type": "cpu",
        "pods": 1,
        "execution": CallableExecution(artifact_uri="gs://trusted/train.pkl", sha256="a" * 64, python="3.12"),
        "idempotency_key": "cpu-callable-1",
    }
    arguments.update(overrides)
    return await service.submit_pool_callable(_operator(), **arguments)  # type: ignore[arg-type]


def _service() -> ControlPlaneService:
    return ControlPlaneService(
        InMemoryRepository(),
        callable_trust=CallableProjectTrustPolicy.from_projects({"acme/research"}),
    )


@pytest.mark.asyncio
async def test_a_cpu_callable_lands_on_a_cpu_pool() -> None:
    """The whole defect: this was refused with "is not a TPU pool"."""

    workload, replayed = await _submit(_service(), [_cpu_pool()])

    assert replayed is False
    assert workload.spec.required_labels.get("eyvan_pool") == "cpu-pool"


@pytest.mark.asyncio
async def test_a_pool_carrying_an_accelerator_is_refused_for_a_cpu_callable() -> None:
    """A callable placed there would hold a TPU nothing asked for.

    Nothing would account it to the workload either, which is how capacity ends
    up busy and unbilled.
    """

    mislabelled = _cpu_pool(accelerator=ResourceVector(cpu=4, memory_gib=16, tpu=8))

    with pytest.raises(ConflictError, match="is not a CPU pool"):
        await _submit(_service(), [mislabelled])


@pytest.mark.asyncio
async def test_a_cpu_callable_still_respects_the_capacity_class_it_asked_for() -> None:
    """The additive branch must not skip the checks the TPU path applies."""

    from eyvan.records import CapacityClass

    with pytest.raises(ConflictError, match="does not offer"):
        await _submit(_service(), [_cpu_pool()], capacity_class=CapacityClass.ON_DEMAND)


@pytest.mark.asyncio
async def test_two_cpu_pools_are_ambiguous_rather_than_arbitrary() -> None:
    """Picking one would place work somewhere the caller did not choose."""

    with pytest.raises(ConflictError, match="ambiguous"):
        await _submit(_service(), [_cpu_pool(pool_id="cpu-a"), _cpu_pool(pool_id="cpu-b")])


@pytest.mark.asyncio
async def test_a_tpu_request_is_never_answered_by_a_cpu_pool() -> None:
    """The property the previous attempt at this broke.

    A gate that only asked whether a pool was coherent for its *own* kind lost
    the caller's intent, so a pool named for a TPU could answer a TPU request
    while declaring something else. The branch here cannot: it is chosen by the
    requested type, not by what the pool happens to be.
    """

    cpu_named_like_a_tpu = _cpu_pool().model_copy(update={"accelerator_type": "v5p-8"})

    with pytest.raises(ConflictError, match="is not a TPU pool"):
        await _submit(_service(), [cpu_named_like_a_tpu], accelerator_type="v5p-8")


# --------------------------------------------------------------------------- the third kind


def _gpu_pool(*, pool_id: str = "gpu-pool", accelerator: ResourceVector | None = None) -> CapacityPoolRecord:
    resources = accelerator or ResourceVector(cpu=8, memory_gib=32, gpu=1)
    return CapacityPoolRecord(
        id=pool_id,
        fleet_name="research-fleet",
        node_type="gpu",
        provider="gcp",
        capacity_backend="gcp-compute",
        provider_project="gcp-project-a",
        region="us-central1",
        zone="us-central1-a",
        capacity=resources,
        allocatable=resources,
        accelerator_type="nvidia-tesla-p100",
        capacity_class="spot",
        executor="eyvan-native",
        agent_endpoint="http://agent:8090",
        labels={"eyvan_resource_kind": "gpu"},
        max_cells=4,
    )


@pytest.mark.asyncio
async def test_a_gpu_callable_lands_on_a_gpu_pool() -> None:
    """The kind that had no gate at all.

    Not decided by the hardware catalog, which carries no GPU shapes whatsoever:
    ``shape_for('nvidia-tesla-p100')`` is ``None``, so a catalog-driven rule
    would refuse every GPU type there is.
    """

    workload, replayed = await _submit(_service(), [_gpu_pool()], accelerator_type="nvidia-tesla-p100")

    assert replayed is False
    assert workload.spec.required_labels.get("eyvan_pool") == "gpu-pool"


@pytest.mark.asyncio
async def test_a_tpu_generation_the_catalog_never_heard_of_is_still_a_tpu_request() -> None:
    """The property that makes deriving the kind from the request safe.

    GPU is what remains after CPU and TPU, so anything mistaken for "not a TPU"
    would silently become a GPU request. The accelerator type is *parsed* rather
    than looked up precisely so a generation the catalog has not got still takes
    the TPU path -- P1 already treats a missing catalog entry as inconclusive
    rather than absent, and this must agree with it.
    """

    # Named for the unknown generation so the request gets past the exact-match
    # step and actually reaches the kind gate, which is what is under test.
    unknown_generation = _gpu_pool().model_copy(update={"accelerator_type": "v99z-8"})

    with pytest.raises(ConflictError, match="is not a TPU pool"):
        await _submit(_service(), [unknown_generation], accelerator_type="v99z-8")


@pytest.mark.asyncio
async def test_a_pool_mislabelled_gpu_still_cannot_answer_a_tpu_request() -> None:
    """The regression the first attempt at a general kind gate introduced.

    Deriving the kind from the pool lost the caller's intent, so a pool named for
    a TPU while declaring something else could answer a TPU request. The kind is
    decided from the requested type before any pool is consulted, so it cannot.
    """

    mislabelled = _gpu_pool().model_copy(update={"accelerator_type": "v5p-8"})

    with pytest.raises(ConflictError, match="is not a TPU pool"):
        await _submit(_service(), [mislabelled], accelerator_type="v5p-8")


@pytest.mark.asyncio
async def test_a_pool_carrying_a_tpu_is_refused_for_a_gpu_callable() -> None:
    """Same rule the CPU gate applies: no holding hardware nobody asked for."""

    mislabelled = _gpu_pool(accelerator=ResourceVector(cpu=8, memory_gib=32, gpu=1, tpu=4))

    with pytest.raises(ConflictError, match="is not a GPU pool"):
        await _submit(_service(), [mislabelled], accelerator_type="nvidia-tesla-p100")


# --------------------------------------------------------------------------- the kind that is still refused


def test_a_gpu_callable_is_refused_before_anything_is_uploaded() -> None:
    """The refusal existed; where it happened was the problem.

    The control plane answered 409 only after the callable had been serialized
    and put in the artifact store -- an object left behind, and an error naming
    a pool rather than the limitation. Nothing is uploaded now.
    """

    from eyvan import GpuConfig, execute
    from eyvan.sdk.execution import ExecutionContext

    uploaded: list[object] = []

    class _Store:
        def put(self, payload: object) -> object:
            uploaded.append(payload)
            raise AssertionError("a GPU callable must be refused before it is uploaded")

        def allocate_result_uri(self, invocation_id: str) -> str:
            return f"gs://results/{invocation_id}"

    class _Submitter:
        def submit_callable(self, request: object) -> object:
            raise AssertionError("a GPU callable must be refused before it is submitted")

    context = ExecutionContext(
        org_id="acme",
        project_id="research",
        fleet="research",
        artifact_store=_Store(),  # type: ignore[arg-type]
        submitter=_Submitter(),  # type: ignore[arg-type]
    )

    @execute(GpuConfig(type="nvidia-tesla-a100"), context=context)
    def train() -> int:
        return 1

    with pytest.raises(NotImplementedError, match="GPU pool"):
        train.submit()

    assert uploaded == []


def test_the_refusal_names_what_to_use_instead() -> None:
    """A limit an operator cannot act on is a limit stated badly."""

    from eyvan import GpuConfig
    from eyvan.sdk.execution import _refuse_unplaceable_config

    with pytest.raises(NotImplementedError) as error:
        _refuse_unplaceable_config(GpuConfig(type="nvidia-h100-80gb"))

    assert "CpuConfig" in str(error.value)
    assert "eyvan workload submit" in str(error.value)


@pytest.mark.parametrize("config_name", ["cpu", "tpu"])
def test_the_placeable_kinds_are_not_refused(config_name: str) -> None:
    """The check must not become a gate on everything it can see."""

    from eyvan import CpuConfig, TpuConfig
    from eyvan.sdk.execution import _refuse_unplaceable_config

    _refuse_unplaceable_config(CpuConfig() if config_name == "cpu" else TpuConfig(type="v5p-16"))
