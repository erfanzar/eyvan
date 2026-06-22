# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""A callback address the worker cannot reach, refused before the cell is used.

Found by running P4 on hardware. A secret mount was accepted, admitted,
scheduled, dispatched, and failed eight minutes later with ``Connection
refused`` -- because this plane advertises ``http://127.0.0.1:8081`` and a GCE
worker resolves that on its own host.

Three things were wrong with that, in increasing order of cost: the failure was
late, it was attributed to the worker rather than to the control plane's
configuration, and it was knowable before any cell was contacted.

Secret mounts and in-job submission are the two features that hand a worker an
address and expect a call back, so they are the two this covers. Local cells are
exempt, because loopback genuinely works for them and that is the entire
development topology.
"""

from __future__ import annotations

import pytest

from eyvan.records import CellRecord, CellState, CommandExecution, ResourceVector, WorkloadDefinition, WorkloadRecord
from eyvan.server.controller.workload_commands import (
    _LOCAL_PROVIDERS,
    _LOOPBACK_HOSTS,
    _ControllerWorkloadCommands,
)


class _Access:
    def __init__(self, public_url: str) -> None:
        self.public_url = public_url


class _Dispatcher:
    """The one method under test, lifted off the controller's other machinery."""

    _refuse_unreachable_callback = _ControllerWorkloadCommands._refuse_unreachable_callback

    def __init__(self, public_url: str | None) -> None:
        self.artifact_access = None if public_url is None else _Access(public_url)


def _cell(provider: str = "gcp") -> CellRecord:
    gcp = {"provider_project": "proj", "zone": "us-east5-a"} if provider == "gcp" else {}
    return CellRecord(
        id="cell-a",
        provider=provider,
        region="us-east5",
        **gcp,
        state=CellState.READY,
        capacity=ResourceVector(cpu=4, memory_gib=16),
        allocatable=ResourceVector(cpu=4, memory_gib=16),
        endpoint="http://agent:8090",
    )


def _workload(**spec_overrides: object) -> WorkloadRecord:
    return WorkloadRecord(
        id="wl-1",
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(
            name="train",
            execution=CommandExecution(command="python train.py"),
            **spec_overrides,
        ),
        created_by="alice",
    )


def test_a_remote_cell_and_a_loopback_url_is_refused_at_dispatch() -> None:
    """The case that cost eight minutes and produced a misleading record."""

    dispatcher = _Dispatcher("http://127.0.0.1:8081")
    workload = _workload(secret_mounts={"TOKEN": "gcp-secret://p/t"})

    with pytest.raises(RuntimeError, match="cannot reach"):
        dispatcher._refuse_unreachable_callback(workload, _cell())


def test_the_refusal_names_the_setting_that_fixes_it() -> None:
    """An error an operator cannot act on is only marginally better than a hang."""

    dispatcher = _Dispatcher("http://127.0.0.1:8081")

    with pytest.raises(RuntimeError, match="EYVAN_PUBLIC_URL"):
        dispatcher._refuse_unreachable_callback(_workload(secret_mounts={"T": "gcp-secret://p/t"}), _cell())


def test_in_job_submission_is_covered_too() -> None:
    """The other feature that hands out a callback address, and the same trap."""

    dispatcher = _Dispatcher("http://127.0.0.1:8081")

    with pytest.raises(RuntimeError, match="in-job submission"):
        dispatcher._refuse_unreachable_callback(_workload(can_submit_children=True), _cell())


def test_a_local_cell_is_exempt() -> None:
    """Loopback works for a cell on this host, and that is the dev topology.

    Refusing here would break every local development run that mounts a secret,
    which is the configuration most likely to be exercised.
    """

    dispatcher = _Dispatcher("http://127.0.0.1:8081")

    dispatcher._refuse_unreachable_callback(_workload(secret_mounts={"T": "gcp-secret://p/t"}), _cell("local"))


def test_a_reachable_url_is_not_refused() -> None:
    """The gate must not block the configuration it exists to require."""

    dispatcher = _Dispatcher("https://eyvan.example.test")

    dispatcher._refuse_unreachable_callback(_workload(secret_mounts={"T": "gcp-secret://p/t"}), _cell())


def test_a_workload_needing_no_callback_is_never_refused() -> None:
    """Most workloads never call back, and a loopback plane runs them fine."""

    dispatcher = _Dispatcher("http://127.0.0.1:8081")

    dispatcher._refuse_unreachable_callback(_workload(), _cell())


@pytest.mark.parametrize("host", sorted(_LOOPBACK_HOSTS))
def test_every_loopback_spelling_is_caught(host: str) -> None:
    """`localhost` and `127.0.0.1` are the same mistake spelled two ways.

    `0.0.0.0` is included because it is a *bind* address that reads as a public
    one: a plane advertising it hands the worker something that resolves to the
    worker itself.
    """

    # IPv6 literals are bracketed in a URL; unbracketed, `urlparse` does not
    # read `::1` as a host at all, so the test would be asserting nothing.
    literal = f"[{host}]" if ":" in host else host
    dispatcher = _Dispatcher(f"http://{literal}:8081")

    with pytest.raises(RuntimeError):
        dispatcher._refuse_unreachable_callback(_workload(secret_mounts={"T": "gcp-secret://p/t"}), _cell())


def test_local_providers_are_stated_rather_than_guessed() -> None:
    """A provider list this short should be readable, not inferred from behaviour."""

    assert _LOCAL_PROVIDERS == frozenset({"local"})
