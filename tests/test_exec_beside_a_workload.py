# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Running a diagnostic next to a stuck job.

``exec_in_workload`` was declared on the execution-driver protocol with no
implementation anywhere and no route, and ``supports_exec`` was set by two
capacity drivers and read by nothing. Two halves of a feature, neither
connected to the other.

The shape is deliberately narrow, and each narrowing is a decision rather than
an omission:

* **Beside the workload, not inside its cgroup.** A process in that cgroup
  competes for the memory the trainer is sized against, and its death can take
  the trainer with it. A diagnostic that can kill the job it diagnoses is not
  one.
* **One command, not a PTY.** A session needs a bidirectional long-lived socket
  through five synchronous hops, and ``MutationContext`` has no representation
  for a session -- so a stale controller could hold a shell on a cell whose
  lease was revoked.
* **A list of arguments, never a shell string**, because a string is split by a
  shell on the host.
"""

from __future__ import annotations

import subprocess

import pytest

from eyvan.capacity.drivers.base import UnsupportedCapabilityError
from eyvan.worker.agent.models import AgentCapabilities
from eyvan.worker.agent.service import InvalidWorkloadRequestError


def _executor(tmp_path, runner):
    from eyvan.worker.runtime.systemd_transient import SystemdTransientServiceExecutor

    return SystemdTransientServiceExecutor(tmp_path, command_runner=runner, boot_id_reader=lambda: "boot-a")


def _record(executor, remote_id: str, **fields) -> None:
    import json

    path = executor._record_path(remote_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"unit_name": "eyvan-wl-1.service", **fields}), encoding="utf-8")


# --------------------------------------------------------------------------- the driver


def test_the_systemd_executor_implements_the_exec_capability() -> None:
    """The protocol had no implementer at all."""

    from eyvan.worker.runtime.systemd_transient import SystemdTransientServiceExecutor

    assert hasattr(SystemdTransientServiceExecutor, "exec_in_workload")


def test_exec_runs_a_sibling_unit_rather_than_entering_the_workloads_cgroup(tmp_path) -> None:
    """The narrowing that keeps a diagnostic from killing what it diagnoses."""

    seen: list[tuple[str, ...]] = []

    def runner(command, **_kwargs):
        seen.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, "python 3.11\n", "")

    executor = _executor(tmp_path, runner)
    _record(executor, "wl-1", control_group="/eyvan.slice/eyvan-wl-1.service")

    result = executor.exec_in_workload("wl-1", ["python", "--version"], timeout_s=15)

    assert seen[0][0] == "systemd-run", "exec entered the workload's unit instead of running beside it"
    assert "--" in seen[0] and seen[0][-2:] == ("python", "--version")
    assert result["exit_code"] == 0
    assert result["stdout"] == "python 3.11\n"


def test_the_deadline_is_enforced_by_systemd_not_by_this_process(tmp_path) -> None:
    """A bound this agent enforces dies with this agent.

    A subprocess timeout would leave the unit running with nobody waiting for
    it; ``RuntimeMaxSec`` is enforced by the thing that owns the unit's lifetime.
    """

    seen: list[tuple[str, ...]] = []

    def runner(command, **_kwargs):
        seen.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, "", "")

    executor = _executor(tmp_path, runner)
    _record(executor, "wl-1")

    executor.exec_in_workload("wl-1", ["true"], timeout_s=45)

    assert "--property=RuntimeMaxSec=45" in seen[0]


def test_the_source_job_is_identified_so_a_tool_can_target_it(tmp_path) -> None:
    """``py-spy dump --pid`` needs to know which process to look at.

    ``process_status`` already exposes the control group and the PIDs; this is
    what makes those reachable from a command running outside the unit.
    """

    seen: list[tuple[str, ...]] = []

    def runner(command, **_kwargs):
        seen.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, "", "")

    executor = _executor(tmp_path, runner)
    _record(executor, "wl-1", control_group="/eyvan.slice/eyvan-wl-1.service")

    executor.exec_in_workload("wl-1", ["py-spy", "dump"], timeout_s=30)

    joined = " ".join(seen[0])
    assert "--setenv=EYVAN_SOURCE_JOB_ID=wl-1" in joined
    assert "--setenv=EYVAN_SOURCE_CONTROL_GROUP=/eyvan.slice/eyvan-wl-1.service" in joined


def test_cluster_topology_is_not_inherited(tmp_path) -> None:
    """The source workload is still running and still holds its coordinator port.

    Inheriting it would make the diagnostic try to join a cluster it is only
    meant to look at -- the same filter the checkpoint hook applies, for the
    same reason.
    """

    seen: list[tuple[str, ...]] = []

    def runner(command, **_kwargs):
        seen.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, "", "")

    executor = _executor(tmp_path, runner)
    _record(
        executor,
        "wl-1",
        environment={"JAX_COORDINATOR_ADDRESS": "10.0.0.1:1234", "MY_SETTING": "keep-me"},
    )

    executor.exec_in_workload("wl-1", ["true"], timeout_s=10)

    joined = " ".join(seen[0])
    assert "JAX_COORDINATOR_ADDRESS" not in joined
    assert "--setenv=MY_SETTING=keep-me" in joined


def test_an_empty_command_is_refused(tmp_path) -> None:
    """An empty argument list would run systemd-run with no unit to start."""

    executor = _executor(tmp_path, lambda command, **_k: subprocess.CompletedProcess(command, 0, "", ""))
    _record(executor, "wl-1")

    with pytest.raises(ValueError, match="requires a command"):
        executor.exec_in_workload("wl-1", [], timeout_s=10)


def test_exec_against_an_absent_workload_is_refused(tmp_path) -> None:
    """ "Never ran here" must not read as "ran and produced nothing"."""

    executor = _executor(tmp_path, lambda command, **_k: subprocess.CompletedProcess(command, 0, "", ""))

    with pytest.raises(RuntimeError, match="absent"):
        executor.exec_in_workload("wl-missing", ["true"], timeout_s=10)


# --------------------------------------------------------------------------- the two gates


class _ExecCapable:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[str], float]] = []

    def exec_in_workload(self, remote_id, command, *, timeout_s):
        self.calls.append((remote_id, list(command), timeout_s))
        return {"exit_code": 0, "stdout": "ok", "stderr": ""}


class _NotExecCapable:
    def logs(self, remote_id: str) -> str:
        return ""


def _capabilities(*, supports_exec: bool = False) -> AgentCapabilities:
    """The minimum a capability document needs, so a test states only what it tests."""

    return AgentCapabilities(
        provider="local",
        executor="eyvan-native",
        accelerator_types=(),
        capacity_classes=(),
        supports_checkpoint=True,
        supports_callables=True,
        supports_exec=supports_exec,
        max_gang_replicas=None,
        resource_enforcement="backend",
    )


def _service(executor, *, permitted: bool):
    from eyvan.config import EyvanSettings
    from eyvan.worker.agent.service import CellAgentService

    class _Capacity:
        def capabilities(self):
            return _capabilities(supports_exec=permitted)

    service = CellAgentService.__new__(CellAgentService)
    service.settings = EyvanSettings()
    service.capacity = _Capacity()
    service.execution_backend = "eyvan-native"
    service.executors = None
    return service


def test_a_fleet_that_does_not_permit_exec_says_so(monkeypatch) -> None:
    """The operator's admission decision, distinct from what a backend can do."""

    executor = _ExecCapable()
    service = _service(executor, permitted=False)
    monkeypatch.setattr(type(service), "_executor", lambda _self, _cell: executor)

    with pytest.raises(UnsupportedCapabilityError, match="does not permit exec"):
        service.exec_in_workload("cell-a", "wl-1", ["true"])


def test_a_backend_that_cannot_exec_says_something_different(monkeypatch) -> None:
    """Permitted-but-incapable and capable-but-forbidden are different fixes.

    One is a line of fleet YAML; the other is choosing a different execution
    backend. Collapsing them into one message sends an operator to the wrong
    place.
    """

    executor = _NotExecCapable()
    service = _service(executor, permitted=True)
    monkeypatch.setattr(type(service), "_executor", lambda _self, _cell: executor)

    with pytest.raises(UnsupportedCapabilityError, match="cannot exec"):
        service.exec_in_workload("cell-a", "wl-1", ["true"])


def test_a_permitted_capable_backend_runs_the_command(monkeypatch) -> None:
    """Both gates open is the only combination that runs anything."""

    executor = _ExecCapable()
    service = _service(executor, permitted=True)
    monkeypatch.setattr(type(service), "_executor", lambda _self, _cell: executor)

    result = service.exec_in_workload("cell-a", "wl-1", ["py-spy", "dump"], timeout_s=12)

    assert executor.calls == [("wl-1", ["py-spy", "dump"], 12)]
    assert result["exit_code"] == 0


def test_an_empty_command_is_refused_at_the_agent(monkeypatch) -> None:
    """422 rather than a unit that starts nothing."""

    executor = _ExecCapable()
    service = _service(executor, permitted=True)
    monkeypatch.setattr(type(service), "_executor", lambda _self, _cell: executor)

    with pytest.raises(InvalidWorkloadRequestError):
        service.exec_in_workload("cell-a", "wl-1", [])


def test_the_admission_flag_is_refused_by_default_and_stated_explicitly() -> None:
    """A default-on capability nothing read is precisely how this became dead.

    Two layers, and they are different on purpose. The *driver* capability
    defaults to refusing, so a backend that never thought about exec does not
    silently permit it. The *wire* model has no default at all, so an agent
    cannot report a capability document that omits its answer -- which is
    stronger than a default, because a missing field and "no" stop looking the
    same on the wire.
    """

    from pydantic import ValidationError

    from eyvan.capacity.drivers.base import CellCapabilities

    assert CellCapabilities.__dataclass_fields__["supports_exec"].default is False

    with pytest.raises(ValidationError):
        AgentCapabilities(
            provider="local",
            executor="eyvan-native",
            accelerator_types=(),
            capacity_classes=(),
            supports_checkpoint=True,
            supports_callables=True,
            max_gang_replicas=None,
            resource_enforcement="backend",
        )


def test_a_command_is_a_list_not_a_shell_string() -> None:
    """A string would be split by a shell on the host.

    That turns a diagnostic into an injection surface the moment any part of it
    is templated from a workload id or a user-supplied name.
    """

    from pydantic import ValidationError

    from eyvan.worker.agent.models import ExecRequest

    assert ExecRequest(command=("py-spy", "dump")).command == ("py-spy", "dump")
    with pytest.raises(ValidationError):
        ExecRequest(command=())


# --------------------------------------------------------------------------- init inside a job


def test_init_does_not_start_a_control_plane_on_a_worker(monkeypatch) -> None:
    """``init()`` inside a job would otherwise try to start a daemon on a TPU host.

    The default base URL is loopback, and loopback plus "no client configured"
    is the local-development signal -- so a callable that calls ``init()`` to get
    a client would have started a second control plane on the worker, fighting
    for the state directory and answering with an empty fleet.
    """

    from eyvan.sdk import execution

    started: list[object] = []
    monkeypatch.setattr(execution, "ensure_loopback_control_plane", lambda endpoint: started.append(endpoint))
    monkeypatch.setattr(execution, "_configured_gcp_runtime_profile", lambda _endpoint: None)

    execution.init(
        environ={
            "EYVAN_WORKLOAD_ID": "wl-1",
            "EYVAN_CHILDREN_URL": "https://eyvan.test/api/v1/workloads/wl-1/children",
            "EYVAN_WORKLOAD_TOKEN": "token-value",
        }
    )

    assert started == [], "init started a control plane inside a running job"


def test_init_still_starts_a_control_plane_on_a_laptop(monkeypatch) -> None:
    """The local-development path must keep working; only the in-job case changes."""

    from eyvan.sdk import execution

    started: list[object] = []
    monkeypatch.setattr(execution, "ensure_loopback_control_plane", lambda endpoint: started.append(endpoint))
    monkeypatch.setattr(execution, "_configured_gcp_runtime_profile", lambda _endpoint: None)

    execution.init(environ={})

    assert started, "init stopped starting the local control plane"
