# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import json
import stat
import threading
from pathlib import Path

import pytest

from eyvan.capacity.drivers.base import ExecutionWorkloadStatus, MutationContext, StaleMutationError
from eyvan.worker.runtime.systemd_transient import (
    SystemdCommandResult,
    SystemdLogsUnavailableError,
    SystemdTransientServiceExecutor,
    systemd_unit_name,
)


def _context(*, key: str = "submit-1", generation: int = 1, token: str = "token-1") -> MutationContext:
    return MutationContext(idempotency_key=key, generation=generation, fencing_token=token)


def _spec(
    *,
    submission_id: str = "workload-1",
    workload_id: str | None = None,
    entrypoint: str = "python -c 'print(1)'",
) -> dict:
    return {
        "entrypoint": entrypoint,
        "submission_id": submission_id,
        "environment": {"EXPERIMENT": "systemd"},
        "gang": {"replicas": 1, "per_replica": {"cpu": 1}, "required_topology": {}},
        "metadata": {"eyvan_workload_id": workload_id or submission_id},
    }


class FakeSystemd:
    """Small stateful command runner that models the systemd lifecycle seam."""

    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.units: dict[str, dict[str, str]] = {}
        self.launch_options: dict[str, set[str]] = {}
        self.background_children: dict[str, bool] = {}
        self.journal: dict[str, str] = {}
        self.fail_launch = False
        self.fail_journal = False

    @staticmethod
    def _unit_from_run(command: tuple[str, ...]) -> str:
        return next(part.removeprefix("--unit=") for part in command if part.startswith("--unit="))

    def complete(self, unit_name: str, *, result: str = "success", status: str = "0") -> None:
        self.units[unit_name] = {
            "LoadState": "loaded",
            "ActiveState": "active",
            "SubState": "exited",
            "Result": result,
            "ExecMainCode": "exited",
            "ExecMainStatus": status,
            "ControlGroup": f"/system.slice/{unit_name}",
        }

    def remove(self, unit_name: str) -> None:
        self.units.pop(unit_name, None)

    def __call__(self, command: tuple[str, ...]) -> SystemdCommandResult:
        self.commands.append(command)
        if command[0] == "systemd-run":
            unit_name = self._unit_from_run(command)
            if self.fail_launch:
                return SystemdCommandResult(returncode=1, stderr="manager unavailable")
            self.units[unit_name] = {
                "LoadState": "loaded",
                "ActiveState": "active",
                "SubState": "running",
                "Result": "success",
                "ExecMainCode": "exited",
                "ExecMainStatus": "0",
                "ControlGroup": f"/system.slice/{unit_name}",
            }
            self.launch_options[unit_name] = set(command)
            self.background_children[unit_name] = True
            return SystemdCommandResult(returncode=0)
        if command[:2] == ("systemctl", "show"):
            unit_name = command[-1]
            properties = self.units.get(unit_name)
            if properties is None:
                return SystemdCommandResult(returncode=4, stdout="LoadState=not-found\n")
            return SystemdCommandResult(
                returncode=0,
                stdout="".join(f"{key}={value}\n" for key, value in properties.items()),
            )
        if command[:2] == ("systemctl", "stop"):
            unit_name = command[-1]
            if unit_name not in self.units:
                return SystemdCommandResult(returncode=4, stderr="unit is absent")
            # This child becomes dead only when the production command declares
            # cgroup-wide kill semantics. The assertion below therefore covers
            # the property that makes a shell's `command &` safe to stop.
            if "--property=KillMode=control-group" in self.launch_options[unit_name]:
                self.background_children[unit_name] = False
            self.units[unit_name].update({"ActiveState": "inactive", "SubState": "dead", "Result": "success"})
            return SystemdCommandResult(returncode=0)
        if command[:2] == ("systemctl", "reset-failed"):
            unit_name = command[-1]
            if unit_name not in self.units:
                return SystemdCommandResult(returncode=4, stderr="unit is absent")
            self.units.pop(unit_name, None)
            return SystemdCommandResult(returncode=0)
        if command[0] == "journalctl":
            unit_name = next(part.removeprefix("--unit=") for part in command if part.startswith("--unit="))
            if self.fail_journal:
                return SystemdCommandResult(returncode=1, stderr="journal unavailable")
            return SystemdCommandResult(returncode=0, stdout=self.journal.get(unit_name, ""))
        raise AssertionError(f"unexpected systemd command: {command}")


def _record(executor: SystemdTransientServiceExecutor, remote_id: str) -> dict:
    return json.loads(executor._record_path(remote_id).read_text(encoding="utf-8"))


def _environment_file_from_launch(launch: tuple[str, ...]) -> Path:
    option = next(part for part in launch if part.startswith("--property=EnvironmentFile="))
    # No "-" ignore prefix: a missing environment file must fail the launch loudly.
    assert not option.startswith("--property=EnvironmentFile=-")
    return Path(option.removeprefix("--property=EnvironmentFile="))


def _environment_from_launch(launch: tuple[str, ...]) -> dict[str, str]:
    env_path = _environment_file_from_launch(launch)
    assert stat.S_IMODE(env_path.stat().st_mode) == 0o600
    return dict(line.split("=", 1) for line in env_path.read_text(encoding="utf-8").splitlines() if line)


def test_unit_names_are_deterministic_sanitized_and_collision_resistant() -> None:
    remote_id = "training/team-a/run 1/with:unsafe!characters"
    unit_name = systemd_unit_name(remote_id)

    assert unit_name == systemd_unit_name(remote_id)
    assert unit_name.startswith("eyvan-training-team-a-run-1-with-unsafe-characters-")
    assert unit_name.endswith(".service")
    assert "/" not in unit_name
    assert " " not in unit_name
    assert systemd_unit_name("same prefix?") != systemd_unit_name("same prefix!")


def test_submit_launches_a_contained_systemd_cgroup_and_stop_kills_background_child(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        use_exit_type_cgroup=True,
        workload_user="eyvan-workload",
        workload_group="eyvan-workload",
        workload_directory="/var/lib/eyvan/workload",
        base_environment={
            "HOME": "/var/lib/eyvan/workload",
            "PATH": "/opt/eyvan-native/venv/bin:/usr/bin",
            "VIRTUAL_ENV": "/opt/eyvan-native/venv",
        },
    )
    context = _context()
    remote_id = executor.submit(_spec(entrypoint="python train.py & wait"), context)
    record = _record(executor, remote_id)
    unit_name = record["unit_name"]

    launch = systemd.commands[0]
    assert launch[:3] == ("systemd-run", "--quiet", "--no-block")
    assert f"--unit={unit_name}" in launch
    assert "--service-type=exec" in launch
    assert "--property=ExitType=cgroup" in launch
    assert "--property=RemainAfterExit=yes" in launch
    assert "--property=KillMode=control-group" in launch
    assert "--property=LimitMEMLOCK=infinity" in launch
    assert "--property=LimitNOFILE=100000" in launch
    assert "--property=NoNewPrivileges=yes" in launch
    assert "--property=User=eyvan-workload" in launch
    assert "--property=Group=eyvan-workload" in launch
    assert "--property=WorkingDirectory=/var/lib/eyvan/workload" in launch
    assert not any(option == "--property=PrivateTmp=yes" for option in launch)
    assert "-lc" not in launch
    # The workload environment must not be visible in unit argv: systemctl
    # show, ps, and the journal would leak values to every host user.
    assert not any(option.startswith("--setenv=") for option in launch)
    assert not any("/opt/eyvan-native/venv" in option for option in launch)
    assert not any("EXPERIMENT" in option for option in launch)
    env_path = _environment_file_from_launch(launch)
    assert env_path.parent == tmp_path / "env"
    env = _environment_from_launch(launch)
    assert env["EXPERIMENT"] == "systemd"
    assert env["HOME"] == "/var/lib/eyvan/workload"
    assert env["PATH"] == "/opt/eyvan-native/venv/bin:/usr/bin"
    assert env["VIRTUAL_ENV"] == "/opt/eyvan-native/venv"
    assert env["EYVAN_REPLICA_INDEX"] == "0"
    assert env["EYVAN_REPLICA_COUNT"] == "1"
    assert launch[-3:] == ("/bin/sh", "-c", "python train.py & wait")
    assert systemd.background_children[unit_name] is True
    assert executor.status(remote_id).status is ExecutionWorkloadStatus.RUNNING

    executor.stop(remote_id, context)

    assert ("systemctl", "stop", "--no-pager", unit_name) in systemd.commands
    assert systemd.background_children[unit_name] is False
    assert executor.status(remote_id).status is ExecutionWorkloadStatus.STOPPED


def test_systemd_249_fallback_observes_the_cgroup_before_marking_an_exited_main_process_complete(tmp_path: Path) -> None:
    systemd = FakeSystemd()

    def cgroup_has_processes(control_group: str) -> bool | None:
        unit_name = control_group.rsplit("/", 1)[-1]
        return systemd.background_children.get(unit_name)

    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        cgroup_processes_reader=cgroup_has_processes,
        use_exit_type_cgroup=False,
    )
    remote_id = executor.submit(_spec(entrypoint="python train.py &"), _context())
    unit_name = _record(executor, remote_id)["unit_name"]
    systemd.complete(unit_name)

    launch = systemd.commands[0]
    assert "--property=ExitType=cgroup" not in launch
    assert executor.status(remote_id).status is ExecutionWorkloadStatus.RUNNING

    systemd.background_children[unit_name] = False

    assert executor.status(remote_id).status is ExecutionWorkloadStatus.SUCCEEDED


def test_systemd_249_fallback_accepts_an_exited_unit_after_its_cgroup_is_released(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        cgroup_processes_reader=lambda _control_group: pytest.fail("released cgroups must not be probed"),
        use_exit_type_cgroup=False,
    )
    remote_id = executor.submit(_spec(), _context())
    unit_name = _record(executor, remote_id)["unit_name"]
    systemd.complete(unit_name)
    systemd.units[unit_name]["ControlGroup"] = ""

    observation = executor.status(remote_id)

    assert observation.status is ExecutionWorkloadStatus.SUCCEEDED
    assert "completed successfully" in observation["message"]


def test_systemd_249_rechecks_a_transiently_unobservable_cgroup(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    readable = False
    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        cgroup_processes_reader=lambda _control_group: False if readable else None,
        use_exit_type_cgroup=False,
    )
    remote_id = executor.submit(_spec(), _context())
    unit_name = _record(executor, remote_id)["unit_name"]
    systemd.complete(unit_name)

    assert executor.status(remote_id).status is ExecutionWorkloadStatus.INDETERMINATE
    readable = True

    assert executor.status(remote_id).status is ExecutionWorkloadStatus.SUCCEEDED


def test_systemd_249_recheckable_cgroup_ambiguity_becomes_terminal_when_the_unit_disappears(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        cgroup_processes_reader=lambda _control_group: None,
        use_exit_type_cgroup=False,
    )
    remote_id = executor.submit(_spec(), _context())
    unit_name = _record(executor, remote_id)["unit_name"]
    systemd.complete(unit_name)

    assert executor.status(remote_id).message == "cannot observe systemd workload cgroup before completion"
    systemd.remove(unit_name)

    observation = executor.status(remote_id)

    assert observation.status is ExecutionWorkloadStatus.INDETERMINATE
    assert observation.message == "systemd service disappeared before a terminal state was recorded"


def test_systemd_249_fallback_fails_closed_when_the_cgroup_cannot_be_read(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        cgroup_processes_reader=lambda _control_group: None,
        use_exit_type_cgroup=False,
    )
    remote_id = executor.submit(_spec(), _context())
    unit_name = _record(executor, remote_id)["unit_name"]
    systemd.complete(unit_name)

    observation = executor.status(remote_id)

    assert observation.status is ExecutionWorkloadStatus.INDETERMINATE
    assert "cannot observe" in observation["message"]


def test_missing_service_before_a_terminal_observation_is_indeterminate_and_persisted(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        use_exit_type_cgroup=True,
    )
    remote_id = executor.submit(_spec(), _context())
    unit_name = _record(executor, remote_id)["unit_name"]
    systemd.remove(unit_name)

    observation = executor.status(remote_id)

    assert observation.status is ExecutionWorkloadStatus.INDETERMINATE
    assert "disappeared" in observation["message"]
    assert _record(executor, remote_id)["status"] == ExecutionWorkloadStatus.INDETERMINATE.value
    with pytest.raises(RuntimeError, match="cannot safely retire ambiguous"):
        executor.retire(remote_id, _context())


def test_malformed_durable_status_is_indeterminate_with_an_explicit_reason(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        use_exit_type_cgroup=True,
    )
    remote_id = executor.submit(_spec(), _context())
    record_path = executor._record_path(remote_id)
    record = _record(executor, remote_id)
    record["status"] = "not-a-systemd-status"
    record_path.write_text(json.dumps(record), encoding="utf-8")
    commands_before = len(systemd.commands)

    observation = executor.status(remote_id)

    assert observation.status is ExecutionWorkloadStatus.INDETERMINATE
    assert "unsupported persisted status" in observation.message
    assert _record(executor, remote_id)["status"] == ExecutionWorkloadStatus.INDETERMINATE.value
    assert len(systemd.commands) == commands_before


def test_terminal_record_survives_transient_unit_collection(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        use_exit_type_cgroup=True,
    )
    remote_id = executor.submit(_spec(), _context())
    unit_name = _record(executor, remote_id)["unit_name"]
    systemd.complete(unit_name)

    assert executor.status(remote_id).status is ExecutionWorkloadStatus.SUCCEEDED
    assert _record(executor, remote_id)["unit_collected"] is True
    assert ("systemctl", "reset-failed", "--no-pager", unit_name) in systemd.commands
    systemd.remove(unit_name)

    observation = executor.status(remote_id)

    assert observation.remote_id == remote_id
    assert observation.status is ExecutionWorkloadStatus.SUCCEEDED
    assert observation.message == "systemd service completed successfully"


def test_inactive_unit_without_a_prior_success_observation_is_not_reported_successful(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        use_exit_type_cgroup=True,
    )
    remote_id = executor.submit(_spec(), _context())
    unit_name = _record(executor, remote_id)["unit_name"]
    systemd.units[unit_name].update({"ActiveState": "inactive", "SubState": "dead", "Result": "success"})

    observation = executor.status(remote_id)

    assert observation.status is ExecutionWorkloadStatus.FAILED
    assert "before Eyvan observed completion" in observation["message"]


def test_boot_id_change_records_lost_without_querying_a_new_manager(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    boot_id = {"value": "boot-a"}
    executor = SystemdTransientServiceExecutor(tmp_path, command_runner=systemd, boot_id_reader=lambda: boot_id["value"])
    remote_id = executor.submit(_spec(), _context())
    boot_id["value"] = "boot-b"
    commands_before = len(systemd.commands)

    observation = executor.status(remote_id)

    assert observation.status is ExecutionWorkloadStatus.LOST
    assert "boot ID changed" in observation["message"]
    assert len(systemd.commands) == commands_before


def test_submit_is_idempotent_and_rejects_stale_fences(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(tmp_path, command_runner=systemd, boot_id_reader=lambda: "boot-a")
    spec = _spec()
    context = _context(generation=7, token="token-7")

    assert executor.submit(spec, context) == "workload-1"
    assert executor.submit(spec, context) == "workload-1"
    assert len([command for command in systemd.commands if command[0] == "systemd-run"]) == 1
    with pytest.raises(StaleMutationError, match="does not own"):
        executor.submit(spec, _context(generation=8, token="token-8"))
    with pytest.raises(StaleMutationError, match="fencing token"):
        executor.submit(spec, _context(generation=7, token="other"))


def test_newer_logical_attempt_stops_the_previous_cgroup_before_launching(tmp_path: Path, monkeypatch) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(tmp_path, command_runner=systemd, boot_id_reader=lambda: "boot-a")
    monkeypatch.setenv("EYVAN_SUPERSEDE_POLL_S", "0.01")
    first = executor.submit(_spec(submission_id="attempt-1", workload_id="training"), _context(generation=1))
    first_unit = _record(executor, first)["unit_name"]

    second = executor.submit(
        _spec(submission_id="attempt-2", workload_id="training"),
        _context(key="submit-2", generation=2, token="token-2"),
    )

    second_unit = _record(executor, second)["unit_name"]
    stop_index = systemd.commands.index(("systemctl", "stop", "--no-pager", first_unit))
    second_launch_index = systemd.commands.index(
        next(command for command in systemd.commands if f"--unit={second_unit}" in command)
    )
    assert systemd.background_children[first_unit] is False
    assert stop_index < second_launch_index


def test_concurrent_newer_generation_cannot_launch_during_the_prior_generation_fence_gap(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    first_launch_entered = threading.Event()
    release_first_launch = threading.Event()
    second_launch_entered = threading.Event()
    failures: list[BaseException] = []
    results: dict[str, str] = {}

    def runner(command: tuple[str, ...]) -> SystemdCommandResult:
        if command[0] == "systemd-run":
            unit_name = FakeSystemd._unit_from_run(command)
            if "attempt-1" in unit_name:
                first_launch_entered.set()
                if not release_first_launch.wait(timeout=3):
                    raise TimeoutError("first launch was not released")
            if "attempt-2" in unit_name:
                second_launch_entered.set()
        return systemd(command)

    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=runner,
        boot_id_reader=lambda: "boot-a",
        use_exit_type_cgroup=True,
    )

    def submit(name: str, spec: dict, context: MutationContext) -> None:
        try:
            results[name] = executor.submit(spec, context)
        except BaseException as exc:  # pragma: no cover - asserted by the caller
            failures.append(exc)

    first = threading.Thread(
        target=submit,
        args=("first", _spec(submission_id="attempt-1", workload_id="training"), _context(generation=1)),
    )
    first.start()
    assert first_launch_entered.wait(timeout=3)

    second = threading.Thread(
        target=submit,
        args=(
            "second",
            _spec(submission_id="attempt-2", workload_id="training"),
            _context(key="submit-2", generation=2, token="token-2"),
        ),
    )
    second.start()
    assert not second_launch_entered.wait(timeout=0.1)

    release_first_launch.set()
    first.join(timeout=3)
    second.join(timeout=3)

    assert not first.is_alive()
    assert not second.is_alive()
    assert failures == []
    assert results == {"first": "attempt-1", "second": "attempt-2"}
    launches = [FakeSystemd._unit_from_run(command) for command in systemd.commands if command[0] == "systemd-run"]
    assert launches[0].startswith("eyvan-attempt-1-")
    assert launches[1].startswith("eyvan-attempt-2-")
    workload_fence = json.loads(executor._workload_fence_path("training").read_text(encoding="utf-8"))
    assert workload_fence["generation"] == 2
    assert workload_fence["remote_id"] == "attempt-2"


def test_launch_failure_is_recorded_only_after_systemd_proves_unit_absent(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    systemd.fail_launch = True
    executor = SystemdTransientServiceExecutor(tmp_path, command_runner=systemd, boot_id_reader=lambda: "boot-a")

    with pytest.raises(RuntimeError, match="failed to launch systemd execution"):
        executor.submit(_spec(), _context())

    assert _record(executor, "workload-1")["status"] == ExecutionWorkloadStatus.FAILED.value


def test_submit_rejects_multiline_environment_values_before_recording(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(tmp_path, command_runner=systemd, boot_id_reader=lambda: "boot-a")
    spec = _spec()
    spec["environment"] = {"TOKEN": "first\nsecond"}

    with pytest.raises(ValueError, match="must not contain a newline"):
        executor.submit(spec, _context())

    assert not executor._record_path("workload-1").exists()
    assert not any(command[0] == "systemd-run" for command in systemd.commands)


def test_logs_distinguish_a_journal_failure_from_an_empty_log(tmp_path: Path) -> None:
    systemd = FakeSystemd()
    executor = SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=systemd,
        boot_id_reader=lambda: "boot-a",
        use_exit_type_cgroup=True,
    )
    remote_id = executor.submit(_spec(), _context())
    unit_name = _record(executor, remote_id)["unit_name"]

    # A genuinely silent workload and a workload unknown to this host both
    # return an empty string; neither is a log transport failure.
    assert executor.logs(remote_id) == ""
    assert executor.logs("absent-execution") == ""

    systemd.journal[unit_name] = "real output\n"
    assert executor.logs(remote_id) == "real output\n"

    systemd.fail_journal = True
    with pytest.raises(SystemdLogsUnavailableError) as captured:
        executor.logs(remote_id)
    assert captured.value.unit_name == unit_name
    assert "journal unavailable" in captured.value.stderr
