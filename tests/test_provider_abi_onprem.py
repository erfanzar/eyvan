# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""On-premises SSH capacity: the claim, the fence, and the marker script itself.

The fake fleet below models what the remote script does; the last section runs
the *real* script through ``/bin/sh`` so that model is not the only thing
tested. What no host test can cover is a real ``ssh`` binary against a real
host -- ``OpenSshCommandRunner`` is deliberately the thinnest possible seam for
exactly that reason.
"""

from __future__ import annotations

import re
import subprocess
import typing as tp
from contextlib import contextmanager
from pathlib import Path

import pytest

from eyvan.capacity import CapacityStatus
from eyvan.capacity.conformance import ProviderConformanceHarness
from eyvan.capacity.drivers.base import (
    CapacityDriver,
    CapacityProviderError,
    CapacityProviderFailureCode,
    MutationContext,
    StaleMutationError,
)
from eyvan.capacity.drivers.onprem_ssh import (
    MARKER_SCRIPT,
    WORKER_SECRET_FILE,
    OnPremSshCapacityDriver,
    RemoteResult,
    RemoteTransportError,
    marker_fingerprint,
)
from eyvan.capacity.drivers.registry import DEFAULT_CAPACITY_BACKENDS
from eyvan.capacity.fleet_state import FleetStateStore, LocalBackend
from eyvan.config import EyvanSettings
from eyvan.worker.runtime.native_bootstrap import derive_native_worker_token

HOSTS = ("eyvan@rack-a", "eyvan@rack-b")


def _context(key: str, generation: int = 1, token: str = "fence-1") -> MutationContext:
    return MutationContext(idempotency_key=key, generation=generation, fencing_token=token)


def _marker(cell: str, uid: str, generation: int, fence: str) -> str:
    return f"cell_id={cell}\nmanagement_uid={uid}\nprovider_generation={generation}\nfence={fence}\n"


def _field(payload: str, name: str) -> str:
    for line in payload.splitlines():
        key, separator, value = line.partition("=")
        if separator and key == name:
            return value
    return ""


class FakeSshFleet:
    """An in-memory rack that answers the marker protocol like a real host.

    An independent implementation of the same state machine as ``MARKER_SCRIPT``
    -- not a call into it -- so a change to one is caught by the script tests at
    the bottom of this module rather than mirrored silently into both.
    """

    def __init__(self) -> None:
        self.markers: dict[str, str] = {}
        #: ``eyvan-cell.marker.started``: the identity each worker was last
        #: started under, or ``"pending"``.
        self.started: dict[str, str] = {}
        #: ``{state_dir}/worker-secret`` on each host.
        self.secrets: dict[str, str] = {}
        #: Every token push, in order, as ``(host, token)``.
        self.secret_pushes: list[tuple[str, str]] = []
        #: Every argv any host was asked to run, for the never-in-argv check.
        self.argvs: list[tuple[str, ...]] = []
        self.lifecycle_commands: list[tuple[str, tuple[str, ...]]] = []
        self.unreachable = False
        self.refuse_claims = False
        self.lifecycle_exit_code = 0

    def run(
        self,
        host: str,
        argv: tp.Sequence[str],
        *,
        stdin: str = "",
        timeout_s: float,
    ) -> RemoteResult:
        """Answer one remote command, or fail the transport inconclusively."""

        del timeout_s
        if self.unreachable:
            raise RemoteTransportError(f"connection to {host} refused")
        self.argvs.append(tuple(argv))
        if tuple(argv[:3]) != ("sh", "-s", "--"):
            self.lifecycle_commands.append((host, tuple(argv)))
            return RemoteResult(exit_code=self.lifecycle_exit_code, stdout="", stderr="")
        secret = self._preamble(stdin)
        action, _path, cell, uid, generation, fence = argv[3:9]
        command = tuple(argv[9:])
        current = self.markers.get(host)
        if action == "observe":
            if current is None:
                return RemoteResult(exit_code=4, stdout="", stderr="")
            return RemoteResult(exit_code=0, stdout=current, stderr="")
        if action in ("claim", "reclaim"):
            if self.refuse_claims:
                return RemoteResult(exit_code=3, stdout="", stderr="host is claimed elsewhere")
            if current is not None and not self._supersedable(current, cell, uid, generation, fence):
                return RemoteResult(exit_code=3, stdout="", stderr="host is claimed elsewhere")
            payload = _marker(cell, uid, int(generation), fence)
            identity = f"{cell} {uid} {generation} {fence}"
            changed = current != payload
            if host in self.started:
                changed = self.started[host] != identity
            self.markers[host] = payload
            if action == "claim":
                start = bool(secret or command)
            else:
                start = changed
            if start:
                self.started[host] = "pending"
                if secret:
                    self.secrets[host] = secret
                    self.secret_pushes.append((host, secret))
                if command:
                    self.lifecycle_commands.append((host, command))
                    if self.lifecycle_exit_code:
                        return RemoteResult(
                            exit_code=8, stdout="", stderr=f"lifecycle command exited {self.lifecycle_exit_code}"
                        )
                self.started[host] = identity
            elif changed:
                self.started[host] = "pending"
            return RemoteResult(exit_code=0, stdout=payload, stderr="")
        if action == "release":
            if current is not None:
                expected = _marker(cell, uid, int(generation), fence)
                if current != expected:
                    return RemoteResult(exit_code=3, stdout="", stderr="host is claimed elsewhere")
                if command:
                    self.lifecycle_commands.append((host, command))
                    if self.lifecycle_exit_code:
                        return RemoteResult(
                            exit_code=8, stdout="", stderr=f"lifecycle command exited {self.lifecycle_exit_code}"
                        )
                self.started.pop(host, None)
                self.markers.pop(host)
            return RemoteResult(exit_code=0, stdout="", stderr="")
        return RemoteResult(exit_code=6, stdout="", stderr="unknown action")

    @staticmethod
    def _preamble(stdin: str) -> str:
        """Return the pushed token, requiring the audited script after it."""

        assert stdin.endswith(MARKER_SCRIPT), "the marker protocol must ship the audited script, not a built command"
        preamble = stdin[: len(stdin) - len(MARKER_SCRIPT)]
        if not preamble:
            return ""
        match = re.fullmatch(r"worker_secret=([0-9a-f]{64})\n", preamble)
        assert match is not None, f"only the token assignment may precede the script, got {preamble!r}"
        return match.group(1)

    @staticmethod
    def _supersedable(current: str, cell: str, uid: str, generation: str, fence: str) -> bool:
        if _field(current, "cell_id") != cell or _field(current, "management_uid") != uid:
            return False
        observed = _field(current, "provider_generation")
        return observed.isdigit() and (
            int(observed) < int(generation) or (int(observed) == int(generation) and _field(current, "fence") == fence)
        )


class OnPremSshConformanceHarness(ProviderConformanceHarness):
    """Run the conformance suite against the on-premises driver.

    The worked example ``docs/providers.md`` points an out-of-tree provider at:
    six hooks, each a fact about a substrate only this provider can produce.
    """

    def __init__(self, tmp_path: Path, factory: type[OnPremSshCapacityDriver] = OnPremSshCapacityDriver) -> None:
        self.tmp_path = tmp_path
        self.factory = factory
        self._built = 0

    @property
    def provider_name(self) -> str:
        """Name the provider under test."""

        return "onprem-ssh"

    def new_driver(self) -> CapacityDriver:
        """Return a driver over a private fake rack and durable document."""

        self._built += 1
        state = FleetStateStore(LocalBackend(self.tmp_path / f"fleet-{self._built}.json"))
        return self.factory(
            state,
            runner=FakeSshFleet(),
            native_worker_port=8100,
            native_worker_state_dir="/var/lib/eyvan/native-worker",
        )

    @staticmethod
    def fleet(driver: CapacityDriver) -> FakeSshFleet:
        """Return the fake rack a driver was built over."""

        return tp.cast(FakeSshFleet, tp.cast(OnPremSshCapacityDriver, driver).runner)

    def desired_capacity(self, cell_id: str) -> dict:
        """Return a two-host on-premises pool declaration."""

        del cell_id
        return {
            "provider": "onprem-ssh",
            "capacity_backend": "onprem-ssh",
            "executor": "eyvan-native",
            "accelerator_type": "gpu",
            "capacity_class": "reserved",
            "region": "rack-1",
            "provider_options": {
                "hosts": list(HOSTS),
                "worker_port": 8100,
                "state_dir": "/var/lib/eyvan/native-worker",
                "start_command": ["/usr/local/bin/eyvan-worker", "start"],
                "stop_command": ["/usr/local/bin/eyvan-worker", "stop"],
            },
        }

    def provider_incarnations(self, driver: CapacityDriver, cell_id: str) -> int:
        """Count distinct claimed incarnations of ``cell_id`` across the rack."""

        fleet = self.fleet(driver)
        return len(
            {
                (_field(payload, "provider_generation"), _field(payload, "fence"))
                for payload in fleet.markers.values()
                if _field(payload, "cell_id") == cell_id
            }
        )

    def leak_previous_incarnation(self, driver: CapacityDriver, cell_id: str) -> None:
        """Leave one host holding a claim from a fencing token nobody owns.

        Written unconditionally rather than by editing whatever the driver
        claimed: a provider that never claimed anything must still face a
        substrate holding something, or the check would be measuring the
        harness instead of the driver.
        """

        fleet = self.fleet(driver)
        payload = fleet.markers.get(HOSTS[0], "")
        fleet.markers[HOSTS[0]] = _marker(
            cell_id,
            _field(payload, "management_uid") or "uid-of-a-previous-incarnation",
            int(_field(payload, "provider_generation") or 1),
            marker_fingerprint("a-token-this-cell-never-held"),
        )

    @contextmanager
    def provider_unreachable(self, driver: CapacityDriver) -> tp.Iterator[None]:
        """Make every ssh call fail without establishing an outcome."""

        fleet = self.fleet(driver)
        fleet.unreachable = True
        try:
            yield
        finally:
            fleet.unreachable = False

    @contextmanager
    def provider_rejects_permanently(self, driver: CapacityDriver) -> tp.Iterator[None]:
        """Make every host conclusively refuse the claim."""

        fleet = self.fleet(driver)
        fleet.refuse_claims = True
        try:
            yield
        finally:
            fleet.refuse_claims = False


def _driver(tmp_path: Path) -> tuple[OnPremSshCapacityDriver, FakeSshFleet, dict]:
    harness = OnPremSshConformanceHarness(tmp_path)
    driver = tp.cast(OnPremSshCapacityDriver, harness.new_driver())
    return driver, harness.fleet(driver), harness.desired_capacity("cell-a")


# --------------------------------------------------------------------------- the driver


def test_apply_claims_every_host_and_publishes_their_endpoints(tmp_path) -> None:
    """A claimed rack is HEALTHY and its worker endpoints are the cell's."""

    driver, fleet, desired = _driver(tmp_path)

    observation = driver.apply_capacity("cell-a", desired, _context("apply"))

    assert observation.state is CapacityStatus.HEALTHY
    assert observation.metadata["native_worker_endpoints"] == ["http://rack-a:8100", "http://rack-b:8100"]
    assert observation.runtime_process_count == 2
    assert set(fleet.markers) == set(HOSTS)
    assert _field(fleet.markers["eyvan@rack-a"], "fence") == marker_fingerprint("fence-1")
    assert ("eyvan@rack-a", ("/usr/local/bin/eyvan-worker", "start")) in fleet.lifecycle_commands


def test_a_host_claimed_by_another_cell_is_refused_conclusively(tmp_path) -> None:
    """Two Eyvan cells cannot both own one machine, and retrying cannot fix it."""

    driver, fleet, desired = _driver(tmp_path)
    fleet.markers["eyvan@rack-b"] = _marker("someone-elses-cell", "uid", 4, "f" * 32)

    with pytest.raises(CapacityProviderError) as raised:
        driver.apply_capacity("cell-a", desired, _context("apply"))

    assert raised.value.code is CapacityProviderFailureCode.CONFIGURATION
    assert raised.value.retryable is False
    assert _field(fleet.markers["eyvan@rack-b"], "cell_id") == "someone-elses-cell"


def test_an_unreachable_rack_is_unobserved_not_absent(tmp_path) -> None:
    """SSH failing is a maintenance window, not evidence the hardware is gone."""

    driver, fleet, desired = _driver(tmp_path)
    driver.apply_capacity("cell-a", desired, _context("apply"))
    fleet.unreachable = True

    observation = driver.observe("cell-a")

    assert observation.state is CapacityStatus.UNOBSERVED
    assert observation.failure_code is CapacityProviderFailureCode.PROVIDER_UNAVAILABLE
    assert observation.metadata["native_worker_endpoints"] == []


def test_a_host_that_lost_its_marker_is_recovered_by_reconcile(tmp_path) -> None:
    """A rebuilt host converges back instead of stranding working hardware."""

    driver, fleet, desired = _driver(tmp_path)
    driver.apply_capacity("cell-a", desired, _context("apply"))
    fleet.markers.pop("eyvan@rack-b")

    assert driver.observe("cell-a").state is CapacityStatus.NEEDS_RECREATE

    reconciled = driver.reconcile("cell-a", _context("reconcile"))

    assert reconciled.state is CapacityStatus.HEALTHY
    assert set(fleet.markers) == set(HOSTS)


def test_release_stops_the_workers_and_drops_every_claim(tmp_path) -> None:
    """Release is what makes an on-premises host reusable by another cell."""

    driver, fleet, desired = _driver(tmp_path)
    driver.apply_capacity("cell-a", desired, _context("apply"))

    released = driver.release("cell-a", _context("release", generation=2))

    assert released.state is CapacityStatus.RELEASED
    assert fleet.markers == {}
    assert ("eyvan@rack-a", ("/usr/local/bin/eyvan-worker", "stop")) in fleet.lifecycle_commands


def test_a_release_replay_removes_the_markers_a_crash_left_behind(tmp_path) -> None:
    """The tombstone carries the host identity, so a replay is not a no-op."""

    driver, fleet, desired = _driver(tmp_path)
    driver.apply_capacity("cell-a", desired, _context("apply"))
    claimed_marker = fleet.markers["eyvan@rack-a"]
    release = _context("release", generation=2)
    driver.release("cell-a", release)
    # A crash after the tombstone, before the host was cleaned: the marker is
    # back and the durable record is already gone.
    fleet.markers["eyvan@rack-a"] = claimed_marker

    replayed = driver.release("cell-a", release)

    assert replayed.state is CapacityStatus.RELEASED
    assert fleet.markers == {}


def test_release_never_deletes_another_cells_claim(tmp_path) -> None:
    """The one destructive mistake this driver could make, it does not make."""

    driver, fleet, desired = _driver(tmp_path)
    driver.apply_capacity("cell-a", desired, _context("apply"))
    fleet.markers["eyvan@rack-a"] = _marker("another-cell", "uid", 9, "e" * 32)

    driver.release("cell-a", _context("release", generation=2))

    assert _field(fleet.markers["eyvan@rack-a"], "cell_id") == "another-cell"


def test_drain_keeps_the_claim_so_a_running_workload_stays_fenced(tmp_path) -> None:
    """A draining cell is still running work; an unclaimed host is claimable."""

    driver, fleet, desired = _driver(tmp_path)
    driver.apply_capacity("cell-a", desired, _context("apply"))

    drained = driver.drain("cell-a", _context("drain"))

    assert drained.state is CapacityStatus.DRAINING
    assert set(fleet.markers) == set(HOSTS)
    assert driver.observe("cell-a").metadata["native_worker_endpoints"] == []


def test_a_stale_fencing_token_cannot_release_the_rack(tmp_path) -> None:
    """The durable fence is checked before any host is touched."""

    driver, fleet, desired = _driver(tmp_path)
    driver.apply_capacity("cell-a", desired, _context("apply"))

    with pytest.raises(StaleMutationError):
        driver.release("cell-a", _context("release", generation=1, token="someone-else"))

    assert set(fleet.markers) == set(HOSTS)


ROOT_SECRET = "root-secret-for-onprem-tests-0123456789"


def _secret_driver(tmp_path: Path) -> tuple[OnPremSshCapacityDriver, FakeSshFleet, dict]:
    driver, fleet, desired = _driver(tmp_path)
    driver.shared_secret = ROOT_SECRET
    return driver, fleet, desired


def _agent_token(observation) -> str:
    """Derive the bearer the agent will present, exactly as its registry does."""

    token = derive_native_worker_token(
        ROOT_SECRET,
        observation.metadata["management_uid"],
        provider_generation=observation.metadata["provider_generation"],
    )
    assert token is not None
    return token


def _starts(fleet: FakeSshFleet) -> list[str]:
    return [host for host, command in fleet.lifecycle_commands if command[-1] == "start"]


def test_management_uid_is_derived_but_the_worker_token_follows_the_incarnation(tmp_path) -> None:
    """The UID is stable; the token is not, and the host is told every time it moves.

    The token is ``HMAC(root, management_uid, provider_generation)``. A replay
    keeps it, a reconcile under the same fence keeps it, and each of a new
    accepted apply, a reconcile under a new fence, and a replacement cell
    changes it -- which is only safe because each of those also pushes the
    new token to every host and restarts the worker.
    """

    first, _, desired = _driver(tmp_path / "a")
    second, _, _ = _driver(tmp_path / "b")
    assert (
        first.apply_capacity("cell-a", desired, _context("apply")).metadata["management_uid"]
        == second.apply_capacity("cell-a", desired, _context("apply")).metadata["management_uid"]
    )

    driver, fleet, desired = _secret_driver(tmp_path / "c")
    applied = driver.apply_capacity("cell-a", desired, _context("apply"))
    first_token = _agent_token(applied)
    assert fleet.secrets == {host: first_token for host in HOSTS}

    replayed = driver.apply_capacity("cell-a", desired, _context("apply"))
    assert _agent_token(replayed) == first_token
    same_fence = driver.reconcile("cell-a", _context("reconcile"))
    assert _agent_token(same_fence) == first_token

    reapplied = driver.apply_capacity("cell-a", desired, _context("apply-again"))
    second_token = _agent_token(reapplied)
    assert second_token != first_token
    assert fleet.secrets == {host: second_token for host in HOSTS}

    moved = driver.reconcile("cell-a", _context("reconcile-2", generation=2, token="fence-2"))
    third_token = _agent_token(moved)
    assert moved.state is CapacityStatus.HEALTHY
    assert third_token not in (first_token, second_token)
    assert fleet.secrets == {host: third_token for host in HOSTS}


def test_apply_pushes_the_agents_token_to_every_host_before_starting(tmp_path) -> None:
    """The file the worker reads holds exactly what the agent will present."""

    driver, fleet, desired = _secret_driver(tmp_path)

    observation = driver.apply_capacity("cell-a", desired, _context("apply"))

    token = _agent_token(observation)
    assert fleet.secret_pushes == [(host, token) for host in HOSTS]
    assert _starts(fleet) == list(HOSTS)
    assert all(fleet.started[host].startswith("cell-a ") for host in HOSTS)


def test_apply_pushes_the_token_even_without_a_start_command(tmp_path) -> None:
    """A unit with Restart=always picks the token up on its own next start."""

    driver, fleet, desired = _secret_driver(tmp_path)
    desired["provider_options"].pop("start_command")

    observation = driver.apply_capacity("cell-a", desired, _context("apply"))

    assert fleet.secrets == {host: _agent_token(observation) for host in HOSTS}
    assert _starts(fleet) == []


def test_reconcile_with_an_unchanged_identity_pushes_and_restarts_nothing(tmp_path) -> None:
    """A reconcile interval must never bounce a worker that is running a workload."""

    driver, fleet, desired = _secret_driver(tmp_path)
    driver.apply_capacity("cell-a", desired, _context("apply"))
    pushes, commands = list(fleet.secret_pushes), list(fleet.lifecycle_commands)

    for index in range(3):
        assert driver.reconcile("cell-a", _context(f"reconcile-{index}")).state is CapacityStatus.HEALTHY

    assert fleet.secret_pushes == pushes
    assert fleet.lifecycle_commands == commands


def test_a_replacement_cell_that_claims_through_reconcile_pushes_and_starts(tmp_path) -> None:
    """The live failure: the replacement's apply raced the release, reconcile took the host.

    Before this, reconcile re-claimed without ``start_command``; the worker kept
    the predecessor's token, answered the agent with 401, and the cell stayed
    ``registering``.
    """

    driver, fleet, desired = _secret_driver(tmp_path)
    driver.apply_capacity("cell-0001", desired, _context("apply-1"))
    with pytest.raises(CapacityProviderError) as raised:
        driver.apply_capacity("cell-0002", desired, _context("apply-2"))
    assert raised.value.code is CapacityProviderFailureCode.CONFIGURATION
    driver.release("cell-0001", _context("release-1", generation=2))
    fleet.lifecycle_commands.clear()
    fleet.secret_pushes.clear()

    reconciled = driver.reconcile("cell-0002", _context("reconcile-2"))

    assert reconciled.state is CapacityStatus.HEALTHY
    token = _agent_token(reconciled)
    assert fleet.secret_pushes == [(host, token) for host in HOSTS]
    assert _starts(fleet) == list(HOSTS)


def test_reconcile_finishes_an_apply_whose_start_failed(tmp_path) -> None:
    """A claim that never started is recorded pending, not mistaken for a running worker."""

    driver, fleet, desired = _secret_driver(tmp_path)
    fleet.lifecycle_exit_code = 2
    with pytest.raises(CapacityProviderError):
        driver.apply_capacity("cell-a", desired, _context("apply"))
    assert fleet.started[HOSTS[0]] == "pending"
    fleet.lifecycle_exit_code = 0
    fleet.lifecycle_commands.clear()

    reconciled = driver.reconcile("cell-a", _context("reconcile"))

    assert reconciled.state is CapacityStatus.HEALTHY
    assert _starts(fleet) == list(HOSTS)
    assert fleet.secrets == {host: _agent_token(reconciled) for host in HOSTS}


def test_reconcile_never_pushes_or_starts_on_a_foreign_host(tmp_path) -> None:
    """The ownership check still precedes everything the host is asked to do."""

    driver, fleet, desired = _secret_driver(tmp_path)
    driver.apply_capacity("cell-a", desired, _context("apply"))
    fleet.markers[HOSTS[0]] = _marker("someone-elses-cell", "uid", 9, "e" * 32)
    fleet.started[HOSTS[0]] = "someone-elses-cell uid 9 " + "e" * 32
    pushes, commands = list(fleet.secret_pushes), list(fleet.lifecycle_commands)

    with pytest.raises(CapacityProviderError) as raised:
        driver.reconcile("cell-a", _context("reconcile", generation=2, token="fence-2"))

    assert raised.value.code is CapacityProviderFailureCode.CONFIGURATION
    assert fleet.secret_pushes == pushes
    assert fleet.lifecycle_commands == commands
    assert _field(fleet.markers[HOSTS[0]], "cell_id") == "someone-elses-cell"


def test_neither_the_token_nor_the_root_secret_is_ever_in_argv(tmp_path) -> None:
    """argv is visible to every user on the host through /proc; stdin is not."""

    driver, fleet, desired = _secret_driver(tmp_path)
    applied = driver.apply_capacity("cell-a", desired, _context("apply"))
    driver.reconcile("cell-a", _context("reconcile", generation=2, token="fence-2"))
    driver.release("cell-a", _context("release", generation=3, token="fence-3"))

    secrets = {ROOT_SECRET, _agent_token(applied), *fleet.secrets.values()}
    assert fleet.secret_pushes
    for argv in fleet.argvs:
        assert not any(secret in part for part in argv for secret in secrets), argv


def test_without_a_root_secret_nothing_is_pushed(tmp_path) -> None:
    """``None`` is an answer: the operator delivers the token another way."""

    driver, fleet, desired = _driver(tmp_path)

    driver.apply_capacity("cell-a", desired, _context("apply"))

    assert fleet.secret_pushes == []
    assert _starts(fleet) == list(HOSTS)


def test_a_start_command_failure_is_conclusive_not_a_retry_loop(tmp_path) -> None:
    """A launcher that exits non-zero will do so again; the operator must see it."""

    driver, fleet, desired = _driver(tmp_path)
    fleet.lifecycle_exit_code = 2

    with pytest.raises(CapacityProviderError) as raised:
        driver.apply_capacity("cell-a", desired, _context("apply"))

    assert raised.value.retryable is False


@pytest.mark.parametrize(
    ("hosts", "match"),
    (
        ([], "non-empty 'hosts' list"),
        (["rack-a", "rack-a"], "must not contain duplicates"),
        (["http://rack-a:8100"], "ssh destination"),
        (["rack a"], "ssh destination"),
        ([" "], "non-empty string"),
    ),
)
def test_the_host_inventory_is_validated_before_anything_is_claimed(tmp_path, hosts, match) -> None:
    driver, fleet, desired = _driver(tmp_path)
    desired["provider_options"]["hosts"] = hosts

    with pytest.raises(ValueError, match=match):
        driver.apply_capacity("cell-a", desired, _context("apply"))
    assert fleet.markers == {}


def test_on_premises_capacity_refuses_spot(tmp_path) -> None:
    """Operator-owned hardware is never provider-preemptible."""

    driver, _, desired = _driver(tmp_path)
    desired["capacity_class"] = "spot"

    with pytest.raises(ValueError, match="never provider-preemptible"):
        driver.apply_capacity("cell-a", desired, _context("apply"))


def test_the_registry_builds_the_on_premises_driver_from_settings(tmp_path) -> None:
    """The new backend is reachable the way an operator selects it."""

    state = FleetStateStore(LocalBackend(tmp_path / "fleet.json"))

    driver = DEFAULT_CAPACITY_BACKENDS.create(
        "onprem-ssh",
        state=state,
        settings=EyvanSettings(capacity_backend="onprem-ssh", native_worker_port=9100),
    )

    assert isinstance(driver, OnPremSshCapacityDriver)
    assert driver.native_worker_port == 9100
    assert driver.shared_secret is None

    secured = DEFAULT_CAPACITY_BACKENDS.create(
        "onprem-ssh",
        state=state,
        settings=EyvanSettings(capacity_backend="onprem-ssh", native_worker_root_secret=ROOT_SECRET),
    )

    assert tp.cast(OnPremSshCapacityDriver, secured).shared_secret == ROOT_SECRET


# --------------------------------------------------------------------------- the real script


def _sh(
    marker: Path,
    action: str,
    cell: str,
    generation: int,
    fence: str = "f" * 32,
    *,
    command: tp.Sequence[str] = (),
    secret: str | None = None,
) -> subprocess.CompletedProcess:
    """Run the shipped marker script through a real POSIX shell."""

    return subprocess.run(
        ["sh", "-s", "--", action, str(marker), cell, "uid-1", str(generation), fence, *command],
        input=OnPremSshCapacityDriver._marker_stdin(secret),
        capture_output=True,
        text=True,
        check=False,
    )


def test_the_marker_script_claims_observes_and_releases(tmp_path) -> None:
    """The audited script, run by /bin/sh, implements the protocol it claims."""

    marker = tmp_path / "state" / "eyvan-cell.marker"

    claimed = _sh(marker, "claim", "cell-a", 1)
    assert claimed.returncode == 0
    assert _field(claimed.stdout, "cell_id") == "cell-a"
    assert marker.read_text() == claimed.stdout

    observed = _sh(marker, "observe", "cell-a", 1)
    assert observed.returncode == 0
    assert _field(observed.stdout, "provider_generation") == "1"

    released = _sh(marker, "release", "cell-a", 1)
    assert released.returncode == 0
    assert not marker.exists()

    assert _sh(marker, "observe", "cell-a", 1).returncode == 4


def test_the_marker_script_refuses_another_cells_host(tmp_path) -> None:
    """The claim is the leak detector; it must never take a foreign host."""

    marker = tmp_path / "eyvan-cell.marker"
    assert _sh(marker, "claim", "cell-a", 1).returncode == 0

    assert _sh(marker, "claim", "cell-b", 9).returncode == 3
    assert _sh(marker, "release", "cell-b", 9).returncode == 3
    assert _field(marker.read_text(), "cell_id") == "cell-a"


def test_the_marker_script_refuses_an_older_incarnation_and_supersedes_a_newer_claim(tmp_path) -> None:
    """An older generation cannot overwrite the claim that replaced it."""

    marker = tmp_path / "eyvan-cell.marker"
    assert _sh(marker, "claim", "cell-a", 5).returncode == 0

    assert _sh(marker, "claim", "cell-a", 4).returncode == 3
    assert _field(marker.read_text(), "provider_generation") == "5"
    assert _sh(marker, "claim", "cell-a", 6).returncode == 0
    assert _field(marker.read_text(), "provider_generation") == "6"


def test_the_marker_script_refuses_a_corrupt_marker(tmp_path) -> None:
    """Something wrote it, and it was not this incarnation."""

    marker = tmp_path / "eyvan-cell.marker"
    marker.write_text("cell_id=cell-a\nprovider_generation=not-a-number\n")

    assert _sh(marker, "claim", "cell-a", 1).returncode == 3


def test_the_marker_script_reports_an_unknown_action(tmp_path) -> None:
    """A protocol mismatch is an error, never a silent success."""

    assert _sh(tmp_path / "eyvan-cell.marker", "destroy", "cell-a", 1).returncode == 6


def _launcher(tmp_path: Path) -> tuple[list[str], Path]:
    """A start command that records each run and what the environment exposed."""

    log = tmp_path / "starts.log"
    launcher = tmp_path / "start-worker"
    launcher.write_text('#!/bin/sh\nprintf "%s\\n" "${worker_secret:-unset}" >> "$1"\n')
    launcher.chmod(0o700)
    return [str(launcher), str(log)], log


def _runs(log: Path) -> list[str]:
    return log.read_text().splitlines() if log.exists() else []


def test_the_marker_script_writes_the_pushed_token_root_only_before_starting(tmp_path) -> None:
    """0600 from creation, replaced atomically, and never exported to the launcher."""

    marker = tmp_path / "state" / "eyvan-cell.marker"
    command, log = _launcher(tmp_path)
    token = "a" * 64

    claimed = _sh(marker, "claim", "cell-a", 1, command=command, secret=token)

    assert claimed.returncode == 0, claimed.stderr
    secret = marker.parent / WORKER_SECRET_FILE
    assert secret.read_text() == token
    assert secret.stat().st_mode & 0o777 == 0o600
    assert _runs(log) == ["unset"]
    assert marker.with_name("eyvan-cell.marker.started").read_text().split() == ["cell-a", "uid-1", "1", "f" * 32]
    assert sorted(path.name for path in marker.parent.iterdir()) == [
        "eyvan-cell.marker",
        "eyvan-cell.marker.started",
        WORKER_SECRET_FILE,
    ]


def test_the_marker_script_reclaims_without_restarting_an_unchanged_worker(tmp_path) -> None:
    """Same identity: no push and no start. A new one: both, after the marker check."""

    marker = tmp_path / "eyvan-cell.marker"
    command, log = _launcher(tmp_path)
    assert _sh(marker, "claim", "cell-a", 1, command=command, secret="a" * 64).returncode == 0

    assert _sh(marker, "reclaim", "cell-a", 1, command=command, secret="b" * 64).returncode == 0
    assert _runs(log) == ["unset"]
    assert (tmp_path / WORKER_SECRET_FILE).read_text() == "a" * 64

    assert _sh(marker, "reclaim", "cell-a", 2, "e" * 32, command=command, secret="c" * 64).returncode == 0
    assert _runs(log) == ["unset", "unset"]
    assert (tmp_path / WORKER_SECRET_FILE).read_text() == "c" * 64

    assert _sh(marker, "reclaim", "cell-b", 9, command=command, secret="d" * 64).returncode == 3
    assert _runs(log) == ["unset", "unset"]
    assert (tmp_path / WORKER_SECRET_FILE).read_text() == "c" * 64


def test_the_marker_script_finishes_a_pending_start_and_spares_a_legacy_claim(tmp_path) -> None:
    """An arbitration-only claim is pending; a claim from before the record is not restarted."""

    marker = tmp_path / "eyvan-cell.marker"
    command, log = _launcher(tmp_path)

    assert _sh(marker, "claim", "cell-a", 1).returncode == 0
    assert marker.with_name("eyvan-cell.marker.started").read_text() == "pending\n"
    assert _sh(marker, "reclaim", "cell-a", 1, command=command).returncode == 0
    assert _runs(log) == ["unset"]

    marker.with_name("eyvan-cell.marker.started").unlink()
    assert _sh(marker, "reclaim", "cell-a", 1, command=command).returncode == 0
    assert _runs(log) == ["unset"]

    assert _sh(marker, "release", "cell-a", 1).returncode == 0
    assert not marker.exists()
    assert not marker.with_name("eyvan-cell.marker.started").exists()
    assert _sh(marker, "reclaim", "cell-a", 1, command=command).returncode == 0
    assert _runs(log) == ["unset", "unset"]
