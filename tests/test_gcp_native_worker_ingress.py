# Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Coverage for profile-managed local public ingress to native GCP workers."""

from __future__ import annotations

import ipaddress
import json
import subprocess
import typing as tp
from dataclasses import dataclass, field

import pytest

from eyvan._json import JsonObject
from eyvan.providers.gcp.native_worker_ingress import (
    FirewallNetworkReference,
    FirewallRuleConflictError,
    FirewallRuleSpec,
    GcloudNativeWorkerPublicIngress,
)

_TAG = "eyvan-nw-0123456789abcdef01234567"


def _result(returncode: int = 0, *, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(["gcloud"], returncode, stdout, stderr)


@dataclass(frozen=True, slots=True)
class StaticPublicIpv4Resolver:
    """Return one known address without calling a public discovery service."""

    address: ipaddress.IPv4Address

    def resolve(self) -> ipaddress.IPv4Address:
        """Return the configured address."""

        return self.address


@dataclass(slots=True)
class FirewallRunner:
    """In-memory strict gcloud firewall-rule surface for unit tests."""

    rules: dict[str, JsonObject] = field(default_factory=dict)
    calls: list[tuple[str, ...]] = field(default_factory=list)
    create_race: bool = False

    def __call__(self, args: tp.Sequence[str]) -> subprocess.CompletedProcess[str]:
        """Handle describe/create/update commands emitted by the manager."""

        command = tuple(args)
        self.calls.append(command)
        operation = command[2]
        rule_name = command[3]
        if operation == "describe":
            rule = self.rules.get(rule_name)
            return _result(stdout=json.dumps(rule)) if rule is not None else _result(1, stderr="NOT_FOUND")
        if operation == "create":
            if self.create_race:
                self.create_race = False
                self.rules[rule_name] = self._rule_from_create(command)
                return _result(1, stderr="ALREADY_EXISTS")
            self.rules[rule_name] = self._rule_from_create(command)
            return _result()
        if operation == "update":
            self.rules[rule_name] = self._rule_from_update(command, self.rules[rule_name])
            return _result()
        raise AssertionError(f"unexpected gcloud firewall operation {operation!r}")

    @staticmethod
    def _value(args: tuple[str, ...], flag: str) -> str:
        if flag in args:
            return args[args.index(flag) + 1]
        prefix = f"{flag}="
        for value in args:
            if value.startswith(prefix):
                return value.removeprefix(prefix)
        raise AssertionError(f"missing flag {flag!r}")

    @classmethod
    def _rule_from_create(cls, args: tuple[str, ...]) -> JsonObject:
        project = cls._value(args, "--project")
        network = cls._value(args, "--network")
        return {
            "description": cls._value(args, "--description"),
            "direction": "INGRESS",
            "network": f"https://www.googleapis.com/compute/v1/projects/{project}/global/networks/{network}",
            "priority": int(cls._value(args, "--priority")),
            "targetTags": [cls._value(args, "--target-tags")],
            "allowed": [
                {
                    "IPProtocol": "tcp",
                    "ports": [cls._value(args, "--allow").split(":", 1)[1]],
                }
            ],
            "sourceRanges": [cls._value(args, "--source-ranges")],
            "disabled": False,
        }

    @classmethod
    def _rule_from_update(cls, args: tuple[str, ...], existing: JsonObject) -> JsonObject:
        updated = dict(existing)
        updated["description"] = cls._value(args, "--description")
        updated["priority"] = int(cls._value(args, "--priority"))
        updated["targetTags"] = [cls._value(args, "--target-tags")]
        updated["allowed"] = [
            {
                "IPProtocol": "tcp",
                "ports": [cls._value(args, "--allow").split(":", 1)[1]],
            }
        ]
        updated["sourceRanges"] = [cls._value(args, "--source-ranges")]
        updated["disabled"] = False
        return updated


def _manager(runner: FirewallRunner, address: str = "203.0.113.24") -> GcloudNativeWorkerPublicIngress:
    return GcloudNativeWorkerPublicIngress(
        network_tag=_TAG,
        public_ipv4_resolver=StaticPublicIpv4Resolver(ipaddress.IPv4Address(address)),
        runner=runner,
    )


def _existing_rule(*, source_range: str = "198.51.100.8/32") -> JsonObject:
    network = FirewallNetworkReference.from_config(capacity_project="project-a", network="research")
    return {
        "description": f"eyvan-native-worker-ingress:v1:{_TAG}",
        "direction": "INGRESS",
        "network": f"https://www.googleapis.com/compute/v1/{network.canonical_name}",
        "priority": 1000,
        "targetTags": [_TAG],
        "allowed": [{"IPProtocol": "tcp", "ports": ["8100"]}],
        "sourceRanges": [source_range],
        "disabled": False,
    }


def _rule_name(network: str = "research") -> str:
    target = FirewallNetworkReference.from_config(capacity_project="project-a", network=network)
    return FirewallRuleSpec(
        network=target,
        network_tag=_TAG,
        port=8100,
        source_range=ipaddress.IPv4Network("203.0.113.24/32"),
    ).rule_name


def test_creates_a_narrow_rule_for_only_the_current_public_ipv4() -> None:
    runner = FirewallRunner()

    tag = _manager(runner).ensure(project="project-a", network=None, port=8100)

    assert tag == _TAG
    assert [call[2] for call in runner.calls] == ["describe", "create"]
    create = runner.calls[-1]
    assert "--source-ranges=203.0.113.24/32" in create
    assert "--target-tags=eyvan-nw-0123456789abcdef01234567" in create
    assert "--allow=tcp:8100" in create
    assert all("0.0.0.0/0" not in value for value in create)
    assert runner.rules[_rule_name("default")]["sourceRanges"] == ["203.0.113.24/32"]


def test_existing_exact_rule_is_a_read_only_no_op() -> None:
    runner = FirewallRunner()
    manager = _manager(runner)
    runner.rules[_rule_name()] = _existing_rule(source_range="203.0.113.24/32")

    manager.ensure(project="project-a", network="research", port=8100)

    assert [call[2] for call in runner.calls] == ["describe"]


def test_changed_local_ip_updates_only_the_owned_narrow_rule() -> None:
    runner = FirewallRunner()
    manager = _manager(runner)
    runner.rules[_rule_name()] = _existing_rule()

    manager.ensure(project="project-a", network="research", port=8100)

    assert [call[2] for call in runner.calls] == ["describe", "update"]
    update = runner.calls[-1]
    assert "--source-ranges=203.0.113.24/32" in update
    assert all("0.0.0.0/0" not in value for value in update)
    assert runner.rules[_rule_name()]["sourceRanges"] == ["203.0.113.24/32"]


def test_refuses_to_modify_a_foreign_deterministic_rule() -> None:
    runner = FirewallRunner()
    manager = _manager(runner)
    foreign = _existing_rule()
    foreign["description"] = "not-eyvan"
    runner.rules[_rule_name()] = foreign

    with pytest.raises(FirewallRuleConflictError, match="not owned"):
        manager.ensure(project="project-a", network="research", port=8100)

    assert [call[2] for call in runner.calls] == ["describe"]


def test_create_race_recovers_only_after_validating_the_managed_rule() -> None:
    runner = FirewallRunner(create_race=True)

    _manager(runner).ensure(project="project-a", network="research", port=8100)

    assert [call[2] for call in runner.calls] == ["describe", "create", "describe"]


def test_shared_vpc_uses_the_network_host_project_for_the_rule() -> None:
    runner = FirewallRunner()

    _manager(runner).ensure(
        project="project-a",
        network="projects/network-host/global/networks/research",
        port=8100,
    )

    create = runner.calls[-1]
    assert create[create.index("--project") + 1] == "network-host"
    assert create[create.index("--network") + 1] == "research"
