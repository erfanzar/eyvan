# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Coverage for `eyvan head up`, the one-command GCP control-plane head.

Nothing here talks to GCP, and nothing here claims the commands were accepted by
GCP. What is proven is everything that sits between the operator and the
provider: which commands are issued and in what order, what is refused, what a
second run does *not* do, and where a generated credential is allowed to appear.

The seam is the injected runner, the same boundary
``capacity/providers/gcp/runtime/bootstrap.py`` uses. Assertions are on the exact
argv, the exact bytes delivered on stdin, and the returned values -- never on
whether a mock was called.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import shutil
import subprocess
import typing as tp
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from click.testing import CliRunner

import eyvan.sdk.cli as cli
import eyvan.sdk.commands.head as head_commands
from eyvan.capacity.fleet_config import EyvanFleetConfig
from eyvan.sdk.commands.head import (
    HEAD_ENVIRONMENT_PATH,
    HEAD_FLEET_LABEL_KEY,
    HEAD_MANAGED_LABEL_KEY,
    HEAD_MANAGED_LABEL_VALUE,
    HeadCommandError,
    HeadIngressError,
    HeadOwnershipError,
    HeadProvisioner,
    HeadSecrets,
    HeadSpec,
    HeadStep,
    _LocalWheel,
    render_head_bootstrap_script,
)

FLEET_YAML = """
version: 1
name: research
provider:
  type: gcp
  project: example-project
  region: us-east5
  zone: us-east5-a
executor: eyvan-native
pools:
  cpu:
    kind: cpu
    machine_type: n2-standard-4
    min_workers: 0
    max_workers: 1
    resources:
      CPU: 4
      MEMORY_GIB: 16
    provider_options:
      bootstrap_egress: private-google-access
"""

#: Deterministic material so a test can assert exactly where a secret may appear.
STUB_SECRETS = HeadSecrets(
    metrics_token="metrics-" + "m" * 32,
    artifact_token_key="artifact-" + "a" * 32,
    agent_shared_secret="agent-" + "g" * 32,
    database_password="database-" + "d" * 24,
    bootstrap_token="bootstrap-" + "b" * 32,
)


#: Tests must never build or upload a real distribution. The digest is what the
#: fake host will echo back, so the verification path is still exercised.
_FAKE_WHEEL_BYTES = b"PK\x03\x04 not a real wheel"
_FAKE_WHEEL_DIGEST = __import__("hashlib").sha256(_FAKE_WHEEL_BYTES).hexdigest()


def _fake_wheel() -> _LocalWheel:
    """Return a deterministic stand-in for this installation's wheel."""

    return _LocalWheel(data=_FAKE_WHEEL_BYTES, digest=_FAKE_WHEEL_DIGEST)


def _spec(**overrides: tp.Any) -> HeadSpec:
    """Return one complete head spec for the tests that do not read YAML."""

    values: dict[str, tp.Any] = {
        "fleet": "research",
        "project": "example-project",
        "region": "us-east5",
        "zone": "us-east5-a",
        "egress": "private-google-access",
    }
    values.update(overrides)
    return HeadSpec(**values)


def _owned_instance(spec: HeadSpec, *, address: str = "10.128.0.7") -> dict[str, tp.Any]:
    """Return the instance description gcloud would return for an Eyvan head."""

    return {
        "name": spec.instance_name,
        "status": "RUNNING",
        "labels": {
            HEAD_MANAGED_LABEL_KEY: HEAD_MANAGED_LABEL_VALUE,
            HEAD_FLEET_LABEL_KEY: spec.fleet,
        },
        "networkInterfaces": [{"networkIP": address}],
    }


def _owned_firewall(spec: HeadSpec, name: str, *, ranges: tuple[str, ...]) -> dict[str, tp.Any]:
    """Return the rule description gcloud would return for a managed rule."""

    return {
        "name": name,
        "description": spec.firewall_description,
        "sourceRanges": list(ranges),
    }


@dataclass
class Call:
    """One command the provisioner issued, with the bytes it delivered."""

    args: tuple[str, ...]
    stdin_text: str | None

    @property
    def remote_command(self) -> str | None:
        """Return the remote shell command when this call was an SSH call."""

        if "--command" not in self.args:
            return None
        return self.args[self.args.index("--command") + 1]


@dataclass
class FakeGcloud:
    """An in-memory GCP project that answers the exact commands a head issues."""

    spec: HeadSpec
    instance: dict[str, tp.Any] | None = None
    firewall: dict[str, dict[str, tp.Any]] = field(default_factory=dict)
    subnets: tuple[str, ...] = ("10.128.0.0/20",)
    address: str = "10.128.0.7"
    environment_present: bool = False
    #: Remote-command or gcloud-verb fragment that should fail, and its result.
    failures: dict[str, subprocess.CompletedProcess[str]] = field(default_factory=dict)
    calls: list[Call] = field(default_factory=list)

    def __call__(
        self,
        args: tp.Sequence[str],
        *,
        stdin_text: str | None,
        timeout: float,
    ) -> subprocess.CompletedProcess[str]:
        recorded = Call(args=tuple(args), stdin_text=stdin_text)
        self.calls.append(recorded)
        assert timeout > 0
        # A real host decodes the wheel and echoes sha256sum. The digest check is
        # part of the contract, so the double answers it instead of being bypassed.
        remote_command = args[args.index("--command") + 1] if "--command" in args else ""
        if "base64 -d" in remote_command and stdin_text is not None:
            digest = hashlib.sha256(base64.b64decode(stdin_text)).hexdigest()
            return subprocess.CompletedProcess(list(args), 0, f"{digest}\n", "")
        joined = " ".join(args)
        for fragment, result in self.failures.items():
            if fragment in joined:
                return result
        return self._dispatch(recorded)

    # -- helpers ----------------------------------------------------------

    @property
    def issued(self) -> list[tuple[str, ...]]:
        """Return every argv issued, in order."""

        return [call.args for call in self.calls]

    def issued_matching(self, *fragments: str) -> list[tuple[str, ...]]:
        """Return every argv containing all of ``fragments`` as elements."""

        return [args for args in self.issued if all(fragment in args for fragment in fragments)]

    def stdin_for(self, remote_fragment: str) -> str:
        """Return the stdin delivered to the one SSH call matching a fragment."""

        payloads = [
            call.stdin_text
            for call in self.calls
            if call.stdin_text is not None and remote_fragment in (call.remote_command or "")
        ]
        assert len(payloads) == 1, f"expected one call matching {remote_fragment!r}, got {len(payloads)}"
        assert payloads[0] is not None
        return payloads[0]

    # -- dispatch ---------------------------------------------------------

    def _dispatch(self, call: Call) -> subprocess.CompletedProcess[str]:
        args = call.args
        if args[:3] == ("compute", "instances", "describe"):
            if self.instance is None:
                return _missing(args)
            if any(argument.startswith("--format=value(") for argument in args):
                return _ok(args, self.address)
            return _ok(args, json.dumps(self.instance))
        if args[:3] == ("compute", "instances", "create"):
            self.instance = _owned_instance(self.spec, address=self.address)
            return _ok(args, "")
        if args[:3] == ("compute", "instances", "delete"):
            self.instance = None
            return _ok(args, "")
        if args[:4] == ("compute", "networks", "subnets", "list"):
            return _ok(args, "\n".join(self.subnets) + "\n")
        if args[:3] == ("compute", "firewall-rules", "describe"):
            existing = self.firewall.get(args[3])
            return _ok(args, json.dumps(existing)) if existing is not None else _missing(args)
        if args[:3] == ("compute", "firewall-rules", "create"):
            self.firewall[args[3]] = {
                "name": args[3],
                "description": _flag_value(args, "--description="),
                "sourceRanges": _flag_value(args, "--source-ranges=").split(","),
            }
            return _ok(args, "")
        if args[:3] == ("compute", "firewall-rules", "delete"):
            self.firewall.pop(args[3], None)
            return _ok(args, "")
        if args[:2] == ("compute", "ssh"):
            return self._ssh(call)
        raise AssertionError(f"unexpected gcloud command: {args}")

    def _ssh(self, call: Call) -> subprocess.CompletedProcess[str]:
        remote = call.remote_command or ""
        if "eyvan-head-env-present" in remote:
            marker = "eyvan-head-env-present" if self.environment_present else "eyvan-head-env-absent"
            return _ok(call.args, marker + "\n")
        if HEAD_ENVIRONMENT_PATH in remote and "install -D" in remote:
            self.environment_present = True
            return _ok(call.args, "")
        if "is-active" in remote:
            return _ok(call.args, "active\n")
        return _ok(call.args, "")


def _ok(args: tp.Sequence[str], stdout: str) -> subprocess.CompletedProcess[str]:
    """Return a successful process result."""

    return subprocess.CompletedProcess(list(args), 0, stdout=stdout, stderr="")


def _missing(args: tp.Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Return the not-found result gcloud produces for an absent resource."""

    return subprocess.CompletedProcess(list(args), 1, stdout="", stderr="ERROR: (gcloud) NOT_FOUND: was not found\n")


def _failed(detail: str, *, returncode: int = 1) -> subprocess.CompletedProcess[str]:
    """Return a failing process result with a diagnostic on stderr."""

    return subprocess.CompletedProcess([], returncode, stdout="", stderr=detail)


def _ok_with_status(stdout: str, *, returncode: int) -> subprocess.CompletedProcess[str]:
    """Return a result that prints on stdout and still exits non-zero."""

    return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr="")


def _flag_value(args: tp.Sequence[str], prefix: str) -> str:
    """Return the value of one ``--flag=value`` argument."""

    for argument in args:
        if argument.startswith(prefix):
            return argument[len(prefix) :]
    raise AssertionError(f"no argument starting with {prefix!r} in {args}")


def _environment(text: str) -> dict[str, str]:
    """Parse a rendered systemd environment file."""

    return dict(line.split("=", 1) for line in text.splitlines() if line)


@pytest.fixture(autouse=True)
def never_reach_a_real_project(monkeypatch) -> None:
    """Make the real gcloud boundary unreachable from this module.

    Not a precaution in the abstract: an earlier version of these tests bound
    the real runner as a default argument, and running the suite created a VM
    and two firewall rules in a live project. The seam is only a seam if
    forgetting to inject it fails loudly.
    """

    def refuse(args: tp.Sequence[str], *, stdin_text: str | None, timeout: float) -> tp.NoReturn:
        raise AssertionError(f"tests must not run gcloud: {' '.join(args)}")

    monkeypatch.setattr(head_commands, "_run_gcloud", refuse)


def test_a_provisioner_with_no_injected_runner_uses_the_module_boundary() -> None:
    """The guard above only works if the runner is resolved at call time.

    A default argument would bind the real function at import, which is exactly
    how the accident happened.
    """

    with pytest.raises(AssertionError, match="tests must not run gcloud"):
        HeadProvisioner(_spec()).status()


def test_a_dry_run_prints_every_action_and_resource_name_and_issues_no_command(tmp_path: Path) -> None:
    """A rehearsal that runs a command is not a rehearsal.

    The plan is what makes this command approvable before it is affordable, so
    the guarantee has to be that nothing at all reaches the provider boundary --
    not "nothing mutating", which is a judgement call a reader cannot check.
    """

    config = tmp_path / "fleet.yaml"
    config.write_text(FLEET_YAML, encoding="utf-8")
    calls: list[tuple[str, ...]] = []

    def refuse(args: tp.Sequence[str], *, stdin_text: str | None, timeout: float):
        calls.append(tuple(args))
        raise AssertionError("a dry run must not reach gcloud")

    spec = HeadSpec.from_fleet_config(EyvanFleetConfig.from_yaml(config))
    plan = HeadProvisioner(spec, runner=refuse).plan()
    rendered = "\n".join(plan.render_lines())

    assert calls == []
    for name in spec.resource_names.values():
        assert name in rendered
    steps = [action.step for action in plan.actions]
    assert steps == [
        HeadStep.INSTANCE_LOOKUP,
        HeadStep.INSTANCE_CREATE,
        HeadStep.INSTANCE_ADDRESS,
        HeadStep.SUBNET_LOOKUP,
        HeadStep.FIREWALL_LOOKUP,
        HeadStep.FIREWALL_CREATE,
        HeadStep.FIREWALL_LOOKUP,
        HeadStep.FIREWALL_CREATE,
        HeadStep.SECRET_LOOKUP,
        HeadStep.CONFIGURE,
        HeadStep.STAGE_WHEEL,
        HeadStep.INSTALL,
        HeadStep.BOOTSTRAP,
        HeadStep.MIGRATE,
        HeadStep.SERVICE_START,
        HeadStep.SERVICE_VERIFY,
    ]
    for action in plan.actions:
        assert action.command in rendered


def test_the_dry_run_cli_changes_nothing_and_names_every_resource(monkeypatch, tmp_path: Path) -> None:
    """The flag, not just the class: --dry-run is what an operator actually types."""

    config = tmp_path / "fleet.yaml"
    config.write_text(FLEET_YAML, encoding="utf-8")

    def refuse(args, *, stdin_text, timeout):
        raise AssertionError("a dry run must not reach gcloud")

    monkeypatch.setattr(head_commands, "_run_gcloud", refuse)

    invoked = CliRunner().invoke(head_commands.head_group, ["up", str(config), "--dry-run", "--json"])

    assert invoked.exit_code == 0, invoked.output
    document = json.loads(invoked.output)
    assert document["dry_run"] is True
    assert document["spec"]["resources"] == {
        "firewall_rule": "research-head-api",
        "instance": "research-head",
        "network_tag": "research-head",
        "ssh_firewall_rule": "research-head-iap-ssh",
    }
    assert [action["step"] for action in document["actions"]].count("firewall_create") == 2
    assert all(action["command"].startswith("gcloud ") for action in document["actions"])


def test_the_executed_sequence_is_exactly_the_planned_sequence() -> None:
    """A plan that diverges from the run is a plan that cannot be reviewed.

    Both are built from the same action constructors, and this is the test that
    keeps them there: the only difference permitted is the source range, which a
    dry run deliberately does not discover.
    """

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)
    planned = [action.args for action in HeadProvisioner(spec, runner=gcloud).plan().actions]

    HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    executed = gcloud.issued
    assert len(executed) == len(planned)
    for planned_args, executed_args in zip(planned, executed, strict=True):
        if head_commands.UNRESOLVED_SOURCE_RANGES in " ".join(planned_args):
            assert "--source-ranges=10.128.0.0/20" in executed_args
            continue
        assert planned_args == executed_args


def test_up_creates_one_labelled_head_and_returns_its_internal_address() -> None:
    """Adoption is only safe if creation leaves the label adoption checks for."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)

    result = HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    created = gcloud.issued_matching("create", "research-head")[0]
    assert "--labels" in created
    assert created[created.index("--labels") + 1] == (
        f"{HEAD_FLEET_LABEL_KEY}=research,{HEAD_MANAGED_LABEL_KEY}={HEAD_MANAGED_LABEL_VALUE}"
    )
    assert "--no-address" in created
    assert result.address == "10.128.0.7"
    assert result.url == "http://10.128.0.7:8080"
    assert result.next_command == "eyvan --url http://10.128.0.7:8080 up <fleet.yaml>"
    assert result.service_state == "active"
    assert result.adopted_instance is False


def test_control_plane_ingress_is_the_vpc_only_and_never_the_internet() -> None:
    """The one rule that must never be got wrong, asserted on the emitted argv."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec, subnets=("10.128.0.0/20", "172.16.4.0/22"))

    result = HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    api_rule = gcloud.issued_matching("create", "research-head-api")[0]
    assert _flag_value(api_rule, "--source-ranges=") == "10.128.0.0/20,172.16.4.0/22"
    assert _flag_value(api_rule, "--allow=") == "tcp:8080"
    assert _flag_value(api_rule, "--target-tags=") == "research-head"
    assert result.source_ranges == ("10.128.0.0/20", "172.16.4.0/22")
    assert "0.0.0.0/0" not in " ".join(" ".join(args) for args in gcloud.issued)


def test_a_subnet_range_that_is_not_internal_is_refused_rather_than_opened() -> None:
    """Refusing is the requirement. A VPC Eyvan cannot prove is private is one
    whose ranges it must not put in front of a control-plane port."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec, subnets=("10.128.0.0/20", "203.0.113.0/24"))

    with pytest.raises(HeadIngressError, match=re.escape("203.0.113.0/24")) as failure:
        HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    assert failure.value.step is HeadStep.SUBNET_LOOKUP
    assert "--source-range" in failure.value.detail
    assert gcloud.issued_matching("firewall-rules", "create") == []


def test_a_vpc_with_no_subnet_in_the_region_stops_before_any_ingress_rule() -> None:
    """No discovered range is not an empty allowlist; it is an unanswerable question."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec, subnets=())

    with pytest.raises(HeadIngressError, match="cannot restrict"):
        HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    assert gcloud.issued_matching("firewall-rules", "create") == []


def test_an_owned_rule_widened_to_the_internet_stops_the_run() -> None:
    """Eyvan created the rule; something else opened it. Continuing would put a
    control plane behind an allowlist that is not one."""

    spec = _spec()
    gcloud = FakeGcloud(
        spec=spec,
        instance=_owned_instance(spec),
        firewall={spec.firewall_rule_name: _owned_firewall(spec, spec.firewall_rule_name, ranges=("0.0.0.0/0",))},
    )

    with pytest.raises(HeadIngressError, match=re.escape("0.0.0.0/0")) as failure:
        HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    assert failure.value.step is HeadStep.FIREWALL_LOOKUP
    assert gcloud.issued_matching("compute", "ssh") == []


def test_explicit_source_ranges_replace_discovery_and_must_still_be_internal() -> None:
    """An operator may pin the allowlist; they may not widen it."""

    gcloud = FakeGcloud(spec=_spec())
    pinned = _spec(source_ranges=("10.24.0.0/16",))

    HeadProvisioner(pinned, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    assert gcloud.issued_matching("compute", "networks", "subnets", "list") == []
    assert _flag_value(gcloud.issued_matching("create", "research-head-api")[0], "--source-ranges=") == "10.24.0.0/16"
    with pytest.raises(ValueError, match="not an internal VPC range"):
        _spec(source_ranges=("0.0.0.0/0",))
    with pytest.raises(ValueError, match="not an internal VPC range"):
        _spec(source_ranges=("8.8.8.0/24",))


def test_up_refuses_a_same_named_instance_it_did_not_create() -> None:
    """The most expensive kind of idempotence is the kind that adopts a stranger."""

    spec = _spec()
    gcloud = FakeGcloud(
        spec=spec,
        instance={"name": spec.instance_name, "status": "RUNNING", "labels": {"team": "someone-else"}},
    )

    with pytest.raises(HeadOwnershipError, match="did not create it") as failure:
        HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    assert failure.value.step is HeadStep.INSTANCE_LOOKUP
    assert gcloud.issued_matching("compute", "instances", "create") == []
    assert gcloud.issued_matching("compute", "ssh") == []


def test_up_refuses_a_firewall_rule_name_another_rule_already_owns() -> None:
    """A rule with the right name and someone else's description is not ours."""

    spec = _spec()
    gcloud = FakeGcloud(
        spec=spec,
        instance=_owned_instance(spec),
        firewall={
            spec.firewall_rule_name: {
                "name": spec.firewall_rule_name,
                "description": "platform team: allow prometheus",
                "sourceRanges": ["10.128.0.0/20"],
            }
        },
    )

    with pytest.raises(HeadOwnershipError, match="will not change it") as failure:
        HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    assert failure.value.step is HeadStep.FIREWALL_LOOKUP
    assert gcloud.issued_matching("firewall-rules", "create") == []


def test_a_second_up_creates_nothing_and_keeps_the_first_run_credentials() -> None:
    """Running it twice must produce one head, and must not rotate its keys.

    Rewriting ``EYVAN_ARTIFACT_TOKEN_KEY`` on a re-run would invalidate every
    capability token already minted, which looks exactly like an outage in the
    artifact path and nothing like the idempotent command that caused it.
    """

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)
    provisioner = HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS)

    first = provisioner.up()
    gcloud.calls.clear()
    second = provisioner.up()

    assert first.adopted_instance is False
    assert first.wrote_environment is True
    assert second.adopted_instance is True
    assert second.wrote_environment is False
    assert second.created_firewall_rules == ()
    assert gcloud.issued_matching("compute", "instances", "create") == []
    assert gcloud.issued_matching("firewall-rules", "create") == []
    assert [
        call for call in gcloud.calls if call.stdin_text is not None and "EYVAN_METRICS_TOKEN" in call.stdin_text
    ] == []


def test_rotate_secrets_is_the_only_way_an_adopted_head_gets_new_credentials() -> None:
    """Opt-in, because the cost of rotating is paid by tokens already in flight."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec, instance=_owned_instance(spec), environment_present=True)

    result = HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up(rotate_secrets=True)

    assert result.wrote_environment is True
    assert "EYVAN_METRICS_TOKEN" in gcloud.stdin_for(HEAD_ENVIRONMENT_PATH)


def test_the_head_environment_carries_every_setting_the_server_refuses_to_start_without() -> None:
    """Three crash-restart cycles, removed by generating all three at once.

    A network-bound Eyvan server refuses to start without ``postgres`` and a
    metrics token, and refuses to mint artifact tokens without a signing key. The
    manual path discovers those one restart at a time.
    """

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)

    HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    environment = _environment(gcloud.stdin_for(HEAD_ENVIRONMENT_PATH))
    assert environment["EYVAN_REPOSITORY_BACKEND"] == "postgres"
    assert environment["EYVAN_METRICS_TOKEN"] == STUB_SECRETS.metrics_token
    assert environment["EYVAN_ARTIFACT_TOKEN_KEY"] == STUB_SECRETS.artifact_token_key
    assert environment["EYVAN_AGENT_SHARED_SECRET"] == STUB_SECRETS.agent_shared_secret
    assert environment["EYVAN_DATABASE_URL"] == (
        f"postgresql+asyncpg://eyvan:{STUB_SECRETS.database_password}@127.0.0.1:5432/eyvan"
    )
    assert environment["EYVAN_PUBLIC_URL"] == "http://10.128.0.7:8080"


def test_generated_credentials_are_delivered_on_stdin_and_never_in_an_argument() -> None:
    """A credential in argv is readable in ``ps`` on both ends of the connection."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)

    HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    for value in STUB_SECRETS.values():
        assert not any(value in argument for args in gcloud.issued for argument in args)
    configure = [call for call in gcloud.calls if call.stdin_text and "EYVAN_METRICS_TOKEN" in call.stdin_text]
    assert len(configure) == 1
    assert configure[0].remote_command == f"sudo install -D -m 0600 -o root -g root /dev/stdin {HEAD_ENVIRONMENT_PATH}"


def test_generated_credentials_are_distinct_high_entropy_values() -> None:
    """One value reused across three settings collapses three trust boundaries."""

    generated = HeadSecrets.generate()

    values = generated.values()
    assert len(set(values)) == 4
    assert all(len(value) >= 24 for value in values)
    assert all(len(value) >= 32 for value in values[:3])
    assert "***" in repr(generated)
    assert not any(value in repr(generated) for value in values)


def test_the_non_secret_result_document_contains_no_generated_material() -> None:
    """Automation output is copied into logs and tickets."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)

    result = HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    encoded = json.dumps(result.as_document())
    assert not any(value in encoded for value in STUB_SECRETS.values())
    assert result.as_document()["url"] == "http://10.128.0.7:8080"


def test_a_head_with_no_issuer_says_so_instead_of_answering_401_silently() -> None:
    """The server starts either way; only one of those is worth reporting as ready."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)

    result = HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    warning = result.authentication_warning
    assert warning is not None
    assert "bootstrap token" in warning
    assert "--oidc-issuer" in warning, "the warning must name the way to retire the bootstrap token"

    environment = _environment(gcloud.stdin_for(HEAD_ENVIRONMENT_PATH))
    assert environment["EYVAN_BOOTSTRAP_TOKEN"] == STUB_SECRETS.bootstrap_token
    assert "EYVAN_OIDC_ISSUER" not in environment


def test_configuring_an_issuer_removes_the_warning_and_reaches_the_head() -> None:
    """The remedy the warning names has to be the one the flag performs."""

    spec = _spec(oidc_issuer="https://issuer.example.com", oidc_audience="eyvan-prod")
    gcloud = FakeGcloud(spec=spec)

    result = HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    environment = _environment(gcloud.stdin_for(HEAD_ENVIRONMENT_PATH))
    assert environment["EYVAN_OIDC_ISSUER"] == "https://issuer.example.com"
    assert environment["EYVAN_OIDC_AUDIENCE"] == "eyvan-prod"
    assert result.authentication_warning is None


def test_a_step_that_fails_midway_names_the_step_and_stops_the_sequence() -> None:
    """Half a head is the normal outcome of a fifteen-step manual install.

    What must not happen is a half-installed head reported as running, so the
    failure carries the step and nothing after it is attempted.
    """

    spec = _spec()
    failure_result = _failed("E: Unable to fetch archives")
    gcloud = FakeGcloud(spec=spec, failures={"--command sudo /opt/eyvan/head-bootstrap.sh": failure_result})

    with pytest.raises(HeadCommandError, match="Unable to fetch archives") as failure:
        HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    assert failure.value.step is HeadStep.BOOTSTRAP
    remote_commands = [call.remote_command for call in gcloud.calls]
    assert "sudo /opt/eyvan/head-bootstrap.sh" in remote_commands
    assert not any(remote and "eyvan-migrate" in remote for remote in remote_commands)
    assert not any(remote and "systemctl enable" in remote for remote in remote_commands)


def test_an_unreachable_head_never_becomes_a_reason_to_rotate_its_credentials() -> None:
    """The probe's failure mode matters more than the probe.

    Reading "ssh failed" as "no credentials yet" would rewrite a live head's
    signing key over one bad minute of network.
    """

    spec = _spec()
    gcloud = FakeGcloud(
        spec=spec,
        instance=_owned_instance(spec),
        environment_present=True,
        failures={
            "eyvan-head-env-present": _failed("ssh: connect to host port 22: Connection timed out", returncode=255)
        },
    )

    with pytest.raises(HeadCommandError, match="Connection timed out") as failure:
        HeadProvisioner(
            spec,
            runner=gcloud,
            secret_factory=lambda: STUB_SECRETS,
            ssh_ready_attempts=2,
            ssh_ready_delay_s=0,
            sleep=lambda _: None,
        ).up()

    assert failure.value.step is HeadStep.SECRET_LOOKUP
    assert [call for call in gcloud.calls if call.stdin_text is not None] == []


def test_a_head_that_answers_something_unexpected_is_not_treated_as_fresh() -> None:
    """A shell that prints a banner is not a head without credentials."""

    spec = _spec()
    gcloud = FakeGcloud(
        spec=spec,
        instance=_owned_instance(spec),
        failures={"eyvan-head-env-present": _ok([], "Welcome to Ubuntu\n")},
    )

    with pytest.raises(HeadCommandError, match="credential-presence marker"):
        HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()


def test_the_installed_unit_survives_ssh_exit_and_reboot() -> None:
    """`nohup ... &` is the failure this replaces; systemd is the fix, stated."""

    script = render_head_bootstrap_script(_spec())

    assert "[Install]\nWantedBy=multi-user.target" in script
    assert "Restart=always" in script
    assert "ExecStart=/opt/eyvan/venv/bin/eyvan-server --host 0.0.0.0 --port 8080" in script
    assert f"EnvironmentFile={HEAD_ENVIRONMENT_PATH}" in script
    assert "systemctl daemon-reload" in script
    plan = HeadProvisioner(_spec()).plan()
    start = next(action for action in plan.actions if action.step is HeadStep.SERVICE_START)
    assert start.args[-1] == "sudo systemctl enable --now eyvan-head.service"


def test_the_installer_carries_no_credential_and_does_not_require_uv() -> None:
    """`uv` missing on a fresh VM was one of the six stop-the-world failures."""

    script = render_head_bootstrap_script(_spec())

    assert not any(value in script for value in STUB_SECRETS.values())
    assert "uv build" not in script
    assert "uv pip" not in script
    assert "python3 -m venv /opt/eyvan/venv" in script
    # The staged wheel, not the name `eyvan`: that name resolves only from an
    # index, and a real bootstrap died on "No matching distribution found".
    assert (
        "/opt/eyvan/venv/bin/python -m pip install --disable-pip-version-check --upgrade /opt/eyvan/eyvan-head.whl"
        in script
    )
    assert not any(line.rstrip().endswith(" eyvan") for line in script.splitlines() if "pip install" in line)


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash is required to parse the generated installer")
def test_the_generated_installer_is_syntactically_valid_bash() -> None:
    """It runs unattended under sudo on a machine nobody is watching."""

    for spec in (_spec(), _spec(database_url="postgresql+asyncpg://eyvan:pw@10.9.0.3:5432/eyvan")):
        script = render_head_bootstrap_script(spec)
        parsed = subprocess.run(
            ["bash", "-n"],
            input=script,
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        assert parsed.returncode == 0, parsed.stderr


def test_an_external_database_replaces_the_head_local_postgresql() -> None:
    """A fleet with a managed database should not get a second one on the head."""

    external = _spec(database_url="postgresql+asyncpg://eyvan:pw@10.9.0.3:5432/eyvan")
    gcloud = FakeGcloud(spec=external)

    HeadProvisioner(external, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    script = render_head_bootstrap_script(external)
    assert "install -y --no-install-recommends postgresql" not in script
    assert "createdb" not in script
    assert "After=network-online.target\n" in script
    environment = _environment(gcloud.stdin_for(HEAD_ENVIRONMENT_PATH))
    assert environment["EYVAN_DATABASE_URL"] == "postgresql+asyncpg://eyvan:pw@10.9.0.3:5432/eyvan"
    local = render_head_bootstrap_script(_spec())
    assert "install -y --no-install-recommends postgresql" in local
    assert "createdb --owner eyvan eyvan" in local


def test_a_database_url_a_shell_would_reinterpret_is_refused() -> None:
    """It becomes a line in an environment file the migration step sources."""

    with pytest.raises(ValueError, match="percent-encode it"):
        _spec(database_url="postgresql+asyncpg://eyvan:pa$$word@10.9.0.3:5432/eyvan")


def test_migration_runs_with_the_heads_own_environment_before_the_service_starts() -> None:
    """A control plane started against an unmigrated schema fails at first request."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)

    HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    remote_commands = [call.remote_command for call in gcloud.calls if call.remote_command]
    migrate = next(command for command in remote_commands if "eyvan-migrate" in command)
    assert HEAD_ENVIRONMENT_PATH in migrate
    assert remote_commands.index(migrate) < remote_commands.index("sudo systemctl enable --now eyvan-head.service")


def test_public_internet_egress_keeps_an_external_address_and_still_no_public_ingress() -> None:
    """An external address is an egress decision; ingress stays the VPC's."""

    spec = _spec(egress="public-internet")
    gcloud = FakeGcloud(spec=spec)

    HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    created = gcloud.issued_matching("compute", "instances", "create")[0]
    assert "--no-address" not in created
    assert _flag_value(gcloud.issued_matching("create", "research-head-api")[0], "--source-ranges=") == "10.128.0.0/20"


def test_the_egress_modes_are_the_ones_the_capacity_path_accepts() -> None:
    """The set an operator has to guess today, stated in one importable place."""

    assert head_commands.BOOTSTRAP_EGRESS_MODES == ("cloud-nat", "private-google-access", "public-internet")
    with pytest.raises(ValueError, match="cloud-nat, private-google-access, public-internet"):
        _spec(egress="nat")


def test_a_head_inherits_project_zone_and_egress_from_the_fleet_it_will_control(tmp_path: Path) -> None:
    """The head belongs where the fleet is; nothing should have to be retyped."""

    config = tmp_path / "fleet.yaml"
    config.write_text(FLEET_YAML, encoding="utf-8")

    spec = HeadSpec.from_fleet_config(EyvanFleetConfig.from_yaml(config))

    assert (spec.project, spec.region, spec.zone) == ("example-project", "us-east5", "us-east5-a")
    assert spec.egress == "private-google-access"
    assert spec.instance_name == "research-head"
    assert spec.has_external_address is False


def test_pools_that_disagree_about_egress_produce_an_error_naming_the_legal_values(tmp_path: Path) -> None:
    """The failure that cost an afternoon: a rejected value whose legal set was
    documented nowhere. The message is the documentation."""

    config = tmp_path / "fleet.yaml"
    config.write_text(
        FLEET_YAML
        + """  nvidia-tesla-a100:
    kind: gpu
    machine_type: a2-highgpu-1g
    resources:
      CPU: 12
      MEMORY_GIB: 85
      GPU: 1
    provider_options:
      bootstrap_egress: public-internet
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="cloud-nat, private-google-access, public-internet"):
        HeadSpec.from_fleet_config(EyvanFleetConfig.from_yaml(config))

    resolved = HeadSpec.from_fleet_config(EyvanFleetConfig.from_yaml(config), egress="cloud-nat")
    assert resolved.egress == "cloud-nat"


def test_status_reports_the_address_ingress_and_unit_state_of_a_running_head() -> None:
    """ "Is it up" has three answers and an operator needs all three."""

    spec = _spec()
    gcloud = FakeGcloud(
        spec=spec,
        instance=_owned_instance(spec),
        firewall={
            spec.firewall_rule_name: _owned_firewall(spec, spec.firewall_rule_name, ranges=("10.128.0.0/20",)),
        },
    )

    result = HeadProvisioner(spec, runner=gcloud).status()

    assert result.exists is True
    assert result.instance_status == "RUNNING"
    assert result.address == "10.128.0.7"
    assert result.service_state == "active"
    assert result.source_ranges == ("10.128.0.0/20",)
    assert result.as_document()["url"] == "http://10.128.0.7:8080"


def test_status_reports_an_absent_head_without_failing_and_without_ssh() -> None:
    """Asking about a head that was never created is a question, not an error."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)

    result = HeadProvisioner(spec, runner=gcloud).status()

    assert result.exists is False
    assert result.address is None
    assert result.service_state is None
    assert gcloud.issued_matching("compute", "ssh") == []


def test_status_refuses_to_describe_a_same_named_instance_it_does_not_own() -> None:
    """Reporting someone else's VM as the fleet's control plane is worse than
    reporting nothing."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec, instance={"name": spec.instance_name, "status": "RUNNING", "labels": {}})

    with pytest.raises(HeadOwnershipError):
        HeadProvisioner(spec, runner=gcloud).status()


def test_down_without_confirmation_deletes_nothing() -> None:
    """The head holds the control plane's durable state on its own disk."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec, instance=_owned_instance(spec))

    with pytest.raises(head_commands.HeadError, match="pass --yes to confirm"):
        HeadProvisioner(spec, runner=gcloud).down(confirmed=False)

    assert gcloud.issued == []
    assert gcloud.instance is not None


def test_down_deletes_the_head_and_the_rules_it_owns() -> None:
    """Teardown that leaves the firewall rules behind is a name collision next time."""

    spec = _spec()
    gcloud = FakeGcloud(
        spec=spec,
        instance=_owned_instance(spec),
        firewall={
            spec.firewall_rule_name: _owned_firewall(spec, spec.firewall_rule_name, ranges=("10.128.0.0/20",)),
            spec.ssh_firewall_rule_name: _owned_firewall(
                spec, spec.ssh_firewall_rule_name, ranges=(head_commands.IAP_TCP_FORWARDING_RANGE,)
            ),
        },
    )

    result = HeadProvisioner(spec, runner=gcloud).down(confirmed=True)

    assert result.deleted_instance is True
    assert result.deleted_firewall_rules == (spec.firewall_rule_name, spec.ssh_firewall_rule_name)
    assert gcloud.instance is None
    assert gcloud.firewall == {}


def test_down_refuses_to_delete_an_instance_eyvan_does_not_own() -> None:
    """A destructive command must fail closed on an ownership question."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec, instance={"name": spec.instance_name, "labels": {"owner": "platform"}})

    with pytest.raises(HeadOwnershipError):
        HeadProvisioner(spec, runner=gcloud).down(confirmed=True)

    assert gcloud.issued_matching("compute", "instances", "delete") == []


def test_the_down_command_requires_the_flag_and_not_merely_a_terminal(monkeypatch) -> None:
    """An explicit flag, so a confirmation cannot be produced by a tty."""

    def refuse(args, *, stdin_text, timeout):
        raise AssertionError("unconfirmed teardown must not reach gcloud")

    monkeypatch.setattr(head_commands, "_run_gcloud", refuse)

    invoked = CliRunner().invoke(
        head_commands.head_group,
        [
            "down",
            "--fleet",
            "research",
            "--project",
            "example-project",
            "--region",
            "us-east5",
            "--zone",
            "us-east5-a",
        ],
    )

    assert invoked.exit_code != 0
    assert "pass --yes to confirm" in invoked.output


def test_the_up_command_refuses_to_provision_without_confirmation_outside_a_terminal(monkeypatch) -> None:
    """Non-interactive automation must state its intent to spend money."""

    def refuse(args, *, stdin_text, timeout):
        raise AssertionError("unconfirmed provisioning must not reach gcloud")

    monkeypatch.setattr(head_commands, "_run_gcloud", refuse)
    monkeypatch.setattr(head_commands, "is_interactive", lambda: False)

    invoked = CliRunner().invoke(
        head_commands.head_group,
        [
            "up",
            "--fleet",
            "research",
            "--project",
            "example-project",
            "--region",
            "us-east5",
            "--zone",
            "us-east5-a",
            "--egress",
            "cloud-nat",
        ],
    )

    assert invoked.exit_code != 0
    assert "requires --yes" in invoked.output


def test_the_up_command_reports_the_address_the_ingress_and_the_next_command(monkeypatch) -> None:
    """The last line of a successful install should be the next command, typed."""

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)
    monkeypatch.setattr(head_commands, "_run_gcloud", gcloud)
    monkeypatch.setattr(head_commands.HeadSecrets, "generate", staticmethod(lambda: STUB_SECRETS))

    invoked = CliRunner().invoke(
        head_commands.head_group,
        [
            "up",
            "--fleet",
            "research",
            "--project",
            "example-project",
            "--region",
            "us-east5",
            "--zone",
            "us-east5-a",
            "--egress",
            "private-google-access",
            "--yes",
        ],
    )

    assert invoked.exit_code == 0, invoked.output
    assert "http://10.128.0.7:8080" in invoked.output
    assert "restricted to 10.128.0.0/20" in invoked.output
    assert "Next: eyvan --url http://10.128.0.7:8080 up <fleet.yaml>" in invoked.output
    assert not any(value in invoked.output for value in STUB_SECRETS.values())


def test_the_head_commands_are_reachable_from_the_root_cli(monkeypatch, tmp_path: Path) -> None:
    """A command nobody can type is a command that does not exist."""

    monkeypatch.setenv("EYVAN_CONFIG_FILE", str(tmp_path / "contexts.json"))

    invoked = CliRunner().invoke(cli.main, ["head", "up", "--help"])

    assert invoked.exit_code == 0, invoked.output
    assert "--dry-run" in invoked.output
    assert "UNQUALIFIED AGAINST REAL GCP" in invoked.output
    for mode in head_commands.BOOTSTRAP_EGRESS_MODES:
        assert mode in invoked.output


def test_a_head_name_that_cannot_exist_is_refused_before_any_command() -> None:
    """GCP's limits are the spec's limits, checked where they can be explained."""

    with pytest.raises(ValueError, match="too long"):
        _spec(fleet="a" * 60)
    with pytest.raises(ValueError, match="not in region"):
        _spec(zone="us-central1-a")
    with pytest.raises(ValueError, match="port"):
        _spec(port=0)


def test_explicit_flags_override_the_fleet_file_without_colliding(tmp_path: Path) -> None:
    """Overrides and file values meet in one constructor call.

    The first version of that merge passed ``region`` twice and raised a
    ``TypeError`` for every operator who overrode it.
    """

    config = tmp_path / "fleet.yaml"
    config.write_text(FLEET_YAML, encoding="utf-8")

    spec = HeadSpec.from_fleet_config(
        EyvanFleetConfig.from_yaml(config),
        region="us-central1",
        zone="us-central1-b",
        project="other-project-id",
        machine_type="e2-standard-2",
        port=9443,
        egress="cloud-nat",
    )

    assert (spec.region, spec.zone, spec.project) == ("us-central1", "us-central1-b", "other-project-id")
    create = next(action for action in HeadProvisioner(spec).plan().actions if action.step is HeadStep.INSTANCE_CREATE)
    assert create.args[create.args.index("--machine-type") + 1] == "e2-standard-2"
    assert create.args[create.args.index("--zone") + 1] == "us-central1-b"
    ingress = next(action for action in HeadProvisioner(spec).plan().actions if action.step is HeadStep.FIREWALL_CREATE)
    assert "--allow=tcp:9443" in ingress.args


def test_status_and_down_do_not_demand_an_egress_mode_to_read_or_delete_a_head(monkeypatch, tmp_path: Path) -> None:
    """Recovery commands must not require a creation-time decision.

    A fleet whose pools disagree about egress can still be inspected and torn
    down; only ``up`` has to know how the head reaches the network.
    """

    monkeypatch.setenv("EYVAN_CONFIG_FILE", str(tmp_path / "contexts.json"))
    where = ["--fleet", "research", "--project", "example-project", "--region", "us-east5", "--zone", "us-east5-a"]

    status = CliRunner().invoke(head_commands.head_group, ["status", *where, "--json"])
    teardown = CliRunner().invoke(head_commands.head_group, ["down", *where, "--dry-run", "--json"])

    assert status.exit_code != 0
    assert "tests must not run gcloud" in str(status.exception)
    assert teardown.exit_code == 0, teardown.output
    assert json.loads(teardown.output)["would_delete"] == [
        "research-head",
        "research-head-api",
        "research-head-iap-ssh",
    ]


def test_the_install_page_states_the_command_its_scope_and_that_it_is_unqualified() -> None:
    """The doc gap is half the failure being fixed here.

    The egress value that stopped the live attempt was rejected by a message
    naming no legal set, and the set appeared in no page. It appears in this one
    now, and the page must keep saying the command is unproven against GCP.
    """

    page = (Path(__file__).resolve().parent.parent / "docs" / "install.md").read_text(encoding="utf-8")

    assert "eyvan head up" in page
    assert "has not been run against real GCP" in page
    for mode in head_commands.BOOTSTRAP_EGRESS_MODES:
        assert mode in page
    assert "0.0.0.0/0" in page
    assert "--rotate-secrets" in page


def test_a_new_head_is_waited_for_rather_than_failed_on_because_sshd_is_young() -> None:
    """`instances create` returns before sshd listens.

    Without a bounded wait the very first run of this command fails on every
    project, for a reason that resolves itself in under a minute.
    """

    spec = _spec()
    gcloud = FakeGcloud(spec=spec)
    refusals = {"count": 3}
    slept: list[float] = []

    original = gcloud._ssh

    def flaky(call: Call) -> subprocess.CompletedProcess[str]:
        if "eyvan-head-env-present" in (call.remote_command or "") and refusals["count"]:
            refusals["count"] -= 1
            return _failed("ssh: connect to host 10.128.0.7 port 22: Connection refused", returncode=255)
        return original(call)

    gcloud._ssh = flaky  # type: ignore[method-assign]

    result = HeadProvisioner(
        spec,
        runner=gcloud,
        secret_factory=lambda: STUB_SECRETS,
        ssh_ready_attempts=5,
        ssh_ready_delay_s=2.5,
        sleep=slept.append,
    ).up()

    assert refusals["count"] == 0
    assert slept == [2.5, 2.5, 2.5]
    assert result.service_state == "active"
    assert result.wrote_environment is True


def test_the_wait_for_ssh_is_bounded_and_its_failure_writes_nothing() -> None:
    """A head that never comes up must not be waited on forever, or half-configured."""

    spec = _spec()
    gcloud = FakeGcloud(
        spec=spec,
        failures={"eyvan-head-env-present": _failed("ssh: connect to host: Connection refused", returncode=255)},
    )
    slept: list[float] = []

    with pytest.raises(HeadCommandError, match="after 4 attempts") as failure:
        HeadProvisioner(
            spec,
            runner=gcloud,
            secret_factory=lambda: STUB_SECRETS,
            ssh_ready_attempts=4,
            ssh_ready_delay_s=1.0,
            sleep=slept.append,
        ).up()

    assert failure.value.step is HeadStep.SECRET_LOOKUP
    assert slept == [1.0, 1.0, 1.0]
    assert [call for call in gcloud.calls if call.stdin_text is not None] == []


def test_a_unit_that_did_not_start_is_never_reported_as_a_running_head() -> None:
    """`is-active` prints the state and exits non-zero for every state but active.

    Reporting an address for a control plane that is not listening is the one
    output that would send an operator to debug the network instead of the unit.
    """

    spec = _spec()
    gcloud = FakeGcloud(spec=spec, failures={"is-active": _ok_with_status("failed\n", returncode=3)})

    with pytest.raises(HeadCommandError, match="is failed, not active") as failure:
        HeadProvisioner(spec, runner=gcloud, secret_factory=lambda: STUB_SECRETS).up()

    assert failure.value.step is HeadStep.SERVICE_VERIFY
    assert "journalctl -u eyvan-head.service" in failure.value.detail
