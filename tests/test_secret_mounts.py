# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Passing a credential to a workload without persisting it in the spec.

The documented advice is that secrets are never persisted in specs. The only
mechanism for passing one was ``WorkloadDefinition.environment`` -- a persisted
``dict[str, str]`` validated for identifier names and NUL bytes -- so the advice
was contradicted by the only available means of following it.

Two pieces here. A reference side: values live in a store the operator already
runs, and the spec carries a reference resolved at dispatch by something holding
the agent's identity. And the gate that makes the reference safe.

**The gate is the security-critical piece.** Without it, any submitter in a
project can name any secret the *agent's* identity can read -- and that identity
is broad by construction, because it provisions capacity. It turns "may submit
jobs" into "may read every secret in the project", available to anyone who can
write a spec.
"""

from __future__ import annotations

import subprocess

import pytest

from eyvan.records import (
    CommandExecution,
    PolicyRecord,
    Principal,
    Role,
    RoleGrant,
    WorkloadDefinition,
)
from eyvan.secrets import (
    FileSecretStore,
    GcpSecretManagerStore,
    SecretUnavailable,
    read_secret,
    secret_store_from_reference,
)
from eyvan.server.persistence.memory import InMemoryRepository
from eyvan.server.service.control_plane import ControlPlaneService
from eyvan.server.service.errors import AuthorizationError
from eyvan.server.service.scheduling import CoreSchedulingService

OPERATOR = Principal(
    subject="alice",
    grants=(RoleGrant(role=Role.PROJECT_OPERATOR, org_id="acme", project_id="research"),),
)
ADMIN = Principal(subject="root", grants=(RoleGrant(role=Role.FLEET_ADMIN),))


def _service() -> ControlPlaneService:
    return ControlPlaneService(InMemoryRepository(), scheduler=CoreSchedulingService())


def _spec(**mounts: str) -> WorkloadDefinition:
    return WorkloadDefinition(
        name="train",
        execution=CommandExecution(command="python train.py"),
        secret_mounts=dict(mounts),
    )


async def _policy(service: ControlPlaneService, *prefixes: str, project: str = "research") -> None:
    await service.put_policy(
        ADMIN,
        PolicyRecord(
            org_id="acme",
            project_id=project,
            allowed_secret_references=prefixes,
            updated_by="root",
        ),
        idempotency_key=f"policy-{project}",
    )


# --------------------------------------------------------------------------- the gate


async def test_an_unlisted_secret_reference_is_refused_at_submission() -> None:
    """The escalation this exists to prevent.

    The agent's identity can read every secret in the project because it
    provisions capacity. Without this check, naming one in a spec is how a
    submitter borrows that identity.
    """

    service = _service()
    await _policy(service, "gcp-secret://my-project/training-")

    with pytest.raises(AuthorizationError, match="not permitted in this project"):
        await service.submit_workload(
            OPERATOR,
            org_id="acme",
            project_id="research",
            spec=_spec(DB_PASSWORD="gcp-secret://my-project/production-database"),
            idempotency_key="w1",
        )


async def test_a_listed_prefix_is_admitted() -> None:
    """The gate must not block the configuration it exists to permit."""

    service = _service()
    await _policy(service, "gcp-secret://my-project/training-")

    workload, _ = await service.submit_workload(
        OPERATOR,
        org_id="acme",
        project_id="research",
        spec=_spec(HF_TOKEN="gcp-secret://my-project/training-hf-token"),
        idempotency_key="w1",
    )

    assert workload.spec.secret_mounts == {"HF_TOKEN": "gcp-secret://my-project/training-hf-token"}


async def test_a_project_with_no_policy_may_mount_nothing() -> None:
    """The empty allow-list is "none", not "any".

    A permissive default would mean every project that never configured a policy
    silently grants every submitter the agent's read access.
    """

    service = _service()

    with pytest.raises(AuthorizationError):
        await service.submit_workload(
            OPERATOR,
            org_id="acme",
            project_id="research",
            spec=_spec(TOKEN="gcp-secret://my-project/anything"),
            idempotency_key="w1",
        )


async def test_a_workload_with_no_mounts_is_unaffected() -> None:
    """Every existing workload must keep submitting."""

    service = _service()

    workload, _ = await service.submit_workload(
        OPERATOR,
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(name="train", execution=CommandExecution(command="python train.py")),
        idempotency_key="w1",
    )

    assert workload.spec.secret_mounts == {}


async def test_every_mount_is_checked_not_just_the_first() -> None:
    """A spec that mixes a permitted reference with a forbidden one is forbidden.

    The permitted name sorts *before* the forbidden one deliberately: with the
    forbidden mount first, an implementation that inspects only one entry still
    refuses, and the test would not distinguish the two. Checking one would make
    the escalation available to anyone who also names something legitimate.
    """

    service = _service()
    await _policy(service, "gcp-secret://my-project/training-")

    with pytest.raises(AuthorizationError, match="production-database"):
        await service.submit_workload(
            OPERATOR,
            org_id="acme",
            project_id="research",
            spec=_spec(
                HF_TOKEN="gcp-secret://my-project/training-hf-token",
                WANDB_KEY="gcp-secret://my-project/production-database",
            ),
            idempotency_key="w1",
        )


async def test_a_prefix_does_not_leak_across_projects() -> None:
    """One project's allow-list must not admit another's submissions.

    Both projects have a policy, so this distinguishes "read *this* project's
    allow-list" from "read *an* allow-list" -- which a project with no policy at
    all cannot.
    """

    service = _service()
    await _policy(service, "gcp-secret://my-project/training-")
    await _policy(service, "gcp-secret://my-project/platform-", project="platform")

    other = Principal(
        subject="mallory",
        grants=(RoleGrant(role=Role.PROJECT_OPERATOR, org_id="acme", project_id="platform"),),
    )

    with pytest.raises(AuthorizationError):
        await service.submit_workload(
            other,
            org_id="acme",
            project_id="platform",
            spec=_spec(HF_TOKEN="gcp-secret://my-project/training-hf-token"),
            idempotency_key="w1",
        )


# --------------------------------------------------------------------------- the store


def test_a_scheme_selects_its_backend() -> None:
    """Adding a backend is adding a scheme, not editing a dispatch table."""

    assert isinstance(secret_store_from_reference("gcp-secret://p/name"), GcpSecretManagerStore)
    assert isinstance(secret_store_from_reference("file:///run/secrets/token"), FileSecretStore)


def test_an_unknown_scheme_is_refused_rather_than_read_as_a_path() -> None:
    """A typo in ``gcp-secret`` would otherwise become a missing-file error.

    That error names the wrong problem, and an operator would go looking for a
    file nobody meant to create.
    """

    with pytest.raises(SecretUnavailable, match="unknown secret scheme"):
        secret_store_from_reference("gcpsecret://p/name")


def test_a_file_secret_is_read(tmp_path) -> None:
    """The local-development and projected-volume case share one reader."""

    path = tmp_path / "token"
    path.write_bytes(b"s3cret")

    assert read_secret(f"file://{path}") == b"s3cret"


def test_an_unreadable_file_names_the_reference(tmp_path) -> None:
    """A resolution failure with no reference in it is a support ticket."""

    with pytest.raises(SecretUnavailable, match="could not read"):
        read_secret(f"file://{tmp_path / 'absent'}")


def test_a_gcp_reference_is_resolved_through_gcloud() -> None:
    """The agent already authenticates gcloud.

    A second credential path would be a second thing to get wrong on the host
    whose identity is the entire security boundary.
    """

    seen: list[tuple[str, ...]] = []

    def runner(command):
        seen.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, "value", "")

    assert GcpSecretManagerStore(runner=runner).read("gcp-secret://my-project/hf-token") == b"value"
    joined = " ".join(seen[0])
    assert "--secret=hf-token" in joined
    assert "--project=my-project" in joined
    assert "access latest" in joined


def test_a_pinned_version_is_honored() -> None:
    """Pinning is how a rotation is rolled out deliberately rather than at once."""

    seen: list[tuple[str, ...]] = []

    def runner(command):
        seen.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, "value", "")

    GcpSecretManagerStore(runner=runner).read("gcp-secret://my-project/hf-token/7")

    assert "access 7" in " ".join(seen[0])


def test_a_denied_read_surfaces_the_reason() -> None:
    """The useful half is almost always "permission denied for service account X".

    That names the exact grant an operator has to add, so it is kept.
    """

    def runner(command):
        return subprocess.CompletedProcess(command, 1, "", "PERMISSION_DENIED: caller lacks secretAccessor")

    with pytest.raises(SecretUnavailable, match="secretAccessor"):
        GcpSecretManagerStore(runner=runner).read("gcp-secret://my-project/hf-token")


def test_a_malformed_gcp_reference_is_refused() -> None:
    """A reference missing its project would resolve against the agent's default.

    That is a different secret in a different project, read successfully, with
    nothing to indicate the mistake.
    """

    with pytest.raises(SecretUnavailable, match="not a gcp-secret"):
        GcpSecretManagerStore(runner=lambda command: subprocess.CompletedProcess(command, 0, "", "")).read(
            "gcp-secret:///name"
        )


# --------------------------------------------------------------------------- the surfaces


def _cli(monkeypatch, *args):
    """Run one CLI command against the recording client the other CLI tests use."""

    from click.testing import CliRunner
    from test_cli import FakeClient, _json_argument

    from eyvan.sdk import cli

    fake = FakeClient()
    monkeypatch.setattr(cli, "EyvanClient", lambda *_args, **_kwargs: fake)
    result = CliRunner().invoke(
        cli.main,
        ["--user", "alice", "--org", "acme", "--project", "research", *args],
    )
    payloads = [_json_argument(call) for call in fake.calls]
    return result, payloads


def test_a_submitter_can_declare_a_mount_from_the_cli(monkeypatch) -> None:
    """A spec field with no way to set it is a feature nobody has."""

    result, payloads = _cli(
        monkeypatch,
        "workload",
        "submit",
        "--name",
        "train",
        "--command",
        "python train.py",
        "--secret",
        "HF_TOKEN=gcp-secret://my-project/training-hf-token",
    )

    assert result.exit_code == 0, result.output
    assert payloads[-1]["spec"]["secret_mounts"] == {"HF_TOKEN": "gcp-secret://my-project/training-hf-token"}


def test_a_malformed_secret_option_names_its_own_flag(monkeypatch) -> None:
    """An error naming --env sends a user to the wrong line of their command."""

    result, _payloads = _cli(monkeypatch, "workload", "submit", "--name", "t", "--command", "c", "--secret", "HF_TOKEN")

    assert result.exit_code != 0
    assert "--secret" in result.output


def test_an_operator_can_open_the_allow_list_from_the_cli(monkeypatch) -> None:
    """The gate is unusable if the only way to configure it is a database write."""

    from test_governance_cli import FakeClient, _invoke, _json_argument

    fake = FakeClient()

    result = _invoke(
        fake, monkeypatch, ["policy", "set", "--allow-secret-reference", "gcp-secret://my-project/training-"]
    )

    assert result.exit_code == 0, result.output
    assert _json_argument(fake.calls[0])["allowed_secret_references"] == ["gcp-secret://my-project/training-"]


def test_an_omitted_secret_allow_list_forbids_rather_than_permits(monkeypatch) -> None:
    """The inversion, asserted rather than only documented.

    Its neighbours -- regions, providers, capacity classes -- all mean "no
    restriction" when empty, and one of them has a test saying so. This one has
    to mean the opposite, and the two conventions living in one command is
    exactly the kind of thing that gets "fixed" into a security hole.
    """

    from test_governance_cli import FakeClient, _invoke, _json_argument

    fake = FakeClient()

    assert _invoke(fake, monkeypatch, ["policy", "set"]).exit_code == 0
    payload = _json_argument(fake.calls[0])

    assert payload["allowed_secret_references"] == []
    assert payload["allowed_regions"] == []


def test_the_inverted_default_is_documented_where_an_operator_reads_it() -> None:
    """Every other allow-list here means "no restriction" when empty; this one does not.

    An operator who assumes the usual convention would read a populated list as
    narrowing an otherwise-open dimension, when it is in fact the only thing
    opening a closed one.
    """

    from click.testing import CliRunner

    from eyvan.sdk import cli

    output = CliRunner().invoke(cli.main, ["policy", "set", "--help"]).output

    assert "--allow-secret-reference" in output
    assert "NOTHING" in output
