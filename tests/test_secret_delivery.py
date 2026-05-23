# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Getting a secret value to a running attempt without persisting it anywhere.

Three pieces have to line up, and each of them is a place where this could be
built, tested, and still never happen to a real workload:

* dispatch mints one capability per mount and injects the address to redeem it
  at -- if it does not, the route is reachable by nothing;
* the route resolves a mount only for the workload's **current** attempt -- if it
  does not, the credential is a bearer token with a TTL and nothing more;
* the cell agent redeems the capabilities and writes the values into the
  owner-only environment file it already writes -- if it does not, the job sees
  a URL and a token instead of its secret.

Every test here asserts one of those seams rather than the pieces in isolation.
"""

from __future__ import annotations

import base64
import json
import time

import pytest

from eyvan.artifacts._capability_tokens import InvalidCapabilityToken, sign_claims
from eyvan.artifacts.secret_capability import (
    SECRET_TOKEN_VERSION,
    InvalidSecretToken,
    SecretCapabilitySigner,
)
from eyvan.artifacts.workload_capability import WorkloadCapabilitySigner
from eyvan.worker.runtime.secret_materialization import (
    SecretMaterializationError,
    materialize_secrets,
    secret_environment_keys,
)

SECRET = b"k" * 32


def _signer(*, ttl_seconds: float = 3600, clock=time.time) -> SecretCapabilitySigner:
    return SecretCapabilitySigner(SECRET, ttl_seconds=ttl_seconds, clock=clock)


def _issue(signer: SecretCapabilitySigner, *, attempt: int = 1, name: str = "HF_TOKEN") -> str:
    return signer.issue(
        workload_id="wl-1",
        org_id="acme",
        project_id="research",
        attempt=attempt,
        name=name,
    )


# --------------------------------------------------------------------------- the token


def test_a_secret_token_cannot_verify_as_a_submit_capability() -> None:
    """Audience separation, and the reason the prefix is inside the signed material.

    Same deployment key, so a claim check is the only thing between the two --
    and a claim check is exactly what should not be load-bearing here.
    """

    token = _issue(_signer())

    # The base class, not the secret-specific one: the prefix is checked in the
    # shared primitive, before any signer's claim rules run. That is the point --
    # the separation does not depend on either signer being careful.
    with pytest.raises(InvalidCapabilityToken, match="invalid capability token"):
        WorkloadCapabilitySigner(SECRET).verify(token, workload_id="wl-1", generation=1)


def test_a_submit_capability_cannot_verify_as_a_secret_token() -> None:
    """The same separation in the direction that would leak a value."""

    token = WorkloadCapabilitySigner(SECRET).issue(
        workload_id="wl-1", org_id="acme", project_id="research", generation=1
    )

    with pytest.raises(InvalidCapabilityToken, match="invalid capability token"):
        _signer().verify(token, workload_id="wl-1", org_id="acme", project_id="research")


def test_a_secret_token_expires() -> None:
    """The one capability that must expire, because a read cannot be undone.

    A submit capability is safe unexpiring: the control plane re-authorizes the
    action on every use. A secret capability hands back a value, and no later
    revocation un-reads it.
    """

    now = [1_000.0]
    signer = _signer(ttl_seconds=60, clock=lambda: now[0])
    token = _issue(signer)

    signer.verify(token, workload_id="wl-1", org_id="acme", project_id="research")
    now[0] += 61

    with pytest.raises(InvalidSecretToken, match="expired"):
        signer.verify(token, workload_id="wl-1", org_id="acme", project_id="research")


def test_a_token_for_another_project_is_refused() -> None:
    """Scope is checked against the request, not merely carried in the claims."""

    token = _issue(_signer())

    with pytest.raises(InvalidSecretToken, match="different project"):
        _signer().verify(token, workload_id="wl-1", org_id="acme", project_id="platform")


def test_an_added_claim_is_refused_rather_than_ignored() -> None:
    """Honoring the claims a build recognizes is how a capability quietly widens."""

    signer = _signer()
    payload = _issue(signer).split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    claims["scope"] = "everything"
    # Signed with the real key, so the signature check passes and the claim-set
    # check is what has to refuse it. An unsigned forgery would prove only that
    # HMAC works.
    forged = sign_claims(SECRET, SECRET_TOKEN_VERSION, claims)

    with pytest.raises(InvalidSecretToken, match="unexpected claims"):
        signer.verify(forged, workload_id="wl-1", org_id="acme", project_id="research")


def test_the_token_names_a_mount_and_not_a_reference() -> None:
    """A reference in the token would outlive the spec that authorized it.

    An operator who corrected a mount would find the old reference still
    readable until the capability expired.
    """

    token = _issue(_signer())
    payload = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))

    assert claims["name"] == "HF_TOKEN"
    assert not any("gcp-secret" in str(value) for value in claims.values())
    assert token.startswith(f"{SECRET_TOKEN_VERSION}.")


# --------------------------------------------------------------------------- dispatch


async def test_a_workload_with_mounts_receives_capabilities_at_dispatch(tmp_path) -> None:
    """The seam that makes the route reachable from a real cell.

    Without it the capability is mintable, the route accepts it, and nothing
    running has ever been handed one.
    """

    environment, workload, signer = await _dispatch_with_mounts(tmp_path)

    assert environment["EYVAN_SECRET_COUNT"] == "1"
    assert environment["EYVAN_SECRET_0_NAME"] == "HF_TOKEN"
    assert environment["EYVAN_SECRET_0_URL"].startswith("https://eyvan.test/api/v1/secrets/content?")
    assert f"workload_id={workload.id}" in environment["EYVAN_SECRET_0_URL"]
    claims = signer.verify(
        environment["EYVAN_SECRET_0_TOKEN"],
        workload_id=workload.id,
        org_id="acme",
        project_id="research",
    )
    assert claims.attempt == workload.attempt


async def test_the_capability_carries_the_attempt_being_dispatched(tmp_path) -> None:
    """Not attempt 1, and not a constant.

    Written with an attempt that is *not* the default on purpose: a test whose
    fixture uses the default value of the field under test cannot tell "read the
    field" from "hardcode the default", and would pass against a capability that
    was permanently pinned to the first attempt.
    """

    environment, workload, signer = await _dispatch_with_mounts(tmp_path, attempt=4)

    assert workload.attempt == 4
    claims = signer.verify(
        environment["EYVAN_SECRET_0_TOKEN"],
        workload_id=workload.id,
        org_id="acme",
        project_id="research",
    )
    assert claims.attempt == 4


async def test_no_secret_value_reaches_the_dispatch(tmp_path) -> None:
    """The property the whole design exists for.

    The agent request is persisted in an outbox and echoed into logs; a value in
    it would be a value in both.
    """

    environment, _workload, _signer = await _dispatch_with_mounts(tmp_path)

    assert not any("hf_abcdef" in value for value in environment.values())
    assert "gcp-secret://my-project/training-hf-token" not in json.dumps(environment)


async def test_a_workload_without_mounts_receives_no_scaffolding(tmp_path) -> None:
    """Every existing workload's dispatch environment stays as it was."""

    environment, _workload, _signer = await _dispatch_with_mounts(tmp_path, mounts={})

    assert not [key for key in environment if key.startswith("EYVAN_SECRET_")]


async def test_dispatch_records_which_mounts_were_granted(tmp_path) -> None:
    """One issuance event per attempt, in the dispatch transaction.

    It is the pairing that carries the signal: a ``secret.read`` for an attempt
    with no matching ``secret.issued`` is a capability being used outside the
    dispatch that minted it.
    """

    _environment, workload, _signer = await _dispatch_with_mounts(tmp_path)
    events = await _events(workload.id)
    issued = [event for event in events if event.action == "secret.issued"]

    assert len(issued) == 1
    assert issued[0].payload["names"] == ["HF_TOKEN"]
    assert issued[0].payload["attempt"] == workload.attempt


# --------------------------------------------------------------------------- the cell


def test_the_agent_resolves_declared_mounts_into_the_launch_environment() -> None:
    """What the job actually ends up seeing."""

    resolved = materialize_secrets(
        {
            "EYVAN_SECRET_COUNT": "2",
            "EYVAN_SECRET_0_NAME": "HF_TOKEN",
            "EYVAN_SECRET_0_URL": "https://eyvan.test/api/v1/secrets/content?name=HF_TOKEN",
            "EYVAN_SECRET_0_TOKEN": "eyvs1.a.b",
            "EYVAN_SECRET_1_NAME": "WANDB_KEY",
            "EYVAN_SECRET_1_URL": "https://eyvan.test/api/v1/secrets/content?name=WANDB_KEY",
            "EYVAN_SECRET_1_TOKEN": "eyvs1.c.d",
        },
        fetch=lambda url, _token: b"value-for-" + url.rsplit("=", 1)[1].encode(),
    )

    assert resolved == {"HF_TOKEN": "value-for-HF_TOKEN", "WANDB_KEY": "value-for-WANDB_KEY"}


def test_the_capability_does_not_survive_into_the_job() -> None:
    """A job receives the value it asked for, not the means to ask again.

    Otherwise a leaked process environment hands over a working credential for
    the rest of the attempt rather than one secret.
    """

    environment = {
        "EYVAN_SECRET_COUNT": "1",
        "EYVAN_SECRET_0_NAME": "HF_TOKEN",
        "EYVAN_SECRET_0_URL": "https://eyvan.test/api/v1/secrets/content",
        "EYVAN_SECRET_0_TOKEN": "eyvs1.a.b",
        "EYVAN_WORKLOAD_ID": "wl-1",
    }

    dropped = secret_environment_keys(environment)

    assert set(dropped) == {
        "EYVAN_SECRET_COUNT",
        "EYVAN_SECRET_0_NAME",
        "EYVAN_SECRET_0_URL",
        "EYVAN_SECRET_0_TOKEN",
    }
    assert "EYVAN_WORKLOAD_ID" not in dropped


def test_an_unresolvable_mount_refuses_the_launch() -> None:
    """A job that starts without the credential it declared fails further away.

    Usually as an authentication error against someone else's service, which is
    a much longer path to the actual cause.
    """

    def fetch(_url, _token):
        raise RuntimeError("403")

    with pytest.raises(SecretMaterializationError, match="HF_TOKEN"):
        materialize_secrets(
            {
                "EYVAN_SECRET_COUNT": "1",
                "EYVAN_SECRET_0_NAME": "HF_TOKEN",
                "EYVAN_SECRET_0_URL": "https://eyvan.test/x",
                "EYVAN_SECRET_0_TOKEN": "eyvs1.a.b",
            },
            fetch=fetch,
        )


def test_a_launch_with_no_mounts_fetches_nothing() -> None:
    """Every existing launch keeps its behaviour, including making no requests."""

    def fetch(_url, _token):
        raise AssertionError("a launch with no declared mounts must not call the control plane")

    assert materialize_secrets({"EYVAN_WORKLOAD_ID": "wl-1"}, fetch=fetch) == {}


def test_the_systemd_launcher_writes_resolved_values_and_not_capabilities(tmp_path, monkeypatch) -> None:
    """The wiring, not the helper.

    ``materialize_secrets`` passing its own tests proves nothing about whether a
    launch calls it -- which is the shape half of this package's defects have
    taken.
    """

    from eyvan.worker.runtime import systemd_transient

    monkeypatch.setattr(
        systemd_transient,
        "materialize_secrets",
        lambda environment: {"HF_TOKEN": "hf_resolved"} if environment.get("EYVAN_SECRET_COUNT") else {},
    )
    written: dict[str, str] = {}
    monkeypatch.setattr(
        systemd_transient,
        "_write_environment_file",
        lambda _path, environment: written.update(environment),
    )
    executor = systemd_transient.SystemdTransientServiceExecutor(
        tmp_path,
        command_runner=lambda *_args, **_kwargs: None,
        boot_id_reader=lambda: "boot-a",
    )

    executor._prepare_launch(
        {"remote_id": "eyvan-wl-1"},
        {
            "environment": {
                "EYVAN_SECRET_COUNT": "1",
                "EYVAN_SECRET_0_NAME": "HF_TOKEN",
                "EYVAN_SECRET_0_URL": "https://eyvan.test/x",
                "EYVAN_SECRET_0_TOKEN": "eyvs1.a.b",
                "EYVAN_WORKLOAD_ID": "wl-1",
            }
        },
    )

    assert written["HF_TOKEN"] == "hf_resolved"
    assert not [key for key in written if key.startswith("EYVAN_SECRET_")]
    assert written["EYVAN_WORKLOAD_ID"] == "wl-1"


# --------------------------------------------------------------------------- helpers

_LAST_REPOSITORY: list[object] = []


async def _dispatch_with_mounts(tmp_path, mounts: dict[str, str] | None = None, *, attempt: int = 1):
    """Dispatch one workload through the real controller and return its environment."""

    from test_controller import FakeAgent, principal

    from eyvan.artifacts.managed import ArtifactTokenSigner, LocalManagedArtifactStore
    from eyvan.artifacts.transfers import ManagedArtifactAccess
    from eyvan.config import EyvanSettings
    from eyvan.records import (
        CellRecord,
        CellState,
        CommandExecution,
        PolicyRecord,
        ResourceVector,
        Role,
        WorkloadDefinition,
    )
    from eyvan.server.controller import EyvanController
    from eyvan.server.persistence.memory import InMemoryRepository
    from eyvan.server.service.control_plane import ControlPlaneService
    from eyvan.server.service.scheduling import CoreSchedulingService

    declared = {"HF_TOKEN": "gcp-secret://my-project/training-hf-token"} if mounts is None else mounts
    repository = InMemoryRepository()
    _LAST_REPOSITORY[:] = [repository]
    service = ControlPlaneService(repository, scheduler=CoreSchedulingService())
    signer = SecretCapabilitySigner(SECRET, ttl_seconds=3600)
    fake_agent = FakeAgent()
    controller = EyvanController(
        repository,
        service,
        settings=EyvanSettings(),
        agent_factory=lambda _cell: fake_agent,
        artifact_access=ManagedArtifactAccess(
            store=LocalManagedArtifactStore(tmp_path / "artifacts"),
            signer=ArtifactTokenSigner(SECRET, default_ttl_seconds=3600, max_ttl_seconds=3600),
            public_url="https://eyvan.test",
            token_ttl_seconds=3600,
        ),
        secret_capability=signer,
    )
    await service.put_policy(
        principal(Role.FLEET_ADMIN),
        PolicyRecord(
            org_id="acme",
            project_id="research",
            allowed_secret_references=("gcp-secret://my-project/training-",),
            updated_by="root",
        ),
        idempotency_key="policy",
    )
    await service.upsert_cell(
        principal(Role.FLEET_ADMIN),
        CellRecord(
            id="cell-a",
            provider="local",
            region="local",
            state=CellState.REGISTERING,
            capacity=ResourceVector(cpu=8, memory_gib=64),
            allocatable=ResourceVector(cpu=8, memory_gib=64),
            endpoint="http://agent:8090",
        ),
        idempotency_key="cell",
    )
    assert await controller.dispatch_outbox_once() == 1
    workload, _ = await service.submit_workload(
        principal(Role.PROJECT_OPERATOR),
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(
            name="trainer",
            execution=CommandExecution(command="python train.py"),
            secret_mounts=declared,
        ),
        idempotency_key="trainer",
    )
    if attempt != 1:
        async with repository.transaction() as tx:
            workload = workload.model_copy(update={"attempt": attempt})
            await tx.put_workload(workload)
    await controller.reconcile_once()
    assert await controller.dispatch_outbox_once() == 1
    async with repository.transaction() as tx:
        dispatched = await tx.get_workload(workload.id)
    assert dispatched is not None
    return fake_agent.submissions[0][1].spec["environment"], dispatched, signer


async def _events(workload_id: str):
    repository = _LAST_REPOSITORY[0]
    async with repository.transaction() as tx:  # type: ignore[attr-defined]
        return [event for event in await tx.list_events(limit=200) if event.entity_id == workload_id]


# --------------------------------------------------------------------------- the route


def _plane(tmp_path):
    from fastapi.testclient import TestClient

    from eyvan.config import EyvanSettings
    from eyvan.server.api.app import create_app

    app = create_app(
        settings=EyvanSettings(
            dev_auth_enabled=True,
            artifact_token_key="k" * 32,
            artifact_store_uri=str(tmp_path / "artifacts"),
        )
    )
    return TestClient(app), app


async def _live_workload(app, mounts: dict[str, str], prefixes: tuple[str, ...] = ()):
    """Submit a workload, allow its mounts, and put it in a state that can read."""

    from eyvan.records import (
        CommandExecution,
        PolicyRecord,
        Principal,
        Role,
        RoleGrant,
        WorkloadDefinition,
        WorkloadState,
    )

    service = app.state.service
    admin = Principal(subject="root", grants=(RoleGrant(role=Role.FLEET_ADMIN),))
    operator = Principal(
        subject="alice",
        grants=(RoleGrant(role=Role.PROJECT_OPERATOR, org_id="acme", project_id="research"),),
    )
    await service.put_policy(
        admin,
        PolicyRecord(
            org_id="acme",
            project_id="research",
            allowed_secret_references=prefixes,
            updated_by="root",
        ),
        idempotency_key="policy",
    )
    workload, _ = await service.submit_workload(
        operator,
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(
            name="trainer",
            execution=CommandExecution(command="python train.py"),
            secret_mounts=mounts,
        ),
        idempotency_key="trainer",
    )
    async with service.repository.transaction() as tx:
        running = workload.model_copy(update={"state": WorkloadState.RUNNING})
        await tx.put_workload(running)
    return running


def _read(client, app, workload, *, name="HF_TOKEN", token=None, attempt=None):
    capability = token or app.state.secret_capability.issue(
        workload_id=workload.id,
        org_id="acme",
        project_id="research",
        attempt=workload.attempt if attempt is None else attempt,
        name=name,
    )
    return client.get(
        "/api/v1/secrets/content",
        params={"org_id": "acme", "project_id": "research", "workload_id": workload.id, "name": name},
        headers={"X-Eyvan-Secret-Token": capability},
    )


def test_a_live_attempt_reads_its_declared_secret(tmp_path, monkeypatch) -> None:
    """The happy path, end to end over HTTP."""

    import anyio

    from eyvan.server.api import app as app_module

    client, app = _plane(tmp_path)
    workload = anyio.from_thread.BlockingPortal
    workload = anyio.run(_live_workload, app, {"HF_TOKEN": "file:///run/secrets/hf"}, ("file:///run/secrets/",))
    monkeypatch.setattr(app_module, "read_secret", lambda reference: b"hf_abcdef")

    response = _read(client, app, workload)

    assert response.status_code == 200, response.text
    assert response.content == b"hf_abcdef"
    assert response.headers["Cache-Control"] == "no-store"


def test_a_token_from_a_finished_attempt_is_refused(tmp_path, monkeypatch) -> None:
    """What makes the credential per-attempt rather than merely short-lived.

    A retry or a preemption resume bumps ``attempt``, and the capability minted
    for the previous one stops working immediately -- without a revocation list,
    and without waiting out the TTL.
    """

    import anyio

    from eyvan.server.api import app as app_module

    client, app = _plane(tmp_path)
    workload = anyio.run(_live_workload, app, {"HF_TOKEN": "file:///run/secrets/hf"}, ("file:///run/secrets/",))
    monkeypatch.setattr(app_module, "read_secret", lambda reference: b"hf_abcdef")

    stale = _read(client, app, workload, attempt=workload.attempt + 1)

    assert stale.status_code == 403, stale.text
    assert b"hf_abcdef" not in stale.content


def test_a_mount_the_policy_no_longer_allows_is_refused(tmp_path, monkeypatch) -> None:
    """Narrowing the allow-list after a leak has to be a real revocation.

    An allow-list checked only at submission would bar future specs while every
    already-running workload kept reading the secret that leaked.
    """

    import anyio

    from eyvan.server.api import app as app_module

    client, app = _plane(tmp_path)
    workload = anyio.run(_live_workload, app, {"HF_TOKEN": "file:///run/secrets/hf"}, ("file:///run/secrets/",))
    monkeypatch.setattr(app_module, "read_secret", lambda reference: b"hf_abcdef")
    assert _read(client, app, workload).status_code == 200

    async def narrow():
        from eyvan.records import PolicyRecord, Principal, Role, RoleGrant

        await app.state.service.put_policy(
            Principal(subject="root", grants=(RoleGrant(role=Role.FLEET_ADMIN),)),
            PolicyRecord(
                org_id="acme",
                project_id="research",
                allowed_secret_references=("file:///run/secrets/other-",),
                updated_by="root",
            ),
            idempotency_key="narrow",
        )

    anyio.run(narrow)

    assert _read(client, app, workload).status_code == 403


def test_a_capability_cannot_be_redirected_to_another_mount(tmp_path, monkeypatch) -> None:
    """A token names one mount, and the request names one mount.

    Without the comparison, a job holding a capability for its least sensitive
    mount could read its most sensitive one.
    """

    import anyio

    from eyvan.server.api import app as app_module

    client, app = _plane(tmp_path)
    workload = anyio.run(
        _live_workload,
        app,
        {"HF_TOKEN": "file:///run/secrets/hf", "DB_PASSWORD": "file:///run/secrets/db"},
        ("file:///run/secrets/",),
    )
    monkeypatch.setattr(app_module, "read_secret", lambda reference: b"value")
    capability = app.state.secret_capability.issue(
        workload_id=workload.id,
        org_id="acme",
        project_id="research",
        attempt=workload.attempt,
        name="HF_TOKEN",
    )

    response = client.get(
        "/api/v1/secrets/content",
        params={
            "org_id": "acme",
            "project_id": "research",
            "workload_id": workload.id,
            "name": "DB_PASSWORD",
        },
        headers={"X-Eyvan-Secret-Token": capability},
    )

    assert response.status_code == 403, response.text


def test_an_undeclared_mount_is_refused(tmp_path, monkeypatch) -> None:
    """A capability is not a licence to name anything.

    The mount has to still be in the spec, so removing one takes effect at once.
    """

    import anyio

    from eyvan.server.api import app as app_module

    client, app = _plane(tmp_path)
    workload = anyio.run(_live_workload, app, {"HF_TOKEN": "file:///run/secrets/hf"}, ("file:///run/secrets/",))
    monkeypatch.setattr(app_module, "read_secret", lambda reference: b"value")

    assert _read(client, app, workload, name="NOT_DECLARED").status_code == 403


def test_both_outcomes_of_a_secret_read_are_audited(tmp_path, monkeypatch) -> None:
    """One event per fetch, granted and refused, in the authorization transaction.

    A granted read that could be committed unaudited would make the ledger a
    record of what the control plane noticed rather than of what it did.
    """

    import anyio

    from eyvan.server.api import app as app_module

    client, app = _plane(tmp_path)
    workload = anyio.run(_live_workload, app, {"HF_TOKEN": "file:///run/secrets/hf"}, ("file:///run/secrets/",))
    monkeypatch.setattr(app_module, "read_secret", lambda reference: b"hf_abcdef")

    assert _read(client, app, workload).status_code == 200
    assert _read(client, app, workload, name="NOT_DECLARED").status_code == 403
    assert _read(client, app, workload, token="eyvs1.forged.signature").status_code == 403

    async def actions():
        async with app.state.service.repository.transaction() as tx:
            return [event.action for event in await tx.list_events(limit=200) if event.action.startswith("secret.")]

    recorded = anyio.run(actions)

    assert "secret.read" in recorded
    assert "secret.read_denied" in recorded
    assert "secret.token_rejected" in recorded


def test_no_secret_value_appears_in_the_audit_trail(tmp_path, monkeypatch) -> None:
    """The ledger is read by more people than the spec is.

    So it carries the mount *name*, never the reference and never the value.
    """

    import anyio

    from eyvan.server.api import app as app_module

    client, app = _plane(tmp_path)
    workload = anyio.run(_live_workload, app, {"HF_TOKEN": "file:///run/secrets/hf"}, ("file:///run/secrets/",))
    monkeypatch.setattr(app_module, "read_secret", lambda reference: b"hf_abcdef")
    assert _read(client, app, workload).status_code == 200

    async def payloads():
        async with app.state.service.repository.transaction() as tx:
            return json.dumps(
                [event.payload for event in await tx.list_events(limit=200) if event.action.startswith("secret.")]
            )

    recorded = anyio.run(payloads)

    assert "HF_TOKEN" in recorded
    assert "hf_abcdef" not in recorded
    assert "/run/secrets/hf" not in recorded


def test_an_unreachable_store_is_retryable_not_a_bad_request(tmp_path, monkeypatch) -> None:
    """A store that cannot be reached is an incident, not the caller's mistake.

    A 4xx would make an agent give up on a launch that a retry would complete.
    """

    import anyio

    from eyvan.secrets import SecretUnavailable
    from eyvan.server.api import app as app_module

    client, app = _plane(tmp_path)
    workload = anyio.run(_live_workload, app, {"HF_TOKEN": "file:///run/secrets/hf"}, ("file:///run/secrets/",))

    def unavailable(reference):
        raise SecretUnavailable(f"could not read {reference!r}: PERMISSION_DENIED")

    monkeypatch.setattr(app_module, "read_secret", unavailable)

    response = _read(client, app, workload)

    assert response.status_code == 503, response.text
    assert response.headers["Retry-After"] == "5"
    assert "PERMISSION_DENIED" in response.json()["detail"]


# --------------------------------------------------------------------------- the SDK


def test_a_callable_config_carries_its_mounts_to_the_submission(tmp_path) -> None:
    """``@execute`` is the SDK's primary interface.

    A ``secret_mounts`` field that only ``eyvan workload submit`` can set would
    leave every callable with ``environment`` -- the persisted path -- as its
    only way to pass a credential, which is the practice this replaces.
    """

    import httpx

    from eyvan.artifacts import LocalArtifactStore
    from eyvan.records import CallableExecution, WorkloadDefinition, WorkloadRecord
    from eyvan.sdk import EyvanCallableSubmitter, EyvanClient
    from eyvan.sdk.execution import CallableSubmission, TpuConfig
    from eyvan.worker.runtime.callables import dump_callable

    store = LocalArtifactStore(tmp_path)
    stored_artifact = store.put(dump_callable(lambda: None))
    execution = CallableExecution(
        artifact_uri=stored_artifact.uri,
        sha256=stored_artifact.sha256,
        python=stored_artifact.python,
        result_uri="gs://artifacts/results/run-1.result",
    )
    pending = WorkloadRecord(
        id="wl-1",
        org_id="acme",
        project_id="research",
        spec=WorkloadDefinition(name="train", execution=execution),
        created_by="alice",
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(201, json=pending.model_dump(mode="json"))

    client = EyvanClient(client=httpx.Client(transport=httpx.MockTransport(handler), base_url="http://test"))
    submitter = EyvanCallableSubmitter(client, store)

    submitter.submit_callable(
        CallableSubmission(
            invocation_id="call-1",
            fleet="research",
            org_id="acme",
            project_id="research",
            callable_name="tests.train",
            tpu=TpuConfig(type="v5p-16", secret_mounts={"HF_TOKEN": "gcp-secret://my-project/training-hf-token"}),
            artifact=stored_artifact,
            result_uri="gs://artifacts/results/run-1.result",
        )
    )

    assert json.loads(requests[0].content)["secret_mounts"] == {"HF_TOKEN": "gcp-secret://my-project/training-hf-token"}
    client.close()
