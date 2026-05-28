# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The managed-artifact lifecycle, from a terminal.

Everything under here existed server-side and could be reached by nothing but
hand-written HTTP: the index, the quota accounting, the reverse-reference
index, legal holds, and the collectability analysis the collector consults.

That combination is the worst one for storage. An operator who cannot see what
is collectable, what references what, or what is held cannot answer the only
question that matters when a bucket is filling -- *what is safe to delete* --
and the safe answer in the absence of evidence is "nothing", which is how
managed storage grows without bound while the machinery to bound it sits
unused.

Holds are deliberately here rather than under a destructive-operations group.
Placing one is how a human stops the collector from touching evidence, and it
has to be the easiest thing in this file to find.
"""

from __future__ import annotations

import click

from eyvan.sdk.commands.common import JsonRenderer
from eyvan.sdk.commands.state import CliSession


def _emit(payload: object) -> None:
    JsonRenderer().emit(payload)


@click.group("artifacts")
def artifacts() -> None:
    """Inspect and hold managed artifacts."""


@artifacts.command("list")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--kind", help="Restrict to one artifact kind (callables, workspaces, results, logs).")
@click.pass_obj
def list_artifacts(session: CliSession, org_id: str | None, project_id: str | None, kind: str | None) -> None:
    """List the managed artifacts this project owns."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    params: dict[str, str] = {"org_id": scope.org_id, "project_id": scope.project_id}
    if kind is not None:
        params["kind"] = kind
    _emit(session.client.request("GET", "/api/v1/artifacts", params=params))


@artifacts.command("usage")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def artifact_usage(session: CliSession, org_id: str | None, project_id: str | None) -> None:
    """Report stored bytes against this project's artifact quota."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    _emit(
        session.client.request(
            "GET", "/api/v1/artifacts/usage", params={"org_id": scope.org_id, "project_id": scope.project_id}
        ),
    )


@artifacts.command("collectable")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.pass_obj
def collectable(session: CliSession, org_id: str | None, project_id: str | None) -> None:
    """List artifacts the collector would remove, and why each qualifies.

    Read this before enabling collection, not after. It is the only view that
    distinguishes "unreferenced and past its window" from "still referenced by a
    queued workload", and those look identical in a bucket listing.
    """

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    _emit(
        session.client.request(
            "GET", "/api/v1/artifacts/collectable", params={"org_id": scope.org_id, "project_id": scope.project_id}
        )
    )


@artifacts.command("references")
@click.option("--uri", required=True, help="The managed artifact URI to trace.")
@click.pass_obj
def references(session: CliSession, uri: str) -> None:
    """List the workloads that reference one artifact.

    The question behind "is this safe to delete", answered from the reverse
    index rather than by scanning every spec.
    """

    _emit(session.client.request("GET", "/api/v1/artifacts/references", params={"uri": uri}))


@artifacts.command("hold")
@click.option("--uri", required=True)
@click.option("--reason", required=True, help="Why this artifact must survive collection.")
@click.option("--idempotency-key")
@click.pass_obj
def place_hold(session: CliSession, uri: str, reason: str, idempotency_key: str | None) -> None:
    """Stop the collector from removing one artifact.

    A reason is required because a hold outlives the incident that motivated it,
    and an unexplained hold is indistinguishable from a forgotten one -- which is
    how holds accumulate until nothing is collectable at all.
    """

    _emit(
        session.client.request(
            "POST",
            "/api/v1/artifacts/holds",
            json={"uri": uri, "reason": reason},
            headers=({} if idempotency_key is None else {"Idempotency-Key": idempotency_key}),
        ),
    )


@artifacts.command("release-hold")
@click.option("--uri", required=True)
@click.option("--idempotency-key")
@click.pass_obj
def release_hold(session: CliSession, uri: str, idempotency_key: str | None) -> None:
    """Release a legal hold, making the artifact collectable again."""

    _emit(
        session.client.request(
            "DELETE",
            "/api/v1/artifacts/holds",
            params={"uri": uri},
            headers=({} if idempotency_key is None else {"Idempotency-Key": idempotency_key}),
        ),
    )


def register_artifact_commands(main: click.Group) -> None:
    """Attach the artifact command group to the CLI root."""

    main.add_command(artifacts)


__all__ = ("artifacts", "register_artifact_commands")
