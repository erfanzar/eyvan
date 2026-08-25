# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Rendering and dispatching workload, gang, cancel, and checkpoint commands."""

from __future__ import annotations

import asyncio
import shlex
import typing as tp
import uuid
from datetime import timedelta
from urllib.parse import urlencode, urlparse

from pydantic import ValidationError

from eyvan.capacity import (
    READY_CAPACITY_STATUSES,
)
from eyvan.records import (
    MAX_ATTEMPT_RANK_COUNT,
    AuditEvent,
    CellRecord,
    CellState,
    LeaseRecord,
    LeaseState,
    OutboxRecord,
    OutboxState,
    WorkloadAttemptTopology,
    WorkloadDispatchReservation,
    WorkloadRecord,
    WorkloadState,
    WorkloadTopologyCell,
    WorkspaceRef,
    utcnow,
)
from eyvan.server.controller._support import (
    StaleOutboxCommand,
)
from eyvan.server.service.state import ACTIVE_LEASE_STATES, TERMINAL_WORKLOAD_STATES
from eyvan.worker.agent.models import (
    CheckpointRequest,
    GangWorkloadMember,
    GangWorkloadMutationRequest,
    GangWorkloadRequest,
    MutationModel,
    WorkloadMutationRequest,
    WorkloadRequest,
)
from eyvan.worker.distributed_environment import NATIVE_RENDEZVOUS_PORTS

if tp.TYPE_CHECKING:
    from eyvan.server.persistence import RepositorySession


#: Hosts a remote worker cannot reach when it resolves them itself.
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0"})

#: Providers whose cells share a host with the control plane, so loopback works.
_LOCAL_PROVIDERS = frozenset({"local"})

#: Executors that actually redeem a secret capability into the job's environment.
#:
#: Stated rather than probed because the consequence of getting it wrong is
#: silent: an executor that ignores ``secret_mounts`` runs the job with the
#: variable simply absent, and the only trace is the workload failing on a
#: KeyError somewhere in user code. Only ``systemd_transient`` calls
#: ``materialize_secrets``; add a name here when another runtime learns to.
_SECRET_MATERIALIZING_EXECUTORS = frozenset({"eyvan-native"})

#: Stored on the outbox row in the same transaction as the workload marker.
#: Keeping both copies makes the response writeback prove ownership of the
#: aggregate *and* of the exact at-least-once delivery that contacted the agent.
_DISPATCH_RESERVATION_KEY = "dispatch_reservation"
_RETRIABLE_DISPATCH_OUTBOX_STATES = frozenset({OutboxState.PENDING, OutboxState.PROCESSING, OutboxState.FAILED})


class PermanentDispatchRefusal(RuntimeError):
    """A dispatch that cannot be made to succeed by trying it again.

    The outbox retries a failed command forever with a capped backoff and has no
    dead letter, so a refusal raised as a bare error would hold the lease and keep
    its cell undeletable indefinitely -- the same stuck-lease shape that already
    blocks ``eyvan down``. Every subclass is instead settled by
    :meth:`_ControllerWorkloadCommands._settle_unlaunchable_attempt`, which fails
    the workload with the reason attached and gives the capacity back.

    Subclassing rather than one class per site because the *handling* is the
    shared decision -- "do not retry this, say why" -- while the reasons are
    unrelated and an operator has to be able to tell them apart in
    ``failure_reason``.
    """


class InsecureManagedArtifactTransport(PermanentDispatchRefusal):
    """A managed-artifact workload would carry its capability token over plain HTTP.

    Retrying cannot change the scheme of ``EYVAN_PUBLIC_URL``, so this belongs in
    the family that settles instead of the family that retries. It was raised as
    a bare ``RuntimeError``, which is the exact shape this class exists to
    prevent: the outbox retried it forever with a capped backoff while the lease
    held its accelerator, and the accelerator billed the whole time. Observed on
    a live v5p-16 and a live v6e-8 -- ten attempts over twenty-one minutes, the
    workload stuck ``leased``, the TPU held, and no diagnostic on any surface.

    Every head built by ``eyvan head up`` hits this, because that command writes
    an ``http://`` public URL and terminates no TLS, so it refused every callable
    it was otherwise configured to accept.
    """


class AttemptRankSpaceTooLarge(PermanentDispatchRefusal):
    """The composed rank space of an attempt is above the ceiling.

    Permanent by construction: the width comes from the leased cells, so retrying
    the same dispatch produces the same refusal.
    """


class GangRendezvousCollision(PermanentDispatchRefusal):
    """Two native gangs would be told to meet at one rendezvous endpoint.

    A gang's ranks meet at ``<coordinator host>:<port>``, and both ports are
    fixed constants (:data:`~eyvan.worker.distributed_environment.NATIVE_RENDEZVOUS_PORTS`)
    because a firewall has to be able to open them once. Two gangs whose
    coordinators land on one machine are therefore handed the *same* socket, and
    a rank of one gang can register with the other gang's coordinator -- two jobs
    silently exchanging tensors, which fails as a wrong answer rather than as an
    error.

    Refused here rather than dodged by moving a port. ``worker/runtime/registry.py``
    records what a port no firewall permits costs: every rank of every ``pods=1``
    run died in ``RegisterTask`` with DEADLINE_EXCEEDED on 2026-08-02. A port
    that moves per workload has that failure mode in every deployment whose rules
    name these two numbers.

    Permanent in the sense that matters: the placement that produced it is one
    the scheduler is configured never to make (see ``exclusive_workloads``), so a
    retry re-runs the same placement and refuses again. Failing the workload with
    the reason names the configuration to change instead of looping on it.
    """


class _ControllerWorkloadCommands:
    """Rendering and dispatching workload, gang, cancel, and checkpoint commands.

    A concern of :class:`~eyvan.server.controller.EyvanController`; not
    instantiated on its own.
    """

    @staticmethod
    def _workload_lease_ids(workload: WorkloadRecord) -> tuple[str, ...]:
        """Return one workload's ordered lease membership, including legacy records."""

        if workload.lease_ids:
            return workload.lease_ids
        return () if workload.lease_id is None else (workload.lease_id,)

    @staticmethod
    def _ordered_gang_member_pairs(
        members: tp.Sequence[tuple[LeaseRecord, CellRecord]],
    ) -> tuple[tuple[LeaseRecord, CellRecord], ...]:
        """Validate and deterministically order native group members by cell ID."""

        ordered = tuple(sorted(members, key=lambda member: member[0].cell_id))
        if len(ordered) < 2:
            raise RuntimeError("a native workload gang requires at least two leased cells")
        if len({lease.cell_id for lease, _cell in ordered}) != len(ordered):
            raise RuntimeError("a native workload gang cannot contain duplicate cells")
        endpoints = {cell.endpoint for _lease, cell in ordered}
        if None in endpoints or len(endpoints) != 1:
            raise RuntimeError("native workload gang cells must use one trusted cell-agent endpoint")
        return ordered

    @classmethod
    def _gang_members(
        cls,
        members: tp.Sequence[tuple[LeaseRecord, CellRecord]],
        *,
        idempotency_prefix: str,
    ) -> tuple[GangWorkloadMember, ...]:
        """Build per-cell fenced mutations for one native workload group."""

        return tuple(
            GangWorkloadMember(
                cell_id=lease.cell_id,
                agent_endpoint=cls._required_agent_endpoint(cell),
                mutation=MutationModel(
                    idempotency_key=f"{idempotency_prefix}:{lease.id}",
                    generation=lease.generation,
                    fencing_token=lease.fencing_token,
                ),
            )
            for lease, cell in cls._ordered_gang_member_pairs(members)
        )

    @staticmethod
    def _cell_worker_endpoints(cell: CellRecord) -> tuple[str, ...]:
        """Return one cell's ordered per-host identities, when the provider names them.

        Only a declared list that is exactly as long as the cell's durable
        runtime process width counts. A shorter list is the defect this guards
        against -- it would freeze a narrower rank space than the cell actually
        runs -- and a longer one describes hosts the width says are not there.
        Anything else is recorded as unknown rather than padded or truncated.
        """

        declared = cell.provider_options.get("hosts")
        if not isinstance(declared, list) or len(declared) != cell.runtime_process_count:
            return ()
        endpoints = tuple(entry.strip() for entry in declared if isinstance(entry, str) and entry.strip())
        if len(endpoints) != cell.runtime_process_count or len(set(endpoints)) != len(endpoints):
            return ()
        return endpoints

    @classmethod
    def _rendezvous_site(cls, cell: CellRecord) -> tuple[str, str]:
        """Return what identifies the machine one gang's rendezvous would bind on.

        A gang's coordinator is the lexicographically first of its leased cells,
        and the agent forms the rendezvous address from that cell's first native
        worker endpoint. So two gangs collide exactly when these two values are
        equal, and the value has two spellings because the control plane knows
        the host for some providers and only the cell for others:

        * ``("host", h)`` when the provider *names* the cell's per-host worker
          endpoints. Two different cells on one machine collide, and only this
          spelling can see that.
        * ``("cell", id)`` otherwise. Deliberately not a guessed host: an
          unnamed endpoint is unknown, and an unknown host that was read as
          "some host of its own" would answer "no collision" without evidence.

        The consequence of the fallback is stated rather than hidden: for a
        provider that does not publish its hosts, two gangs on two *different*
        cells of one machine are not detected here. What is detected in every
        case is two gangs on one cell, which is the state ``exclusive_workloads``
        exists to prevent.
        """

        endpoints = cls._cell_worker_endpoints(cell)
        host = urlparse(endpoints[0]).hostname if endpoints else None
        return ("host", host) if host else ("cell", cell.id)

    async def _refuse_shared_gang_rendezvous(
        self,
        transaction: RepositorySession,
        workload: WorkloadRecord,
        coordinator_cell: CellRecord,
    ) -> None:
        """Refuse a native gang that would be handed a live gang's rendezvous endpoint.

        This is the enforcement of a property that until now was only *masked*.
        ``server/service/scheduling.py`` marks every ``native-local`` and
        ``eyvan-native`` cell ``exclusive_workloads``, and ``domain/scheduler.py``
        will not place a second workload on a busy exclusive cell -- so no two
        gangs share a coordinator today, and the fixed rendezvous ports are safe
        by consequence rather than by decision. The moment that flag is off, the
        second gang is handed the first gang's socket and nothing says so.

        Only another *gang* is a conflict. A single-cell workload forms no
        rendezvous -- ``NativeGangExecutor`` is the only thing that writes
        ``JAX_COORDINATOR_ADDRESS`` and ``MASTER_ADDR`` for a cell group -- so
        one sharing the machine is left alone.

        Terminal workloads are skipped even when a lease has not been released
        yet. Their ranks are gone, the endpoint is free, and refusing on a
        settling lease would fail a workload permanently over a few seconds of
        bookkeeping.

        Args:
            transaction: The dispatch transaction; this reads durable rows only.
            workload: The gang being dispatched.
            coordinator_cell: Its coordinator cell -- the lexicographically first
                of its leased cells, whose host the rendezvous is formed on.

        Raises:
            GangRendezvousCollision: If a live gang already holds that endpoint.
        """

        site = self._rendezvous_site(coordinator_cell)
        cells_by_workload: dict[str, list[str]] = {}
        for lease in await transaction.list_leases():
            if lease.workload_id == workload.id or lease.state not in ACTIVE_LEASE_STATES:
                continue
            cells_by_workload.setdefault(lease.workload_id, []).append(lease.cell_id)
        for other_id, cell_ids in sorted(cells_by_workload.items()):
            if len(cell_ids) < 2:
                continue
            other = await transaction.get_workload(other_id)
            if other is None or other.state in TERMINAL_WORKLOAD_STATES:
                continue
            if other.spec.required_executor != "eyvan-native":
                continue
            # The same rule dispatch itself uses: the coordinator is the
            # lexicographically first cell of the group, so it is derived rather
            # than remembered, and stays right across a re-dispatch.
            other_cell = await transaction.get_cell(min(cell_ids))
            if other_cell is None or self._rendezvous_site(other_cell) != site:
                continue
            ports = ", ".join(str(port) for port in NATIVE_RENDEZVOUS_PORTS)
            where = f"host {site[1]!r}" if site[0] == "host" else f"cell {site[1]!r}"
            raise GangRendezvousCollision(
                f"workload {workload.id!r} would form its gang rendezvous on {where}, where native gang "
                f"{other_id!r} already holds one; both are handed the same fixed ports ({ports}), so a rank "
                f"of one gang can join the other gang's coordinator. Native execution cells are marked "
                f"exclusive_workloads precisely so a second workload is never placed on a busy one; this "
                f"dispatch means either exclusive_workloads is off for these cells, or two cells name one "
                f"machine. Eyvan will not move the rendezvous port to work around it -- these numbers are "
                f"what a deployment's firewall opens."
            )

    @classmethod
    def _freeze_attempt_topology(
        cls,
        workload: WorkloadRecord,
        cells: tp.Sequence[CellRecord],
    ) -> WorkloadAttemptTopology:
        """Capture the rank space this attempt is about to be dispatched onto.

        Once, here, and never recomputed. Reading the widths back from live cell
        records is what let a reconciliation that changed one cell's observed
        process count shift every later cell's global ranks mid-attempt, so two
        pages of one attempt disagreed about which host was rank N.

        Also the only place the *total* rank count is bounded. The per-cell and
        per-group ceilings are each 10,000 and compose into a hundred million
        ranks; every one of those would be a cursor position and an archive
        object, so the composed width is refused before anything is launched.
        """

        try:
            return WorkloadAttemptTopology.freeze(
                workload_id=workload.id,
                attempt=workload.attempt,
                generation=workload.generation,
                cells=[
                    WorkloadTopologyCell(
                        cell_id=cell.id,
                        generation=cell.generation,
                        runtime_process_count=cell.runtime_process_count,
                        global_rank_offset=0,
                        worker_endpoints=cls._cell_worker_endpoints(cell),
                    )
                    for cell in cells
                ],
            )
        except ValueError as exc:
            raise AttemptRankSpaceTooLarge(
                f"workload {workload.id!r} cannot be dispatched with a bounded rank space "
                f"(ceiling {MAX_ATTEMPT_RANK_COUNT}): {exc}"
            ) from exc

    async def _persist_attempt_topology(
        self,
        transaction: RepositorySession,
        workload: WorkloadRecord,
        cells: tp.Sequence[CellRecord],
    ) -> WorkloadAttemptTopology:
        """Durably freeze this attempt's rank space in the reservation transaction.

        Written before the submit RPC and in the same transaction as the dispatch
        reservation, so an attempt can never be running with no proof of the map
        its ranks mean. Re-preparing the same attempt rewrites it: this path only
        runs while the attempt has no remote id, and the topology that matters is
        the one the process that actually started was launched against.
        """

        topology = self._freeze_attempt_topology(workload, cells)
        await transaction.put_workload_topology(topology)
        return topology

    async def _load_workload_members(
        self,
        workload: WorkloadRecord,
    ) -> tuple[tuple[LeaseRecord, CellRecord], ...]:
        """Load every current lease and cell belonging to a workload group."""

        lease_ids = self._workload_lease_ids(workload)
        if not lease_ids:
            raise StaleOutboxCommand(f"workload {workload.id!r} has no current lease")
        async with self.repository.transaction() as transaction:
            leases = tuple([await transaction.get_lease(lease_id) for lease_id in lease_ids])
            concrete_leases = tuple(lease for lease in leases if lease is not None)
            if len(concrete_leases) != len(leases):
                raise StaleOutboxCommand(f"workload {workload.id!r} references a missing lease")
            cells = tuple([await transaction.get_cell(lease.cell_id) for lease in concrete_leases])
        concrete_cells = tuple(cell for cell in cells if cell is not None)
        if len(concrete_cells) != len(cells):
            raise StaleOutboxCommand(f"workload {workload.id!r} references a missing execution cell")
        if any(lease.workload_id != workload.id for lease in concrete_leases):
            raise StaleOutboxCommand(f"workload {workload.id!r} lease membership changed")
        return tuple(zip(concrete_leases, concrete_cells, strict=True))

    def _workspace_entrypoint(
        self,
        workload: WorkloadRecord,
        workspace: WorkspaceRef,
        command: str,
        environment: dict[str, str],
    ) -> tuple[str, dict[str, str]]:
        """Wrap a command so its workspace is materialized before it runs.

        The wrapper `exec`s the command, so the workload keeps the PID systemd
        started and signals, exit status and process introspection continue to
        describe the job rather than a shim.
        """

        parts = [
            "python",
            "-m",
            "eyvan.worker.runtime.workspace_entry",
            "--workspace-sha256",
            workspace.sha256,
        ]
        if workspace.signature:
            parts.extend(("--workspace-signature", workspace.signature))
        if workspace.uri.startswith("eyvan://"):
            if self.artifact_access is None:
                raise RuntimeError("managed workspaces are not configured on this control-plane replica")
            transfer = self.artifact_access.workspace_download_transfer(
                workspace.uri,
                org_id=workload.org_id,
                project_id=workload.project_id,
                workload_id=workload.id,
            )
            environment["EYVAN_MANAGED_WORKSPACE_URL"] = transfer.url
            environment["EYVAN_MANAGED_WORKSPACE_TOKEN"] = transfer.token
            parts.extend(
                (
                    "--workspace-uri-env",
                    "EYVAN_MANAGED_WORKSPACE_URL",
                    "--workspace-token-env",
                    "EYVAN_MANAGED_WORKSPACE_TOKEN",
                )
            )
        else:
            parts.extend(("--workspace-uri", workspace.uri))
        parts.append("--")
        return " ".join(shlex.quote(part) for part in parts) + " " + command, environment

    def _refuse_unreachable_callback(self, workload: WorkloadRecord, cell: CellRecord) -> None:
        """Refuse a dispatch whose callback URL the target cell cannot reach.

        Secret mounts and in-job submission hand the worker an address and expect
        it to call back. That address is this plane's ``public_url``, and a
        loopback one resolves on the *worker's* host -- so a remote cell gets
        connection-refused several minutes later, and the durable failure blames
        the worker for a control-plane misconfiguration.

        Found by running P4: a secret mount was accepted, dispatched, and failed
        eight minutes later with ``Connection refused``, when the impossibility
        was knowable before the cell was ever contacted.

        Local cells are exempt because loopback genuinely works for them, which
        is the whole development topology and must keep working.
        """

        if workload.spec.secret_mounts and (self.secret_capability is None or self.artifact_access is None):
            raise RuntimeError("secret delivery is not configured on this control-plane replica")
        if workload.spec.secret_mounts and cell.executor not in _SECRET_MATERIALIZING_EXECUTORS:
            # Refused rather than run without them. The control plane has already
            # minted a per-attempt capability and written ``secret.issued``, so a
            # silent drop leaves an issuance with no matching read -- which is
            # precisely the signal a *leaked* token produces. Normal operation on
            # such an executor would make that signal meaningless.
            raise RuntimeError(
                f"executor {cell.executor!r} does not materialize secret mounts, and this workload declares "
                f"{sorted(workload.spec.secret_mounts)}; it would run with those variables absent"
            )
        needs_callback = bool(workload.spec.secret_mounts) or workload.spec.can_submit_children
        if not needs_callback or self.artifact_access is None:
            return
        base = self.artifact_access.public_url
        host = urlparse(base).hostname or ""
        if host not in _LOOPBACK_HOSTS or cell.provider in _LOCAL_PROVIDERS:
            return
        wants = "secret mounts" if workload.spec.secret_mounts else "in-job submission"
        raise RuntimeError(
            f"this control plane advertises {base!r}, which a cell on provider {cell.provider!r} cannot reach; "
            f"{wants} require a callback address the worker can resolve -- set EYVAN_PUBLIC_URL"
        )

    def _children_url(self, workload: WorkloadRecord) -> str:
        """Return the address a job posts its children to.

        Built here for the same reason the progress URL is: a job should receive
        a complete address rather than a base URL plus the knowledge of how to
        assemble one, which would be a second place for the route to be spelled.
        """

        base = "" if self.artifact_access is None else self.artifact_access.public_url
        return f"{base}/api/v1/workloads/{workload.id}/children"

    def _secret_environment(self, workload: WorkloadRecord) -> dict[str, str]:
        """Return the addresses and capabilities for this attempt's secret mounts.

        Values are conspicuously absent. What travels is one short-lived
        capability per mount and the address to redeem it at, so the secret
        itself never enters a dispatch command, an agent request, or any record
        either of them is persisted in.

        A fresh token per *attempt* is the revocation: the previous attempt's
        capabilities stop being honored the moment ``attempt`` advances, which
        every retry and every preemption resume does.
        """

        mounts = workload.spec.secret_mounts
        if not mounts:
            return {}
        if self.secret_capability is None or self.artifact_access is None:
            # Fail before contacting the cell.  Launching without a declared
            # mount is a successful-looking privilege/configuration failure and
            # is never an acceptable fallback.
            raise RuntimeError("secret delivery is not configured on this control-plane replica")
        reservation = workload.dispatch_reservation
        if reservation is None:
            raise RuntimeError("secret capabilities require a committed dispatch reservation")
        environment = {"EYVAN_SECRET_COUNT": str(len(mounts))}
        for index, name in enumerate(sorted(mounts)):
            environment[f"EYVAN_SECRET_{index}_NAME"] = name
            environment[f"EYVAN_SECRET_{index}_URL"] = self._secret_url(workload, name)
            environment[f"EYVAN_SECRET_{index}_TOKEN"] = self.secret_capability.issue(
                workload_id=workload.id,
                org_id=workload.org_id,
                project_id=workload.project_id,
                attempt=workload.attempt,
                generation=workload.generation,
                dispatch_fence=reservation.fence,
                name=name,
            )
        return environment

    def _secret_url(self, workload: WorkloadRecord, name: str) -> str:
        """Return the address one mount is redeemed at, fully assembled.

        Same reasoning as ``_children_url``: handing a job a base URL plus the
        knowledge of how to build the rest makes the route exist in two places.
        """

        if self.artifact_access is None:
            raise RuntimeError("secret delivery is not configured on this control-plane replica")
        base = self.artifact_access.public_url
        query = urlencode(
            {
                "org_id": workload.org_id,
                "project_id": workload.project_id,
                "workload_id": workload.id,
                "name": name,
            }
        )
        return f"{base}/api/v1/secrets/content?{query}"

    async def _audit_secret_issuance(self, transaction: RepositorySession, workload: WorkloadRecord) -> None:
        """Record which mounts an attempt was granted, in the dispatch transaction.

        The other half of the pair the fetch route writes. One issuance here and
        one read there is what makes a leaked capability visible: a ``secret.read``
        whose attempt has no matching ``secret.issued`` is a token being used
        outside the dispatch that minted it, and that is invisible if only reads
        are recorded.
        """

        mounts = workload.spec.secret_mounts
        if not mounts:
            return
        if self.secret_capability is None or self.artifact_access is None:
            raise RuntimeError("secret delivery is not configured on this control-plane replica")
        await self.service.record_secret_issuance(
            org_id=workload.org_id,
            project_id=workload.project_id,
            workload_id=workload.id,
            attempt=workload.attempt,
            generation=workload.generation,
            dispatch_reservation=workload.dispatch_reservation,
            names=tuple(sorted(mounts)),
            tx=transaction,
        )

    def _render_execution(
        self,
        workload: WorkloadRecord,
        *,
        runtime_environment: tp.Mapping[str, str],
    ) -> tuple[str, dict[str, str]]:
        """Return the entrypoint and environment for one workload dispatch.

        Shared by the single-cell and gang dispatch paths. The callable contract
        -- which managed URIs are minted, which env vars carry them, and which
        worker flags name those vars -- must stay identical on both paths, so it
        lives here rather than being written out twice.
        """

        environment = {
            **runtime_environment,
            **workload.spec.environment,
            "EYVAN_WORKLOAD_ID": workload.id,
            "EYVAN_ATTEMPT": str(workload.attempt),
            "EYVAN_RESUME_CHECKPOINT_URI": workload.checkpoint_uri or "",
        }
        if workload.spec.can_submit_children and self.workload_capability is not None:
            # Beside the workload identity rather than inside the managed-result
            # block: a command workload fans out too, and only callables get a
            # managed result. The credential is issued per attempt, pinned to
            # this generation, so a retry's dispatch mints a new one and the
            # dead attempt's stops working.
            environment["EYVAN_WORKLOAD_TOKEN"] = self.workload_capability.issue(
                workload_id=workload.id,
                org_id=workload.org_id,
                project_id=workload.project_id,
                generation=workload.generation,
            )
            environment["EYVAN_CHILDREN_URL"] = self._children_url(workload)
        environment.update(self._secret_environment(workload))
        execution = workload.spec.execution
        if execution.kind == "command":
            # A workspace belongs to the workload, not to one execution kind, so
            # it is resolved before this branch rather than inside the callable
            # one. Everything workspace-related used to live below this early
            # return, which is precisely why `eyvan run` could not ship a
            # working tree and running your own code meant baking an image.
            workspace = workload.spec.resolved_workspace()
            if workspace is None:
                return execution.command, environment
            return self._workspace_entrypoint(workload, workspace, execution.command, environment)

        artifact_uri = execution.artifact_uri
        result_uri = execution.result_uri
        workspace_uri = execution.workspace_uri
        managed_transfer = (
            artifact_uri.startswith("eyvan://")
            or bool(result_uri and result_uri.startswith("eyvan://"))
            or bool(workspace_uri and workspace_uri.startswith("eyvan://"))
        )
        if managed_transfer and self.artifact_access is None:
            raise RuntimeError("managed callable artifacts are not configured on this control-plane replica")
        parts = [
            "python",
            "-m",
            "eyvan.worker.runtime.callable_worker",
            "--sha256",
            execution.sha256,
            "--python",
            execution.python,
            "--format-version",
            str(execution.format_version),
        ]
        if artifact_uri.startswith("eyvan://"):
            assert self.artifact_access is not None
            transfer = self.artifact_access.callable_download_transfer(
                artifact_uri,
                org_id=workload.org_id,
                project_id=workload.project_id,
                workload_id=workload.id,
            )
            environment["EYVAN_MANAGED_CALLABLE_URL"] = transfer.url
            environment["EYVAN_MANAGED_CALLABLE_TOKEN"] = transfer.token
            parts.extend(
                (
                    "--uri-env",
                    "EYVAN_MANAGED_CALLABLE_URL",
                    "--uri-token-env",
                    "EYVAN_MANAGED_CALLABLE_TOKEN",
                )
            )
        else:
            parts.extend(("--uri", artifact_uri))
        if execution.signature:
            parts.extend(("--signature", execution.signature))
        if workspace_uri:
            parts.extend(("--workspace-sha256", str(execution.workspace_sha256)))
            if execution.workspace_signature:
                parts.extend(("--workspace-signature", execution.workspace_signature))
            if workspace_uri.startswith("eyvan://"):
                assert self.artifact_access is not None
                transfer = self.artifact_access.workspace_download_transfer(
                    workspace_uri,
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    workload_id=workload.id,
                )
                environment["EYVAN_MANAGED_WORKSPACE_URL"] = transfer.url
                environment["EYVAN_MANAGED_WORKSPACE_TOKEN"] = transfer.token
                parts.extend(
                    (
                        "--workspace-uri-env",
                        "EYVAN_MANAGED_WORKSPACE_URL",
                        "--workspace-token-env",
                        "EYVAN_MANAGED_WORKSPACE_TOKEN",
                    )
                )
            else:
                parts.extend(("--workspace-uri", workspace_uri))
        if result_uri and result_uri.startswith("eyvan://"):
            assert self.artifact_access is not None
            transfer = self.artifact_access.result_upload_transfer(
                result_uri,
                org_id=workload.org_id,
                project_id=workload.project_id,
                workload_id=workload.id,
            )
            environment["EYVAN_MANAGED_RESULT_URL"] = transfer.url
            environment["EYVAN_MANAGED_RESULT_TOKEN"] = transfer.token
            # The progress channel had a route, a schema, a record, a migration
            # and a read path, and no producer anywhere -- so nothing a job could
            # do would ever fill it. A job learns the address the same way it
            # learns every other one: fully formed, from its environment, so
            # nothing inside the job builds a control-plane URL. Authentication
            # reuses the result-write token deliberately; the route documents why.
            environment["EYVAN_PROGRESS_URL"] = self.artifact_access.progress_report_url(
                workload_id=workload.id,
                org_id=workload.org_id,
                project_id=workload.project_id,
            )
            parts.extend(
                (
                    "--result-uri-env",
                    "EYVAN_MANAGED_RESULT_URL",
                    "--result-token-env",
                    "EYVAN_MANAGED_RESULT_TOKEN",
                )
            )
        elif result_uri:
            parts.extend(("--result-uri", result_uri))
        if (
            managed_transfer
            and self.artifact_access is not None
            and self.artifact_access.public_url.startswith("http://")
        ):
            if not (self.settings.dev_auth_enabled or self.settings.allow_insecure_artifact_http):
                raise InsecureManagedArtifactTransport(
                    f"managed artifact transfer would send a capability token to {self.artifact_access.public_url} "
                    "in clear text; put the control plane behind TLS and set EYVAN_PUBLIC_URL to its https:// "
                    "address, or accept the risk explicitly with EYVAN_ALLOW_INSECURE_ARTIFACT_HTTP=1 "
                    "(defensible only where the plane is unreachable outside a private network)"
                )
            environment["EYVAN_ALLOW_INSECURE_ARTIFACT_HTTP"] = "1"
        return shlex.join(parts), environment

    @staticmethod
    def _outbox_dispatch_reservation(command: OutboxRecord) -> WorkloadDispatchReservation | None:
        """Decode the strict reservation stored on one dispatch command."""

        raw = command.payload.get(_DISPATCH_RESERVATION_KEY)
        if raw is None:
            return None
        try:
            return WorkloadDispatchReservation.model_validate(raw)
        except ValidationError:
            return None

    async def _owned_dispatch_command(
        self,
        transaction: RepositorySession,
        command: OutboxRecord,
    ) -> OutboxRecord:
        """Load the exact live outbox claim allowed to enter a submit RPC."""

        current = await transaction.get_outbox(command.id)
        now = utcnow()
        if (
            current is None
            or current.topic != "workload.dispatch"
            or current.aggregate_type != "workload"
            or current.aggregate_id != command.aggregate_id
            or current.generation != command.generation
            or current.attempts != command.attempts
            or current.state is not OutboxState.PROCESSING
            or current.claimed_by != self.worker_id
            or current.claimed_until is None
            or current.claimed_until <= now
        ):
            raise StaleOutboxCommand(f"dispatch command {command.id!r} claim expired or moved to another worker")
        return current

    @staticmethod
    def _same_dispatch_reservation(
        command: OutboxRecord,
        reservation: WorkloadDispatchReservation,
    ) -> bool:
        return _ControllerWorkloadCommands._outbox_dispatch_reservation(command) == reservation

    @staticmethod
    def _dispatch_is_already_finalized(
        workload: WorkloadRecord,
        leases: tp.Sequence[LeaseRecord],
        reservation_command_id: str,
    ) -> bool:
        """Return whether a replay only needs to retire its outbox command."""

        reservation = workload.dispatch_reservation
        return bool(
            reservation is not None
            and reservation.outbox_id == reservation_command_id
            and reservation.generation == workload.generation
            and reservation.workload_attempt == workload.attempt
            and reservation.lease_ids == _ControllerWorkloadCommands._workload_lease_ids(workload)
            and workload.remote_id
            and workload.state in {WorkloadState.DISPATCHING, WorkloadState.RUNNING, WorkloadState.CANCELLING}
            and leases
            and all(lease.state is LeaseState.ACTIVE for lease in leases)
        )

    async def _quarantined_dispatch_cells(
        self,
        command: OutboxRecord,
        lease_ids: tuple[str, ...],
    ) -> tuple[CellRecord, ...]:
        """Snapshot cells whose pending leases need an out-of-transaction probe."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
            leases = tuple([await transaction.get_lease(lease_id) for lease_id in lease_ids])
            members = tuple(lease for lease in leases if lease is not None)
            if workload is None or len(members) != len(leases):
                raise StaleOutboxCommand("dispatch command references a missing workload or lease")
            cells = tuple([await transaction.get_cell(lease.cell_id) for lease in members])
            await self._owned_dispatch_command(transaction, command)
            if self._dispatch_is_already_finalized(workload, members, command.id):
                return ()
            if workload.generation != command.generation or self._workload_lease_ids(workload) != lease_ids:
                raise StaleOutboxCommand("dispatch command no longer owns the workload lease generation")
        concrete_cells = tuple(cell for cell in cells if cell is not None)
        if len(concrete_cells) != len(cells):
            raise StaleOutboxCommand("dispatch command references a missing execution cell")
        return tuple(
            cell for lease, cell in zip(members, concrete_cells, strict=True) if lease.state is LeaseState.QUARANTINED
        )

    async def _recover_quarantined_dispatch_cells(
        self,
        command: OutboxRecord,
        lease_ids: tuple[str, ...],
    ) -> frozenset[str]:
        """Probe quarantined members without retaining any repository lock."""

        recovered: set[str] = set()
        for cell in await self._quarantined_dispatch_cells(command, lease_ids):
            observation = await asyncio.to_thread(self._agent_factory(cell).observe, cell.id)
            if observation.state not in READY_CAPACITY_STATUSES:
                raise RuntimeError(f"cell {cell.id!r} has not recovered the quarantined lease")
            recovered.add(cell.id)
        return frozenset(recovered)

    @staticmethod
    def _new_dispatch_reservation(
        command: OutboxRecord,
        workload: WorkloadRecord,
        lease_ids: tuple[str, ...],
    ) -> WorkloadDispatchReservation:
        return WorkloadDispatchReservation(
            fence=f"dispatch_{uuid.uuid4().hex}",
            outbox_id=command.id,
            outbox_attempt=command.attempts,
            generation=command.generation,
            workload_attempt=workload.attempt,
            lease_ids=lease_ids,
        )

    async def _persist_dispatch_reservation(
        self,
        transaction: RepositorySession,
        command: OutboxRecord,
        current_command: OutboxRecord,
        workload: WorkloadRecord,
        reservation: WorkloadDispatchReservation,
    ) -> WorkloadRecord:
        """Atomically persist workload/outbox fences and issuance audit."""

        reserved = workload.model_copy(update={"dispatch_reservation": reservation, "updated_at": utcnow()})
        payload = dict(current_command.payload)
        payload[_DISPATCH_RESERVATION_KEY] = reservation.model_dump(mode="json")
        await transaction.put_outbox(
            current_command.model_copy(
                update={
                    "payload": payload,
                    "last_error": None,
                    "failure_code": None,
                    "updated_at": utcnow(),
                }
            )
        )
        await transaction.put_workload(reserved)
        await transaction.append_event(
            AuditEvent(
                id=f"evt_{uuid.uuid4().hex}",
                actor=self._principal.subject,
                org_id=reserved.org_id,
                project_id=reserved.project_id,
                entity_type="workload",
                entity_id=reserved.id,
                action="workload.dispatch_reserved",
                previous_state=workload.state.value,
                new_state=reserved.state.value,
                payload={
                    "outbox_id": reservation.outbox_id,
                    "outbox_attempt": reservation.outbox_attempt,
                    "attempt": reservation.workload_attempt,
                    "generation": reservation.generation,
                    "lease_ids": list(reservation.lease_ids),
                },
            )
        )
        await self._audit_secret_issuance(transaction, reserved)
        return reserved

    async def _prepare_single_dispatch(
        self,
        command: OutboxRecord,
        *,
        recovered_cells: frozenset[str],
    ) -> tuple[WorkloadRecord, LeaseRecord, CellRecord, WorkloadRequest, WorkloadDispatchReservation] | None:
        """Validate and reserve one single-cell submit in one short transaction."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
            lease = await transaction.get_lease(str(command.payload["lease_id"]))
            cell = await transaction.get_cell(str(command.payload["cell_id"]))
            if workload is None or lease is None or cell is None:
                raise StaleOutboxCommand("dispatch command references a missing workload, lease, or cell")
            current_command = await self._owned_dispatch_command(transaction, command)
            self._validate_workload_command(command, workload, lease, cell)
            if self._dispatch_is_already_finalized(workload, (lease,), command.id):
                return None
            if workload.state is WorkloadState.CANCELLING:
                prior = workload.dispatch_reservation
                if prior is None or prior.outbox_id != command.id:
                    raise StaleOutboxCommand("cancelled workload has no possibly-started dispatch to recover")
            elif workload.state is not WorkloadState.LEASED:
                raise StaleOutboxCommand(f"workload is {workload.state.value}, not leased")
            if lease.state not in {LeaseState.PENDING, LeaseState.QUARANTINED}:
                raise StaleOutboxCommand(f"lease is {lease.state.value}, not pending")
            if cell.state is not CellState.READY:
                raise RuntimeError(f"cell {cell.id!r} is {cell.state.value}, not ready for dispatch")
            if lease.state is LeaseState.QUARANTINED and cell.id not in recovered_cells:
                raise RuntimeError(f"cell {cell.id!r} quarantine recovery was not proven before dispatch")

            self._refuse_unreachable_callback(workload, cell)
            reservation = self._new_dispatch_reservation(command, workload, (lease.id,))
            reserved = await self._persist_dispatch_reservation(
                transaction,
                command,
                current_command,
                workload,
                reservation,
            )
            await self._persist_attempt_topology(transaction, reserved, (cell,))
            entrypoint, environment = self._render_execution(reserved, runtime_environment=cell.runtime.env)
            request = WorkloadRequest(
                mutation=self._mutation(command, lease.fencing_token),
                spec={
                    "entrypoint": entrypoint,
                    "submission_id": f"{reserved.id}-a{reserved.attempt}-g{reserved.generation}",
                    "environment": environment,
                    "backend_options": reserved.spec.backend_options,
                    "gang": {
                        **reserved.spec.gang.model_dump(mode="json"),
                        "placement_strategy": "PACK",
                    },
                    "metadata": {
                        **reserved.spec.metadata,
                        "eyvan_workload_id": reserved.id,
                        "eyvan_org_id": reserved.org_id,
                        "eyvan_project_id": reserved.project_id,
                        "eyvan_lease_id": lease.id,
                    },
                },
            )
            return reserved, lease, cell, request, reservation

    async def _prepare_gang_dispatch(
        self,
        command: OutboxRecord,
        lease_ids: tuple[str, ...],
        *,
        recovered_cells: frozenset[str],
    ) -> (
        tuple[
            WorkloadRecord,
            tuple[LeaseRecord, ...],
            tuple[CellRecord, ...],
            LeaseRecord,
            CellRecord,
            GangWorkloadRequest,
            WorkloadDispatchReservation,
        ]
        | None
    ):
        """Validate and reserve an all-or-nothing native gang submit."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
            leases = tuple([await transaction.get_lease(lease_id) for lease_id in lease_ids])
            members = tuple(lease for lease in leases if lease is not None)
            if workload is None or len(members) != len(leases):
                raise StaleOutboxCommand("gang dispatch command references a missing workload or lease")
            current_command = await self._owned_dispatch_command(transaction, command)
            if self._dispatch_is_already_finalized(workload, members, command.id):
                return None
            if workload.state is WorkloadState.CANCELLING:
                prior = workload.dispatch_reservation
                if prior is None or prior.outbox_id != command.id:
                    raise StaleOutboxCommand("cancelled workload has no possibly-started gang dispatch to recover")
            elif workload.state is not WorkloadState.LEASED:
                raise StaleOutboxCommand(f"workload is {workload.state.value}, not leased")
            if workload.lease_ids != lease_ids or workload.lease_id != lease_ids[0]:
                raise StaleOutboxCommand("gang dispatch command no longer owns the workload lease group")
            if any(lease.workload_id != workload.id or lease.generation != command.generation for lease in members):
                raise StaleOutboxCommand("gang dispatch members no longer own the workload generation")
            if any(lease.state not in {LeaseState.PENDING, LeaseState.QUARANTINED} for lease in members):
                raise StaleOutboxCommand("gang dispatch lease is not pending")
            cells = tuple([await transaction.get_cell(lease.cell_id) for lease in members])
            concrete_cells = tuple(cell for cell in cells if cell is not None)
            if len(concrete_cells) != len(cells):
                raise StaleOutboxCommand("gang dispatch references a missing execution cell")
            if any(cell.state is not CellState.READY for cell in concrete_cells):
                bad = next(cell for cell in concrete_cells if cell.state is not CellState.READY)
                raise RuntimeError(f"cell {bad.id!r} is {bad.state.value}, not ready for gang dispatch")
            if any(
                lease.cell_generation != cell.generation for lease, cell in zip(members, concrete_cells, strict=True)
            ):
                raise StaleOutboxCommand("gang dispatch cell generation changed after placement")
            if any(
                lease.state is LeaseState.QUARANTINED and cell.id not in recovered_cells
                for lease, cell in zip(members, concrete_cells, strict=True)
            ):
                raise RuntimeError("gang dispatch quarantine recovery was not proven for every member")
            if workload.spec.required_executor != "eyvan-native":
                raise RuntimeError("multi-cell pod groups currently require the eyvan-native executor")
            endpoints = {cell.endpoint for cell in concrete_cells}
            if None in endpoints or len(endpoints) != 1:
                raise RuntimeError("native pod group cells must use the same trusted cell-agent endpoint")

            leader_lease = members[0]
            leader_cell = concrete_cells[0]
            self._refuse_unreachable_callback(workload, leader_cell)
            # One definition of "the coordinator", used by the rendezvous refusal
            # below and by the request built further down. Two derivations of the
            # same rule is how a check ends up guarding a different cell from the
            # one that is dispatched.
            paired = tuple(sorted(zip(members, concrete_cells, strict=True), key=lambda item: item[0].cell_id))
            coordinator_lease, coordinator_cell = paired[0]
            # Before anything is reserved or written, and before the agent is
            # contacted: a colliding environment must never be emitted, and a
            # refusal after the reservation would leave a dispatch marker for a
            # launch that never happened.
            await self._refuse_shared_gang_rendezvous(transaction, workload, coordinator_cell)
            reservation = self._new_dispatch_reservation(command, workload, lease_ids)
            reserved = await self._persist_dispatch_reservation(
                transaction,
                command,
                current_command,
                workload,
                reservation,
            )
            await self._persist_attempt_topology(transaction, reserved, concrete_cells)
            entrypoint, environment = self._render_execution(reserved, runtime_environment=leader_cell.runtime.env)
            spec = {
                "entrypoint": entrypoint,
                "submission_id": f"{reserved.id}-a{reserved.attempt}-g{reserved.generation}",
                "environment": environment,
                "backend_options": reserved.spec.backend_options,
                "gang": {
                    **reserved.spec.gang.model_dump(mode="json"),
                    "placement_strategy": "PACK",
                },
                "metadata": {
                    **reserved.spec.metadata,
                    "eyvan_workload_id": reserved.id,
                    "eyvan_org_id": reserved.org_id,
                    "eyvan_project_id": reserved.project_id,
                    "eyvan_lease_id": leader_lease.id,
                    "eyvan_lease_ids": ",".join(lease.id for lease in members),
                },
            }
            request = GangWorkloadRequest(
                members=tuple(
                    GangWorkloadMember(
                        cell_id=lease.cell_id,
                        agent_endpoint=self._required_agent_endpoint(cell),
                        mutation=MutationModel(
                            idempotency_key=f"{command.idempotency_key}:{lease.id}",
                            generation=lease.generation,
                            fencing_token=lease.fencing_token,
                        ),
                    )
                    for lease, cell in paired
                ),
                spec=spec,
            )
            return reserved, members, concrete_cells, coordinator_lease, coordinator_cell, request, reservation

    async def _release_dispatch_reservation(
        self,
        command: OutboxRecord,
        reservation: WorkloadDispatchReservation,
    ) -> bool:
        """Revoke one failed submit's capabilities while its exact fence is owned."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
            if (
                workload is None
                or workload.dispatch_reservation != reservation
                or workload.generation != reservation.generation
                or workload.attempt != reservation.workload_attempt
                or self._workload_lease_ids(workload) != reservation.lease_ids
                or workload.state not in {WorkloadState.LEASED, WorkloadState.CANCELLING}
            ):
                return False
            leases = tuple([await transaction.get_lease(lease_id) for lease_id in reservation.lease_ids])
            if any(
                lease is None
                or lease.workload_id != workload.id
                or lease.generation != reservation.generation
                or lease.state not in {LeaseState.PENDING, LeaseState.QUARANTINED}
                for lease in leases
            ):
                return False
            try:
                current_command = await self._owned_dispatch_command(transaction, command)
            except StaleOutboxCommand:
                return False
            if not self._same_dispatch_reservation(current_command, reservation):
                return False
            payload = dict(current_command.payload)
            payload.pop(_DISPATCH_RESERVATION_KEY, None)
            await transaction.put_outbox(current_command.model_copy(update={"payload": payload, "updated_at": utcnow()}))
            released = workload.model_copy(update={"dispatch_reservation": None, "updated_at": utcnow()})
            await transaction.put_workload(released)
            await transaction.append_event(
                AuditEvent(
                    id=f"evt_{uuid.uuid4().hex}",
                    actor=self._principal.subject,
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    entity_type="workload",
                    entity_id=workload.id,
                    action="workload.dispatch_reservation_failed",
                    previous_state=workload.state.value,
                    new_state=released.state.value,
                    payload={
                        "outbox_id": reservation.outbox_id,
                        "outbox_attempt": reservation.outbox_attempt,
                        "attempt": reservation.workload_attempt,
                        "generation": reservation.generation,
                        "lease_ids": list(reservation.lease_ids),
                    },
                )
            )
            return True

    async def _finalize_dispatch_response(
        self,
        command: OutboxRecord,
        reservation: WorkloadDispatchReservation,
        remote_id: str,
    ) -> None:
        """Atomically activate every member after revalidating all dispatch fences."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
            if (
                workload is None
                or workload.dispatch_reservation != reservation
                or workload.generation != reservation.generation
                or workload.attempt != reservation.workload_attempt
                or self._workload_lease_ids(workload) != reservation.lease_ids
            ):
                raise StaleOutboxCommand("dispatch response no longer owns its workload reservation")
            leases = tuple([await transaction.get_lease(lease_id) for lease_id in reservation.lease_ids])
            members = tuple(lease for lease in leases if lease is not None)
            if len(members) != len(leases) or any(
                lease.workload_id != workload.id or lease.generation != reservation.generation for lease in members
            ):
                raise StaleOutboxCommand("dispatch response no longer owns every lease generation")
            current_command = await self._owned_dispatch_command(transaction, command)
            if not self._same_dispatch_reservation(current_command, reservation):
                raise StaleOutboxCommand("dispatch response no longer owns its outbox reservation")
            if self._dispatch_is_already_finalized(workload, members, command.id):
                if workload.remote_id != remote_id:
                    raise StaleOutboxCommand("dispatch replay returned a different remote workload id")
                return
            if workload.state not in {WorkloadState.LEASED, WorkloadState.CANCELLING}:
                raise StaleOutboxCommand(f"dispatch response found workload {workload.state.value}")
            if any(lease.state not in {LeaseState.PENDING, LeaseState.QUARANTINED} for lease in members):
                raise StaleOutboxCommand("dispatch response found a lease that is no longer pending")

            now = utcnow()
            active_leases = tuple(
                lease.model_copy(
                    update={
                        "state": LeaseState.ACTIVE,
                        "expires_at": now + timedelta(seconds=self.settings.lease_ttl_s),
                        "run_started_at": lease.run_started_at or now,
                        "updated_at": now,
                    }
                )
                for lease in members
            )
            for active in active_leases:
                await transaction.put_lease(active)
            next_state = WorkloadState.DISPATCHING if workload.state is WorkloadState.LEASED else workload.state
            dispatched = workload.model_copy(update={"state": next_state, "remote_id": remote_id, "updated_at": now})
            await transaction.put_workload(dispatched)
            for lease, active in zip(members, active_leases, strict=True):
                await transaction.append_event(
                    AuditEvent(
                        id=f"evt_{uuid.uuid4().hex}",
                        actor=self._principal.subject,
                        org_id=lease.org_id,
                        project_id=lease.project_id,
                        entity_type="lease",
                        entity_id=lease.id,
                        action="lease.activated",
                        previous_state=lease.state.value,
                        new_state=active.state.value,
                    )
                )
            await transaction.append_event(
                AuditEvent(
                    id=f"evt_{uuid.uuid4().hex}",
                    actor=self._principal.subject,
                    org_id=workload.org_id,
                    project_id=workload.project_id,
                    entity_type="workload",
                    entity_id=workload.id,
                    action="workload.dispatched",
                    previous_state=workload.state.value,
                    new_state=dispatched.state.value,
                    payload={"remote_id": remote_id, "lease_ids": list(reservation.lease_ids)},
                )
            )

    async def _settle_unlaunchable_attempt(
        self,
        command: OutboxRecord,
        lease_ids: tp.Sequence[str],
        refusal: PermanentDispatchRefusal,
    ) -> None:
        """Fail a workload whose dispatch cannot be made to succeed by retrying.

        Retrying is pointless -- the rank width is a property of the leased cells,
        and a rendezvous collision is a property of a placement the scheduler is
        configured never to make -- and a command that retries forever holds the
        lease, which keeps the cell draining and blocks ``eyvan down``. Settling
        here converts a permanent refusal into a terminal state with the reason
        attached, so the capacity comes back and the operator is told what to
        change.
        """

        workload_id = str(command.payload["workload_id"])
        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
            lease = await transaction.get_lease(str(lease_ids[0])) if lease_ids else None
        if workload is None or lease is None:
            raise StaleOutboxCommand("unlaunchable attempt references a missing workload or lease")
        if workload.generation != command.generation:
            raise StaleOutboxCommand(f"workload generation is {workload.generation}, command is {command.generation}")
        await self._release_terminal(
            workload,
            lease,
            WorkloadState.FAILED,
            "workload.failed",
            failure_reason=str(refusal),
        )

    async def _dispatch_workload(self, command: OutboxRecord) -> None:
        lease_ids = tuple(str(value) for value in command.payload.get("lease_ids") or ())
        if len(lease_ids) > 1:
            await self._dispatch_workload_gang(command, lease_ids)
            return
        lease_id = str(command.payload["lease_id"])
        recovered_cells = await self._recover_quarantined_dispatch_cells(command, (lease_id,))
        try:
            prepared = await self._prepare_single_dispatch(command, recovered_cells=recovered_cells)
        except PermanentDispatchRefusal as exc:
            await self._settle_unlaunchable_attempt(command, (lease_id,), exc)
            return
        if prepared is None:
            return
        _workload, _lease, cell, request, reservation = prepared
        try:
            remote = await asyncio.to_thread(self._agent_factory(cell).submit, cell.id, request)
        except Exception:
            await self._release_dispatch_reservation(command, reservation)
            raise
        await self._finalize_dispatch_response(command, reservation, remote.remote_id)

    async def _dispatch_workload_gang(self, command: OutboxRecord, lease_ids: tuple[str, ...]) -> None:
        """Launch one no-Ray native JAX job across independently leased cells."""

        recovered_cells = await self._recover_quarantined_dispatch_cells(command, lease_ids)
        try:
            prepared = await self._prepare_gang_dispatch(command, lease_ids, recovered_cells=recovered_cells)
        except PermanentDispatchRefusal as exc:
            await self._settle_unlaunchable_attempt(command, lease_ids, exc)
            return
        if prepared is None:
            return
        _workload, _members, _cells, coordinator_lease, coordinator_cell, request, reservation = prepared
        try:
            remote = await asyncio.to_thread(
                self._agent_factory(coordinator_cell).submit_gang,
                coordinator_lease.cell_id,
                request,
            )
        except Exception:
            await self._release_dispatch_reservation(command, reservation)
            raise
        await self._finalize_dispatch_response(command, reservation, remote.remote_id)

    @staticmethod
    def _validate_workload_command(
        command: OutboxRecord,
        workload: WorkloadRecord,
        lease: LeaseRecord,
        cell: CellRecord,
    ) -> None:
        if command.generation != workload.generation or lease.generation != workload.generation:
            raise StaleOutboxCommand(
                f"workload/lease generation is {workload.generation}/{lease.generation}, command is {command.generation}"
            )
        if workload.lease_id != lease.id or lease.workload_id != workload.id:
            raise StaleOutboxCommand("command lease no longer owns the workload")
        payload_cell = command.payload.get("cell_id")
        if lease.cell_id != cell.id or (payload_cell is not None and str(payload_cell) != cell.id):
            raise StaleOutboxCommand("command cell no longer owns the lease")
        if lease.cell_generation != cell.generation:
            raise StaleOutboxCommand(
                f"lease cell generation is {lease.cell_generation}, current cell generation is {cell.generation}"
            )

    async def _workload_command_state(
        self,
        command: OutboxRecord,
        *,
        workload_states: set[WorkloadState],
        lease_states: set[LeaseState],
    ):
        workload_id = str(command.payload["workload_id"])
        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
            lease = await transaction.get_lease(str(command.payload["lease_id"]))
            cell = await transaction.get_cell(lease.cell_id) if lease is not None else None
        if workload is None or lease is None or cell is None:
            raise StaleOutboxCommand("workload command references a missing workload, lease, or cell")
        self._validate_workload_command(command, workload, lease, cell)
        if workload.state not in workload_states:
            raise StaleOutboxCommand(f"workload is {workload.state.value}, expected one of {workload_states}")
        if lease.state not in lease_states:
            raise StaleOutboxCommand(f"lease is {lease.state.value}, expected one of {lease_states}")
        return workload, lease, cell

    async def _dispatch_reservation_may_have_started(self, workload_id: str) -> bool:
        """Return whether cancellation must wait for a submit outcome.

        The reservation is written before the RPC, so its presence is
        conservative: after a controller crash the agent may have accepted the
        request even when no response was recorded.  A cancel must let the
        idempotent dispatch replay recover the remote id rather than releasing
        its lease and orphaning that process.
        """

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(workload_id)
            if workload is None or workload.remote_id is not None:
                return False
            reservation = workload.dispatch_reservation
            if (
                reservation is None
                or reservation.generation != workload.generation
                or reservation.workload_attempt != workload.attempt
                or reservation.lease_ids != self._workload_lease_ids(workload)
            ):
                return False
            command = await transaction.get_outbox(reservation.outbox_id)
            return bool(
                command is not None
                and command.topic == "workload.dispatch"
                and command.aggregate_type == "workload"
                and command.aggregate_id == workload.id
                and command.generation == reservation.generation
                and command.state in _RETRIABLE_DISPATCH_OUTBOX_STATES
                and self._same_dispatch_reservation(command, reservation)
            )

    async def _dispatch_cancel(self, command: OutboxRecord) -> None:
        lease_ids = tuple(str(value) for value in command.payload.get("lease_ids") or ())
        if len(lease_ids) > 1:
            await self._dispatch_gang_cancel(command, lease_ids)
            return
        workload, lease, cell = await self._workload_command_state(
            command,
            workload_states={WorkloadState.CANCELLING},
            lease_states={LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.REVOKING, LeaseState.QUARANTINED},
        )
        remote_id = workload.remote_id or command.payload.get("remote_id")
        if not remote_id and await self._dispatch_reservation_may_have_started(workload.id):
            raise RuntimeError("workload dispatch is still resolving its remote id; cancellation will retry")
        if remote_id:
            request = WorkloadMutationRequest(mutation=self._mutation(command, lease.fencing_token))
            await asyncio.to_thread(self._agent_factory(cell).stop, cell.id, str(remote_id), request)
        await self._release_terminal(workload, lease, WorkloadState.CANCELLED, "workload.cancelled")

    async def _dispatch_gang_cancel(self, command: OutboxRecord, lease_ids: tuple[str, ...]) -> None:
        """Stop every member of a multi-cell workload before settling its leases."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(str(command.payload["workload_id"]))
            leases = tuple([await transaction.get_lease(lease_id) for lease_id in lease_ids])
            members = tuple(lease for lease in leases if lease is not None)
            if workload is None or len(members) != len(leases):
                raise StaleOutboxCommand("gang cancel command references a missing workload or lease")
            if workload.state is not WorkloadState.CANCELLING:
                raise StaleOutboxCommand(f"workload is {workload.state.value}, not cancelling")
            if self._workload_lease_ids(workload) != lease_ids or workload.lease_id != lease_ids[0]:
                raise StaleOutboxCommand("gang cancel command no longer owns the workload lease group")
            if any(lease.workload_id != workload.id or lease.generation != command.generation for lease in members):
                raise StaleOutboxCommand("gang cancel members no longer own the workload generation")
            if any(
                lease.state not in {LeaseState.PENDING, LeaseState.ACTIVE, LeaseState.REVOKING, LeaseState.QUARANTINED}
                for lease in members
            ):
                raise StaleOutboxCommand("gang cancel lease is no longer active")
            cells = tuple([await transaction.get_cell(lease.cell_id) for lease in members])
            concrete_cells = tuple(cell for cell in cells if cell is not None)
            if len(concrete_cells) != len(cells):
                raise StaleOutboxCommand("gang cancel command references a missing execution cell")
            pairs = tuple(zip(members, concrete_cells, strict=True))

        remote_id = workload.remote_id or command.payload.get("remote_id")
        if not remote_id and await self._dispatch_reservation_may_have_started(workload.id):
            raise RuntimeError("gang dispatch is still resolving its remote id; cancellation will retry")
        if remote_id:
            ordered = self._ordered_gang_member_pairs(pairs)
            request = GangWorkloadMutationRequest(
                members=self._gang_members(ordered, idempotency_prefix=command.idempotency_key)
            )
            coordinator_lease, coordinator_cell = ordered[0]
            await asyncio.to_thread(
                self._agent_factory(coordinator_cell).stop_gang,
                coordinator_lease.cell_id,
                workload.id,
                str(remote_id),
                request,
            )
        await self._release_terminal(workload, members[0], WorkloadState.CANCELLED, "workload.cancelled")

    async def _dispatch_checkpoint(self, command: OutboxRecord) -> None:
        workload, lease, cell = await self._workload_command_state(
            command,
            workload_states={WorkloadState.CHECKPOINTING},
            lease_states={LeaseState.ACTIVE, LeaseState.QUARANTINED},
        )
        hook = command.payload.get("hook")
        if not isinstance(hook, str) or not hook or not workload.remote_id:
            raise RuntimeError("checkpoint command requires a hook and active remote workload")
        output_uri = command.payload.get("output_uri")
        if output_uri is not None and not isinstance(output_uri, str):
            raise RuntimeError("checkpoint command output URI must be a string when set")
        request = CheckpointRequest(
            mutation=self._mutation(command, lease.fencing_token),
            hook=hook,
            output_uri=output_uri,
        )
        observation = await asyncio.to_thread(
            self._agent_factory(cell).checkpoint,
            cell.id,
            workload.remote_id,
            request,
        )
        await self._wait_for_checkpoint(
            self._agent_factory(cell),
            cell.id,
            observation.remote_id,
            timeout_s=self.settings.checkpoint_grace_s,
        )
        await self.service.transition_workload(
            self._principal,
            workload.id,
            WorkloadState.RUNNING,
            action="workload.checkpoint_completed",
            checkpoint_uri=observation.checkpoint_uri,
            expected_generation=command.generation,
            expected_lease_id=lease.id,
            expected_state=WorkloadState.CHECKPOINTING,
        )
