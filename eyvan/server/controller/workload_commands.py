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

from eyvan.capacity import (
    READY_CAPACITY_STATUSES,
)
from eyvan.records import (
    AuditEvent,
    CellRecord,
    CellState,
    LeaseRecord,
    LeaseState,
    OutboxRecord,
    WorkloadRecord,
    WorkloadState,
    WorkspaceRef,
    utcnow,
)
from eyvan.server.controller._support import (
    StaleOutboxCommand,
)
from eyvan.worker.agent.models import (
    CheckpointRequest,
    GangWorkloadMember,
    GangWorkloadMutationRequest,
    GangWorkloadRequest,
    MutationModel,
    WorkloadMutationRequest,
    WorkloadRequest,
)


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

    def _children_url(self, workload: WorkloadRecord) -> str:
        """Return the address a job posts its children to.

        Built here for the same reason the progress URL is: a job should receive
        a complete address rather than a base URL plus the knowledge of how to
        assemble one, which would be a second place for the route to be spelled.
        """

        base = "" if self.artifact_access is None else self.artifact_access.public_url
        return f"{base}/api/v1/workloads/{workload.id}/children"

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
            if not self.settings.dev_auth_enabled:
                raise RuntimeError("managed artifact transfer requires an HTTPS EYVAN_PUBLIC_URL")
            environment["EYVAN_ALLOW_INSECURE_ARTIFACT_HTTP"] = "1"
        return shlex.join(parts), environment

    async def _dispatch_workload(self, command: OutboxRecord) -> None:
        lease_ids = tuple(str(value) for value in command.payload.get("lease_ids") or ())
        if len(lease_ids) > 1:
            await self._dispatch_workload_gang(command, lease_ids)
            return
        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
            lease = await transaction.get_lease(str(command.payload["lease_id"]))
            cell = await transaction.get_cell(str(command.payload["cell_id"]))
            if workload is None or lease is None or cell is None:
                raise StaleOutboxCommand("dispatch command references a missing workload, lease, or cell")
            self._validate_workload_command(command, workload, lease, cell)
            if workload.state is not WorkloadState.LEASED:
                raise StaleOutboxCommand(f"workload is {workload.state.value}, not leased")
            if lease.state not in {LeaseState.PENDING, LeaseState.QUARANTINED}:
                raise StaleOutboxCommand(f"lease is {lease.state.value}, not pending")
            if cell.state is not CellState.READY:
                raise RuntimeError(f"cell {cell.id!r} is {cell.state.value}, not ready for dispatch")
            if lease.state is LeaseState.QUARANTINED:
                observation = await asyncio.to_thread(self._agent_factory(cell).observe, cell.id)
                if observation.state not in READY_CAPACITY_STATUSES:
                    raise RuntimeError(f"cell {cell.id!r} has not recovered the quarantined lease")

            entrypoint, environment = self._render_execution(workload, runtime_environment=cell.runtime.env)
            request = WorkloadRequest(
                mutation=self._mutation(command, lease.fencing_token),
                spec={
                    "entrypoint": entrypoint,
                    "submission_id": f"{workload.id}-a{workload.attempt}-g{workload.generation}",
                    "environment": environment,
                    "backend_options": workload.spec.backend_options,
                    "gang": {
                        **workload.spec.gang.model_dump(mode="json"),
                        "placement_strategy": "PACK",
                    },
                    "metadata": {
                        **workload.spec.metadata,
                        "eyvan_workload_id": workload.id,
                        "eyvan_org_id": workload.org_id,
                        "eyvan_project_id": workload.project_id,
                        "eyvan_lease_id": lease.id,
                    },
                },
            )
            remote = await asyncio.to_thread(self._agent_factory(cell).submit, cell.id, request)
            now = utcnow()
            active_lease = lease.model_copy(
                update={
                    "state": LeaseState.ACTIVE,
                    "expires_at": now + timedelta(seconds=self.settings.lease_ttl_s),
                    # Write-once: a lease re-entering ACTIVE after a quarantine
                    # recovery is the same run, so its start must not move.
                    "run_started_at": lease.run_started_at or now,
                    "updated_at": now,
                }
            )
            dispatched = workload.model_copy(
                update={"state": WorkloadState.DISPATCHING, "remote_id": remote.remote_id, "updated_at": now}
            )
            await transaction.put_lease(active_lease)
            await transaction.put_workload(dispatched)
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
                    new_state=active_lease.state.value,
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
                    payload={"remote_id": remote.remote_id},
                )
            )

    async def _dispatch_workload_gang(self, command: OutboxRecord, lease_ids: tuple[str, ...]) -> None:
        """Launch one no-Ray native JAX job across independently leased cells."""

        async with self.repository.transaction() as transaction:
            workload = await transaction.get_workload(command.aggregate_id)
            leases = tuple([await transaction.get_lease(lease_id) for lease_id in lease_ids])
            members = tuple(lease for lease in leases if lease is not None)
            if workload is None or len(members) != len(leases):
                raise StaleOutboxCommand("gang dispatch command references a missing workload or lease")
            if workload.state is not WorkloadState.LEASED:
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
            if workload.spec.required_executor != "eyvan-native":
                raise RuntimeError("multi-cell pod groups currently require the eyvan-native executor")
            endpoints = {cell.endpoint for cell in concrete_cells}
            if None in endpoints or len(endpoints) != 1:
                raise RuntimeError("native pod group cells must use the same trusted cell-agent endpoint")

            leader_lease = members[0]
            leader_cell = concrete_cells[0]
            entrypoint, environment = self._render_execution(workload, runtime_environment=leader_cell.runtime.env)

            spec = {
                "entrypoint": entrypoint,
                "submission_id": f"{workload.id}-a{workload.attempt}-g{workload.generation}",
                "environment": environment,
                "backend_options": workload.spec.backend_options,
                "gang": {
                    **workload.spec.gang.model_dump(mode="json"),
                    "placement_strategy": "PACK",
                },
                "metadata": {
                    **workload.spec.metadata,
                    "eyvan_workload_id": workload.id,
                    "eyvan_org_id": workload.org_id,
                    "eyvan_project_id": workload.project_id,
                    "eyvan_lease_id": leader_lease.id,
                    "eyvan_lease_ids": ",".join(lease.id for lease in members),
                },
            }
            paired = tuple(sorted(zip(members, concrete_cells, strict=True), key=lambda item: item[0].cell_id))
            coordinator_lease, coordinator_cell = paired[0]
            gang_request = GangWorkloadRequest(
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
            remote = await asyncio.to_thread(
                self._agent_factory(coordinator_cell).submit_gang,
                coordinator_lease.cell_id,
                gang_request,
            )
            now = utcnow()
            for lease in members:
                await transaction.put_lease(
                    lease.model_copy(
                        update={
                            "state": LeaseState.ACTIVE,
                            "expires_at": now + timedelta(seconds=self.settings.lease_ttl_s),
                            "run_started_at": lease.run_started_at or now,
                            "updated_at": now,
                        }
                    )
                )
            dispatched = workload.model_copy(
                update={"state": WorkloadState.DISPATCHING, "remote_id": remote.remote_id, "updated_at": now}
            )
            await transaction.put_workload(dispatched)
            for lease in members:
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
                        new_state=LeaseState.ACTIVE.value,
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
                    payload={"remote_id": remote.remote_id, "lease_ids": list(lease_ids)},
                )
            )

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
