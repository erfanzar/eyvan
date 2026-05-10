# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Cell-agent service composed from independent capacity, state, and execution backends."""

from __future__ import annotations

import typing as tp
from dataclasses import replace

from fastapi import HTTPException

from eyvan._internal.json_types import JsonObject, JsonValue, model_json
from eyvan.capacity import READY_CAPACITY_STATUSES, CapacityStatus
from eyvan.capacity.drivers.base import (
    CapacityDriver,
    CellObservation,
    ExecutionDriver,
    ExecutionDriverResolver,
    ExecutionWorkloadObservation,
    ExecutionWorkloadReport,
    ExecutionWorkloadStatus,
    MutationContext,
    StaleMutationError,
    UnsupportedCapabilityError,
    canonical_execution_backend,
)
from eyvan.capacity.drivers.registry import DEFAULT_CAPACITY_BACKENDS
from eyvan.capacity.fleet_state import FleetStateStore, StaleFenceError
from eyvan.config import EyvanSettings
from eyvan.worker.agent.models import (
    AgentCapabilities,
    AgentObservation,
    CapacityRequest,
    CheckpointRequest,
    GangWorkloadMember,
    GangWorkloadMutationRequest,
    GangWorkloadRequest,
    WorkloadMutationRequest,
    WorkloadObservation,
    WorkloadRequest,
)
from eyvan.worker.runtime.registry import IntrospectableExecutionDriver, NativeGangCell

if tp.TYPE_CHECKING:
    from eyvan.worker.runtime.native_gang import NativeGangWorkloadResult

MutationRequest: tp.TypeAlias = CapacityRequest | CheckpointRequest | WorkloadMutationRequest | WorkloadRequest
GangMemberKey: tp.TypeAlias = tuple[str, str, int, str] | tuple[str, str, int, str, str]


class InvalidWorkloadRequestError(HTTPException, ValueError):  # pyright: ignore[reportUnsafeMultipleInheritance]
    """A workload request that failed validation, mapped to HTTP 422 by the agent API.

    Also subclasses :class:`ValueError` so direct in-process callers keep the
    established exception contract.

    The suppression is for the hazard this class does not have: the rule warns
    that a base initializer may go uncalled, and ``__init__`` below calls both
    explicitly. ``test_native_gang_agent`` asserts both halves of the contract --
    ``isinstance(error, ValueError)`` and ``status_code == 422`` -- on the same
    raised object, so a regression in either fails a test rather than a linter.
    """

    def __init__(self, detail: str) -> None:
        HTTPException.__init__(self, status_code=422, detail=detail)
        ValueError.__init__(self, detail)


class GangExecutionDriver(tp.Protocol):
    """Native multi-cell execution operations used by the agent."""

    def submit(self, spec: tp.Mapping[str, JsonValue]) -> str: ...

    def status(self, remote_id: str) -> NativeGangWorkloadResult: ...

    def stop(self, remote_id: str) -> None: ...

    def retire(self, remote_id: str) -> None: ...

    def logs(self, remote_id: str) -> str: ...

    def close(self) -> None: ...


@tp.runtime_checkable
class ReadinessResolver(tp.Protocol):
    """Resolver capability for probing a cell runtime."""

    def readiness(self, cell_id: str, observation: CellObservation) -> tuple[bool, str]: ...


@tp.runtime_checkable
class ClosableResolver(tp.Protocol):
    """Resolver capability for releasing every cached driver."""

    def close(self) -> None: ...


@tp.runtime_checkable
class CellClosableResolver(tp.Protocol):
    """Resolver capability for releasing one cell's cached drivers."""

    def close_cell(self, cell_id: str) -> None: ...


@tp.runtime_checkable
class GangExecutionResolver(tp.Protocol):
    """Resolver capability for constructing a native multi-cell driver."""

    def resolve_gang(self, cells: tp.Sequence[NativeGangCell]) -> GangExecutionDriver: ...


def _context(request: MutationRequest) -> MutationContext:
    mutation = request.mutation
    return MutationContext(
        idempotency_key=mutation.idempotency_key,
        generation=mutation.generation,
        fencing_token=mutation.fencing_token,
    )


def _workload_observation(report: ExecutionWorkloadReport) -> WorkloadObservation:
    """Project a typed executor report onto the stable agent wire model."""

    if isinstance(report, ExecutionWorkloadObservation):
        return WorkloadObservation(
            remote_id=report.remote_id,
            status=report.status,
            message=report.message,
        )
    return WorkloadObservation(
        remote_id=report["remote_id"],
        status=report["status"],
        message=report["message"],
    )


class CellAgentService:
    """Reconcile capacity and delegate workloads to a selected execution backend."""

    def __init__(
        self,
        capacity: CapacityDriver | None = None,
        *,
        state: FleetStateStore | None = None,
        executors: ExecutionDriverResolver | None = None,
        settings: EyvanSettings | None = None,
        remote_retire: tp.Callable[[str, str, WorkloadMutationRequest], None] | None = None,
    ):
        settings = settings or EyvanSettings.from_env()
        self.settings = settings
        self.state = state or FleetStateStore.from_uri(settings.fleet_state_uri)
        if capacity is None:
            capacity = DEFAULT_CAPACITY_BACKENDS.create(
                settings.capacity_backend,
                state=self.state,
                settings=settings,
            )
        if executors is None:
            from eyvan.worker.runtime.registry import ExecutionDriverResolver as ConfiguredExecutionResolver

            executors = ConfiguredExecutionResolver(
                capacity.capabilities().executor,
                state_dir=settings.agent_state_dir,
                ray_jobs_port=settings.ray_jobs_port,
                native_worker_port=settings.native_worker_port,
                native_worker_token=settings.native_worker_root_secret,
            )
        self.capacity = capacity
        self.execution_backend = canonical_execution_backend(capacity.capabilities().executor)
        self.executors = executors
        self._remote_retire = remote_retire or self._retire_on_source_agent

    @staticmethod
    def _observation(value: CellObservation) -> AgentObservation:
        return AgentObservation(
            cell_id=value.cell_id,
            state=value.state,
            generation=value.generation,
            execution_endpoint=value.execution_endpoint,
            detail=value.detail,
            metadata=value.metadata,
            observed_tpu_vms=value.observed_tpu_vms,
        )

    def _with_execution_readiness(self, value: CellObservation) -> CellObservation:
        if value.state not in READY_CAPACITY_STATUSES:
            return value
        if not isinstance(self.executors, ReadinessResolver):
            return value
        try:
            ready, detail = self.executors.readiness(value.cell_id, value)
        except Exception as exc:
            # A failing probe must not take down observation endpoints: report
            # the cell as still connecting so the controller keeps fencing
            # scheduling instead of seeing an opaque HTTP 500.
            return replace(
                value,
                state=CapacityStatus.EXECUTION_CONNECTING,
                detail=f"execution readiness probe failed: {exc}",
            )
        if ready:
            return value
        # Provider capacity is still alive. Keep this distinct from a provider
        # provisioning state so the controller fences scheduling without
        # recovering a workload that can still be running on the VM.
        return replace(value, state=CapacityStatus.EXECUTION_CONNECTING, detail=detail)

    def capabilities(self) -> AgentCapabilities:
        capabilities = self.capacity.capabilities()
        return AgentCapabilities(
            provider=capabilities.provider,
            executor=capabilities.executor,
            accelerator_types=capabilities.accelerator_types,
            capacity_classes=capabilities.capacity_classes,
            supports_checkpoint=capabilities.supports_checkpoint,
            supports_callables=capabilities.supports_callables,
            supports_exec=capabilities.supports_exec,
            max_gang_replicas=capabilities.max_gang_replicas,
            resource_enforcement=capabilities.resource_enforcement,
        )

    def inventory(self) -> list[dict[str, str | int | None]]:
        """Return every cell this agent holds durable state for.

        Every other route is keyed by ``cell_id``, so an agent's inventory was
        unenumerable: the only way to see what it believed it owned was to read
        its state document out of object storage by hand.

        That matters because the document outlives the control plane that wrote
        it. A control plane whose database is replaced leaves its cells behind
        with ``desired_state: up``, and the agent faithfully keeps re-ensuring
        them -- capacity nobody asked for, recreated indefinitely, invisible to
        the control plane that would otherwise own it. Found on 2026-08-01: a
        v5p-128 queued resource being resurrected from a fleet deleted days
        earlier.

        Read-only and deliberately narrow: identity, lifecycle and intent, so an
        operator can diff it against a control plane. Nothing here can mutate.
        """

        return [
            {
                "name": record.name,
                "state": str(record.state),
                "desired_state": record.desired_state,
                "generation": record.generation,
                "project": record.project,
                "zone": record.zone,
                "accelerator_type": record.accelerator_type,
                "qr_id": record.qr_id,
            }
            # Sorted here rather than relying on the storage layer: the local
            # backend happens to write with sorted keys, but that is its choice
            # and not a contract this method should inherit.
            for record in sorted(self.state.load().values(), key=lambda item: item.name)
        ]

    def observe(self, cell_id: str) -> AgentObservation:
        return self._observation(self._with_execution_readiness(self.capacity.observe(cell_id)))

    def reconcile(self, cell_id: str, request: WorkloadMutationRequest) -> AgentObservation:
        value = self.capacity.reconcile(cell_id, _context(request))
        return self._observation(self._with_execution_readiness(value))

    def apply_capacity(self, cell_id: str, request: CapacityRequest) -> AgentObservation:
        value = self.capacity.apply_capacity(cell_id, request.desired, _context(request))
        return self._observation(self._with_execution_readiness(value))

    def drain(self, cell_id: str, request: WorkloadMutationRequest) -> AgentObservation:
        return self._observation(self.capacity.drain(cell_id, _context(request)))

    def release(self, cell_id: str, request: WorkloadMutationRequest) -> AgentObservation:
        value = self.capacity.release(cell_id, _context(request))
        self._close_execution_cell(cell_id)
        return self._observation(value)

    def close(self) -> None:
        """Release execution-driver resources when the agent process stops."""

        if isinstance(self.executors, ClosableResolver):
            self.executors.close()

    def submit(self, cell_id: str, request: WorkloadRequest) -> WorkloadObservation:
        metadata = request.spec.get("metadata")
        workload_id = metadata.get("eyvan_workload_id") if isinstance(metadata, dict) else None
        if not isinstance(workload_id, str) or not workload_id.strip():
            raise InvalidWorkloadRequestError("workload metadata must include a non-empty eyvan_workload_id")
        context = _context(request)
        expected_remote_id = request.spec.get("submission_id")
        if expected_remote_id is None:
            remote_id = context.idempotency_key
        elif isinstance(expected_remote_id, str) and expected_remote_id.strip():
            remote_id = expected_remote_id.strip()
        else:
            raise InvalidWorkloadRequestError("workload submission_id must be a non-empty string")
        submitted_executor = None
        launched_remote_id: str | None = None
        try:
            with self.state.workload_submission(
                workload_id.strip(),
                cell_id,
                remote_id,
                context.generation,
                context.fencing_token,
            ) as predecessors:
                for predecessor in predecessors:
                    self._retire_predecessor(workload_id.strip(), cell_id, context, predecessor)
                    self.state.require_lease()
                self.state.clear_workload_predecessors(
                    workload_id.strip(),
                    context.generation,
                    context.fencing_token,
                )
                self.state.require_lease()
                submitted_executor = self._executor(cell_id)
                remote_id = submitted_executor.submit(request.spec, context)
                launched_remote_id = remote_id
                self.state.require_lease()
        except StaleFenceError as exc:
            if submitted_executor is not None and launched_remote_id is not None:
                try:
                    submitted_executor.retire(launched_remote_id, context)
                except BaseException as cleanup_exc:
                    raise StaleMutationError(
                        f"{exc}; launched execution {launched_remote_id!r} could not be retired after lease loss"
                    ) from cleanup_exc
            raise StaleMutationError(str(exc)) from exc
        return WorkloadObservation(remote_id=remote_id, status=ExecutionWorkloadStatus.SUBMITTED)

    @staticmethod
    def _gang_member_key(member: GangWorkloadMember, *, include_idempotency_key: bool) -> GangMemberKey:
        mutation = member.mutation
        key = (
            member.cell_id,
            member.agent_endpoint,
            mutation.generation,
            mutation.fencing_token,
        )
        return (*key, mutation.idempotency_key) if include_idempotency_key else key

    @classmethod
    def _ordered_gang_members(cls, members: tp.Sequence[GangWorkloadMember]) -> tuple[GangWorkloadMember, ...]:
        ordered = tuple(sorted(members, key=lambda member: member.cell_id))
        if len(ordered) < 2:
            raise ValueError("native workload gang requires at least two member cells")
        if len({member.cell_id for member in ordered}) != len(ordered):
            raise ValueError("native workload gang member cells must be unique")
        if len({member.agent_endpoint for member in ordered}) != 1:
            raise ValueError("native workload gang members must use the same agent_endpoint")
        generations = {member.mutation.generation for member in ordered}
        if len(generations) != 1:
            raise ValueError("native workload gang member generations must match")
        return ordered

    @staticmethod
    def _gang_workload_id(spec: tp.Mapping[str, JsonValue]) -> str:
        metadata = spec.get("metadata")
        workload_id = metadata.get("eyvan_workload_id") if isinstance(metadata, dict) else None
        if not isinstance(workload_id, str) or not workload_id.strip():
            raise InvalidWorkloadRequestError("workload metadata must include a non-empty eyvan_workload_id")
        return workload_id.strip()

    @classmethod
    def _gang_member_payload(cls, members: tp.Sequence[GangWorkloadMember]) -> tuple[JsonObject, ...]:
        return tuple(model_json(member) for member in cls._ordered_gang_members(members))

    def _gang_executor(self, members: tp.Sequence[GangWorkloadMember]) -> GangExecutionDriver:
        """Resolve direct native-worker clients for every cell in one gang."""

        if not isinstance(self.executors, GangExecutionResolver):
            raise ValueError(f"execution backend {self.execution_backend!r} does not support native workload gangs")

        cells = []
        for member in self._ordered_gang_members(members):
            observation = self.capacity.observe(member.cell_id)
            if observation.cell_id != member.cell_id:
                raise StaleMutationError(
                    f"native gang observation belongs to {observation.cell_id!r}, not member {member.cell_id!r}"
                )
            actual_endpoint = observation.metadata.get("agent_endpoint")
            if isinstance(actual_endpoint, str) and actual_endpoint.strip() and actual_endpoint != member.agent_endpoint:
                raise StaleMutationError(f"native gang member {member.cell_id!r} changed agent endpoint after placement")
            cells.append(
                NativeGangCell(
                    cell_id=member.cell_id,
                    observation=observation,
                    context=_context(WorkloadMutationRequest(mutation=member.mutation)),
                )
            )
        return self.executors.resolve_gang(tuple(cells))

    @staticmethod
    def _close_gang_executor(executor: GangExecutionDriver) -> None:
        executor.close()

    def _stored_gang_members(
        self,
        coordinator_cell_id: str,
        workload_id: str,
        remote_id: str,
    ) -> tuple[GangWorkloadMember, ...]:
        fence = self.state.workload_fence(workload_id)
        if not isinstance(fence, dict) or fence.get("kind") != "gang":
            raise StaleMutationError(f"workload {workload_id!r} is not owned by a native workload gang")
        if fence.get("coordinator_cell_id") != coordinator_cell_id or fence.get("remote_id") != remote_id:
            raise StaleMutationError(f"native workload gang no longer owns {workload_id!r}")
        raw_members = fence.get("members")
        if not isinstance(raw_members, list):
            raise StaleMutationError(f"native workload gang {workload_id!r} has invalid member state")
        try:
            members = tuple(GangWorkloadMember.model_validate(member) for member in raw_members)
            return self._ordered_gang_members(members)
        except Exception as exc:
            raise StaleMutationError(f"native workload gang {workload_id!r} has invalid member state") from exc

    @classmethod
    def _validate_gang_mutation_members(
        cls,
        stored: tp.Sequence[GangWorkloadMember],
        supplied: tp.Sequence[GangWorkloadMember],
    ) -> tuple[GangWorkloadMember, ...]:
        ordered = cls._ordered_gang_members(supplied)
        expected = tuple(cls._gang_member_key(member, include_idempotency_key=False) for member in stored)
        actual = tuple(cls._gang_member_key(member, include_idempotency_key=False) for member in ordered)
        if actual != expected:
            raise StaleMutationError("native workload gang mutation does not own every member fence")
        return ordered

    def _retire_gang_predecessor(
        self,
        workload_id: str,
        coordinator_cell_id: str,
        generation: int,
        predecessor: JsonObject,
    ) -> None:
        """Fence a predecessor gang before launching a successor generation."""

        if predecessor.get("kind") != "gang":
            mutation = MutationContext(
                idempotency_key=f"supersede:{workload_id}:{generation}",
                generation=generation,
                fencing_token=f"gang:{coordinator_cell_id}:{generation}",
            )
            self._retire_predecessor(workload_id, coordinator_cell_id, mutation, predecessor)
            return
        remote_id = predecessor.get("remote_id")
        raw_members = predecessor.get("members")
        if not isinstance(remote_id, str) or not remote_id or not isinstance(raw_members, list):
            raise StaleMutationError(f"workload {workload_id!r} has an invalid predecessor gang fence")
        try:
            prior_members = tuple(GangWorkloadMember.model_validate(member) for member in raw_members)
            prior_members = self._ordered_gang_members(prior_members)
        except Exception as exc:
            raise StaleMutationError(f"workload {workload_id!r} has an invalid predecessor gang fence") from exc
        retire_members = tuple(
            member.model_copy(
                update={
                    "mutation": member.mutation.model_copy(
                        update={"idempotency_key": f"supersede:{workload_id}:{generation}:{member.cell_id}"}
                    )
                }
            )
            for member in prior_members
        )
        executor = self._gang_executor(retire_members)
        try:
            executor.retire(remote_id)
        finally:
            self._close_gang_executor(executor)

    def submit_gang(self, coordinator_cell_id: str, request: GangWorkloadRequest) -> WorkloadObservation:
        """Submit a fenced workload across every selected native execution cell."""

        members = self._ordered_gang_members(request.members)
        if coordinator_cell_id != members[0].cell_id:
            raise InvalidWorkloadRequestError(
                "native workload gang coordinator must be the lexicographically first member cell"
            )
        workload_id = self._gang_workload_id(request.spec)
        submission_id = request.spec.get("submission_id")
        if not isinstance(submission_id, str) or not submission_id.strip():
            raise InvalidWorkloadRequestError("native workload gang requires a non-empty submission_id")
        expected_remote_id = submission_id.strip()
        member_payload = self._gang_member_payload(members)
        submitted_executor = None
        launched_remote_id: str | None = None
        try:
            with self.state.gang_workload_submission(
                workload_id,
                remote_id=expected_remote_id,
                coordinator_cell_id=coordinator_cell_id,
                members=member_payload,
            ) as predecessors:
                generation = members[0].mutation.generation
                for predecessor in predecessors:
                    self._retire_gang_predecessor(workload_id, coordinator_cell_id, generation, predecessor)
                    self.state.require_lease()
                self.state.clear_gang_workload_predecessors(workload_id, members=member_payload)
                self.state.require_lease()
                submitted_executor = self._gang_executor(members)
                remote_id = submitted_executor.submit(request.spec)
                launched_remote_id = remote_id
                if remote_id != expected_remote_id:
                    # The gang driver contract is to return the fenced remote ID
                    # or raise after retiring its partial launch, so this branch
                    # is defensive. Retire the fully launched gang before
                    # surfacing the mismatch so no rank is left running.
                    try:
                        submitted_executor.retire(remote_id)
                    except BaseException as cleanup_exc:
                        raise RuntimeError(
                            f"native workload gang returned unexpected remote ID {remote_id!r}; "
                            f"expected {expected_remote_id!r}; the launched gang could not be retired"
                        ) from cleanup_exc
                    raise RuntimeError(
                        f"native workload gang returned unexpected remote ID {remote_id!r}; "
                        f"expected {expected_remote_id!r}; the launched gang was retired"
                    )
                self.state.require_lease()
        except StaleFenceError as exc:
            if submitted_executor is not None and launched_remote_id is not None:
                try:
                    submitted_executor.retire(launched_remote_id)
                except BaseException as cleanup_exc:
                    raise StaleMutationError(
                        f"{exc}; launched gang execution {launched_remote_id!r} could not be retired after lease loss"
                    ) from cleanup_exc
            raise StaleMutationError(str(exc)) from exc
        finally:
            if submitted_executor is not None:
                self._close_gang_executor(submitted_executor)
        return WorkloadObservation(remote_id=expected_remote_id, status=ExecutionWorkloadStatus.SUBMITTED)

    def gang_workload(self, coordinator_cell_id: str, workload_id: str, remote_id: str) -> WorkloadObservation:
        """Return aggregate status for every rank in a native workload gang."""

        members = self._stored_gang_members(coordinator_cell_id, workload_id, remote_id)
        executor = self._gang_executor(members)
        try:
            return _workload_observation(executor.status(remote_id))
        finally:
            self._close_gang_executor(executor)

    def stop_gang(
        self,
        coordinator_cell_id: str,
        workload_id: str,
        remote_id: str,
        request: GangWorkloadMutationRequest,
    ) -> WorkloadObservation:
        """Stop every native rank using the member fences supplied by the controller."""

        stored = self._stored_gang_members(coordinator_cell_id, workload_id, remote_id)
        members = self._validate_gang_mutation_members(stored, request.members)
        executor = self._gang_executor(members)
        try:
            executor.stop(remote_id)
        finally:
            self._close_gang_executor(executor)
        return WorkloadObservation(remote_id=remote_id, status=ExecutionWorkloadStatus.STOPPING)

    def retire_gang(
        self,
        coordinator_cell_id: str,
        workload_id: str,
        remote_id: str,
        request: GangWorkloadMutationRequest,
    ) -> WorkloadObservation:
        """Retire every native rank using the member fences supplied by the controller."""

        stored = self._stored_gang_members(coordinator_cell_id, workload_id, remote_id)
        members = self._validate_gang_mutation_members(stored, request.members)
        executor = self._gang_executor(members)
        try:
            executor.retire(remote_id)
        finally:
            self._close_gang_executor(executor)
        return WorkloadObservation(remote_id=remote_id, status=ExecutionWorkloadStatus.STOPPED)

    def gang_logs(self, coordinator_cell_id: str, workload_id: str, remote_id: str) -> str:
        """Return rank-prefixed logs from every member of a native workload gang."""

        members = self._stored_gang_members(coordinator_cell_id, workload_id, remote_id)
        executor = self._gang_executor(members)
        try:
            return executor.logs(remote_id)
        finally:
            self._close_gang_executor(executor)

    def _executor(self, cell_id: str) -> ExecutionDriver:
        return self.executors.resolve(cell_id, self.capacity.observe(cell_id))

    def _close_execution_cell(self, cell_id: str) -> None:
        if isinstance(self.executors, CellClosableResolver):
            self.executors.close_cell(cell_id)

    def _retire_predecessor(
        self,
        workload_id: str,
        target_cell_id: str,
        context: MutationContext,
        predecessor: JsonObject,
    ) -> None:
        cell_id = predecessor.get("cell_id")
        remote_id = predecessor.get("remote_id")
        generation = predecessor.get("generation")
        fencing_token = predecessor.get("fencing_token")
        if (
            not isinstance(cell_id, str)
            or not isinstance(remote_id, str)
            or isinstance(generation, bool)
            or not isinstance(generation, int)
            or not isinstance(fencing_token, str)
        ):
            raise StaleMutationError(f"workload {workload_id!r} has an invalid predecessor fence")
        predecessor_context = MutationContext(
            idempotency_key=f"supersede:{workload_id}:{context.generation}",
            generation=generation,
            fencing_token=fencing_token,
        )
        if cell_id != target_cell_id:
            self._remote_retire(
                cell_id,
                remote_id,
                WorkloadMutationRequest(
                    mutation={
                        "idempotency_key": predecessor_context.idempotency_key,
                        "generation": predecessor_context.generation,
                        "fencing_token": predecessor_context.fencing_token,
                    }
                ),
            )
            return
        self._executor(cell_id).retire(remote_id, predecessor_context)

    def _retire_on_source_agent(
        self,
        cell_id: str,
        remote_id: str,
        request: WorkloadMutationRequest,
    ) -> None:
        observation = self.capacity.observe(cell_id)
        endpoint = observation.metadata.get("agent_endpoint")
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise RuntimeError(f"cell {cell_id!r} has no agent endpoint for remote retirement")
        from eyvan.worker.agent.client import CellAgentClient

        client = CellAgentClient(endpoint, token=self.settings.agent_shared_secret)
        try:
            client.retire(cell_id, remote_id, request)
        finally:
            client.close()

    def retire(self, cell_id: str, remote_id: str, request: WorkloadMutationRequest) -> WorkloadObservation:
        self._executor(cell_id).retire(remote_id, _context(request))
        return WorkloadObservation(remote_id=remote_id, status=ExecutionWorkloadStatus.STOPPED)

    def workload(self, cell_id: str, remote_id: str) -> WorkloadObservation:
        return _workload_observation(self._executor(cell_id).status(remote_id))

    def stop(self, cell_id: str, remote_id: str, request: WorkloadMutationRequest) -> WorkloadObservation:
        executor = self._executor(cell_id)
        executor.stop(remote_id, _context(request))
        return WorkloadObservation(remote_id=remote_id, status=ExecutionWorkloadStatus.STOPPING)

    def checkpoint(self, cell_id: str, remote_id: str, request: CheckpointRequest) -> WorkloadObservation:
        checkpoint_job_id = self._executor(cell_id).checkpoint(
            remote_id,
            hook=request.hook,
            output_uri=request.output_uri,
            context=_context(request),
        )
        return WorkloadObservation(
            remote_id=checkpoint_job_id,
            status=ExecutionWorkloadStatus.CHECKPOINTING,
            checkpoint_uri=request.output_uri,
        )

    def logs(self, cell_id: str, remote_id: str) -> str:
        return self._executor(cell_id).logs(remote_id)

    def process_status(self, cell_id: str, remote_id: str) -> JsonObject:
        """Return live process detail for one workload on one cell.

        Not every execution backend can introspect processes -- ``native-local``
        and the Ray adapter do not -- so an unsupported backend is reported as
        such rather than faked.
        """

        executor = self._executor(cell_id)
        if not isinstance(executor, IntrospectableExecutionDriver):
            raise UnsupportedCapabilityError(
                f"execution backend {self.execution_backend!r} does not expose process introspection"
            )
        return executor.process_status(remote_id)
