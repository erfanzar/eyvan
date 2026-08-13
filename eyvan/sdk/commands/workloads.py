# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Workload submission and lifecycle commands for the Eyvan CLI."""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass, field
from pathlib import Path

import click
import yaml
from pydantic import ValidationError

from eyvan._internal.json_types import JsonValue, is_json_object, model_json
from eyvan._internal.workspace import WorkspaceError, build_workspace
from eyvan.artifacts.managed import workspace_uri as managed_workspace_uri
from eyvan.capacity.autoscale import AutoscaleConfigCache, QueueAdmissionDenialRecorder
from eyvan.records import (
    DEFAULT_MAX_RETRIES,
    CapacityClass,
    CommandExecution,
    PriorityBand,
    ResourceGang,
    ResourceVector,
    WorkloadDefinition,
    WorkloadExplanation,
    WorkloadRecord,
    WorkloadState,
    WorkspaceRef,
)
from eyvan.sdk.client.idempotency import IdempotencyKeyFactory
from eyvan.sdk.client.requests import pool_workload_request
from eyvan.sdk.commands.common import JsonRenderer, parse_environment, shell_command
from eyvan.sdk.commands.state import CliSession, RunTarget, WorkloadScope
from eyvan.sdk.job_receipt import JobSubmissionReceipt, JobTerminalOutput
from eyvan.sdk.terminal import stream_is_terminal
from eyvan.server.api.schemas import (
    BatchSubmitRequest,
    DagNodeRequest,
    DagSubmitRequest,
    WorkloadEstimateRequest,
    WorkloadSubmitRequest,
)
from eyvan.server.service.batch import DEFAULT_MAX_FAN_OUT

WorkloadAction = tp.Literal["cancel", "checkpoint", "retry"]
_WORKLOAD_ACTIONS: tuple[WorkloadAction, ...] = ("cancel", "checkpoint", "retry")


@dataclass(frozen=True, slots=True)
class WorkloadCommandService:
    """Typed application service used by workload Click callbacks."""

    session: CliSession
    renderer: JsonRenderer = field(default_factory=JsonRenderer)
    keys: IdempotencyKeyFactory = field(default_factory=lambda: IdempotencyKeyFactory(prefix="cli"))

    def _available_fleets(self, scope: WorkloadScope) -> tuple[str, ...]:
        payload = self.session.client.request(
            "GET",
            "/api/v1/fleets",
            params={"org_id": scope.org_id, "project_id": scope.project_id},
            headers=self.session.workload_headers(scope),
        )
        if not isinstance(payload, list) or not all(isinstance(fleet, str) for fleet in payload):
            raise click.ClickException("control plane returned an invalid fleet-name list")
        return tuple(sorted(set(payload)))

    @staticmethod
    def _submission_id(response: JsonValue) -> str | None:
        """Return the durable workload ID from a valid JSON submission response."""

        if not is_json_object(response):
            return None
        workload_id = response.get("id")
        return workload_id if isinstance(workload_id, str) and workload_id else None

    def _emit_submission_receipt(self, response: JsonValue, *, fleet: str | None) -> None:
        """Write human guidance to stderr without changing JSON stdout contracts."""

        workload_id = self._submission_id(response)
        if workload_id is None:
            return
        JobTerminalOutput(stream=click.get_text_stream("stderr")).submission(
            JobSubmissionReceipt(workload_id=workload_id, fleet=fleet)
        )

    @staticmethod
    def _workload_records(response: JsonValue) -> tuple[WorkloadRecord, ...]:
        """Parse complete workload records without changing a raw CLI response."""

        candidates: tuple[JsonValue, ...]
        if is_json_object(response):
            candidates = (response,)
        elif isinstance(response, list):
            candidates = tuple(response)
        else:
            return ()

        records: list[WorkloadRecord] = []
        for candidate in candidates:
            if not is_json_object(candidate):
                continue
            try:
                records.append(WorkloadRecord.model_validate(candidate))
            except ValidationError:
                continue
        return tuple(records)

    def _record_queue_admission_denials(self, response: JsonValue) -> None:
        """Persist provider-proven denials without changing workload CLI output."""

        records = self._workload_records(response)
        if not records:
            return
        try:
            recorder = QueueAdmissionDenialRecorder(AutoscaleConfigCache(self.session.autoscale_cache_dir))
            for record in records:
                recorder.record_workload(record)
        except (OSError, ValueError):
            # Local eligibility caching is optional; the workload response remains authoritative.
            return

    def _emit_workload_response(self, response: JsonValue) -> None:
        """Record any durable denial before emitting the unchanged JSON response."""

        self._record_queue_admission_denials(response)
        self.renderer.emit(response)

    def default_fleet(self, scope: WorkloadScope) -> str:
        """Use the active fleet, or infer the only submit-capable fleet."""

        active = self.session.active_context
        if active is not None and active.base_url == self.session.base_url:
            return active.fleet
        fleets = self._available_fleets(scope)
        if len(fleets) == 1:
            return fleets[0]
        if not fleets:
            raise click.ClickException("no applied fleet is available; run `eyvan up` first")
        available = ", ".join(fleets)
        raise click.ClickException(
            f"multiple fleets are applied: {available}; run `eyvan use FLEET` once or use FLEET/TYPE"
        )

    def _upload_workspace(self, root: Path, scope: WorkloadScope) -> WorkspaceRef:
        """Package the working tree and store it content-addressed, once.

        The digest *is* the storage key, so re-running an unchanged tree uploads
        nothing new and two developers on the same commit share one object.
        """

        try:
            bundle = build_workspace(root)
        except WorkspaceError as exc:
            raise click.ClickException(str(exc)) from exc
        self.session.client.request(
            "PUT",
            f"/api/v1/artifacts/workspaces/{bundle.sha256}",
            params={"org_id": scope.org_id, "project_id": scope.project_id},
            content=bundle.payload,
            headers={"Content-Type": "application/gzip", "Idempotency-Key": f"workspace-{bundle.sha256}"},
        )
        click.echo(f"  workspace  {bundle.file_count} files, {bundle.size_bytes / 1e6:.1f} MB", err=True)
        return WorkspaceRef(uri=managed_workspace_uri(bundle.sha256), sha256=bundle.sha256)

    def run(
        self,
        *,
        target_value: str,
        org_id: str | None,
        project_id: str | None,
        name: str | None,
        environment: tuple[str, ...],
        max_retries: int,
        idempotency_key: str | None,
        command: tuple[str, ...],
        max_failure_retries: int = 0,
        workspace: Path | None = None,
        checkpointable: bool = False,
        checkpoint_hook: str | None = None,
        checkpoint_output_uri: str | None = None,
    ) -> None:
        """Submit one pool-targeted command using context and fleet defaults."""

        scope = self.session.workload_scope(org_id=org_id, project_id=project_id)
        workspace_ref = self._upload_workspace(workspace, scope) if workspace is not None else None
        default_fleet = None if "/" in target_value else self.default_fleet(scope)
        try:
            target = RunTarget.parse(target_value, default_fleet=default_fleet)
            request = pool_workload_request(
                org_id=scope.org_id,
                project_id=scope.project_id,
                command=shell_command(command),
                name=name,
                environment=parse_environment(environment),
                metadata=None,
                max_retries=max_retries,
                max_failure_retries=max_failure_retries,
                workspace=workspace_ref,
                checkpointable=checkpointable,
                checkpoint_hook=checkpoint_hook,
                checkpoint_output_uri=checkpoint_output_uri,
            )
        except (ValidationError, ValueError) as exc:
            raise click.ClickException(str(exc)) from exc
        response = self.session.client.request(
            "POST",
            f"/api/v1/fleets/{target.fleet}/node-types/{target.pool}/workloads",
            json=model_json(request),
            headers={
                **self.session.workload_headers(scope),
                "Idempotency-Key": self.keys.create("run", idempotency_key),
            },
        )
        self.renderer.emit(response)
        self._emit_submission_receipt(response, fleet=target.fleet)

    def submit(
        self,
        request: WorkloadSubmitRequest,
        *,
        scope: WorkloadScope,
        idempotency_key: str | None,
    ) -> None:
        """Submit one fully specified workload request."""

        response = self.session.client.request(
            "POST",
            "/api/v1/workloads",
            json=model_json(request, exclude_unset=True),
            headers={
                **self.session.workload_headers(scope),
                "Idempotency-Key": self.keys.create("workload", idempotency_key),
            },
        )
        self.renderer.emit(response)
        self._emit_submission_receipt(response, fleet=None)

    def submit_batch(
        self,
        request: BatchSubmitRequest,
        *,
        scope: WorkloadScope,
        idempotency_key: str | None,
    ) -> None:
        """Fan one template out into an array of ordinary workloads."""

        response = self.session.client.request(
            "POST",
            "/api/v1/batches",
            json=model_json(request, exclude_unset=True),
            headers={
                **self.session.workload_headers(scope),
                "Idempotency-Key": self.keys.create("batch", idempotency_key),
            },
        )
        self.renderer.emit(response)

    def submit_dag(
        self,
        request: DagSubmitRequest,
        *,
        scope: WorkloadScope,
        idempotency_key: str | None,
    ) -> None:
        """Submit a pipeline of dependent workloads in one transaction."""

        response = self.session.client.request(
            "POST",
            "/api/v1/dags",
            json=model_json(request, exclude_unset=True),
            headers={
                **self.session.workload_headers(scope),
                "Idempotency-Key": self.keys.create("dag", idempotency_key),
            },
        )
        self.renderer.emit(response)

    def list(self, scope: WorkloadScope, state: WorkloadState | None) -> None:
        """List workload records and retain any durable queue-admission denials."""

        params: dict[str, str] = {"org_id": scope.org_id, "project_id": scope.project_id}
        if state is not None:
            params["state"] = state.value
        response = self.session.client.request(
            "GET",
            "/api/v1/workloads",
            params=params,
            headers=self.session.workload_headers(scope),
        )
        self._emit_workload_response(response)

    def show(self, workload_id: str) -> None:
        """Display one workload record and retain any durable queue-admission denial."""

        self._emit_workload_response(self.session.client.request("GET", f"/api/v1/workloads/{workload_id}"))

    def process_status(self, workload_id: str) -> None:
        """Emit live process detail for one workload attempt."""

        self.renderer.emit(self.session.client.request("GET", f"/api/v1/workloads/{workload_id}/process-status"))

    def explain(self, workload_id: str) -> None:
        """Emit why one workload is or is not currently placeable."""

        response = self.session.client.request("GET", f"/api/v1/workloads/{workload_id}/explain")
        self._emit_explanation_summary(response)
        self.renderer.emit(response)

    @staticmethod
    def _emit_explanation_summary(response: JsonValue) -> None:
        """Write a compact human summary to stderr, leaving JSON stdout unchanged."""

        stream = click.get_text_stream("stderr")
        if not stream_is_terminal(stream) or not is_json_object(response):
            return
        try:
            explanation = WorkloadExplanation.model_validate(response)
        except ValidationError:
            return
        verdict = "placeable now" if explanation.placeable else "not placeable"
        lines = [f"Eyvan: {explanation.workload_id} is {explanation.state.value}", f"  verdict     {verdict}"]
        if explanation.placement_cell_id is not None:
            lines.append(f"  cell        {explanation.placement_cell_id}")
        if explanation.message:
            prefix = f"{explanation.code.value}: " if explanation.code is not None else ""
            lines.append(f"  reason      {prefix}{explanation.message}")
        if explanation.considered_cell_count:
            shown = len(explanation.cells)
            omitted = explanation.truncated_cell_count
            summary = f"{explanation.considered_cell_count} rejected"
            if omitted:
                summary = f"{summary} (showing {shown}, {omitted} not shown)"
            lines.append(f"  cells       {summary}")
            lines.extend(f"    {cell.cell_id}  {cell.code.value}  {cell.message}" for cell in explanation.cells)
        if explanation.autoscale_block_reason is not None:
            lines.append(f"  autoscale   {explanation.autoscale_block_reason.value}")
        if explanation.queue_admission_denial is not None:
            lines.append(f"  queue       admission denied for {explanation.queue_admission_denial}")
        click.echo("\n".join(lines), file=stream)

    def mutate(self, workload_id: str, action: WorkloadAction, idempotency_key: str | None) -> None:
        """Apply one workload lifecycle mutation."""

        response = self.session.client.request(
            "POST",
            f"/api/v1/workloads/{workload_id}/{action}",
            headers={"Idempotency-Key": self.keys.create(action, idempotency_key)},
        )
        self.renderer.emit(response)


@click.command("run")
@click.argument("target")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--name")
@click.option("--env", "environment", multiple=True, metavar="KEY=VALUE")
@click.option("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, show_default=True)
@click.option(
    "--max-failure-retries",
    type=int,
    default=0,
    show_default=True,
    help="Retries for the job's own non-zero exit. Separate from --max-retries, which pays for capacity the fleet lost.",
)
@click.option("--idempotency-key")
@click.option(
    "--workspace",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    is_flag=False,
    flag_value=".",
    default=None,
    help="Upload this directory (respecting .gitignore) and run the command inside it. Bare --workspace means '.'.",
)
@click.option(
    "--checkpointable",
    is_flag=True,
    default=False,
    help="This job can be checkpointed. Pair it with --checkpoint-hook: the scheduler only checkpoints on "
    "preemption when both are set, so on spot this is the difference between resuming and restarting from zero.",
)
@click.option("--checkpoint-hook", help="Command run on the cell to write a checkpoint before the job is stopped.")
@click.option("--checkpoint-output-uri", help="Where the hook writes, normally a gs:// prefix.")
@click.argument("command", nargs=-1, required=True, type=click.UNPROCESSED)
@click.pass_obj
def run_workload(
    session: CliSession,
    target: str,
    org_id: str | None,
    project_id: str | None,
    name: str | None,
    environment: tuple[str, ...],
    max_retries: int,
    max_failure_retries: int,
    idempotency_key: str | None,
    workspace: Path | None,
    checkpointable: bool,
    checkpoint_hook: str | None,
    checkpoint_output_uri: str | None,
    command: tuple[str, ...],
) -> None:
    """Queue COMMAND on TYPE or FLEET/TYPE using applied fleet defaults."""

    WorkloadCommandService(session).run(
        target_value=target,
        org_id=org_id,
        project_id=project_id,
        name=name,
        environment=environment,
        max_retries=max_retries,
        max_failure_retries=max_failure_retries,
        idempotency_key=idempotency_key,
        command=command,
        workspace=workspace,
        checkpointable=checkpointable,
        checkpoint_hook=checkpoint_hook,
        checkpoint_output_uri=checkpoint_output_uri,
    )


@click.command("exec")
@click.argument("workload_id")
@click.option("--rank", type=int, default=None, help="Run on one rank only. Omitted runs on every rank.")
@click.option("--timeout", "timeout_s", type=float, default=30.0, show_default=True)
@click.argument("command", nargs=-1, required=True, type=click.UNPROCESSED)
@click.pass_obj
def exec_in_workload(
    session: CliSession,
    workload_id: str,
    rank: int | None,
    timeout_s: float,
    command: tuple[str, ...],
) -> None:
    """Run COMMAND beside a running workload, on every rank by default.

    Beside it, not inside its cgroup: a process there competes for the memory
    the trainer is sized against, and its death can take the trainer with it.
    Every rank by default because the question this answers is usually "which
    rank is stuck", and one rank cannot distinguish a hung collective from a
    slow step.
    """

    WorkloadCommandService(session).renderer.emit(
        session.client.request(
            "POST",
            f"/api/v1/workloads/{workload_id}/exec",
            json={"command": list(command), "timeout_s": timeout_s, "rank": rank},
        )
    )


@click.group("workload")
def workload() -> None:
    """Submit and operate whole workloads."""


@workload.command("children")
@click.argument("workload_id")
@click.pass_obj
def list_children(session: CliSession, workload_id: str) -> None:
    """List the workloads one workload submitted or fanned out to.

    The only view of a job tree. Without it a batch, a DAG or an in-job fan-out
    is a parent id and a guess: every child is an ordinary workload in the
    listing, indistinguishable from work a human submitted.
    """

    JsonRenderer().emit(session.client.request("GET", f"/api/v1/workloads/{workload_id}/children"))


@workload.command("endpoints")
@click.argument("workload_id")
@click.pass_obj
def list_endpoints(session: CliSession, workload_id: str) -> None:
    """List the addresses a service's replicas registered.

    Registration is the readiness signal -- an app says it can serve *after* it
    can -- so this is also the answer to "is the service actually up", which no
    other view gives.
    """

    JsonRenderer().emit(session.client.request("GET", f"/api/v1/workloads/{workload_id}/endpoints"))


@workload.command("log-page")
@click.argument("workload_id")
@click.option("--after", default="", help="Resume cursor from a previous page.")
@click.option("--limit", type=int, default=200, show_default=True)
@click.pass_obj
def log_page(session: CliSession, workload_id: str, after: str, limit: int) -> None:
    """Read one structured, per-rank page of a workload's logs.

    Distinct from ``workload logs``, which returns concatenated text with rank
    banners. This keeps records attributed and resumable, which is what a
    multi-host job needs and what a banner cannot express.
    """

    JsonRenderer().emit(
        session.client.request(
            "GET",
            f"/api/v1/workloads/{workload_id}/log-page",
            params={"after": after, "limit": str(limit)},
        )
    )


@workload.command("estimate")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--name", default="estimate", show_default=True)
@click.option("--command", default="true", show_default=True, help="Not run; a spec needs one to validate.")
@click.option("--cpu", type=float, default=1.0, show_default=True)
@click.option("--memory-gib", type=float, default=1.0, show_default=True)
@click.option("--gpu", type=int, default=0, show_default=True)
@click.option("--tpu", type=int, default=0, show_default=True)
@click.option("--replicas", type=int, default=1, show_default=True)
@click.option(
    "--capacity-class",
    type=click.Choice(tuple(capacity.value for capacity in CapacityClass)),
    default=CapacityClass.ON_DEMAND.value,
    show_default=True,
)
@click.option("--accelerator-type")
@click.option("--provider")
@click.option("--region")
@click.option(
    "--duration-s",
    "estimated_duration_seconds",
    type=int,
    default=3600,
    show_default=True,
    help="How long the job is expected to run. Every figure is an estimate over this number, so the answer is only "
    "as honest as it is.",
)
@click.pass_obj
def estimate_workload(
    session: CliSession,
    org_id: str | None,
    project_id: str | None,
    name: str,
    command: str,
    cpu: float,
    memory_gib: float,
    gpu: int,
    tpu: int,
    replicas: int,
    capacity_class: str,
    accelerator_type: str | None,
    provider: str | None,
    region: str | None,
    estimated_duration_seconds: int,
) -> None:
    """Ask what a workload would cost before submitting it.

    The route has existed since budgets did; nothing outside the raw HTTP API
    could reach it, so the question it answers still required submitting a job
    and reading the resulting lease.

    Not a scheduling dry run. It consults declared pool attributes and prices,
    never live cell availability -- an estimate that claimed to predict placement
    would be wrong the moment the fleet moved.
    """

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    try:
        request = WorkloadEstimateRequest(
            org_id=scope.org_id,
            project_id=scope.project_id,
            spec=WorkloadDefinition(
                name=name,
                execution=CommandExecution(kind="command", command=command),
                gang=ResourceGang(
                    replicas=replicas,
                    per_replica=ResourceVector(cpu=cpu, memory_gib=memory_gib, gpu=gpu, tpu=tpu),
                ),
                capacity_class=CapacityClass(capacity_class),
                accelerator_type=accelerator_type,
                provider=provider,
                region=region,
                estimated_duration_seconds=estimated_duration_seconds,
            ),
        )
    except (ValidationError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    JsonRenderer().emit(
        session.client.request(
            "POST",
            "/api/v1/workloads/estimate",
            json=model_json(request),
            headers=session.workload_headers(scope),
        )
    )


@workload.command("submit")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--name", required=True)
@click.option("--command", required=True)
@click.option("--cpu", type=float, default=1.0, show_default=True)
@click.option("--memory-gib", type=float, default=1.0, show_default=True)
@click.option("--gpu", type=int, default=0, show_default=True)
@click.option("--tpu", type=int, default=0, show_default=True)
@click.option("--replicas", type=int, default=1, show_default=True)
@click.option(
    "--capacity-class",
    type=click.Choice(tuple(capacity.value for capacity in CapacityClass)),
    default=CapacityClass.ON_DEMAND.value,
    show_default=True,
)
@click.option("--accelerator-type")
@click.option("--provider")
@click.option("--region")
@click.option(
    "--additional-capacity-class",
    "additional_capacity_classes",
    multiple=True,
    type=click.Choice(tuple(capacity.value for capacity in CapacityClass)),
    help="Further capacity classes this job will accept, in preference order after --capacity-class. This is how a "
    "spot-first job says it would rather fall back than wait: the scheduler has always ranked a portfolio.",
)
@click.option(
    "--allowed-region",
    "allowed_regions",
    multiple=True,
    help="Regions this job will accept. Mutually exclusive with --region, which pins one.",
)
@click.option(
    "--allowed-zone",
    "allowed_zones",
    multiple=True,
    help="Zones this job will accept. Same rule as --allowed-region.",
)
@click.option(
    "--allowed-accelerator-type",
    "allowed_accelerator_types",
    multiple=True,
    help="Accelerator types this job will accept. Mutually exclusive with --accelerator-type.",
)
@click.option(
    "--named-asset",
    "named_assets",
    multiple=True,
    help="Logical names of large inputs this job pulls -- a dataset URI, a checkpoint, an image. Only a hint: cells "
    "that recently ran something naming the same asset are preferred, so a stale guess costs one pull.",
)
@click.option("--required-executor")
@click.option(
    "--priority-band",
    type=click.Choice(tuple(band.value for band in PriorityBand)),
    default=PriorityBand.INTERACTIVE.value,
    show_default=True,
    help="Scheduling band. A higher band may preempt a lower one for its capacity.",
)
@click.option(
    "--intra-band-priority",
    type=int,
    default=0,
    show_default=True,
    help="Rank within the band; higher wins.",
)
@click.option("--checkpointable", is_flag=True)
@click.option("--checkpoint-hook")
@click.option("--checkpoint-output-uri")
@click.option("--max-retries", type=int, default=0, show_default=True)
@click.option(
    "--max-failure-retries",
    type=int,
    default=0,
    show_default=True,
    help="Retries for the job's own non-zero exit. Separate from --max-retries, which pays for capacity the fleet lost.",
)
@click.option(
    "--max-preemption-retries",
    type=int,
    default=10,
    show_default=True,
    help="Restarts allowed after the provider reclaims the capacity. Separate from --max-retries because a spot "
    "reclamation is expected and cheap to retry, while a stockout means the pool is empty.",
)
@click.option(
    "--secret",
    "secret_mounts",
    multiple=True,
    metavar="NAME=REFERENCE",
    help=(
        "Mount a secret as an environment variable, by reference. The reference is resolved at dispatch and the "
        "value is never stored in the spec -- unlike --env, which is persisted. The project policy's "
        "allowed_secret_references must permit the reference."
    ),
)
@click.option(
    "--can-submit-children",
    is_flag=True,
    default=False,
    help=(
        "Hand this job a capability to submit children of its own. Children run under this workload's owner "
        "rather than the workload, inherit its scope and quota, are cancelled with it, and are capped at 256 "
        "live at once. Off by default: a credential nobody asked for is one nobody is watching."
    ),
)
@click.option("--idempotency-key")
@click.pass_obj
def submit_workload(
    session: CliSession,
    org_id: str | None,
    project_id: str | None,
    name: str,
    command: str,
    cpu: float,
    memory_gib: float,
    gpu: int,
    tpu: int,
    replicas: int,
    capacity_class: str,
    accelerator_type: str | None,
    provider: str | None,
    region: str | None,
    additional_capacity_classes: tuple[str, ...],
    allowed_regions: tuple[str, ...],
    allowed_zones: tuple[str, ...],
    allowed_accelerator_types: tuple[str, ...],
    named_assets: tuple[str, ...],
    required_executor: str | None,
    priority_band: str,
    intra_band_priority: int,
    checkpointable: bool,
    checkpoint_hook: str | None,
    checkpoint_output_uri: str | None,
    max_retries: int,
    max_failure_retries: int,
    max_preemption_retries: int,
    secret_mounts: tuple[str, ...],
    can_submit_children: bool,
    idempotency_key: str | None,
) -> None:
    """Submit a workload with explicit resource and placement intent."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    mounts = parse_environment(secret_mounts, option="--secret")
    try:
        request = WorkloadSubmitRequest(
            org_id=scope.org_id,
            project_id=scope.project_id,
            spec=WorkloadDefinition(
                name=name,
                execution=CommandExecution(kind="command", command=command),
                gang=ResourceGang(
                    replicas=replicas,
                    per_replica=ResourceVector(cpu=cpu, memory_gib=memory_gib, gpu=gpu, tpu=tpu),
                ),
                capacity_class=CapacityClass(capacity_class),
                additional_capacity_classes=tuple(CapacityClass(value) for value in additional_capacity_classes),
                accelerator_type=accelerator_type,
                provider=provider,
                region=region,
                allowed_regions=allowed_regions,
                allowed_zones=allowed_zones,
                allowed_accelerator_types=allowed_accelerator_types,
                named_assets=named_assets,
                required_executor=required_executor,
                priority_band=PriorityBand(priority_band),
                intra_band_priority=intra_band_priority,
                checkpointable=checkpointable,
                checkpoint_hook=checkpoint_hook,
                checkpoint_output_uri=checkpoint_output_uri,
                max_retries=max_retries,
                max_failure_retries=max_failure_retries,
                max_preemption_retries=max_preemption_retries,
                secret_mounts=mounts,
                can_submit_children=can_submit_children,
            ),
        )
    except (ValidationError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    WorkloadCommandService(session).submit(request, scope=scope, idempotency_key=idempotency_key)


@workload.command("submit-batch")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--name", required=True)
@click.option("--command", required=True)
@click.option("--count", type=int, required=True, help="Number of tasks to create.")
@click.option("--name-prefix", default="", help="Task names become {prefix}-{index}; defaults to --name.")
@click.option("--cpu", type=float, default=0.0, show_default=True)
@click.option("--memory-gib", type=float, default=0.0, show_default=True)
@click.option("--gpu", type=int, default=0, show_default=True)
@click.option("--tpu", type=int, default=0, show_default=True)
@click.option("--replicas", type=int, default=1, show_default=True)
@click.option(
    "--capacity-class",
    type=click.Choice(tuple(capacity.value for capacity in CapacityClass)),
    default=CapacityClass.ON_DEMAND.value,
    show_default=True,
)
@click.option("--accelerator-type")
@click.option("--max-fan-out", type=int, default=DEFAULT_MAX_FAN_OUT, show_default=True)
@click.option("--idempotency-key")
@click.pass_obj
def submit_batch(
    session: CliSession,
    org_id: str | None,
    project_id: str | None,
    name: str,
    command: str,
    count: int,
    name_prefix: str,
    cpu: float,
    memory_gib: float,
    gpu: int,
    tpu: int,
    replicas: int,
    capacity_class: str,
    accelerator_type: str | None,
    max_fan_out: int,
    idempotency_key: str | None,
) -> None:
    """Submit an array of N identical tasks, each with EYVAN_BATCH_INDEX set.

    The whole array commits in one transaction. The returned parent is a
    coordinator that is never scheduled; cancel it to cancel every task.
    """

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    try:
        request = BatchSubmitRequest(
            org_id=scope.org_id,
            project_id=scope.project_id,
            template=WorkloadDefinition(
                name=name,
                execution=CommandExecution(kind="command", command=command),
                gang=ResourceGang(
                    replicas=replicas,
                    per_replica=ResourceVector(cpu=cpu, memory_gib=memory_gib, gpu=gpu, tpu=tpu),
                ),
                capacity_class=CapacityClass(capacity_class),
                accelerator_type=accelerator_type,
            ),
            count=count,
            name_prefix=name_prefix,
            max_fan_out=max_fan_out,
        )
    except (ValidationError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    WorkloadCommandService(session).submit_batch(request, scope=scope, idempotency_key=idempotency_key)


def _parse_dag_nodes(document: object) -> tuple[DagNodeRequest, ...]:
    """Build node requests from a parsed pipeline document.

    Structured input gets a file rather than repeated flags: a DAG is a set of
    edges, and every flag syntax that encodes edges is harder to read than the
    YAML it replaces.
    """

    if not isinstance(document, dict) or not isinstance(document.get("nodes"), list):
        raise click.ClickException("pipeline file must be a mapping with a 'nodes' list")
    nodes: list[DagNodeRequest] = []
    for index, entry in enumerate(document["nodes"]):
        if not isinstance(entry, dict):
            raise click.ClickException(f"pipeline node {index} must be a mapping")
        name = entry.get("name")
        command = entry.get("command")
        if not isinstance(name, str) or not isinstance(command, str):
            raise click.ClickException(f"pipeline node {index} needs string 'name' and 'command' fields")
        depends_on = entry.get("depends_on", [])
        if not isinstance(depends_on, list) or not all(isinstance(item, str) for item in depends_on):
            raise click.ClickException(f"pipeline node {name!r}: 'depends_on' must be a list of node names")
        resources = entry.get("resources", {})
        if not isinstance(resources, dict):
            raise click.ClickException(f"pipeline node {name!r}: 'resources' must be a mapping")
        try:
            nodes.append(
                DagNodeRequest(
                    name=name,
                    template=WorkloadDefinition(
                        name=name,
                        execution=CommandExecution(kind="command", command=command),
                        gang=ResourceGang(
                            replicas=int(entry.get("replicas", 1)),
                            per_replica=ResourceVector(
                                cpu=float(resources.get("cpu", 0.0)),
                                memory_gib=float(resources.get("memory_gib", 0.0)),
                                gpu=int(resources.get("gpu", 0)),
                                tpu=int(resources.get("tpu", 0)),
                            ),
                        ),
                        capacity_class=CapacityClass(entry.get("capacity_class", CapacityClass.ON_DEMAND.value)),
                        accelerator_type=entry.get("accelerator_type"),
                    ),
                    depends_on=tuple(depends_on),
                )
            )
        except (ValidationError, ValueError) as exc:
            raise click.ClickException(f"pipeline node {name!r}: {exc}") from exc
    return tuple(nodes)


@workload.command("submit-dag")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option(
    "--file",
    "pipeline_file",
    required=True,
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="YAML or JSON pipeline document.",
)
@click.option("--name-prefix", default="", help="Node names become {prefix}-{node}.")
@click.option("--max-fan-out", type=int, default=DEFAULT_MAX_FAN_OUT, show_default=True)
@click.option("--idempotency-key")
@click.pass_obj
def submit_dag(
    session: CliSession,
    org_id: str | None,
    project_id: str | None,
    pipeline_file: str,
    name_prefix: str,
    max_fan_out: int,
    idempotency_key: str | None,
) -> None:
    """Submit a pipeline of dependent workloads from a file, in one transaction.

    \b
    nodes:
      - name: prepare
        command: python prepare.py
      - name: train
        command: python train.py
        depends_on: [prepare]
        replicas: 4
        accelerator_type: v5p-16
        resources: {tpu: 4}

    A node runs only after every node it depends on has SUCCEEDED. If one fails
    or is cancelled, its dependents are cancelled rather than left waiting.
    Dependencies order execution only -- results travel as eyvan:// URIs.
    """

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    try:
        with Path(pipeline_file).expanduser().open(encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
    except (OSError, yaml.YAMLError) as exc:
        raise click.ClickException(f"could not read {pipeline_file}: {exc}") from exc
    try:
        request = DagSubmitRequest(
            org_id=scope.org_id,
            project_id=scope.project_id,
            nodes=_parse_dag_nodes(document),
            name_prefix=name_prefix,
            max_fan_out=max_fan_out,
        )
    except (ValidationError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    WorkloadCommandService(session).submit_dag(request, scope=scope, idempotency_key=idempotency_key)


@workload.command("list")
@click.option("--org", "org_id")
@click.option("--project", "project_id")
@click.option("--state", type=click.Choice(tuple(state.value for state in WorkloadState)))
@click.pass_obj
def list_workloads(
    session: CliSession,
    org_id: str | None,
    project_id: str | None,
    state: str | None,
) -> None:
    """List workloads in a project, optionally filtered by lifecycle state."""

    scope = session.workload_scope(org_id=org_id, project_id=project_id)
    WorkloadCommandService(session).list(scope, None if state is None else WorkloadState(state))


@workload.command("get")
@click.argument("workload_id")
@click.pass_obj
def get_workload(session: CliSession, workload_id: str) -> None:
    """Get one workload record."""

    WorkloadCommandService(session).show(workload_id)


@workload.command("status")
@click.argument("workload_id")
@click.pass_obj
def workload_status(session: CliSession, workload_id: str) -> None:
    """Get the durable lifecycle state of one workload."""

    WorkloadCommandService(session).show(workload_id)


@workload.command("explain")
@click.argument("workload_id")
@click.pass_obj
def workload_explain(session: CliSession, workload_id: str) -> None:
    """Explain why one workload is or is not currently placeable."""

    WorkloadCommandService(session).explain(workload_id)


@workload.command("process-status")
@click.argument("workload_id")
@click.pass_obj
def workload_process_status(session: CliSession, workload_id: str) -> None:
    """Show live process detail for a workload attempt.

    Use this when a job is RUNNING but not progressing: it reports each rank's
    systemd state and cgroup process list, which distinguishes a healthy step
    loop from a hung collective.
    """

    WorkloadCommandService(session).process_status(workload_id)


@workload.command("logs")
@click.argument("workload_id")
@click.pass_obj
def workload_logs(session: CliSession, workload_id: str) -> None:
    """Print driver logs for a workload attempt."""

    click.echo(session.client.request_text("GET", f"/api/v1/workloads/{workload_id}/logs"))


def _workload_action_command(action: WorkloadAction) -> click.Command:
    @click.command(action)
    @click.argument("workload_id")
    @click.option("--idempotency-key")
    @click.pass_obj
    def command(session: CliSession, workload_id: str, idempotency_key: str | None) -> None:
        WorkloadCommandService(session).mutate(workload_id, action, idempotency_key)

    return command


for _action in _WORKLOAD_ACTIONS:
    workload.add_command(_workload_action_command(_action))

workload.add_command(exec_in_workload)


def register_workload_commands(group: click.Group) -> None:
    """Attach workload commands to the root command group."""

    group.add_command(run_workload)
    group.add_command(workload)


__all__ = (
    "WorkloadCommandService",
    "register_workload_commands",
    "run_workload",
    "workload",
    "workload_explain",
    "workload_process_status",
    "workload_status",
)
