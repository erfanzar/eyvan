# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Fleet apply, teardown, status, and validation commands."""

from __future__ import annotations

import json
import subprocess
import typing as tp
from dataclasses import dataclass, field
from pathlib import Path

import click
from pydantic import ValidationError

import eyvan.sdk.commands.autoscale as autoscale_commands
import eyvan.sdk.commands.fleet_status as fleet_status_rendering
from eyvan._internal.json_types import JsonObject, model_json
from eyvan.capacity.autoscale import AutoscaleConfigCache, CachedAutoscaleConfig, QueueAdmissionDenialRecorder
from eyvan.capacity.fleet_config import EyvanFleetConfig, validate_fleet_name
from eyvan.capacity.providers.gcp.runtime.supervisor import (
    GcpNativeRuntimeSupervisor,
    NativeGcpRuntimePreparer,
    has_gcp_native_runtime_profile,
    requires_native_gcp_runtime,
)
from eyvan.records import NativeRuntimeConfig
from eyvan.sdk.client.endpoint import ControlPlaneEndpoint
from eyvan.sdk.client.idempotency import IdempotencyKeyFactory
from eyvan.sdk.client.identity import resolve_control_plane_identity
from eyvan.sdk.commands.common import FleetConfigLoader, JsonRenderer
from eyvan.sdk.commands.state import CliSession
from eyvan.sdk.contexts import ContextConfigError
from eyvan.server.api.schemas import CellUpsertRequest, FleetApplyRequest, FleetStatusResponse
from eyvan.server.local.daemon import LocalDaemonError, LocalDaemonResult, ensure_loopback_control_plane

LoopbackControlPlanePreparer: tp.TypeAlias = tp.Callable[[str | ControlPlaneEndpoint], LocalDaemonResult]
NativeGcpRuntimeProfileAvailability: tp.TypeAlias = tp.Callable[[], bool]


def _default_loopback_control_plane(endpoint: str | ControlPlaneEndpoint) -> LocalDaemonResult:
    """Resolve ordinary local-daemon supervision at call time for CLI embedding and tests."""

    return ensure_loopback_control_plane(endpoint)


def _zones_for_region(region: str, project: str) -> tuple[str, ...]:
    """Return available GCP zones in ``region`` for ``project`` via gcloud."""

    result = subprocess.run(
        [
            "gcloud",
            "compute",
            "zones",
            "list",
            f"--filter=region:{region}",
            f"--project={project}",
            "--format=value(name)",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if result.returncode != 0:
        raise click.ClickException(f"could not list zones for {region}: {result.stderr.strip()}")
    zones = tuple(line.strip() for line in result.stdout.splitlines() if line.strip())
    if not zones:
        raise click.ClickException(f"no zones found for region {region!r}")
    return zones


def _with_init_commands(config: EyvanFleetConfig, commands: tuple[str, ...]) -> EyvanFleetConfig:
    """Append startup commands to every pool's native runtime."""

    pools: dict[str, tp.Any] = {}
    for name, pool in config.pools.items():
        runtime = pool.runtime or NativeRuntimeConfig()
        updated_runtime = NativeRuntimeConfig(
            env=runtime.env,
            init_commands=(*runtime.init_commands, *commands),
            artifacts=runtime.artifacts,
            vm_image=runtime.vm_image,
            container=runtime.container,
        )
        pools[name] = pool.model_copy(update={"runtime": updated_runtime})
    return config.model_copy(update={"pools": pools})


def _print_capacity(session: CliSession, *, as_json: bool) -> None:
    """Fetch and print the available TPU pool catalog."""

    scope = session.workload_scope()
    pools = session.client.request(
        "GET",
        "/api/v1/capacity",
        params={"org_id": scope.org_id, "project_id": scope.project_id},
    )
    if not isinstance(pools, dict) or "pools" not in pools:
        raise click.ClickException("control plane returned an unexpected capacity response")
    if as_json:
        click.echo(json.dumps(pools, sort_keys=True, indent=2))
        return
    rows = pools["pools"]
    if not rows:
        click.echo("No enabled capacity pools available.")
        return
    click.echo("Available capacity pools:")
    for pool in rows:
        click.echo(
            f"  {pool['fleet']:30} {pool['accelerator_type']:10} "
            f"{pool['capacity_class']:10} {pool['region']:10} {pool['zone'] or '-':10} "
            f"max={pool['max_cells']}"
        )


def _default_native_gcp_profile_availability() -> bool:
    """Resolve the current user's profile availability at local-runtime selection time."""

    return has_gcp_native_runtime_profile()


def _record_queue_admission_denials(cache: AutoscaleConfigCache, status: FleetStatusResponse) -> None:
    """Persist provider-proven Spot TPU denials observed in fleet status.

    Fleet status can surface a denial recorded before this client version was
    installed. Recording it here keeps the local autoscale selector from
    offering that exact unusable worker shape again.
    """

    recorder = QueueAdmissionDenialRecorder(cache)
    for cell in status.cells:
        recorder.record_cell(cell)


@dataclass(frozen=True, slots=True)
class FleetCommandService:
    """Typed application service for declarative fleet operations."""

    session: CliSession
    loader: FleetConfigLoader = field(default_factory=FleetConfigLoader)
    renderer: JsonRenderer = field(default_factory=JsonRenderer)
    keys: IdempotencyKeyFactory = field(default_factory=lambda: IdempotencyKeyFactory(prefix="cli"))
    gcp_runtime: NativeGcpRuntimePreparer | None = None
    loopback_control_plane: LoopbackControlPlanePreparer = _default_loopback_control_plane
    native_gcp_profile_available: NativeGcpRuntimeProfileAvailability = _default_native_gcp_profile_availability

    @staticmethod
    def apply_request(config: EyvanFleetConfig) -> FleetApplyRequest:
        """Validate compiler output against the public fleet API contract."""

        return FleetApplyRequest.model_validate({"pools": config.pool_payloads()})

    @staticmethod
    def initial_cell_ids(config: EyvanFleetConfig) -> tuple[str, ...]:
        """Validate initial cell compiler output and return its stable IDs."""

        return tuple(CellUpsertRequest.model_validate(cell).id for cell in config.desired_cells())

    def ensure_local_runtime(self, config: EyvanFleetConfig) -> None:
        """Start the correct private loopback runtime before applying a local fleet."""

        endpoint = ControlPlaneEndpoint.parse(self.session.base_url)
        if not endpoint.is_loopback:
            return
        if requires_native_gcp_runtime(config) or self.native_gcp_profile_available():
            runtime = self.gcp_runtime if self.gcp_runtime is not None else GcpNativeRuntimeSupervisor()
            runtime.ensure(endpoint)
            return
        self.loopback_control_plane(endpoint)

    def apply(
        self,
        config: EyvanFleetConfig,
        *,
        generated_payload: JsonObject | None,
        selected: CachedAutoscaleConfig | None,
        autoscale_cache_dir: Path | None,
        dry_run: bool,
        as_json: bool,
        idempotency_key: str | None,
    ) -> None:
        """Validate and apply one fleet, then persist it as the active context."""

        request = self.apply_request(config)
        request_document = model_json(request)
        if dry_run:
            payload: JsonObject = {
                "fleet": config.name,
                "pools": request_document["pools"],
                "desired_initial_cells": list(self.initial_cell_ids(config)),
            }
            if generated_payload is not None:
                payload["autoscale"] = generated_payload
            self.renderer.emit(payload)
            return

        applied = self.session.client.request(
            "PUT",
            f"/api/v1/fleets/{config.name}",
            json=request_document,
            headers={"Idempotency-Key": self.keys.create("up", idempotency_key)},
        )
        try:
            self.session.activate_fleet(config.name, autoscale_cache_dir=autoscale_cache_dir)
        except (ContextConfigError, ValidationError, ValueError) as exc:
            raise click.ClickException(f"fleet applied, but its local context could not be saved: {exc}") from exc
        if autoscale_commands.is_interactive() and not as_json:
            autoscale_commands.render_fleet_applied(
                autoscale_commands.autoscale_console(),
                config,
                selected=selected,
            )
        elif generated_payload is None:
            self.renderer.emit(applied)
        else:
            self.renderer.emit({"apply": applied, "autoscale": generated_payload})

    def down(
        self,
        config_path: Path,
        *,
        yes: bool,
        dry_run: bool,
        idempotency_key: str | None,
    ) -> None:
        """Disable and release one declared fleet."""

        config = self.loader.load(config_path)
        if dry_run:
            self.renderer.emit(self.session.client.request("GET", f"/api/v1/fleets/{config.name}"))
            return
        if not yes:
            click.confirm(f"Disable, drain, and release fleet {config.name!r}?", abort=True)
        self.renderer.emit(
            self.session.client.request(
                "DELETE",
                f"/api/v1/fleets/{config.name}",
                headers={"Idempotency-Key": self.keys.create("down", idempotency_key)},
            )
        )


@click.command("up")
@click.argument("config_path", required=False, type=click.Path(path_type=Path, dir_okay=False))
@click.option("--dry-run", is_flag=True, help="Validate and print the desired fleet without changing it.")
@click.option(
    "--quota-based",
    is_flag=True,
    help="Record Spot TPU quota provenance, then select one generated zone fleet to apply.",
)
@click.option(
    "--region",
    help="Discover zones in a GCP region, generate Spot TPU fleets, and apply the first one (implies --quota-based).",
)
@click.option("--preemptible", is_flag=True, help="Require the selected fleet to contain only Spot TPU pools.")
@click.option(
    "--select",
    "selected_fleet",
    metavar="FLEET",
    help="Choose a cached generated fleet without an interactive selector.",
)
@click.option("--zone", "zones", multiple=True, help="Limit generated autoscale configs to a GCP zone; repeatable.")
@click.option(
    "--tpu-generation",
    "generations",
    multiple=True,
    help="Limit generated configs to a TPU generation, such as v5p.",
)
@click.option(
    "--defaults",
    "defaults_path",
    type=click.Path(path_type=Path, dir_okay=False),
    help="Read autoscale identity/defaults from YAML instead of ~/.eyvan/autoscale/defaults.yaml.",
)
@click.option("--cache-dir", type=click.Path(path_type=Path, file_okay=False), help="Override ~/.eyvan/autoscale.")
@click.option(
    "--init-command",
    "init_commands",
    multiple=True,
    help="Append a startup command to every generated pool (repeatable).",
)
@click.option("--json", "as_json", is_flag=True, help="Emit the full API and quota-discovery result as JSON.")
@click.option("--idempotency-key", help="Reuse one key only when retrying the same apply operation.")
@click.pass_obj
def up_fleet(
    session: CliSession,
    config_path: Path | None,
    dry_run: bool,
    quota_based: bool,
    region: str | None,
    preemptible: bool,
    selected_fleet: str | None,
    zones: tuple[str, ...],
    generations: tuple[str, ...],
    defaults_path: Path | None,
    cache_dir: Path | None,
    init_commands: tuple[str, ...],
    as_json: bool,
    idempotency_key: str | None,
) -> None:
    """Create a declared fleet, or select a generated autoscale fleet when omitted."""

    autoscale_options = (
        ("--quota-based", quota_based),
        ("--region", region is not None),
        ("--select", selected_fleet is not None),
        ("--zone", bool(zones)),
        ("--tpu-generation", bool(generations)),
        ("--defaults", defaults_path is not None),
        ("--cache-dir", cache_dir is not None),
    )
    supplied_autoscale_options = [name for name, supplied in autoscale_options if supplied]
    if config_path is not None and supplied_autoscale_options:
        supplied = ", ".join(supplied_autoscale_options)
        raise click.ClickException(f"{supplied} requires omitting CONFIG_PATH so Eyvan can use autoscale configuration")
    if region is not None and bool(zones):
        raise click.ClickException("--region and --zone cannot be used together")
    if config_path is None and not quota_based and region is None:
        generation_only_options = [
            name
            for name, supplied in (
                ("--zone", bool(zones)),
                ("--tpu-generation", bool(generations)),
                ("--defaults", defaults_path is not None),
            )
            if supplied
        ]
        if generation_only_options:
            supplied = ", ".join(generation_only_options)
            raise click.ClickException(
                f"{supplied} requires --quota-based so Eyvan can regenerate fleet configurations with those filters"
            )

    generated_payload: JsonObject | None = None
    selected: CachedAutoscaleConfig | None = None
    autoscale_cache_dir: Path | None = None
    service = FleetCommandService(session)
    if config_path is not None:
        config = service.loader.load(config_path)
    else:
        cache = AutoscaleConfigCache(cache_dir)
        autoscale_cache_dir = cache.root
        if quota_based or region is not None:
            defaults = autoscale_commands.load_autoscale_defaults(
                cache,
                defaults_path,
                persist_if_prompted=not dry_run,
            )
            if region is not None:
                if not defaults.project:
                    raise click.ClickException(
                        "--region requires a project in the autoscale defaults; run `eyvan autoscale init`"
                    )
                zones = _zones_for_region(region, defaults.project)
            generation = autoscale_commands.generate_autoscale_entries(
                cache=cache,
                defaults=defaults,
                zones=zones,
                generations=generations,
                persist=not dry_run,
            )
            entries = generation.entries
            generated_payload = generation.as_dict()
            if not entries:
                raise click.ClickException(
                    "no eligible preemptible TPU worker pools have usable live quota in the requested scope"
                )
        else:
            entries = cache.list_eligible()
        if region is not None and selected_fleet is None and entries:
            selected_fleet = entries[0].config.name
        selected = autoscale_commands.choose_cached_config(entries, selected_fleet=selected_fleet)
        config = selected.config
    if init_commands:
        config = _with_init_commands(config, init_commands)
    if preemptible:
        autoscale_commands.require_preemptible_tpus(config)
    if not dry_run and ControlPlaneEndpoint.parse(session.base_url).is_loopback:
        try:
            service.ensure_local_runtime(config)
        except LocalDaemonError as exc:
            raise click.ClickException(str(exc)) from exc
        endpoint = ControlPlaneEndpoint.parse(session.base_url)
        session.replace_identity(
            resolve_control_plane_identity(
                endpoint,
                token=session.identity.token,
                dev_user=session.identity.dev_user,
                dev_org=session.identity.dev_org,
                dev_project=session.identity.dev_project,
                dev_roles=session.identity.dev_roles,
            )
        )
    service.apply(
        config,
        generated_payload=generated_payload,
        selected=selected,
        autoscale_cache_dir=autoscale_cache_dir,
        dry_run=dry_run,
        as_json=as_json,
        idempotency_key=idempotency_key,
    )
    if region is not None and not dry_run:
        click.echo()
        _print_capacity(session, as_json=as_json)
        click.echo("\nServer is running. Submit with:")
        click.echo("  python launch.py  # or any @eyvan.execute(eyvan.TpuConfig(...)) callable")
        click.echo("  eyvan status <fleet>")
        click.echo(f"  eyvan up --region {region}  # to refresh/restart")


@click.command("down")
@click.argument("config_path", type=click.Path(path_type=Path, dir_okay=False))
@click.option("-y", "--yes", is_flag=True, help="Do not ask for confirmation.")
@click.option("--dry-run", is_flag=True, help="Print managed cells without draining or deleting them.")
@click.option("--idempotency-key", help="Reuse one key only when retrying the same disable operation.")
@click.pass_obj
def down_fleet(
    session: CliSession,
    config_path: Path,
    yes: bool,
    dry_run: bool,
    idempotency_key: str | None,
) -> None:
    """Drain and release every cell managed by CONFIG_PATH."""

    FleetCommandService(session).down(
        config_path,
        yes=yes,
        dry_run=dry_run,
        idempotency_key=idempotency_key,
    )


@click.command("status")
@click.argument("fleet_or_config")
@click.option("--json", "as_json", is_flag=True, help="Emit the full server-side fleet record as JSON.")
@click.option("--cache-dir", type=click.Path(path_type=Path, file_okay=False), help="Override ~/.eyvan/autoscale.")
@click.pass_obj
def fleet_status(session: CliSession, fleet_or_config: str, as_json: bool, cache_dir: Path | None) -> None:
    """Show server-side pool and cell status for a fleet."""

    candidate = Path(fleet_or_config).expanduser()
    if candidate.exists():
        fleet_name = FleetConfigLoader().load(candidate).name
    else:
        try:
            fleet_name = validate_fleet_name(fleet_or_config)
        except ValueError as exc:
            raise click.ClickException(str(exc)) from exc
    payload = session.client.request("GET", f"/api/v1/fleets/{fleet_name}")
    try:
        status = FleetStatusResponse.model_validate(payload)
    except ValidationError as exc:
        if as_json or not fleet_status_rendering.is_interactive_output():
            JsonRenderer().emit(payload)
            return
        raise click.ClickException(f"server returned an invalid fleet status response: {exc}") from exc
    cache_root = cache_dir if cache_dir is not None else session.autoscale_cache_dir
    _record_queue_admission_denials(AutoscaleConfigCache(cache_root), status)
    if as_json or not fleet_status_rendering.is_interactive_output():
        JsonRenderer().emit(payload)
        return
    fleet_status_rendering.FleetStatusRenderer(fleet_status_rendering.fleet_status_console()).emit(status)


@click.group("config")
def fleet_config() -> None:
    """Inspect declarative fleet configuration."""


@fleet_config.command("validate")
@click.argument("config_path", type=click.Path(path_type=Path, dir_okay=False))
def validate_fleet_config(config_path: Path) -> None:
    """Validate CONFIG_PATH and print its normalized capacity pools."""

    config = FleetConfigLoader().load(config_path)
    request = FleetCommandService.apply_request(config)
    JsonRenderer().emit(
        {
            "valid": True,
            "fleet": config.name,
            "node_pools": model_json(request)["pools"],
            "desired_initial_cells": len(FleetCommandService.initial_cell_ids(config)),
        }
    )


def register_fleet_commands(group: click.Group) -> None:
    """Attach fleet commands to the root command group."""

    for command in (up_fleet, down_fleet, fleet_status, fleet_config):
        group.add_command(command)


__all__ = ("FleetCommandService", "register_fleet_commands")
