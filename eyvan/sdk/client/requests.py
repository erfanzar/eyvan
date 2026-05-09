# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Shared typed request construction for Eyvan SDK clients."""

from __future__ import annotations

import typing as tp

from pydantic import BaseModel

from eyvan._internal.json_types import JsonObject, model_json
from eyvan.records import CallableExecution, CapacityClass, WorkloadState, WorkspaceRef
from eyvan.sdk.client.jobs import reject_remote_callable_signature, reject_remote_callable_signing_environment
from eyvan.sdk.client.types import QueryParams
from eyvan.server.api.schemas import PoolCallableSubmitRequest, PoolWorkloadSubmitRequest


def pool_request_json(request: BaseModel) -> JsonObject:
    """Serialize a pool request, omitting only server-defaulted top-level fields."""

    document = model_json(request)
    for field_name in set(type(request).model_fields) - request.model_fields_set:
        document.pop(field_name, None)
    return document


def pool_workload_request(
    *,
    org_id: str,
    project_id: str,
    command: str,
    name: str | None,
    environment: tp.Mapping[str, str] | None,
    metadata: tp.Mapping[str, str] | None,
    max_retries: int,
    max_failure_retries: int = 0,
    workspace: WorkspaceRef | None = None,
) -> PoolWorkloadSubmitRequest:
    """Build the shared pool-command request payload."""

    return PoolWorkloadSubmitRequest(
        org_id=org_id,
        project_id=project_id,
        command=command,
        name=name,
        environment=dict(environment or {}),
        metadata=dict(metadata or {}),
        max_retries=max_retries,
        max_failure_retries=max_failure_retries,
        workspace=workspace,
    )


def pool_callable_request(
    *,
    org_id: str,
    project_id: str,
    accelerator_type: str,
    pods: int,
    capacity_class: CapacityClass,
    execution: CallableExecution,
    name: str | None,
    environment: tp.Mapping[str, str] | None,
    metadata: tp.Mapping[str, str] | None,
    max_retries: int,
    max_failure_retries: int = 0,
) -> PoolCallableSubmitRequest:
    """Build the shared pool-callable request payload."""

    reject_remote_callable_signature(execution.signature)
    reject_remote_callable_signing_environment(environment)
    return PoolCallableSubmitRequest(
        org_id=org_id,
        project_id=project_id,
        accelerator_type=accelerator_type,
        pods=pods,
        capacity_class=capacity_class,
        execution=execution,
        name=name,
        environment=dict(environment or {}),
        metadata=dict(metadata or {}),
        max_retries=max_retries,
        max_failure_retries=max_failure_retries,
    )


def workload_list_params(*, org_id: str, project_id: str, state: WorkloadState | None) -> QueryParams:
    """Build workload-list query parameters without serializing empty state."""

    return {"org_id": org_id, "project_id": project_id, **({"state": state.value} if state else {})}
