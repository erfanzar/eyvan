# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""HTTP execution driver for one Eyvan native worker."""

from __future__ import annotations

import os
import typing as tp

import httpx
from pydantic import ValidationError

from eyvan._internal.json_types import JsonObject, JsonValue, is_json_object, model_json
from eyvan.capacity.drivers.base import ExecutionWorkloadObservation, MutationContext, StaleMutationError
from eyvan.worker.agent.models import (
    CheckpointRequest,
    MutationConflict,
    MutationConflictCode,
    MutationModel,
    NativeWorkerHealth,
    WorkloadMutationRequest,
    WorkloadReference,
    WorkloadRequest,
)
from eyvan.worker.agent.models import WorkloadObservation as AgentWorkloadObservation


class NativeRemoteExecutor:
    """Delegate fenced workload operations to one native worker over HTTP."""

    def __init__(
        self,
        endpoint: str,
        *,
        secret: str | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        if not endpoint or endpoint != endpoint.strip():
            raise ValueError("native worker endpoint must be non-empty and trimmed")
        if not isinstance(secret, str) or not secret.strip():
            raise ValueError("native worker requests require a non-empty bearer secret")
        self.endpoint = endpoint.rstrip("/")
        self._headers = {"Authorization": f"Bearer {secret}"}
        self._owns_client = client is None
        self.client = client or httpx.Client(base_url=self.endpoint, timeout=30.0)
        try:
            retire_wait_s = float(os.getenv("EYVAN_SUPERSEDE_WAIT_TIMEOUT_S", "300"))
        except ValueError:
            retire_wait_s = 300.0
        self._retire_timeout_s = max(retire_wait_s, 0.1) + 15.0

    @staticmethod
    def _mutation(context: MutationContext) -> MutationModel:
        return MutationModel(
            idempotency_key=context.idempotency_key,
            generation=context.generation,
            fencing_token=context.fencing_token,
        )

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> httpx.Response:
        if response.status_code == 409:
            try:
                conflict = MutationConflict.model_validate_json(response.content)
            except (ValidationError, ValueError):
                response.raise_for_status()
                return response
            if conflict.code is MutationConflictCode.STALE_MUTATION:
                raise StaleMutationError(conflict.detail)
        response.raise_for_status()
        return response

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: JsonObject | None = None,
        timeout: float | None = None,
    ) -> httpx.Response:
        if timeout is None:
            return self.client.request(method, path, headers=self._headers, json=json)
        return self.client.request(method, path, headers=self._headers, json=json, timeout=timeout)

    def health(self) -> bool:
        """Return whether the native worker accepts HTTP requests."""

        try:
            response = self._request(
                "GET",
                "/api/v1/health",
                timeout=5.0 if self._owns_client else None,
            )
            if response.status_code != 200:
                return False
            NativeWorkerHealth.model_validate_json(response.content)
            return True
        except (httpx.HTTPError, ValidationError, ValueError):
            return False

    def submit(self, spec: tp.Mapping[str, JsonValue], context: MutationContext) -> str:
        response = self._raise_for_status(
            self._request(
                "POST",
                "/api/v1/workloads",
                json=model_json(WorkloadRequest(mutation=self._mutation(context), spec=dict(spec))),
            )
        )
        return WorkloadReference.model_validate(response.json()).remote_id

    def status(self, remote_id: str) -> ExecutionWorkloadObservation:
        response = self._raise_for_status(self._request("GET", f"/api/v1/workloads/{remote_id}"))
        payload = response.json()
        try:
            observation = AgentWorkloadObservation.model_validate(payload)
        except ValidationError as exc:
            detail = ""
            if is_json_object(payload):
                message = payload.get("message")
                if isinstance(message, str) and message.strip():
                    detail = f"worker message {message.strip()!r}; "
                if "status" not in payload:
                    detail = f"omitted status; {detail}"
            raise ValueError(f"{detail}invalid native worker observation: {exc}") from exc
        return ExecutionWorkloadObservation(
            remote_id=observation.remote_id,
            status=observation.status,
            message=observation.message,
        )

    def stop(self, remote_id: str, context: MutationContext) -> None:
        self._raise_for_status(
            self._request(
                "POST",
                f"/api/v1/workloads/{remote_id}/stop",
                json=model_json(WorkloadMutationRequest(mutation=self._mutation(context))),
            )
        )

    def retire(self, remote_id: str, context: MutationContext) -> None:
        self._raise_for_status(
            self._request(
                "POST",
                f"/api/v1/workloads/{remote_id}/retire",
                json=model_json(WorkloadMutationRequest(mutation=self._mutation(context))),
                timeout=self._retire_timeout_s if self._owns_client else None,
            )
        )

    def checkpoint(
        self,
        remote_id: str,
        *,
        hook: str,
        output_uri: str | None,
        context: MutationContext,
    ) -> str:
        response = self._raise_for_status(
            self._request(
                "POST",
                f"/api/v1/workloads/{remote_id}/checkpoint",
                json=model_json(
                    CheckpointRequest(
                        mutation=self._mutation(context),
                        hook=hook,
                        output_uri=output_uri,
                    )
                ),
            )
        )
        return WorkloadReference.model_validate(response.json()).remote_id

    def logs(self, remote_id: str) -> str:
        return self._raise_for_status(self._request("GET", f"/api/v1/workloads/{remote_id}/logs")).text

    def process_status(self, remote_id: str) -> JsonObject:
        """Return live process detail for one workload on this host."""

        response = self._raise_for_status(self._request("GET", f"/api/v1/workloads/{remote_id}/process-status"))
        payload = response.json()
        if not is_json_object(payload):
            raise ValueError("native worker process status must be a JSON object")
        return payload

    def close(self) -> None:
        """Close the internally owned HTTP connection pool."""

        if self._owns_client:
            self.client.close()
        self._headers.clear()


__all__ = ("NativeRemoteExecutor",)
