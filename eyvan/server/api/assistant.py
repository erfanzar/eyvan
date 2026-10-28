# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""The fleet assistant: a bounded tool-calling loop the plane runs for one user.

The plane, not the browser or CLI, talks to an OpenAI-compatible
chat-completions endpoint configured by the operator in the platform file
(``assistant:`` in ``/etc/eyvan/platform.yaml``; the key is read from ``api_key_file``).
The provider key never leaves this process: it is sent only as the provider's
``Authorization`` header and is scrubbed from anything echoed back.

Every tool is a typed call into ``ControlPlaneService`` made with the *asking
user's* principal, so the assistant can never read or do more than that user
could with their own token. Acting tools do not act on their own:

* ``draft_workload`` returns a validated submission the user must confirm;
  confirming is an ordinary ``POST /api/v1/workloads`` by the user's client.
* ``file_request`` files a capacity/approval request for the fleet admins,
  which carries no authority until an admin resolves it.
"""

from __future__ import annotations

import json
import logging
import typing as tp

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from eyvan._internal.platform import AssistantConfig
from eyvan.config import EyvanSettings
from eyvan.domain.sizing import (
    DEFAULT_CANDIDATE_TYPES,
    SizingRequest,
    model_shape_from_config,
    shape_from_parameter_count,
    size_job,
)
from eyvan.records import (
    CapacityClass,
    CommandExecution,
    Principal,
    PriorityBand,
    ResourceGang,
    ResourceVector,
    TeamRequestRecord,
    WorkloadDefinition,
    WorkloadState,
)
from eyvan.server.api.auth import authenticated_principal
from eyvan.server.api.team_routes import require_team_mode
from eyvan.server.service.errors import EyvanServiceError

if tp.TYPE_CHECKING:
    from eyvan.server.service import ControlPlaneService

logger = logging.getLogger(__name__)

_Principal = tp.Annotated[Principal, Depends(authenticated_principal)]
_TeamOn = tp.Annotated[None, Depends(require_team_mode)]

#: Bounds on what one chat turn may carry and produce.
MAX_CLIENT_MESSAGES = 40
MAX_MESSAGE_CHARS = 8000
MAX_TOOL_RESULT_CHARS = 12000
MAX_LISTED_WORKLOADS = 25
PROVIDER_TIMEOUT_S = 120.0

SYSTEM_PROMPT = """You are the Eyvan fleet assistant for one signed-in user of an Eyvan control plane.
Eyvan schedules jobs (mostly EasyDeL/JAX training on TPUs) onto fleets of capacity pools.
Use the tools to answer from real data; never invent fleets, quotas or numbers.
Tools run with this user's own permissions; if a tool returns an error, say what is not allowed.
To size a job, call size_job (prefer the model's HuggingFace config fields, else a parameter count),
then compare the recommendation with what list_capacity shows the user can actually use.
You cannot submit anything. draft_workload prepares a submission the user must confirm themselves.
If the user needs capacity or an approval they do not have, offer to file_request to the fleet admins;
only file one when the user asks for it. Be brief and concrete."""


def _scrub(text: str, secret: str | None) -> str:
    return text.replace(secret, "[redacted]") if secret else text


def _jsonable(value: tp.Any) -> tp.Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value


# ------------------------------------------------------------------ tools


def _schema(properties: dict[str, tp.Any], required: tp.Sequence[str] = ()) -> dict[str, tp.Any]:
    return {"type": "object", "properties": properties, "required": list(required), "additionalProperties": False}


_STR = {"type": "string"}
_INT = {"type": "integer"}

TOOL_SPECS: tuple[dict[str, tp.Any], ...] = (
    {
        "name": "list_fleets",
        "description": "Fleets this user may submit to in the current org/project.",
        "parameters": _schema({}),
    },
    {
        "name": "list_capacity",
        "description": "Enabled capacity pools this user can submit to, optionally filtered by accelerator type.",
        "parameters": _schema({"accelerator_type": _STR, "capacity_class": _STR}),
    },
    {
        "name": "list_capacity_pools",
        "description": "Every capacity pool in detail (fleet admins only).",
        "parameters": _schema({"fleet_name": _STR}),
    },
    {
        "name": "get_quota",
        "description": "Quotas that apply to this user in the current org/project, and whether any binds.",
        "parameters": _schema({}),
    },
    {
        "name": "list_workloads",
        "description": "This project's most recent workloads, optionally filtered by state.",
        "parameters": _schema({"state": _STR}),
    },
    {
        "name": "explain_workload",
        "description": "Why one workload is or is not placeable right now.",
        "parameters": _schema({"workload_id": _STR}, ["workload_id"]),
    },
    {
        "name": "size_job",
        "description": (
            "Estimate TPU memory for a training/inference job and the smallest slice that fits. Pass "
            "model_config (HuggingFace config.json fields) or parameters (count)."
        ),
        "parameters": _schema(
            {
                "model_config": {"type": "object"},
                "parameters": {"type": "number"},
                "dtype": {"type": "string", "enum": ["bfloat16", "float16", "float32"]},
                "optimizer": {"type": "string", "enum": ["adamw", "adafactor", "lion", "sgd", "none"]},
                "trainer": {"type": "string", "enum": ["pretrain", "sft", "dpo", "orpo", "grpo", "inference"]},
                "sequence_length": _INT,
                "global_batch_size": _INT,
                "gradient_checkpointing": {"type": "boolean"},
                "lora_rank": _INT,
                "generation_tokens": _INT,
                "num_generations": _INT,
                "accelerator_types": {"type": "array", "items": _STR},
            }
        ),
    },
    {
        "name": "draft_workload",
        "description": (
            "Prepare (NOT submit) a workload for the user to confirm. command is the shell command to run; "
            "tpu_chips is chips per replica."
        ),
        "parameters": _schema(
            {
                "name": _STR,
                "command": _STR,
                "accelerator_type": _STR,
                "capacity_class": {"type": "string", "enum": [item.value for item in CapacityClass]},
                "tpu_chips": _INT,
                "replicas": _INT,
                "priority_band": {"type": "string", "enum": [item.value for item in PriorityBand]},
                "checkpointable": {"type": "boolean"},
                "max_preemption_retries": _INT,
            },
            ["name", "command"],
        ),
    },
    {
        "name": "list_my_requests",
        "description": "Capacity/approval requests (yours; fleet admins see everyone's).",
        "parameters": _schema({"state": {"type": "string", "enum": ["open", "approved", "declined", "withdrawn"]}}),
    },
    {
        "name": "file_request",
        "description": "File a capacity or approval request to the fleet admins. Only when the user asks.",
        "parameters": _schema(
            {
                "kind": {"type": "string", "enum": ["capacity", "approval"]},
                "summary": _STR,
                "details": {"type": "object"},
            },
            ["kind", "summary"],
        ),
    },
)
TOOL_NAMES = frozenset(spec["name"] for spec in TOOL_SPECS)


class ToolOutcome(tp.NamedTuple):
    name: str
    ok: bool
    result: tp.Any


class AssistantTools:
    """Typed reads and drafts executed as one principal in one org/project scope."""

    def __init__(self, service: ControlPlaneService, principal: Principal, *, org_id: str, project_id: str) -> None:
        self.service = service
        self.principal = principal
        self.org_id = org_id
        self.project_id = project_id
        self.drafts: list[dict[str, tp.Any]] = []
        self.requests: list[TeamRequestRecord] = []
        self.team_mode = False

    async def call(self, name: str, arguments: dict[str, tp.Any]) -> ToolOutcome:
        if name not in TOOL_NAMES:
            return ToolOutcome(name, False, {"error": f"unknown tool {name!r}"})
        try:
            result = await getattr(self, f"_tool_{name}")(**arguments)
        except EyvanServiceError as exc:
            return ToolOutcome(name, False, {"error": f"{type(exc).__name__}: {exc}"})
        except (TypeError, ValueError, ValidationError) as exc:
            return ToolOutcome(name, False, {"error": f"invalid arguments: {str(exc)[:500]}"})
        return ToolOutcome(name, True, _jsonable(result))

    async def _tool_list_fleets(self) -> tp.Any:
        return await self.service.list_submit_fleets(self.principal, org_id=self.org_id, project_id=self.project_id)

    async def _tool_list_capacity(
        self, accelerator_type: str | None = None, capacity_class: str | None = None
    ) -> tp.Any:
        pools = await self.service.list_capacity(
            self.principal,
            org_id=self.org_id,
            project_id=self.project_id,
            accelerator_type=accelerator_type,
            capacity_class=CapacityClass(capacity_class) if capacity_class else None,
        )
        return [
            {
                "fleet": pool.fleet_name,
                "pool": pool.node_type,
                "accelerator_type": pool.accelerator_type,
                "capacity_class": pool.capacity_class,
                "region": pool.region,
                "zone": pool.zone,
                "min_cells": pool.min_cells,
                "max_cells": pool.max_cells,
            }
            for pool in pools
        ]

    async def _tool_list_capacity_pools(self, fleet_name: str | None = None) -> tp.Any:
        return await self.service.list_capacity_pools(self.principal, fleet_name=fleet_name)

    async def _tool_get_quota(self) -> tp.Any:
        quota = await self.service.get_quota(
            self.principal, org_id=self.org_id, project_id=self.project_id, principal_id=self.principal.subject
        )
        # ``unlimited`` is a property, which model_dump drops; without it the
        # model would have to infer "no limit" from an empty-looking chain.
        return {**quota.model_dump(mode="json"), "unlimited": quota.unlimited}

    async def _tool_list_workloads(self, state: str | None = None) -> tp.Any:
        workloads = await self.service.list_workloads(
            self.principal,
            org_id=self.org_id,
            project_id=self.project_id,
            state=WorkloadState(state) if state else None,
        )
        recent = sorted(workloads, key=lambda item: item.created_at, reverse=True)[:MAX_LISTED_WORKLOADS]
        return {
            "total": len(workloads),
            "workloads": [
                {
                    "id": item.id,
                    "name": item.spec.name,
                    "state": item.state,
                    "accelerator_type": item.spec.accelerator_type,
                    "created_by": item.created_by,
                    "created_at": item.created_at.isoformat(),
                }
                for item in recent
            ],
        }

    async def _tool_explain_workload(self, workload_id: str) -> tp.Any:
        return await self.service.explain_workload(self.principal, workload_id)

    async def _tool_size_job(
        self,
        model_config: dict[str, tp.Any] | None = None,
        parameters: float | None = None,
        accelerator_types: list[str] | None = None,
        **options: tp.Any,
    ) -> tp.Any:
        return size_request(
            model_config=model_config, parameters=parameters, accelerator_types=accelerator_types, **options
        )

    async def _tool_draft_workload(
        self,
        name: str,
        command: str,
        accelerator_type: str | None = None,
        capacity_class: str | None = None,
        tpu_chips: int | None = None,
        replicas: int = 1,
        priority_band: str | None = None,
        checkpointable: bool = False,
        max_preemption_retries: int | None = None,
    ) -> tp.Any:
        fields: dict[str, tp.Any] = {
            "name": name,
            "execution": CommandExecution(command=command),
            "accelerator_type": accelerator_type,
            "checkpointable": checkpointable,
        }
        if capacity_class:
            fields["capacity_class"] = CapacityClass(capacity_class)
        if priority_band:
            fields["priority_band"] = PriorityBand(priority_band)
        if max_preemption_retries is not None:
            fields["max_preemption_retries"] = max_preemption_retries
        if tpu_chips:
            fields["gang"] = ResourceGang(replicas=replicas, per_replica=ResourceVector(tpu=tpu_chips))
        spec = WorkloadDefinition(**fields)
        document = spec.model_dump(mode="json", exclude_defaults=True)
        # The execution's ``kind`` is its union discriminator and also its
        # default, so exclude_defaults would drop it and make the draft
        # unsubmittable. Keep the execution block whole.
        document["execution"] = spec.execution.model_dump(mode="json")
        draft = {"org_id": self.org_id, "project_id": self.project_id, "spec": document}
        self.drafts.append(draft)
        return {"drafted": True, "submitted": False, "note": "the user must confirm this draft to submit it"}

    async def _tool_list_my_requests(self, state: str | None = None) -> tp.Any:
        if not self.team_mode:
            return {"error": "requests need team mode on this plane"}
        return await self.service.list_team_requests(self.principal, state=state)

    async def _tool_file_request(self, kind: str, summary: str, details: dict[str, tp.Any] | None = None) -> tp.Any:
        if not self.team_mode:
            return {"error": "requests need team mode on this plane"}
        request = await self.service.file_team_request(
            self.principal,
            kind=kind,
            summary=summary,
            details=details,
            org_id=self.org_id,
            project_id=self.project_id,
        )
        self.requests.append(request)
        return {"filed": True, "id": request.id, "state": request.state}


def size_request(
    *,
    model_config: dict[str, tp.Any] | None = None,
    parameters: float | None = None,
    accelerator_types: list[str] | None = None,
    **options: tp.Any,
) -> dict[str, tp.Any]:
    """Run the sizing model from a config or a parameter count (shared by the route and the tool)."""

    if model_config:
        shape = model_shape_from_config(model_config)
    elif parameters:
        shape = shape_from_parameter_count(int(parameters))
    else:
        raise ValueError("pass model_config (HuggingFace config fields) or parameters")
    request = SizingRequest(shape=shape, **{key: value for key, value in options.items() if value is not None})
    return size_job(request, tuple(accelerator_types) if accelerator_types else DEFAULT_CANDIDATE_TYPES)


# ------------------------------------------------------------------- loop


class AssistantProviderError(RuntimeError):
    """The configured provider could not answer; the message never carries the key."""


class AssistantProvider:
    """One OpenAI-compatible chat-completions endpoint."""

    def __init__(self, config: AssistantConfig, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.url = config.url.rstrip("/") + "/chat/completions"
        self.model = config.model
        self._key = config.api_key
        self._transport = transport

    async def complete(self, messages: list[dict[str, tp.Any]]) -> dict[str, tp.Any]:
        headers = {"Content-Type": "application/json"}
        if self._key:
            headers["Authorization"] = f"Bearer {self._key}"
        body = {
            "model": self.model,
            "messages": messages,
            "tools": [{"type": "function", "function": spec} for spec in TOOL_SPECS],
            "tool_choice": "auto",
        }
        try:
            async with httpx.AsyncClient(timeout=PROVIDER_TIMEOUT_S, transport=self._transport) as client:
                response = await client.post(self.url, json=body, headers=headers)
        except httpx.HTTPError as exc:
            logger.warning("assistant provider unreachable: %s", type(exc).__name__)
            raise AssistantProviderError(f"the assistant provider is unreachable ({type(exc).__name__})") from None
        if response.status_code != 200:
            logger.warning("assistant provider answered HTTP %s", response.status_code)
            detail = _scrub(response.text[:300], self._key)
            raise AssistantProviderError(f"the assistant provider answered HTTP {response.status_code}: {detail}")
        try:
            message = response.json()["choices"][0]["message"]
        except (ValueError, KeyError, IndexError, TypeError):
            raise AssistantProviderError("the assistant provider returned an unreadable answer") from None
        if not isinstance(message, dict):
            raise AssistantProviderError("the assistant provider returned an unreadable answer")
        return message


async def run_chat(
    provider: AssistantProvider, tools: AssistantTools, history: list[dict[str, str]], *, max_steps: int
) -> dict[str, tp.Any]:
    """Run at most ``max_steps`` provider round trips and return the reply plus what tools did."""

    messages: list[dict[str, tp.Any]] = [
        {
            "role": "system",
            "content": f"{SYSTEM_PROMPT}\nUser: {tools.principal.subject}. Scope: {tools.org_id}/{tools.project_id}.",
        },
        *history,
    ]
    trace: list[dict[str, tp.Any]] = []
    for step in range(1, max_steps + 1):
        message = await provider.complete(messages)
        calls = message.get("tool_calls") or []
        if not calls:
            return _turn(str(message.get("content") or ""), step, trace, tools)
        messages.append({"role": "assistant", "content": message.get("content"), "tool_calls": calls})
        for call in calls:
            function = call.get("function") or {}
            name = str(function.get("name", ""))
            try:
                arguments = json.loads(function.get("arguments") or "{}")
                if not isinstance(arguments, dict):
                    raise ValueError("arguments must be an object")
            except ValueError:
                outcome = ToolOutcome(name, False, {"error": "arguments were not a JSON object"})
            else:
                outcome = await tools.call(name, arguments)
            content = json.dumps(outcome.result, default=str)
            if len(content) > MAX_TOOL_RESULT_CHARS:
                content = content[:MAX_TOOL_RESULT_CHARS] + '..."(truncated)"'
            trace.append({"tool": name, "ok": outcome.ok})
            messages.append({"role": "tool", "tool_call_id": call.get("id", ""), "content": content})
    return _turn(
        f"I stopped after {max_steps} steps without a final answer; ask a narrower question.", max_steps, trace, tools
    )


def _turn(reply: str, steps: int, trace: list[dict[str, tp.Any]], tools: AssistantTools) -> dict[str, tp.Any]:
    return {
        "reply": reply,
        "steps": steps,
        "tool_calls": trace,
        "drafts": tools.drafts,
        "requests": [request.model_dump(mode="json") for request in tools.requests],
    }


# ----------------------------------------------------------------- routes


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ChatMessage(_Body):
    #: Only the visible conversation travels from clients; system and tool
    #: messages are the plane's own and cannot be injected.
    role: tp.Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=MAX_MESSAGE_CHARS)


class ChatBody(_Body):
    messages: list[ChatMessage] = Field(min_length=1, max_length=MAX_CLIENT_MESSAGES)
    org_id: str = Field(default="default", min_length=1, max_length=128)
    project_id: str = Field(default="default", min_length=1, max_length=128)


class SizeBody(_Body):
    model_config_fields: dict[str, tp.Any] | None = Field(default=None, alias="model_config")
    parameters: float | None = Field(default=None, gt=0)
    dtype: str | None = None
    optimizer: str | None = None
    trainer: str | None = None
    sequence_length: int | None = Field(default=None, ge=1)
    global_batch_size: int | None = Field(default=None, ge=1)
    gradient_checkpointing: bool | None = None
    lora_rank: int | None = Field(default=None, ge=1)
    generation_tokens: int | None = Field(default=None, ge=0)
    num_generations: int | None = Field(default=None, ge=1)
    accelerator_types: list[str] | None = None


class RequestBody(_Body):
    kind: tp.Literal["capacity", "approval"]
    summary: str = Field(min_length=1, max_length=2000)
    details: dict[str, tp.Any] = Field(default_factory=dict)
    org_id: str | None = Field(default=None, max_length=128)
    project_id: str | None = Field(default=None, max_length=128)


class ResolveBody(_Body):
    resolution: tp.Literal["approved", "declined", "withdrawn"]
    note: str = Field(default="", max_length=2000)


def register_assistant_routes(
    app: FastAPI,
    *,
    settings: EyvanSettings,
    service: ControlPlaneService,
    assistant: AssistantConfig | None,
) -> None:
    """Register assistant, sizing and team-request routes.

    ``assistant`` comes from the operator's platform configuration file; when
    it is ``None`` the chat route answers 404 and status reports disabled.
    """

    def provider(request: Request) -> AssistantProvider:
        if assistant is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="the assistant is not configured")
        # A test/embedding seam, not configuration: a fake provider transport.
        return AssistantProvider(assistant, transport=getattr(request.app.state, "assistant_transport", None))

    @app.get("/api/v1/assistant/status", tags=["assistant"])
    async def assistant_status(principal: _Principal) -> dict[str, tp.Any]:
        """Whether this plane runs the assistant, and with which model (never the key)."""

        del principal
        return {"enabled": assistant is not None, "model": assistant.model if assistant is not None else None}

    @app.post("/api/v1/assistant/chat", tags=["assistant"])
    async def assistant_chat(body: ChatBody, request: Request, principal: _Principal) -> dict[str, tp.Any]:
        """Run one assistant turn as the caller; drafts are returned, never submitted."""

        chosen = provider(request)
        tools = AssistantTools(service, principal, org_id=body.org_id, project_id=body.project_id)
        tools.team_mode = settings.team_mode
        history = [message.model_dump() for message in body.messages]
        try:
            assert assistant is not None  # provider() already refused an unconfigured plane
            return await run_chat(chosen, tools, history, max_steps=assistant.max_steps)
        except AssistantProviderError as exc:
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from None

    @app.post("/api/v1/assistant/size", tags=["assistant"])
    async def assistant_size(body: SizeBody, principal: _Principal) -> dict[str, tp.Any]:
        """Estimate per-chip TPU memory and the smallest fitting slice (no provider involved)."""

        del principal
        options = body.model_dump(exclude={"model_config_fields", "parameters", "accelerator_types"})
        try:
            return size_request(
                model_config=body.model_config_fields,
                parameters=body.parameters,
                accelerator_types=body.accelerator_types,
                **options,
            )
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from None

    @app.post("/api/v1/team/requests", status_code=201, tags=["team"])
    async def file_team_request(body: RequestBody, principal: _Principal, _on: _TeamOn) -> dict[str, tp.Any]:
        record = await service.file_team_request(
            principal,
            kind=body.kind,
            summary=body.summary,
            details=body.details,
            org_id=body.org_id,
            project_id=body.project_id,
        )
        return record.model_dump(mode="json")

    @app.get("/api/v1/team/requests", tags=["team"])
    async def list_team_requests(principal: _Principal, _on: _TeamOn, state: str | None = None) -> dict[str, tp.Any]:
        records = await service.list_team_requests(principal, state=state)
        return {"requests": [record.model_dump(mode="json") for record in records]}

    @app.post("/api/v1/team/requests/{request_id}/resolve", tags=["team"])
    async def resolve_team_request(
        request_id: str, body: ResolveBody, principal: _Principal, _on: _TeamOn
    ) -> dict[str, tp.Any]:
        record = await service.resolve_team_request(principal, request_id, resolution=body.resolution, note=body.note)
        return record.model_dump(mode="json")


__all__ = (
    "SYSTEM_PROMPT",
    "TOOL_SPECS",
    "AssistantProvider",
    "AssistantProviderError",
    "AssistantTools",
    "register_assistant_routes",
    "run_chat",
    "size_request",
)
