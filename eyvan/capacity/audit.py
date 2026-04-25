# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Transition-only structured logging for capacity lifecycle changes."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from eyvan._internal.compat import StrEnum
from eyvan.capacity import DEFINITIVE_CAPACITY_LOSS_STATUSES, CapacityStatus
from eyvan.server.service.models import CellRecord, CellState


class CapacityLifecycleEvent(StrEnum):
    """Operator-visible events in one physical capacity lifecycle."""

    PLANNED = "planned"
    REQUESTING = "requesting"
    PROVISIONING = "provisioning"
    PROVIDER_TRANSITION = "provider_transition"
    READY = "ready"
    PREEMPTED = "preempted"
    UNHEALTHY = "unhealthy"
    LOST = "lost"
    RETRY = "retry"
    REPLACEMENT = "replacement"
    REMOVED = "removed"


class CapacityReplacementReason(StrEnum):
    """Closed planner reasons for retiring capacity before replacement."""

    PROVIDER_LOST = "provider_lost"
    TEMPLATE_CHANGED = "template_changed"
    PROVISIONING_TIMEOUT = "provisioning_timeout"
    UNHEALTHY = "unhealthy"
    SHARED_QUOTA_REBALANCE = "shared_quota_rebalance"


_WARNING_EVENTS = frozenset(
    {
        CapacityLifecycleEvent.PREEMPTED,
        CapacityLifecycleEvent.UNHEALTHY,
        CapacityLifecycleEvent.LOST,
        CapacityLifecycleEvent.REPLACEMENT,
    }
)


def _encoded(value: str) -> str:
    """Quote an untrusted log value so whitespace and newlines stay structured."""

    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def _quantity(value: float | int) -> str:
    """Render one resource quantity without redundant decimal places."""

    return str(int(value)) if float(value).is_integer() else f"{value:g}"


def _resource_context(cell: CellRecord) -> tuple[str, str]:
    """Return the resource kind and per-worker provider shape."""

    if cell.capacity.tpu:
        return "tpu", cell.accelerator_type or f"{cell.capacity.tpu}-tpu"
    if cell.capacity.gpu:
        return "gpu", cell.accelerator_type or f"{cell.capacity.gpu}-gpu"
    return "cpu", f"{_quantity(cell.capacity.cpu)}-cpu"


@dataclass(frozen=True, slots=True)
class CapacityLifecycleLogger:
    """Classify and emit durable capacity transitions without poll noise."""

    logger: logging.Logger

    @staticmethod
    def classify(previous: CellRecord, current: CellRecord) -> CapacityLifecycleEvent | None:
        """Map one persisted state change to its operator lifecycle event."""

        if previous.state is current.state and previous.provider_phase is current.provider_phase:
            return None
        if current.state is CellState.DELETED:
            return CapacityLifecycleEvent.REMOVED
        if current.state is CellState.READY:
            return CapacityLifecycleEvent.READY
        if previous.state is CellState.READY and current.state is CellState.REGISTERING:
            return CapacityLifecycleEvent.PREEMPTED
        if current.state is CellState.UNAVAILABLE:
            if current.provider_phase in DEFINITIVE_CAPACITY_LOSS_STATUSES:
                return CapacityLifecycleEvent.LOST
            return CapacityLifecycleEvent.UNHEALTHY
        if previous.state is CellState.UNAVAILABLE and current.state is CellState.REGISTERING:
            return CapacityLifecycleEvent.RETRY
        if current.provider_phase is CapacityStatus.REQUESTING:
            return CapacityLifecycleEvent.REQUESTING
        if (
            current.provider_phase is CapacityStatus.PROVISIONING
            and previous.provider_phase is not CapacityStatus.PROVISIONING
        ):
            return CapacityLifecycleEvent.PROVISIONING
        if previous.provider_phase is not current.provider_phase:
            return CapacityLifecycleEvent.PROVIDER_TRANSITION
        return None

    def transition(self, previous: CellRecord, current: CellRecord, *, reason: str | None = None) -> bool:
        """Log one persisted transition, returning whether an event was emitted."""

        event = self.classify(previous, current)
        if event is None:
            return False
        self.event(event, current, previous=previous, reason=reason)
        return True

    def event(
        self,
        event: CapacityLifecycleEvent,
        current: CellRecord,
        *,
        previous: CellRecord | None = None,
        reason: str | None = None,
    ) -> None:
        """Emit an explicit controller or planner lifecycle decision."""

        resource_kind, resource_shape = _resource_context(current)
        fleet = current.labels.get("eyvan_fleet", "-")
        previous_state = "absent" if previous is None else previous.state.value
        previous_phase = "none" if previous is None or previous.provider_phase is None else previous.provider_phase.value
        current_phase = "none" if current.provider_phase is None else current.provider_phase.value
        fields = (
            f"event={event.value}",
            f"fleet={_encoded(fleet)}",
            f"pool={_encoded(current.pool_id or '-')}",
            f"cell={_encoded(current.id)}",
            f"provider={_encoded(current.provider)}",
            f"project={_encoded(current.provider_project or '-')}",
            f"location={_encoded(current.zone or current.region)}",
            f"resource_kind={resource_kind}",
            f"resource={_encoded(resource_shape)}",
            f"workers={current.worker_count}",
            f"capacity_class={current.capacity_class.value}",
            f"cell_state={current.state.value}",
            f"provider_state={current_phase}",
            f"previous_cell_state={previous_state}",
            f"previous_provider_state={previous_phase}",
        )
        message = "capacity lifecycle " + " ".join(fields)
        if reason is not None:
            message = f"{message} reason={_encoded(reason[:500])}"
        level = logging.WARNING if event in _WARNING_EVENTS else logging.INFO
        self.logger.log(level, "%s", message)


__all__ = ("CapacityLifecycleEvent", "CapacityLifecycleLogger", "CapacityReplacementReason")
