# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Team governance: per-user fleet rules, approval holds and borrow/reclaim.

Off unless the service is built with ``governance_enabled`` (team mode). The
decisions themselves live in :mod:`eyvan.domain.governance`; this mixin only
collects facts inside a transaction and applies the verdict.

A held submission is an ordinary PENDING workload stored ``schedulable=False``
-- the gate the scheduler and the capacity planner already honour -- plus an
``approval`` request for the fleet admins. Approving flips the gate; declining
or withdrawing cancels the workload. No new workload state reaches the wire.
"""

from __future__ import annotations

import secrets
import typing as tp

from eyvan.domain.governance import (
    HELD_WORKLOAD_KEY,
    REFUSE,
    AdmissionVerdict,
    ReclaimPlan,
    RunningJob,
    UserRules,
    admission_verdict,
    plan_reclaim,
    requested_tpu_units,
    targeted_pool_ids,
)
from eyvan.records import TeamRequestRecord, UserRecord
from eyvan.server.service._control_plane.shared import (
    AuditEvent,
    AuthorizationError,
    ConflictError,
    InputError,
    NotFoundError,
    Permission,
    Principal,
    RepositorySession,
    Role,
    RoleGrant,
    WorkloadDefinition,
    WorkloadRecord,
    WorkloadState,
    _new_id,
    utcnow,
)

#: The resolution string that releases a held workload.
HOLD_APPROVED = "approved"

#: States in which a workload holds capacity, for borrow accounting.
_ACTIVE_STATES = frozenset(
    {
        WorkloadState.LEASED,
        WorkloadState.DISPATCHING,
        WorkloadState.RUNNING,
        WorkloadState.CHECKPOINTING,
    }
)


def user_rules(user: UserRecord) -> UserRules:
    return UserRules(
        allowed_fleets=user.allowed_fleets,
        approval_tpu_threshold=user.approval_tpu_threshold,
        guaranteed_tpu_units=user.guaranteed_tpu_units,
    )


class GovernanceMixin:
    """Transactional governance operations."""

    governance_enabled: bool

    # ------------------------------------------------------------- rules

    async def set_user_rules(
        self,
        principal: Principal,
        subject: str,
        *,
        allowed_fleets: tp.Sequence[str] | None,
        approval_tpu_threshold: int | None,
        guaranteed_tpu_units: int | None,
    ) -> UserRecord:
        """Replace one account's rules (fleet admins only); ``None`` clears a rule."""

        if not self.authorizer.allows(principal, Permission.MANAGE_FLEET):  # type: ignore[attr-defined]
            raise AuthorizationError(f"{principal.subject!r} may not set governance rules")
        fleets = None if allowed_fleets is None else tuple(sorted({name.strip() for name in allowed_fleets}))
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            user = await tx.get_user(subject)
            if user is None:
                raise NotFoundError(f"user {subject!r} not found")
            try:
                updated = UserRecord.model_validate(
                    {
                        **user.model_dump(mode="json"),
                        "password_hash": user.password_hash,
                        "allowed_fleets": fleets,
                        "approval_tpu_threshold": approval_tpu_threshold,
                        "guaranteed_tpu_units": guaranteed_tpu_units,
                        "updated_at": utcnow(),
                    }
                )
            except ValueError as exc:
                raise InputError(str(exc)) from exc
            await tx.put_user(updated)
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="user",
                    entity_id=subject,
                    action="user.rules_updated",
                    payload={
                        "allowed_fleets": list(fleets) if fleets is not None else None,
                        "approval_tpu_threshold": approval_tpu_threshold,
                        "guaranteed_tpu_units": guaranteed_tpu_units,
                    },
                )
            )
            return updated

    # --------------------------------------------------------- admission

    async def _governance_admission(
        self,
        tx: RepositorySession,
        *,
        spec: WorkloadDefinition,
        subject: str,
        operation: str,
        top_level: bool,
    ) -> AdmissionVerdict | None:
        """Return the verdict for one new submission, or ``None`` when no rule applies.

        Raises ``AuthorizationError`` for a fleet the person may not use and
        ``ConflictError`` for an over-threshold submission that cannot be held.
        """

        if not getattr(self, "governance_enabled", False):
            return None
        user = await tx.get_user(subject)
        if user is None:
            return None
        rules = user_rules(user)
        if rules.empty:
            return None
        pool_ids = targeted_pool_ids(spec)
        fleets: list[str] = []
        for pool_id in pool_ids:
            pool = await tx.get_capacity_pool(pool_id)
            if pool is not None:
                fleets.append(pool.fleet_name)
        verdict = admission_verdict(
            rules,
            tpu_units=requested_tpu_units(spec),
            fleets=fleets,
            pinned=bool(pool_ids),
            operation=operation,
            top_level=top_level,
        )
        if verdict.action == REFUSE:
            if verdict.refusal == "fleet":
                raise AuthorizationError(verdict.reason)
            raise ConflictError(verdict.reason)
        return verdict

    async def _hold_for_approval(
        self,
        tx: RepositorySession,
        principal: Principal,
        workload: WorkloadRecord,
        verdict: AdmissionVerdict,
    ) -> TeamRequestRecord:
        """File the approval request that owns one held workload."""

        request = TeamRequestRecord(
            id=f"req_{secrets.token_hex(8)}",
            kind="approval",
            subject=workload.created_by,
            org_id=workload.org_id,
            project_id=workload.project_id,
            summary=f"Run {workload.spec.name} ({verdict.tpu_units} TPU units)"[:2000],
            details={
                HELD_WORKLOAD_KEY: workload.id,
                "tpu_units": verdict.tpu_units,
                "reason": verdict.reason,
            },
        )
        await tx.put_team_request(request)
        await tx.append_event(
            AuditEvent(
                id=_new_id("evt"),
                actor=principal.subject,
                entity_type="team_request",
                entity_id=request.id,
                action="team_request.filed",
                org_id=request.org_id,
                project_id=request.project_id,
                payload={"kind": request.kind, HELD_WORKLOAD_KEY: workload.id},
            )
        )
        await self._audit(  # type: ignore[attr-defined]
            tx,
            principal,
            workload,
            "workload.approval_held",
            new_state=workload.state.value,
            payload={"request_id": request.id, "tpu_units": verdict.tpu_units},
        )
        return request

    async def _apply_approval_resolution(
        self,
        tx: RepositorySession,
        principal: Principal,
        request: TeamRequestRecord,
        resolution: str,
    ) -> None:
        """Release (approved) or cancel (declined/withdrawn) the workload a request holds."""

        workload_id = request.details.get(HELD_WORKLOAD_KEY)
        if request.kind != "approval" or not isinstance(workload_id, str):
            return
        workload = await tx.get_workload(workload_id)
        if (
            workload is None
            or workload.state is not WorkloadState.PENDING
            or workload.schedulable
            or workload.parent_workload_id is not None
            or workload.depends_on
            or workload.coordination_kind is not None
        ):
            # Already cancelled by its owner, or not a governance hold at all.
            return
        if resolution == HOLD_APPROVED:
            released = workload.model_copy(
                update={
                    "schedulable": True,
                    "scheduling_reason": f"approved by {principal.subject}; waiting for scheduling",
                    "updated_at": utcnow(),
                }
            )
            await tx.put_workload(released)
            await self._audit(  # type: ignore[attr-defined]
                tx, principal, released, "workload.approved", payload={"request_id": request.id}
            )
            return
        action = "workload.approval_withdrawn" if resolution == "withdrawn" else "workload.approval_declined"
        await self._cancel_one(tx, principal, workload, action)  # type: ignore[attr-defined]

    # ---------------------------------------------------- borrow / reclaim

    async def _reclaim_inputs(
        self, tx: RepositorySession
    ) -> tuple[list[RunningJob], dict[str, int], dict[str, WorkloadRecord]]:
        active = [workload for workload in await tx.list_workloads() if workload.state in _ACTIVE_STATES]
        ordered = sorted(active, key=lambda item: (item.created_at, item.id))
        jobs = [
            RunningJob(
                workload_id=workload.id,
                subject=workload.created_by,
                tpu_units=requested_tpu_units(workload.spec),
                priority_band=workload.spec.priority_band.value,
                started_order=index,
                reclaimable=workload.state is WorkloadState.RUNNING,
            )
            for index, workload in enumerate(ordered)
        ]
        guarantees = {
            user.subject: user.guaranteed_tpu_units
            for user in await tx.list_users()
            if user.guaranteed_tpu_units is not None
        }
        return jobs, guarantees, {workload.id: workload for workload in active}

    async def plan_reclaim_for(self, principal: Principal, *, tpu_units: int) -> ReclaimPlan:
        """Return the deterministic plan for ``principal`` reclaiming ``tpu_units`` (read-only)."""

        if not getattr(self, "governance_enabled", False):
            raise NotFoundError("governance is not enabled on this plane")
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            jobs, guarantees, _ = await self._reclaim_inputs(tx)
        return plan_reclaim(principal.subject, tpu_units, jobs, guarantees)

    async def reclaim_borrowed(
        self, principal: Principal, *, tpu_units: int, reason: str, idempotency_key: str
    ) -> tuple[ReclaimPlan, list[str]]:
        """Reclaim borrowed capacity for ``principal`` by the deterministic rule.

        The rule, not the caller, picks the victims, and only someone still
        within their own guarantee may ask. Each victim goes through the
        ordinary checkpoint-first ``reclaim_workload`` command, issued under a
        narrow ``reclaim:<subject>`` identity: the caller never gains general
        fleet authority, and the audit trail names who triggered it.

        Returns the plan and the victim ids a reclaim command was issued for.
        A victim that stopped running between planning and issuing is skipped
        (its capacity is already free) rather than failing the whole request.
        """

        plan = await self.plan_reclaim_for(principal, tpu_units=tpu_units)
        if not plan.allowed:
            raise ConflictError(plan.reason)
        reason = reason.strip() or "borrowed capacity reclaimed"
        actor = Principal(
            subject=f"reclaim:{principal.subject}",
            grants=(RoleGrant(role=Role.FLEET_ADMIN),),
        )
        issued: list[str] = []
        for workload_id in plan.victims:
            try:
                await self.reclaim_workload(  # type: ignore[attr-defined]
                    actor,
                    workload_id,
                    idempotency_key=f"{idempotency_key}:{workload_id}",
                    reason=f"{reason} (for {principal.subject})"[:500],
                )
            except ConflictError:
                continue
            issued.append(workload_id)
        return plan, issued


__all__ = ("HELD_WORKLOAD_KEY", "GovernanceMixin", "user_rules")
