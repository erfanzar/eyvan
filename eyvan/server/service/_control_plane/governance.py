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
import weakref

from eyvan.domain.governance import (
    CALLABLE_NEEDS_CAP,
    HELD_WORKLOAD_KEY,
    REFUSE,
    RULE_UNCHANGED,
    AdmissionVerdict,
    ReclaimPlan,
    RuleUnchanged,
    RunningJob,
    UserRules,
    admission_verdict,
    plan_reclaim,
    requested_tpu_units,
    targeted_pool_ids,
    team_tier,
    tpu_cap_refusal,
    tpu_units_in_use,
)
from eyvan.records import TeamRequestRecord, TeamTier, UserRecord
from eyvan.server.service._control_plane.shared import (
    TERMINAL_WORKLOAD_STATES,
    AuditEvent,
    AuthorizationError,
    CallableExecution,
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

#: Workload states that still hold or wait for capacity, for the hard TPU cap.
_UNFINISHED_STATES = frozenset(WorkloadState) - TERMINAL_WORKLOAD_STATES

#: Running TPU usage per owner inside one transaction, keyed by the session.
#: Counted once, then raised by every workload admitted in that transaction, so
#: a batch, DAG or group admitting N children reads the owner's rows once. The
#: entry dies with the session; a retried transaction gets a fresh session.
_USAGE_IN_TX: weakref.WeakKeyDictionary[tp.Any, dict[str, int]] = weakref.WeakKeyDictionary()

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
        max_tpu_units=user.max_tpu_units,
    )


class GovernanceMixin:
    """Transactional governance operations."""

    governance_enabled: bool

    # -------------------------------------------------------- tier and cap

    @staticmethod
    async def _team_tier_in(tx: RepositorySession, user: UserRecord) -> TeamTier:
        """Derive one account's tier from its durable grants and ``trusted`` flag."""

        return team_tier(user.trusted, await tx.list_role_grants(subject=user.subject), utcnow())

    @staticmethod
    async def _lock_tpu_cap(tx: RepositorySession, subject: str) -> None:
        """Serialize every cap decision for one person, across all of their orgs and projects.

        The idempotency lookup takes a transaction-scoped PostgreSQL advisory lock
        on the key (the same mechanism ``_lock_admission_scope`` uses), so two
        cap decisions for one person never overlap. It is a backstop, not the
        guarantee: the transaction's snapshot predates the lock, so a waiter that
        resumes reads stale usage, and it is PostgreSQL's SERIALIZABLE conflict
        detection that aborts it (retryable, surfaced as 503 + Retry-After) rather
        than admitting it. The memory and local stores run whole transactions
        serially, so there the count is always current.
        """

        await tx.get_idempotency("fleet", "governance.tpu_cap", subject)

    @staticmethod
    async def _tpu_units_in_use(tx: RepositorySession, subject: str, *, exclude_id: str | None = None) -> int:
        owned = await tx.list_workloads_created_by(subject, states=_UNFINISHED_STATES)
        return tpu_units_in_use(owned, subject, exclude_id=exclude_id)

    async def _require_tpu_cap(
        self,
        tx: RepositorySession,
        user: UserRecord,
        tpu_units: int,
        *,
        exclude_id: str | None = None,
        prefix: str = "",
    ) -> None:
        """Refuse (``ConflictError``) when ``tpu_units`` more would break the account's hard cap.

        Without ``exclude_id`` this is an admission: the caller stores the job
        next, so the transaction's running total is raised by ``tpu_units``.
        With it (approval release, retry) usage is always counted afresh.
        """

        if user.max_tpu_units is None or tpu_units <= 0:
            return
        running = _USAGE_IN_TX.setdefault(tx, {}) if exclude_id is None else None
        in_use = running.get(user.subject) if running is not None else None
        if in_use is None:
            await self._lock_tpu_cap(tx, user.subject)
            in_use = await self._tpu_units_in_use(tx, user.subject, exclude_id=exclude_id)
        refusal = tpu_cap_refusal(user.max_tpu_units, in_use, tpu_units)
        if refusal is not None:
            raise ConflictError(prefix + refusal)
        if running is not None:
            running[user.subject] = in_use + tpu_units

    async def _governance_retry(self, tx: RepositorySession, workload: WorkloadRecord) -> None:
        """Re-apply the owner's callable rule and hard TPU cap before a retry re-queues a workload.

        A retry returns a FAILED/CANCELLED job to PENDING, so it is an admission
        and must not be a way around either rule -- whoever asks for it. The
        owner (``created_by``) is judged, not the operator retrying.
        """

        if not getattr(self, "governance_enabled", False) or workload.coordination_kind is not None:
            return
        if isinstance(workload.spec.execution, CallableExecution):
            await self._require_team_callable(
                tx, subject=workload.created_by, org_id=workload.org_id, project_id=workload.project_id
            )
        owner = await tx.get_user(workload.created_by)
        if owner is not None:
            await self._require_tpu_cap(
                tx,
                owner,
                requested_tpu_units(workload.spec),
                exclude_id=workload.id,
                prefix=f"cannot retry {workload.id}: ",
            )

    async def _require_team_callable(self, tx: RepositorySession, *, subject: str, org_id: str, project_id: str) -> None:
        """Apply the team-mode callable rule for the identity a callable workload belongs to.

        A project the trust policy names is allowed as before, and nothing
        changes on a plane without team governance. Otherwise an ``admin`` or
        ``trusted`` account may run callables; a ``user`` only once a fleet admin
        has set their TPU cap, which then bounds them. Anyone without an enabled
        team account (bootstrap, OIDC-only and similar principals) keeps the
        project rule.

        The cap counts TPU units only, so a user-tier callable on a CPU or GPU
        pool (0 TPU units) is admitted once a cap exists but is not bounded by it.
        """

        trust = self.callable_trust  # type: ignore[attr-defined]
        if not getattr(self, "governance_enabled", False) or trust.allows(org_id, project_id):
            return
        user = await tx.get_user(subject)
        if user is None or user.disabled:
            trust.require(org_id, project_id)
            return
        if await self._team_tier_in(tx, user) is TeamTier.USER and user.max_tpu_units is None:
            raise AuthorizationError(f"{subject!r} may not run callable workloads yet; {CALLABLE_NEEDS_CAP}")

    async def team_tpu_cap(self, principal: Principal) -> dict[str, tp.Any] | None:
        """Return the caller's tier, hard TPU cap and current usage, or ``None`` off team mode."""

        if not getattr(self, "governance_enabled", False):
            return None
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            user = await tx.get_user(principal.subject)
            if user is None:
                return None
            tier = await self._team_tier_in(tx, user)
            in_use = await self._tpu_units_in_use(tx, user.subject)
        return {"tier": tier.value, "max_tpu_units": user.max_tpu_units, "tpu_units_in_use_or_queued": in_use}

    # ------------------------------------------------------------- rules

    async def set_user_rules(
        self,
        principal: Principal,
        subject: str,
        *,
        allowed_fleets: tp.Sequence[str] | None | RuleUnchanged = RULE_UNCHANGED,
        approval_tpu_threshold: int | None | RuleUnchanged = RULE_UNCHANGED,
        guaranteed_tpu_units: int | None | RuleUnchanged = RULE_UNCHANGED,
        max_tpu_units: int | None | RuleUnchanged = RULE_UNCHANGED,
    ) -> UserRecord:
        """Update one account's rules (fleet admins only).

        ``None`` clears a rule; a rule left at ``RULE_UNCHANGED`` (omitted by the
        caller) keeps its stored value, so a client that predates a rule cannot
        clear it by not knowing it exists.
        """

        if not self.authorizer.allows(principal, Permission.MANAGE_FLEET):  # type: ignore[attr-defined]
            raise AuthorizationError(f"{principal.subject!r} may not set governance rules")
        async with self.repository.transaction() as tx:  # type: ignore[attr-defined]
            user = await tx.get_user(subject)
            if user is None:
                raise NotFoundError(f"user {subject!r} not found")
            if isinstance(allowed_fleets, RuleUnchanged):
                fleets = user.allowed_fleets
            else:
                fleets = None if allowed_fleets is None else tuple(sorted({name.strip() for name in allowed_fleets}))
            rules: dict[str, tp.Any] = {
                "allowed_fleets": fleets,
                "approval_tpu_threshold": user.approval_tpu_threshold
                if isinstance(approval_tpu_threshold, RuleUnchanged)
                else approval_tpu_threshold,
                "guaranteed_tpu_units": user.guaranteed_tpu_units
                if isinstance(guaranteed_tpu_units, RuleUnchanged)
                else guaranteed_tpu_units,
                "max_tpu_units": user.max_tpu_units if isinstance(max_tpu_units, RuleUnchanged) else max_tpu_units,
            }
            try:
                updated = UserRecord.model_validate(
                    {
                        **user.model_dump(mode="json"),
                        "password_hash": user.password_hash,
                        **rules,
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
                        "approval_tpu_threshold": updated.approval_tpu_threshold,
                        "guaranteed_tpu_units": updated.guaranteed_tpu_units,
                        "max_tpu_units": updated.max_tpu_units,
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
        holds_capacity: bool = True,
    ) -> AdmissionVerdict | None:
        """Return the verdict for one new submission, or ``None`` when no rule applies.

        Raises ``AuthorizationError`` for a fleet the person may not use and
        ``ConflictError`` for an over-threshold submission that cannot be held
        or one that would break their hard TPU cap. The ``admin`` and
        ``trusted`` tiers are never held, but the cap binds every tier.
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
        tpu_units = requested_tpu_units(spec)
        exempt = rules.approval_tpu_threshold is not None and await self._team_tier_in(tx, user) is not TeamTier.USER
        verdict = admission_verdict(
            rules,
            tpu_units=tpu_units,
            fleets=fleets,
            pinned=bool(pool_ids),
            operation=operation,
            top_level=top_level,
            exempt_from_approval=exempt,
        )
        if verdict.action == REFUSE:
            if verdict.refusal == "fleet":
                raise AuthorizationError(verdict.reason)
            raise ConflictError(verdict.reason)
        # Held jobs count too: approving one later must not be how a cap is passed.
        # A coordinator (batch/DAG/group/service parent) holds nothing itself;
        # each of its children is checked as it is stored.
        if holds_capacity:
            await self._require_tpu_cap(tx, user, tpu_units)
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
            owner = await tx.get_user(workload.created_by) if getattr(self, "governance_enabled", False) else None
            if owner is not None:
                # Re-checked at release: the cap may have been lowered while the job waited.
                await self._require_tpu_cap(
                    tx,
                    owner,
                    requested_tpu_units(workload.spec),
                    exclude_id=workload.id,
                    prefix=f"cannot release {workload.id} for {owner.subject}: ",
                )
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
