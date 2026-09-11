# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

from eyvan.server.service._control_plane.shared import (
    MAX_JOB_TREE_DEPTH,
    MAX_LIVE_CHILDREN,
    TERMINAL_WORKLOAD_STATES,
    AuthorizationError,
    ConflictError,
    JobTreeTooDeepError,
    JobTreeTooWideError,
    Principal,
    WorkloadDefinition,
    WorkloadRecord,
    WorkloadState,
    _fingerprint,
    _scope,
    _validate_idempotency_key,
    parent_depth,
)


class ChildSubmissionMixin:
    """Transactional child submission operations."""

    async def submit_child_workload(
        self,
        *,
        parent: WorkloadRecord,
        spec: WorkloadDefinition,
        idempotency_key: str,
        expected_parent_generation: int | None = None,
    ) -> tuple[WorkloadRecord, bool]:
        """Admit a workload submitted by a running job on its own authority.

        The caller has already been authenticated by its workload capability, so
        there is no ``Principal`` here to authorize against -- and inventing one
        would mean synthesizing a grant no identity provider issued, inside the
        most security-sensitive route in the package.

        Instead the parent *is* the authorization: a child inherits its scope,
        its identity, and its place in the fair-share tree, and can therefore
        reach nothing its parent could not.

        Args:
            parent: The workload whose capability authorized this submission.
            spec: The child's definition.
            idempotency_key: Scoped to the parent, so sibling retries deduplicate.
            expected_parent_generation: The parent generation the caller's
                capability was verified against, when it read the parent in its
                own transaction. Re-checked against the fresh read inside the
                admitting transaction so a retry or kick that bumped the
                generation in between cannot admit on a superseded token.
        """

        if not parent.spec.can_submit_children:
            raise AuthorizationError(f"workload {parent.id!r} was not submitted with can_submit_children")
        # Inheriting the parent's scope does not inherit permission to have a
        # pickle deserialized: trust is a property of the project, and a child
        # submitted in-job reaches the same executor by the same route. Checked
        # against the parent's scope because that is the scope the child runs in.
        self._authorize_callable_spec(spec, org_id=parent.org_id, project_id=parent.project_id)
        if parent.state in TERMINAL_WORKLOAD_STATES or parent.state is WorkloadState.CANCELLING:
            raise ConflictError(f"workload {parent.id!r} is {parent.state.value}; it cannot gain new children")

        # Derived, not supplied. A launcher's natural key is a step number, and
        # two siblings both calling their first child "step-1" would otherwise
        # collide -- the second one silently returning the first one's handle.
        scoped_key = _validate_idempotency_key(f"{parent.id}:{idempotency_key}")
        scope = _scope(parent.org_id, parent.project_id)
        fingerprint = _fingerprint(spec)
        async with self.repository.transaction() as tx:

            async def _parent_of(workload_id: str) -> WorkloadRecord | None:
                """Return a workload's parent, not the workload itself.

                Spelled out because ``get_workload`` reads as a plausible parent
                loader and is not one: passing it directly makes every workload
                its own parent, which the cycle guard then reports as a corrupt
                tree on the very first submission.
                """

                record = await tx.get_workload(workload_id)
                if record is None or record.parent_workload_id is None:
                    return None
                return await tx.get_workload(record.parent_workload_id)

            depth = await parent_depth(parent.id, _parent_of)
            if depth + 1 >= MAX_JOB_TREE_DEPTH:
                raise JobTreeTooDeepError(
                    f"workload {parent.id!r} is already {depth} levels deep; a child would exceed {MAX_JOB_TREE_DEPTH}"
                )
            live = [
                child
                for child in await tx.list_child_workloads(parent.id)
                if child.state not in TERMINAL_WORKLOAD_STATES
            ]
            if len(live) >= MAX_LIVE_CHILDREN:
                raise JobTreeTooWideError(
                    f"workload {parent.id!r} already has {len(live)} live children; "
                    f"at most {MAX_LIVE_CHILDREN} may run at once"
                )
            return await self._store_workload(
                tx,
                # The actor is truthfully the workload, which is what the audit
                # trail should show. The identity is the parent's owner, which
                # is what quota and fair share must see.
                Principal(subject=f"workload:{parent.id}", grants=()),
                org_id=parent.org_id,
                project_id=parent.project_id,
                spec=spec,
                scope=scope,
                operation="workload.submit_child",
                key=scoped_key,
                fingerprint=fingerprint,
                parent_workload_id=parent.id,
                expected_parent_generation=expected_parent_generation,
                created_by=parent.created_by,
            )
