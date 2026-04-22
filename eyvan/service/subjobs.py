# Copyright 2026 The EasyDeL/Eyvan Author @erfanzar (Erfan Zare Chavoshi).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Job-tree traversal for parent/child workloads.

**Why a tree is not a scheduling level.** Eyvan's fair-share hierarchy is
tenant -> project -> principal and is structural: ``IdentityScope`` caps its own
depth, ``WorkloadIdentity.scopes`` returns a literal three-tuple, and the DRF
queue writes that parent chain verbatim. Adding a fourth level would ripple into
quota scopes, budget scopes, and the per-scope lease accounting indexes. So a
child inherits its parent's ``(org, project, created_by)`` and competes as an
ordinary sibling; job structure is a *relation* plus a *cancellation cascade*,
nothing more. Revisit only if fairness *between* trees becomes a real complaint.

Traversal here is deliberately defensive. ``parent_workload_id`` is an ordinary
persisted field, so a corrupted or hand-edited record could describe a cycle or
an unbounded chain. Every walk is therefore visited-set guarded and depth-capped
rather than trusting the data to be a tree.
"""

from __future__ import annotations

import typing as tp

from eyvan.service.errors import ConflictError, InputError
from eyvan.service.models import WorkloadRecord

#: Maximum tree depth walked in one traversal. Deep enough for launcher ->
#: stage -> shard patterns, shallow enough that a malformed chain cannot make a
#: cancellation walk unbounded.
MAX_JOB_TREE_DEPTH = 32


class ChildLoader(tp.Protocol):
    """Load the direct children of one workload."""

    async def __call__(self, parent_workload_id: str) -> list[WorkloadRecord]: ...


class JobTreeError(InputError):
    """A named parent/child relation is not a valid tree.

    An ``EyvanServiceError`` rather than a bare ``ValueError`` because it reaches
    the HTTP layer: subclassing ``ValueError`` meant no handler matched and a
    caller naming a parent in another project got a 500 instead of being told
    what was wrong with their request.
    """


class JobTreeTooDeepError(ConflictError):
    """The durable tree is deeper than the cascade protocol will walk.

    Distinct from :class:`JobTreeError` and a 409, not a 400, because the request
    was well formed -- the *stored* tree is the problem. Cancelling such a parent
    with ``cascade=False`` is the way out, which is why that flag is exposed on
    the route.
    """


async def collect_descendants(
    root_id: str,
    load_children: ChildLoader,
    *,
    max_depth: int = MAX_JOB_TREE_DEPTH,
) -> list[WorkloadRecord]:
    """Return every descendant of ``root_id``, parents before their children.

    The order matters for cancellation: cancelling a parent before its children
    means the parent cannot admit new children while the walk is still running.

    A workload already visited is skipped rather than re-walked, so a cycle
    terminates instead of looping forever.
    """

    visited: set[str] = {root_id}
    ordered: list[WorkloadRecord] = []
    frontier = [(root_id, 0)]
    while frontier:
        parent_id, depth = frontier.pop(0)
        if depth >= max_depth:
            raise JobTreeTooDeepError(
                f"job tree below {root_id!r} is deeper than {max_depth} levels; refusing to walk further. "
                "Cancel individual subtrees, or cancel this workload alone with cascade=false."
            )
        for child in await load_children(parent_id):
            if child.id in visited:
                # Either a cycle or a diamond; both mean this node is already
                # accounted for, and re-walking it would not terminate.
                continue
            visited.add(child.id)
            ordered.append(child)
            frontier.append((child.id, depth + 1))
    return ordered


def validate_parent(child_scope: tuple[str, str], parent: WorkloadRecord) -> None:
    """Reject a parent that would create a cross-tenant or cross-project tree.

    A child must stay inside its parent's scope: it inherits the parent's
    fair-share leaf and budget, so a cross-project child would spend one
    project's budget against another's quota.
    """

    org_id, project_id = child_scope
    if parent.org_id != org_id or parent.project_id != project_id:
        raise JobTreeError(
            f"parent workload {parent.id!r} belongs to {parent.org_id}/{parent.project_id}, "
            f"not the child's {org_id}/{project_id}"
        )


__all__ = (
    "MAX_JOB_TREE_DEPTH",
    "ChildLoader",
    "JobTreeError",
    "JobTreeTooDeepError",
    "collect_descendants",
    "validate_parent",
)
