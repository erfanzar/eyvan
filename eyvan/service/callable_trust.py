"""Explicit project trust policy for serialized Python callables."""

from __future__ import annotations

import typing as tp
from dataclasses import dataclass

from eyvan.service.errors import AuthorizationError


@dataclass(frozen=True, slots=True)
class CallableProjectTrustPolicy:
    """Authorize callable deserialization for explicitly selected projects."""

    trusted_projects: frozenset[str] = frozenset()

    @classmethod
    def from_projects(cls, projects: tp.Collection[str]) -> CallableProjectTrustPolicy:
        """Build a policy from fully scoped ``org/project`` IDs or bare project IDs.

        Bare project IDs match that project in every organization, so they must
        be globally unique; prefer fully scoped IDs whenever they are not.
        """

        return cls(trusted_projects=frozenset(projects))

    def allows(self, org_id: str, project_id: str) -> bool:
        """Return whether a project may submit serialized callables.

        A bare (unscoped) trusted project ID matches in any organization; a
        scoped ``org/project`` entry matches only within that organization.
        """

        return project_id in self.trusted_projects or f"{org_id}/{project_id}" in self.trusted_projects

    def require(self, org_id: str, project_id: str) -> None:
        """Reject callable submission for a project outside this policy."""

        if not self.allows(org_id, project_id):
            raise AuthorizationError(f"callable workloads are not trusted for project {org_id}/{project_id}")


__all__ = ("CallableProjectTrustPolicy",)
