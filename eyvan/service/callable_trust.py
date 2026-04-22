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
