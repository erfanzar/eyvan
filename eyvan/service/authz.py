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
"""Role-based authorization independent of HTTP authentication mechanics."""

from __future__ import annotations

from eyvan.service.errors import AuthorizationError
from eyvan.service.models import Permission, Principal, Role

_ROLE_PERMISSIONS: dict[Role, frozenset[Permission]] = {
    Role.FLEET_ADMIN: frozenset(Permission),
    Role.ORG_ADMIN: frozenset({Permission.READ, Permission.SUBMIT, Permission.OPERATE, Permission.MANAGE_PROJECT}),
    Role.PROJECT_ADMIN: frozenset({Permission.READ, Permission.SUBMIT, Permission.OPERATE, Permission.MANAGE_PROJECT}),
    Role.PROJECT_OPERATOR: frozenset({Permission.READ, Permission.SUBMIT, Permission.OPERATE}),
    Role.PROJECT_VIEWER: frozenset({Permission.READ}),
}


class Authorizer:
    """Evaluate explicit organization/project grants without implicit inheritance."""

    def allows(
        self,
        principal: Principal,
        permission: Permission,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
    ) -> bool:
        for grant in principal.grants:
            if permission not in _ROLE_PERMISSIONS[grant.role]:
                continue
            if grant.role == Role.FLEET_ADMIN:
                return True
            if org_id is None or grant.org_id != org_id:
                continue
            if grant.role == Role.ORG_ADMIN:
                return permission != Permission.MANAGE_FLEET
            if grant.project_id is not None and grant.project_id == project_id:
                return True
        return False

    def require(
        self,
        principal: Principal,
        permission: Permission,
        *,
        org_id: str | None = None,
        project_id: str | None = None,
    ) -> None:
        """Raise when the principal lacks a scoped permission."""

        if not self.allows(principal, permission, org_id=org_id, project_id=project_id):
            raise AuthorizationError(
                f"{principal.subject!r} lacks {permission.value} permission"
                + (f" for {org_id}/{project_id}" if org_id is not None else "")
            )
