# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of EasyDeL.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""Role-based authorization independent of HTTP authentication mechanics."""

from __future__ import annotations

from eyvan.server.service.errors import AuthorizationError
from eyvan.server.service.models import Permission, Principal, Role

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
