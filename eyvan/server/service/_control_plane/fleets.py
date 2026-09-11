# Copyright (C) 2026 Erfan Zare Chavoshi
# This file is part of Eyvan.
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

from eyvan.server.service._control_plane.shared import (
    AuditEvent,
    CapacityClass,
    CapacityPoolRecord,
    CellRecord,
    CellState,
    ConflictError,
    IdempotencyRecord,
    InputError,
    LeaseState,
    NotFoundError,
    Permission,
    Principal,
    _fingerprint,
    _new_id,
    _pool_declaration,
    _validate_idempotency_key,
    datetime,
    tp,
    utcnow,
    validate_capacity_pool_quota_groups,
)


class FleetsMixin:
    """Transactional fleets operations."""

    async def upsert_capacity_pool(
        self,
        principal: Principal,
        pool: CapacityPoolRecord,
        *,
        idempotency_key: str,
    ) -> tuple[CapacityPoolRecord, bool]:
        """Create or update one declarative autoscaling node type."""

        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint(
            pool.model_copy(update={"generation": 1, "created_at": datetime.min, "updated_at": datetime.min})
        )
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "capacity-pool.upsert", key, fingerprint)
            if existing_key is not None:
                existing = await tx.get_capacity_pool(existing_key.resource_id)
                if existing is None:
                    raise ConflictError("idempotency record points to a missing capacity pool")
                return existing, True
            existing = await tx.get_capacity_pool(pool.id)
            # Authorized inside the transaction, against the stored owner rather
            # than the one the request asserts: reading it outside would leave a
            # window in which a pool changes hands between the check and the write.
            self._require_capacity_administration(
                principal,
                declared_owner=pool.owner_org_id,
                existing_owner=None if existing is None else existing.owner_org_id,
            )
            if existing is not None and pool.owner_org_id != existing.owner_org_id:
                # Reassignment is a fleet act even for a fleet admin coming
                # through this route: it changes who may administer the pool
                # from here on, which deserves its own deliberate call rather
                # than being a side effect of an update that changed max_cells.
                self.authorizer.require(principal, Permission.MANAGE_FLEET)
            # A pool joining (or editing) a shared-quota group must agree with
            # the group it lands in. The planner spends the *minimum* member
            # limit, so a silently disagreeing upsert would tighten the whole
            # group's budget without telling anyone; rejecting it here turns
            # that into a loud, per-pool configuration error instead.
            if stored_quota_group := pool.quota_group:
                peers = [
                    candidate
                    for candidate in await tx.list_capacity_pools(fleet_name=pool.fleet_name)
                    if candidate.quota_group == stored_quota_group and candidate.id != pool.id
                ]
                try:
                    validate_capacity_pool_quota_groups([*peers, pool])
                except ValueError as exc:
                    raise InputError(str(exc)) from exc
            now = utcnow()
            stored = pool.model_copy(
                update={
                    "generation": 1 if existing is None else existing.generation + 1,
                    "created_at": now if existing is None else existing.created_at,
                    "updated_at": now,
                }
            )
            await tx.put_capacity_pool(stored)
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="capacity-pool.upsert",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="capacity_pool",
                    resource_id=stored.id,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="capacity_pool",
                    entity_id=stored.id,
                    action="capacity_pool.created" if existing is None else "capacity_pool.updated",
                    payload={
                        "fleet_name": stored.fleet_name,
                        "node_type": stored.node_type,
                        "generation": stored.generation,
                    },
                )
            )
            return stored, False

    async def apply_fleet(
        self,
        principal: Principal,
        fleet_name: str,
        pools: tp.Sequence[CapacityPoolRecord],
        *,
        idempotency_key: str,
    ) -> tuple[list[CapacityPoolRecord], bool]:
        """Atomically apply every node type in one declarative fleet.

        Fleet administration only, deliberately, even though single-pool
        mutations now accept an owning organization. This is a *declarative*
        apply: pools absent from the request are disabled, so an organization
        admin running it would take down every pool in the fleet they do not
        own. Per-pool ``upsert``/``disable`` are the ownership-scoped surface.
        """

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        if not fleet_name or fleet_name != fleet_name.strip():
            raise InputError("fleet name must be non-empty and trimmed")
        if not pools:
            raise InputError("fleet configuration must contain at least one capacity pool")
        if any(pool.fleet_name != fleet_name for pool in pools):
            raise InputError("capacity pool fleet_name must match the fleet URL")
        if len({pool.id for pool in pools}) != len(pools):
            raise InputError("fleet configuration contains duplicate capacity pool IDs")
        try:
            validate_capacity_pool_quota_groups(pools)
        except ValueError as exc:
            raise InputError(str(exc)) from exc
        key = _validate_idempotency_key(idempotency_key)
        normalized = [
            pool.model_copy(update={"generation": 1, "created_at": datetime.min, "updated_at": datetime.min}).model_dump(
                mode="json"
            )
            for pool in sorted(pools, key=lambda item: item.id)
        ]
        fingerprint = _fingerprint({"fleet_name": fleet_name, "pools": normalized})
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "fleet.apply", key, fingerprint)
            if existing_key is not None:
                return await tx.list_capacity_pools(fleet_name=fleet_name), True
            existing = {pool.id: pool for pool in await tx.list_capacity_pools(fleet_name=fleet_name)}
            now = utcnow()
            supplied_ids = {pool.id for pool in pools}
            for pool in pools:
                previous = existing.get(pool.id)
                # An apply that declares exactly what is already stored changes
                # nothing and must bump nothing. The generation is what cells are
                # fenced against, so bumping it invalidates every cell of the pool
                # -- including requests still in flight at the provider, which are
                # then drained and re-launched. Re-applying an unchanged fleet
                # therefore threw away in-flight capacity and asked for it again:
                # twenty cells in one zone across nineteen hours, one per apply,
                # each granted and released without ever becoming READY.
                #
                # `enabled` is normalised to True on both sides, so re-applying a
                # drained fleet still counts as a change and still bumps, which is
                # what brings its pools back.
                if previous is not None and _pool_declaration(previous) == _pool_declaration(pool):
                    continue
                await tx.put_capacity_pool(
                    pool.model_copy(
                        update={
                            "enabled": True,
                            "initialized_at": None,
                            "generation": 1 if previous is None else previous.generation + 1,
                            "created_at": now if previous is None else previous.created_at,
                            "updated_at": now,
                        }
                    )
                )
            for pool_id, previous in existing.items():
                if pool_id in supplied_ids or not previous.enabled:
                    continue
                await tx.put_capacity_pool(
                    previous.model_copy(
                        update={
                            "enabled": False,
                            "min_cells": 0,
                            "max_cells": 0,
                            # Zeroed with the others, and not an afterthought:
                            # the record requires min <= initial <= max, so a
                            # pool that declared a floor kept `initial_cells` at
                            # 1 and became invalid the moment anything
                            # re-validated it. `model_copy` does not re-run
                            # validators, so the bad record was written silently
                            # and surfaced later as a 422 on an unrelated call --
                            # with the fleet still enabled and still billing.
                            "initial_cells": 0,
                            "generation": previous.generation + 1,
                            "updated_at": now,
                        }
                    )
                )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="fleet.apply",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="fleet",
                    resource_id=fleet_name,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="fleet",
                    entity_id=fleet_name,
                    action="fleet.applied",
                    payload={"capacity_pools": sorted(supplied_ids)},
                )
            )
            return await tx.list_capacity_pools(fleet_name=fleet_name), False

    async def list_submit_fleets(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
    ) -> list[str]:
        """Return sorted runnable fleet names without exposing pool topology."""

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            return sorted(
                {pool.fleet_name for pool in await tx.list_capacity_pools() if pool.enabled and pool.max_cells > 0}
            )

    async def list_capacity_pools(
        self,
        principal: Principal,
        *,
        fleet_name: str | None = None,
    ) -> list[CapacityPoolRecord]:
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            return await tx.list_capacity_pools(fleet_name=fleet_name)

    async def get_capacity_pool(self, principal: Principal, *, pool_id: str) -> CapacityPoolRecord:
        """Return one capacity pool record."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            pool = await tx.get_capacity_pool(pool_id)
            if pool is None:
                raise NotFoundError(f"capacity pool {pool_id!r} was not found")
            return pool

    async def list_capacity(
        self,
        principal: Principal,
        *,
        org_id: str,
        project_id: str,
        accelerator_type: str | None = None,
        capacity_class: CapacityClass | None = None,
    ) -> list[CapacityPoolRecord]:
        """Return enabled pools a caller may submit to, optionally filtered by shape."""

        self.authorizer.require(principal, Permission.SUBMIT, org_id=org_id, project_id=project_id)
        async with self.repository.transaction() as tx:
            pools = await tx.list_capacity_pools()
        result: list[CapacityPoolRecord] = []
        for pool in pools:
            if not pool.enabled or pool.max_cells <= 0:
                continue
            if accelerator_type is not None and pool.accelerator_type != accelerator_type:
                continue
            if capacity_class is not None and pool.capacity_class != capacity_class:
                continue
            result.append(pool)
        return result

    async def disable_capacity_pool(
        self,
        principal: Principal,
        pool_id: str,
        *,
        idempotency_key: str,
    ) -> tuple[CapacityPoolRecord, bool]:
        """Disable a pool so its managed cells drain to zero."""

        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint({"pool_id": pool_id, "enabled": False})
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "capacity-pool.disable", key, fingerprint)
            pool = await tx.get_capacity_pool(pool_id)
            if pool is None:
                raise NotFoundError(f"capacity pool {pool_id!r} was not found")
            self._require_capacity_administration(
                principal,
                declared_owner=pool.owner_org_id,
                existing_owner=pool.owner_org_id,
            )
            if existing_key is not None:
                return pool, True
            stored = pool.model_copy(
                update={
                    "enabled": False,
                    "min_cells": 0,
                    "max_cells": 0,
                    # Same reason as the prune in ``apply_fleet`` and the sweep in
                    # ``disable_fleet``: the record requires min <= initial <= max,
                    # so a pool that declared a floor kept ``initial_cells`` above
                    # the new max of 0 and became invalid on re-read.
                    "initial_cells": 0,
                    "generation": pool.generation + 1,
                    "updated_at": utcnow(),
                }
            )
            await tx.put_capacity_pool(stored)
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="capacity-pool.disable",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="capacity_pool",
                    resource_id=stored.id,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="capacity_pool",
                    entity_id=stored.id,
                    action="capacity_pool.disabled",
                    payload={"fleet_name": stored.fleet_name, "node_type": stored.node_type},
                )
            )
            return stored, False

    async def disable_fleet(
        self,
        principal: Principal,
        fleet_name: str,
        *,
        idempotency_key: str,
    ) -> tuple[list[CapacityPoolRecord], bool]:
        """Disable every pool in a fleet so the autoscaler drains it to zero."""

        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint({"fleet_name": fleet_name, "enabled": False})
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", "fleet.disable", key, fingerprint)
            pools = await tx.list_capacity_pools(fleet_name=fleet_name)
            if not pools:
                raise NotFoundError(f"fleet {fleet_name!r} was not found")
            if existing_key is not None:
                return pools, True
            now = utcnow()
            for pool in pools:
                if not pool.enabled and pool.max_cells == 0:
                    continue
                await tx.put_capacity_pool(
                    pool.model_copy(
                        update={
                            "enabled": False,
                            "min_cells": 0,
                            "max_cells": 0,
                            # Same reason as the prune in ``apply_fleet``: the
                            # record requires min <= initial <= max, so leaving a
                            # floor's ``initial_cells`` at 1 wrote a pool that is
                            # invalid on re-read. ``eyvan down`` then failed with
                            # a 422 naming a field the operator never set, and
                            # the fleet stayed enabled and billing.
                            "initial_cells": 0,
                            "generation": pool.generation + 1,
                            "updated_at": now,
                        }
                    )
                )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation="fleet.disable",
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="fleet",
                    resource_id=fleet_name,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="fleet",
                    entity_id=fleet_name,
                    action="fleet.disabled",
                )
            )
            return await tx.list_capacity_pools(fleet_name=fleet_name), False

    async def list_cells(self, principal: Principal) -> list[CellRecord]:
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            return await tx.list_cells()

    async def get_cell(self, principal: Principal, cell_id: str) -> CellRecord:
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        async with self.repository.transaction() as tx:
            cell = await tx.get_cell(cell_id)
            if cell is None:
                raise NotFoundError(f"cell {cell_id!r} was not found")
            return cell

    async def drain_cell(
        self,
        principal: Principal,
        cell_id: str,
        *,
        idempotency_key: str,
    ) -> tuple[CellRecord, bool]:
        """Stop new placement and ask the cell agent to drain capacity."""

        return await self._change_cell_state(
            principal,
            cell_id,
            target=CellState.DRAINING,
            topic="capacity.drain",
            operation="cell.drain",
            idempotency_key=idempotency_key,
        )

    async def delete_cell(
        self,
        principal: Principal,
        cell_id: str,
        *,
        idempotency_key: str,
    ) -> tuple[CellRecord, bool]:
        """Release provider capacity after every lease has left the cell."""

        return await self._change_cell_state(
            principal,
            cell_id,
            target=CellState.DELETING,
            topic="capacity.release",
            operation="cell.delete",
            idempotency_key=idempotency_key,
            require_empty=True,
            bump_generation=True,
        )

    async def _change_cell_state(
        self,
        principal: Principal,
        cell_id: str,
        *,
        target: CellState,
        topic: str,
        operation: str,
        idempotency_key: str,
        require_empty: bool = False,
        bump_generation: bool = False,
    ) -> tuple[CellRecord, bool]:
        self.authorizer.require(principal, Permission.MANAGE_FLEET)
        key = _validate_idempotency_key(idempotency_key)
        fingerprint = _fingerprint({"cell_id": cell_id, "target": target.value})
        async with self.repository.transaction() as tx:
            existing_key = await self._idempotent_resource(tx, "fleet", operation, key, fingerprint)
            cell = await tx.get_cell(cell_id)
            if cell is None:
                raise NotFoundError(f"cell {cell_id!r} was not found")
            if existing_key is not None:
                return cell, True
            if require_empty:
                active = [
                    lease
                    for lease in await tx.list_leases()
                    if lease.cell_id == cell_id and lease.state not in {LeaseState.RELEASED, LeaseState.EXPIRED}
                ]
                if active:
                    raise ConflictError(f"cell {cell_id!r} still owns {len(active)} active lease(s)")
            stored = cell.model_copy(
                update={
                    "state": target,
                    "generation": cell.generation + int(bump_generation),
                    "updated_at": utcnow(),
                }
            )
            await tx.put_cell(stored)
            await self._outbox(
                tx,
                topic=topic,
                aggregate_type="cell",
                aggregate_id=stored.id,
                idempotency_key=f"{operation}:{stored.id}:{stored.generation}",
                generation=stored.generation,
                payload={"cell_id": stored.id},
            )
            await tx.put_idempotency(
                IdempotencyRecord(
                    scope="fleet",
                    operation=operation,
                    key=key,
                    fingerprint=fingerprint,
                    resource_type="cell",
                    resource_id=stored.id,
                )
            )
            await tx.append_event(
                AuditEvent(
                    id=_new_id("evt"),
                    actor=principal.subject,
                    entity_type="cell",
                    entity_id=stored.id,
                    action=topic,
                    previous_state=cell.state.value,
                    new_state=target.value,
                )
            )
            return stored, False
