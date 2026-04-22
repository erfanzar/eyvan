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
"""PostgreSQL advisory-lock leadership for the global scheduler."""

from __future__ import annotations

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

#: Verify this backend session still *holds* the advisory lock, rather than
#: merely that its connection is alive. ``pg_try_advisory_lock`` is re-entrant,
#: so re-calling it would succeed and silently increment the lock counter; and a
#: bare ``SELECT 1`` proves only that some connection answered.
#:
#: The bigint form of an advisory lock is stored split across ``classid`` (high
#: 32 bits) and ``objid`` (low 32 bits) with ``objsubid = 1``.
_LOCK_OWNERSHIP_SQL = """
SELECT EXISTS (
    SELECT 1
    FROM pg_locks
    WHERE locktype = 'advisory'
      AND granted
      AND pid = pg_backend_pid()
      AND objsubid = 1
      AND ((classid::bigint << 32) | objid::bigint) = :lock_id
)
"""


class PostgresAdvisoryLeader:
    """Hold a session-scoped advisory lock on one dedicated connection.

    **Deployment constraint.** Session-scoped advisory locks require a stable
    backend session. Behind a transaction-pooling proxy (PgBouncer in
    ``transaction`` mode) a connection can be handed to a different backend
    between statements, so the lock this object believes it holds may live in a
    session it no longer owns. ``acquire`` re-verifies ownership on every renewal
    precisely so that situation demotes this replica instead of producing two
    leaders -- but the supported deployment is a direct connection or a
    session-pooling proxy.
    """

    def __init__(self, engine: AsyncEngine, *, lock_id: int = 0x4D4552494449414E):
        self.engine = engine
        self.lock_id = lock_id
        self._connection: AsyncConnection | None = None
        self._guard = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._guard:
            if self._connection is not None:
                try:
                    still_owned = bool(
                        await self._connection.scalar(text(_LOCK_OWNERSHIP_SQL), {"lock_id": self.lock_id})
                    )
                except Exception:
                    connection, self._connection = self._connection, None
                    await connection.close()
                else:
                    if still_owned:
                        return True
                    # The session is alive but no longer holds the lock. Drop it
                    # and fall through to a clean re-acquisition attempt: another
                    # replica may legitimately be leader now.
                    connection, self._connection = self._connection, None
                    await connection.close()
            connection = await self.engine.connect()
            connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
            try:
                acquired = bool(
                    await connection.scalar(text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": self.lock_id})
                )
            except Exception:
                await connection.close()
                raise
            if not acquired:
                await connection.close()
                return False
            self._connection = connection
            return True

    async def release(self) -> None:
        async with self._guard:
            if self._connection is None:
                return
            connection, self._connection = self._connection, None
            try:
                await connection.execute(text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": self.lock_id})
            finally:
                await connection.close()
