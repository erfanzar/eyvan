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
"""Add declarative capacity pools for demand autoscaling.

Revision ID: 0003_capacity_pools
Revises: 0002_transactional_outbox
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0003_capacity_pools"
down_revision = "0002_transactional_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "capacity_pools",
        sa.Column("id", sa.String(192), primary_key=True),
        sa.Column("data", sa.JSON(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("capacity_pools")
