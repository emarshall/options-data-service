"""greeks_source partial index

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-29

Adds a partial index on `option_bars_1m (time, contract_id)` WHERE
`greeks_source IS NULL`.

The `greeks-backfill` job selects candidate rows with exactly that
predicate, once per batch, for as many batches as the backlog has rows.
With no supporting index, each of those selections seq-scans the
hypertable chunk by chunk — the plan showed 18 chunks being considered to
satisfy a `LIMIT 500`. The partial index turns that into a direct index
scan over just the unfinished rows, and its leading column matches the
job's `ORDER BY time, contract_id` so the keyset cursor can use it for
paging without a sort.

Partial rather than a full index on `greeks_source` because the column is
NULL for the overwhelming majority of rows right now (6.0M of 23.8M) and
`IS NULL` doesn't benefit from a plain btree the way an equality lookup
would. As the backfill drains, the index shrinks with it.

Note: TimescaleDB propagates indexes on a hypertable to its chunks
automatically, and — unlike a unique index — a partial index needs no
extra per-chunk bookkeeping. `create_index` is used rather than raw SQL
so the name matches the convention the other indexes follow.
"""

from typing import Sequence, Union

import sqlalchemy
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_PENDING = sqlalchemy.text("greeks_source IS NULL")


def upgrade() -> None:
    op.create_index(
        "ix_option_bars_1m_pending_greeks",
        "option_bars_1m",
        ["time", "contract_id"],
        unique=False,
        postgresql_where=_PENDING,
        sqlite_where=_PENDING,
    )


def downgrade() -> None:
    op.drop_index("ix_option_bars_1m_pending_greeks", table_name="option_bars_1m")
