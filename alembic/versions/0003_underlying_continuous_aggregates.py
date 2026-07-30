"""underlying bar continuous aggregates

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-24

Adds TimescaleDB continuous aggregates on `underlying_bars_1m` for
5m/15m/30m/1h/1d/1w — the same treatment migration 0002 gave
`option_bars_1m`, extended to the underlying now that Task 8's query API
needs to serve non-1m underlying bars too. Compression for
`underlying_bars_1m` was already set up in migration 0002 (it compresses
both bar hypertables together) — nothing more needed there.

**Same caveat as migration 0002 applies: this could not be tested against
a real TimescaleDB instance** (the extension isn't installable in the
environment this was built in — see migration 0002's docstring and
PLAN.md Section 7 for the full explanation). Carefully reasoned, not
live-verified.

Much simpler than the option bars aggregates: no enum columns (so none of
migration 0002's `"right"`-quoting concern applies here), no Greeks, no
per-contract dimension — just OHLCV grouped by `(time_bucket, ticker)`.
Same `start_offset` reasoning as migration 0002: uniformly wide (65 days)
regardless of bucket size, since Task 5's backfill can insert historical
underlying bars up to 60 days back at any time, not just once at startup.
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Same shape as migration 0002's _PERIODS — kept as a separate copy rather
# than importing from 0002, since Alembic migrations are meant to stand
# alone (importing between revision files is a known footgun if either
# ever gets edited independently after the fact).
_PERIODS = [
    ("5m", "5 minutes", "1 minute", "2 minutes"),
    ("15m", "15 minutes", "5 minutes", "10 minutes"),
    ("30m", "30 minutes", "10 minutes", "20 minutes"),
    ("1h", "1 hour", "15 minutes", "30 minutes"),
    ("1d", "1 day", "30 minutes", "1 hour"),
    ("1w", "1 week", "2 hours", "4 hours"),
]

_START_OFFSET = "65 days"  # see migration 0002's docstring for the reasoning


def _view_name(suffix: str) -> str:
    return f"underlying_bars_{suffix}"


def _create_view_sql(suffix: str, bucket: str) -> str:
    view = _view_name(suffix)
    return f"""
        CREATE MATERIALIZED VIEW {view}
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('{bucket}', time) AS time,
            ticker,
            first(open, time) AS open,
            max(high) AS high,
            min(low) AS low,
            last(close, time) AS close,
            sum(volume) AS volume
        FROM underlying_bars_1m
        GROUP BY time_bucket('{bucket}', time), ticker
        WITH NO DATA
    """


def upgrade() -> None:
    for suffix, bucket, _schedule, _end_offset in _PERIODS:
        op.execute(_create_view_sql(suffix, bucket))

    with op.get_context().autocommit_block():
        for suffix, _bucket, schedule, end_offset in _PERIODS:
            view = _view_name(suffix)
            op.execute(
                f"SELECT add_continuous_aggregate_policy('{view}', "
                f"start_offset => INTERVAL '{_START_OFFSET}', "
                f"end_offset => INTERVAL '{end_offset}', "
                f"schedule_interval => INTERVAL '{schedule}')"
            )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for suffix, _bucket, _schedule, _end_offset in _PERIODS:
            view = _view_name(suffix)
            op.execute(f"SELECT remove_continuous_aggregate_policy('{view}', if_exists => true)")

    for suffix, _bucket, _schedule, _end_offset in _PERIODS:
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {_view_name(suffix)}")
