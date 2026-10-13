"""drop continuous aggregates

Revision ID: 0005
Revises: 0004
Create Date: 2026-10-14

Drops the 12 TimescaleDB continuous aggregates created in migrations 0002
(option_bars_5m/15m/30m/1h/1d/1w) and 0003 (underlying_bars_*, same six).
They cost ~6.7 GB — ~45% of the database — while nothing actually read
them: the on-the-fly monitors only ever query 1m, and the backtester reads
1m plus underlying_bars_1d. The query API now aggregates coarser periods
from option_bars_1m / underlying_bars_1m at request time instead (see
service/db/aggregations.py), so the materialized copies are pure overhead.

The base-table compression policies stay in place — this only removes the
refresh policies and the views themselves, not compression.

NOTE (same caveat as 0002/0003, in reverse): this couldn't be exercised
against a real TimescaleDB instance during development. `downgrade()`
recreates the aggregates exactly as 0002/0003 defined them, so this is
reversible, but the recreation path also hasn't been run live.
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# (view_name_suffix, time_bucket interval, refresh schedule_interval, end_offset)
# Copied from migrations 0002/0003 rather than imported — Alembic revisions
# are meant to stand alone (see 0003's note).
_PERIODS = [
    ("5m", "5 minutes", "1 minute", "2 minutes"),
    ("15m", "15 minutes", "5 minutes", "10 minutes"),
    ("30m", "30 minutes", "10 minutes", "20 minutes"),
    ("1h", "1 hour", "15 minutes", "30 minutes"),
    ("1d", "1 day", "30 minutes", "1 hour"),
    ("1w", "1 week", "2 hours", "4 hours"),
]

_START_OFFSET = "65 days"  # margin above backfill's 60-day lookback — see 0002


def _option_view(suffix: str) -> str:
    return f"option_bars_{suffix}"


def _underlying_view(suffix: str) -> str:
    return f"underlying_bars_{suffix}"


def _option_view_sql(suffix: str, bucket: str) -> str:
    # "right" must stay double-quoted — it's a keyword in Postgres (see 0002).
    view = _option_view(suffix)
    return f"""
        CREATE MATERIALIZED VIEW {view}
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket('{bucket}', time) AS time,
            contract_id,
            underlying_ticker,
            expiration_date,
            strike,
            "right",
            first(open, time) AS open,
            max(high) AS high,
            min(low) AS low,
            last(close, time) AS close,
            last(bid, time) AS bid,
            last(ask, time) AS ask,
            last(delta, time) AS delta,
            last(gamma, time) AS gamma,
            last(theta, time) AS theta,
            last(vega, time) AS vega,
            last(rho, time) AS rho,
            last(iv, time) AS iv,
            last(greeks_source, time) AS greeks_source,
            sum(volume) AS volume,
            last(open_interest, time) AS open_interest,
            CASE WHEN sum(volume) > 0
                THEN sum(vwap * volume) / sum(volume)
                ELSE NULL
            END AS vwap,
            sum(bid_volume) AS bid_volume,
            sum(ask_volume) AS ask_volume
        FROM option_bars_1m
        GROUP BY time_bucket('{bucket}', time), contract_id, underlying_ticker,
                 expiration_date, strike, "right"
        WITH NO DATA
    """


def _underlying_view_sql(suffix: str, bucket: str) -> str:
    view = _underlying_view(suffix)
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
    # Policy management is documented to sometimes require non-transactional
    # execution — same defensive autocommit block as 0002/0003.
    with op.get_context().autocommit_block():
        for suffix, _bucket, _schedule, _end_offset in _PERIODS:
            op.execute(
                f"SELECT remove_continuous_aggregate_policy('{_option_view(suffix)}', if_exists => true)"
            )
            op.execute(
                f"SELECT remove_continuous_aggregate_policy('{_underlying_view(suffix)}', if_exists => true)"
            )

    for suffix, _bucket, _schedule, _end_offset in _PERIODS:
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {_option_view(suffix)}")
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {_underlying_view(suffix)}")


def downgrade() -> None:
    for suffix, bucket, _schedule, _end_offset in _PERIODS:
        op.execute(_option_view_sql(suffix, bucket))
        op.execute(_underlying_view_sql(suffix, bucket))

    with op.get_context().autocommit_block():
        for suffix, _bucket, schedule, end_offset in _PERIODS:
            for view in (_option_view(suffix), _underlying_view(suffix)):
                op.execute(
                    f"SELECT add_continuous_aggregate_policy('{view}', "
                    f"start_offset => INTERVAL '{_START_OFFSET}', "
                    f"end_offset => INTERVAL '{end_offset}', "
                    f"schedule_interval => INTERVAL '{schedule}')"
                )
