"""continuous aggregates and compression policies

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-19

Adds TimescaleDB continuous aggregates on `option_bars_1m` for 5m/15m/30m/
1h/1d/1w, plus compression policies on both bar hypertables. See PLAN.md
Task 7 for the full design writeup — a few decisions worth knowing before
reading the SQL below:

**IMPORTANT — unlike every other migration in this repo, this one could
NOT be tested against a real TimescaleDB instance.** The TimescaleDB
extension itself isn't installable in the sandbox this was developed in
(confirmed back in Task 1 — it requires Timescale's own package repo).
Migration 0001's hypertable/enum SQL went through several real rounds of
"run it, hit an error, fix it" against an actual TimescaleDB container —
this one hasn't had that chance yet. Treat it as carefully-reasoned but
higher-risk than the rest of the codebase; run `alembic upgrade head` (or
`docker compose up`) and expect that it may need iteration, the same way
migration 0001 did.

**Why every policy uses the same generous ~65-75 day window, regardless of
the view's own bucket size:** Task 5's backfill can insert historical bars
up to `DEFAULT_LOOKBACK_DAYS` (60, see service/ingestion/backfill.py) in
the past, at any time — not just once at startup. A continuous aggregate's
refresh policy only re-examines a *moving window* relative to "now"
(`start_offset` back from "now"); if that window were narrower than the
backfill lookback, backfilled data older than the window would silently
never make it into the aggregated views. So `start_offset` is set to 65
days (60 + margin) on every policy here, not tuned per-view. TimescaleDB's
invalidation-log-based incremental refresh means a wide `start_offset` is
cheap in practice — it doesn't force a full re-scan, it only touches time
ranges actually flagged as changed since the last refresh, so this isn't
the storage/compute tradeoff it might look like at first glance.

**Why compression is delayed to 75 days, not the "7-14 days" example
originally sketched in the plan draft:** the same backfill window creates
a real conflict — if chunks compressed at (say) 14 days old, any backfill
run touching data 15-60 days back would be inserting into already-
compressed chunks. TimescaleDB versions differ in how gracefully they
handle that (older versions effectively required decompression first;
newer ones handle it more transparently but it's still slower and a
sharper edge than necessary). Simplest, safest fix: don't compress
anything until it's safely past the entire backfill window. This trades
some near-term storage savings for avoiding that whole class of problem —
worth revisiting once real data volume is observed (this was already an
explicit open question in the original plan).

**Each aggregate is built directly from `option_bars_1m`, not chained from
a coarser aggregate onto a finer one** (TimescaleDB supports "aggregates
on aggregates," which would be marginally more efficient for the 1w view
in particular). Skipped deliberately: that feature has had version-
specific edge cases historically, and this can't be tested live here to
confirm which side of those edges the actual deployed TimescaleDB version
falls on. Simpler and more likely to just work, at a small efficiency cost
revisitable later.

**Aggregation choice per column:** true OHLC (first/max/min/last) for
price columns; last-value-in-bucket for Greeks and open_interest (both are
point-in-time snapshots, not additive); sum for volume/bid_volume/
ask_volume (additive counts); a genuine volume-weighted average for vwap
(`sum(vwap * volume) / sum(volume)`, not a naive `avg(vwap)` — a plain
average would misrepresent it whenever volume varies bar to bar).
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# (view_name_suffix, time_bucket interval, refresh schedule_interval, end_offset)
# start_offset is uniform across all of these — see module docstring.
_PERIODS = [
    ("5m", "5 minutes", "1 minute", "2 minutes"),
    ("15m", "15 minutes", "5 minutes", "10 minutes"),
    ("30m", "30 minutes", "10 minutes", "20 minutes"),
    ("1h", "1 hour", "15 minutes", "30 minutes"),
    ("1d", "1 day", "30 minutes", "1 hour"),
    ("1w", "1 week", "2 hours", "4 hours"),
]

_START_OFFSET = "65 days"  # margin above backfill's 60-day lookback — see module docstring
_COMPRESS_AFTER = "75 days"  # margin above _START_OFFSET — see module docstring


def _view_name(suffix: str) -> str:
    return f"option_bars_{suffix}"


def _create_view_sql(suffix: str, bucket: str) -> str:
    view = _view_name(suffix)
    # NOTE: "right" is double-quoted throughout — unlike migration 0001
    # (which used SQLAlchemy Core's op.create_table/sa.Column, both of
    # which auto-quote identifiers), this migration is hand-written raw
    # SQL, so it doesn't get that automatic protection. Postgres's grammar
    # treats RIGHT specially (it's part of RIGHT JOIN syntax), which
    # breaks parsing when it's used unquoted as a bare column reference in
    # a SELECT list — confirmed by actually running this migration and
    # hitting exactly that syntax error (see PLAN.md Section 7).
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


def upgrade() -> None:
    # --- Continuous aggregate views ---
    # WITH NO DATA above: the scheduled policy (added below) populates
    # these incrementally rather than forcing a slow synchronous full
    # materialization as part of running this migration.
    for suffix, bucket, _schedule, _end_offset in _PERIODS:
        op.execute(_create_view_sql(suffix, bucket))

    # --- Refresh + compression policies ---
    # Wrapped in an autocommit block: TimescaleDB's policy-management
    # procedures are documented to sometimes require non-transactional
    # execution (this is the standard pattern for TimescaleDB + Alembic —
    # see e.g. TimescaleDB's own migration examples). Since this couldn't
    # be verified live, this is a defensive choice rather than a confirmed
    # necessity for the specific version that ends up deployed.
    with op.get_context().autocommit_block():
        for suffix, _bucket, schedule, end_offset in _PERIODS:
            view = _view_name(suffix)
            op.execute(
                f"SELECT add_continuous_aggregate_policy('{view}', "
                f"start_offset => INTERVAL '{_START_OFFSET}', "
                f"end_offset => INTERVAL '{end_offset}', "
                f"schedule_interval => INTERVAL '{schedule}')"
            )

        op.execute(
            "ALTER TABLE option_bars_1m SET ("
            "timescaledb.compress, "
            "timescaledb.compress_segmentby = 'contract_id', "
            "timescaledb.compress_orderby = 'time DESC'"
            ")"
        )
        op.execute(f"SELECT add_compression_policy('option_bars_1m', INTERVAL '{_COMPRESS_AFTER}')")

        op.execute(
            "ALTER TABLE underlying_bars_1m SET ("
            "timescaledb.compress, "
            "timescaledb.compress_segmentby = 'ticker', "
            "timescaledb.compress_orderby = 'time DESC'"
            ")"
        )
        op.execute(f"SELECT add_compression_policy('underlying_bars_1m', INTERVAL '{_COMPRESS_AFTER}')")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SELECT remove_compression_policy('underlying_bars_1m', if_exists => true)")
        op.execute("SELECT remove_compression_policy('option_bars_1m', if_exists => true)")
        for suffix, _bucket, _schedule, _end_offset in _PERIODS:
            view = _view_name(suffix)
            op.execute(f"SELECT remove_continuous_aggregate_policy('{view}', if_exists => true)")

    for suffix, _bucket, _schedule, _end_offset in _PERIODS:
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {_view_name(suffix)}")

    # Note: this disables compression for *future* chunks but does not
    # decompress chunks that were already compressed — fully reversing
    # that requires an explicit per-chunk decompress step, out of scope
    # for this downgrade path. Not expected to matter in practice (nobody
    # downgrades a production DB with months of compressed data), but
    # worth knowing if this downgrade is ever actually run for real.
    op.execute("ALTER TABLE option_bars_1m SET (timescaledb.compress = false)")
    op.execute("ALTER TABLE underlying_bars_1m SET (timescaledb.compress = false)")
