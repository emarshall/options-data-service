"""
On-the-fly aggregation for the query API.

The API serves `agg=5m/15m/30m/1h/1d/1w` by aggregating `option_bars_1m` /
`underlying_bars_1m` at query time with TimescaleDB's `time_bucket()` +
`first()`/`last()` aggregates, rather than reading from pre-materialized
continuous aggregates. Migration 0005 dropped those caggs (they cost
~6.7 GB and ~45% of the database while nothing actually read them — the
monitors and backtester only ever query 1m/1d) — this is the replacement.

**Why the aggregation isn't simply `func.time_bucket`:** TimescaleDB's
`time_bucket(interval, ts)` needs an actual `interval`, and rendering the
string as a normal bind parameter leaves Postgres to infer the type, which
is ambiguous (time_bucket is overloaded) and fails under asyncpg's prepared
statements. The `time_bucket` FunctionElement below renders the interval as
a literal on Postgres (`INTERVAL '5 minutes'`) instead. It carries a
SQLite-specific compilation too (`time_bucket('5 minutes', ts)`) so the test
suite can execute these queries end-to-end against SQLite with registered
shim functions — see tests/test_api.py.

**Aggregation semantics mirror the old caggs exactly** (see the now-deleted
alembic/versions/0002_continuous_aggregates.py): true OHLC (first/max/min/
last); last-value-in-bucket for Greeks and open_interest (point-in-time
snapshots, not additive); sum for volume/bid_volume/ask_volume; and a real
volume-weighted average for vwap (`sum(vwap*volume)/sum(volume)`, NULL when
there's no volume) rather than a naive `avg(vwap)`.
"""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import DateTime, and_, case, func, select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from service.db.models import OptionBar1m, UnderlyingBar1m

AGG_PERIODS = ("1m", "5m", "15m", "30m", "1h", "1d", "1w")

# agg -> the interval literal handed to TimescaleDB's time_bucket().
_BUCKET_INTERVALS = {
    "5m": "5 minutes",
    "15m": "15 minutes",
    "30m": "30 minutes",
    "1h": "1 hour",
    "1d": "1 day",
    "1w": "1 week",
}


class time_bucket(FunctionElement):
    """TimescaleDB's `time_bucket(interval, ts)`, with the interval rendered
    as a literal rather than a bind parameter (see module docstring). Typed
    as a tz-aware DateTime so both asyncpg and SQLite's DateTime result
    processor hand the route a real `datetime` rather than a raw value."""

    name = "time_bucket"
    inherit_cache = True
    type = DateTime(timezone=True)


def _interval_and_column(element):
    """`time_bucket("5 minutes", col)` stores its first argument as a bound
    parameter; recover the raw interval string and the column expression
    from the compiled element's clauses."""
    interval_param, column = element.clauses
    return interval_param.value, column


@compiles(time_bucket)
def _compile_time_bucket(element, compiler, **kw):
    """Postgres/TimescaleDB (and any dialect without its own override):
    render the interval as a real INTERVAL literal."""
    interval, column = _interval_and_column(element)
    return f"time_bucket(INTERVAL '{interval}', {compiler.process(column, **kw)})"


@compiles(time_bucket, "sqlite")
def _compile_time_bucket_sqlite(element, compiler, **kw):
    """Test-only form: a plain two-argument call, satisfied by the shim
    function tests register on their SQLite engine."""
    interval, column = _interval_and_column(element)
    return f"time_bucket('{interval}', {compiler.process(column, **kw)})"


def _bucket(agg: str, column):
    return time_bucket(_BUCKET_INTERVALS[agg], column)


def build_option_bars_query(
    agg: str,
    *,
    ticker: str,
    start,
    end,
    right=None,
    min_delta: float | None = None,
    max_delta: float | None = None,
    expiration=None,
    contract_id: str | None = None,
    limit: int,
    offset: int,
):
    """Builds the SELECT for GET /options/bars. For `agg="1m"` this selects
    the raw hypertable directly; anything coarser is GROUP BY'd on a
    `time_bucket` of the same table."""
    t = OptionBar1m
    conditions = [t.underlying_ticker == ticker, t.time >= start, t.time <= end]
    if right is not None:
        conditions.append(t.right == right)
    if min_delta is not None:
        conditions.append(t.delta >= min_delta)
    if max_delta is not None:
        conditions.append(t.delta <= max_delta)
    if expiration is not None:
        conditions.append(t.expiration_date == expiration)
    if contract_id is not None:
        conditions.append(t.contract_id == contract_id)

    if agg == "1m":
        return (
            select(t.__table__)
            .where(and_(*conditions))
            .order_by(t.time.asc())
            .limit(limit)
            .offset(offset)
        )

    bucket = _bucket(agg, t.time).label("time")
    volume = func.sum(t.volume)
    vwap = (
        case((volume > 0, func.sum(t.vwap * t.volume) / volume), else_=None)
        .label("vwap")
    )
    stmt = (
        select(
            bucket,
            t.contract_id,
            t.underlying_ticker,
            t.expiration_date,
            t.strike,
            t.right,
            func.first(t.open, t.time).label("open"),
            func.max(t.high).label("high"),
            func.min(t.low).label("low"),
            func.last(t.close, t.time).label("close"),
            func.last(t.bid, t.time).label("bid"),
            func.last(t.ask, t.time).label("ask"),
            func.last(t.delta, t.time).label("delta"),
            func.last(t.gamma, t.time).label("gamma"),
            func.last(t.theta, t.time).label("theta"),
            func.last(t.vega, t.time).label("vega"),
            func.last(t.rho, t.time).label("rho"),
            func.last(t.iv, t.time).label("iv"),
            func.last(t.greeks_source, t.time).label("greeks_source"),
            volume.label("volume"),
            func.last(t.open_interest, t.time).label("open_interest"),
            vwap,
            func.sum(t.bid_volume).label("bid_volume"),
            func.sum(t.ask_volume).label("ask_volume"),
        )
        .where(and_(*conditions))
        .group_by(
            bucket,
            t.contract_id,
            t.underlying_ticker,
            t.expiration_date,
            t.strike,
            t.right,
        )
        .order_by(bucket.asc())
        .limit(limit)
        .offset(offset)
    )
    return stmt


def build_underlying_bars_query(
    agg: str,
    *,
    ticker: str,
    start,
    end,
    limit: int,
    offset: int,
):
    """Same idea as build_option_bars_query, for GET /underlying/bars —
    OHLCV grouped by `(time_bucket, ticker)`."""
    t = UnderlyingBar1m
    conditions = [t.ticker == ticker, t.time >= start, t.time <= end]

    if agg == "1m":
        return (
            select(t.__table__)
            .where(and_(*conditions))
            .order_by(t.time.asc())
            .limit(limit)
            .offset(offset)
        )

    bucket = _bucket(agg, t.time).label("time")
    stmt = (
        select(
            bucket,
            t.ticker,
            func.first(t.open, t.time).label("open"),
            func.max(t.high).label("high"),
            func.min(t.low).label("low"),
            func.last(t.close, t.time).label("close"),
            func.sum(t.volume).label("volume"),
        )
        .where(and_(*conditions))
        .group_by(bucket, t.ticker)
        .order_by(bucket.asc())
        .limit(limit)
        .offset(offset)
    )
    return stmt
