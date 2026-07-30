"""
Lightweight SQLAlchemy Core `Table` definitions for the read-only
continuous aggregate views created in
alembic/versions/0002_continuous_aggregates.py and
0003_underlying_continuous_aggregates.py (Task 7).

These are deliberately NOT ORM-mapped classes — nothing ever inserts into
a continuous aggregate view, TimescaleDB's refresh policy populates them —
this is just enough Core metadata for the query API (Task 8) to build
safe, correctly-typed SELECT queries against them.

**Why the column types matter, specifically for `right`/`greeks_source`:**
using a bare/untyped column for either would risk a real class of bug —
comparing an untyped bound parameter against a native Postgres enum column
can fail with "operator does not exist" depending on how the driver types
the parameter. Confirmed directly against a real Postgres instance before
relying on this: matching the exact typed `Enum(..., values_callable=...)`
column definitions used in service/db/models.py (not a plain string
column) avoids that entirely — see PLAN.md Section 7 for the enum
serialization bug this project already hit once, which is exactly the
kind of thing that reappears if these two schemas drift apart. (The
underlying bar views have no enum columns at all, so this specific concern
doesn't apply to them — flagged here only because it's why the option bar
views below are typed the way they are.)
"""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, Date, DateTime, Enum, MetaData, Numeric, String, Table

from service.db.models import GreeksSource, OptionBar1m, OptionRight, UnderlyingBar1m, _enum_values

_metadata = MetaData()

AGG_PERIODS = ("1m", "5m", "15m", "30m", "1h", "1d", "1w")
_VIEW_PERIODS = ("5m", "15m", "30m", "1h", "1d", "1w")  # "1m" is the real hypertable, not a view


def _make_option_bars_view_table(suffix: str) -> Table:
    return Table(
        f"option_bars_{suffix}",
        _metadata,
        Column("time", DateTime(timezone=True)),
        Column("contract_id", String),
        Column("underlying_ticker", String),
        Column("expiration_date", Date),
        Column("strike", Numeric(12, 4)),
        Column("right", Enum(OptionRight, name="option_right", values_callable=_enum_values)),
        Column("open", Numeric(12, 4)),
        Column("high", Numeric(12, 4)),
        Column("low", Numeric(12, 4)),
        Column("close", Numeric(12, 4)),
        Column("bid", Numeric(12, 4)),
        Column("ask", Numeric(12, 4)),
        Column("delta", Numeric(9, 6)),
        Column("gamma", Numeric(11, 8)),
        Column("theta", Numeric(11, 6)),
        Column("vega", Numeric(11, 6)),
        Column("rho", Numeric(11, 8)),
        Column("iv", Numeric(9, 6)),
        Column("greeks_source", Enum(GreeksSource, name="greeks_source", values_callable=_enum_values)),
        Column("volume", BigInteger),
        Column("open_interest", BigInteger),
        Column("vwap", Numeric(12, 6)),
        Column("bid_volume", BigInteger),
        Column("ask_volume", BigInteger),
    )


def _make_underlying_bars_view_table(suffix: str) -> Table:
    return Table(
        f"underlying_bars_{suffix}",
        _metadata,
        Column("time", DateTime(timezone=True)),
        Column("ticker", String),
        Column("open", Numeric(12, 4)),
        Column("high", Numeric(12, 4)),
        Column("low", Numeric(12, 4)),
        Column("close", Numeric(12, 4)),
        Column("volume", BigInteger),
    )


OPTION_BARS_VIEW_TABLES = {suffix: _make_option_bars_view_table(suffix) for suffix in _VIEW_PERIODS}
UNDERLYING_BARS_VIEW_TABLES = {suffix: _make_underlying_bars_view_table(suffix) for suffix in _VIEW_PERIODS}


def get_option_bars_table(agg: str):
    """Returns the appropriate table for the given aggregation period:
    the real ORM-mapped hypertable for "1m", or the matching continuous
    aggregate view's lightweight Core Table for anything coarser. Raises
    ValueError for anything not in AGG_PERIODS."""
    if agg == "1m":
        return OptionBar1m.__table__
    if agg in OPTION_BARS_VIEW_TABLES:
        return OPTION_BARS_VIEW_TABLES[agg]
    raise ValueError(f"Unsupported aggregation period: {agg!r}. Valid: {AGG_PERIODS}")


def get_underlying_bars_table(agg: str):
    """Same idea as get_option_bars_table, for underlying_bars_1m and its
    Task 7 (migration 0003) continuous aggregate views."""
    if agg == "1m":
        return UnderlyingBar1m.__table__
    if agg in UNDERLYING_BARS_VIEW_TABLES:
        return UNDERLYING_BARS_VIEW_TABLES[agg]
    raise ValueError(f"Unsupported aggregation period: {agg!r}. Valid: {AGG_PERIODS}")
