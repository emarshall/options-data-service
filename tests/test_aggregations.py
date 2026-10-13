"""
Unit tests for service/db/aggregations.py's query builders. Pure logic —
compiles the generated SQL and asserts on its structure (which expressions
the bucket/OHLC/Greeks use). The *behavior* of the resulting query (actual
aggregated values) is covered end-to-end in tests/test_api.py (SQLite, with
shim functions) and validated against real TimescaleDB separately.
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy.dialects import postgresql

from service.db.aggregations import (
    AGG_PERIODS,
    build_option_bars_query,
    build_underlying_bars_query,
)

_COARSE = ["5m", "15m", "30m", "1h", "1d", "1w"]
# agg -> the interval literal expected in the compiled time_bucket() call.
_INTERVALS = {
    "5m": "5 minutes",
    "15m": "15 minutes",
    "30m": "30 minutes",
    "1h": "1 hour",
    "1d": "1 day",
    "1w": "1 week",
}

_T0 = datetime(2026, 10, 12, 14, 30, tzinfo=timezone.utc)
_T1 = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)


def _compile(stmt) -> str:
    return str(stmt.compile(dialect=postgresql.dialect()))


def _option_query(agg):
    return build_option_bars_query(
        agg,
        ticker="SPX",
        start=_T0,
        end=_T1,
        limit=10,
        offset=0,
    )


def test_agg_periods_includes_1m_and_all_coarse_buckets():
    assert "1m" in AGG_PERIODS
    assert set(_COARSE).issubset(set(AGG_PERIODS))


def test_option_bars_1m_selects_raw_hypertable_not_aggregated():
    sql = _compile(_option_query("1m"))
    assert "time_bucket" not in sql
    assert "GROUP BY" not in sql
    assert "first(" not in sql
    assert "option_bars_1m" in sql


@pytest.mark.parametrize("agg", _COARSE)
def test_option_bars_coarse_agg_uses_time_bucket_and_group_by(agg):
    sql = _compile(_option_query(agg))
    assert f"time_bucket(INTERVAL '{_INTERVALS[agg]}', option_bars_1m.time)" in sql
    assert "GROUP BY" in sql
    # OHLC + snapshot semantics mirror the dropped continuous aggregates.
    assert "first(option_bars_1m.open, option_bars_1m.time)" in sql
    assert "max(option_bars_1m.high)" in sql
    assert "min(option_bars_1m.low)" in sql
    assert "last(option_bars_1m.close, option_bars_1m.time)" in sql
    assert "last(option_bars_1m.delta, option_bars_1m.time)" in sql
    assert "last(option_bars_1m.greeks_source, option_bars_1m.time)" in sql
    assert "sum(option_bars_1m.volume)" in sql


def test_option_bars_filters_are_present():
    stmt = build_option_bars_query(
        "5m",
        ticker="SPX",
        start=_T0,
        end=_T1,
        right=None,
        min_delta=0.3,
        max_delta=0.5,
        expiration=None,
        contract_id=".SPXW261012C7770",
        limit=10,
        offset=0,
    )
    sql = _compile(stmt)
    assert "option_bars_1m.underlying_ticker =" in sql
    assert "option_bars_1m.delta >=" in sql
    assert "option_bars_1m.delta <=" in sql
    assert "option_bars_1m.contract_id =" in sql


def test_option_bars_ordering_is_by_bucket():
    sql = _compile(_option_query("5m"))
    assert "ORDER BY" in sql


def test_option_bars_invalid_agg_raises():
    with pytest.raises(KeyError):
        _option_query("3m")


def _underlying_query(agg):
    return build_underlying_bars_query(agg, ticker="SPX", start=_T0, end=_T1, limit=10, offset=0)


def test_underlying_bars_1m_selects_raw_hypertable_not_aggregated():
    sql = _compile(_underlying_query("1m"))
    assert "time_bucket" not in sql
    assert "GROUP BY" not in sql
    assert "underlying_bars_1m" in sql


@pytest.mark.parametrize("agg", _COARSE)
def test_underlying_bars_coarse_agg_uses_time_bucket(agg):
    sql = _compile(_underlying_query(agg))
    assert f"time_bucket(INTERVAL '{_INTERVALS[agg]}', underlying_bars_1m.time)" in sql
    assert "first(underlying_bars_1m.open, underlying_bars_1m.time)" in sql
    assert "last(underlying_bars_1m.close, underlying_bars_1m.time)" in sql
    assert "GROUP BY" in sql


def test_underlying_bars_invalid_agg_raises():
    with pytest.raises(KeyError):
        _underlying_query("3m")
