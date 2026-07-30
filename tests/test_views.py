"""
Unit tests for service/db/views.py's table-selection logic. Pure logic,
no DB needed — the actual query behavior against these tables is covered
in tests/test_api.py (SQLite, structural) and validated separately against
real Postgres (see PLAN.md Task 7/8 notes).
"""

import pytest

from service.db.models import OptionBar1m, UnderlyingBar1m
from service.db.views import (
    OPTION_BARS_VIEW_TABLES,
    UNDERLYING_BARS_VIEW_TABLES,
    get_option_bars_table,
    get_underlying_bars_table,
)


def test_get_option_bars_table_1m_returns_real_hypertable():
    assert get_option_bars_table("1m") is OptionBar1m.__table__


@pytest.mark.parametrize("agg", ["5m", "15m", "30m", "1h", "1d", "1w"])
def test_get_option_bars_table_view_periods(agg):
    assert get_option_bars_table(agg) is OPTION_BARS_VIEW_TABLES[agg]
    assert get_option_bars_table(agg).name == f"option_bars_{agg}"


def test_get_option_bars_table_invalid_agg_raises():
    with pytest.raises(ValueError):
        get_option_bars_table("3m")


def test_get_underlying_bars_table_1m_returns_real_hypertable():
    assert get_underlying_bars_table("1m") is UnderlyingBar1m.__table__


@pytest.mark.parametrize("agg", ["5m", "15m", "30m", "1h", "1d", "1w"])
def test_get_underlying_bars_table_view_periods(agg):
    assert get_underlying_bars_table(agg) is UNDERLYING_BARS_VIEW_TABLES[agg]
    assert get_underlying_bars_table(agg).name == f"underlying_bars_{agg}"


def test_get_underlying_bars_table_invalid_agg_raises():
    with pytest.raises(ValueError):
        get_underlying_bars_table("3m")


def test_option_and_underlying_view_tables_cover_the_same_periods():
    assert set(OPTION_BARS_VIEW_TABLES.keys()) == set(UNDERLYING_BARS_VIEW_TABLES.keys())
