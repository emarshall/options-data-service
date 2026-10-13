"""
Unit tests for the live_underlying-style options candle poller
(service/ingestion/live_options.py). Focused on the pure, testable pieces
(the Eastern-today boundary and the no-Greeks chain-fallback filter); the
rest of the poller is a thin reuse of BackfillJob's already-tested write
path and mirrors live_underlying's loop, which has no unit tests of its
own for the same reason (DB + live candle feed are the test harness).
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pytest

from service.config.settings import TickerConfig
from service.ingestion.live_options import eastern_today, filter_chain_for_today
from service.ingestion.contract_manager import OptionRight, SettlementType


@dataclass
class FakeOption:
    streamer_symbol: str
    expiration_date: date
    strike_price: float
    option_type: str
    settlement_type: str = "PM"
    days_to_expiration: int = 1


def _cfg(**overrides) -> TickerConfig:
    base = dict(
        ticker="NDX",
        call_delta_min=0.15,
        call_delta_max=0.85,
        put_delta_min=-0.85,
        put_delta_max=-0.15,
        exclude_am_settled=True,
        capture_underlying_bars=True,
        max_days_to_expiration=7,
    )
    base.update(overrides)
    return TickerConfig(**base)


def test_eastern_today_utc_date_unchanged_during_afternoon_utc():
    """15:00 UTC in winter is 10:00 ET the same day — same date."""
    now = datetime(2026, 1, 15, 15, 0, tzinfo=timezone.utc)
    assert eastern_today(now) == date(2026, 1, 15)


def test_eastern_today_wraps_midnight_et_during_dst():
    """02:00 UTC in July is 22:00 ET the PREVIOUS day (EDT = UTC-4)."""
    now = datetime(2026, 7, 1, 2, 0, tzinfo=timezone.utc)
    assert eastern_today(now) == date(2026, 6, 30)


def test_eastern_today_wraps_midnight_et_during_standard_time():
    """05:00 UTC in January is 00:00 ET the SAME day (EST = UTC-5), but
    04:00 UTC is 23:00 ET the PREVIOUS day."""
    assert eastern_today(datetime(2026, 1, 15, 5, 0, tzinfo=timezone.utc)) == date(2026, 1, 15)
    assert eastern_today(datetime(2026, 1, 15, 4, 0, tzinfo=timezone.utc)) == date(2026, 1, 14)


def test_filter_chain_for_today_only_keeps_todays_expiration():
    day = date(2026, 10, 13)
    chain = {
        day: [
            FakeOption("NDX..C20261013T16800", day, 3000.0, "call"),
            FakeOption("NDX..P20261013T16800", day, 3000.0, "put"),
        ],
        date(2026, 10, 14): [FakeOption("NDX..C20261014T16800", date(2026, 10, 14), 3010.0, "call")],
    }
    got = filter_chain_for_today(chain, _cfg(), day, spot=3000.0, strike_window_pct=5.0, max_days_to_expiration=7)
    assert sorted(rc.contract_id for rc in got) == [
        "NDX..C20261013T16800",
        "NDX..P20261013T16800",
    ]
    assert got[0].right == OptionRight.CALL
    assert got[1].right == OptionRight.PUT
    assert got[0].settlement_type == SettlementType.PM
    assert got[0].expiration_date == day


def test_filter_chain_for_today_excludes_am_settled_when_configured():
    day = date(2026, 10, 13)
    chain = {
        day: [
            FakeOption("AM1", day, 3000.0, "call", settlement_type="AM"),
            FakeOption("PM1", day, 3000.0, "call", settlement_type="PM"),
        ]
    }
    got = filter_chain_for_today(chain, _cfg(), day, spot=None, strike_window_pct=5.0, max_days_to_expiration=7)
    assert [rc.contract_id for rc in got] == ["PM1"]

    got = filter_chain_for_today(
        chain, _cfg(exclude_am_settled=False), day, spot=None, strike_window_pct=5.0, max_days_to_expiration=7
    )
    assert sorted(rc.contract_id for rc in got) == ["AM1", "PM1"]


def test_filter_chain_for_today_excludes_strikes_outside_window():
    day = date(2026, 10, 13)
    spot = 5000.0
    chain = {
        day: [
            FakeOption("Near", day, 4800.0, "call"),   # inside 5%
            FakeOption("Edge", day, 5250.0, "call"),   # inside 5%
            FakeOption("Far", day, 5400.0, "call"),    # outside 5% (250 / 5000)
        ]
    }
    got = filter_chain_for_today(chain, _cfg(), day, spot=spot, strike_window_pct=5.0, max_days_to_expiration=7)
    assert sorted(rc.contract_id for rc in got) == ["Edge", "Near"]


def test_filter_chain_for_today_no_strike_filter_without_spot():
    """Before any underlying bar exists (fresh day, pre-open) the window
    can't be centered — spot=None must mean "accept the chain", not
    "accept nothing"."""
    day = date(2026, 10, 13)
    chain = {day: [FakeOption("Far", day, 999999.0, "call")]}
    got = filter_chain_for_today(chain, _cfg(), day, spot=None, strike_window_pct=5.0, max_days_to_expiration=7)
    assert [rc.contract_id for rc in got] == ["Far"]


def test_filter_chain_for_today_zero_spot_means_no_window():
    """Regression: a 0.0 underlying close (placeholder rows the upstream
    poller writes before real NDX/VIX quotes land) must behave exactly like
    spot=None -- "accept the chain". A 0-centered window would accept
    nothing, silently disabling the fallback path for a whole ticker."""
    day = date(2026, 10, 13)
    chain = {day: [FakeOption("Far", day, 999999.0, "call")]}
    got = filter_chain_for_today(chain, _cfg(), day, spot=0.0, strike_window_pct=5.0, max_days_to_expiration=7)
    assert [rc.contract_id for rc in got] == ["Far"]


def test_filter_chain_for_today_zero_spot_center_falls_back_to_chain():
    day = date(2026, 10, 13)
    chain = {day: [FakeOption("A", day, 4000.0, "call"), FakeOption("B", day, 6000.0, "call")]}
    got = filter_chain_for_today(chain, _cfg(), day, spot=0.0, strike_window_pct=5.0, max_days_to_expiration=7)
    assert sorted(rc.contract_id for rc in got) == ["A", "B"]


def test_filter_chain_for_today_respects_max_dte():
    day = date(2026, 10, 13)
    chain = {day: [FakeOption("DTE0", day, 5000.0, "call", days_to_expiration=0)]}
    got = filter_chain_for_today(chain, _cfg(), day, spot=None, strike_window_pct=5.0, max_days_to_expiration=7)
    assert [rc.contract_id for rc in got] == ["DTE0"]