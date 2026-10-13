"""
Unit tests for the `underlying-live` poller, covering the trade-fed 1m
bar path added because the DXLink `Candle` feed went zeroed for index
underlyings (see service/ingestion/live_underlying.py's module docstring).

Uses a fake streamer (no credentials/network) plus a real in-memory SQLite
database for the persistence half — the skip-if-exists + refuse-invalid-close
writes are exactly the kind of thing that looks right and isn't.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from service.db.models import Base, UnderlyingBar1m
from service.ingestion.live_underlying import (
    _run_catch_ups,
    _TradeBarAggregator,
    catch_up_ticker,
    poll_ticker_trades,
)
from service.sources._async_utils import close_ok


@dataclass
class FakeTrade:
    event_symbol: str
    price: float
    time: int  # epoch ms
    size: int | None = None


@dataclass
class FakeStreamer:
    events: list = field(default_factory=list)
    subscribed: list = field(default_factory=list)
    unsubscribed: list = field(default_factory=list)

    async def subscribe(self, event_type, symbols):
        self.subscribed.append((event_type, list(symbols)))

    async def unsubscribe(self, event_type, symbols):
        self.unsubscribed.append((event_type, list(symbols)))

    async def listen(self, event_type):
        for ev in self.events:
            yield ev


class FakeSource:
    def __init__(self, streamer: FakeStreamer):
        self._streamer = streamer

    async def _ensure_streamer(self):
        return self._streamer


@pytest.fixture
async def db_session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


def _ms(y: int, mo: int, d: int, h: int, mi: int, s: int = 0) -> int:
    """Epoch milliseconds for an aligned UTC minute."""
    return int(datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc).timestamp() * 1000)


def _for_minute(t_ms: int) -> datetime:
    return datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc)


def _utc(dt: datetime) -> datetime:
    """Normalizes whatever tz shape aiosqlite returns back to UTC-aware."""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# close_ok guard
# ---------------------------------------------------------------------------


def test_close_ok_accepts_only_real_prices():
    assert close_ok(0.0) is False
    assert close_ok(0) is False
    assert close_ok(None) is False
    assert close_ok(float("nan")) is False
    assert close_ok(float("inf")) is True  # unusual, but not "no data"
    assert close_ok(-1.0) is False
    assert close_ok(30858.26) is True
    assert close_ok("0") is False
    assert close_ok("15.42") is True


# ---------------------------------------------------------------------------
# trade aggregator
# ---------------------------------------------------------------------------


def test_aggregator_builds_aligned_ohlc_from_ticks():
    agg = _TradeBarAggregator()
    minute = _ms(2026, 10, 13, 13, 30)
    agg.feed("VIX", FakeTrade("VIX", 15.40, minute + 1000, size=1))
    agg.feed("VIX", FakeTrade("VIX", 15.42, minute + 2000, size=2))
    agg.feed("VIX", FakeTrade("VIX", 15.30, minute + 3000, size=5))
    agg.feed("VIX", FakeTrade("VIX", 15.31, minute + 4000, size=3))

    bars = agg.ready_bars(minute + 60_000)
    assert len(bars) == 1
    ticker, bar_time, b = bars[0]
    assert ticker == "VIX"
    assert bar_time == datetime(2026, 10, 13, 13, 30, tzinfo=timezone.utc)
    assert b == {"open": 15.40, "high": 15.42, "low": 15.30, "close": 15.31, "volume": 11}


def test_aggregator_does_not_ready_in_progress_minute():
    agg = _TradeBarAggregator()
    minute = _ms(2026, 10, 13, 13, 30)
    agg.feed("NDX", FakeTrade("NDX", 30500.0, minute + 1000))
    # Half a minute into the bar: must not be ready.
    assert agg.ready_bars(minute + 30_000) == []
    # Exactly at the close boundary it becomes ready.
    assert len(agg.ready_bars(minute + 60_000)) == 1


def test_aggregator_ignores_junk_ticks():
    agg = _TradeBarAggregator()
    minute = _ms(2026, 10, 13, 13, 30)
    agg.feed("VIX", FakeTrade("VIX", 0.0, minute + 1000))       # zero price
    agg.feed("VIX", FakeTrade("VIX", -3.0, minute + 2000))      # negative
    agg.feed("VIX", FakeTrade("VIX", float("nan"), minute + 3000))
    agg.feed("VIX", FakeTrade("VIX", None, minute + 4000))      # no price attr
    agg.feed("VIX", FakeTrade("VIX", 15.42, minute + 5000))
    bars = agg.ready_bars(minute + 60_000)
    assert len(bars) == 1
    assert bars[0][2]["open"] == 15.42
    assert bars[0][2]["close"] == 15.42


def test_aggregator_keeps_tickers_isolated():
    agg = _TradeBarAggregator()
    minute = _ms(2026, 10, 13, 13, 30)
    agg.feed("VIX", FakeTrade("VIX", 15.0, minute + 1000))
    agg.feed("NDX", FakeTrade("NDX", 30000.0, minute + 1000))
    bars = agg.ready_bars(minute + 60_000)
    assert {(t, b["close"]) for t, _, b in bars} == {("VIX", 15.0), ("NDX", 30000.0)}


def test_drop_ready_consumes_flushed_minutes_only():
    agg = _TradeBarAggregator()
    minute = _ms(2026, 10, 13, 13, 30)
    agg.feed("VIX", FakeTrade("VIX", 15.0, minute + 1000))
    agg.feed("VIX", FakeTrade("VIX", 15.1, minute + 60_000 + 1000))  # next minute
    flushed = agg.drop_ready(minute + 60_000)
    assert [b["close"] for _, _, b in flushed] == [15.0]
    # Next-minute bucket survives (still in progress at that flush horizon).
    remaining = agg.ready_bars(minute + 120_000)
    assert [b["close"] for _, _, b in remaining] == [15.1]


# ---------------------------------------------------------------------------
# poll_ticker_trades end-to-end (fake streamer + real sqlite)
# ---------------------------------------------------------------------------


async def test_poll_ticker_trades_writes_real_bars_and_skips_existing(db_session_factory):
    minute = _ms(2026, 10, 13, 13, 30)
    now = minute + 60_000
    preexisting_bar_time = _for_minute(minute + 60_000)  # already-present minute
    async with db_session_factory() as session:
        session.add(
            UnderlyingBar1m(
                time=preexisting_bar_time, ticker="NDX",
                open=1, high=1, low=1, close=1,
            )
        )
        await session.commit()

    events = [
        FakeTrade("NDX", 30500.0, minute + 1000),
        FakeTrade("NDX", 30510.0, minute + 2000),
        FakeTrade("VIX", 15.0, minute + 1000),
        FakeTrade("VIX", 15.1, minute + 2000),
        # A completed minute that already exists in the DB (must be skipped):
        FakeTrade("NDX", 30600.0, minute + 60_000 + 1000),
    ]
    streamer = FakeStreamer(events)
    source = FakeSource(streamer)
    agg = _TradeBarAggregator()

    await poll_ticker_trades(source, db_session_factory, agg, ["VIX", "NDX"], listen_s=1.0)

    async with db_session_factory() as session:
        rows = (await session.execute(
            select(UnderlyingBar1m).order_by(UnderlyingBar1m.ticker, UnderlyingBar1m.time)
        )).scalars().all()
    by_key = {(r.ticker, _utc(r.time)): r for r in rows}
    assert ("NDX", _for_minute(minute)) in by_key
    assert float(by_key[("NDX", _for_minute(minute))].close) == 30510.0
    assert float(by_key[("NDX", _for_minute(minute))].open) == 30500.0
    assert ("VIX", _for_minute(minute)) in by_key
    assert float(by_key[("VIX", _for_minute(minute))].close) == 15.1
    assert len(by_key) == 3  # 2 written + 1 pre-existing

    # Unsubscribed cleanly, and only the expected symbol set was subscribed.
    assert streamer.subscribed == [("Trade", ["VIX", "NDX"])] or streamer.subscribed[0][1] == ["VIX", "NDX"]
    assert len(streamer.unsubscribed) == 1


async def test_poll_ticker_trades_wont_write_invalid_close(db_session_factory):
    minute = _ms(2026, 10, 13, 14, 0)
    events = [FakeTrade("VIX", 0.0, minute + 1000)]  # zeroed feed-shaped tick
    source = FakeSource(FakeStreamer(events))
    agg = _TradeBarAggregator()
    await poll_ticker_trades(source, db_session_factory, agg, ["VIX"], listen_s=1.0)
    async with db_session_factory() as session:
        n = len((await session.execute(
            select(UnderlyingBar1m)
        )).scalars().all())
    assert n == 0


# ---------------------------------------------------------------------------
# startup candle catch-up (catch_up_ticker)
# ---------------------------------------------------------------------------


@dataclass
class FakeCandle:
    time: int  # epoch ms
    open: float
    high: float
    low: float
    close: float
    volume: int = 0


class FakeCandleSource:
    """Minimal candle-capable source (the catch-up uses request_candles)."""

    def __init__(self, candles_by_ticker: dict):
        self._candles = candles_by_ticker
        self.requests: list = []

    async def request_candles(
        self, symbol, period, start_time,
        timeout_s=240.0, idle_timeout_s=3.0, diagnostics=None,
    ):
        self.requests.append((symbol, period, start_time))
        return self._candles.get(symbol, [])


def _candle(y, mo, d, h, mi, close, volume=1) -> FakeCandle:
    return FakeCandle(time=_ms(y, mo, d, h, mi), open=close, high=close, low=close, close=close, volume=volume)


async def test_catch_up_ticker_fills_gap_and_skips_existing(db_session_factory):
    async with db_session_factory() as session:
        # A pre-existing bar at 13:30 that must NOT be overwritten.
        session.add(UnderlyingBar1m(
            time=_for_minute(_ms(2026, 10, 13, 13, 30)), ticker="NDX",
            open=1, high=1, low=1, close=1,
        ))
        await session.commit()

    src = FakeCandleSource({"NDX": [
        _candle(2026, 10, 13, 13, 30, 30500.0),  # dup — skipped
        _candle(2026, 10, 13, 13, 31, 30510.0),
        _candle(2026, 10, 13, 13, 32, 30520.0),
    ]})
    now = _for_minute(_ms(2026, 10, 13, 13, 33))

    written = await catch_up_ticker(src, db_session_factory, "NDX", now, max_lookback_hours=8)

    assert written == 2  # 3 candles - 1 already stored
    async with db_session_factory() as session:
        rows = (await session.execute(
            select(UnderlyingBar1m).order_by(UnderlyingBar1m.time)
        )).scalars().all()
    assert [float(r.close) for r in rows] == [1.0, 30510.0, 30520.0]
    # Requested from the newest stored bar (13:30), not the lookback floor.
    assert len(src.requests) == 1
    assert src.requests[0][2] == _for_minute(_ms(2026, 10, 13, 13, 30))


async def test_catch_up_ticker_refuses_zeroed_candles(db_session_factory):
    src = FakeCandleSource({"VIX": [
        _candle(2026, 10, 13, 14, 0, 0.0),   # zeroed feed-shaped → refused
        _candle(2026, 10, 13, 14, 1, 15.42),
    ]})
    now = _for_minute(_ms(2026, 10, 13, 14, 2))

    written = await catch_up_ticker(src, db_session_factory, "VIX", now, max_lookback_hours=8)

    assert written == 1
    async with db_session_factory() as session:
        rows = (await session.execute(select(UnderlyingBar1m))).scalars().all()
    assert len(rows) == 1
    assert float(rows[0].close) == 15.42


async def test_catch_up_ticker_is_noop_when_already_current(db_session_factory):
    minute = _ms(2026, 10, 13, 15, 0)
    async with db_session_factory() as session:
        session.add(UnderlyingBar1m(
            time=_for_minute(minute), ticker="NDX", open=1, high=1, low=1, close=1,
        ))
        await session.commit()

    src = FakeCandleSource({"NDX": [_candle(2026, 10, 13, 15, 1, 30000.0)]})
    now = _for_minute(minute + 30_000)  # 30s after the newest stored bar

    written = await catch_up_ticker(src, db_session_factory, "NDX", now, max_lookback_hours=8)

    assert written == 0
    assert src.requests == []  # current to within a minute → no request at all


async def test_catch_up_ticker_honors_write_lock(db_session_factory):
    src = FakeCandleSource({"NDX": [
        _candle(2026, 10, 13, 13, 30, 30500.0),
        _candle(2026, 10, 13, 13, 31, 30510.0),
    ]})
    now = _for_minute(_ms(2026, 10, 13, 13, 32))
    written = await catch_up_ticker(
        src, db_session_factory, "NDX", now, max_lookback_hours=8,
        write_lock=asyncio.Lock(),
    )
    assert written == 2


async def test_run_catch_ups_processes_every_ticker_and_swallows_errors(db_session_factory):
    class FlakySource:
        def __init__(self):
            self.seen = []

        async def request_candles(self, symbol, period, start_time, **kw):
            self.seen.append(symbol)
            if symbol == "VIX":
                raise RuntimeError("feed blew up")
            return [_candle(2026, 10, 13, 13, 30, 30500.0)]

    src = FlakySource()
    await _run_catch_ups(
        src, db_session_factory, ["VIX", "NDX"], max_lookback_hours=8,
        write_lock=asyncio.Lock(),
    )
    # VIX raised but didn't stop NDX from being caught up.
    assert src.seen == ["VIX", "NDX"]
    async with db_session_factory() as session:
        rows = (await session.execute(select(UnderlyingBar1m))).scalars().all()
    assert [r.ticker for r in rows] == ["NDX"]


async def test_catch_up_ticker_floors_start_for_long_dormant_ticker(db_session_factory):
    # Newest bar is far in the past (a long outage): start must be floored to
    # the bounded lookback rather than replaying from that old bar.
    async with db_session_factory() as session:
        session.add(UnderlyingBar1m(
            time=_for_minute(_ms(2026, 10, 1, 13, 30)), ticker="NDX",
            open=1, high=1, low=1, close=1,
        ))
        await session.commit()

    src = FakeCandleSource({"NDX": []})
    now = _for_minute(_ms(2026, 10, 13, 15, 0))

    await catch_up_ticker(src, db_session_factory, "NDX", now, max_lookback_hours=8)

    assert len(src.requests) == 1
    assert src.requests[0][2] == now - timedelta(hours=8)