"""
Integration-style tests for IngestionPipeline: fake source (no
credentials/network), a real in-memory SQLite DB (persistence logic
tested against a real engine, not mocked), and an injectable clock so
bucket-flush timing is deterministic instead of needing to sleep real
wall-clock seconds.
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from service.config.settings import AppConfig, TickerConfig
from service.db.models import Base, GreeksSource, OptionBar1m, UnderlyingBar1m
from service.ingestion.pipeline import IngestionPipeline


@dataclass
class FakeOption:
    streamer_symbol: str
    expiration_date: date
    strike_price: float
    option_type: str
    settlement_type: str = "PM"
    days_to_expiration: int = 10


@dataclass
class FakeGreeksSnapshot:
    event_symbol: str
    delta: float


class FakeEvent:
    def __init__(self, event_symbol, **kwargs):
        self.event_symbol = event_symbol
        for k, v in kwargs.items():
            setattr(self, k, v)


class FakeSource:
    """Implements just what IngestionPipeline (directly, and via
    ContractManager) actually calls."""

    def __init__(self):
        self.chains: dict[str, dict] = {}
        self.greeks_snapshot: dict[str, float] = {}
        self.quote_callback = None
        self.greeks_callback = None
        self.quote_symbols: set[str] = set()
        self.greeks_symbols: set[str] = set()
        self.unsubscribed: list[str] = []
        # Config ticker -> feed-native symbol, for exercising the case
        # where the two genuinely differ (e.g. an index). Defaults to
        # identity, which is what most instruments do.
        self.streamer_symbols: dict[str, str] = {}

    async def authenticate(self):
        pass

    async def close(self):
        pass

    async def get_option_chain(self, ticker):
        return self.chains.get(ticker, {})

    async def get_underlying_streamer_symbol(self, ticker):
        return self.streamer_symbols.get(ticker, ticker)

    async def snapshot_greeks(self, symbols, timeout_s):
        return {
            s: FakeGreeksSnapshot(event_symbol=s, delta=self.greeks_snapshot[s])
            for s in symbols
            if s in self.greeks_snapshot
        }

    async def subscribe_quotes(self, symbols, callback):
        self.quote_symbols |= set(symbols)
        self.quote_callback = callback

    async def subscribe_greeks(self, symbols, callback):
        self.greeks_symbols |= set(symbols)
        self.greeks_callback = callback

    async def unsubscribe(self, symbols):
        self.unsubscribed.extend(symbols)
        self.quote_symbols -= set(symbols)
        self.greeks_symbols -= set(symbols)

    async def request_candles(self, symbol, period, start_time):
        return []

    # --- test helpers, not part of MarketDataSource ---

    async def emit_quote(self, symbol, bid, ask):
        await self.quote_callback(FakeEvent(symbol, bid_price=bid, ask_price=ask))

    async def emit_greeks(self, symbol, delta, gamma=0.01, theta=-0.05, vega=0.1, rho=0.02, iv=0.3):
        await self.greeks_callback(
            FakeEvent(symbol, delta=delta, gamma=gamma, theta=theta, vega=vega, rho=rho, volatility=iv)
        )


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


def _settings(**ticker_overrides) -> AppConfig:
    defaults = dict(
        ticker="SPY", call_delta_min=0.15, call_delta_max=0.85,
        put_delta_min=-0.85, put_delta_max=-0.15, max_days_to_expiration=45,
        capture_underlying_bars=True,
    )
    defaults.update(ticker_overrides)
    return AppConfig(tickers=[TickerConfig(**defaults)])


class FakeClock:
    """Controllable clock — starts at a fixed time, advance() moves it
    forward without needing to sleep real seconds."""

    def __init__(self, start: datetime):
        self._now = start

    def __call__(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


@pytest.fixture
def clock():
    return FakeClock(datetime(2026, 7, 18, 14, 30, 0, tzinfo=timezone.utc))


async def _setup_pipeline(db_session_factory, clock, exp_days=10):
    source = FakeSource()
    exp = date.today() + timedelta(days=exp_days)
    source.chains["SPY"] = {
        exp: [FakeOption("SPY_C450", exp, 450.0, "C", days_to_expiration=exp_days)]
    }
    source.greeks_snapshot = {"SPY_C450": 0.5}

    pipeline = IngestionPipeline(source, db_session_factory, _settings(), clock=clock)
    await pipeline._refresh_and_subscribe()
    return source, pipeline


@pytest.mark.asyncio
async def test_refresh_and_subscribe_subscribes_option_and_underlying(db_session_factory, clock):
    source, pipeline = await _setup_pipeline(db_session_factory, clock)

    assert "SPY_C450" in source.quote_symbols
    assert "SPY" in source.quote_symbols  # underlying, via capture_underlying_bars
    assert source.greeks_symbols == {"SPY_C450"}


@pytest.mark.asyncio
async def test_option_quote_and_greeks_flush_to_option_bars_table(db_session_factory, clock):
    source, pipeline = await _setup_pipeline(db_session_factory, clock)

    await source.emit_quote("SPY_C450", bid=1.0, ask=1.2)
    await source.emit_greeks("SPY_C450", delta=0.5)

    clock.advance(70)  # past the minute + grace period
    await pipeline._flush_ready_bars()

    async with db_session_factory() as session:
        result = await session.execute(select(OptionBar1m))
        rows = result.scalars().all()
        assert len(rows) == 1
        row = rows[0]
        assert row.contract_id == "SPY_C450"
        assert row.underlying_ticker == "SPY"
        assert float(row.close) == 1.1
        assert float(row.delta) == 0.5
        assert row.greeks_source == GreeksSource.LIVE
        # Known limitation, deliberate — see pipeline.py module docstring.
        assert row.volume is None


@pytest.mark.asyncio
async def test_underlying_quote_flushes_to_underlying_bars_table(db_session_factory, clock):
    source, pipeline = await _setup_pipeline(db_session_factory, clock)

    await source.emit_quote("SPY", bid=440.0, ask=440.2)

    clock.advance(70)
    await pipeline._flush_ready_bars()

    async with db_session_factory() as session:
        result = await session.execute(select(UnderlyingBar1m))
        rows = result.scalars().all()
        assert len(rows) == 1
        assert rows[0].ticker == "SPY"
        assert float(rows[0].close) == 440.1


@pytest.mark.asyncio
async def test_quote_for_unknown_symbol_is_dropped_not_written(db_session_factory, clock):
    """A quote for a symbol that isn't tracked as an option or underlying
    (e.g. a stale event for something just unsubscribed) should be
    silently dropped at flush time, not crash or write garbage."""
    source, pipeline = await _setup_pipeline(db_session_factory, clock)

    # Bypass the normal callback registration to simulate an event for a
    # symbol the pipeline never subscribed to.
    await pipeline._on_quote(FakeEvent("SOME_OTHER_SYMBOL", bid_price=1.0, ask_price=1.0))

    clock.advance(70)
    await pipeline._flush_ready_bars()

    async with db_session_factory() as session:
        result = await session.execute(select(OptionBar1m))
        assert result.scalars().all() == []


@pytest.mark.asyncio
async def test_bars_not_flushed_before_minute_elapses(db_session_factory, clock):
    source, pipeline = await _setup_pipeline(db_session_factory, clock)
    await source.emit_quote("SPY_C450", bid=1.0, ask=1.2)

    clock.advance(10)  # nowhere near a minute yet
    await pipeline._flush_ready_bars()

    async with db_session_factory() as session:
        result = await session.execute(select(OptionBar1m))
        assert result.scalars().all() == []


@pytest.mark.asyncio
async def test_close_flushes_partial_minute_bars(db_session_factory, clock):
    """On shutdown, even a bucket whose minute hasn't technically elapsed
    yet should be flushed — better to persist partial data than lose it."""
    source, pipeline = await _setup_pipeline(db_session_factory, clock)
    await source.emit_quote("SPY_C450", bid=1.0, ask=1.2)

    clock.advance(5)  # nowhere near ready under normal flush rules
    await pipeline.close()

    async with db_session_factory() as session:
        result = await session.execute(select(OptionBar1m))
        rows = result.scalars().all()
        assert len(rows) == 1


@pytest.mark.asyncio
async def test_removed_contract_gets_unsubscribed(db_session_factory, clock):
    source, pipeline = await _setup_pipeline(db_session_factory, clock)
    assert "SPY_C450" in source.quote_symbols

    # Contract expires / drops out of the chain entirely.
    source.chains["SPY"] = {}
    await pipeline._refresh_and_subscribe()

    assert "SPY_C450" in source.unsubscribed


# --- Task 9: adaptive contract-refresh cadence (fast near market open) ---


class FakeUtcClock:
    """A clock fixed to a specific instant, given as naive
    America/New_York wall-clock time — for cadence tests, expressing the
    fixture in ET is much easier to reason about than UTC + DST offset."""

    def __init__(self, year, month, day, hour, minute):
        from datetime import datetime as _dt
        from zoneinfo import ZoneInfo

        self._now = _dt(year, month, day, hour, minute, tzinfo=ZoneInfo("America/New_York")).astimezone(
            timezone.utc
        )

    def __call__(self) -> datetime:
        return self._now


def _pipeline_for_cadence(clock, **settings_overrides):
    settings = _settings()
    for k, v in settings_overrides.items():
        setattr(settings, k, v)
    return IngestionPipeline(FakeSourceStub(), None, settings, clock=clock)


class FakeSourceStub:
    """IngestionPipeline's constructor only needs *something* to hand to
    ContractManager — cadence tests never actually call refresh/subscribe,
    so no methods need to be implemented here."""


@pytest.mark.parametrize(
    "when,expected",
    [
        # Wed 9:15 ET — before the fast window starts.
        ((2026, 7, 22, 9, 15), "normal"),
        # Wed 9:25 ET — right at the fast window's start (inclusive).
        ((2026, 7, 22, 9, 25), "fast"),
        # Wed 9:30 ET — market open, well inside the window.
        ((2026, 7, 22, 9, 30), "fast"),
        # Wed 10:00 ET — right at the fast window's end (inclusive).
        ((2026, 7, 22, 10, 0), "fast"),
        # Wed 10:01 ET — just past the window.
        ((2026, 7, 22, 10, 1), "normal"),
        # Wed 14:00 ET — mid-day, well outside the window.
        ((2026, 7, 22, 14, 0), "normal"),
        # Saturday 9:30 ET — inside the time-of-day window, but a weekend,
        # so nothing lists and it should never use the fast cadence.
        ((2026, 7, 25, 9, 30), "normal"),
        # Sunday 9:30 ET — same reasoning.
        ((2026, 7, 26, 9, 30), "normal"),
    ],
)
def test_refresh_cadence_switches_on_market_open_window(when, expected):
    clock = FakeUtcClock(*when)
    pipeline = _pipeline_for_cadence(clock)

    interval = pipeline._current_refresh_interval_s()

    if expected == "fast":
        assert interval == pipeline._contract_refresh_fast_interval_s
    else:
        assert interval == pipeline._contract_refresh_interval_s
    # Sanity: the two cadences are actually configured differently, or this
    # test would pass trivially regardless of which branch is taken.
    assert pipeline._contract_refresh_fast_interval_s != pipeline._contract_refresh_interval_s


def test_refresh_cadence_respects_custom_window_and_intervals():
    """Not just the defaults — a narrower, later window and different
    interval values should be honored too."""
    settings = _settings()
    settings.contract_refresh_interval_s = 120.0
    settings.contract_refresh_fast_interval_s = 15.0
    settings.contract_refresh_fast_window_start = "15:55"
    settings.contract_refresh_fast_window_end = "16:05"

    pipeline = IngestionPipeline(
        FakeSourceStub(), None, settings, clock=FakeUtcClock(2026, 7, 22, 16, 0)
    )
    assert pipeline._current_refresh_interval_s() == 15.0

    pipeline_outside = IngestionPipeline(
        FakeSourceStub(), None, settings, clock=FakeUtcClock(2026, 7, 22, 12, 0)
    )
    assert pipeline_outside._current_refresh_interval_s() == 120.0


def test_explicit_constructor_args_override_settings():
    """Tests (and any future caller) should still be able to force a
    specific cadence directly, bypassing settings entirely."""
    settings = _settings()
    pipeline = IngestionPipeline(
        FakeSourceStub(),
        None,
        settings,
        contract_refresh_interval_s=999.0,
        contract_refresh_fast_interval_s=1.0,
        clock=FakeUtcClock(2026, 7, 22, 14, 0),
    )
    assert pipeline._contract_refresh_interval_s == 999.0
    assert pipeline._contract_refresh_fast_interval_s == 1.0


# --- Underlying feed-symbol resolution ---
#
# The config ticker (e.g. "SPX") is a chain underlying code; the feed may
# stream the same instrument under a different symbol. Subscribing with the
# config ticker produced little or no live underlying data, so the real
# mapping is now resolved explicitly and used for subscribing, while rows
# are still keyed by the config ticker.


@pytest.mark.asyncio
async def test_underlying_subscribes_under_feed_symbol_not_config_ticker(db_session_factory, clock):
    source = FakeSource()
    source.streamer_symbols = {"SPX": "/SPX"}  # feed uses the index convention
    exp = date.today() + timedelta(days=3)
    source.chains["SPX"] = {exp: [FakeOption(".SPX_C500", exp, 500.0, "C")]}
    source.greeks_snapshot = {".SPX_C500": 0.5}
    settings = AppConfig(tickers=[TickerConfig(ticker="SPX", capture_underlying_bars=True)])

    pipeline = IngestionPipeline(source, db_session_factory, settings, clock=clock)
    await pipeline._refresh_and_subscribe()

    assert "/SPX" in source.quote_symbols, "should subscribe using the feed-native symbol"
    assert "SPX" not in source.quote_symbols, "config ticker is not the subscription symbol"
    assert ".SPX_C500" in source.quote_symbols, "option contracts still subscribe normally"


@pytest.mark.asyncio
async def test_underlying_quote_routed_from_feed_symbol_to_config_ticker(db_session_factory, clock):
    """The event comes back tagged with the *feed* symbol, but the row has
    to be keyed by the *config* ticker — that's what the API and backfill
    speak in."""
    source = FakeSource()
    source.streamer_symbols = {"SPX": "/SPX"}
    settings = AppConfig(tickers=[TickerConfig(ticker="SPX", capture_underlying_bars=True)])

    pipeline = IngestionPipeline(source, db_session_factory, settings, clock=clock)
    await pipeline._refresh_and_subscribe()
    await source.emit_quote("/SPX", bid=6500.0, ask=6500.4)

    clock.advance(70)
    await pipeline._flush_ready_bars()

    async with db_session_factory() as session:
        rows = (await session.execute(select(UnderlyingBar1m))).scalars().all()
        assert len(rows) == 1
        assert rows[0].ticker == "SPX"
        assert float(rows[0].close) == 6500.2


@pytest.mark.asyncio
async def test_underlying_symbol_lookup_failure_falls_back_to_config_ticker(db_session_factory, clock):
    """A failed lookup must never block startup or subscribe to something
    wrong — it degrades to exactly the previous behavior."""

    class BrokenSource(FakeSource):
        async def get_underlying_streamer_symbol(self, ticker):
            raise RuntimeError("instruments API down")

    source = BrokenSource()
    settings = AppConfig(tickers=[TickerConfig(ticker="SPX", capture_underlying_bars=True)])

    pipeline = IngestionPipeline(source, db_session_factory, settings, clock=clock)
    await pipeline._refresh_and_subscribe()

    assert "SPX" in source.quote_symbols


# --- Write-failure handling: bars must not be lost, and must be isolated ---


class _CommitProxy:
    """Delegates everything to a real session, but lets commit() be made
    to fail on demand. Deliberately wraps the *real* session rather than
    mocking the DB away, so everything up to and including the insert still
    behaves exactly as in production."""

    def __init__(self, session, owner):
        self._session = session
        self._owner = owner

    def __getattr__(self, name):
        return getattr(self._session, name)

    async def commit(self):
        self._owner.commit_attempts += 1
        if self._owner.fail_next_commit:
            self._owner.fail_next_commit = False
            raise RuntimeError("simulated commit failure")
        await self._session.commit()


class CommitControllableSessionFactory:
    def __init__(self, factory):
        self._factory = factory
        self.fail_next_commit = False
        self.commit_attempts = 0

    def __call__(self):
        return self._ctx()

    @asynccontextmanager
    async def _ctx(self):
        async with self._factory() as session:
            yield _CommitProxy(session, self)


@pytest.mark.asyncio
async def test_failed_option_flush_keeps_bars_for_retry(db_session_factory, clock):
    """Before the fix, pop_ready() removed the buckets before the write,
    so a failed commit discarded them permanently — the log even claimed
    the next interval would retry, which it never did."""
    source, pipeline = await _setup_pipeline(db_session_factory, clock)
    controllable = CommitControllableSessionFactory(db_session_factory)
    pipeline._session_factory = controllable

    await source.emit_quote("SPY_C450", bid=1.0, ask=1.2)
    clock.advance(70)
    controllable.fail_next_commit = True
    await pipeline._flush_ready_bars()

    # Nothing written, but the bar is back in the aggregator.
    async with db_session_factory() as session:
        assert (await session.execute(select(OptionBar1m))).scalars().all() == []
    assert pipeline._option_bars.pending_count() == 1

    # Next cycle writes it for real.
    await pipeline._flush_ready_bars()
    async with db_session_factory() as session:
        rows = (await session.execute(select(OptionBar1m))).scalars().all()
        assert len(rows) == 1
        assert float(rows[0].close) == 1.1
    assert pipeline._option_bars.pending_count() == 0


@pytest.mark.asyncio
async def test_failed_underlying_flush_keeps_bars_for_retry(db_session_factory, clock):
    source, pipeline = await _setup_pipeline(db_session_factory, clock)
    controllable = CommitControllableSessionFactory(db_session_factory)
    pipeline._session_factory = controllable

    await source.emit_quote("SPY", bid=440.0, ask=440.2)
    clock.advance(70)
    controllable.fail_next_commit = True
    await pipeline._flush_ready_bars()

    async with db_session_factory() as session:
        assert (await session.execute(select(UnderlyingBar1m))).scalars().all() == []
    assert pipeline._underlying_bars.pending_count() == 1

    await pipeline._flush_ready_bars()
    async with db_session_factory() as session:
        assert len((await session.execute(select(UnderlyingBar1m))).scalars().all()) == 1
    assert pipeline._underlying_bars.pending_count() == 0


@pytest.mark.asyncio
async def test_option_write_failure_does_not_drop_underlying_bars(db_session_factory, clock):
    """Options and underlyings used to share one transaction, so a failure
    in the high-volume option path rolled back the underlying bars in the
    same flush too. They now fail independently."""
    source, pipeline = await _setup_pipeline(db_session_factory, clock)
    controllable = CommitControllableSessionFactory(db_session_factory)
    pipeline._session_factory = controllable

    await source.emit_quote("SPY_C450", bid=1.0, ask=1.2)
    await source.emit_quote("SPY", bid=440.0, ask=440.2)
    clock.advance(70)
    controllable.fail_next_commit = True  # hits the option write only
    await pipeline._flush_ready_bars()

    async with db_session_factory() as session:
        assert (await session.execute(select(OptionBar1m))).scalars().all() == []
        underlying = (await session.execute(select(UnderlyingBar1m))).scalars().all()
        assert len(underlying) == 1, "underlying bar must survive the option failure"
        assert float(underlying[0].close) == 440.1
    # And the option bar is queued for retry, not lost.
    assert pipeline._option_bars.pending_count() == 1


@pytest.mark.asyncio
async def test_underlying_flush_skips_row_that_already_exists(db_session_factory, clock):
    """Backfill and live ingestion write the same (time, ticker) rows. A
    plain INSERT would raise IntegrityError on overlap and take the whole
    flush down with it; the live path is conflict-tolerant instead."""
    source, pipeline = await _setup_pipeline(db_session_factory, clock)

    # Work out the bar's minute from the clock *before* advancing it —
    # the quote lands in the current minute, not the one we advance into.
    minute = int(clock().timestamp() // 60 * 60)
    bar_time = datetime.fromtimestamp(minute, tz=timezone.utc)
    await source.emit_quote("SPY", bid=440.0, ask=440.2)
    clock.advance(70)

    # Simulate backfill having already written this exact minute.
    async with db_session_factory() as session:
        session.add(UnderlyingBar1m(time=bar_time, ticker="SPY", close=999.0, volume=1234))
        await session.commit()

    await pipeline._flush_ready_bars()  # must not raise

    async with db_session_factory() as session:
        rows = (await session.execute(select(UnderlyingBar1m))).scalars().all()
        assert len(rows) == 1
        assert float(rows[0].close) == 999.0, "existing row left untouched"
    assert pipeline._underlying_bars.pending_count() == 0


@pytest.mark.asyncio
async def test_quote_events_are_counted_by_class(db_session_factory, clock):
    """The counters that make a silently-dead underlying subscription
    visible: a symbol the feed doesn't recognize produces no events and no
    error, which is otherwise indistinguishable from a quiet market."""
    source, pipeline = await _setup_pipeline(db_session_factory, clock)

    await source.emit_quote("SPY", bid=440.0, ask=440.2)
    await source.emit_quote("SPY_C450", bid=1.0, ask=1.2)
    await source.emit_quote("SPY_C450", bid=1.1, ask=1.3)

    assert pipeline._quote_events == {"underlying": 1, "option": 2}
