"""
Unit tests for GreeksBackfillJob. Real in-memory SQLite with actual rows —
this is the persistence + orchestration layer around black_scholes.py
(already unit-tested on its own in tests/test_black_scholes.py), so these
tests focus on the row-selection, skip-reason, and update logic rather than
re-verifying the math.
"""

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from service.db.models import Base, GreeksSource, OptionBar1m, OptionRight, UnderlyingBar1m
from service.greeks.backfill_job import GreeksBackfillJob

_MARKET_TZ = ZoneInfo("America/New_York")
_RISK_FREE_RATE = 0.045


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


def _expiring_in(days: int) -> date:
    return (datetime.now(timezone.utc) + timedelta(days=days)).date()


def _bar_time_before_close(expiration: date, hours_before_close: float = 2.0) -> datetime:
    close = datetime.combine(expiration, time(16, 0), tzinfo=_MARKET_TZ)
    return (close - timedelta(hours=hours_before_close)).astimezone(timezone.utc)


async def _add_option_bar(session, bar_time=None, **overrides):
    defaults = dict(
        contract_id="SPY_C450", underlying_ticker="SPY",
        expiration_date=_expiring_in(10), strike=450.0, right=OptionRight.CALL,
        close=5.0, iv=0.30, greeks_source=None,
    )
    defaults.update(overrides)
    resolved_time = bar_time if bar_time is not None else _bar_time_before_close(defaults["expiration_date"])
    session.add(OptionBar1m(time=resolved_time, **defaults))
    await session.commit()
    return resolved_time


async def _add_underlying_bar(session, ticker, time_, close):
    session.add(UnderlyingBar1m(time=time_, ticker=ticker, close=close))
    await session.commit()


@pytest.mark.asyncio
async def test_computes_greeks_when_underlying_bar_and_iv_present(db_session_factory):
    async with db_session_factory() as session:
        bar_time = await _add_option_bar(session)
        await _add_underlying_bar(session, "SPY", bar_time, close=452.0)

    job = GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE)
    summary = await job.run()

    assert summary["computed"] == 1

    async with db_session_factory() as session:
        row = await session.get(OptionBar1m, {"time": bar_time, "contract_id": "SPY_C450"})
        assert row.greeks_source == GreeksSource.COMPUTED
        assert row.delta is not None
        assert 0.0 < float(row.delta) < 1.0  # a call delta should be in (0, 1)
        assert row.gamma is not None
        assert row.theta is not None
        assert row.vega is not None
        assert row.rho is not None
        # iv was already present — should be left as-is, not overwritten.
        assert float(row.iv) == 0.30


@pytest.mark.asyncio
async def test_back_solves_iv_from_price_when_iv_missing(db_session_factory):
    async with db_session_factory() as session:
        bar_time = await _add_option_bar(session, iv=None, close=6.5)
        await _add_underlying_bar(session, "SPY", bar_time, close=452.0)

    job = GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE)
    summary = await job.run()

    assert summary["computed"] == 1
    async with db_session_factory() as session:
        row = await session.get(OptionBar1m, {"time": bar_time, "contract_id": "SPY_C450"})
        assert row.iv is not None  # back-solved and persisted
        assert row.delta is not None


@pytest.mark.asyncio
async def test_skips_row_with_no_matching_underlying_bar(db_session_factory):
    async with db_session_factory() as session:
        await _add_option_bar(session)
        # deliberately no underlying bar added

    job = GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE)
    summary = await job.run()

    assert summary["computed"] == 0
    assert summary["skipped_no_underlying_bar"] == 1


@pytest.mark.asyncio
async def test_skips_already_expired_row(db_session_factory):
    async with db_session_factory() as session:
        expiration = _expiring_in(-2)  # expired 2 days ago
        # 5pm ET — after that day's 4pm market close, so T is genuinely
        # negative relative to the bar's own expiration (not just "in the
        # past relative to now" — the function doesn't know or care about
        # "now", only about the bar's own timestamp vs. its expiration's
        # close, since these are historical bars being computed as-of their
        # own time).
        bar_time = datetime.combine(expiration, time(17, 0), tzinfo=_MARKET_TZ).astimezone(timezone.utc)
        await _add_option_bar(session, bar_time=bar_time, expiration_date=expiration)
        await _add_underlying_bar(session, "SPY", bar_time, close=452.0)

    job = GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE)
    summary = await job.run()

    assert summary["computed"] == 0
    assert summary["skipped_expired"] == 1


@pytest.mark.asyncio
async def test_skips_row_with_neither_iv_nor_price(db_session_factory):
    async with db_session_factory() as session:
        bar_time = await _add_option_bar(session, iv=None, close=None)
        await _add_underlying_bar(session, "SPY", bar_time, close=452.0)

    job = GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE)
    summary = await job.run()

    assert summary["computed"] == 0
    assert summary["skipped_no_iv_or_price"] == 1


@pytest.mark.asyncio
async def test_rows_with_greeks_source_already_set_are_left_alone(db_session_factory):
    """A row already computed or live-observed shouldn't be touched or
    re-processed — greeks_source IS NOT NULL means it's out of scope."""
    async with db_session_factory() as session:
        bar_time = await _add_option_bar(
            session, greeks_source=GreeksSource.LIVE, delta=0.42, gamma=0.01,
            theta=-0.05, vega=0.1, rho=0.02,
        )
        await _add_underlying_bar(session, "SPY", bar_time, close=452.0)

    job = GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE)
    summary = await job.run()

    assert summary["rows_processed"] == 0
    async with db_session_factory() as session:
        row = await session.get(OptionBar1m, {"time": bar_time, "contract_id": "SPY_C450"})
        assert float(row.delta) == 0.42  # untouched


@pytest.mark.asyncio
async def test_put_option_gets_negative_delta(db_session_factory):
    async with db_session_factory() as session:
        bar_time = await _add_option_bar(session, contract_id="SPY_P450", right=OptionRight.PUT)
        await _add_underlying_bar(session, "SPY", bar_time, close=452.0)

    job = GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE)
    await job.run()

    async with db_session_factory() as session:
        row = await session.get(OptionBar1m, {"time": bar_time, "contract_id": "SPY_P450"})
        assert float(row.delta) < 0.0


@pytest.mark.asyncio
async def test_multiple_rows_all_get_processed(db_session_factory):
    async with db_session_factory() as session:
        bar_time = await _add_option_bar(session, contract_id="SPY_A", strike=440.0)
        await _add_option_bar(session, bar_time=bar_time, contract_id="SPY_B", strike=460.0)
        await _add_underlying_bar(session, "SPY", bar_time, close=452.0)

    job = GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE)
    summary = await job.run()

    assert summary["computed"] == 2


@pytest.mark.asyncio
async def test_max_rows_caps_work_done_in_one_run(db_session_factory):
    async with db_session_factory() as session:
        bar_time = await _add_option_bar(session, contract_id="SPY_A", strike=440.0)
        await _add_option_bar(session, bar_time=bar_time, contract_id="SPY_B", strike=460.0)
        await _add_option_bar(session, bar_time=bar_time, contract_id="SPY_C", strike=450.0)
        await _add_underlying_bar(session, "SPY", bar_time, close=452.0)

    job = GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE, batch_size=500)
    summary = await job.run(max_rows=2)

    assert summary["rows_processed"] == 2
    assert summary["computed"] == 2
    # The remaining row should still be unprocessed (greeks_source still
    # NULL) — a follow-up run with no cap should pick up exactly one more.
    remaining = await GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE).run()
    assert remaining["computed"] == 1


@pytest.mark.asyncio
async def test_batching_processes_more_rows_than_one_batch_size(db_session_factory):
    async with db_session_factory() as session:
        bar_time = await _add_option_bar(session, contract_id="SPY_0", strike=400.0)
        for i in range(1, 5):
            await _add_option_bar(session, bar_time=bar_time, contract_id=f"SPY_{i}", strike=400.0 + i)
        await _add_underlying_bar(session, "SPY", bar_time, close=452.0)

    job = GreeksBackfillJob(db_session_factory, _RISK_FREE_RATE, batch_size=2)
    summary = await job.run()

    assert summary["rows_processed"] == 5
    assert summary["computed"] == 5
