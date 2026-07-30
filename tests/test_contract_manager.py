"""
Unit tests for ContractManager.

Uses a fake MarketDataSource (no credentials/network needed) and a real
in-memory SQLite database (via aiosqlite) for the persistence half — the
DB layer is worth testing against a real engine, not a mock, since the
upsert logic (existing vs. new contract_id) is exactly the kind of thing
that looks right and isn't.
"""

from dataclasses import dataclass, field
from datetime import date, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from service.config.settings import TickerConfig
from service.db.models import Base, Contract, OptionRight, SettlementType
from service.ingestion.contract_manager import ContractManager


@dataclass
class FakeOption:
    streamer_symbol: str
    expiration_date: date
    strike_price: float
    option_type: str  # "C" / "P", matching the real tastytrade.instruments.OptionType values
    settlement_type: str = "PM"
    days_to_expiration: int = 10


@dataclass
class FakeGreeksEvent:
    event_symbol: str
    delta: float


class FakeSource:
    """Implements just the MarketDataSource methods ContractManager
    actually calls."""

    def __init__(self):
        self.chains: dict[str, dict[date, list[FakeOption]]] = {}
        self.greeks: dict[str, float] = {}  # symbol -> delta, for snapshot_greeks to return
        self.snapshot_calls: list[list[str]] = []

    async def get_option_chain(self, ticker):
        return self.chains.get(ticker, {})

    async def snapshot_greeks(self, symbols, timeout_s):
        self.snapshot_calls.append(list(symbols))
        return {
            s: FakeGreeksEvent(event_symbol=s, delta=self.greeks[s])
            for s in symbols
            if s in self.greeks
        }


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


def _cfg(**overrides) -> TickerConfig:
    defaults = dict(ticker="SPY", call_delta_min=0.15, call_delta_max=0.85,
                     put_delta_min=-0.85, put_delta_max=-0.15, max_days_to_expiration=45)
    defaults.update(overrides)
    return TickerConfig(**defaults)


@pytest.mark.asyncio
async def test_resolves_only_contracts_within_delta_range(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {
        exp: [
            FakeOption("SPY_CALL_INRANGE", exp, 450, "C", days_to_expiration=10),
            FakeOption("SPY_CALL_TOODEEP", exp, 300, "C", days_to_expiration=10),
            FakeOption("SPY_PUT_INRANGE", exp, 400, "P", days_to_expiration=10),
        ]
    }
    source.greeks = {
        "SPY_CALL_INRANGE": 0.40,
        "SPY_CALL_TOODEEP": 0.95,  # out of range (max 0.85)
        "SPY_PUT_INRANGE": -0.30,
    }

    mgr = ContractManager(source, db_session_factory, [_cfg()])
    diff = await mgr.refresh()

    assert set(diff.current.keys()) == {"SPY_CALL_INRANGE", "SPY_PUT_INRANGE"}
    assert diff.added.keys() == diff.current.keys()
    assert diff.removed == set()


@pytest.mark.asyncio
async def test_excludes_am_settled_when_configured(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=5)
    source.chains["SPY"] = {
        exp: [
            FakeOption("SPY_PM", exp, 450, "C", settlement_type="PM", days_to_expiration=5),
            FakeOption("SPY_AM", exp, 450, "C", settlement_type="AM", days_to_expiration=5),
        ]
    }
    source.greeks = {"SPY_PM": 0.5, "SPY_AM": 0.5}

    mgr = ContractManager(source, db_session_factory, [_cfg(exclude_am_settled=True)])
    diff = await mgr.refresh()

    assert "SPY_PM" in diff.current
    assert "SPY_AM" not in diff.current


@pytest.mark.asyncio
async def test_excludes_contracts_beyond_max_days_to_expiration(db_session_factory):
    source = FakeSource()
    near = date.today() + timedelta(days=10)
    far = date.today() + timedelta(days=200)
    source.chains["SPY"] = {
        near: [FakeOption("SPY_NEAR", near, 450, "C", days_to_expiration=10)],
        far: [FakeOption("SPY_FAR", far, 450, "C", days_to_expiration=200)],
    }
    source.greeks = {"SPY_NEAR": 0.5, "SPY_FAR": 0.5}

    mgr = ContractManager(source, db_session_factory, [_cfg(max_days_to_expiration=45)])
    diff = await mgr.refresh()

    assert "SPY_NEAR" in diff.current
    assert "SPY_FAR" not in diff.current


@pytest.mark.asyncio
async def test_already_tracked_contracts_are_not_re_snapshotted(db_session_factory):
    """Core design decision under test: once tracked, a contract's delta
    is never re-checked, and it must not appear in subsequent
    snapshot_greeks() calls (see contract_manager.py module docstring for
    why — re-snapshotting an already-subscribed symbol risks unsubscribing
    the persistent ingestion subscription for it)."""
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_A", exp, 450, "C", days_to_expiration=10)]}
    source.greeks = {"SPY_A": 0.5}

    mgr = ContractManager(source, db_session_factory, [_cfg()])
    await mgr.refresh()
    assert source.snapshot_calls == [["SPY_A"]]

    # Second refresh: same contract still in the chain, nothing new.
    diff2 = await mgr.refresh()
    assert diff2.added == {}
    assert diff2.removed == set()
    assert "SPY_A" in diff2.current
    # Crucially: no second snapshot call for a symbol we already track.
    assert source.snapshot_calls == [["SPY_A"]]


@pytest.mark.asyncio
async def test_removed_when_contract_disappears_from_chain(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_A", exp, 450, "C", days_to_expiration=10)]}
    source.greeks = {"SPY_A": 0.5}

    mgr = ContractManager(source, db_session_factory, [_cfg()])
    await mgr.refresh()

    # Contract expired / dropped from the chain entirely.
    source.chains["SPY"] = {}
    diff2 = await mgr.refresh()

    assert diff2.removed == {"SPY_A"}
    assert diff2.current == {}


@pytest.mark.asyncio
async def test_persists_contract_metadata_to_db(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_A", exp, 450.0, "C", settlement_type="PM", days_to_expiration=10)]}
    source.greeks = {"SPY_A": 0.5}

    mgr = ContractManager(source, db_session_factory, [_cfg()])
    await mgr.refresh()

    async with db_session_factory() as session:
        row = await session.get(Contract, "SPY_A")
        assert row is not None
        assert row.underlying_ticker == "SPY"
        assert row.expiration_date == exp
        assert float(row.strike) == 450.0
        assert row.right == OptionRight.CALL
        assert row.settlement_type == SettlementType.PM
        assert row.first_seen is not None
        assert row.last_seen is not None


@pytest.mark.asyncio
async def test_persist_upserts_rather_than_duplicating(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_A", exp, 450, "C", days_to_expiration=10)]}
    source.greeks = {"SPY_A": 0.5}

    mgr = ContractManager(source, db_session_factory, [_cfg()])
    await mgr.refresh()
    first_seen_after_first_refresh = mgr._tracked["SPY_A"]

    await mgr.refresh()  # second refresh, same contract still present

    async with db_session_factory() as session:
        result = await session.execute(select(Contract).where(Contract.contract_id == "SPY_A"))
        rows = result.scalars().all()
        assert len(rows) == 1, "should update the existing row, not insert a duplicate"


@pytest.mark.asyncio
async def test_multiple_tickers_resolved_independently(db_session_factory):
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {exp: [FakeOption("SPY_A", exp, 450, "C", days_to_expiration=10)]}
    source.chains["QQQ"] = {exp: [FakeOption("QQQ_A", exp, 380, "C", days_to_expiration=10)]}
    source.greeks = {"SPY_A": 0.5, "QQQ_A": 0.4}

    mgr = ContractManager(source, db_session_factory, [_cfg(ticker="SPY"), _cfg(ticker="QQQ")])
    diff = await mgr.refresh()

    assert set(diff.current.keys()) == {"SPY_A", "QQQ_A"}
    assert diff.current["SPY_A"].underlying_ticker == "SPY"
    assert diff.current["QQQ_A"].underlying_ticker == "QQQ"


@pytest.mark.asyncio
async def test_missing_greeks_snapshot_skips_contract_gracefully(db_session_factory):
    """If a candidate's Greeks snapshot never arrives (e.g. a timeout),
    it should just be skipped this cycle, not crash the whole refresh."""
    source = FakeSource()
    exp = date.today() + timedelta(days=10)
    source.chains["SPY"] = {
        exp: [
            FakeOption("SPY_HAS_GREEKS", exp, 450, "C", days_to_expiration=10),
            FakeOption("SPY_NO_GREEKS", exp, 460, "C", days_to_expiration=10),
        ]
    }
    source.greeks = {"SPY_HAS_GREEKS": 0.5}  # SPY_NO_GREEKS deliberately missing

    mgr = ContractManager(source, db_session_factory, [_cfg()])
    diff = await mgr.refresh()

    assert "SPY_HAS_GREEKS" in diff.current
    assert "SPY_NO_GREEKS" not in diff.current
