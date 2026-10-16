"""
SQLite-runnable tests for `insert_ignore`'s bind-parameter batching.

`test_upsert_postgres.py` covers the conflict semantics against real
Postgres but is skipped unless TEST_DATABASE_URL is set, so the batching
logic added after the 2026-10-16 incident would otherwise be untested in
the default suite. That incident: a single pipeline flush bound ~80k
parameters in one INSERT (asyncpg caps at 32,767 per statement), the
`ingestion` service crash-looped on InterfaceError, and every live SPX
option bar for the session was lost.

The batching code is dialect-shared (both the postgres and sqlite branches
go through the same chunk loop), so SQLite exercises the actual logic —
chunk boundaries, batch sizing from column count, and conflict tolerance
holding across chunks.
"""

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from service.db.models import Base, UnderlyingBar1m
from service.db.upsert import insert_ignore


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


def _rows(n: int, ticker: str = "NDX") -> list[dict]:
    """n same-shape underlying-bar dicts, one per minute."""
    t0 = datetime(2026, 10, 16, 13, 30, tzinfo=timezone.utc)
    return [
        {
            "time": t0 + timedelta(minutes=i),
            "ticker": ticker,
            "open": 30500.0 + i, "high": 30510.0 + i,
            "low": 30490.0 + i, "close": 30505.0 + i,
            "volume": i,
        }
        for i in range(n)
    ]


async def _count(factory) -> int:
    async with factory() as session:
        return (await session.execute(select(func.count()).select_from(UnderlyingBar1m))).scalar_one()


async def test_batch_larger_than_one_chunk_all_rows_land(db_session_factory, monkeypatch):
    """More rows than fit in one batched statement must all land — the
    pre-fix code bound everything in one statement and died at the driver's
    parameter cap."""
    from service.db import upsert

    # 7 columns per row -> 18 // 7 = 2 rows per statement; 10 rows -> 5 statements.
    monkeypatch.setattr(upsert, "_MAX_BIND_PARAMS", 18)
    rows = _rows(10)
    assert len(rows[0]) == 7, "row shape drifted — update the cap math above"

    async with db_session_factory() as session:
        await insert_ignore(session, UnderlyingBar1m.__table__, rows)
        await session.commit()

    assert await _count(db_session_factory) == 10


async def test_conflict_tolerance_holds_across_chunk_boundaries(db_session_factory, monkeypatch):
    """Re-running a chunked batch must skip every already-present row, not
    just the ones in the first chunk — ON CONFLICT DO NOTHING semantics are
    per-row and must survive the split."""
    from service.db import upsert

    monkeypatch.setattr(upsert, "_MAX_BIND_PARAMS", 18)  # 7 cols -> 2 rows per statement
    rows = _rows(10)

    async with db_session_factory() as session:
        await insert_ignore(session, UnderlyingBar1m.__table__, rows)
        await session.commit()
    # Same keys, different closes — second run must be a pure no-op.
    rerun = [{**r, "close": 99999.0, "volume": 0} for r in rows]
    async with db_session_factory() as session:
        await insert_ignore(session, UnderlyingBar1m.__table__, rerun)
        await session.commit()

    assert await _count(db_session_factory) == 10
    async with db_session_factory() as session:
        first = (await session.execute(
            select(UnderlyingBar1m).order_by(UnderlyingBar1m.time)
        )).scalars().first()
    assert float(first.close) == 30505.0, "existing rows must be left untouched"


async def test_duplicates_within_a_chunk_still_collapse(db_session_factory, monkeypatch):
    """A duplicate pair that straddles or sits inside one chunk must still
    collapse to one row (retreat-retry can hand back two rows for a key)."""
    from service.db import upsert

    monkeypatch.setattr(upsert, "_MAX_BIND_PARAMS", 18)
    rows = _rows(4)
    rows[2] = {**rows[2], "close": 1.0}  # same (time, ticker) as rows[2] will collide
    dup = {**rows[2], "close": 2.0}

    async with db_session_factory() as session:
        await insert_ignore(session, UnderlyingBar1m.__table__, rows[:3] + [dup] + rows[3:])
        await session.commit()

    assert await _count(db_session_factory) == 4


async def test_empty_batch_is_a_noop(db_session_factory):
    async with db_session_factory() as session:
        await insert_ignore(session, UnderlyingBar1m.__table__, [])
        await session.commit()
    assert await _count(db_session_factory) == 0
