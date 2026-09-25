"""
Validates the live ingestion *write path* against a REAL Postgres instance.

Same motivation as `test_enum_serialization_postgres.py`: this project's
test suite runs against SQLite, and SQLite structurally cannot exercise
either of the two things this file checks.

1. **Enum serialization through a Core insert.** The live pipeline now
   writes bars via `service/db/upsert.py`'s Core `insert()`, not through
   the ORM's `session.add()`. Both end up using the same column type (and
   therefore the same bind processor), but that's a reasoning step, not
   something a test proves. The original enum-serialization bug in this
   project was exactly this class of mismatch — the ORM sent `"CALL"`
   where the DB's native type wanted `"call"` — and it cost real time to
   find. This file re-checks it for the new code path rather than
   assuming.

2. **`ON CONFLICT DO NOTHING` against a composite primary key.** The
   conflict-tolerant insert is what stops an overlapping `backfill` run
   from turning a live flush into an `IntegrityError`. That behavior is
   dialect-specific, and SQLite's implementation of it is not evidence
   about Postgres's.

Skipped by default (no real Postgres in most environments this suite runs
in). To actually run it:
    TEST_DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/dbname pytest tests/test_upsert_postgres.py

Uses a small standalone table rather than the real `option_bars_1m`, but
mounts the *actual* column types taken from the real models, and the
enum types are created via raw SQL exactly as migration 0001 creates them.
That combination is the point: the enum values are independently defined
by the database, which is the only way the mismatch above can show up at
all.
"""

import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import Column, DateTime, MetaData, Numeric, String, Table, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from service.db.models import (
    Base,
    Contract,
    GreeksSource,
    OptionBar1m,
    OptionRight,
    UnderlyingBar1m,
)
from service.db.upsert import insert_ignore

_TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")

# Fixed instant so the pipeline's injected clock is deterministic here too.
_NOW = datetime(2026, 7, 20, 14, 30, 0, tzinfo=timezone.utc)

pytestmark = pytest.mark.skipif(
    not _TEST_DATABASE_URL,
    reason="Set TEST_DATABASE_URL to run this against a real Postgres instance.",
)

# Mounts the genuine column types from the real models onto a minimal
# table, so what's exercised is the real encoding logic rather than a
# lookalike.
_test_table = Table(
    "bar_probe",
    MetaData(),
    Column("time", DateTime(timezone=True), primary_key=True),
    Column("contract_id", String, primary_key=True),  # composite PK, as the real tables have
    Column("right", OptionBar1m.__table__.c.right.type, nullable=False),
    Column("greeks_source", OptionBar1m.__table__.c.greeks_source.type, nullable=True),
    Column("close", Numeric(12, 4), nullable=True),
)


@pytest.fixture
async def pg_session_factory():
    engine = create_async_engine(_TEST_DATABASE_URL)
    # CASCADE throughout: a test that fails mid-way can otherwise leave
    # dependent tables behind, which then blocks the `DROP TYPE` in the
    # *next* run's setup and cascades the failure across every test in the
    # file. Being blunt about cleanup is the right trade here.
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS bar_probe CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS option_bars_1m CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS underlying_bars_1m CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS contracts CASCADE"))
        await conn.execute(text("DROP TYPE IF EXISTS option_right CASCADE"))
        await conn.execute(text("DROP TYPE IF EXISTS greeks_source CASCADE"))
        await conn.execute(text("DROP TYPE IF EXISTS settlement_type CASCADE"))
        # Same raw SQL as alembic/versions/0001_initial_schema.py — the
        # lowercase values are the whole thing under test.
        await conn.execute(text("CREATE TYPE option_right AS ENUM ('call', 'put')"))
        await conn.execute(text("CREATE TYPE greeks_source AS ENUM ('live', 'computed')"))
        await conn.execute(
            text(
                """
                CREATE TABLE bar_probe (
                    time TIMESTAMPTZ NOT NULL,
                    contract_id VARCHAR NOT NULL,
                    "right" option_right NOT NULL,
                    greeks_source greeks_source,
                    close NUMERIC(12,4),
                    PRIMARY KEY (time, contract_id)
                )
                """
            )
        )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS bar_probe CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS option_bars_1m CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS underlying_bars_1m CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS contracts CASCADE"))
        await conn.execute(text("DROP TYPE IF EXISTS option_right CASCADE"))
        await conn.execute(text("DROP TYPE IF EXISTS greeks_source CASCADE"))
        await conn.execute(text("DROP TYPE IF EXISTS settlement_type CASCADE"))
    await engine.dispose()


def _row(minute: int, close: float = 101.0):
    from datetime import datetime, timezone

    return {
        "time": datetime(2026, 7, 20, 14, minute, tzinfo=timezone.utc),
        "contract_id": ".SPY260720C742",
        "right": OptionRight.CALL,
        "greeks_source": GreeksSource.LIVE,
        "close": close,
    }


@pytest.mark.asyncio
async def test_core_insert_serializes_enums_to_lowercase_values(pg_session_factory):
    """The failure mode this guards against is `invalid input value for enum
    option_right: "CALL"` — member name instead of value."""
    async with pg_session_factory() as session:
        await insert_ignore(session, _test_table, [_row(30)])
        await session.commit()

    async with pg_session_factory() as session:
        result = await session.execute(
            text('SELECT "right", greeks_source FROM bar_probe')
        )
        right, greeks_source = result.one()
        assert right == "call"
        assert greeks_source == "live"


@pytest.mark.asyncio
async def test_duplicate_row_is_skipped_not_an_error(pg_session_factory):
    """This is the whole point of the conflict-tolerant insert: backfill and
    live ingestion write the same (time, identifier) rows, and an overlap
    must not fail the live flush."""
    async with pg_session_factory() as session:
        await insert_ignore(session, _test_table, [_row(30, close=101.0)])
        await session.commit()

    # Second insert of the same key, with a different close.
    async with pg_session_factory() as session:
        await insert_ignore(session, _test_table, [_row(30, close=555.0)])
        await session.commit()

    async with pg_session_factory() as session:
        result = await session.execute(text("SELECT close FROM bar_probe"))
        rows = result.fetchall()
        assert len(rows) == 1, "duplicate must be skipped, not inserted twice"
        assert float(rows[0][0]) == 101.0, "the existing row must be left untouched"


@pytest.mark.asyncio
async def test_duplicate_within_a_single_batch_is_handled(pg_session_factory):
    """Retries can hand back two rows for the same key in one batch (e.g. a
    restored bucket colliding with one that re-formed in the meantime)."""
    async with pg_session_factory() as session:
        await insert_ignore(session, _test_table, [_row(30, close=1.0), _row(30, close=2.0)])
        await session.commit()

    async with pg_session_factory() as session:
        result = await session.execute(text("SELECT close FROM bar_probe"))
        assert len(result.fetchall()) == 1


@pytest.mark.asyncio
async def test_distinct_rows_in_one_batch_all_land(pg_session_factory):
    """The conflict tolerance must not cost us ordinary inserts."""
    rows = [_row(m, close=100.0 + m) for m in range(1, 6)]
    async with pg_session_factory() as session:
        await insert_ignore(session, _test_table, rows)
        await session.commit()

    async with pg_session_factory() as session:
        result = await session.execute(text("SELECT count(*) FROM bar_probe"))
        assert result.scalar() == 5


@pytest.mark.asyncio
async def test_real_underlying_bars_table_upsert_is_conflict_tolerant(pg_session_factory):
    """End-to-end against the real `underlying_bars_1m` table definition, to
    confirm the live path's actual target works the same way."""
    from datetime import datetime, timezone

    async with pg_session_factory() as session:
        await session.execute(
            text("""
            CREATE TABLE underlying_bars_1m (
                time TIMESTAMPTZ NOT NULL,
                ticker VARCHAR NOT NULL,
                open NUMERIC(12,4), high NUMERIC(12,4),
                low NUMERIC(12,4), close NUMERIC(12,4),
                volume BIGINT,
                PRIMARY KEY (time, ticker)
            )
            """)
        )
        await session.commit()  # DDL in a session transaction needs this
    try:
        t = datetime(2026, 7, 20, 14, 30, tzinfo=timezone.utc)
        async with pg_session_factory() as session:
            await insert_ignore(
                session, UnderlyingBar1m.__table__,
                [{"time": t, "ticker": "SPX", "close": 6500.0, "volume": 0}],
            )
            await session.commit()
        async with pg_session_factory() as session:
            await insert_ignore(
                session, UnderlyingBar1m.__table__,
                [{"time": t, "ticker": "SPX", "close": 9999.0, "volume": 5}],
            )
            await session.commit()
        async with pg_session_factory() as session:
            result = await session.execute(text("SELECT close, volume FROM underlying_bars_1m"))
            rows = result.fetchall()
            assert len(rows) == 1
            assert float(rows[0][0]) == 6500.0
    finally:
        async with pg_session_factory() as session:
            await session.execute(text("DROP TABLE IF EXISTS underlying_bars_1m"))
            await session.commit()


@pytest.mark.asyncio
async def test_full_pipeline_flush_against_real_postgres(pg_session_factory):
    """Runs the real `IngestionPipeline` flush against real Postgres.

    Deliberately narrow, but it covers something no other test in this suite
    can. The pipeline builds its insert rows as hand-written dicts rather
    than via the ORM, so a column-name typo or a missing required field
    would only surface at the database. SQLite tends to tolerate what
    Postgres rejects, and `option_bars_1m` in particular has enum-typed
    columns plus a real FOREIGN KEY to `contracts` that SQLite doesn't
    enforce by default.

    Reuses the fakes from `test_pipeline.py` rather than duplicating them —
    if the pipeline's constructor changes, only one place needs updating.
    """
    from datetime import timedelta
    from pathlib import Path
    import sys

    sys.path.insert(0, str(Path(__file__).parent))
    from test_pipeline import FakeClock, _settings  # noqa: E402

    from service.db.models import Base  # noqa: E402
    from service.ingestion.pipeline import IngestionPipeline  # noqa: E402
    from tests.test_pipeline import FakeSource  # noqa: E402

    engine = pg_session_factory.kw["bind"]
    async with engine.begin() as conn:
        for stmt in (
            "DROP TABLE IF EXISTS option_bars_1m CASCADE",
            "DROP TABLE IF EXISTS underlying_bars_1m CASCADE",
            "DROP TABLE IF EXISTS contracts CASCADE",
            "DROP TYPE IF EXISTS option_right CASCADE",
            "DROP TYPE IF EXISTS greeks_source CASCADE",
            "DROP TYPE IF EXISTS settlement_type CASCADE",
        ):
            await conn.execute(text(stmt))
        await conn.run_sync(
            lambda sync_conn: Base.metadata.create_all(
                sync_conn,
                tables=[
                    Contract.__table__,
                    UnderlyingBar1m.__table__,
                    OptionBar1m.__table__,
                ],
            )
        )

    # The pipeline resolves the contract through its own ContractManager,
    # so seed the chain and let it do the real work.
    clock = FakeClock(_NOW)
    source = FakeSource()
    exp = _NOW.date() + timedelta(days=10)
    from test_pipeline import FakeOption

    source.chains["SPY"] = {
        exp: [FakeOption("SPY_C450", exp, 450.0, "C", days_to_expiration=10)]
    }
    source.greeks_snapshot = {"SPY_C450": 0.5}

    pipeline = IngestionPipeline(source, pg_session_factory, _settings(), clock=clock)
    await pipeline._refresh_and_subscribe()

    await source.emit_quote("SPY_C450", bid=1.0, ask=1.2)
    await source.emit_greeks("SPY_C450", delta=0.5)
    await source.emit_quote("SPY", bid=440.0, ask=440.2)

    clock.advance(70)  # past the minute + grace period
    await pipeline._flush_ready_bars()

    async with pg_session_factory() as session:
        row = (
            await session.execute(
                text(
                    "SELECT \"right\", greeks_source, close FROM option_bars_1m"
                )
            )
        ).one()
        assert row[0] == "call", "enum must land as the lowercase value"
        assert row[1] == "live"
        assert float(row[2]) == pytest.approx(1.1, abs=0.01)

        urows = (
            await session.execute(
                text("SELECT ticker, close, volume FROM underlying_bars_1m")
            )
        ).fetchall()
        assert len(urows) == 1
        assert urows[0][0] == "SPY", "DB row must be keyed by the config ticker"
        assert urows[0][2] is None, "live bars carry no volume; that's expected"

    # Re-flushing the same minute is a no-op rather than an IntegrityError.
    await pipeline._flush_ready_bars()
    async with pg_session_factory() as session:
        result = await session.execute(text("SELECT count(*) FROM option_bars_1m"))
        assert result.scalar() == 1

    async with engine.begin() as conn:
        for stmt in (
            "DROP TABLE IF EXISTS option_bars_1m CASCADE",
            "DROP TABLE IF EXISTS underlying_bars_1m CASCADE",
            "DROP TABLE IF EXISTS contracts CASCADE",
            "DROP TYPE IF EXISTS option_right CASCADE",
            "DROP TYPE IF EXISTS greeks_source CASCADE",
            "DROP TYPE IF EXISTS settlement_type CASCADE",
        ):
            await conn.execute(text(stmt))
