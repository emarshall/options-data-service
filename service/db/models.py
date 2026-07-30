"""
Database schema (SQLAlchemy models).

See PLAN.md Task 1 for the full rationale. Quick summary:

- `contracts` is metadata about each option contract we've ever tracked (one
  row per contract, not per bar).
- `option_bars_1m` is the base granularity we actually store from the live
  feed / backfill. Every coarser timeframe (5m/15m/30m/1h/1d/1w) is derived
  from this via TimescaleDB continuous aggregates (Task 7) — we do not store
  or compute those independently.
- `underlying_bars_1m` stores the underlying's own price alongside the
  options. This resolves the "should we also capture underlying bars" open
  question from PLAN.md as yes: it's needed for the Black-Scholes fallback
  (Task 6, which needs a spot price to compute Greeks) and gives the future
  backtest script spot price context for free.
- `greeks_source` on `option_bars_1m` distinguishes bars where delta/gamma/
  theta/vega/rho came from TastyTrade's live Greeks stream ('live') vs. our
  own Black-Scholes calculator ('computed', used for backfilled/gap rows —
  see Task 5/6). This matters for backtesting: you may want to know how
  trustworthy a given row's Greeks are.

Both bar tables use a composite primary key of (time, <identifier>) — this
is the TimescaleDB-recommended pattern for hypertables (the partitioning
column, `time`, must be part of the primary key).
"""

import enum

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Numeric,
    String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# SQLAlchemy's Enum(some_python_enum_class) defaults to persisting the
# enum's *member name* (e.g. "CALL"), not its *value* ("call") — a classic
# gotcha, confirmed the hard way: the Postgres enum types created in
# alembic/versions/0001_initial_schema.py use lowercase values ('call',
# 'put', etc., matching each enum's .value below), so without this,
# every insert fails with "invalid input value for enum option_right:
# CALL". Applied to every Enum(...) column below via values_callable.
def _enum_values(enum_cls):
    return [e.value for e in enum_cls]


class OptionRight(str, enum.Enum):
    CALL = "call"
    PUT = "put"


class SettlementType(str, enum.Enum):
    AM = "am"
    PM = "pm"


class GreeksSource(str, enum.Enum):
    LIVE = "live"  # observed directly from TastyTrade's live Greeks event stream
    COMPUTED = "computed"  # derived via our own Black-Scholes calculator (Task 6)


class Contract(Base):
    """One row per distinct option contract ever tracked (not per bar)."""

    __tablename__ = "contracts"

    # The DXLink/dxfeed streamer symbol (e.g. ".SPY260716C750") — stable,
    # unique per contract, and directly usable as a subscription symbol, so
    # it doubles as our primary key rather than inventing a surrogate one.
    contract_id: Mapped[str] = mapped_column(String, primary_key=True)

    underlying_ticker: Mapped[str] = mapped_column(String, index=True, nullable=False)
    expiration_date: Mapped[object] = mapped_column(Date, index=True, nullable=False)
    strike: Mapped[object] = mapped_column(Numeric(12, 4), nullable=False)
    right: Mapped[OptionRight] = mapped_column(
        Enum(OptionRight, name="option_right", values_callable=_enum_values), nullable=False
    )

    # Nullable because we may not always be able to determine this (e.g. for
    # equity options, which are always PM-settled and physically delivered —
    # settlement_type mainly matters for index products like SPX/XSP).
    settlement_type: Mapped[SettlementType | None] = mapped_column(
        Enum(SettlementType, name="settlement_type", values_callable=_enum_values), nullable=True
    )

    # First/last time we observed this contract in a chain lookup or feed
    # subscription — useful operational metadata (e.g. sanity-checking
    # Task 3's contract-roll logic), not used in backtest queries directly.
    first_seen: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)


class OptionBar1m(Base):
    """Base-granularity (1-minute) OHLC + Greeks bars for option contracts.

    This is the table everything else derives from. See module docstring.
    """

    __tablename__ = "option_bars_1m"

    time: Mapped[object] = mapped_column(DateTime(timezone=True), primary_key=True)
    contract_id: Mapped[str] = mapped_column(
        String, ForeignKey("contracts.contract_id"), primary_key=True
    )

    # Denormalized from `contracts`, deliberately. The query API (Task 8)
    # filters directly by ticker/right/expiration on this table; requiring
    # a join against `contracts` on every single query would be wasteful
    # for a hypertable this is meant to scale on. Small storage cost,
    # meaningful query-speed win — standard time-series tradeoff. Kept in
    # sync by whatever writes bars (Task 4/5), sourced from `contracts`.
    underlying_ticker: Mapped[str] = mapped_column(String, index=True, nullable=False)
    expiration_date: Mapped[object] = mapped_column(Date, index=True, nullable=False)
    strike: Mapped[object] = mapped_column(Numeric(12, 4), nullable=False)
    right: Mapped[OptionRight] = mapped_column(
        Enum(OptionRight, name="option_right", values_callable=_enum_values), nullable=False
    )

    # Price OHLC (of mark price — see PLAN.md Task 4 open question; may be
    # revisited to use last-trade price instead once real trading-frequency
    # data is observed for 0DTE contracts specifically).
    open: Mapped[object] = mapped_column(Numeric(12, 4), nullable=True)
    high: Mapped[object] = mapped_column(Numeric(12, 4), nullable=True)
    low: Mapped[object] = mapped_column(Numeric(12, 4), nullable=True)
    close: Mapped[object] = mapped_column(Numeric(12, 4), nullable=True)

    # Latest bid/ask observed within the bar (not OHLC — a single snapshot).
    bid: Mapped[object] = mapped_column(Numeric(12, 4), nullable=True)
    ask: Mapped[object] = mapped_column(Numeric(12, 4), nullable=True)

    # Greeks — either live-observed or Black-Scholes-computed, see
    # `greeks_source`.
    delta: Mapped[object] = mapped_column(Numeric(9, 6), nullable=True)
    gamma: Mapped[object] = mapped_column(Numeric(11, 8), nullable=True)
    theta: Mapped[object] = mapped_column(Numeric(11, 6), nullable=True)
    vega: Mapped[object] = mapped_column(Numeric(11, 6), nullable=True)
    rho: Mapped[object] = mapped_column(Numeric(11, 8), nullable=True)
    iv: Mapped[object] = mapped_column(Numeric(9, 6), nullable=True)
    greeks_source: Mapped[GreeksSource | None] = mapped_column(
        Enum(GreeksSource, name="greeks_source", values_callable=_enum_values), nullable=True
    )

    # Confirmed available on every Candle event during Task 0's spike, and
    # directly useful (open_interest especially, as a standard liquidity
    # filter) — capturing them since they're "free" alongside the bar.
    volume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    open_interest: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    vwap: Mapped[object] = mapped_column(Numeric(12, 6), nullable=True)
    bid_volume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    ask_volume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


class UnderlyingBar1m(Base):
    """Base-granularity (1-minute) OHLC bars for the underlying itself
    (e.g. SPY's own price, not an option contract on it).

    Needed by the Black-Scholes fallback (Task 6, which requires a spot
    price to compute Greeks) and useful spot-price context for the future
    backtest script.
    """

    __tablename__ = "underlying_bars_1m"

    time: Mapped[object] = mapped_column(DateTime(timezone=True), primary_key=True)
    ticker: Mapped[str] = mapped_column(String, primary_key=True)

    open: Mapped[object] = mapped_column(Numeric(12, 4), nullable=True)
    high: Mapped[object] = mapped_column(Numeric(12, 4), nullable=True)
    low: Mapped[object] = mapped_column(Numeric(12, 4), nullable=True)
    close: Mapped[object] = mapped_column(Numeric(12, 4), nullable=True)
    volume: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
