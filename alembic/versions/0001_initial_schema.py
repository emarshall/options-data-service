"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-07-17

Creates the three core tables (contracts, option_bars_1m, underlying_bars_1m)
and converts the two bar tables into TimescaleDB hypertables partitioned on
`time`. See service/db/models.py for the full rationale on each column.

Continuous aggregates for 5m/15m/30m/1h/1d/1w (derived from option_bars_1m)
are NOT created here — that's Task 7, once there's real data flowing to
validate the aggregation logic against.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import ENUM as PGEnum

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # TimescaleDB extension must exist before we can call create_hypertable().
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    # Create the enum types via raw, idempotent SQL rather than relying on
    # SQLAlchemy's automatic type-creation-on-table-create machinery.
    #
    # Background: generic sa.Enum(create_type=False) does NOT reliably
    # suppress auto-creation when the same enum is reused as the type for
    # columns across multiple op.create_table() calls — SQLAlchemy still
    # attempts to (re-)create it while emitting each table's DDL, which
    # collides with whichever attempt created it first ("type already
    # exists"). This is a known SQLAlchemy gotcha, not a config mistake.
    # The DO-block below is safe to run even if the type somehow already
    # exists (catches duplicate_object and moves on), so this migration
    # can't fail this way regardless of prior partial state.
    for type_name, values in [
        ("option_right", "'call', 'put'"),
        ("settlement_type", "'am', 'pm'"),
        ("greeks_source", "'live', 'computed'"),
    ]:
        op.execute(
            f"DO $$ BEGIN "
            f"CREATE TYPE {type_name} AS ENUM ({values}); "
            f"EXCEPTION WHEN duplicate_object THEN NULL; "
            f"END $$;"
        )

    # For the column definitions below, use the Postgres-specific ENUM
    # class (not generic sa.Enum) with create_type=False — this is the
    # combination SQLAlchemy actually respects for "this type already
    # exists, just reference it by name, don't try to create it."
    option_right = PGEnum("call", "put", name="option_right", create_type=False)
    settlement_type = PGEnum("am", "pm", name="settlement_type", create_type=False)
    greeks_source = PGEnum("live", "computed", name="greeks_source", create_type=False)

    # --- contracts ---
    op.create_table(
        "contracts",
        sa.Column("contract_id", sa.String(), primary_key=True),
        sa.Column("underlying_ticker", sa.String(), nullable=False),
        sa.Column("expiration_date", sa.Date(), nullable=False),
        sa.Column("strike", sa.Numeric(12, 4), nullable=False),
        sa.Column("right", option_right, nullable=False),
        sa.Column("settlement_type", settlement_type, nullable=True),
        sa.Column("first_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_contracts_underlying_ticker", "contracts", ["underlying_ticker"])
    op.create_index("ix_contracts_expiration_date", "contracts", ["expiration_date"])

    # --- option_bars_1m ---
    op.create_table(
        "option_bars_1m",
        sa.Column("time", sa.DateTime(timezone=True), primary_key=True),
        sa.Column(
            "contract_id",
            sa.String(),
            sa.ForeignKey("contracts.contract_id"),
            primary_key=True,
        ),
        sa.Column("underlying_ticker", sa.String(), nullable=False),
        sa.Column("expiration_date", sa.Date(), nullable=False),
        sa.Column("strike", sa.Numeric(12, 4), nullable=False),
        sa.Column("right", option_right, nullable=False),
        sa.Column("open", sa.Numeric(12, 4), nullable=True),
        sa.Column("high", sa.Numeric(12, 4), nullable=True),
        sa.Column("low", sa.Numeric(12, 4), nullable=True),
        sa.Column("close", sa.Numeric(12, 4), nullable=True),
        sa.Column("bid", sa.Numeric(12, 4), nullable=True),
        sa.Column("ask", sa.Numeric(12, 4), nullable=True),
        sa.Column("delta", sa.Numeric(9, 6), nullable=True),
        sa.Column("gamma", sa.Numeric(11, 8), nullable=True),
        sa.Column("theta", sa.Numeric(11, 6), nullable=True),
        sa.Column("vega", sa.Numeric(11, 6), nullable=True),
        sa.Column("rho", sa.Numeric(11, 8), nullable=True),
        sa.Column("iv", sa.Numeric(9, 6), nullable=True),
        sa.Column("greeks_source", greeks_source, nullable=True),
        sa.Column("volume", sa.BigInteger(), nullable=True),
        sa.Column("open_interest", sa.BigInteger(), nullable=True),
        sa.Column("vwap", sa.Numeric(12, 6), nullable=True),
        sa.Column("bid_volume", sa.BigInteger(), nullable=True),
        sa.Column("ask_volume", sa.BigInteger(), nullable=True),
    )
    op.create_index("ix_option_bars_1m_underlying_ticker", "option_bars_1m", ["underlying_ticker"])
    op.create_index("ix_option_bars_1m_expiration_date", "option_bars_1m", ["expiration_date"])
    # Composite index supporting the primary Task 8 query pattern: ticker +
    # time range (+ optionally right/expiration), without needing the PK's
    # contract_id first.
    op.create_index(
        "ix_option_bars_1m_ticker_time",
        "option_bars_1m",
        ["underlying_ticker", "time"],
    )

    # --- underlying_bars_1m ---
    op.create_table(
        "underlying_bars_1m",
        sa.Column("time", sa.DateTime(timezone=True), primary_key=True),
        sa.Column("ticker", sa.String(), primary_key=True),
        sa.Column("open", sa.Numeric(12, 4), nullable=True),
        sa.Column("high", sa.Numeric(12, 4), nullable=True),
        sa.Column("low", sa.Numeric(12, 4), nullable=True),
        sa.Column("close", sa.Numeric(12, 4), nullable=True),
        sa.Column("volume", sa.BigInteger(), nullable=True),
    )
    op.create_index("ix_underlying_bars_1m_ticker", "underlying_bars_1m", ["ticker"])

    # --- Convert to hypertables, partitioned on `time` ---
    # migrate_data => true is harmless here (tables are empty on first
    # migration) but keeps this migration safe to re-run against a table
    # that already has rows, if it's ever adapted for that.
    op.execute(
        "SELECT create_hypertable('option_bars_1m', by_range('time'), "
        "migrate_data => true, if_not_exists => true)"
    )
    op.execute(
        "SELECT create_hypertable('underlying_bars_1m', by_range('time'), "
        "migrate_data => true, if_not_exists => true)"
    )


def downgrade() -> None:
    op.drop_table("underlying_bars_1m")
    op.drop_table("option_bars_1m")
    op.drop_table("contracts")
    op.execute("DROP TYPE IF EXISTS greeks_source")
    op.execute("DROP TYPE IF EXISTS settlement_type")
    op.execute("DROP TYPE IF EXISTS option_right")
