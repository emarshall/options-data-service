"""
Config loading. Split deliberately into two sources:

- `config.yaml` — non-secret, human-edited settings: which tickers to
  track, delta ranges, AM-settlement handling. This is what you edit when
  you want to add/remove a ticker.
- Environment variables (via `.env`) — secrets and deployment-specific
  values: DB connection string, TastyTrade credentials, sandbox flag.

They're merged into one `AppConfig` object so the rest of the codebase
doesn't need to care which source a given value came from.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class TickerConfig(BaseModel):
    """Per-ticker settings: which contracts to actually subscribe to."""

    ticker: str

    call_delta_min: float = 0.15
    call_delta_max: float = 0.85
    put_delta_min: float = -0.85
    put_delta_max: float = -0.15

    # See PLAN.md — default is to exclude AM-settled products (e.g. some
    # index option variants) since they behave differently around
    # expiration than the PM-settled contracts this project is mainly
    # built around.
    exclude_am_settled: bool = True

    # Also capture the underlying's own 1m bars (see UnderlyingBar1m in
    # service/db/models.py) — needed for Black-Scholes backfill (Task 6).
    capture_underlying_bars: bool = True

    # Added during Task 3: bounds how far out in expiration to even
    # consider contracts. Without this, resolving contracts means fetching
    # live Greeks for every strike across every expiration in the full
    # chain (which can run out 1-2+ years for equities) just to filter by
    # delta — expensive and pointless for a project centered on 0DTE/
    # short-dated strategies. Defaults to 45 days, deliberately a bit past
    # Task 0's confirmed ~6-week (~43 day) candle-history retention window,
    # so nothing relevant to backfill falls outside this net.
    max_days_to_expiration: int = 45

    @field_validator("ticker")
    @classmethod
    def uppercase_ticker(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("call_delta_min", "call_delta_max")
    @classmethod
    def call_delta_range(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"call delta must be in [0, 1], got {v}")
        return v

    @field_validator("put_delta_min", "put_delta_max")
    @classmethod
    def put_delta_range(cls, v: float) -> float:
        if not (-1.0 <= v <= 0.0):
            raise ValueError(f"put delta must be in [-1, 0], got {v}")
        return v


class YamlConfig(BaseModel):
    """Shape of config.yaml."""

    tickers: list[TickerConfig] = Field(default_factory=list)


class AppConfig(BaseSettings):
    """The merged, final config object the rest of the app uses.

    Secrets/deployment values come from environment variables (via
    pydantic-settings' automatic env-var loading). `tickers` is populated
    separately from config.yaml by `load_config()` below, since
    pydantic-settings doesn't natively merge a YAML file in.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Database ---
    # Async driver (asyncpg) for runtime use. See alembic/env.py for how
    # this gets adapted to a sync driver for migrations.
    database_url: str = Field(
        default="postgresql+asyncpg://options_user:options_pass@localhost:5432/options_data",
        alias="DATABASE_URL",
    )

    # --- TastyTrade auth (OAuth2 — see PLAN.md, username/password auth was
    # discontinued Dec 1 2025) ---
    tastytrade_client_secret: str = Field(default="", alias="TASTYTRADE_CLIENT_SECRET")
    tastytrade_refresh_token: str = Field(default="", alias="TASTYTRADE_REFRESH_TOKEN")
    tastytrade_use_sandbox: bool = Field(default=False, alias="TASTYTRADE_USE_SANDBOX")

    # --- Black-Scholes calculator (Task 6) ---
    # Doesn't need to be hyper-precise for backtesting purposes (per
    # PLAN.md's own reasoning) — a reasonable current short-term rate.
    # Override via env if it drifts far enough to matter.
    risk_free_rate: float = Field(default=0.045, alias="RISK_FREE_RATE")

    # --- Query API (Task 8) ---
    # Optional. If unset (the default, None), the API requires no auth —
    # fine for a localhost/private-network deployment, per PLAN.md Task 8.
    # If set, every request needs a matching X-API-Key header. Deliberately
    # `None`, not `""`, as the "unset" sentinel — see service/api/auth.py,
    # which checks `is None` specifically.
    api_key: str | None = Field(default=None, alias="API_KEY")

    # --- Query API (Task 8) ---
    # Optional lightweight guard, per the plan's own framing: this is meant
    # for local/private-network use, not a public service, so auth is
    # opt-in rather than mandatory. If set, every request must send this
    # value in an `X-API-Key` header; if unset (the default), the API is
    # open — appropriate for the common case of a home-server deployment
    # only reachable on a private network.
    api_key: str | None = Field(default=None, alias="API_KEY")

    # --- Populated from config.yaml, not env vars ---
    tickers: list[TickerConfig] = Field(default_factory=list)


def load_config(config_path: str | Path = "config.yaml") -> AppConfig:
    """Load and merge config.yaml + environment variables into one AppConfig."""
    config_path = Path(config_path)
    yaml_data: dict = {}
    if config_path.exists():
        with open(config_path) as f:
            yaml_data = yaml.safe_load(f) or {}
    yaml_config = YamlConfig.model_validate(yaml_data)

    app_config = AppConfig(tickers=yaml_config.tickers)
    return app_config


@lru_cache
def get_settings() -> AppConfig:
    """Cached accessor — most of the app should call this rather than
    `load_config()` directly, so config is only read/parsed once per
    process. Respects the CONFIG_PATH env var if set (e.g. for tests
    pointing at a fixture file), defaulting to ./config.yaml."""
    import os

    return load_config(os.environ.get("CONFIG_PATH", "config.yaml"))
