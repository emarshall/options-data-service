# Options Data Service

Self-hosted service that streams options market data (quotes, mark price,
Greeks) from TastyTrade via DXLink, stores it in TimescaleDB, and exposes a
query API for backtesting — see `PLAN.md` for the full project plan and
rationale. This is the **Task 1** deliverable: repo skeleton, Docker Compose
stack, and the initial DB schema/migration. Real streaming logic lands in
Tasks 2-4.

## What's here vs. not yet

Working right now:
- `docker compose up` brings up TimescaleDB, applies migrations, starts
  the real `ingestion` service (as of Task 4 — auth, contract resolution,
  live bar aggregation, DB writes), and the real `api` service (as of
  Task 8 — see below; `/health` was all it had before).
- Full DB schema (`contracts`, `option_bars_1m`, `underlying_bars_1m`) as
  TimescaleDB hypertables.
- Config loading (`config.yaml` for tickers/deltas, `.env` for secrets).
- `TastyTradeSource` (`service/sources/tastytrade.py`): OAuth2 auth, option
  chain lookup, live quote/Greeks subscriptions with automatic reconnect +
  resubscribe on disconnect, one-off historical candle requests, and a
  one-off Greeks snapshot method (added in Task 3, kept separate from the
  persistent Greeks subscription). Not yet wired into the actual ingestion
  pipeline — that's Task 4. Covered by real unit tests
  (`tests/test_tastytrade_source.py`, mocked) and a real-credentials smoke
  test (`scripts/task2_smoke_test.py`) — both passing, the latter verified
  against a real TastyTrade account.
- `ContractManager` (`service/ingestion/contract_manager.py`): resolves
  which option contracts to track per configured ticker (chain lookup →
  filter by days-to-expiration/AM-settlement → one-time Greeks snapshot to
  filter by delta), persists contract metadata, and returns what changed
  since the last refresh. Covered by 9 unit tests
  (`tests/test_contract_manager.py`, fake source + a real in-memory SQLite
  DB) and a real-credentials smoke test (`scripts/task3_smoke_test.py`).
- `IngestionPipeline` (`service/ingestion/pipeline.py`) +
  `BarAggregator` (`service/ingestion/bar_aggregator.py`): the real thing —
  wires `TastyTradeSource` and `ContractManager` together into live
  quote/Greeks streaming, 1-minute OHLC bar aggregation (mark = bid/ask
  midpoint), and actual writes to `option_bars_1m`/`underlying_bars_1m`.
  `service/ingestion/main.py` now runs this for real (replacing Task 1's
  placeholder), with graceful shutdown that flushes in-progress bars.
  Covered by 18 unit/integration tests (`tests/test_bar_aggregator.py`,
  `tests/test_pipeline.py` — fake source, real in-memory SQLite, an
  injectable clock for deterministic bucket-timing tests) and a
  real-credentials smoke test (`scripts/task4_smoke_test.py`).
  **Known limitation, deliberate:** live-aggregated bars don't have
  volume/open_interest/vwap/bid_volume/ask_volume populated — those only
  come from Candle events (Task 5's backfill).
- `BackfillJob` (`service/ingestion/backfill.py`): pulls ~6 weeks of
  historical 1-minute candles (Task 0's confirmed retention window) for
  every tracked option contract *and* each configured underlying (needed
  so Task 6's Black-Scholes calculator has a spot price to work with).
  Idempotent — safe to re-run, only fills in gaps, never overwrites a
  row that already exists (e.g. one written by live ingestion). Also
  recovers contracts that fully expired during a service outage — not
  just currently-live-resolvable ones — by pulling additional candidates
  directly from the `contracts` table, since `ContractManager` alone can't
  rediscover a contract with no live Greeks left. Backfilled rows get
  price/volume/open_interest/vwap/IV from the candle but leave Greeks
  NULL — that's Task 6's job. Runnable standalone
  (`python -m service.ingestion.backfill`) or as an on-demand docker-compose
  service (`docker compose run --rm backfill`, kept out of default
  `docker compose up` via a profile since it's not a continuously-running
  service). Covered by 10 unit tests (`tests/test_backfill.py`, fake source
  + real in-memory SQLite) and a real-credentials smoke test
  (`scripts/task5_smoke_test.py`).
- `black_scholes.py` (`service/greeks/black_scholes.py`) + `GreeksBackfillJob`
  (`service/greeks/backfill_job.py`): computes delta/gamma/theta/vega/rho
  for any `option_bars_1m` row with `greeks_source IS NULL` — backfilled
  rows (which never get Greeks from Task 5) and any Task 4 live-ingestion
  gaps. Uses the row's own IV directly when present (the common case,
  confirmed by Task 0 — no need to back-solve), falling back to solving IV
  from price otherwise. Requires a matching underlying bar at the same
  minute for a spot price; skips (doesn't error on) rows it can't compute,
  with a specific skip reason logged for each. Validated against known
  textbook Black-Scholes reference values (exact match to published
  call/put prices and delta for a standard example), not just internal
  consistency checks. Runnable standalone
  (`python -m service.greeks.backfill_job`) or via
  `docker compose run --rm greeks-backfill` (profile-gated, like `backfill`).
  Doesn't need TastyTrade credentials at all — pure DB reconciliation.
  Covered by 30 unit tests (`tests/test_black_scholes.py`,
  `tests/test_greeks_backfill_job.py`) and a smoke test
  (`scripts/task6_smoke_test.py`).
- Continuous aggregates (Task 7) now cover **both** `option_bars_1m` (migration 0002) and
  `underlying_bars_1m` (migration 0003) at 5m/15m/30m/1h/1d/1w — extended after the initial Task 7/8
  delivery specifically so the query API could serve non-1m underlying bars too.
- Query API (`service/api/routes.py`): `GET /options/bars` (filterable by
  ticker, time range, `agg` — 1m/5m/15m/30m/1h/1d/1w, right, delta range,
  expiration, exact contract), `GET /underlying/bars` (same full set of
  `agg` periods now, not just 1m), `GET /contracts` (filterable by ticker,
  expiration range, right), and `GET /metadata` (no filters — a quick
  summary of what data actually exists: which tickers have any
  `option_bars_1m` rows, each ticker's observed time range, and a total
  row count, useful for a client to sanity-check coverage before querying
  bars).
  Limit/offset pagination on the three bar/contract endpoints, capped at
  20,000 rows/request (`/metadata` isn't paginated — it's a fixed-shape
  summary, not a filtered list).
  Optional `X-API-Key` header auth (`API_KEY` env var — unset by default,
  fine for a private-network deployment). `service/db/views.py` defines
  lightweight (non-ORM) typed table references for the Task 7 continuous
  aggregate views (both option and underlying), reusing the exact same
  `Enum(..., values_callable=...)` column types as the real models where
  applicable — confirmed directly against real Postgres that this avoids a
  real class of "operator does not exist" error an untyped column would
  risk against a native enum type. Covered by 18 integration tests
  (`tests/test_api.py`, real HTTP requests via httpx against the real
  FastAPI app, backed by real in-memory SQLite — including tables shaped
  like the continuous aggregate views, so `agg=5m` routing is genuinely
  exercised, not just `agg=1m`) plus 17 unit tests for the table-selection
  logic itself (`tests/test_views.py`).

Not yet (see `PLAN.md` for when):
- Task 9 onward (scheduling, deployment polish, monitoring) — the core
  data pipeline (Tasks 1-8) is functionally complete

**Task 7 additions — worth reading before running them:** `alembic/versions/0002_continuous_aggregates.py`
adds continuous aggregates (5m/15m/30m/1h/1d/1w, derived from `option_bars_1m`) and compression policies
on both bar hypertables; `0003_underlying_continuous_aggregates.py` adds the same aggregates for
`underlying_bars_1m`. **Unlike every other migration in this repo, neither of these could be tested
against a real TimescaleDB instance** — the extension isn't installable in the environment this was
built in. Both are carefully reasoned (see each migration file's own docstring for the full rationale on
window sizing, compression timing, etc.) but higher-risk than the rest of the codebase. Run
`alembic upgrade head` (or `docker compose up`) and expect it may need a round of fixes, the same way
migration 0001 originally did.

## Setup

```bash
cp .env.example .env      # fill in POSTGRES_PASSWORD and TastyTrade OAuth creds
cp config.example.yaml config.yaml   # adjust tickers/deltas as desired
docker compose up --build
```

This will:
1. Start TimescaleDB and wait for it to be healthy.
2. Run `alembic upgrade head` once (the `migrate` service) to create the schema.
3. Start `ingestion` and `api` containers.

Check it worked:
```bash
curl http://localhost:8000/health
# {"status":"ok","database":"ok","tracked_tickers":["SPY","QQQ"]}

curl http://localhost:8000/
docker compose logs ingestion   # should show it connected to the DB
```

Interactive API docs (auto-generated by FastAPI) at http://localhost:8000/docs.

## Using the query API

```bash
# 1-minute option bars for SPY calls with delta 0.15-0.85, over a date range
curl "http://localhost:8000/options/bars?ticker=SPY&start=2026-07-01T00:00:00Z&end=2026-07-02T00:00:00Z&right=call&min_delta=0.15&max_delta=0.85"

# Same, but 5-minute aggregated (from Task 7's continuous aggregate view instead of the raw table)
curl "http://localhost:8000/options/bars?ticker=SPY&start=2026-07-01T00:00:00Z&end=2026-07-02T00:00:00Z&agg=5m"

# A single contract's full history
curl "http://localhost:8000/options/bars?ticker=SPY&start=2026-07-01T00:00:00Z&end=2026-08-15T00:00:00Z&contract_id=.SPY260814C450"

# Underlying (SPY itself) bars - 1m or aggregated (5m/15m/30m/1h/1d/1w), same as options
curl "http://localhost:8000/underlying/bars?ticker=SPY&start=2026-07-01T00:00:00Z&end=2026-07-02T00:00:00Z"
curl "http://localhost:8000/underlying/bars?ticker=SPY&start=2026-07-01T00:00:00Z&end=2026-07-08T00:00:00Z&agg=1h"

# Which contracts are/were tracked for SPY, expiring in a given window
curl "http://localhost:8000/contracts?ticker=SPY&start=2026-07-01&end=2026-07-31"

# Quick summary of what data exists: tracked tickers, each one's date range, total row count
curl "http://localhost:8000/metadata"
```

`/options/bars`, `/underlying/bars`, and `/contracts` paginate via `limit`/`offset` (default limit 1000,
max 20000) — check the response's `returned` field against `limit`: if they're equal, there may be more
data, page with `offset`. `/metadata` is unpaginated (fixed-shape summary, not a filtered list).

If `API_KEY` is set in `.env`, every request needs a matching `X-API-Key` header (`/health` and `/` are
exempt).

## Running tests

Unit tests (mocked — no TastyTrade credentials, no Docker needed):
```bash
pip install -r requirements-dev.txt
pytest
```

**One test file needs a real Postgres to actually run** (skipped by default, shown as `s` in pytest's
output): `tests/test_enum_serialization_postgres.py`, which exists specifically because of a real bug
(see PLAN.md Section 7) that was completely invisible to the rest of this SQLite-backed test suite —
SQLite has no native enum type, so it can't catch a mismatch between how SQLAlchemy serializes a Python
enum and how a real Postgres enum type actually validates it. To run it for real:
```bash
docker compose up -d timescaledb
TEST_DATABASE_URL=postgresql+asyncpg://<user>:<password>@localhost:$HOST_DB_PORT/<db> \
  pytest tests/test_enum_serialization_postgres.py
```
(use the same user/password/db/port as your `.env`)

**Smoke tests — via `docker compose run` (recommended, no local Python setup needed).** Every service
shares the same built image, so any smoke test can run by overriding the command on an existing
service — reusing its env vars (`.env`), network access (can reach `timescaledb` by service name), and
`config.yaml` mount, without shelling into a container or setting up a local venv:
```bash
docker compose build   # after pulling any code update, before the run commands below

docker compose run --rm ingestion python -m scripts.task2_smoke_test --ticker SPY
docker compose run --rm ingestion python -m scripts.task3_smoke_test
docker compose run --rm ingestion python -m scripts.task4_smoke_test
docker compose run --rm ingestion python -m scripts.task5_smoke_test --lookback-days 10
docker compose run --rm ingestion python -m scripts.task6_smoke_test
```
(`ingestion` is just a convenient existing service to borrow the environment from — `api` would work
identically. `task6_smoke_test` doesn't need TastyTrade credentials at all, pure DB reconciliation, so
it'll run fine even without `.env` fully filled in.)

The real backfill/reconciliation jobs have dedicated one-off services (profile-gated, so they don't run
as part of default `docker compose up`):
```bash
docker compose run --rm backfill
docker compose run --rm greeks-backfill
```

**If you have a local Python environment set up instead** (venv with `requirements.txt` installed, and
`DATABASE_URL` pointing at `localhost:$HOST_DB_PORT` rather than the `timescaledb` service name), the
same scripts run directly:
```bash
python -m scripts.task2_smoke_test --ticker SPY
```

`task4_smoke_test.py` runs the real pipeline end-to-end and writes actual rows to your database — worth
running once via plain `docker compose up` too (the `ingestion` service does this for real now, not
just a placeholder loop).

## TastyTrade credentials

Same OAuth2 setup as the Task 0 spike script — see `scripts/task0_spike/README.md`
for how to obtain `TASTYTRADE_CLIENT_SECRET` and `TASTYTRADE_REFRESH_TOKEN`.
Not required for this task to work (the placeholder ingestion service just
logs a warning if they're missing), but you'll need them starting Task 2.

## Working with the database directly

```bash
docker compose exec timescaledb psql -U options_user -d options_data
```

## Running migrations without the full stack

If you're iterating on the schema and don't want to rebuild the image every
time:
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql+asyncpg://options_user:changeme@localhost:${HOST_DB_PORT:-5432}/options_data
alembic upgrade head
```
(Requires TimescaleDB to already be running and reachable on localhost —
e.g. via `docker compose up timescaledb`. If you changed `HOST_DB_PORT` in
`.env` because 5432 was already taken, use that same port here.)

## Repo layout

```
service/
  sources/     — MarketDataSource interface (Task 2 implements TastyTradeSource)
  ingestion/   — streaming pipeline, bar aggregation (Task 4)
  greeks/      — Black-Scholes calculator (Task 6)
  api/         — FastAPI query app (Task 8)
  db/          — SQLAlchemy models, session management
  config/      — config loading/validation
alembic/       — DB migrations
scripts/       — one-off utility scripts, e.g. task0_spike/ (the Task 0
                  feasibility spike that confirmed candle-history depth)
docker-compose.yml
Dockerfile
PLAN.md        — the full project plan; read this first
```
