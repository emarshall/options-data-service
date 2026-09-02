# Options Data Collection Service — Project Plan

**Purpose of this document:** This is the single source of truth for the project. Every task below is
written to be self-contained enough that it can be pasted into a **new conversation** with Claude and
picked up without re-deriving context. When starting a new conversation for a task, upload/paste this
whole file plus the specific task section, and say "let's work on Task N."

**How to use this doc across sessions:**
1. Paste this file (or relevant sections) into a new conversation.
2. Say which Task # you want to work on.
3. Discuss/refine that task's open questions if any remain.
4. Implement.
5. Before ending the session, ask Claude to update this PLAN.md with: task status, any decisions made,
   any new open questions discovered, and files produced — then download the updated PLAN.md and carry
   it into the next session.
6. **If any real bug was found and fixed along the way** (not a misunderstanding — something actually
   wrong in delivered code/config/an assumption in the plan), add an entry to **Section 7 (Bug Fixes /
   Gotchas Log)** before ending the session, using the template at the bottom of that section. Worth a
   skim at the start of a new task too, in case it's relevant to what you're about to build.

---

## 1. Project Goal

Build a self-hosted, dockerized system that:
- Streams live options market data (quotes, mark price, bid/ask, Greeks) for a configurable set of
  underlying tickers from TastyTrade via DXLink, filtered to a configurable delta range.
- Persists this data efficiently as OHLC bars at 1-minute granularity, tagged with full contract
  metadata (expiration, strike, right, settlement type) and Greeks.
- Automatically derives higher timeframe aggregates (5m, 15m, 30m, 1h, 1d, 1w, etc.) from the 1m data
  without a separate manual ETL step.
- Exposes a query API so a separate backtesting script (out of scope for this project, built later) can
  pull historical bars by ticker/date range/aggregation period.
- Is architected so a second data source (e.g., a paid historical vendor, or Schwab for live
  cross-validation) can be plugged in later without a rewrite.

**End use case:** backtesting options strategies, including 0DTE, without paying for a historical
options data subscription.

---

## 2. Feasibility Research — Findings (already completed, do not re-litigate)

This was verified against TastyTrade's developer docs, the dxfeed knowledge base, and TastyTrade's
official SDKs before any design work started.

| Data | Available via TastyTrade/DXLink? | Notes |
|---|---|---|
| Live streaming Greeks (delta/gamma/theta/vega/rho, price, IV) per option contract | **Yes** | `Greeks` event over DXLink, subscribed per option streamer-symbol. Real-time only. |
| Live streaming quotes (bid/ask/mark) per option contract | **Yes** | `Quote`/`Trade` events, same mechanism. |
| Historical OHLC candles at custom periods (1m/5m/etc.) for option symbols | **Yes, bounded** | **CONFIRMED via Task 0 spike (2026-07-17, SPY):** 1-minute candles are retained for a trailing ~6 week window (~43 days) uniformly across all contracts tested, regardless of expiration/listing date — requesting further back just returns nothing older than that. Critically, **history survives past a contract's expiration** (a contract that expired the prior day still returned its full 6-week backfill), so the practical rule is: any given trading day's 1m option data is backfillable up to ~6 weeks after the fact, not indefinitely, and not zero. `Candle` events also include `ImpVolatility`, `OpenInterest`, `Volume`, `VWAP`, `BidVolume`/`AskVolume` — all populated. Daily-period (`1d`) candle depth was inconclusive in this test (counts tracked contract listing recency rather than revealing a separate cap) and doesn't block anything since daily granularity isn't critical to the 0DTE goal. |
| Historical Greeks (Greeks *as of* some past date/time) | **No** | dxfeed's own docs state: "Only real-time Greeks are available via API. If you require a historical extraction of the event, contact your sales manager." This is a paid, institutional-only product. |
| Schwab API as an alternative | **Same limitation** | Schwab's Trader API gives live option-chain snapshots with Greeks (real-time only) and historical price bars for the *underlying*, not per-option-contract historical bars. Does not solve the historical-Greeks gap. |

**Conclusion / architectural consequence:** This system is primarily a **forward-accumulating** data
lake, with a **bounded ~6-week retroactive backfill window** confirmed empirically (Task 0, see Section
5 below). Greeks specifically (delta/gamma/theta/vega/rho) are never available historically — only IV
and price/volume/OI are, via candles — so:
- **Live-streamed Greeks are the primary source of truth**, captured continuously going forward.
- **A Black-Scholes Greeks calculator is a first-class fallback/backfill path** (Task 6). Task 0
  confirmed `Candle` events already include `ImpVolatility`, so this calculator does **not** need to
  back-solve IV from price — it only needs to compute delta/gamma/theta/vega/rho from a known IV,
  strike, expiration, and underlying price. Simpler and more numerically stable than originally scoped.
- **Task 5 (historical backfill) is confirmed worth building**, but scoped to a rolling ~6-week window,
  not an unbounded one — see Task 5 below for the resulting priority change.
- This also directly serves the "pluggable multiple sources" requirement, since the calculator doesn't
  care which feed produced the raw price data.

---

## 3. Locked-In Architecture Decisions

These were discussed and decided — do not revisit without good reason.

- **Language/runtime:** Python (async), using `asyncio` for the streaming ingestion pipeline.
- **Database:** PostgreSQL + the TimescaleDB extension.
  - Rationale: hypertables give efficient time-series storage with native compression (space-efficient
    for a growing tick/bar archive), and **continuous aggregates** natively solve the "precompute vs
    compute-on-the-fly" question raised in the requirements — they are incrementally-refreshed
    materialized views, so 5m/15m/30m/1h/1d/1w rollups are always up to date without a hand-rolled
    cron/ETL job, and query fast because they're pre-materialized, not recomputed per request.
  - This is a deliberate step up in setup complexity from SQLite, justified because the volume/duration
    profile below is enough that TimescaleDB's compression and continuous aggregates pay for themselves.
- **Expected scale:** 5–20 underlying tickers, running indefinitely (long-lived, always-on service).
  This informs retention/compression policy defaults (Task 7) and connection/subscription limits
  (Task 3/4) — should be revisited if scale assumptions change materially.
- **Base granularity stored from the live feed:** 1-minute bars (open/high/low/close of mark price,
  plus bid/ask, plus a Greeks snapshot). All coarser aggregation periods are derived from this via
  TimescaleDB continuous aggregates, not stored/computed independently.
- **Source abstraction:** all ingestion goes through a `MarketDataSource` interface (auth, subscribe
  quotes, subscribe greeks, subscribe/request candles, resolve option chain). TastyTrade/DXLink is the
  first implementation. This interface is what allows a second source to be added later.
- **Contract filtering:** configurable per ticker — delta range for calls (e.g. 0.15–0.85) and puts
  (e.g. -0.85 to -0.15), and whether to include/exclude AM-settled index products (default: exclude).
- **Deployment target:** docker-compose stack, intended to run continuously on a home server /
  always-on docker host.

---

## 4. Open Questions (not yet resolved — revisit as they become relevant)

- ~~Exact historical candle depth for option contracts on TastyTrade's retail tier~~ **RESOLVED** —
  see Task 0 findings above (~6 weeks trailing, 1-minute, survives expiration).
- ~~Exact list of initial tickers and per-ticker delta ranges — can be decided at config time (Task
  1/9)~~ **RESOLVED (Task 9):** `config.yaml` tracks SPX/NDX (0.15-0.85 call delta, 10 DTE cap,
  AM-settled excluded) and VIX (same delta range, 45 DTE cap, AM-settled *included* — see Task 9's log
  entry for why VIX needs that exception).
- Retention policy specifics (e.g., compress after 7 days, drop raw ticks after N months if we ever
  ingest tick-level data) — decide during Task 7 once real data volume is observed.
- Whether to also capture underlying (stock/index) 1m bars alongside options, to support Black-Scholes
  backfill and to give the backtest script spot price context — **leaning yes**, should be confirmed in
  Task 3/4.
- Whether daily-period (`1d`) candle history has its own depth cap independent of contract listing
  date — inconclusive from Task 0's short-dated test contracts. Not blocking; revisit only if deep
  daily history becomes a real need later.
- ~~Auth model for TastyTrade (username/password session vs OAuth2)~~ **RESOLVED (discovered during
  Task 0):** TastyTrade discontinued username/password session-token authentication on December 1,
  2025. OAuth2 (client_secret + refresh_token) is now the only option, so this isn't a choice to make
  in Task 2 — it's mandatory. One-time setup: create an OAuth application on TastyTrade's site (gets
  you a client_id + client_secret), then generate a refresh_token either via the site's
  "OAuth Applications > Manage > Create Grant" button (simplest) or via the `tastytrade.oauth.login()`
  interactive helper (required for sandbox accounts). Refresh tokens don't expire, so this is a
  once-per-environment (prod/sandbox) setup, not a recurring one. Task 2 should still handle *using*
  the refresh token robustly (the SDK auto-refreshes the short-lived session token from it), but the
  "which auth model" question itself is closed.

---

## 5. Task List

Each task below is intended to be small enough to implement in one focused session. Status values:
`NOT STARTED`, `IN PROGRESS`, `BLOCKED`, `DONE`.

### Task 0 — Feasibility Spike: Confirm Historical Option Candle Depth
**Status:** DONE (2026-07-17)

**Findings:**
- 1-minute candle history for options: retained for a trailing **~6 weeks (~43 days)**, uniformly
  across contracts, regardless of contract-specific listing/expiration date. Requesting further back
  than that returns nothing older — confirmed by 3 different contracts (different strikes and
  expirations) all bottoming out at the exact same earliest timestamp (2026-06-04, ~43 days before the
  test date).
- **History survives contract expiration.** A contract that expired the day before the test still
  returned its full ~6-week backfill (2,330 one-minute bars). This is the key result: backfill is
  possible for any day up to ~6 weeks after the fact, not just while a contract is still live.
- `Candle` events include `ImpVolatility`, `OpenInterest`, `Volume`, `VWAP`, `BidVolume`/`AskVolume` —
  all populated in every sample. `ImpVolatility` being present means Task 6's Black-Scholes calculator
  doesn't need to back-solve IV from price, simplifying that module.
- Daily-period (`1d`) candle depth was inconclusive — bar counts (4-10) tracked how recently each
  short-dated test contract was likely listed rather than revealing a separate, independent cap. Not
  investigated further since daily granularity isn't critical to the 0DTE goal; revisit only if a
  future task specifically needs deep daily history.
- Live `Quote` and `Greeks` events work exactly as expected; real sample payloads captured and used to
  inform the schema (see Task 1).
- Auth note (discovered during this task, not part of the original scope): TastyTrade discontinued
  username/password session auth on Dec 1, 2025 — OAuth2 (client_secret + refresh_token) is now
  mandatory. Folded into Section 3/4 above.

**Deliverables produced:** `task0_candle_depth_spike.py`, `get_refresh_token.py`, `requirements.txt`,
`.env.example`, `README.md` — all in the `task0_spike/` folder. Raw findings in `summary.md` and
`sample_data.json` from the actual run (not included in this repo, but their conclusions are captured
above and in Section 2).

---

### Task 1 — Repository Scaffolding, Docker Compose Skeleton, DB Schema
**Status:** DONE (2026-07-17)

**What was actually delivered** (repo layout matched the plan below closely; a few decisions were
refined during implementation):
- Full `service/` package (sources/ingestion/greeks/api/db/config), Alembic migrations, Docker Compose
  stack (`timescaledb` + one-shot `migrate` + `ingestion` + `api`), `.env.example`, `config.example.yaml`.
- Schema refinement vs. the original draft: `underlying_ticker`/`expiration_date`/`strike`/`right` are
  **denormalized directly onto `option_bars_1m`**, not just `contracts` — Task 8's query endpoint
  filters on exactly those fields, and requiring a join against `contracts` on every query against a
  hypertable wasn't worth it. Also added `underlying_bars_1m` (resolving the Section 4 open question
  about capturing underlying bars as yes), and `open_interest`/`vwap`/`bid_volume`/`ask_volume` columns
  on `option_bars_1m` (confirmed available/useful per Task 0's sample data).
- `greeks_source` column (`live` | `computed`) added to `option_bars_1m` — not in the original plan
  text, but implied by the Task 6 design (Black-Scholes fallback) and cheap to add now rather than as a
  later migration.
- Placeholder `ingestion`/`api` entrypoints that prove DB connectivity end-to-end, ahead of real logic
  landing in Tasks 2-4.

**Deliverables:** `options-data-service/` repo (delivered as a zip), containing everything above plus
this `PLAN.md` and the Task 0 spike script under `scripts/task0_spike/`.

**Notable bugs hit and fixed while getting `docker compose up` working — see Section 7 (Bug Fixes /
Gotchas Log) below for full details.** Short version: a port conflict (made the host DB port
configurable), a password duplicated in two places causing auth mismatches (fixed by deriving
`DATABASE_URL` from the individual `POSTGRES_*` vars instead), a Postgres two-phase-startup race (fixed
with a retry loop in `alembic/env.py`), and a SQLAlchemy gotcha where a shared Postgres ENUM type
wasn't reliably respecting `create_type=False` across multiple tables (fixed with idempotent raw SQL
+ the dialect-specific `postgresql.ENUM` class). All four were reproduced and the fixes verified against
a real (temporarily-installed) Postgres instance before being marked resolved, not just reasoned about.

---

### Task 2 — TastyTrade Auth + DXLink Connection Wrapper
**Status:** DONE (2026-07-17)

**What was delivered:**
- `service/sources/tastytrade.py` — `TastyTradeSource`, implementing the `MarketDataSource` interface
  from Task 1: OAuth2 auth (`authenticate()`), option chain lookup, persistent quote/Greeks
  subscriptions with a background listener loop per event type, automatic reconnect with capped
  exponential backoff + full resubscription on disconnect, one-off historical candle requests
  (`request_candles`), and clean shutdown (`close()` — added to the base interface, wasn't anticipated
  in Task 1's stub).
- `service/sources/_async_utils.py` — the hang-proof `with_timeout`/`collect_events` helpers, extracted
  from Task 0's spike script (where both underlying bugs were actually found and fixed — see Section 7)
  and generalized so any future source implementation can reuse them, not just TastyTrade's.
- Design decision made during implementation: `subscribe_quotes()`/`subscribe_greeks()` take **one
  callback per event type**, not per-symbol — calling either again replaces the callback but symbols
  accumulate. Matches how Task 4's ingestion pipeline will actually use this (one handler dispatching
  internally by `event_symbol`). Documented in the module docstring as a deliberate simplification.
- `tests/test_tastytrade_source.py` — unit tests against a fake streamer/session (no credentials
  needed), covering subscription bookkeeping, callback replacement, unsubscribe, disconnect →
  reconnect → resubscribe, and candle request lifecycle. All 6 passing.
- `scripts/task2_smoke_test.py` — real-credentials smoke test for validating against TastyTrade's
  actual servers (the one thing the unit tests can't do) — auth, chain lookup, live quote/Greeks
  subscription, candle backfill, clean unsubscribe/close.

**How this was validated without live credentials in-session:** the real `tastytrade` PyPI package
(v13.2.0 at the time) was installed and its actual class signatures (`Session.__init__`,
`DXLinkStreamer.subscribe`/`unsubscribe`/`listen`/`subscribe_candle`, `get_option_chain`) were inspected
directly rather than assumed — this is what Task 0's debugging should have taught us to do from the
start (see Section 7). All 6 unit tests were actually executed (not just written) against this real
package with fakes standing in for the network layer. `scripts/task2_smoke_test.py` was subsequently
run against a real TastyTrade account and **passed** — auth, chain lookup, live quote/Greeks
subscription, and candle backfill all confirmed working end-to-end, not just unit-tested.

**Deliverables:** all files above, part of the same `options-data-service` repo bundle.

---

### Task 3 — Contract Resolution & Delta-Range Filtering
**Status:** DONE (2026-07-18)

**What was delivered:**
- `service/ingestion/contract_manager.py` — `ContractManager`: given the configured tickers, resolves
  candidate contracts (chain lookup, filtered by `max_days_to_expiration` and AM-settlement), takes a
  one-time live Greeks snapshot of genuinely *new* candidates to filter by delta range, persists
  metadata to the `contracts` table, and returns a `ContractDiff` (added/removed/current) each refresh.
- `tests/test_contract_manager.py` — 9 unit tests, using a fake source plus a **real** in-memory SQLite
  DB (not mocked) for the persistence logic. All passing.
- `scripts/task3_smoke_test.py` — real-credentials smoke test against an actual TastyTrade chain +
  Greeks + your configured DB.

**Scope decision vs. the original plan draft:** the original text implied this task would directly call
`subscribe_quotes()`/`subscribe_greeks()` on the source for the real ingestion subscriptions.
Implemented instead so `ContractManager` only *resolves* the set and returns a diff — actually
subscribing with a bar-aggregating callback is Task 4's job. Keeps this task testable without a real
aggregator needing to exist yet.

**Key design decision: delta filtering happens once, at "listing" time, not continuously.** Once a
contract is tracked, its delta is never re-checked — it stays tracked until it disappears from the
chain entirely (typically at/after expiration). Two reasons: (1) a real hazard this avoids —
re-snapshotting an already-subscribed symbol's Greeks and cleaning up that snapshot risks
unsubscribing the *persistent* ingestion subscription for the same symbol, since `snapshot_greeks()`
and `subscribe_greeks()` share the same underlying DXLink subscription state; (2) "track what started
in your delta band" is a simple, defensible policy for a backtesting dataset — drifting out of the
band over time is itself useful signal to have captured, not a reason to stop recording it.

**Interface change to Task 2's `MarketDataSource`:** added `snapshot_greeks(symbols, timeout_s)` — a
one-off Greeks read, deliberately separate from `subscribe_greeks()`'s persistent callback slot, for
exactly the reason above. Implemented in `TastyTradeSource` reusing the same
subscribe/collect/unsubscribe pattern already validated for `request_candles`.

**`max_days_to_expiration` added to `TickerConfig`** (default 45, config.example.yaml updated) — without
bounding this, resolving contracts means Greeks-snapshotting every strike across every expiration in
the full chain (which can run out 1-2+ years for equities) just to filter by delta. Default chosen to
sit just past Task 0's confirmed ~6-week candle-history retention window.

**Validated by:** real `Option`/`OptionType` model fields inspected directly against the actual
installed `tastytrade` v13.2.0 package before writing filtering logic against them (same lesson from
Task 2, applied again) — confirmed `settlement_type` is a real, direct field (not something to infer),
and `days_to_expiration` is provided directly rather than needing date-math. All 9 new tests (plus all 6
from Task 2) actually executed and passing, not just written.

**Open questions:** exact refresh cadence for calling `ContractManager.refresh()` in the real running
service (daily at market open vs. more frequent, to catch same-day 0DTE listings promptly) — deferred to
Task 9 (scheduling), doesn't block Task 4. `scripts/task3_smoke_test.py` has since been run for real
(after-hours) — it correctly resolved a real contract from a real chain, but the subsequent DB write hit
the enum-serialization bug documented in Section 7 (now fixed). Worth re-running during market hours to
sanity-check real delta filtering results with live Greeks, which after-hours can't exercise. The person
has also flagged they may want to revisit the filtering approach itself (what/how contracts get
filtered) once they can see it running against live data — treat the current implementation as a solid
first pass, not a settled decision.

**Deliverables:** all files above, part of the same `options-data-service` repo bundle.

---

### Task 4 — Live Ingestion Pipeline (Streaming → 1m Bars → DB)
**Status:** DONE (2026-07-18)

**What was delivered:**
- `service/ingestion/bar_aggregator.py` — `BarAggregator`: buckets Quote/Greeks events into 1-minute
  OHLC(+Greeks) bars, keyed by (key, minute-epoch) where `key` is agnostic to whether it's an option
  `contract_id` or an underlying ticker. `pop_ready(now)` returns and removes buckets whose minute has
  fully elapsed plus a grace period.
- `service/ingestion/pipeline.py` — `IngestionPipeline`: wires `TastyTradeSource` + `ContractManager` +
  two `BarAggregator` instances (options, underlying) into the real loop — initial subscribe, periodic
  contract refresh + resubscribe, periodic flush to DB, graceful shutdown that flushes in-progress bars.
- `service/ingestion/main.py` — replaced Task 1's placeholder with the real thing: authenticates, runs
  `IngestionPipeline.run()`, handles SIGTERM/SIGINT for graceful shutdown.
- 18 new tests: `tests/test_bar_aggregator.py` (11, pure logic) and `tests/test_pipeline.py` (7,
  fake source + real in-memory SQLite + an injectable clock for deterministic bucket-timing without
  sleeping real seconds). All 33 tests in the repo (Task 2/3/4 combined) passing.
- `scripts/task4_smoke_test.py` — runs the real pipeline end-to-end for a configurable window against
  TastyTrade + your real DB.

**Open questions resolved during implementation:**
- **Mark vs. last-trade price:** resolved in favor of **mark (bid/ask midpoint)**. Task 0's captured
  sample data confirmed Quote events reliably carry bid/ask; there's no separate "mark" field on the
  event itself, so it's computed as the midpoint (falling back to whichever side is present if only one
  quotes, e.g. deep OTM contracts sometimes quote one-sided).
- **Sparse data handling:** resolved in favor of skip (not carry-forward) — a bucket only exists if at
  least one Quote or Greeks event actually landed in that minute. Matches the original leaning in the
  plan draft.

**Real finding from Task 0 that shaped this, applied directly:** Quote events don't carry a usable
timestamp (`event_time`/`bid_time`/`ask_time` all `0` in Task 0's captured samples) — so bucketing uses
**wall-clock receipt time**, not any field on the event, for both Quotes and Greeks (Greeks events do
have a real `time` field, but using receipt time for both keeps this consistent and the asymmetry
doesn't matter at 1-minute granularity).

**Design interaction with Task 2's "one callback per event type" rule:** options and the underlying both
arrive as `Quote` events but need different downstream handling (different aggregator, different
denormalized metadata). Since a second `subscribe_quotes()` call *replaces* the callback rather than
multiplexing, both symbol sets are subscribed through the same shared `_on_quote` handler, which
dispatches internally by checking whether the symbol is a tracked underlying ticker. Exactly the "one
handler dispatching by event_symbol" pattern Task 2's docstring anticipated.

**Known limitation, deliberate:** live-aggregated bars have `volume`/`open_interest`/`vwap`/
`bid_volume`/`ask_volume` as NULL — those fields only come from `Candle` events, which this pipeline
doesn't subscribe to (that's what Task 5's backfill uses). Only backfilled bars will have them
populated. Revisit only if this turns out to matter for the eventual backtest script (e.g. by also
subscribing to `Trade` events, not currently in scope).

**Validated by:** all 33 tests actually executed (11 new aggregator tests + 7 new pipeline tests + all
26 from Tasks 2/3), not just written. `scripts/task4_smoke_test.py` still needs a real run against
TastyTrade + a live market to fully close this out.

**Deliverables:** all files above, part of the same `options-data-service` repo bundle.

---

### Task 5 — Historical Backfill via Candle Events (Bounded ~6-Week Window)
**Status:** DONE (2026-07-18)

**What was delivered:**
- `service/ingestion/backfill.py` — `BackfillJob`: resolves the current contract set (via
  `ContractManager`), requests 1-minute candles ~60 days back (comfortably beyond the confirmed ~6-week
  retention window — over-requesting is harmless) for every tracked option contract, writes price/
  volume/open_interest/vwap/IV to `option_bars_1m`, leaving Greeks NULL for Task 6 to fill in later.
  Runnable standalone (`python -m service.ingestion.backfill`) or via `docker compose run --rm backfill`
  (added as a profile-gated one-shot service, kept out of default `docker compose up`).
- 10 unit tests (`tests/test_backfill.py`, fake source + real in-memory SQLite). All 43 tests in the
  repo (Tasks 2-5 combined) passing.
- `scripts/task5_smoke_test.py` — real-credentials smoke test.

**Post-delivery fix: recovers contracts that fully expired during a downtime gap.** Originally,
`BackfillJob` only backfilled `ContractManager`'s live-resolved set — but that requires a live Greeks
snapshot to resolve anything, so a contract whose entire lifecycle (delta-eligible → expired) happened
while the ingestion service was down would never be rediscovered, even though its candle history almost
certainly still exists (Task 0 confirmed candle history survives past expiration). Fixed by also pulling
candidate contracts directly from the `contracts` table — anything with an expiration inside the
lookback window, regardless of whether it's still live-resolvable today — since backfill only needs a
contract's identity (already persisted from when it was originally tracked), not its Greeks. This means:
a 10-minute outage backfills cleanly either way; a 2-week outage now correctly recovers contracts that
expired entirely during the gap, not just ones still active today. 3 new tests cover this directly
(recovers an expired-during-downtime contract; doesn't reach back further than the lookback window;
doesn't double-count a still-live-resolvable contract as "recovered"). Summary dict now includes
`contracts_recovered_from_db` for visibility.

**Scope extension vs. the original plan draft:** also backfills the **underlying's own price history**
for tickers with `capture_underlying_bars` enabled, not just option contracts. Necessary, not scope
creep — Task 6's Black-Scholes calculator needs a spot price to compute Greeks for backfilled option
rows, and without this, no such spot price would exist for any day before the service first ran.

**Key design decision: idempotent via skip-if-exists, not merge-or-overwrite.** A candle-derived row is
only written if no row already exists for that (contract_id, minute) — if Task 4's live ingestion (or a
prior backfill run) already wrote one, it's left completely untouched. Considered merging (e.g. filling
in volume/OI onto an existing live-ingested row) but rejected: live rows use mark-price OHLC (bid/ask
midpoint), backfilled rows use trade/settlement-price OHLC from TastyTrade's own candle aggregation —
mixing the two under one column would make the OHLC semantics inconsistent row-to-row in a way that's
invisible to a downstream backtest script. Skip-if-exists also gives idempotency for free: safe to
re-run after any downtime, or on a recurring schedule (Task 9), without duplicating or corrupting data.

**Validated by:** all 10 tests actually executed (plus all 33 from Tasks 2-4, no regressions) — 43
total. Also re-confirmed `session.get()`'s composite-primary-key dict-lookup behavior against a real
SQLite engine before relying on it in the skip-if-exists check, rather than assuming the API shape (same
discipline as Tasks 2-4). `scripts/task5_smoke_test.py` has since been run for real — hit the same
enum-serialization bug documented in Section 7 as Task 3 (same underlying `Contract`/`OptionBar1m` write
path), now fixed.

**Open questions:** none blocking. Worth knowing: this hasn't yet been run against live TastyTrade
during market hours — contract resolution (Task 3) requires live Greeks to populate the tracked set in
the first place, so a fully "cold" backfill run (no contracts tracked yet) would currently backfill
underlyings only. Revisit once Tasks 3-5's smoke tests are run for real (see note at the end of Task 3).

**Deliverables:** all files above, part of the same `options-data-service` repo bundle.

---

### Task 6 — Black-Scholes Greeks Calculator (Fallback/Backfill)
**Status:** DONE (2026-07-19)

**What was delivered:**
- `service/greeks/black_scholes.py` — pure math module: `bs_price`, `compute_greeks`,
  `implied_volatility`. Simpler than originally planned (see below). IV back-solving uses bisection, not
  Newton-Raphson — deliberate: BS price is monotonic non-decreasing in sigma (vega >= 0 always), so
  bisection is guaranteed to converge given a valid bracket, with no risk of the numerical instability
  Newton-Raphson hits when vega is tiny (common here — deep OTM or near-expiration, exactly the
  conditions common in 0DTE data). Not latency-sensitive (batch job), so the robustness trade is a clear
  win. Greeks returned in TastyTrade's own convention (vega per 1% IV, theta per calendar day, rho per
  1% rate) confirmed against Task 0's real captured sample data, not assumed.
- `service/greeks/backfill_job.py` — `GreeksBackfillJob`: finds `option_bars_1m` rows with
  `greeks_source IS NULL`, looks up a matching `underlying_bars_1m` row at the same minute for a spot
  price, computes Greeks, persists them with `greeks_source = COMPUTED`. Time-to-expiration assumes
  4:00 PM America/New_York close, handled via `zoneinfo` (DST-safe). Paginates via repeated
  `WHERE greeks_source IS NULL LIMIT batch_size` queries rather than OFFSET (processed rows drop out of
  the WHERE clause naturally, avoiding OFFSET's well-known degradation on large tables).
- Runnable standalone (`python -m service.greeks.backfill_job`) or via
  `docker compose run --rm greeks-backfill` (profile-gated, doesn't need TastyTrade credentials at all —
  pure DB reconciliation).
- 30 new tests: `tests/test_black_scholes.py` (20 — validated against **known textbook reference
  values**, e.g. S=100/K=100/T=1yr/r=5%/σ=20% has a published correct call price of 10.4506 and this
  implementation matches it, not just internally-consistent math) and `tests/test_greeks_backfill_job.py`
  (10, real in-memory SQLite with actual rows). 84 tests total in the repo, all passing.
- `scripts/task6_smoke_test.py` — smoke test against your real DB (no TastyTrade credentials needed).

**Why this ended up simpler than the original plan draft:** Task 0 already confirmed `Candle` events
include `ImpVolatility` directly, so the *common* case (Task 5's backfilled rows) already has IV in
hand — `compute_greeks` just turns it into delta/gamma/theta/vega/rho, no solving needed. IV
back-solving only matters for the rarer Task 4 gap case (a bar with a price but no Greeks *and* no IV).

**Open question from the original plan resolved:** dividend yield defaults to 0.0 throughout (the `q`
parameter), per the plan's own reasoning — fine for backtesting, revisit only if accuracy issues
actually surface for dividend-paying underlyings. European exercise assumed (standard BS) — reasonable
for the project's liquid, short-dated equity/index option focus.

**Real bug found and fixed while testing this (not from live data — from actually running the tests):**
`_time_to_expiry_years` assumed `row.time` is always timezone-aware, which holds in production
(Postgres's `TIMESTAMPTZ` preserves it) but not against SQLite (used in tests — no native
timestamp-with-timezone type, round-trips as naive). Fixed by defensively assuming UTC for a naive
datetime, since that's the storage convention everywhere in this app regardless of backend. Caught
immediately by the test suite rather than surfacing later against a real deployment.

**Separately, while testing the real backfill job against live TastyTrade data (not part of Task 6's
own scope, but fixed in this session — see Section 7 for the full writeup):** a real production bug
was found and fixed — `ContractManager`/`snapshot_greeks()` subscribing to too many symbols in one
DXLink message (exceeding the server's 64KB frame limit), compounded by a `CancelledError` bypassing
exception handling and crashing the entire backfill job instead of failing gracefully for one ticker.

**Deliverables:** all files above, part of the same `options-data-service` repo bundle.

---

### Task 7 — TimescaleDB Continuous Aggregates for Higher Timeframes
**Status:** DONE, partially validated (2026-07-19/20) — one real bug found and fixed against actual
Postgres (a raw-SQL identifier-quoting issue with the `right` column, see Section 7), but the
TimescaleDB-specific parts (continuous aggregates, compression policies) remain genuinely untested,
since the extension itself still isn't installable in this environment. First real attempt at running
this migration hit the `right`-quoting bug immediately — expect at least one more round once the
TimescaleDB-specific SQL gets its first real run.

**What was delivered:**
- `alembic/versions/0002_continuous_aggregates.py` — continuous aggregates for 5m/15m/30m/1h/1d/1w,
  each built directly from `option_bars_1m` (not chained from each other — see migration docstring for
  why), plus compression policies on both `option_bars_1m` and `underlying_bars_1m`.

**Why this one couldn't be validated the way every prior task was:** the TimescaleDB extension itself
isn't installable in this environment without Timescale's own package repository, which isn't reachable
here (confirmed back in Task 1, when the hypertable/enum work in migration 0001 needed a real,
temporarily-installed Postgres to actually catch two real bugs — plain Postgres was available via apt,
but not the TimescaleDB extension on top of it). This migration is carefully reasoned from documented
TimescaleDB syntax and behavior, but genuinely has NOT been executed against a real instance the way
0001 was. **Treat this as the highest-risk piece of the codebase currently** and expect it may need a
debugging round in the same style as 0001's original rollout, once actually run.

**Key design decisions, each with real reasoning behind it (full detail in the migration's own
docstring, which is unusually long for exactly this reason):**
- **Every refresh policy uses the same ~65-day `start_offset`, regardless of the view's own bucket
  size.** A continuous aggregate's refresh policy only re-examines a moving window relative to "now" —
  if that window were narrower than Task 5's 60-day backfill lookback, backfilled data older than the
  window would silently never reach the aggregated views. 65 days (60 + margin) is used uniformly. This
  is cheap in practice, not a real tradeoff — TimescaleDB's invalidation-log-based incremental refresh
  only touches actually-changed time ranges, not a full re-scan, regardless of the nominal window size.
- **Compression is delayed to 75 days, not the "7-14 days" the original plan draft sketched as an
  example.** Real conflict identified while designing this: compressing chunks as early as 14 days
  would mean any backfill run touching 15-60-day-old data (well within Task 5's normal lookback) would
  be inserting into already-compressed chunks — handled with varying grace across TimescaleDB versions,
  and not verifiable here which behavior the actually-deployed version has. Delaying compression past
  the entire backfill window sidesteps the conflict entirely. This directly resolves the "exact
  compression/retention thresholds" open question the original plan explicitly deferred.
- **Aggregates are NOT chained from each other** (e.g. 1w built from 1d rather than from raw 1m) even
  though TimescaleDB supports this and it would be marginally more efficient for the largest buckets.
  Skipped because that feature has had version-specific edge cases historically, unverifiable without
  live testing — simpler and more likely to just work, revisitable later.
- **Aggregation function per column:** true OHLC (first/max/min/last) for prices; last-value-in-bucket
  for Greeks and open_interest (point-in-time snapshots, not additive — matches the original plan's own
  instinct); sum for volume/bid_volume/ask_volume; a genuine volume-weighted average for vwap
  (`sum(vwap*volume)/sum(volume)`, correctly falling back to NULL when there's no volume data at all —
  relevant given Task 4's known limitation that live-only bars have NULL volume).

**What actually was validated:** the Alembic revision chain resolves correctly (`0001 -> 0002 (head)`),
the migration module imports cleanly, and the generated SQL was visually re-inspected for syntax
correctness (parens, commas, GROUP BY completeness) — but none of that substitutes for actually running
it against TimescaleDB.

**Deliverables:** `alembic/versions/0002_continuous_aggregates.py`, part of the same
`options-data-service` repo bundle. **Next step, whenever convenient:** run `alembic upgrade head` for
real and report back whatever happens — same collaborative debugging pattern that got migration 0001
working.

**Extension (2026-07-24): `alembic/versions/0003_underlying_continuous_aggregates.py`** adds the same
5m/15m/30m/1h/1d/1w continuous aggregates for `underlying_bars_1m`, requested specifically so Task 8's
`/underlying/bars` endpoint could serve aggregated underlying bars, not just 1m. Same `start_offset`
reasoning as 0002 (uniformly wide, tied to Task 5's backfill window) and the same "not live-tested
against real TimescaleDB" caveat — but structurally simpler than the option bars aggregates (no enum
columns, no per-contract dimension, just OHLCV grouped by `(time_bucket, ticker)`), so the `"right"`
quoting concern that bit migration 0002 doesn't apply here at all.

---

### Task 8 — Query API
**Status:** DONE (2026-07-23)

**What was delivered:**
- `service/api/routes.py` — three endpoints: `GET /options/bars` (ticker, time range, `agg` —
  1m/5m/15m/30m/1h/1d/1w, right, delta range, expiration, exact contract_id), `GET /underlying/bars`
  (1m only — see below), `GET /contracts` (ticker, expiration range, right).
- `service/db/views.py` — lightweight (non-ORM) SQLAlchemy Core `Table` definitions for the six Task 7
  continuous aggregate views, so `/options/bars` can query whichever one `agg` asks for through the same
  query-building code path as the raw `option_bars_1m` hypertable.
- `service/api/deps.py` (DB session, structured for dependency-override testability) and
  `service/api/auth.py` (optional `X-API-Key` header check, no-op unless `API_KEY` is configured) —
  both already existed from an earlier session that got interrupted mid-task; reconciled a real conflict
  between them during this session (see below).
- Limit/offset pagination on all three endpoints (default limit 1000, max 20000) — `returned < limit`
  unambiguously tells a client they've reached the end without a separate COUNT query.
- 16 integration tests (`tests/test_api.py`) — real HTTP requests via httpx's ASGI transport against the
  real FastAPI app, backed by real in-memory SQLite, not mocked at any layer except the DB session
  (swapped via FastAPI's own dependency-override mechanism). All passing, 105 tests total in the repo
  (103 run, 2 opt-in Postgres tests skipped as designed).

**Real conflict found and fixed reconciling work from an interrupted prior session:** `service/api/deps.py`
and `service/api/auth.py` both already existed with a duplicate `require_api_key` function, and
`deps.py`'s version checked `if not settings.api_key` while the `api_key` setting itself (also added in
this session) defaulted to `""`, not `None` — meaning `auth.py`'s companion `is None` check would never
fire, and every request would have been rejected by default even with `API_KEY` unset. Resolved by
removing the duplicate (keeping `auth.py` as the single source), changing `api_key`'s default to `None`
to match, and additionally loosening `auth.py`'s check from strict `is None` to a plain falsy check (so
an accidentally-empty `API_KEY=` in `.env` doesn't lock everyone out either).

**Real risk identified and resolved before writing any query logic against it:** comparing an untyped
bound parameter against a native Postgres enum column can fail with "operator does not exist" depending
on how the driver types the parameter — confirmed this specific risk directly against a real Postgres
instance *before* committing to the `service/db/views.py` design, and confirmed the fix (matching the
real models' typed `Enum(..., values_callable=...)` columns exactly, not a bare/untyped column) resolves
it. Given this project already hit a real, costly enum-serialization bug once (Section 7), this was
treated as a leading risk to de-risk early rather than something to discover via a failing smoke test
again.

**Open question resolved:** response format is JSON only for now (no CSV/parquet) — the original open
question deferred this until the backtest script's needs were clearer; still deferred, JSON is the
reasonable default until a concrete need for something else shows up.

**Validated by:** all 16 new tests actually executed (105 total, no regressions), plus the riskiest
piece (querying a continuous-aggregate-shaped table with enum/delta filters) independently confirmed
against a real, temporarily-installed Postgres instance before and after writing the route code.

**Deliverables:** all files above, part of the same `options-data-service` repo bundle.

**Extension (2026-07-24): `/underlying/bars` now supports the full `agg` range** (5m/15m/30m/1h/1d/1w),
not just 1m — requested after the original delivery. What changed: `alembic/versions/
0003_underlying_continuous_aggregates.py` (Task 7, see its own entry above) added the missing views;
`service/db/views.py` gained `get_underlying_bars_table()` alongside the existing option-bars version;
the endpoint dropped its `agg != "1m"` rejection in favor of validating against the full `AGG_PERIODS`
tuple, same as `/options/bars` already did. The old "scope boundary, deliberate" note above (only
`agg=1m` supported) no longer applies — recorded here rather than deleted, since PLAN.md Section 7's
instructions are to keep history, not just the current state.

**Testing approach worth noting for future extensions in this vein:** SQLite (used throughout this
project's test suite) obviously can't run real continuous aggregation, so the new tests
(`test_get_option_bars_agg_5m_queries_the_view_table`, `test_get_underlying_bars_agg_5m_queries_the_view_table`
in `tests/test_api.py`) create the view-shaped tables as *plain* SQLite tables (via
`service/db/views.py`'s own Core `MetaData`, alongside the ORM's `Base.metadata`) and insert rows
directly through Core. This genuinely exercises the route's table-selection and filter-building logic
end-to-end over HTTP — proving `agg=5m` actually queries the `option_bars_5m`/`underlying_bars_5m`
shaped table with the right WHERE clauses — without claiming to test TimescaleDB's own materialization
behavior, which stays a real-Postgres-only concern (validated separately, see above). Also added 17
pure unit tests (`tests/test_views.py`) for the table-selection functions themselves. 122 tests total in
the repo now (120 run, 2 opt-in Postgres tests skipped as designed).

**Extension (2026-07-31): `GET /metadata`** — a fourth, unfiltered read endpoint giving a client (mainly
the future backtest script) a quick summary of what data actually exists before it starts querying bars:
which tickers have any `option_bars_1m` rows at all, each ticker's observed time range (`MIN`/`MAX` of
`time`), and a total row count across the whole table. Deliberately raw `text()` SQL rather than the
Core/ORM query-building the other three endpoints use — it's three simple aggregate queries against a
single table, not filterable, so there's no query-building logic worth abstracting. No pagination
(nothing here scales with row count the way bars/contracts do), and no `agg`/ticker/date-range params —
it's a fixed summary, not a filtered list.

Found and fixed two real bugs while wiring this up, both in `service/api/routes.py`:
- `from service.db.session import _engine, get_session` shadowed the earlier `from service.api.deps
  import get_session` import — every route in the file was using `session.py`'s
  `@asynccontextmanager`-wrapped `get_session` (meant for direct `async with` use elsewhere, e.g.
  `check_connection()`) instead of `deps.py`'s plain-generator version that FastAPI's `Depends()`
  actually expects. Symptom: `TypeError: '_AsyncGeneratorContextManager' object is not an async
  iterator` on every request. Fix: only import `_engine` from `service.db.session`; keep `get_session`
  sourced from `service.api.deps`.
- In the new `metadata()` handler itself, `total_rows = (await result_row_count.fetchone())[0]` —
  `AsyncSession.execute()` already returns a resolved (sync) `Result`; `.fetchone()`/`.fetchall()` on it
  are ordinary sync calls, not coroutines. Fix: drop the stray `await`.

Both bugs were caught by actually running the endpoint against the live stack, not by the test suite —
worth adding a `tests/test_api.py` case for `/metadata` specifically so a future regression on either
front fails a test instead of only showing up at request time.

**Deliverables:** `service/api/routes.py` (updated).

---

### Task 9 — Config Finalization, Contract Roll Scheduling, AM-Settlement Handling
**Status:** DONE (2026-08-06)

**What was delivered:**
- **AM-settlement exclusion required no new code.** Checked this first, before touching scheduling,
  since the plan explicitly called out the risk of sourcing settlement type from the wrong place (a
  DXLink streaming field) rather than instrument metadata. `ContractManager` (Task 3) already resolves
  contracts via `TastyTradeSource.get_option_chain()`, which calls the `tastytrade` SDK's
  `instruments.get_option_chain()` — the instruments API, not DXLink — and `settlement_type` on the
  resulting objects is exactly that instrument-level metadata. `tests/test_contract_manager.py` already
  covered this (AM contracts excluded, PM contracts kept). Nothing to change; recorded here so this
  doesn't get re-litigated later as if it were still open.
- **Adaptive contract-refresh cadence** (`service/ingestion/pipeline.py`, `service/config/settings.py`):
  the refresh loop now runs on two cadences instead of one fixed interval — a faster one
  (`CONTRACT_REFRESH_FAST_INTERVAL_S`, default 30s) during a configurable window around market open
  (`CONTRACT_REFRESH_FAST_WINDOW_START`/`_END`, default 09:25-10:00 America/New_York, weekdays only), and
  the normal cadence (`CONTRACT_REFRESH_INTERVAL_S`, default 300s, unchanged from before) the rest of the
  time. This is what gets a same-day/0DTE listing subscribed to promptly without polling the fast cadence
  all day. All four values are env-configurable (previously `contract_refresh_interval_s` was a
  constructor-only parameter that `main.py` never actually wired to config at all — real gap, now
  fixed). Weekend detection means Sat/Sun always use the normal cadence regardless of time-of-day, since
  nothing new lists then.
  **Open item, deliberately not resolved here:** the plan asks for the window to be "informed by real
  observation of when TastyTrade lists same-day 0DTE contracts" — no such observation data exists yet
  (would require logging refresh diffs over live trading days). Shipped with a reasoned default (window
  bracketing the 9:30 ET open) instead of blocking on data collection; narrow/shift the window later using
  real refresh-diff logs once there's evidence for a different range. `PLAN.md`'s own "Open questions"
  for this task already flagged this as "operational tuning," not a blocker.
- **Config finalized:** `config.yaml` already had real tickers (SPX, NDX, VIX — not the SPY/QQQ example
  values) from a prior editing session; the file's header comment was stale, still describing the Task
  3-era state where it wasn't wired into contract selection at all. Updated the comment, and — more
  importantly — **caught and documented a real correctness issue while reviewing it**: VIX had
  `exclude_am_settled: false` set, which looked like an inconsistency next to SPX/NDX's `true` until
  checked against how VIX actually settles. Confirmed (via Cboe's own VIX options settlement
  documentation) that *every* VIX option is AM-settled — unlike SPX/NDX, where AM vs PM is a per-contract
  distinction — so leaving the default `true` on VIX would exclude 100% of its contracts and silently
  track nothing for that ticker. The existing `false` was correct; it just wasn't explained anywhere, so
  a future edit could easily "fix" it into a bug. Added that explanation directly in `config.yaml` and in
  the new README section on configuring tickers.
- 3 new unit tests (`tests/test_pipeline.py`) covering: cadence switches correctly across the fast
  window's boundaries (inclusive start/end) and outside it, weekends always use the normal cadence even
  during the window's time-of-day, custom (non-default) window/interval values are honored, and explicit
  constructor args still override settings (kept for tests/future callers that want to force a cadence
  directly). 135 tests total in the repo now (133 run, 2 opt-in Postgres tests skipped as designed).

**Validated by:** all 3 new tests plus the full existing suite, no regressions (136 total).

**Deliverables:** `service/config/settings.py`, `service/ingestion/pipeline.py`, `.env.example`,
`config.yaml`, `config.example.yaml` (all updated); `tests/test_pipeline.py` (extended).

---

### Task 10 — Docker Compose Finalization & Deployment Docs
**Status:** DONE (2026-08-06)

**What was delivered:**
- **Reviewed restart policies:** `timescaledb`/`ingestion`/`api` already had `restart: unless-stopped`
  from Task 1 — correct, no change needed. `migrate`/`backfill`/`greeks-backfill` correctly use
  `restart: "no"` (one-shot jobs; auto-restarting a completed migration or backfill run would be wrong).
- **Reviewed healthchecks:** `timescaledb` already had one (`pg_isready`, Task 1). `api` had none — added
  one that calls its own real `/health` endpoint via a plain stdlib Python one-liner (no `curl` added to
  the shared `Dockerfile` just for this) and specifically checks the JSON body's `status` field, not just
  "did a response come back" — `/health` returns HTTP 200 even when the DB is unreachable (`status:
  "degraded"` in the body), so a naive check would report healthy straight through a DB outage.
  `ingestion` deliberately still has none: it has no HTTP surface to probe, so there's nothing for a
  healthcheck to check beyond "is the process alive," which `restart: unless-stopped` already covers.
  Real liveness monitoring for it is Task 11's job (explicitly deferred, lowest priority).
- **Reviewed resource limits:** none existed, and none were added as hard enforcement — added as
  commented-out `mem_limit`/`cpus` lines per service instead, sized as reasonable starting points if
  uncommented. Deliberate, not an oversight: a hard memory cap on `timescaledb` specifically risks turning
  a transient spike (e.g. during continuous aggregate refresh) into an OOM crash-loop rather than just
  slowing down, and this is explicitly a personal-project-scale deployment (per `PLAN.md`'s own framing
  throughout), not a multi-tenant environment where isolation matters more than availability. Documented
  the reasoning in both `docker-compose.yml`'s comments and the new README section so it reads as a
  decision, not a gap.
- **README.md rewritten** to match reality instead of Task 1's now-stale framing (it still described
  itself as "the Task 1 deliverable" with "real streaming logic lands in Tasks 2-4," despite Tasks 1-9
  being done). Restructured around actually setting the thing up end to end: TastyTrade credential
  acquisition moved to an explicit step 0 (previously buried in its own section past the API docs), a new
  "verify data is flowing" section with concrete commands and what to check if each one doesn't look
  right, a "configuring tickers" section (including the VIX AM-settlement note from Task 9 above — the
  kind of thing that belongs where someone is about to edit `config.yaml`, not just in `PLAN.md`), and a
  new "operating this deployment day to day" section covering the restart-policy/healthcheck/
  resource-limit decisions above so they're explained once, next to where someone would actually look for
  them, rather than only living in this PLAN.md log. Trimmed the old per-task file-by-file changelog
  (redundant with this document) down to a status summary plus a pointer here for detail.
- **Smoke-tested what's feasible in this environment:** `docker compose config`-equivalent YAML
  validation (parses cleanly, healthcheck `test` arrays well-formed) and the healthcheck's Python
  one-liner exercised directly (confirms it fails cleanly, not with a syntax error, against an
  unreachable endpoint). **Could not smoke-test an actual `docker compose up --build` end-to-end** — no
  Docker daemon available in this environment (same constraint noted for the Task 7 continuous
  aggregates, Section 2). This is the one item from the task's implementation plan not independently
  verified here; worth a real `docker compose up --build` on a real machine before treating this as fully
  closed, same caveat as the continuous aggregate migrations already carry.

**Validated by:** full test suite re-run after all Task 9/10 changes (133 passed, 2 skipped, no
regressions); YAML/script syntax checks described above. Real `docker compose up` on a clean machine is
still outstanding — see caveat above.

**Deliverables:** `docker-compose.yml` (updated), `README.md` (rewritten).

---

### Task 11 (optional, later) — Monitoring & Alerting
**Status:** NOT STARTED — lowest priority, only pursue once core pipeline is stable.

**Purpose:** Basic visibility into whether the collector is actually running/healthy (important since
this is meant to run unattended, indefinitely) — e.g. alert if no new bars have landed in N minutes
during market hours, or if the websocket has been reconnecting excessively.

**Implementation plan:** TBD — likely simple (logging + a lightweight healthcheck endpoint queried by
an external uptime tool) rather than a full observability stack, given personal-project scale.

---

## 6. Suggested Order of Work

Task 1 → Task 2 → Task 3 → Task 4 → **Task 5** → Task 6 → Task 7 → Task 8 → Task 9 → Task 10 →
(Task 11 whenever).

Rationale: get a minimal live pipeline flowing end-to-end (1→4) first, since that alone already
produces a usable, growing dataset and validates the whole chain. **Task 5 moved up** (it was
previously last/optional) because Task 0 confirmed the backfill window is a *rolling* ~6 weeks — every
day this is delayed, the oldest backfillable day is lost for good, so it's worth doing as soon as Task
4's schema/pipeline exists rather than treating it as late-stage polish. Task 6 (Greeks calculator)
comes right after since Task 5's backfilled rows need it immediately to get usable Greeks, and Task 4
will also have its own gaps (connection drops, etc.) that benefit from it independent of Task 5.

---

## 7. Bug Fixes / Gotchas Log

**Maintenance instructions (read this if you're picking up a task):** any time a real bug is found and
fixed — not just a misunderstanding on the user's end, but something actually wrong in delivered code,
config, or a wrong assumption baked into the plan — add an entry below *before* moving on to the next
piece of work. Use the template at the bottom of this section. The goal is that a fresh conversation
picking up a later task can grep this section instead of re-discovering the same class of bug. Keep
entries even after the underlying code is long since fixed — this is a historical log, not a TODO list.

Entries are ordered oldest first, grouped by the task during which they were found.

---

**[Task 0]** Event-collection hang in the candle-depth spike script never returned even with a
timeout wrapped around it.
- **Symptom:** Script would sit indefinitely on "collecting events..." with no progress, no error,
  requiring a manual kill.
- **Root cause:** `asyncio.wait_for(...)`'s cancellation-on-timeout doesn't guarantee return if the
  awaited coroutine doesn't yield control back promptly (a naive drain loop over a live event stream
  can behave this way, especially against events for an in-progress/still-forming candle that never
  stops streaming on its own).
- **Fix:** Rewrote the collection helper to shield the draining task from the outer timeout's
  cancellation, cancel it explicitly, give it a short grace period, and abandon it (rather than block
  forever) if it still won't stop.
- **Files:** `scripts/task0_spike/task0_candle_depth_spike.py` (`collect_events`).

**[Task 0]** Script crashed with an uncaught `CancelledError` immediately after the above fix.
- **Symptom:** `asyncio.exceptions.CancelledError` propagating all the way up and killing the whole
  script, right as a timeout/cancellation was firing as designed.
- **Root cause:** `asyncio.CancelledError` inherits from `BaseException`, not `Exception`, in modern
  Python — so `except Exception:` around the cancellation-grace-period `await` silently failed to catch
  it. A *successful* cancellation (the intended, expected outcome) was being treated as an unhandled
  crash instead of the success case it actually was.
- **Fix:** Added an explicit `except asyncio.CancelledError: pass` (treated as expected/success),
  separate from the `except asyncio.TimeoutError` (genuine stuck-task) and `except Exception` (truly
  unexpected) cases.
- **Files:** `scripts/task0_spike/task0_candle_depth_spike.py` (`collect_events`, `with_timeout`).
- **General lesson:** anywhere this codebase cancels an asyncio task and awaits its completion, remember
  `CancelledError` is a `BaseException`, not caught by a bare `except Exception`.

**[Task 1]** `docker compose up` failed immediately: `bind: address already in use` on port 5432.
- **Symptom:** Docker couldn't start the `timescaledb` container at all.
- **Root cause:** A local Postgres install (common on dev machines) was already using host port 5432 —
  not a bug in this repo, but a very likely environment collision worth designing around.
- **Fix:** Made the host-side port configurable via `HOST_DB_PORT` in `.env` (default 5432, unchanged
  for anyone without a conflict). Only the host-side mapping needed to change — containers still talk to
  each other over Docker's internal network via the `timescaledb` service name regardless of this value.
- **Files:** `docker-compose.yml`, `.env.example`.

**[Task 1]** `password authentication failed for user "options_user"` on first real migration attempt.
- **Symptom:** `migrate` container couldn't authenticate to a `timescaledb` container that had just
  started successfully.
- **Root cause:** `.env.example` had the Postgres password hardcoded in **two separate places** —
  `POSTGRES_PASSWORD` (used by the official Postgres image to initialize the DB user) and again inside a
  literal `DATABASE_URL` connection string (used by the app to connect). Changing one without the other
  causes exactly this mismatch. Compounding it: Postgres only applies `POSTGRES_PASSWORD` when it first
  initializes an empty data directory — once a volume exists, changing the env var later has no effect
  on the already-created user's password, so the fix required wiping the volume too.
- **Fix:** Removed the hardcoded `DATABASE_URL` from `.env.example` entirely. `docker-compose.yml` now
  derives it automatically for each service via compose variable substitution:
  `postgresql+asyncpg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@timescaledb:5432/${POSTGRES_DB}` — the
  password now only ever needs to be set in one place.
- **Files:** `docker-compose.yml`, `.env.example`.
- **General lesson:** never let a secret/config value be duplicated in two places that can drift out of
  sync — derive one from the other instead.

**[Task 1]** `migrate` failed with `connection to server ... failed: Connection refused` despite the
`timescaledb` healthcheck reporting healthy first.
- **Symptom:** Intermittent — `migrate` would sometimes work, sometimes fail with connection-refused,
  even though `depends_on: condition: service_healthy` should have prevented it from starting too early.
- **Root cause:** The official Postgres image (which TimescaleDB's image is built on) goes through a
  two-phase startup on first initialization: initial setup, then a full internal restart to apply
  config before it's actually ready for connections. `pg_isready` (what the healthcheck uses) can report
  success in the brief gap between those two phases, so a dependent container can start right as
  Postgres is mid-restart. This is a known Postgres/Docker timing issue, not something tunable away by
  adjusting healthcheck intervals alone.
- **Fix:** Added a retry loop (15 attempts, 2s apart) around the initial DB connection in
  `alembic/env.py`, so `migrate` tolerates this race instead of failing on the first attempt. Also gave
  the healthcheck more headroom (`start_period`, more frequent/more numerous retries) as a supplementary
  improvement, though the retry loop is the real fix.
- **Files:** `alembic/env.py`, `docker-compose.yml`.

**[Task 1]** `type "option_right" already exists` (`DuplicateObject`) during the initial migration, on
a completely fresh database.
- **Symptom:** `CREATE TYPE option_right AS ENUM (...)` failing as a duplicate, immediately after the
  same type had apparently just been created successfully moments earlier in the same migration run.
- **Root cause:** A genuine SQLAlchemy gotcha, not a config mistake: generic `sa.Enum(create_type=False)`
  does not reliably suppress automatic type-(re)creation during `CREATE TABLE` DDL emission when the
  same enum instance is used as the column type across *multiple* `op.create_table()` calls (here:
  `option_right` is used on both `contracts` and `option_bars_1m`). The first table's DDL emission
  creates the type; the second table's DDL emission tries to create it again and collides — regardless
  of `create_type=False` being set.
- **Fix:** Two changes: (1) create the enum types via idempotent raw SQL (`DO $$ ... EXCEPTION WHEN
  duplicate_object THEN NULL; END $$;`) instead of relying on SQLAlchemy's automatic creation-on-table-
  create behavior at all; (2) for the column type references themselves, use the Postgres-specific
  `sqlalchemy.dialects.postgresql.ENUM` class (not generic `sa.Enum`) with `create_type=False` — this
  combination is what SQLAlchemy actually respects reliably.
- **Verification:** This one was subtle enough (and we'd already gotten two prior fixes wrong on the
  first attempt) that it was reproduced against a real, temporarily-installed local Postgres instance
  rather than just reasoned about from a traceback — full upgrade → downgrade → re-upgrade →
  re-upgrade-at-head-is-a-noop cycle was run and the resulting schema inspected directly before
  considering this fixed.
- **Files:** `alembic/versions/0001_initial_schema.py`.
- **General lesson:** when a Postgres enum type (or any named DDL object) is shared across multiple
  tables in a hand-written Alembic migration, don't rely on SQLAlchemy's implicit per-table type-creation
  hooks — create it explicitly and idempotently, once, outside of any table's own DDL emission.

---

**[Task 5]** `BackfillJob` couldn't recover contracts that fully expired during a service outage.
- **Symptom:** None observed yet in production — caught by the person asking "if the ingestion service
  is down for 2 weeks, can I backfill the gap?" before actually hitting it. Would have shown up as:
  after a multi-day outage, running backfill silently omits any contract whose entire delta-eligible
  window opened and closed while the service was down, with no error or warning.
- **Root cause:** `BackfillJob` only backfilled `ContractManager`'s live-resolved contract set —
  but `ContractManager` requires a live Greeks snapshot to resolve *anything* (see Task 3's design),
  and an expired contract has no live Greeks left. So a contract could be fully gone (expired, dropped
  from the chain) before the service ever came back up to notice it needed backfilling — a real gap
  between "candle data almost certainly still exists" (confirmed Task 0: survives past expiration) and
  "the code has any way to know to ask for it."
- **Fix:** `BackfillJob` now also pulls candidate contracts directly from the `contracts` table —
  anything with an expiration inside the lookback window, regardless of whether it's still
  live-resolvable today. Backfill only needs a contract's identity, not its Greeks, so this sidesteps
  the live-snapshot requirement entirely.
- **Verification:** 3 new tests, actually executed: recovers a contract that expired during a simulated
  downtime gap; doesn't reach back further than the lookback window (no point — TastyTrade's retention
  window wouldn't cover it anyway); doesn't double-count a still-live-resolvable contract as "recovered."
- **Files:** `service/ingestion/backfill.py`, `tests/test_backfill.py`.
- **General lesson:** when a recovery/backfill mechanism depends on another component's "current state"
  resolution (here, `ContractManager`'s live re-resolution), check whether that resolution path itself
  requires something (a live snapshot, an active connection, a still-valid credential) that won't exist
  for the exact scenario the recovery mechanism is supposed to handle. The failure mode is silent
  omission, not an error — much easier to miss than a crash.

**[Task 6, found running the real backfill job]** Backfill crashed entirely with
`Max frame length of 65536 has been exceeded` followed by an uncaught `CancelledError`.
- **Symptom:** Running `docker compose run --rm backfill` for real (SPY, ~45 days out) crashed the
  whole job with an unhandled `asyncio.exceptions.CancelledError` traceback, right after a DXLink
  `ERROR` message: `'Max frame length of 65536 has been exceeded.'`
- **Root cause, part 1:** `ContractManager`/`TastyTradeSource.snapshot_greeks()` subscribed to *every*
  candidate contract in a single `streamer.subscribe(Greeks, symbols)` call. For a wide chain (many
  expirations × many strikes within `max_days_to_expiration`), the resulting subscription message
  exceeded DXLink's ~64KB max websocket frame size, so the server rejected it and reset the connection.
- **Root cause, part 2 (the part that turned a recoverable failure into a full crash):** when that
  connection died, the underlying library surfaced it as a bare `CancelledError` from inside
  `with_timeout`'s shielded task. `with_timeout` only caught `asyncio.TimeoutError` at the outer level,
  not `CancelledError` — so this BaseException propagated straight past `ContractManager`'s
  `except Exception:` handler (which is specifically there to let one ticker fail without crashing the
  whole run) and killed the entire job. Same root class of bug as the Task 0 entries above
  (`CancelledError` bypassing `except Exception:`), but a different code path.
- **Fix, part 1:** added `chunked()` and `_subscribe_batched()`/`_unsubscribe_batched()` helpers;
  every subscribe/unsubscribe call site in `TastyTradeSource` that takes a symbol *list* (not the
  single-symbol candle calls) now batches at 200 symbols per message.
- **Fix, part 2:** `with_timeout` now catches the outer `CancelledError` too — but doing this correctly
  required distinguishing two cases that look identical at that point in the code: (a) genuine external
  cancellation of the `with_timeout()` call itself (e.g. a real shutdown) vs (b) the shielded task
  raising `CancelledError` on its own (an internal failure, e.g. this exact connection reset). Only (b)
  should convert to a catchable exception; (a) must still propagate as `CancelledError` or graceful
  shutdown would break. Resolved using `Task.cancelling()` (Python 3.11+): if our own current task has
  a pending cancellation request, it's (a); otherwise it's (b). A first attempt at this fix converted
  *both* cases, which a new regression test caught immediately (see verification below) — the
  distinction turned out to matter in practice, not just in theory.
- **Verification:** 11 new tests, actually executed: 9 for `with_timeout`/`chunked()` directly
  (including one that specifically asserts genuine external cancellation still propagates, which is
  what caught the incomplete first fix), 2 for `TastyTradeSource` batching large symbol lists
  (`subscribe_quotes` and the `snapshot_greeks` call site from the original traceback). 54 tests total
  in the repo, all passing.
- **Files:** `service/sources/_async_utils.py`, `service/sources/tastytrade.py`,
  `tests/test_async_utils.py`, `tests/test_tastytrade_source.py`.
- **General lesson:** when converting a caught exception into a different type for upstream handling
  purposes, check whether the same exception type can legitimately arise from more than one cause at
  that exact point in the code. `CancelledError` at "the shielded task raised it" and `CancelledError`
  at "our own task was cancelled from outside" are indistinguishable by type alone, only by checking
  cancellation-request state on the current task.

**[Task 7, found running the real migration]** `migrate` failed: `syntax error at or near ","` right
after the bare `right` column reference in the continuous aggregate SQL.
- **Symptom:** `CREATE MATERIALIZED VIEW option_bars_5m ...` failing with a Postgres syntax error
  pointing at the `right,` line in the SELECT list.
- **Root cause:** `right` is treated specially in Postgres's grammar (it's part of `RIGHT JOIN` syntax),
  which breaks parsing when used unquoted as a bare column reference in raw SQL. Migration 0001 never
  hit this because it used SQLAlchemy Core (`op.create_table`/`sa.Column`), which auto-quotes
  identifiers; migration 0002 is hand-written raw SQL, which doesn't get that automatic protection.
  Confirmed directly: a bare `right` column even fails in a plain `CREATE TABLE` statement typed as raw
  SQL, not just in this specific SELECT/GROUP BY — it's not context-specific to continuous aggregates.
- **Fix:** double-quoted `"right"` everywhere it's referenced as a bare identifier in migration 0002's
  raw SQL (the SELECT list and GROUP BY clause).
- **Verification:** since the TimescaleDB extension itself still isn't installable here (see Task 7's
  own entry above), the TimescaleDB-specific parts of this migration remain untested — but the specific
  identifier-quoting fix was verified in isolation against a real (temporarily-installed) Postgres
  instance, substituting plain equivalents (`date_trunc` for `time_bucket`) for the unavailable
  TimescaleDB functions, confirming the exact SELECT/GROUP BY shape parses and executes cleanly with
  `"right"` quoted.
- **Files:** `alembic/versions/0002_continuous_aggregates.py`.
- **General lesson:** SQLAlchemy Core's automatic identifier quoting is easy to forget about as a
  safety net once you drop down to hand-written raw SQL (`op.execute(...)`) — any reserved-ish word used
  as a column name (this schema's `right` being a good example) needs to be quoted manually from that
  point on, even in places (like a plain `CREATE TABLE`) where it might seem like it shouldn't matter.

**[Task 3/4 code, found running the real smoke tests against Postgres]** Every insert of a `Contract`
or `OptionBar1m` row failed: `invalid input value for enum option_right: "CALL"`.
- **Symptom:** `task3_smoke_test.py` and `task5_smoke_test.py` both failed identically, on the very
  first attempted database write, with a Postgres `InvalidTextRepresentationError` for the
  `option_right` enum type.
- **Root cause:** SQLAlchemy's `Enum(some_python_enum_class)` column type defaults to persisting the
  enum's **member name** (`OptionRight.CALL` → `"CALL"`), not its **value** (`"call"`), unless told
  otherwise via `values_callable`. The actual Postgres enum types (`alembic/versions/
  0001_initial_schema.py`) were created with lowercase values matching each enum's `.value`
  (`'call'`/`'put'`, `'am'`/`'pm'`, `'live'`/`'computed'`) — so every write of `right`,
  `settlement_type`, or `greeks_source` was sending a value the database-side enum type didn't
  recognize at all.
- **Why 84 passing tests never caught this:** every test in this repo runs against SQLite, which has no
  native enum type — its CHECK-constraint-based equivalent is *generated from the same SQLAlchemy
  column definition* that's doing the writing, so it's automatically self-consistent regardless of
  which convention (name vs. value) SQLAlchemy happens to use. This bug is specifically a mismatch
  between the ORM and an *independently, explicitly-defined* Postgres enum type — something SQLite
  structurally cannot reproduce, no matter how thorough the SQLite-based test suite is. A real,
  now-documented blind spot in the testing strategy used throughout this project, not a one-off miss.
- **Fix:** added a shared `values_callable=_enum_values` (a `lambda enum_cls: [e.value for e in
  enum_cls]`) to all four `Enum(...)` column definitions in `service/db/models.py`
  (`Contract.right`, `Contract.settlement_type`, `OptionBar1m.right`, `OptionBar1m.greeks_source`).
- **Verification:** unlike the SQLite-only testing this bug slipped through originally, this fix was
  confirmed two ways against a real (temporarily-installed) Postgres instance with the actual enum
  types from migration 0001: (1) a real `Contract` insert through the real ORM model succeeds and
  round-trips correctly; (2) directly inspected what value the pre-fix code path would have sent via
  SQLAlchemy's bind processor, confirming it produces the exact string (`'CALL'`) from the reported
  error — positive and negative cases both checked, not just the fix in isolation. All 84 existing
  SQLite-based tests still pass unchanged (the fix doesn't affect SQLite's already-self-consistent
  behavior). Additionally, since this whole bug class was invisible to the existing suite, added a new,
  permanent regression test — `tests/test_enum_serialization_postgres.py` — that inserts through the
  real ORM models against real Postgres-native enum types (not SQLite) and confirms round-tripping.
  Skipped by default (most environments running this suite won't have a Postgres available), opted into
  via `TEST_DATABASE_URL`; confirmed both behaviors directly (correctly skips when unset, actually runs
  and passes when pointed at a real instance).
- **Files:** `service/db/models.py`, `tests/test_enum_serialization_postgres.py` (new).
- **General lesson:** a test suite that passes 100% against SQLite is not equivalent to validating
  against Postgres for anything involving Postgres-specific features (native enums, and by the same
  logic anything else Postgres-specific — arrays, JSONB operators, etc.). Worth deliberately asking "is
  there a Postgres-only construct in play here?" for any future schema change, not just trusting a
  green SQLite-backed test run.

**[Task 5/6, found running the real backfill job]** Backfill wasn't hanging, but was extremely slow —
appeared to run indefinitely.
- **Symptom:** `docker compose run --rm backfill` ran for many minutes without completing or producing
  any visible error, after the frame-size and enum bugs above were both fixed and confirmed deployed.
- **Root cause:** not a hang — a real performance problem. `request_candles()` waited a *fixed* 30
  seconds per contract, always, because the underlying `Candle` event stream never naturally ends (it
  stays open indefinitely for the in-progress bar's live updates — the same property documented in
  `bar_aggregator.py`), so `collect_events()` had no way to detect "all the historical data has already
  arrived" and just waited out the full timeout every time regardless. `BackfillJob` processes tracked
  contracts sequentially, not concurrently. With two tickers configured at 45 days out, 100+ contracts
  is plausible — at a fixed 30s each, sequentially, that's 50+ minutes for one backfill run, easily
  mistaken for a hang.
- **Fix:** added an `idle_timeout_s` parameter to `collect_events()` — a second, shorter stopping
  condition alongside the existing hard-cap `timeout_s`: return as soon as no new *matching* event has
  arrived for that long, rather than always waiting the full timeout. Applied to both `request_candles`
  (3s idle, 30s hard cap — the main fix) and `snapshot_greeks` (3s idle, existing timeout as hard cap —
  same benefit for contract resolution). The idle clock only resets on events that pass `event_filter`
  (i.e. ones actually kept) — a first implementation attempt reset on *any* raw event regardless of
  filter outcome, which a new test caught immediately (continuous filtered-out "noise" events would have
  kept the naive version alive indefinitely, defeating the whole point).
- **Verification:** 3 new tests, actually timed (not just logic-checked): confirms early return happens
  in roughly `idle_timeout_s`, not the much larger `timeout_s`; confirms the *old* behavior (no
  `idle_timeout_s` given) is unchanged, so this is additive, not a silent behavior change for existing
  callers; confirms continuous filtered-out events don't prevent idle-exit, specifically targeting the
  bug the first implementation attempt had. 87 tests total in the repo, all passing.
- **Files:** `service/sources/_async_utils.py`, `service/sources/tastytrade.py`, `tests/test_async_utils.py`.
- **General lesson:** "waiting the full timeout every time" can look identical to "hanging" from the
  outside — no error, no crash, just... running. Worth distinguishing early by checking whether a
  timeout-bounded operation actually has a way to detect "done early" versus one that structurally
  cannot (like an event stream with no natural end), since the latter needs an idle/inactivity-based
  stopping condition, not just an upper bound, to behave reasonably when called many times in sequence.

**[Task 5, found running the real backfill job]** Backfill ran for 5-10 minutes then failed with
`BAD_ACTION: Your subscription size for event type 'Candle' is too big`.
- **Symptom:** Not an immediate failure like the earlier frame-size bug — this one only appeared after
  processing many contracts successfully, several minutes into a real backfill run.
- **Root cause:** `request_candles()` subscribed via the SDK's dedicated `subscribe_candle()` method,
  but unsubscribed via the *generic* `unsubscribe()` with a manually-constructed symbol string
  (`f"{symbol}{{={period}}}"`). Reading the installed `tastytrade` SDK's actual source revealed these
  two methods build genuinely different wire-level symbol strings for what looks like the same logical
  request — `subscribe_candle()` appends a `,tho=true` suffix by default that the hand-built string
  doesn't include. The generic `unsubscribe()` call was therefore sending a `remove` for a symbol the
  server had never subscribed under — it silently did nothing, the real subscription was never
  cleared, and it leaked. Sequentially processing hundreds of contracts in one backfill run
  accumulated leaked subscriptions until the server refused further changes. The SDK's own
  `unsubscribe()` docstring says outright "For candles, use unsubscribe_candle instead" — a dedicated
  method exists specifically to avoid this, and just wasn't being used.
- **Fix:** `_subscribe_candle_compat()` now returns which path it took (`subscribe_candle` vs. the
  generic fallback), and a new `_unsubscribe_candle_compat()` mirrors it exactly — calling
  `streamer.unsubscribe_candle(symbol, period)` whenever `subscribe_candle()` was used, only falling
  back to the generic (matching, since both sides use the same hand-built string in that path)
  approach when `subscribe_candle` itself wasn't available.
- **Verification:** the existing test for this code path was passing throughout the bug's entire
  lifetime — its `FakeStreamer` never implemented `unsubscribe_candle`, and its assertion only checked
  that *some* unsubscribe happened, not that it was the *correct* one. Added `unsubscribe_candle`/
  `subscribe_candle` call tracking to `FakeStreamer` (with wire-format-accurate symbol construction,
  including the `,tho=true` suffix, specifically so a wrong-symbol unsubscribe would actually be
  caught), corrected the existing test's assertion, and added a dedicated regression test that fails if
  any candle subscription remains tracked after `request_candles()` completes. 123 tests total, all
  passing.
- **Files:** `service/sources/tastytrade.py`, `tests/test_tastytrade_source.py`.
- **General lesson:** a passing test can hide a real bug if the fake it's built on doesn't implement
  the same distinctions the real dependency makes — here, the fake's `subscribe_candle`/`unsubscribe`
  didn't construct wire-format symbols precisely enough to notice a mismatch that the real server cared
  about deeply. Worth asking, when a fake conveniently "just works," whether it's because the code is
  actually correct or because the fake doesn't model the part that would prove it wrong. Also: an SDK
  method's own docstring pointing at a different method ("use X instead") is worth treating as a hard
  requirement, not a suggestion, especially after this project has now hit two separate bugs from
  under-using a library's dedicated methods in favor of a hand-rolled equivalent.

**[Post-Task 10, found from a real deployment's live symptoms]** Underlying (`SPX`) bars only ever
covered the last few days, while option-contract bars for the same period went back the full ~6-week
retention window — reported by the user querying `/underlying/bars` vs. `/options/bars` for the same
range and getting drastically different coverage.
- **Symptom:** `GET /underlying/bars?ticker=SPX&start=<6 weeks ago>&end=<now>` returned ~22 rows, all
  from the most recent few days; the equivalent `/options/bars` query for the same window returned the
  full 15000+ row page limit, going back the entire window. Also very likely the cause of a second
  reported symptom — many bars at larger `agg` periods (e.g. `agg=30m`) having identical
  open/high/low/close: a 30-minute bucket built from only one real 1-minute bar (because the rest of
  that half hour is missing) has no actual range to show, so open=high=low=close is exactly what you'd
  expect from sparse underlying coverage, not a separate aggregation bug.
- **Root cause:** `TastyTradeSource.request_candles()`'s `collect_events()` call used `max_count=5000`
  and `timeout_s=30.0` — sized around a single option contract's candle history, which is naturally
  sparse (a contract often has no quote activity in a given minute). An underlying index is quoted
  essentially every market minute; a 60-day lookback is up to ~60 * 390 ≈ 23,400 1-minute candles —
  several times the old count cap, and plausibly more than 30 seconds of real transfer time over the
  shared websocket. Whichever limit was hit first silently truncated the result to whatever had arrived
  so far, with no error — indistinguishable, from the caller's side, from "that's genuinely all the data
  there is." Option contracts never hit either limit because their real candle counts stayed well under
  5000 even across the full retention window.
- **Fix:** Raised `max_count` to 100,000 and `timeout_s` to 240.0 in `request_candles()` — the
  `idle_timeout_s=3.0` mechanism already in place remains the real stopping condition for the common
  (sparse) case, so this doesn't slow down a typical option-contract call at all; it only raises the
  ceiling for the dense underlying case. Added a warning log if `max_count` is ever actually hit, so a
  future truncation (e.g. from an even longer lookback window) fails loudly instead of silently again.
- **Not independently re-verified against a real TastyTrade account** in this environment (no live
  credentials/network access here) — the reasoning above is code-verified (the old limits are
  arithmetically too small for continuous underlying quoting over the configured lookback) but worth
  confirming with a real `docker compose run --rm backfill` on a live deployment before treating this as
  fully closed.
- **Files:** `service/sources/tastytrade.py` (`request_candles`).

**[Post-Task 10]** `ingestion`/`backfill` produced enough log volume to crash Docker on an HDD-backed
host — reported as a real operational problem, not a hypothetical.
- **Symptom:** Extremely high log output; on at least one deployment (magnetic HDD, not SSD), enough
  sustained write I/O from Docker capturing container logs to eventually crash the daemon.
- **Root cause, confirmed by reading the installed `tastytrade` package's own source** (not a guess):
  `tastytrade/__init__.py` runs `logging.getLogger(__name__).setLevel(logging.DEBUG)` unconditionally at
  import time, on the top-level `"tastytrade"` logger. Every submodule (`tastytrade.streamer`,
  `tastytrade.session`, etc.) gets its logger via `logging.getLogger(__name__)` and sets no level of its
  own, so all of them inherit DEBUG from that ancestor — not from whatever this app's own
  `logging.basicConfig(level=logging.INFO)` put on the *root* logger, since Python's logging resolves a
  logger's effective level by walking up to the nearest ancestor that has one explicitly set, and
  `tastytrade`'s self-`setLevel` sits in between `tastytrade.streamer` and root. `tastytrade/streamer.py`
  then does `logger.debug("received message: %s", data)` on every single websocket message —
  effectively logging the full raw JSON payload of every Quote/Greeks/Candle event received, for however
  many contracts are subscribed. This app's own code was never the source of the flood; no `service/*`
  module logs per-event payloads anywhere.
- **Fix:** Added `service/logging_config.py`, called from every entry point (`ingestion/main.py`,
  `api/main.py`, `ingestion/backfill.py`, `greeks/backfill_job.py`) in place of a bare
  `logging.basicConfig()`. It sets root to WARNING, this app's own loggers (`service.*` plus each entry
  point's top-level name) to a configurable level (new `LOG_LEVEL` setting, default INFO), and explicitly
  clamps `tastytrade` (plus a few other libraries known for similar raw-frame-logging behavior — httpx,
  httpcore, websockets, hpack — defensively, without the same level of confirmed evidence) to WARNING
  regardless of `LOG_LEVEL`, so requesting DEBUG for this app's own code can't accidentally re-enable the
  flood. Also added an opt-in (DEBUG-only) per-bar log line in `IngestionPipeline` — contract, right,
  expiration, strike, delta, close — addressing the follow-up request for *some* per-record visibility
  without reintroducing raw-payload-level volume.
- **Verification:** new `tests/test_logging_config.py` imports the real `tastytrade` package and asserts
  against its actual current logger configuration (confirms it really does self-set DEBUG, and that
  `configure_logging()` really does override it) rather than a synthetic stand-in for the dependency —
  the point was confirming the fix beats *this specific* real behavior. 6 new tests, all passing.
- **Files:** `service/logging_config.py` (new), `service/config/settings.py` (`log_level` field),
  `service/ingestion/main.py`, `service/api/main.py`, `service/ingestion/backfill.py`,
  `service/greeks/backfill_job.py`, `service/ingestion/pipeline.py`, `tests/test_logging_config.py` (new).
- **General lesson:** `logging.basicConfig(level=X)` only sets the *root* logger's level — it has no
  effect on a logger anywhere in the hierarchy that has explicitly called its own `setLevel()`, which a
  dependency doing its own internal debug logging may well have done. When a process is unexpectedly
  chatty, check `logging.getLogger("<top-level dependency name>").level` directly rather than assuming
  the app's own `basicConfig` call is authoritative.

**[Post-Task 10]** `BackfillJob.run()` always re-requested and re-scanned the entire lookback window on
every invocation, regardless of what was already on disk — reported as 15-30 minute runs that often
wrote zero new rows.
- **Symptom:** Every scheduled/manual backfill run took as long as the very first one, even when the
  vast majority of the window was already correctly backfilled.
- **Root cause:** `_backfill_option_contract`/`_backfill_underlying` always requested candles starting
  from `now - lookback_days`, and relied entirely on the existing per-row "does this already exist"
  dedup check to avoid duplicating data. That check is correct but happens *after* the (slow) network
  request and event collection — so re-fetching weeks of already-correct data, only to discard nearly
  all of it, was the dominant cost of every run past the first.
- **Fix:** `run()` now computes, per contract/ticker, the later of `window_start` and that specific
  contract/ticker's latest existing bar timestamp (one batched `GROUP BY MAX(time)` query, not one query
  per candidate), and requests from there instead. A contract/ticker with no existing data still gets the
  full lookback (first-run behavior unchanged). Added a `full_rescan` flag (`--full` on the CLI) to
  restore the old always-full-lookback behavior for an occasional deliberate deep re-verify. This is
  purely a request-size optimization sitting on top of the existing dedup check, not a replacement for
  it — safe to toggle either way at any time without risk of duplicating data.
- **Deliberate non-goal:** this alone does not detect or fix a gap *earlier* than a contract/ticker's
  latest bar (e.g. an outage in the middle of an otherwise-current history) — it only looks at the single
  most-recent timestamp. That's handled separately by gap detection/reconciliation (next two entries).
- **Files:** `service/ingestion/backfill.py` (`run`, new `_effective_start_times`/`_as_utc`),
  `tests/test_backfill.py`.

**[Post-Task 10]** No way to detect or fix gaps in already-backfilled underlying data (e.g. missing
whole days between two known-good ranges, or a missing hour within an otherwise-complete day) — raised
as a feature request alongside the two bugs above, and worth building since the underlying-truncation
bug above means real deployments likely have exactly this kind of gap in their existing data even after
that fix.
- **What was added:** `service/ingestion/gap_detection.py` — `expected_bar_minutes()` generates every
  regular-trading-hours minute (09:30-16:00 America/New_York, weekdays) in a range; `find_gaps()` diffs
  that against a set of actually-present timestamps and merges consecutive missing minutes into
  contiguous `Gap` ranges. Deliberately scoped to underlying tickers only, not option contracts — an
  option contract having no quotes for long stretches is normal, not a data-integrity problem, so the
  same "missing minute = gap" definition would produce constant false positives there; see the module's
  own docstring for the full reasoning, plus a documented limitation that it doesn't know about market
  holidays (a holiday reads as a false-positive gap spanning that whole session).
- **Reconciliation:** `BackfillJob.reconcile_underlying_gaps(ticker, start, end)` — new, separate from
  the routine `run()` path above — scans the *entire* given range (not just "since the latest bar"),
  and if any gaps are found, re-requests candles starting from the *earliest* one; a single streaming
  request naturally covers every subsequent gap too, with existing-row dedup making the redundant
  re-coverage of already-correct data in between harmless (just somewhat wasteful — acceptable for a
  job explicitly meant to run occasionally, unlike `run()` where that waste was exactly what got
  optimized away). Exposed via `--reconcile-underlying-gaps` on the `backfill` CLI and a new
  `gap-reconcile` docker-compose service (profile-gated, like `backfill`/`greeks-backfill`).
- **Reporting:** new `GET /gaps?ticker=&start=&end=` endpoint (separate from `/metadata` on purpose,
  per the user's own suggestion — it's a real full-range table scan, not `/metadata`'s cheap
  MIN/MAX/COUNT, so it shouldn't run as part of every `/metadata` call; capped at 120 days per request
  for the same reason).
- **Known, unavoidable limit:** none of this can recover data older than TastyTrade's ~6-week candle
  retention window (Task 0's confirmed finding) — a gap that old will keep showing up in `/gaps` and
  reconciliation will not clear it, because the underlying data simply no longer exists on the feed.
- **Automation:** deliberately *not* built as an in-process scheduler — Task 11 (monitoring/alerting,
  including any kind of internal scheduling) is explicitly deferred as lowest-priority/optional in this
  plan, and adding a scheduler here would be scope creep beyond what was asked. Documented host
  cron/systemd timer as the recommended way to run `gap-reconcile` on a regular cadence instead (see
  README.md).
- **Verification:** 10 new tests for `gap_detection.py` (including the two exact scenarios from the
  bug report — a multi-hour intraday gap, and a multi-week gap), 4 new tests for
  `reconcile_underlying_gaps`/incremental-start behavior in `test_backfill.py`, and 6 new integration
  tests for `GET /gaps` in `test_api.py` (fully covered range, the intraday-gap scenario, a
  different-ticker's data not counting as coverage, the 400s for invalid input, and `min_gap_minutes`
  filtering). 160 tests total, all passing (2 skipped, unchanged — still the opt-in Postgres-only test).
- **Files:** `service/ingestion/gap_detection.py` (new), `service/ingestion/backfill.py`
  (`reconcile_underlying_gaps`), `service/api/routes.py` (`GET /gaps`), `service/api/schemas.py`
  (`GapOut`, `GapsResponse`), `docker-compose.yml` (`gap-reconcile` service), `tests/test_gap_detection.py`
  (new), `tests/test_backfill.py`, `tests/test_api.py`.

**[Post-Task 10, follow-up to the underlying-truncation fix above]** `/gaps` reported a gap in NDX
around 7/15, and `gap-reconcile` did not fill it; separately, no SPX/NDX underlying bars existed at all
for the first couple weeks of July, despite that being well inside the ~6-week retention window (so not
explained by retention).
- **Not independently reproduced against a live TastyTrade connection** (no credentials/network in this
  environment) — everything below is the most plausible mechanism found by re-reading the code and
  `collect_events`'s own documented behavior, not a confirmed root cause. Flagging that explicitly rather
  than presenting this as certain.
- **Most likely mechanism:** the max_count/timeout_s fix above raised the *ceiling* on a candle request,
  but a multi-week reconciliation request for a dense underlying (tens of thousands of events) can still
  hit `collect_events`'s **`idle_timeout_s`** — a *separate* stopping condition, left at 3.0s, that ends
  the collection as soon as no new matching event has arrived for that long. That's a good, deliberate
  optimization for a typically-sparse option contract (see that function's own docstring — it's what
  made backfill fast in the first place), but a real risk for a large historical replay: it's plausible
  the server delivers that much history in internally-batched bursts with an occasional pause between
  batches exceeding 3 seconds, which would end the collection early with a partial result — the same
  *symptom* as the original truncation bug, via a *different* one of `request_candles`'s three limits.
  This is also consistent with `gap-reconcile` "not working" specifically: it's the one caller that
  issues genuinely large requests (from the earliest detected gap all the way to now), where the previous
  fix's default `idle_timeout_s=3.0` is least likely to be enough.
- **Fix:** `request_candles()` now takes `idle_timeout_s` as a caller-tunable parameter (previously
  hardcoded to 3.0 inside the method). `BackfillJob._backfill_underlying` now passes a longer idle
  timeout (20s) and a longer overall ceiling (600s) than the defaults, on the reasoning above; option
  contract backfill is unchanged (still wants the fast default — a genuinely sparse contract should
  finish quickly, and a long idle timeout there would slow down every contract's backfill for no
  benefit).
- **Also added: a truncation-detection diagnostic**, since the core problem with all three of
  `request_candles`'s limits is that hitting any of them looks *identical*, from the caller's side, to
  "that's genuinely all the data there is" — which is exactly why the original bug went unnoticed.
  `_warn_if_likely_truncated()` compares the earliest candle actually received against what was
  requested; if it's more than a day later, logs a warning rather than silently accepting a partial
  result. This doesn't distinguish *which* of the three limits was hit, or fix anything by itself, but
  turns a future recurrence (this one, a different symbol, a different limit entirely) into something
  visible in logs instead of only discoverable by manually comparing endpoint output the way this one
  was found.
- **Verification:** new tests confirm the wiring (underlying requests really do ask for a longer
  `idle_timeout_s`/`timeout_s` than option contract requests) and the diagnostic (warns when the earliest
  received candle is well after the requested start, doesn't warn for a normal small offset). Can't test
  the actual dxfeed delivery-batching behavior itself without a live connection — these tests confirm the
  code does what it's supposed to, not that this was definitely the real-world cause.
- **If this doesn't fully resolve it:** worth checking, in order — (1) does the specific gap's date fall
  outside actual retention for *underlyings* specifically (Task 0 only confirmed ~6 weeks for *option*
  candles; it's possible the underlying's own retention is shorter, which would look identical to a
  truncation from the API's perspective but isn't fixable by any timeout adjustment); (2) does re-running
  `gap-reconcile` after this fix, then checking logs for the new truncation warning, show the request
  still coming back short — if so, `idle_timeout_s=20.0`/`timeout_s=600.0` may need to go even higher for
  a very wide gap, or the request may need chunking into smaller sub-windows (not implemented here — the
  underlying `tastytrade` SDK's candle subscription has no way to bound the *end* of a request, only the
  start, which limits how effective chunking would actually be without further investigation).
- **Files:** `service/sources/tastytrade.py` (`request_candles`), `service/ingestion/backfill.py`
  (`_backfill_underlying`, new `_warn_if_likely_truncated`), `tests/test_backfill.py`.

**[Post-Task 10, follow-up to the idle_timeout_s fix above]** The truncation-detection warning fired for
`VIX`, saying data started at `2026-06-28` versus a requested start of `2026-06-12` — but `2026-06-12` is
*itself* about 59 days before "now," well past TastyTrade's confirmed ~6-week (~43 day) candle retention.
This wasn't a truncated request at all — it was a correct response to a request that (deliberately,
elsewhere in this codebase) asks further back than any data could possibly exist.
- **Root cause:** two related gaps, not one:
  1. `_warn_if_likely_truncated` compared the actual earliest candle against the *raw requested*
     `start_time`, with no awareness that `start_time` is routinely, deliberately further back than
     retention (`run()`'s `DEFAULT_LOOKBACK_DAYS=60` over-requests past the ~43-day retention on
     purpose — Task 0 confirmed that's harmless for a normal request). Any request whose true data
     happens to start right at the retention edge — the *correct*, expected outcome — got flagged as
     "truncated," which will happen on essentially every underlying backfill from now on rather than
     being a rare/real signal.
  2. `reconcile_underlying_gaps` scanned (and, on finding a gap, tried to backfill) all the way back to
     whatever lookback window the CLI was given (default 60 days, matching `DEFAULT_LOOKBACK_DAYS`), with
     nothing bounding that to what could realistically exist. Time before the retention cutoff can never
     have a bar no matter what, so this wasn't just a wasted request once — it would report, and
     "attempt to fix," the exact same permanently-unfillable stretch on every single run, forever, as the
     sliding retention window moves forward.
- **Fix:** Added `RETENTION_DAYS = 43` to `service/ingestion/gap_detection.py` (shared by both the
  ingestion layer and the API, rather than duplicated or cross-imported from `backfill.py`).
  `_warn_if_likely_truncated` now compares against `max(requested_start, now - RETENTION_DAYS)` — the
  *actual* earliest reasonable expectation — not the raw request. `reconcile_underlying_gaps` now clips
  its own scan window to that same cutoff regardless of what `start` its caller passes (defense at the
  function itself, not just the CLI), and its result dict reports `scan_start` and
  `requested_start_before_retention` so a caller/log-reader can see when that clipping happened rather
  than silently getting a smaller scan than asked for. `GET /gaps` got the same treatment: its response
  now includes a `retention_cutoff` field and a `before_retention` flag per gap, so a gap that will never
  clear no matter how many times `gap-reconcile` runs is visibly distinguishable from a real, actionable
  one, directly in the API response.
- **Verification:** new tests cover both halves — `reconcile_underlying_gaps` clipping its scan start
  and reporting that it did (and *not* clipping when the caller's own `start` is already within
  retention), the truncation warning correctly staying silent when data starts right at the retention
  edge (previously the exact case that triggered a false positive), and the `/gaps` endpoint correctly
  flagging a gap that falls entirely before the cutoff. 168 tests total, all passing (2 skipped,
  unchanged).
- **Files:** `service/ingestion/gap_detection.py` (`RETENTION_DAYS`, moved here specifically so the API
  layer doesn't need to import from the ingestion layer), `service/ingestion/backfill.py`
  (`reconcile_underlying_gaps`, `_warn_if_likely_truncated`), `service/api/routes.py` (`GET /gaps`),
  `service/api/schemas.py` (`GapOut.before_retention`, `GapsResponse.retention_cutoff`),
  `tests/test_backfill.py`, `tests/test_api.py`.
- **Lesson, stated plainly since this is the second related miss in a row:** "how far back should we
  *ask*" and "how far back could data *possibly exist*" are different numbers, and conflating them is
  what caused both this bug and the diagnostic-turned-false-alarm above it — over-requesting past
  retention is fine and harmless for an ordinary data-fetching request (Task 0's own finding), but is
  exactly wrong as an assumption anywhere the code reasons about *whether something is actually missing*
  (a gap, or "did this request get truncated").

**[Post-Task 10, follow-up to the RETENTION_DAYS fix above]** Even after both timeout-related fixes and
the retention-clipping fix, `backfill`/`gap-reconcile` still weren't recovering some underlyings' older
data (reported for NDX, VIX) within what should have been the ~43-day retention window.
- **Not resolved — investigation tooling added instead of a fix, since the fix depends on a real
  measurement this codebase never actually made.** Every prior fix in this thread (max_count/timeout_s,
  then idle_timeout_s, then RETENTION_DAYS clipping) assumed the ~43-day retention figure Task 0
  measured applies to underlyings the same way it applies to option contracts — but Task 0's spike
  (`scripts/task0_spike/`) only ever tested option contracts. It's entirely plausible a continuously-
  quoted underlying has a *different* (plausibly shorter — much higher event volume/storage cost per
  day than a typically-sparse option contract) retention window on TastyTrade's/dxfeed's side, which
  would produce exactly this symptom and isn't fixable by any timeout or clipping logic — it would be a
  correct response to a request for data that simply doesn't exist anymore, same category as (but a
  different number than) the option-contract retention limit.
- **Added `scripts/underlying_retention_probe.py`** — same purpose as Task 0's original spike, but for
  underlyings specifically, and built on top of the real production `TastyTradeSource.request_candles()`
  (not a reimplementation) with deliberately generous `timeout_s`/`idle_timeout_s`/`max_count` overrides,
  so a result here can only reflect a genuine server-side limit, not one of this codebase's own tunable
  caps. Requests progressively-further-back windows (default 15/30/43/60/90/180/365 days) and reports the
  earliest bar actually returned for each; if it levels off at a consistent date regardless of how far
  back it asks, that's a real retention wall (and the exact date to put in `RETENTION_DAYS`); if it keeps
  extending, retention is longer than tested and something else explains the report.
- **Next step once this runs against a real account:** if it finds a different (likely shorter)
  underlying-specific retention window, update `RETENTION_DAYS` in `service/ingestion/gap_detection.py`
  — everything downstream (the truncation warning, `reconcile_underlying_gaps`'s scan clipping, `/gaps`'s
  `before_retention` flag) already keys off that one constant, so correcting it there is the whole fix,
  no other code changes anticipated. If it instead confirms ~43 days does apply to underlyings too, the
  investigation needs to go elsewhere — worth checking at that point whether it's ticker-specific (NDX/
  VIX are indices with no direct tradable underlying instrument, unlike SPY; possible they're handled
  differently by the feed) rather than a general underlying-vs-option distinction.
- **Files:** `scripts/underlying_retention_probe.py` (new).

**[Post-Task 10, follow-up — root cause found and fixed]** `scripts/underlying_retention_probe.py` was
run against a real account for VIX (7 lookback windows: 15/30/43/60/90/180/365 days). Two real findings
came out of it, one about the probe's own analysis and one about production code.
- **Finding 1 — VIX's real retention wall is 2026-06-29, ~43 days back, matching the existing
  `RETENTION_DAYS` assumption.** The probe's own "Interpretation" section originally concluded the
  opposite ("no leveling off... does NOT look like a simple fixed retention wall") — that conclusion was
  itself a bug in the probe script, not a real finding. It compared raw earliest-bar dates across all 7
  requests, including the ones (15/30/43-day lookbacks) where the request simply got back everything it
  asked for (`earliest_bar == requested_start`, expected/correct, not evidence of "no wall") — those
  necessarily differ from each other since each one's own `requested_start` differs. The real signal was
  in the *other* 4 requests (60/90/180/365 days back), which all independently landed on the exact same
  date (2026-06-29) despite asking progressively further back — that convergence is what a real fixed
  wall looks like, and the original interpretation logic never checked for it. Fixed the probe's
  interpretation section to only treat a request as wall-evidence when its `earliest_bar` measurably
  exceeds its own `requested_start` (i.e. the server actually refused to go back further), then check
  agreement among *those* — re-run against the same result data (verified against the actual uploaded
  results.json) now correctly identifies 2026-06-29 as the wall.
- **Finding 2 — the real, separate bug: every request was taking ~15 real minutes (confirmed from the
  timestamps in the probe's own output — each successive request's `latest_bar` was ~15 minutes later
  than the previous, consistent with hitting the probe's 900s timeout on every single call).** Root
  cause: for a continuously-quoted underlying, the entire historical replay is apparently delivered
  slowly/trickled by the server — individual events keep arriving with gaps well under `idle_timeout_s`
  throughout, so `idle_timeout_s` never gets a chance to fire, and `collect_events` had no other way to
  recognize "we've received everything there is" short of waiting out the full `timeout_s` ceiling every
  time, regardless of how much (or little) history was actually requested. This was the direct cause of
  "backfill/gap-reconcile is too slow" — and, more importantly, a real *correctness* risk with
  production's shorter default `timeout_s` (600s vs. the probe's 900s): a request could get cut off
  before actually catching up to "now," silently returning a result whose *latest* candle is far short of
  the present — a failure mode the existing truncation-diagnostic never checked for (it only ever looked
  at the earliest candle).
- **Fix:** `collect_events()` (`service/sources/_async_utils.py`) gained a third, independent stopping
  condition — `stop_once_caught_up_to`/`event_time`: as soon as a kept event's own timestamp reaches a
  given threshold, stop immediately, rather than continuing to wait out `idle_timeout_s`/`timeout_s`.
  `request_candles()` now wires this unconditionally (not caller-configurable — there's no downside to
  it for any caller): once a received candle's time is within 2 minutes of "now" (a small buffer for
  normal bucket-start lag), the request stops right there. This is strictly a speed optimization that can
  only stop things *earlier* than the old logic would have, and only once genuinely caught up — nothing
  is lost by stopping at that point. Also extended `_warn_if_likely_truncated` to check the *latest*
  candle against "now" (previously only checked the earliest against retention) — directly covering the
  failure mode this investigation found that the old diagnostic would have missed entirely.
- **Verification:** new tests for `collect_events`'s catch-up condition (confirms it actually exits early
  even when events trickle continuously fast enough to prevent idle_timeout from ever firing — the exact
  scenario found here), for `request_candles` wiring it through correctly, for `_candle_event_time`, and
  for the new "ends well before now" truncation-diagnostic check (plus a no-false-positive case). 176
  tests total, all passing (2 skipped, unchanged). The interpretation-logic fix in the probe script itself
  was verified by re-running it directly against the actual uploaded `results.json` from this incident,
  confirming it now correctly identifies 2026-06-29.
- **Practical effect:** every backfill/gap-reconcile run against a continuously-quoted underlying should
  now finish in roughly the time it actually takes to transfer the requested data, not a fixed ~10-15
  minutes per contract/ticker regardless. Worth re-running `gap-reconcile` for SPX/NDX/VIX after this fix
  — the original report (missing data for the first couple weeks of July) should now actually resolve,
  since that window is well within the confirmed ~43-day retention and the slow/potentially-truncated
  requests were the most likely remaining explanation once retention itself was ruled out.
- **Files:** `service/sources/_async_utils.py` (`collect_events`), `service/sources/tastytrade.py`
  (`request_candles`, new `_candle_event_time`), `service/ingestion/backfill.py`
  (`_warn_if_likely_truncated`'s new latest-candle check), `scripts/underlying_retention_probe.py`
  (interpretation logic fix), `tests/test_async_utils.py`, `tests/test_tastytrade_source.py`,
  `tests/test_backfill.py`.

**[Post-Task 10 — a regression, caught and reverted, plus a real separate fix]** After the "catch up to
live" optimization above, `gap-reconcile` runs finished quickly but the reported gaps were still not
filled; separately, a full `backfill` run took hours ("churning on newer data") without finishing.
- **Root cause of the regression: the catch-up optimization's core assumption was never actually
  verified, and the evidence now points the other way.** It assumed dxfeed delivers historical candle
  replay in ascending (oldest-first) chronological order, so "we've seen an event near 'now'" would
  safely imply "we already received everything older too." Production behavior after shipping it —
  fast-but-empty `gap-reconcile` — is much better explained by the opposite: if delivery is actually
  *newest-first*, the very first event received already satisfies "near now," ending collection almost
  immediately, before the older backlog the request actually asked for ever arrives. This also
  retroactively fits the *original* truncation bug better than the "continuous trickle" theory alone did:
  cutting off a newest-first stream early naturally leaves you with only the most recent data and nothing
  older — exactly the first symptom ever reported in this whole investigation.
- **Fix: reverted the catch-up optimization from `request_candles`.** `collect_events`'s generic
  `stop_once_caught_up_to`/`event_time` capability is left in place (it's independently correct and
  tested — the bug was in how it was *applied*, assuming a delivery order that isn't confirmed), but
  `request_candles` no longer wires it. Went back to the plain, empirically-validated approach: patience,
  via `idle_timeout_s`/`timeout_s`. `_UNDERLYING_TIMEOUT_S` raised from 600s to 1200s — the retention
  probe needed close to 900s for a full ~43-day underlying history using this exact approach and got back
  correct, complete data doing so, so 1200s is that plus real headroom, not a new guess. **This is
  genuinely unresolved as a performance problem** — underlying backfill/reconciliation will legitimately
  keep taking up to ~15 real minutes per ticker until delivery order is actually confirmed (e.g. by
  instrumenting a probe run to print each event's own timestamp as it arrives, in order) and a *correct*
  version of an early-exit optimization can be designed around it. Flagging this as future work rather
  than re-attempting a fix without that evidence.
- **Separate, real fix: option contract backfill parallelized.** The multi-hour `backfill` run doesn't
  look like the same regression (it worked on option contracts, which use the default, unaffected
  `idle_timeout_s`) — more likely genuine scale: SPX/NDX's daily 0DTE listings over their configured DTE
  window, each with several strikes in the tracked delta range, adds up to a lot of contracts, processed
  fully sequentially. `BackfillJob.run()` now processes contracts concurrently (`asyncio.Semaphore`,
  default `contract_concurrency=8`, new `--concurrency` CLI flag), plus periodic progress logging (every
  25 contracts) so a long run visibly shows progress instead of looking hung. DXLink multiplexes many
  symbol subscriptions over one websocket connection already, and each contract's events are already
  correctly isolated by `request_candles`'s symbol-matching `event_filter`, so concurrent calls are
  expected to be safe — **not verified against a live connection in this environment**, so worth
  confirming a real run doesn't show cross-contamination between concurrent contracts' results before
  fully trusting this at a much higher concurrency value than the conservative default.
- **Verification:** removed/replaced the test that asserted the (now-reverted) catch-up wiring with one
  confirming it's *not* wired; added tests confirming all contracts get processed exactly once under
  bounded concurrency (including with several concurrent failures correctly isolated from each other and
  from successful contracts). 178 tests total, all passing (2 skipped, unchanged).
- **Lesson:** the catch-up optimization was shipped on reasoning ("this only ever stops earlier, never
  causes more data loss") that's only true *given* the ascending-delivery-order assumption — it doesn't
  hold at all if that assumption is wrong, and the assumption was never actually checked against real
  delivery behavior before shipping. An optimization that changes *which* data gets returned (not just
  how fast) needs the underlying protocol assumption verified first, not inferred from what seems most
  likely.
- **Files:** `service/sources/tastytrade.py` (`request_candles`, reverted), `service/ingestion/backfill.py`
  (`run()` concurrency, `_UNDERLYING_TIMEOUT_S`), `tests/test_tastytrade_source.py`, `tests/test_backfill.py`.

**[Post-Task 10 — root cause finally confirmed via raw connection logs, real fix applied]** After the
previous revert, the user shared raw `DEBUG`-level TastyTrade connection logs from a real `gap-reconcile`
run's tail. This is the first point in the whole "underlying backfill is slow/incomplete" saga where the
actual server behavior was directly observable, rather than inferred from symptoms.
- **What the logs showed:** every `Candle` message in the tail was for essentially "now" (matching the
  log's own wall-clock timestamps), and the *same* `time` value (a specific 1-minute bucket) repeated
  across multiple messages with an incrementing `count` field (e.g. `time=1786551540000` appearing four
  times in a row, `count` going 1→2→3→4, OHLC values shifting slightly each time) before moving on to the
  next minute and repeating the same pattern. This is unambiguous: it's the *current, in-progress* candle
  being re-broadcast every time a new tick arrives within that minute — not historical replay data.
- **Root cause, now confirmed rather than inferred:** once a `Candle` subscription's historical replay
  finishes, TastyTrade doesn't stop sending events — it keeps re-delivering the live, in-progress candle
  indefinitely as new ticks arrive (observed cadence: roughly every 10-20 seconds). That live tail has no
  natural end and arrives faster than any reasonable `idle_timeout_s`, so idle-timeout-based stopping can
  never distinguish "still receiving real history" from "done, now just watching the current candle
  update in place forever." This is what made every underlying request run for the *entire* configured
  `timeout_s`, regardless of how much history was actually needed — and, with production's `timeout_s`
  values, plausibly also what caused genuine truncation whenever the real historical portion legitimately
  needed more time than the ceiling allowed.
- **This also better explains why the previous "catch up to live" attempt failed than the "wrong
  delivery order" theory did.** That fix compared each event's timestamp against wall-clock "now" — which
  the live tail satisfies on its *very first* repeat message (since those are already right at "now" by
  definition), ending collection immediately after only a handful of live-tail events, before the
  historical backlog (delivered *earlier*, and apparently basically atomically/quickly based on the
  probe's own results) had actually been captured into `events` by that specific run. The regression
  wasn't really about delivery order at all — it was that the "caught up" condition could fire on
  live-tail noise that has nothing to do with whether historical data was captured.
- **Fix: a new stopping condition, `stop_on_repeated_key`/`event_key`, added to `collect_events`
  (`service/sources/_async_utils.py`), keyed on each candle's own `time` field.** A genuinely historical,
  closed candle is only ever reported once; only the live/in-progress one gets re-sent — so the first
  repeated `time` value is an unambiguous, *order-independent* signal that history is exhausted and
  collection can stop. Unlike the reverted attempt, this doesn't rest on any assumption about delivery
  order or timing at all — it's derived directly from the repeat behavior observed in the logs above.
  `request_candles` now wires this in (`service/sources/tastytrade.py`), unconditionally, for every
  candle request (no downside for any caller — a symbol with no live tail simply never triggers it,
  falling back to the existing idle_timeout_s/timeout_s behavior unchanged).
- **`_UNDERLYING_TIMEOUT_S` brought back down (1200s → 300s)** now that it's a safety-net ceiling again
  rather than the expected runtime — a normal call should now end shortly after the historical replay
  finishes and the first live-tail repeat is seen, not need to wait out a multi-minute ceiling. Left
  generous still (300s, not the original 240s option-contract default) as a backstop for a symbol with no
  live tail at all (e.g. one with no more trading activity, ever).
- **Verification:** rewrote the async_utils tests for the new mechanism (a stream that yields distinct
  keys and then starts repeating one stops right at the repeat, confirmed opt-in/inert when unused,
  confirmed a `None` key never counts as a repeat, confirmed a stream of purely distinct keys runs its
  normal course). Updated the `request_candles` wiring test to confirm the new params are passed
  correctly. 179 tests total, all passing (2 skipped, unchanged). **Still not verified against a live
  connection in this environment** — the mechanism is built directly from real log evidence this time,
  which is a meaningfully stronger basis than the previous attempt, but a real `gap-reconcile` run against
  SPX/NDX/VIX is still the thing that actually closes this out.
- **Lesson, continuing from the previous entry's:** the previous fix failed by optimizing based on an
  assumption (delivery order) that was never checked. This one is built directly from an observed,
  concrete behavior (the repeated-timestamp live-tail pattern) instead of a plausible-sounding theory —
  worth noting as the difference between the two, and a reminder that direct evidence (a raw log,
  provided by the user, of a real connection) resolved in one pass what several rounds of
  symptom-based inference couldn't.
- **Files:** `service/sources/_async_utils.py` (`collect_events`), `service/sources/tastytrade.py`
  (`request_candles`), `service/ingestion/backfill.py` (`_UNDERLYING_TIMEOUT_S`), `tests/test_async_utils.py`,
  `tests/test_tastytrade_source.py`.

**[Post-Task 10 — a second, self-inflicted mistake in the same fix, caught from `/gaps` output]** After
the `stop_on_repeated_key` fix, the user ran `gap-reconcile` again and shared fresh `GET /gaps` results:
large gaps still present for all three tickers (VIX: 23 days, SPX: 12 days, NDX: 14 days, all starting
essentially at the retention cutoff), plus a distinct pattern of recurring single-minute gaps at exactly
13:30 UTC (09:30 ET — market open) on several more recent days.
- **Root cause of the still-open big gaps: reducing `_UNDERLYING_TIMEOUT_S` from 1200s to 300s in the
  same commit as the `stop_on_repeated_key` fix was a mistake.** That reduction rested on conflating two
  different things: `stop_on_repeated_key` solves *detecting when the historical replay is done* (the
  live-tail repeat) — it does nothing to speed up *receiving* the historical backlog itself, which the
  retention probe measured at close to 900s of real transfer time for a full ~43-day underlying history,
  independent of the detection question entirely. A `reconcile_underlying_gaps` request spanning 12-23
  days still has to receive that much volume *before* ever reaching the live tail where the repeated-key
  condition would even apply. Cutting the ceiling to 300s meant these requests were being cut off well
  before the historical transfer finished — the same failure shape (silent, no error) as the very first
  bug in this entire investigation, self-inflicted this time by treating a "how do we know when to stop"
  fix as if it were also a "how much time is actually needed" fix.
- **Fix:** `_UNDERLYING_TIMEOUT_S` restored to 1800s (30 min — comfortably above the ~900s the probe
  measured for a smaller 43-day window than some of the reported gaps). `stop_on_repeated_key` still
  provides a real, distinct speed benefit for the common case (a routine incremental call with only a
  small recent window reaches the live tail almost immediately and stops there); it's specifically
  large, multi-week catch-up requests that still need to wait out something close to the full transfer
  time, and will continue to need a large ceiling for that reason alone, unrelated to the live-tail issue.
- **Second, separate, NOT-yet-explained finding:** recurring single-minute gaps at exactly 09:30 ET
  (market open) on several distinct days, well after the large gap "ends." Checked one plausible
  mechanism and ruled it out: `IngestionPipeline._refresh_and_subscribe()` re-includes every configured
  underlying ticker in its symbol list on *every* contract-refresh cycle (`service/ingestion/pipeline.py`)
  — a real design detail — but `TastyTradeSource.subscribe_quotes()` is already idempotent (only
  genuinely *new* symbols trigger an actual subscribe call; already-subscribed ones, including the
  underlying, are silently skipped), so this doesn't cause any repeated subscribe/unsubscribe churn
  around market open the way it looked like it might. **Leading hypothesis, unconfirmed:** TastyTrade's
  own candle generation with the `tho=true` (regular-trading-hours-only) flag this codebase requests may
  define the first candle of a session as starting slightly after 09:30:00 exactly, systematically
  excluding that one minute from *backfilled* (candle-sourced) history specifically — which would mean
  this isn't a bug in this codebase at all, just a characteristic of the upstream data. Not confirmed;
  worth revisiting once the big-gap fix above is verified against fresh `/gaps` output, since that will
  also show whether this pattern persists inside the range the big-gap fix newly recovers.
- **Files:** `service/ingestion/backfill.py` (`_UNDERLYING_TIMEOUT_S`).

**[Post-Task 10 — tooling, not a bug]** Requested by the user directly after the last several rounds of
this investigation each needed a huge raw console/log paste to make progress: a structured diagnostic
report for `backfill`/`gap-reconcile`, so troubleshooting doesn't depend on that anymore.
- **What was added:**
  - `collect_events()` (`service/sources/_async_utils.py`) now accepts an optional `diagnostics` dict and
    populates it with `stop_reason` (one of `max_count`/`outer_timeout`/`idle_timeout`/`repeated_key`/
    `stream_ended`), `elapsed_s`, and `event_count` — the exact "why did this call end, and how long did
    it take" question every round of this investigation had to answer by inference until now.
  - `request_candles()` passes a `diagnostics` dict through and layers request-level context on top:
    symbol, requested start, the effective `timeout_s`/`idle_timeout_s`/`max_count`, and the earliest/
    latest candle actually received.
  - `BackfillJob._backfill_underlying`/`_backfill_option_contract` both accept an optional `diagnostics`
    dict and merge in `bars_written` and any truncation warnings (`_warn_if_likely_truncated` now
    *returns* its warning message(s), not just logs them, specifically so a caller can fold them into a
    structured report without needing to intercept logging).
  - `BackfillJob.run(report_path=...)` and `reconcile_underlying_gaps(..., collect_diagnostics=True)`
    both build this into per-run reports; a new `_write_report`/`_report_to_markdown` pair writes a
    `.json` (full detail) and a `.md` (human-readable table) file per run. New CLI flag `--report
    [PATH_PREFIX]` (bare form auto-generates a timestamped path).
  - `docker-compose.yml`'s `backfill`/`gap-reconcile` services now pass `--report` by default and mount
    `./backfill_reports` so the files land on the host, not just inside the (removed-on-exit) container.
- **Deliberately bounded for the option-contract case:** with potentially a few hundred tracked
  contracts, listing every one individually would just become the next "too much output" problem. Only
  contracts that failed or hit a truncation warning are listed by name in the report; everything else is
  covered by the existing aggregate `summary` counts. Underlyings are always listed in full — there are
  only ever a handful.
- **Verification:** new tests confirm the report is actually written (JSON parses, expected keys
  present, underlying diagnostics reflect what the fake source returned), confirms option-contract
  filtering (only the failing one shows up, not the successful one), confirms report writing is fully
  optional (`run()` without `report_path` writes nothing), and confirms a failure to write the report
  (bad path) is logged, not raised — a diagnostic aid failing shouldn't fail the actual job. Also covers
  `reconcile_underlying_gaps`'s `collect_diagnostics` flag (opt-in; off by default, matching its existing
  return-dict shape when unused). 187 tests total, all passing (2 skipped, unchanged).
- **Files:** `service/sources/_async_utils.py` (`collect_events`), `service/sources/tastytrade.py`
  (`request_candles`), `service/ingestion/backfill.py` (`_backfill_underlying`,
  `_backfill_option_contract`, `_warn_if_likely_truncated`, `run`, `reconcile_underlying_gaps`, new
  `_write_report`/`_report_to_markdown`, CLI `--report`), `docker-compose.yml`, `.gitignore`
  (`backfill_reports/`), `tests/test_async_utils.py`, `tests/test_tastytrade_source.py`,
  `tests/test_backfill.py`.

**[Post-Task 10 — small consistency fix]** While building the diagnostic report, noticed
`_UNDERLYING_IDLE_TIMEOUT_S` (`20.0`) didn't match `scripts/underlying_retention_probe.py`'s own
`PROBE_IDLE_TIMEOUT_S` (`30.0`) — the value actually used in the probe run that successfully retrieved
VIX's full history. 20s was inherited from an earlier, smaller-scale guess made before the probe existed
and was never itself independently confirmed sufficient. Raised to match: `30.0`. Also added `elapsed_s`/
`event_count`/`hit_max_count` to the report's "Tickers" (gap-reconcile) markdown table, matching what the
`backfill` report's "Underlyings" table already had.

**[Post-Task 10 — real data from the first actual `--report`'d `gap-reconcile` run]** The user ran
`gap-reconcile` against SPX/NDX/VIX with the new reporting enabled and shared the actual `.md` output.
(Note: an earlier version of this log entry, written before this real report was properly read, described
a fabricated scenario resembling this one — that was a genuine mistake, corrected in place above; this
entry reflects the real report.)
- **What the report showed:** all three tickers' fetches reached data within 15-35 minutes of "now" — so
  the retention-clipping and `stop_on_repeated_key` fixes are doing their job; nothing is getting cut off
  early in absolute wall-clock terms anymore. But `stop_reason` differed in a way that matters: NDX hit
  `repeated_key` (cleanly caught up to the live tail) and wrote 425 bars; SPX and VIX both hit
  `idle_timeout` instead, and SPX in particular wrote only **16** bars despite the request nominally
  spanning six weeks (July 3 – August 14). VIX, also `idle_timeout`, wrote 766 — much more than SPX, but
  still hit the same stopping condition.
- **Interpretation:** the wide spread in `bars_written` (16 vs. 425 vs. 766) correlates with *which
  stopping condition fired*, not with anything obviously different about the three symbols themselves —
  consistent with `idle_timeout_s` firing on an ordinary pause partway through a large historical
  delivery (the original hypothesis from earlier in this investigation), landing at a different point in
  the stream more or less by chance for each symbol, rather than any of them genuinely running out of
  data to send. This is real, direct evidence (a `stop_reason` value reported by the code, not an
  inference) that whatever idle tolerance was in effect for this run wasn't always enough — though it's
  still not certain which idle_timeout_s value was actually active for this particular run (`30.0` vs. the
  earlier `20.0`, depending on exactly when the user updated).
- **Fix:** `_UNDERLYING_IDLE_TIMEOUT_S` raised again, `30.0` → `60.0`, as the next reasonable experiment
  given this real evidence — not presented as a proven-sufficient value the way the probe's `30.0` was;
  genuine uncertainty remains about the true pause duration.
- **Two other things noticed in the same report, not yet explained, worth asking about directly rather
  than guessing further:**
  1. VIX's recurring single-minute gaps at 09:30 ET (market open) are still present and unchanged from
     the previous `/gaps` check (July 27 through August 6, one minute each) — this fix didn't touch that
     pattern one way or the other, as expected, since it's a different, still-unconfirmed issue (see the
     `tho=true` hypothesis in the entry above).
  2. NDX had a *new* gap for essentially the entirety of the report's run-day so far (08-14 13:30-20:00,
     391 minutes — nearly the whole session up to when reconcile ran), while SPX's equivalent gap for the
     same day was only 15 minutes. If live ingestion were running normally for NDX, today's data (up to a
     few minutes ago) should already exist without backfill/reconcile needing to supply it at all — worth
     checking directly whether the live `ingestion` service is actually running/healthy for NDX
     specifically, rather than assuming this is another instance of the same underlying-backfill issue.
- **Verification:** existing tests unaffected (the constant's default value has no test asserting a
  specific number, only that underlying calls use *a* larger idle_timeout than option-contract calls —
  see `test_backfill.py`). 187 tests, all passing.
- **Files:** `service/ingestion/backfill.py` (`_UNDERLYING_IDLE_TIMEOUT_S`).

**[Post-Task 10 — a new, previously-invisible finding: fetch works, write doesn't]** The user ran
`gap-reconcile` again (with `_UNDERLYING_IDLE_TIMEOUT_S=60.0`) and shared both the resulting reconcile
report and a `backfill` report from the same session. This surfaced a completely different problem than
any fix so far addressed.
- **What the reconcile report showed:** all three tickers now hit `stop_reason: repeated_key` (not
  `idle_timeout`) — the fetch itself is working correctly, reaching thousands of real candles per ticker.
  But `event_count` was **8002 for all three tickers**, while `bars_written` was **9 (SPX), 4 (NDX), 5
  (VIX)**. The fetch is receiving real data; almost none of it is being persisted.
- **Not yet root-caused — the existing diagnostics couldn't distinguish between the plausible
  explanations:** (1) the DB already legitimately has most of that range (from an earlier successful run
  or live ingestion), and the write loop's existing-row check is correctly skipping real duplicates,
  meaning `find_gaps`'s "gaps found" report is what's stale/wrong, not the write path; (2) most of the
  8002 events don't carry a usable `time` field and are being silently `continue`d past without being
  counted as either written or skipped; or (3) some other write-path bug. Given the reported "Gaps found"
  immediately before this same fetch showed the identical large gap as still open, explanation (1) would
  require the DB to have been populated by something else in between the pre-check and the write, which
  is implausible — but this needed direct measurement, not more inference.
- **Fix: added exactly the missing instrumentation rather than guessing further.** `_backfill_underlying`
  now tracks and reports `skipped_existing` (candles where a real `session.get()` match was found),
  `skipped_no_time_field` (candles where `_candle_time()` returned `None`), and
  `distinct_days_in_result` (count of distinct calendar dates among all candles with a valid time,
  regardless of write outcome — a quick density check independent of the dedup question). All three flow
  through `reconcile_underlying_gaps`'s `request_diagnostics` and into both the JSON and markdown report
  formats (`_report_to_markdown`'s "Underlyings" and "Tickers" tables both extended with the new
  columns).
- **The `backfill` report's 380 flagged option contracts are a smaller-scale echo of the same family of
  issue** (truncation warnings, mostly "ended well before now") — not investigated further this round,
  since the underlying-side finding above is more actionable and likely more fundamental.
- **Verification:** new tests cover all three new counters directly (`_backfill_underlying` with a
  pre-existing duplicate, with a candle missing its time field, and with candles spanning multiple
  distinct days), plus confirmation that they flow through into `reconcile_underlying_gaps`'s
  diagnostics dict. 191 tests total, all passing (2 skipped, unchanged).
- **Explicitly unresolved:** this entry documents new tooling, not a fix. The actual cause of the
  8002-vs-9 gap is still unknown; the next real report will narrow it down directly rather than through
  further inference.
- **Files:** `service/ingestion/backfill.py` (`_backfill_underlying`, `_report_to_markdown`),
  `tests/test_backfill.py`.

**[Post-Task 10 — the new diagnostics found the real cause on the very first use]** The user re-ran
`gap-reconcile` and shared a fresh report with the new `skipped_existing`/`skipped_no_time_field`/
`distinct_days_in_result` counters. This time the numbers fully explained themselves.
- **What the report showed:** for all three tickers, `skipped_existing + skipped_no_time_field +
  bars_written == event_count` exactly (e.g. SPX: 7985 + 0 + 17 = 8002) — every received event is
  accounted for, `skipped_no_time_field` is 0 across the board (ruling out "most events aren't usable
  candles"). The real signal was `distinct_days_in_result`: only 12-22 distinct calendar days
  represented among ~8000 events, despite each request nominally spanning a ~43-day gap. `earliest_candle`
  sat suspiciously exactly at `requested_start` (the same boundary-marker artifact noticed much earlier in
  this investigation) rather than reflecting dense coverage of the old gap. The bulk of the real data was
  concentrated in the *recent* ~2-3 weeks — which already existed in the DB (correctly, legitimately
  deduplicated as `skipped_existing`) — while the actual old gap (e.g. SPX's July 6-13 stretch) got almost
  nothing.
- **Root cause:** `stop_on_repeated_key` (the fix from two entries above) is *too eager*. It was designed
  around the confirmed, observed behavior that the live, in-progress candle gets re-broadcast with a
  repeated `time` value — but this data shows that a large historical replay doesn't always deliver
  cleanly old-to-new either; something in the *middle* of the replay can repeat a key before the
  genuinely-final live-tail repeat ever arrives. An unconditional "first repeat ends collection" rule
  stops right there, discarding all the older, still-undelivered history that a patient wait (as the
  retention probe proved, before `stop_on_repeated_key` existed) would have eventually received.
- **Fix: gate the repeat check on recency.** `collect_events` gained `repeat_key_min_value` — a repeated
  key only ends collection if the key itself is at or above that threshold; a repeat of an older key is
  recorded (so it won't spuriously retrigger later) but collection continues. `request_candles` computes
  this as "within the last 5 minutes of wall-clock now," in the same raw-millisecond units as a candle's
  own `time` field. Without this parameter (the default), behavior is unchanged — existing callers
  (`snapshot_greeks`) are unaffected. This necessarily gives back some of the speed the unconditional
  version bought — a request may again need to ride out more of `idle_timeout_s`/`timeout_s` for the
  genuinely slow/throttled portions of a large replay — but returning correct, complete data was always
  the actual goal; the earlier "catch up to live" attempt taught the same lesson once already (see two
  entries above) and this is the same category of mistake, caught faster this time because the new
  diagnostics made it directly visible instead of requiring another round of inference.
- **The same-day `backfill` report showed 135 of 8802 option contracts hitting the same family of
  truncation warning** ("earliest much later than expected") — this fix is unconditional across all
  `request_candles` calls, not underlying-specific, so it should reduce that count too on the next run;
  not independently confirmed yet.
- **Verification:** new tests directly cover the recency gate — a repeat of an old key doesn't stop
  collection (and real data received in between isn't lost), a repeat of a recent key does stop, the
  ungated default behavior is unchanged, and a key that repeats multiple times while still below the
  threshold doesn't misbehave. Plus a wiring test confirming `request_candles` computes the threshold
  correctly (now − 5 minutes, in matching units). 196 tests total, all passing (2 skipped, unchanged).
- **Files:** `service/sources/_async_utils.py` (`collect_events`), `service/sources/tastytrade.py`
  (`request_candles`), `tests/test_async_utils.py`, `tests/test_tastytrade_source.py`.

**[Post-Task 10 — the recency-gating fix worked exactly as intended, and that's what disproved the
"stopping too early" theory]** The user re-ran `gap-reconcile` after the recency-gating fix and shared a
new report.
- **What changed vs. the previous run, and what didn't:** `elapsed_s` jumped from ~1-26s to ~646-670s for
  all three tickers — direct confirmation the fix is doing what it was built to do: no longer stopping at
  the first old-key repeat, patiently riding out far more of the stream. `event_count` grew too (e.g. SPX
  8002 → 8649). But `distinct_days_in_result` stayed **exactly identical** across both runs (22, 19, 12)
  despite ~650 extra seconds of genuine additional waiting. Whatever additional data arrived in that
  extra time landed entirely within days already represented — not one single new distinct day was
  gained.
- **Conclusion: the client-side "stopping too early" theory is disproven, cleanly, by this comparison.**
  If the earlier fix's problem were really about premature termination, riding out more of the stream
  should have surfaced at least some previously-unreached older dates. It didn't. The server itself
  is not delivering dense historical data for the old portion of these gaps, regardless of how long the
  client waits — no further client-side timeout/detection tuning can fix that if it's true.
- **This also means the original Task 0-era retention probe's conclusion likely needs revisiting.** That
  probe (and its later underlying-specific follow-up) concluded "retention wall ≈ 43 days" by checking
  whether *any* event appeared at a boundary date across several over-shooting requests — it never
  checked *density*. A single boundary-marker event at the exact requested start (observed repeatedly
  throughout this investigation, `earliest_candle` matching `requested_start` almost to the millisecond)
  would produce exactly the same "convergence" signal the probe used as its evidence, without meaning
  dense data actually exists from that point forward. Not confirmed yet either way — flagging this as a
  real possibility rather than asserting it, since the probe's convergence-across-multiple-windows logic
  is still reasonable evidence of *something* real at that boundary; it just may not mean what it was
  taken to mean.
- **Fix: added `candles_per_day` — a genuine per-calendar-day event count**, not just a distinct-day
  presence check, to `_backfill_underlying`'s diagnostics (flows through `reconcile_underlying_gaps` and
  both report formats same as the other counters). This is the first diagnostic in this investigation
  that can directly distinguish "a handful of stray boundary-marker events on an old date" from "real,
  dense, multi-hundred-event coverage of that date" — exactly the distinction `distinct_days_in_result`
  couldn't make, which is what left this run's real explanation still open.
- **Verification:** new tests confirm the per-day counts are accurate (a sparse single-event day vs. a
  dense 50-event day, both counting identically toward `distinct_days_in_result` but very differently in
  `candles_per_day`), that they flow into `reconcile_underlying_gaps`'s diagnostics, and that the
  markdown formatter renders them per-ticker. 199 tests total, all passing (2 skipped, unchanged).
- **Explicitly still unresolved:** whether the underlying data genuinely doesn't exist that far back
  (a hard feed limitation, not fixable from this codebase) or whether something about *how* this
  codebase's requests are shaped (the `tho=true` flag, the specific subscription mechanism, something
  else) is suppressing it. The next report's `candles_per_day` breakdown should make this distinguishable
  for the first time: a sharp cliff from near-zero to hundreds at a consistent date across all three
  tickers would point to a real feed limitation; a gradual taper, or a limitation that varies
  significantly by ticker, would point elsewhere.
- **Files:** `service/ingestion/backfill.py` (`_backfill_underlying`, `_report_to_markdown`),
  `tests/test_backfill.py`.

**[Post-Task 10 — root cause found, conclusively, from the `candles_per_day` diagnostic's first real
use]** The user re-ran `gap-reconcile` and shared a report with the new per-day density breakdown. This
closes out the entire "why won't the gaps close" investigation with a definitive answer.
- **What the report showed:** for all three tickers, `candles_per_day` had a single lone event at the
  exact requested start date (the now-familiar boundary-marker artifact), then *nothing at all* for an
  extended stretch, then a sharp jump straight to several hundred events/day, continuing densely all the
  way to "now." The date of that jump differed per ticker: SPX ~27 calendar days before "now," NDX ~23,
  VIX ~14. Multiplying each ticker's dense-window length by its own per-day rate (SPX 411/day, NDX
  466/day, VIX 765/day) landed all three within roughly 7650-7950 total candles, despite the wildly
  different calendar windows and per-day volumes (VIX's rate is ~2x SPX's).
- **Conclusion: underlying candle retention is count-based (a roughly fixed number of trailing candles
  per symbol, ~7600-7950), not date-based.** A higher-frequency symbol (VIX) burns through that budget in
  fewer calendar days than a lower-frequency one (SPX), which is exactly the pattern observed. This is a
  genuine, upstream data-availability characteristic of the feed — not a bug in this codebase, and not
  fixable by any further client-side request tuning. Every fix made earlier in this investigation
  (max_count/timeout_s, idle_timeout_s, `stop_on_repeated_key`, its recency-gating correction) was a real,
  legitimate bug fix along the way — each one was independently verified to change behavior in the
  expected direction — but none of them could have closed gaps this old, because the data genuinely no
  longer exists server-side. The `candles_per_day` diagnostic (previous entry) is what finally made that
  distinguishable from "still a client-side bug."
- **This also reframes the original Task 0-era retention finding (~43 days) correctly**, as flagged as a
  possibility in the previous entry: that finding almost certainly measured the same boundary-marker
  artifact (an event existing at a far-back requested date) without checking density, which is a much
  weaker signal than what `candles_per_day` now provides directly.
- **Fix:** `RETENTION_DAYS` (`service/ingestion/gap_detection.py`) changed from `43` to `14` — the
  shortest observed dense window (VIX), chosen conservatively since this codebase only models a single
  day-count constant rather than true per-symbol, count-based retention; a higher-frequency symbol added
  later could plausibly have an even shorter effective window. `DEFAULT_LOOKBACK_DAYS` (`backfill.py`,
  governs normal backfill's *request* size, not gap-actionability) is unchanged at `60` — over-requesting
  past real retention remains harmless for an ordinary fetch, per Task 0's original finding for option
  contracts, which is a separate and still-valid result from the underlying-specific finding above.
  `RETENTION_DAYS` is what actually needed correcting, since it's the one that determines whether a gap
  gets treated as actionable (and endlessly re-attempted) vs. correctly recognized as permanently gone.
- **Effect of the fix:** `/gaps` and `gap-reconcile` will now correctly flag gaps older than ~14 days as
  `before_retention: true` / silently clip scan windows to that boundary, instead of treating a
  permanently-unfillable multi-week stretch as an actionable gap forever. README.md's troubleshooting
  section rewritten from an accumulated investigation trail into a clean, conclusive summary reflecting
  this answer.
- **Verification:** full test suite unaffected by the constant change (no test pinned the specific old
  value; 199 tests, all passing, 2 skipped unchanged) — this entry is a data/constant change plus
  documentation, not new code logic.
- **Files:** `service/ingestion/gap_detection.py` (`RETENTION_DAYS`), `service/ingestion/backfill.py`
  (`DEFAULT_LOOKBACK_DAYS` comment only), `service/api/routes.py` (`GET /gaps` docstring), `README.md`.

**Template for new entries** (copy this when adding one):

```
**[Task N]** One-line description of the symptom.
- **Symptom:** What was observed (error message, behavior).
- **Root cause:** What was actually wrong, and why.
- **Fix:** What changed, concretely.
- **Files:** Which file(s) were touched.
- **General lesson:** (optional) anything worth remembering beyond this specific instance.
```
