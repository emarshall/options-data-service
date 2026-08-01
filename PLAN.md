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
- Exact list of initial tickers and per-ticker delta ranges — can be decided at config time (Task 1/9),
  doesn't block earlier tasks.
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
**Status:** NOT STARTED

**Purpose:** Tie together the scheduling details left open in Task 3 — specifically same-day 0DTE
listing detection, and the AM-settlement exclusion logic — plus finalize the real config file with
actual tickers/delta ranges the user wants to track.

**How it fits in:** Operational polish on top of Task 3/4.

**Implementation plan:**
- Scheduler (e.g. APScheduler or a simple asyncio loop) running the contract refresh job from Task 3 on
  an appropriate cadence, informed by real observation of when TastyTrade lists same-day 0DTE contracts.
- AM-settlement exclusion: identify via instrument metadata from TastyTrade's instruments API (not
  DXLink) — settlement type is account/instrument metadata, not a streaming event field.

**Open questions:** none blocking, mostly operational tuning.

**Deliverables:** finalized `config.yaml`, scheduler wiring.

---

### Task 10 — Docker Compose Finalization & Deployment Docs
**Status:** NOT STARTED

**Purpose:** Final polish pass: make sure `docker-compose up` on a fresh machine works end to end,
document setup (TastyTrade credential acquisition, config, first run, how to verify data is flowing).

**How it fits in:** Wraps up the "docker compose yaml" deliverable and makes the whole thing usable by
future-you on a different machine.

**Implementation plan:**
- Review all services' restart policies, healthchecks, resource limits.
- Write a `README.md` covering setup end to end.
- Smoke test on a clean environment.

**Deliverables:** finalized `docker-compose.yml`, `README.md`.

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

**Template for new entries** (copy this when adding one):

```
**[Task N]** One-line description of the symptom.
- **Symptom:** What was observed (error message, behavior).
- **Root cause:** What was actually wrong, and why.
- **Fix:** What changed, concretely.
- **Files:** Which file(s) were touched.
- **General lesson:** (optional) anything worth remembering beyond this specific instance.
```
