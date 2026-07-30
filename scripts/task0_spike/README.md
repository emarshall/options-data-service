# Task 0 — Candle Depth Spike

## Why

We need to know, empirically, how far back TastyTrade's DXLink feed actually
retains `Candle` (OHLC) history for **option contracts** specifically —
this isn't documented, and it determines whether Task 5 (historical backfill)
is worth building. See PLAN.md Section 2 for full context.

## Setup

**Note:** TastyTrade discontinued username/password session auth on Dec 1, 2025. OAuth2 is now the
only option, so you need a `client_secret` + `refresh_token` pair (not just username/password).

```bash
python3 -m venv venv
source venv/bin/activate   # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

1. Create an OAuth application at TastyTrade's OAuth Applications page (check the market-data-related
   scopes you'll need, set callback to `http://localhost:8000`). This gives you a `client_id` +
   `client_secret`.
2. Generate a `refresh_token` (one-time, refresh tokens don't expire):
   - **Easiest:** on the TastyTrade website, go to **OAuth Applications > Manage > Create Grant**, and
     copy the refresh token shown.
   - **Or:** run `python get_refresh_token.py` (add `--sandbox` for a cert/sandbox account) — opens a
     browser, walks you through consent, prints a refresh token at the end.
3. `cp .env.example .env` and fill in `TASTYTRADE_CLIENT_SECRET` and `TASTYTRADE_REFRESH_TOKEN`.
   (`client_id` isn't needed by the main script at runtime — only during the one-time grant step above.)

## Run

```bash
python task0_candle_depth_spike.py --ticker SPY
```

Optional flags:
- `--use-sandbox` — use TastyTrade's sandbox environment instead of prod.
  Note: sandbox market data may be limited or delayed compared to prod for
  this kind of test; if results look empty/weird, try prod (real account,
  read-only market data access, no orders are placed).
- `--skip-expired-test` — skip the experimental already-expired-contract
  section.
- `--out-dir PATH` — custom output location.

**Best time to run this:** shortly after market close on a day when SPY (or
whatever ticker you pick) had a same-day (0DTE) expiration, so the
"nearest_expiration" test doubles as the expired-contract test. Any time
during market hours also works fine for the live-sample-data and
non-expired depth tests, just less conclusive for the expired-contract
question specifically.

## What to send back

After it finishes, it prints a path like `task0_output/20260716_.../`.
Please paste back (or upload) the contents of:
- `summary.md` (the main findings, human-readable)
- `sample_data.json` (optional, but useful — real payload shapes)

The `raw_events.jsonl` file has every single event received — no need to
share that unless something looks off and we need to dig in.

## If something breaks

The `tastytrade` PyPI package is actively developed and its method
signatures shift between versions occasionally. If you hit a `TypeError` or
`AttributeError` around `subscribe_candle`, run `pip show tastytrade` to
check your version and paste the error back — it's almost always a
one-line fix to how the script calls that method, not a fundamental
problem.
