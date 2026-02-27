# STOCKS ALGO 
### Long-Only, Mean-Reversion Equity Research & Backtesting Framework

---

## Table of Contents

| # | Section |
|---|---|
| 1 | [Project Overview](#1-project-overview) |
| 2 | [Core Strategy Philosophy](#2-core-strategy-philosophy) |
| 3 | [Data Sources](#3-data-sources-research-layer) |
| 4 | [Market Stages — State Machine](#4-market-stages-state-machine) |
| 5 | [Sector Spiders — Macro Filter](#5-sector-spiders-macro-filter) |
| 6 | [Research-First Design](#6-research-first-design) |
| 7a | [End-to-End Pipeline](#7a-end-to-end-data--research-pipeline) |
| 7b | [System Architecture Diagram](#7b-system-architecture-diagram) |
| 7c | [Project Structure](#7c-project-structure) |
| 8 | [Configuration-Driven Design](#8-configuration-driven-design) |
| 9 | [Intended Outcomes](#9-intended-outcomes) |
| 10 | [Project Status](#10-project-status-as-of-2026-02-23) |
| 11a | [Design Lock-In](#11a-design-lock-in-important) |
| 11b | [Operational Notes](#11b-operational-notes-important) |
| 12 | [Phase 09 — Backtest System](#12-phase-09--backtest-system-architecture-usage--configuration) |
| 12a | [Two-Layer Architecture](#12a-two-layer-architecture-design-rationale) |
| 12b | [Signal Detection Logic](#12b-signal-detection-logic-09a) |
| 12c | [Entry & Exit Logic](#12c-entry--exit-logic-09b--enginepy) |
| 12d | [Configuration Reference](#12d-configuration-reference--configbacktestyaml) |
| 12e | [Run Commands](#12e-run-commands-phase-09) |
| 12f | [Output Structure](#12f-output-structure-phase-09) |
| 12g | [Validated Results](#12g-validated-results) |
| 12h | [Phase 09C — Portfolio Simulation](#12h-phase-09c--portfolio-simulation) |
| 12i | [Sensitivity Research Roadmap](#12i-sensitivity-research-roadmap) |
| 12j | [Phase 09D — Universe Filter](#12j-phase-09d--universe-filter--enriched-report) |
| 12k | [Phase 09E — Batched Investor Report](#12k-phase-09e--time-batched-investor-report) |
| 12l | [Scenarios Testing Guide](#12l-scenarios-testing-guide) |
| 13 | [Historical Window Refresh Protocol](#13-historical-window-refresh-protocol-20212026) |

---

## 1. Project Overview

**ALGO-STOCKS** is a research-first, quant-style framework for designing, testing, and validating a  
**long-only, mean-reversion equity strategy** across U.S. listed stocks.

The system is built to:

- Identify **high-probability asymmetric opportunities** where price has deviated from its long-term mean
- Enter **only after mean reversion has started** (never catching falling knives)
- Manage positions through clearly defined **market stages**
- Exit systematically when momentum fades or mean value is approached
- Be **fully backtestable, explainable, and modular**

> - This project is **not a trading bot**.  
> - It is a **research and backtesting platform** designed to validate statistical edge *before* any automation.

---

## 2. Core Strategy Philosophy

### Key Principles

- **Long-only**
  - Buy → manage → exit to cash  
  - No short selling under any circumstance
- **Mean reversion, not value investing**
  - Stocks are bought when **reversion toward the long-term mean has begun**
- **State-based decision making**
  - Every stock exists in **exactly one of nine market stages**
- **Asymmetric risk profile**
  - Small, predefined downside (e.g. ~6%)
  - Large potential upside when reversion completes
- **Regime awareness**
  - Individual stock signals are filtered by **sector (“spider”) behaviour**

---

## 3. Data Sources (Research Layer)

The framework is designed to merge and normalize data from multiple providers:

### Primary Sources
- **Finviz**
  - Broad universe coverage
  - Fundamental & technical snapshot data
- **Nasdaq / Exchange Data**
  - Improved accuracy
  - Survivorship handling

### Optional Overlays
- **Yahoo Finance**
- **Zacks**
  - Analyst ratings and price targets (research overlays only)

All incoming data is treated as **non-canonical** and normalized internally to ensure:

- Reproducibility
- Schema stability
- Point-in-time safe backtesting

---

## 4. Market Stages (State Machine)

Each stock is classified daily into **one of nine market stages**, based on price 
structure, trend behaviour, and volatility characteristics:

The framework is explicitly designed to trade **mean reversion after sharp dislocations** —
not slow drifts or weak trends.

### Stage Definitions

**Stage 1 – Not Eligible**
- Price is above the 200 EMA
- Stock remains ineligible until price breaks below the 200 EMA

**Stage 2 – Sharp Downtrend**
- Price below 200 EMA
- Sharp decline (e.g. >5% over ~3 trading days)
- Price closes below lower Bollinger Band (2 SD)
- EMA10 slope negative and accelerating
- Optional confirmations:
  - Volume spike (>15% above 10-day average)
  - New 20-day Donchian low
- Conditions are evaluated over a rolling window (e.g. 5–7 days), not required to align on a single day

**Stage 3 – Downtrend**
- Price remains below 200 EMA
- Slower, grinding decline
- Price between Bollinger midline and lower band
- No new Donchian lows
- Important rule:
  - **Stage 3 alone is not tradable**
  - A stock must have passed through **Stage 2** at some point to be eligible for entry later

**Stage 4 – Below Zone**
- Price stabilises after Stage 2 or Stage 3
- No new Donchian lows
- Sideways price action, early basing behaviour

**Stage 5 – Lower Zone**
- Price enters upper half of Donchian range
- Price closes above EMA10
- EMA10 crosses above EMA20

**Stage 6 – Breakout (Primary Entry Stage)**
- Price breaks above 20-day Donchian high
- Price above EMA10 with EMA10 > EMA20
- Volume >15% above 10-day average
- **First valid long entry point**

**Stage 7 – Breakout Confirmation**
- Strong follow-through above breakout level
- Elevated volume persists
- Momentum confirms continuation

**Stage 8 – In-Zone**
- Price continues higher but momentum slows
- Position actively managed

**Stage 9 – In-Zone (Fading)**
- Loss of momentum
- Failure to make new highs
- Increased risk of mean reversion

### Stage Transition Rules (Non-Skippable Logic)

- Stage 1 → Stage 2: Sharp breakdown below EMA200
- Stage 1 → Stage 3: Slow drift lower without sharp dislocation
- Stage 2 → Stage 4: Stabilisation, no new lows
- Stage 3 → Stage 4: Sideways basing begins
- Stage 4 → Stage 5: Entry into upper Donchian range + EMA reclaim
- Stage 5 → Stage 6: Breakout with volume and EMA confirmation
- Stage 6 → Stage 7: Follow-through confirmation
- Stage 7 → Stage 8: Momentum slows but structure intact
- Stage 8 → Stage 9: Momentum fades, no new highs
- Stage 9 → Exit: Stop-loss, time stop, or reversion condition (defined later)

**Key Constraint:**
A stock that never experienced a **Stage 2 sharp dislocation** is never eligible for a trade.

### Indicators Used (Core)
- Donchian Channels (20-day high/low)
- Bollinger Bands (20, 2 SD)
- EMA stack (EMA10 / EMA20 / EMA50 / EMA100 / EMA200)
- Volume behaviour (10-day avg + surge flag)
- Momentum indicators (confirmation only; optional MACD/RSI)

### Purpose
Market stages determine:
- Whether a stock is tradable
- When entries are allowed
- How positions are managed
- When exits are triggered

---

## 5. Sector “Spiders” (Macro Filter)

Stocks are grouped into normalised **sector baskets (“spiders”)**.

Each spider:
- Is treated as its own price series
- Has its own Donchian highs/lows and trend regime
- Acts as a **macro permission layer** for individual trades

### Example Behaviour
- Sector making new 20-day highs → signals allowed / sized aggressively
- Sector breaking lower → signals blocked or reduced

This ensures alignment between **micro stock signals** and **macro sector structure**.

---

## 6. Research-First Design

This project is intentionally designed for **research and validation**, not premature automation.

### Research Objectives
- Measure expectancy **by stage**
- Identify which stages generate alpha
- Test sensitivity to:
  - Donchian lookback lengths
  - EMA definitions (EMA200 vs alternatives)
  - Stop-loss logic
  - Sector regime strictness
  - Analyst rating overlays
- Explicit testing of **dislocation requirement**
  - Compare performance of:
    - Stage 2 → 6 setups
    - Stage 3-only drift setups (expected to underperform)

All filters and overlays are **toggleable** so their impact can be measured objectively.

---

## 7a. End-to-End Data & Research Pipeline

This section documents the full deterministic pipeline from raw ingestion to backtest-ready research state.

The architecture is strictly layered and restart-safe.

### Full Pipeline Flow

```
Finviz Universe
↓
06 - TwelveData OHLCV ingestion
↓
07A - Spider memberships
↓
07B - Spider OHLCV series
↓
07C - Spider feature engineering
↓
07D - Spider stage classification
↓
07G - Spider gate daily table (macro permission layer)
↓
08A - Stock feature engineering
↓
08B - Stock stage classification
↓
09A - Raw signal generation (Layer 1, runs once)
↓
09B - Backtest simulation (Layer 2, re-runs freely)
↓
09C - Portfolio simulation (capital constraints + exposure caps)
↓
09D - Universe filter + sector enrichment (identify investable subset)
↓
09E - Time-batched investor report (Batch 1: 2022–2023, Batch 2: 2024–2026)
```

---

### Data Layer Outputs

| Stage | Output Directory |
|--------|------------------|
| 06 | `data/raw/prices_daily/twelvedata/parquets/` |
| 07A | `data/metadata/spiders/` |
| 07B | `data/raw/spiders_daily/` |
| 07C | `data/cleaned/spiders_daily/features/` |
| 07D | `data/cleaned/spiders_daily/stages/` |
| 07G | `data/cleaned/spiders_daily/gate/` |
| 08A | `data/cleaned/stocks_daily/features/` |
| 08B | `data/cleaned/stocks_daily/stages/` |

---

### Design Guarantees

- Expanding-window calculations (no lookahead bias)
- Idempotent stage scripts
- Restart-safe feature builders
- Macro gating separated from micro signals
- Deterministic indicator contract
- Canonical stage classifier reused everywhere

---

## 7b. System Architecture Diagram

- This section visually documents the deterministic architecture of ALGO-STOCKS.
- The system is intentionally layered and modular. Each layer has a single responsibility.

### High-Level Architecture

```yaml
                    ┌────────────────────────────┐
                    │      Finviz Universe       │
                    │   (Trade-Ready Tickers)    │
                    └──────────────┬─────────────┘
                                   │
                                   ▼
                    ┌────────────────────────────┐
                    │     06 - OHLCV Ingestion   │
                    │  Twelve Data (Daily Bars)  │
                    │  Raw Parquets (Per Ticker) │
                    └──────────────┬─────────────┘
                                   │
                 ┌─────────────────┴─────────────────┐
                 ▼                                   ▼
  ┌────────────────────────────┐       ┌────────────────────────────┐
  │ 07A Spider Memberships     │       │ 08A Stock Feature Builder  │
  │ Sector Mapping + Weights   │       │ EMA / BB / Donch / Volume  │
  └──────────────┬─────────────┘       └──────────────┬─────────────┘
                 ▼                                   ▼
  ┌────────────────────────────┐       ┌────────────────────────────┐
  │ 07B Spider OHLCV Builder   │       │ 08B Stock Stage Classifier │
  │ Weighted Sector Series     │       │ 9-Stage State Machine      │
  └──────────────┬─────────────┘       └────────────────────────────┘
                 ▼
  ┌────────────────────────────┐
  │ 07C Spider Features        │
  │ Indicator Surface          │
  └──────────────┬─────────────┘
                 ▼
  ┌────────────────────────────┐
  │ 07D Spider Stages          │
  │ Macro State Machine        │
  └──────────────┬─────────────┘
                 ▼
  ┌────────────────────────────┐
  │ 07G Spider Gate Daily      │
  │ Macro Permission Layer     │
  └──────────────┬─────────────┘
                 ▼
  ┌────────────────────────────┐
  │ 09 Backtest Engine         │
  │ Portfolio + Attribution    │
  └────────────────────────────┘

```

### Layer Responsibilities

| Layer | Responsibility |
|-------|---------------|
| Universe | Define eligible US equities |
| Raw OHLCV | Canonical historical price base |
| Spider Layer | Sector-level macro regime classification |
| Stock Features | Indicator surface per equity |
| Stock Stages | 9-state mean-reversion model |
| Spider Gate | Macro permission enforcement |
| Backtest Engine | Portfolio simulation & research |


### Architectural Principles

- **Strict separation of concerns**
- **No lookahead bias (expanding windows only)**
- **Raw data never overwritten**
- **Derived layers rebuildable at any time**
- **Macro regime separated from micro signal**
- **Stage logic reusable across spiders and stocks**

### Data Layering Model

```
data/
│
├── raw/
│ ├── finviz
│ ├── prices_daily/twelvedata/
│ │ ├── meta
│ │ ├── parquets
│ └── spiders_daily/
│
├── cleaned/
│ ├── spiders_daily/
│ │ ├── features/
│ │ ├── stages/
│ │ └── gate/
│ │
│ └── stocks_daily/
│ │ ├── features/
│ │ ├── stages/
│ │
│ └── universe/
│ │ ├── universe_trade_ready_*
│
├── metadata/
│ ├── spiders/
│ ├── reit_exlusion.csv
│ ├── sector_mapping.csv

```

### Deterministic Rebuild Model *(jump to Section 13)*

If raw OHLCV history changes:

1. Delete all `cleaned/` derived folders
2. Re-run 07 → 08 → 09 in order
3. Outputs guaranteed reproducible

The architecture ensures full reproducibility from raw historical bars.

---

## 7c. Project Structure *(current repo layout)*

````
ALGO-STOCKS/
│
├── backtest/
│ ├── engine.py
│ ├── metrics.py
│ ├── attribution.py
│ └── regime_analysis.py
│
├── config/
│ ├── indicators.yaml
│ ├── stages.yaml
│ ├── spiders.yaml
│ └── portfolio.yaml
│
├── data/
│ ├── raw/
│ │ ├── prices_daily/twelvedata/parquets/ # per-ticker OHLCV (Stage 6)
│ │ └── spiders_daily/ # spider OHLCV series (Stage 7A / 07B)
│ ├── cleaned/
│ └── metadata/
│   └── spiders/ # memberships + summary (Stage 6.5 / 07A)
│
├── features/
│ ├── technicals/
│ │ ├── indicators.py # canonical indicator implementations
│ │ └── pipeline.py # apply_indicators() entrypoint
│ └── spiders/
│ ├── sector_series.py
│ └── sector_regime.py
│
├── filters/
│ ├── static_gates.py
│ ├── spider_gate.py
│ └── analyst_overlay.py
│
├── portfolio/
│ ├── sizing.py
│ ├── constraints.py
│ └── rebalance.py
│
├── research/
│ ├── experiments/ # staged, audit-style build scripts
│ └── reports/
│
├── signals/
│ ├── entry_engine.py
│ ├── exit_engine.py
│ └── asymmetry_metrics.py
│
├── stages/
│ ├── stage_definitions.md
│ ├── stage_classifier.py
│ └── stage_transitions.py
│
├── zTester/ # utilities / visualization sandbox
│
├── run.py
├── requirements.txt
└── README.md
````

---

## 8. Configuration-Driven Design

All strategy assumptions are externalized via **YAML configuration**:

- Indicator settings (`config/indicators.yaml`)
- Stage thresholds (`config/stages.yaml`)
- Sector (“spider”) rules (`config/spiders.yaml`)
- Portfolio constraints (`config/portfolio.yaml`)

This ensures:
- Experiments are reproducible
- Assumptions are explicit
- Logic changes do not require code rewrites

---

## 9. Intended Outcomes

This framework is built to answer:

- Does the strategy produce positive expectancy?
- Which stages actually generate alpha?
- How dependent is performance on sector regime?
- Do analyst ratings improve or degrade results?
- How robust is the edge across market environments?

Only once these questions are answered does automation become relevant.

---

## 10. Project Status *(as of 2026-02-23)*

### Current Phase
- ✅ **Data foundation + spiders scaffold complete. Indicators are canonical and deterministic.**
- ✅ **5-year OHLCV ingestion complete (2021–2026): `TD_START_DATE=2021-01-01`, `TD_END_DATE=2026-02-01`**
- ✅ **Full spider pipeline complete:** 07A → 07B → 07C → 07D → 07G
- ✅ 08A (stock feature engineering) — complete
- ✅ 08B (stock stage classification) — complete
- ✅ 09A (raw signal generation) — complete
- ✅ 09B (single-ticker + universe backtest runner) — complete
- ✅ 09C (portfolio simulation with capital constraints) — complete
- ✅ 09D (universe filter + enriched sector report) — complete
- ✅ 09E (time-batched investor report + Excel) — complete

**Phases 09A through 09E are complete.** The full research pipeline is operational: signal generation → trade simulation → portfolio analysis → universe filtering → investor-ready reporting. All results have been validated across the full 2,582-ticker universe with batched time-window analysis.

### Completed Milestones

**Universe construction (Finviz)**
- ✅ Stage 1: Finviz raw export capture (audit-safe, schema logged)
- ✅ Stage 2: Promote raw → cleaned snapshot (traceable baseline)
- ✅ Stage 3: Contract dataset creation (typed numeric fields, no drops)
- ✅ Stage 4: Trade-ready universe filtering (policy layer)
  - Result: **2,835** trade-ready U.S. equities

**Trade-ready universe**
- ✅ Filters applied:
  - `country == USA`
  - `market_cap >= 300M`
  - REIT exclusions enabled (sector/industry rules)
- ✅ Resulting universe size:
  - `rows_after = 2835` tickers (from 10,892 total Finviz rows)

**Twelve Data validation**
- ✅ Single-ticker OHLCV test (AAPL) confirmed correct window coverage:
  - `start_date = 2021-01-01`
  - `end_date   = 2026-02-01`
  - `rows = ~1275` daily bars (trading days)
  - first/last dates align with US trading calendar
- ✅ Multi-ticker batch ingestion validated
- ✅ Restart-safe progress tracking implemented
- ✅ Credit-aware batch throttling confirmed
- ✅ No silent failures observed (`_errors.jsonl` remains empty)

**Stage 6 — OHLCV ingestion (Twelve Data)**
- ✅ Daily OHLCV collected for full trade-ready universe (with short-history handling)
- ✅ Window: `2021-01-01 → 2026-02-01`
- ✅ Gate: `expected_last_date = 2026-01-30`, `min_rows_ok = 1200`
- ✅ `ok_short_history` correctly applied to IPOs / recent listings
- ✅ Retry pass completed; remaining hard-fails documented (see below)

**Provider Exceptions (final hard-fails after retry)**
The following tickers remain unavailable via TwelveData (symbol invalid / no data on dates):
- `ALUB-U` (also tried `ALUB.U`) → symbol invalid
- `NWAX-U` (also tried `NWAX.U`) → symbol invalid
- `SBXE-U` (also tried `SBXE.U`) → symbol invalid
- `PLYX` → “No data available on specified dates”

**Ingestion outcome**
- Total universe: **2,835 tickers**
- Successfully ingested: **2,831**
- Short-history IPOs: handled via `ok_short_history`
- Permanent exclusions: 4 (API symbol mismatch)

---

### Ingestion guarantees (now enforced)

- ✔️ **Idempotent** — reruns never re-fetch completed tickers
- ✔️ **Restart-safe** — can stop/start indefinitely
- ✔️ **Credit-safe** — zero API calls if `remaining == 0`
- ✔️ **Observable** — batch-level logging shows liveness
- ✔️ **Audit-ready** — every ticker logs:
  - first date
  - last date
  - row count
  - status reason

---

### Short-history ticker handling (important fix)

Tickers with limited trading history (e.g. IPOs) are now handled correctly.

**Rule:**
- A ticker is marked:
  - `ok` → full history present
  - `ok_short_history` → IPO after `START_DATE` but **last_date meets gate**
  - `partial` → missing recent data (retryable)

**Key outcome:**
- Short-history tickers (e.g. IPOs) are:
  - ✅ accepted once
  - ✅ never re-fetched
  - ✅ not treated as errors
  - ✅ safely excluded from wasted API credits

---

### Smoke-test & safety controls

- `TD_SMOKE_N` / `TD_SMOKE_TICKERS` allow dry-runs without burning credits
- Early-exit guard prevents **any API call** when nothing remains
- `.env` and `.venv` removed from version control (now ignored correctly)

---

**Stage 6.5 / 07A — Spider memberships (sector baskets)**
- ✅ `data/metadata/spiders/spider_memberships.csv`
- ✅ `data/metadata/spiders/spider_summary.csv`
- ✅ 10 sector spiders; weights sum to 1 per spider; universe fully mapped

**Stage 7A / 07B — Spider OHLCV series built**
- ✅ 10 sector spider parquets written under:
  - `data/raw/spiders_daily/SECTOR_*.parquet`
- ✅ Robust to missing member tickers (coverage renormalised per date)
- ✅ Coverage medians ~0.98–1.00 across sectors (healthy)

**Indicator foundation locked**
- ✅ Canonical indicator code lives in:
  - `features/technicals/indicators.py`
  - `features/technicals/pipeline.py`
- ✅ Parameters externalised in:
  - `config/indicators.yaml`

**Stage 7C — Spider feature engineering**
- ✅ Spider features created under:
  - `data/cleaned/spiders_daily/features/SECTOR_*.parquet`
- ✅ Indicators applied (EMA, Donchian, Bollinger, volume overlays; optional MACD/RSI)

**Stage 7D — Spider stage classification**
- ✅ Spider stages created under:
  - `data/cleaned/spiders_daily/stages/SECTOR_*.parquet`
- ✅ Shared stage logic reused from `stages/stage_classifier.py`
- ✅ Gating enabled:
  - `stage_logic.require_breakout_before_inzone: true`

**Stage classifier sanity check (example: SECTOR_FINANCIALS)**
- ✅ First observed breakout-confirmation regime (Stage 7) begins around **2024-01-23**
- ✅ Distribution contains meaningful states:
  - Stage 7 / 8 / 9 present post-breakout
  - Early history correctly blocked by `min_history_days` (Stage 1)

---

## 11a. Design Lock-In (Important)

At this stage, the following are considered **design-locked**:

- **Long-only mean-reversion strategy**
- Mandatory sharp dislocation requirement (Stage 2)
- **9-stage state machine** with constrained transitions
- **No trades** allowed from slow downtrends (Stage 3-only)
- **Entry focus on Stage 6 (Breakout)**
- Exit logic intentionally deferred

All future development must respect these constraints unless explicitly tested and justified.

---

## 11b. Operational Notes (Important)

### Twelve Data free/basic limits
Twelve Data free/basic tier is rate/credit limited. The ingestion pipeline is designed to be:
- resumable across multiple days
- safe to interrupt and restart
- able to continue from last completed ticker

---

## 12. Phase 09 — Backtest System *(Architecture, Usage & Configuration)*

---

### 12a. Two-Layer Architecture (Design Rationale)

The backtest is intentionally split into two independent layers.
This is the single most important architectural decision in Phase 09.
```
Layer 1 — Signal Generator (09A)   COMPUTE-HEAVY, run ONCE
    Reads  : data/cleaned/stocks_daily/features/*.parquet  (08A)
             data/cleaned/stocks_daily/stages/*.parquet    (08B)
             data/cleaned/spiders_daily/gate/spider_gate_daily.parquet (07G)
             data/metadata/spiders/spider_memberships.csv
    Writes : output/signals/raw_signals_all.parquet
             output/signals/raw_signals_summary.json

Layer 2 — Backtest Runner (09B)    FAST, re-run FREELY
    Reads  : output/signals/raw_signals_all.parquet (only)
    Writes : output/backtests/<run_tag>/...
```

**Why this matters with 2,831 tickers:**
Scanning every ticker's stage history to find signals takes compute.
Simulating stop logic and sizing is arithmetic — it is nearly instant.
By separating them, you can change your stop from ATR to fixed %, or
change position sizing, or toggle the spider gate on/off, and re-run
the entire universe backtest in under 2 seconds without re-reading
2,831 parquet files.

**Re-run 09A only when:**
- Stage classifications change (08B re-run)
- Spider gate changes (07G re-run)
- `entry_stages` or `require_stage2_history` in `config/backtest.yaml` changes

**Re-run 09B freely when:**
- Changing stop mode (ATR vs fixed %)
- Changing ATR multiplier
- Changing position sizing
- Toggling spider gate on/off
- Changing exit rules (time stop days, Stage 9 exit toggle)
- Changing overlap mode (disabled vs scale-in)

---

### 12b. Signal Detection Logic (09A)

A signal is generated when a ticker's stage **transitions INTO** Stage 6 or Stage 7
from a state that is not Stage 6 or 7. This is transition detection, not level detection.

**Example:** A stock sitting in Stage 7 for 5 consecutive days generates one signal
(on the first day it entered Stage 7), not five.

**Why Stage 6 signals are rare (expected behaviour):**
The stage classifier evaluates Stage 7 conditions before Stage 6.
Stage 7 requires one extra condition: `close > EMA50`.
After a proper Stage 2 dislocation and base formation, by the time price
breaks the 20-day Donchian high with EMA10 > EMA20, it has almost always
already recovered above EMA50 — so it skips Stage 6 entirely.
Stage 6 only fires in the rare case where price breaks out but has not yet
crossed EMA50. **Stage 7 is your real primary entry in practice.**
Both are captured as separate `signal_type` values for research attribution.

**Dislocation prerequisite (design-locked):**
`require_stage2_history: true` means a ticker must have printed Stage 2
at any point *strictly before* the signal bar for the signal to be valid.
This is point-in-time safe: the Stage 2 bar itself does not count as "before."
This is the core thesis constraint — no dislocation, no trade.

---

### 12c. Entry & Exit Logic (09B / engine.py)

**Entry timing:**
Signal is detected at end of signal bar (close).
Entry is at the **open of the next trading day**.
This is lookahead-safe — you cannot observe a close and enter at that same close.

**Stop price:**
Computed on the signal bar and stored in the signals file.
```
stop_price = entry_open - (ATR_14 × atr_multiplier)
```
ATR(14) uses Wilder's smoothing (EWM with alpha = 1/14).
If ATR is zero or missing, falls back to `fixed_stop_pct` automatically.
A safety floor of 0.5% of entry price prevents near-zero stops on stable tickers.

**Position sizing:**
```
risk_dollars   = account_equity × risk_pct_per_trade × gate_risk_multiplier
shares         = floor(risk_dollars / stop_distance_per_share)
notional       = shares × entry_open
```
With default settings: $10,000 equity × 1% = $100 risked per trade.
Position size varies naturally — calm large-caps get more shares,
volatile small-caps get fewer. This is correct quant behaviour.

**Exit hierarchy (checked in this order each day):**
1. Gap protection: open already below stop → exit at open
2. Intraday stop hit: low of day touched stop price → exit at stop price
3. Stage 9 detected: signal observed at close → exit at next day's open
4. Time stop: max hold days reached → exit at next day's open
5. End of data: last available bar → exit at close

**R-multiple:**
Every trade records `pnl_r` = PnL per share ÷ stop distance per share.
A trade stopped out at exactly the stop price = −1.0R (verified in smoke test).
A trade that returns 2× the risk taken = +2.0R.
Expectancy R = average R across all trades. Positive expectancy R = edge exists.

---

### 12d. Configuration Reference — `config/backtest.yaml`

All strategy parameters are controlled from this single file.
**Never edit engine.py or metrics.py to change parameters — use this file.**
09B saves a snapshot of this file into every run folder for full audit trail.
```yaml
# ── RUN SETTINGS ─────────────────────────────────────────────────────────────

run:
  run_tag_prefix: "baseline_v1"
  # Prefix for the auto-generated run folder name.
  # Output lands in: output/backtests/baseline_v1_YYYYMMDD_HHMM/
  # Change this when starting a new research variant so runs don't overwrite.

  smoke_test: true
  # true  = only process the tickers listed in smoke_tickers (fast, for testing)
  # false = process the full universe (2,831 tickers, takes longer)
  # ALWAYS run smoke_test: true first when changing parameters.

  smoke_tickers: ["AAPL", "MSFT", "NVDA", "JPM", "XOM"]
  # Which tickers to use for smoke testing. Change to any tickers you want to
  # inspect in detail. Works for both 09A and 09B independently.

# ── SIGNAL SETTINGS (09A) ────────────────────────────────────────────────────

signal:
  entry_stages: [6, 7]
  # Which stage numbers trigger an entry signal.
  # 6 = Breakout (price breaks Donchian high, trend turning)
  # 7 = Breakout Confirmed (price holds above breakout, EMA stack improving)
  # In practice, almost all signals are Stage 7 (see Section 12b for why).
  # Research variant: try [7] only to measure pure confirmed-breakout edge.

  require_stage2_history: true
  # true  = ticker MUST have printed Stage 2 (sharp dislocation) at some point
  #         in history before the signal bar. This is the core thesis constraint.
  # false = allow entries from slow downtrends (Stage 3) as well.
  # Design-locked: keep true for baseline. Set false only as a sensitivity test
  # to measure how much the dislocation requirement adds to edge.

# ── ENTRY SETTINGS ───────────────────────────────────────────────────────────

entry:
  timing: "next_open"
  # Only valid value for now. Entry always at open of day after signal bar.
  # This is the lookahead-safe approach. Do not change.

# ── STOP LOSS SETTINGS ───────────────────────────────────────────────────────

stop:
  mode: "atr"
  # "atr"        = stop adapts to each stock's actual volatility (recommended)
  #                A calm stock gets a tighter stop; a volatile stock gets room.
  # "fixed_pct"  = same % stop for every stock regardless of volatility.
  #                Simple but blunt — a 6% stop on AAPL ≠ a 6% stop on NVDA.

  atr_period: 14
  # Number of bars used to compute ATR. 14 is the Wilder standard.
  # Increase (e.g. 20) for a wider, more stable stop.
  # Decrease (e.g. 7) for a tighter, more reactive stop.
  # Only used when mode = "atr".

  atr_multiplier: 2.0
  # stop_price = entry_open - (ATR × this value)
  # 1.5 = tighter stop, more stop-outs, fewer losses but smaller winners
  # 2.0 = standard (baseline)
  # 3.0 = wider stop, fewer stop-outs, bigger losses when wrong
  # Research variant: test 1.5 vs 2.0 vs 3.0 to find optimal for this strategy.
  # Only used when mode = "atr".

  fixed_stop_pct: 0.06
  # stop_price = entry_open × (1 - this value)
  # 0.06 = 6% stop below entry price.
  # Only used when mode = "fixed_pct".

  gap_protection: true
  # true  = if a stock opens below the stop price (overnight gap down),
  #         exit immediately at that open price. Realistic.
  # false = only exit if the low of the day hits the stop. Less realistic.

# ── EXIT SETTINGS ────────────────────────────────────────────────────────────

exit:
  stage9_exit_enabled: true
  # true  = exit when the stock enters Stage 9 (momentum fading, trend weakening).
  #         Exit signal observed at close; executed at next day's open.
  # false = ignore Stage 9. Only stop-loss and time stop trigger exits.
  #         Use false as a sensitivity test to measure what Stage 9 adds.

  time_stop_enabled: true
  # true  = force exit after a maximum number of holding days (see below).
  #         Prevents capital being locked in slow-moving positions indefinitely.
  # false = hold until stop-loss or Stage 9. No time limit.

  time_stop_days: 60
  # Maximum number of trading days to hold a position.
  # If the position has neither hit its stop nor reached Stage 9 after this
  # many days, it is closed at the next day's open.
  # 60 trading days ≈ 3 calendar months.
  # Research variant: test 30, 45, 60, 90 to find optimal hold cap.

# ── POSITION SIZING ───────────────────────────────────────────────────────────

sizing:
  account_equity: 10000.0
  # Starting account size in USD. Used for PnL and return calculations.
  # For Phase 09A/09B each trade is simulated independently with this equity.
  # Portfolio-level capital constraints (max concurrent positions, sector caps)
  # are introduced in Phase 09C.

  risk_pct_per_trade: 0.01
  # Fraction of account equity risked on each trade.
  # 0.01 = 1% = $100 risk per trade on a $10,000 account.
  # Position size = risk_dollars ÷ stop_distance_per_share.
  # This means position size automatically adjusts per stock:
  #   - Low volatility stock (small stop) → more shares
  #   - High volatility stock (large stop) → fewer shares

  min_shares: 1
  # Minimum position size. Prevents the calculation returning 0 shares
  # on very wide stops or very low-priced stocks.

  overlap_mode: "disabled"
  # Controls whether multiple trades can be open on the same ticker at once.
  #
  # "disabled"  = one position per ticker at a time (baseline, recommended).
  #               A new signal on a ticker is skipped if that ticker already
  #               has an open trade. This is the conservative, clean baseline.
  #
  # "scale_in"  = allow adding to a position on subsequent signals while
  #               the original trade is still open (pyramiding).
  #               Uses the same risk % and stop per add-on leg.
  #               Capped at max_scale_ins additional entries.
  #               Use this only after establishing baseline edge.

  max_scale_ins: 2
  # Maximum number of additional entries allowed per open position.
  # Only used when overlap_mode = "scale_in".
  # 2 means: original entry + up to 2 add-ons = 3 legs maximum per ticker.

# ── SPIDER GATE (MACRO FILTER) ────────────────────────────────────────────────

spider_gate:
  enabled: false
  # The spider gate is the macro permission layer built in Stage 07G.
  # It checks whether the sector spider (sector basket) is in a supportive
  # regime on the trade entry date.
  #
  # false = gate disabled. All signals trade regardless of sector regime.
  #         Use this for the baseline run to measure raw stock-level edge.
  #         This is the correct first run — you need to know if the stock
  #         signal has edge before adding the macro filter.
  #
  # true  = gate enabled. Signals are blocked if the sector spider is in
  #         Stage 2, 3, or 4 (downtrend / stress). Signals are allowed if
  #         sector spider is in Stage 7, 8, or 9.
  #         Additionally, gate_risk_mult from 07G is applied to position size:
  #           Spider Stage 7 → risk multiplier 1.10 (size up in strong regime)
  #           Spider Stage 8 → risk multiplier 1.00 (neutral)
  #           Spider Stage 9 → risk multiplier 0.80 (size down in fading regime)
  #
  # Research protocol: run baseline with false, then flip to true and re-run.
  # The delta in net PnL and trade count IS the gate attribution — how much
  # value the macro filter adds (or costs) to the strategy.

# ── OUTPUT SETTINGS ──────────────────────────────────────────────────────────

output:
  debug_tail_rows: 200
  # Number of rows written to debug_last_200_rows.parquet per ticker.
  # This file shows the last N rows of joined features + stages for each ticker.
  # Used to visually audit indicator values and stage transitions near end of data.

  base_dir: "output/backtests"
  # Root directory for all backtest run outputs.

  signals_dir: "output/signals"
  # Root directory for Layer 1 (09A) signal outputs.

  save_single_ticker_files: true
  # true  = write per-ticker folders under output/backtests/<run_tag>/single/
  #         Each folder contains trades.parquet, equity.parquet, summary.json,
  #         debug_last_200_rows.parquet. Use true for smoke tests and audit.
  # false = skip per-ticker folders. Only universe/ aggregates are written.
  #         Use false for full universe runs to save disk space and time.
```

---

### 12e. Run Commands (Phase 09)

All commands from project root.

**Standard workflow (smoke test first, then full universe):**
```powershell
# Step 1 — Generate signals (Layer 1)
# Run once. Re-run only if stages or gate data changes.
python research/experiments/09A_generate_raw_signals.py

# Step 2 — Run backtest simulation (Layer 2)
# Re-run freely when changing config/backtest.yaml parameters.
python research/experiments/09B_run_backtest.py
```

**Research variant workflow:**
```powershell
# 1. Edit config/backtest.yaml (change parameters, change run_tag_prefix)
# 2. Re-run 09B only — no need to re-run 09A
python research/experiments/09B_run_backtest.py
# Each run writes to a new timestamped folder — prior runs are preserved
```

**Scale full universe run:**
```powershell
# 1. In config/backtest.yaml set:
#      smoke_test: false
#      save_single_ticker_files: false   (saves disk space)
#      run_tag_prefix: "universe_v1"
# 2. Regenerate signals for full universe
python research/experiments/09A_generate_raw_signals.py
# 3. Run backtest
python research/experiments/09B_run_backtest.py
```

---

### 12f. Output Structure (Phase 09)
```
output/
├── signals/
│   ├── raw_signals_all.parquet          # All entry signals (Layer 1 output)
│   └── raw_signals_summary.json         # Signal count, date range, diagnostics
│
└── backtests/
    └── <run_tag>/                        # e.g. baseline_v1_20260224_2149
        ├── backtest_config_snapshot.yaml # Exact config used for this run (audit)
        ├── single/                       # Per-ticker detail (if enabled)
        │   └── <TICKER>/
        │       ├── trades.parquet        # Trade-by-trade record
        │       ├── equity.parquet        # Equity curve (step at each exit)
        │       ├── summary.json          # Full metrics for this ticker
        │       └── debug_last_200_rows.parquet
        └── universe/
            ├── trades_all.parquet        # All trades concatenated
            ├── summary_by_ticker.csv     # Per-ticker metric table
            ├── failures.jsonl            # Non-fatal failures with reasons
            └── universe_report.json      # Aggregate metrics across all tickers
```

**Full column set in `trades_all.parquet` and per-ticker `trades.parquet`:**

| Column | Type | Description |
|---|---|---|
| `ticker` | string | Stock symbol |
| `signal_date` | date | Date the Stage 6/7 transition was detected |
| `signal_type` | string | `stage6_entry` or `stage7_entry` |
| `entry_date` | date | Next trading day after signal (actual entry date) |
| `entry_price` | float | Open price on entry_date |
| `stop_price` | float | Calculated stop loss price |
| `stop_mode` | string | `atr` or `fixed_pct` (which mode was active) |
| `stop_distance` | float | Distance in $ between entry and stop |
| `shares` | int | Number of shares held |
| `notional` | float | Total position value at entry (shares × entry_price) |
| `exit_date` | date | Date position was closed |
| `exit_price` | float | Price at which position was closed |
| `exit_reason` | string | `stop_hit` / `stop_gap` / `stage9_exit` / `time_stop` / `end_of_data` |
| `hold_days` | int | Number of trading days position was held |
| `pnl_dollar` | float | Profit or loss in USD |
| `pnl_pct` | float | Profit or loss as % of entry price |
| `pnl_r` | float | R-multiple: pnl ÷ risk taken. `-1.0` = stopped out exactly at stop |
| `gate_risk_mult` | float | Spider gate risk multiplier applied to sizing (1.0 if gate disabled) |
| `spider_id` | string | Sector spider this ticker belongs to (e.g. `SECTOR_TECHNOLOGY`) |
| `atr_14` | float | ATR(14) value on signal date, used to compute stop distance |

**Quick audit — inspect any ticker from `trades_all.parquet`:**
```powershell
python -c "
import pandas as pd
df = pd.read_parquet(r'output\backtests\<run_tag>\universe\trades_all.parquet')
t = df[df['ticker']=='AAPL'].sort_values('entry_date')
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
print(t[['entry_date','exit_date','entry_price','stop_price','exit_reason','hold_days','pnl_pct','pnl_r']].to_string())
"
```

Replace `AAPL` with any ticker in the universe. Replace `<run_tag>` with the actual folder name under `output/backtests/`.

**Export any parquet to CSV or XLSX for manual inspection:**
```powershell
# 1. Edit INPUT_PARQUET and FILTER_TICKER at top of script
# 2. Run:
python zTester/04_parquet_inspector.py
# Output lands in: output/exports/
```

**Key metrics in every summary.json / universe_report.json:**

| Metric | What it means |
|---|---|
| `win_rate_pct` | % of trades that closed profitable |
| `avg_win_pct` | Average return on winning trades |
| `avg_loss_pct` | Average loss on losing trades |
| `profit_factor` | Gross wins ÷ gross losses. >1 = profitable system |
| `expectancy_r` | Average R per trade. Positive = edge exists |
| `net_return_pct` | Total PnL as % of starting equity |
| `max_drawdown_pct` | Largest peak-to-trough equity decline |
| `sharpe_ratio` | Risk-adjusted return proxy (trade-level, directional only) |
| `sortino_ratio` | Like Sharpe but only penalises downside volatility |
| `calmar_ratio` | Net return ÷ max drawdown |
| `avg_hold_days` | Average trade duration in trading days |
| `stage6_entries` | How many trades entered on Stage 6 signal |
| `stage7_entries` | How many trades entered on Stage 7 signal |
| `exit_reasons` | Breakdown: stop_hit / stop_gap / stage9_exit / time_stop |

---

### 12g. Validated Results


#### Smoke Test (5 Tickers, Baseline Config)
*Run: `baseline_v1_20260224_2149` — gate disabled, ATR 2×, 1% risk, no overlaps*

| Ticker | Trades | Win% | Profit Factor | Expectancy R | Net Return |
|---|---|---|---|---|---|
| AAPL | 24 | 58% | 2.89 | 0.40 | +9.2% |
| MSFT | 25 | 44% | 1.18 | 0.07 | +1.6% |
| NVDA | 22 | 41% | **3.46** | **0.92** | +19.7% |
| JPM | 25 | 52% | 2.26 | 0.36 | +9.1% |
| XOM | 12 | 25% | 0.91 | 0.003 | −0.6% |


#### Full Universe Run (2,582 Tickers, Baseline Config)
*Run: `universe_baseline_v1_20260224_2310` — gate disabled, ATR 2×, 1% risk, no overlaps*

**Aggregate statistics:**

| Metric | Value | Notes |
|---|---|---|
| Total trades | 44,008 | ~17 per ticker average over 4 years |
| Win rate | 36.2% | Expected — wide universe includes noisy small/mid caps |
| Expectancy R | **0.20** | Positive across full universe — edge confirmed |
| Profit factor | **1.36** | Gross wins exceed gross losses universe-wide |
| Avg hold days | 13.5 | Consistent with smoke test (12.6) |
| Stage 7 entries | 97.1% | Confirms Stage 7 is the real primary entry |

**Edge distribution across 2,582 tickers:**

| Segment | Count | % of Universe |
|---|---|---|
| Positive expectancy R | 1,324 | 51.3% |
| Profit factor > 2.0 (strong edge) | 436 | 16.9% |
| Profit factor < 0.8 (losers) | 936 | 36.2% |

**Profit factor percentiles:**
```
25th pct : 0.62   (losing quarter)
Median   : 1.03   (break-even middle)
75th pct : 1.62   (solid edge in top quarter)
```

**Expectancy R percentiles:**
```
25th pct : -0.21  (losing quarter)
Median   :  0.01  (near break-even middle)
75th pct :  0.25  (solid edge in top quarter)
```

**Key observations:**
- The strategy works on ~51% of the universe. The remaining 49% is noise or permanently broken companies.
- The bottom performers (0% win rate across 10-16 trades) are structurally declining companies where Stage 2 → Stage 7 fires mechanically but the "mean" keeps declining — no genuine reversion. This is the primary motivation for the spider gate filter and a future quality overlay.
- Universe-level max drawdown is meaningless at this stage — it is computed as if all 2,582 independent $10k accounts were one sequential $10k account. Real drawdown requires portfolio-level capital simulation (Phase 09C).
- The outlier at expectancy R = 617 (SSII) is a low-liquidity biotech with one exceptional move. Natural position sizing at 0.5% risk will automatically limit its impact in the portfolio.

---

### 12h. Phase 09C — Portfolio Simulation

**Validated design (locked before build):**

| Parameter | Value | Rationale |
|---|---|---|
| Capital pool | $100,000 | Clean research baseline |
| Risk per trade | 0.5% = $500 | Half of 09B to accommodate more concurrent positions |
| Max concurrent positions | None (uncapped) | Baseline: no cap, measure full opportunity set |
| Signal priority | Chronological | First signal in time order enters first |
| Sector exposure cap | None for baseline | Add after seeing baseline results |
| Spider gate | Disabled for baseline | Same as 09B — measure raw edge first |

**What 09C adds that 09B cannot produce:**
- One shared capital pool — all trades draw from and return to the same $100,000
- Position sizing uses *current* portfolio equity (fixed fractional — self-adjusting)
- Real concurrent position tracking — no more independent per-ticker accounts
- Real daily equity curve — one row per trading day across the full 4-year window
- Real portfolio-level drawdown, Sharpe, Sortino, Calmar
- Sector exposure snapshots — daily breakdown of capital by spider

**Outputs (Phase 09C):**
```
output/backtests/<run_tag>/portfolio/
    portfolio_equity.parquet     — daily equity curve
    portfolio_trades.parquet     — all trades with portfolio-level context
    portfolio_report.json        — real risk-adjusted metrics
    positions_log.parquet        — daily open positions snapshot
    sector_exposure.parquet      — daily capital by spider
```

**Research sequence after 09C baseline:**
1. Baseline: no cap, no gate, 0.5% risk — measure full opportunity set
2. Add spider gate — measure gate attribution (how much does macro filter add?)
3. Add max position cap (e.g. 20) — measure concentration vs diversification tradeoff
4. Add sector cap (e.g. 25% per spider) — measure sector risk reduction cost
5. Compare `overlap_mode: scale_in` vs `disabled` at portfolio level
6. Stage-by-stage attribution — Stage 6 vs Stage 7 edge comparison
7. Stop sensitivity — ATR multiplier 1.5 vs 2.0 vs 3.0
8. Time stop sensitivity — 30 vs 45 vs 60 vs 90 days

---

### 12i. Sensitivity Research Roadmap

Every experiment below requires only a change to `config/backtest.yaml` followed by re-running `09B` or `09C` — no code changes required.

| Experiment | Config Change | Research Question |
|---|---|---|
| Gate attribution | `spider_gate.enabled: true` | Does macro filter add or destroy edge? |
| Stop tightening | `atr_multiplier: 1.5` | Do tighter stops improve or hurt expectancy? |
| Stop widening | `atr_multiplier: 3.0` | Do wider stops capture more trend? |
| Time stop short | `time_stop_days: 30` | Does faster exit improve capital efficiency? |
| Time stop long | `time_stop_days: 90` | Do longer holds capture more of the move? |
| Scale-in | `overlap_mode: scale_in` | Does pyramiding into confirmed trends add alpha? |
| No Stage 2 gate | `require_stage2_history: false` | How much does dislocation requirement add? |
| Stage 7 only | `entry_stages: [7]` | Is Stage 6 entry adding or subtracting edge? |

---

### 12j. Phase 09D — Universe Filter & Enriched Report

**Purpose:** Identify the investable subset of the universe by filtering out tickers where the strategy has no statistical edge, and enrich every ticker with sector information for downstream analysis.

**Filter logic (all conditions must pass):**

| Filter | Default | Description |
|---|---|---|
| `min_trades` | 5 | Minimum trades for statistical validity |
| `min_profit_factor` | 1.0 | Gross wins must exceed gross losses |
| `min_expectancy_r` | 0.0 | Average R per trade must be positive |
| `max_drawdown_pct` | -60.0% | Reject deeply broken tickers |
| `min_win_rate_pct` | 0.0 | Optional win rate floor |

All thresholds are configurable in `config/backtest.yaml` under `filter:`.

**Run:**
```powershell
python research/experiments/09D_filter_enrich_report.py
```

**Outputs — `output/reports/universe_filter/`:**

| File | Description |
|---|---|
| `universe_filter_report.xlsx` | 4-sheet Excel: Full Universe / Passing / Rejected / Settings |
| `filtered_tickers.csv` | Passing tickers list — input for 09E |
| `rejected_tickers.csv` | Full metrics for all rejected tickers |
| `rejected_tickers.xlsx` | Formatted standalone Excel for rejected tickers |
| `summary_enriched.csv` | Full universe with sector column added |
| `filter_report.json` | Summary + config snapshot (audit trail) |

**Filter results (baseline run — `universe_baseline_v1_20260224_2310`):**

| Metric | Value |
|---|---|
| Total tickers | 2,582 |
| Passed | 1,251 (48.5%) |
| Rejected | 1,331 (51.5%) |

**Passing tickers by sector:**

| Sector | Tickers | Avg Profit Factor | Avg Expectancy R |
|---|---|---|---|
| Financials | 320 | 1.93 | 0.332 |
| Healthcare | 214 | **4.94** | **3.337** |
| Industrials | 193 | 2.02 | 0.342 |
| Technology | 180 | 2.08 | 0.420 |
| Consumer Discretionary | 122 | 1.85 | 0.284 |
| Materials | 53 | 2.20 | 0.382 |
| Consumer Staples | 52 | 2.26 | 0.394 |
| Energy | 50 | 2.01 | 0.319 |
| Communication Services | 42 | 1.79 | 0.329 |
| Utilities | 25 | 1.83 | 0.268 |

Healthcare's outlier expectancy R (3.337) is driven by biotech/pharma names with occasional large mean-reversion moves post-dislocation. Statistically meaningful but with high variance.

---

### 12k. Phase 09E — Time-Batched Investor Report

**Purpose:** Split trades into configurable time windows using entry-date attribution, compute per-batch and combined metrics, and produce investor-ready Excel with per-ticker sector breakdown.

**Batch methodology (entry-date attribution):**
Each trade belongs to the batch in which it *entered*. A trade entered in Batch 1 that exits in Batch 2 is counted in Batch 1. Trades are never force-closed. This is the standard fund reporting approach — you measure the vintage of each entry decision.

**Run:**
```powershell
python research/experiments/09E_batched_report.py
```

**Outputs — `output/reports/batched/`:**

| File | Description |
|---|---|
| `investor_report.xlsx` | 4-sheet Excel: Summary / Batch 1 Tickers / Batch 2 Tickers / Combined |
| `batch_1_trades.parquet` | All trades with entry in Batch 1 window |
| `batch_2_trades.parquet` | All trades with entry in Batch 2 window |
| `batch_1_ticker_summary.csv` | Per-ticker metrics for Batch 1 |
| `batch_2_ticker_summary.csv` | Per-ticker metrics for Batch 2 |
| `combined_ticker_summary.csv` | Per-ticker metrics aggregated across both batches |
| `batch_report.json` | Full metrics + config snapshot (audit trail) |

**Validated batch results (filtered universe, 1,251 tickers):**

| Metric | Batch 1 (2022–2023) | Batch 2 (2024–2026) | Combined |
|---|---|---|---|
| Trades | 8,273 | 13,509 | 21,782 |
| Unique tickers | 1,201 | 1,251 | 1,251 |
| Win rate | **43.6%** | 42.8% | 43.1% |
| Profit factor | **3.07** | 1.71 | **2.26** |
| Expectancy R | **1.17** | 0.30 | **0.63** |
| Net PnL ($10k/ticker) | $853,737 | $424,446 | $1,278,183 |

**Key interpretation:**
Batch 1 significantly outperforms Batch 2. The 2022 Fed-driven dislocation created the deepest mean-reversion setups in a decade. Batch 2 (bull market) shows the strategy still works with reduced but positive edge — profit factor 1.71 is solid in a trending environment with fewer genuine dislocations.

**Config for batch windows (`config/backtest.yaml`):**
```yaml
batches:
  source_run_tag: "universe_baseline_v1_20260224_2310"
  use_filtered_tickers: true
  windows:
    - name: "Batch 1 — 2022 to 2023"
      start: "2022-01-01"
      end:   "2023-12-31"
    - name: "Batch 2 — 2024 to 2026"
      start: "2024-01-01"
      end:   "2026-12-31"
```

---

### 12l. Scenarios Testing Guide

All scenarios below change only `config/backtest.yaml`. No code changes required.
For each scenario: make the config change → run the listed commands in order.

**Config audit trail:** Every 09B run saves `backtest_config_snapshot.yaml` in its output folder. Every 09D/09E run saves `config_snapshot` in its JSON report. You always know exactly what config produced each result.

---

#### Scenario Group 1 — Stop Loss Sensitivity

*Research question: Does ATR multiplier significantly affect edge? Is there an optimal stop width?*

**How to run each variant:**
```yaml
# In config/backtest.yaml, change atr_multiplier and run_tag_prefix for each:
stop:
  atr_multiplier: 1.5     # Tight stops
  # atr_multiplier: 2.0   # Baseline (already done)
  # atr_multiplier: 3.0   # Wide stops
```
```powershell
# For each variant:
python research/experiments/09B_run_backtest.py
python research/experiments/09D_filter_enrich_report.py
python research/experiments/09E_batched_report.py
```

| Variant | `run_tag_prefix` | Expected effect |
|---|---|---|
| Tight stop (1.5×) | `"stop_tight_v1"` | More stop-outs, lower win %, potentially higher expectancy if wins are large |
| Baseline (2.0×) | `"universe_baseline_v1"` | Already done |
| Wide stop (3.0×) | `"stop_wide_v1"` | Fewer stop-outs, larger losses when wrong, potentially larger winners |

---

#### Scenario Group 2 — Spider Gate Attribution

*Research question: How much does the macro sector filter add or cost in terms of edge?*
```yaml
spider_gate:
  enabled: true    # flip from false
```
```yaml
run:
  run_tag_prefix: "gate_enabled_v1"
```
```powershell
# 09A does NOT need to be re-run — gate info is already in signals file
python research/experiments/09B_run_backtest.py
python research/experiments/09D_filter_enrich_report.py
python research/experiments/09E_batched_report.py
```

Compare the batched results against `universe_baseline_v1`. The delta in trade count, win rate and expectancy R IS the gate attribution.

---

#### Scenario Group 3 — Overlap Mode (Scale-In vs Disabled)

*Research question: Does pyramiding into confirmed trends add alpha or just add risk?*
```yaml
sizing:
  overlap_mode: "scale_in"
  max_scale_ins: 2
```
```yaml
run:
  run_tag_prefix: "scalein_v1"
```
```powershell
# 09A does NOT need to be re-run
python research/experiments/09B_run_backtest.py
python research/experiments/09D_filter_enrich_report.py
python research/experiments/09E_batched_report.py
```

---

#### Scenario Group 4 — Time Stop Sensitivity

*Research question: What is the optimal maximum holding period?*

Run four variants changing only `time_stop_days`:

| Variant | `time_stop_days` | `run_tag_prefix` |
|---|---|---|
| Short hold | `30` | `"timestop_30_v1"` |
| Medium hold | `45` | `"timestop_45_v1"` |
| Baseline | `60` | Already done |
| Long hold | `90` | `"timestop_90_v1"` |
```powershell
# For each variant (09A not needed):
python research/experiments/09B_run_backtest.py
python research/experiments/09D_filter_enrich_report.py
python research/experiments/09E_batched_report.py
```

---

#### Scenario Group 5 — Portfolio Capital & Risk Sizing

*Research question: What is the optimal capital deployment at portfolio level?*

For each variant, change the `portfolio:` section only and re-run 09C:

| Variant | `capital` | `risk_pct` | `max_positions` | Description |
|---|---|---|---|---|
| A — Reference | $100k | 0.5% | null | Uncapped baseline |
| B — Capped 20 | $100k | 0.5% | 20 | Concentrated portfolio |
| C — Capped 30 | $100k | 0.5% | 30 | Diversified portfolio |
| D — Lower risk | $100k | 0.25% | null | Conservative sizing |
```powershell
# For each variant (no 09A or 09B re-run needed):
python research/experiments/09C_run_portfolio.py
```

---

#### Scenario Group 6 — Dislocation Requirement (Stage 2 Gate)

*Research question: How much does the mandatory Stage 2 prerequisite contribute to edge?*
```yaml
signal:
  require_stage2_history: false   # remove dislocation requirement
```
```yaml
run:
  run_tag_prefix: "no_stage2_req_v1"
```
```powershell
# 09A MUST be re-run for this — it changes signal detection logic
python research/experiments/09A_generate_raw_signals.py
python research/experiments/09B_run_backtest.py
python research/experiments/09D_filter_enrich_report.py
python research/experiments/09E_batched_report.py
```

Compare expectancy R and profit factor vs baseline. The delta tells you exactly how much the dislocation requirement contributes to edge.

---

#### Recommended Testing Order for Investor Presentation

Run these in sequence for a complete evidence base:
```powershell
# 1. Already done — baseline
# universe_baseline_v1_20260224_2310

# 2. Gate attribution (next priority — single config flip)
# Edit: spider_gate.enabled: true, run_tag_prefix: "gate_enabled_v1"
python research/experiments/09B_run_backtest.py
python research/experiments/09D_filter_enrich_report.py
python research/experiments/09E_batched_report.py

# 3. Tight stop sensitivity
# Edit: atr_multiplier: 1.5, run_tag_prefix: "stop_tight_v1"
python research/experiments/09B_run_backtest.py
python research/experiments/09D_filter_enrich_report.py
python research/experiments/09E_batched_report.py

# 4. No Stage 2 requirement (shows value of dislocation thesis)
# Edit: require_stage2_history: false, run_tag_prefix: "no_stage2_req_v1"
python research/experiments/09A_generate_raw_signals.py
python research/experiments/09B_run_backtest.py
python research/experiments/09D_filter_enrich_report.py
python research/experiments/09E_batched_report.py
```

These four runs give you the cleanest investor narrative: baseline edge → macro filter impact → stop optimisation → why the dislocation requirement matters.

# 13. Historical Window Refresh Protocol *(2021–2026)*

This section documents the correct reset + rebuild order when expanding the historical window.

New raw parquets will live in: `ROOT\data\raw\prices_daily\twelvedata\parquets`

- **Currently Running:**
  1. 06 - fetch twelvedata OHLCV
     - for additional data
       - `TD_START_DATE=2021-01-01` 
       - `TD_END_DATE=2026-02-01`
       - Expected last trading day gate: `TD_EXPECTED_LAST_DATE=2026-01-30`
       - Full-history threshold: `TD_MIN_ROWS_OK=1200`
  2. 06B - audit for downloaded data
  3. 06C - retry for stocks missed in batches
     - mainly because misaligned API documentation
     - hence retry 1-by-1 to collect remaining ones

### a. After Full 4-Year OHLCV Fetch

When the laptop fetch is complete and files are copied to the work machine:

#### Step 1 — Delete Derived Layers Only (DO NOT delete raw parquets)

Delete/reset these derived folders (safe to rebuild any time):
```yaml
data/cleaned/spiders_daily/features/
data/cleaned/spiders_daily/stages/
data/cleaned/spiders_daily/gate/
data/cleaned/stocks_daily/features/
data/cleaned/stocks_daily/stages/
```

Delete/reset these restart logs (so a full rebuild actually re-runs everything):
```yaml
data/cleaned/stocks_daily/features/_progress.jsonl
data/cleaned/stocks_daily/features/_errors.jsonl
data/cleaned/stocks_daily/stages/_progress.jsonl
data/cleaned/stocks_daily/stages/_errors.jsonl
```

(If present) also clear spider build logs:
```
data/cleaned/spiders_daily/features/_progress.jsonl
data/cleaned/spiders_daily/features/_errors.jsonl
data/cleaned/spiders_daily/stages/_progress.jsonl
data/cleaned/spiders_daily/stages/_errors.jsonl
data/cleaned/spiders_daily/gate/_progress.jsonl
data/cleaned/spiders_daily/gate/_errors.jsonl
```

- **DO NOT delete/reset:**

```yaml
data/raw/prices_daily/twelvedata/parquets/
data/raw/spiders_daily/ (optional; can be rebuilt but no harm keeping)
data/metadata/spiders/
```

- **Recommended Order for progression:**

  1. 07A - build Spider memberships and Summary
     - optional to run for UI graphics - `zTester/03_spider_treemap.py`
  2. 07B - spiders OHLCV 
  3. 07C - spider features 
  4. 07D - classification of spider stages 
  5. 07G - building spider gates
  6. 08A - build stock features (creates `stocks_daily/features/*.parquet`)
  7. 07E - attach sector stage to stocks
  8. 08B - classification of stock stages

If stock stage classifier later wants sector stage as an input feature, then do 07E before 08B.
If sector stage is purely a gate during backtest/signal evaluation, it can be done after 08B.

### b. Rebuild Order (Strict Execution Order)

#### From project root:

```powershell
python research/experiments/07A_build_spider_memberships.py
python research/experiments/07B_build_spider_ohlcv_from_parquets.py
python research/experiments/07C_compute_spider_features.py
python research/experiments/07D_classify_spider_stages.py
python research/experiments/07G_build_spider_gate_daily.py
python research/experiments/08A_build_stock_features.py
python research/experiments/07E_attach_sector_stage_to_stocks.py
python research/experiments/08B_classify_stock_stages.py
```

### c. Why Full Rebuild Is Required

EMA200 uses 200 periods of warmup.

If earlier history changes, the entire indicator surface shifts.

Therefore:

- Spider features must be recomputed
- Spider stages must be recomputed
- Spider gate must be rebuilt
- Stock features must be recomputed
- Stock stages must be recomputed

Partial rebuilds are not valid when extending historical depth.

### c. Safety Notes

- All stage scripts are restart-safe.
- All feature builders are idempotent.
- Rebuild is purely local compute (no API usage).
- Deterministic outputs guaranteed.

--- 

## Optional Commands
### *(from root folder ALGO-STOCKS)*

- After running **07D - Classification of Spider Stages**
- Running these in order for testing purposes

```
python -c "import pandas as pd; from stages.stage_classifier import classify_stages; df=pd.read_parquet(r'data\cleaned\spiders_daily\features\SECTOR_FINANCIALS.parquet'); out=classify_stages(df=df, cfg={'stage_logic': {'require_breakout_before_inzone': True}}); print(sorted(out['stage'].unique())); print(out[out['stage'].isin([6,7])][['date','stage','stage_reason']].head(10))"
```
```
python -c "import pandas as pd; from stages.stage_classifier import classify_stages; df=pd.read_parquet(r'data\cleaned\spiders_daily\features\SECTOR_FINANCIALS.parquet'); out=classify_stages(df=df, cfg={'stage_logic': {'require_breakout_before_inzone': True}}); print(out['stage'].value_counts().sort_index())"
```
```
python -c "import pandas as pd; df=pd.read_parquet(r'data\cleaned\spiders_daily\features\SECTOR_FINANCIALS.parquet'); print(df.columns.tolist()); print(df[['date','close','high','low','volume']].tail(10))"
```
```
python -c "import pandas as pd; df=pd.read_parquet(r'data\cleaned\spiders_daily\stages\SECTOR_FINANCIALS.parquet'); print(df.tail(5))"
```

- After running **08B - Classification of Stock Stages**
- Running these in order for testing purposes

```
python -c "import pandas as pd; df=pd.read_parquet(r'data\cleaned\stocks_daily\stages\AAPL.parquet'); print(df['stage'].value_counts().sort_index())"
```

- And (optional) check “stage 6 exists in sample”:
```
python -c "import pandas as pd, glob; paths=glob.glob(r'data/cleaned/stocks_daily/stages/*.parquet'); hit=0; 
for p in paths[:500]:
    df=pd.read_parquet(p, columns=['stage'])
    hit += int((df['stage']==6).any())
print('tickers_with_stage6_in_first500=', hit)"
```

---

***End of Project Documentation***