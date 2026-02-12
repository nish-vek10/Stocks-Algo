# ALGO-STOCKS  
### Long-Only, Mean-Reversion Equity Research & Backtesting Framework

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

```yaml
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
09 - Backtest harness + reporting
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

## 10. Project Status *(as of 2026-02-11)*

### Current Phase
- **Data foundation + spiders scaffold complete. Indicators are now canonical and ready for feature engineering.**
- **4-year OHLCV ingestion (2022–2026) running on secondary machine**
- **Full spider pipeline complete (OHLCV → features → stages → gate)**
- **08A Stock Feature Engineering complete for full universe**
- **Ready to run 08B stock stage classification**
- **System stable and deterministic**

The framework is now feature-complete through macro gating and ready for full historical stage classification across all equities.

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
  - `start_date = 2023-01-01`
  - `end_date   = 2026-02-01`
  - `rows = 772` daily bars (trading days)
  - first/last dates align with US trading calendar
- ✅ Multi-ticker batch ingestion validated
- ✅ Restart-safe progress tracking implemented
- ✅ Credit-aware batch throttling confirmed
- ✅ No silent failures observed (`_errors.jsonl` remains empty)

**Stage 6 — OHLCV ingestion (Twelve Data)**
- ✅ Daily OHLCV collected for **2,831 / 2,835** tickers
- ✅ 4 permanently excluded due to provider symbol mismatch (documented)
- ✅ Restart-safe progress tracking + credit-aware throttling
- ✅ Audit scripts for missing/partial/error handling

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

## 12. Next Steps (Locked Order)

### Stage 7B / 07C — Spider feature engineering (local compute)
Compute indicators locally for each spider series (for regime/stage classification):
- **EMA:** 10, 20, 50, 100, 200
- **Donchian:** 20-day high/low
- **Bollinger Bands:** 20 length, 2 std dev
- Volume avg + surge flag
- **Optional:** MACD / RSI (computed, used only if needed)

Target outputs:
- `data/cleaned/spiders_daily/features/{SPIDER_ID}.parquet`

### Stage 7C / 07D — Spider stage classification (optional but recommended)
Run the same stage classifier logic on spiders to produce spider regimes:
- `data/cleaned/spiders_daily/stages/{SPIDER_ID}.parquet`

### Stage 7D+ — Spider gate (macro permission layer)
Implement:
- `filters/spider_gate.py`

### Stage 7D+ — Spider gate (macro permission layer) **(NEXT)**
Implement `filters/spider_gate.py` to act as a macro permission layer for stock signals.

Planned outputs:
- A per-day spider regime/state lookup for each sector spider
- A callable gate used by the signal engine:
  - `is_spider_allowed(spider_id, date, rules) -> bool`
  - optional: `spider_risk_multiplier(spider_id, date) -> float`

### Stage 8 — Stock feature engineering (local compute)
Apply the same indicator pipeline to each ticker OHLCV and write:
- `data/cleaned/stocks_daily/features/{TICKER}.parquet`

### Stage 8B — Stock stage classification (daily)
Run the shared stage classifier across each ticker:
- `data/cleaned/stocks_daily/stages/{TICKER}.parquet`

### Stage 9 — Backtest harness + research reporting
- Portfolio simulation
- Stage-by-stage performance attribution
- Sensitivity testing (Donchian window, EMA spans, stop policies, spider gate strictness)

---

# 13. 4-Year Data Refresh Protocol *(2022–2026)*

This section documents the correct reset + rebuild order when expanding the historical window.

New raw parquets will live in: `ROOT\data\raw\prices_daily\twelvedata\parquets`

- **Currently Running:**
  1. 06 - fetch twelvedata OHLCV
     - for additional data
       - `TD_START_DATE=2022-01-01` 
       - `TD_END_DATE=2026-02-01`
  2. 06B - audit for downloaded data
  3. 06C - retry for stocks missed in batches
     - mainly because misaligned API documentation
     - hence retry 1-by-1 to collect remaining ones

### a. After Full 4-Year OHLCV Fetch

When the laptop fetch is complete and files are copied to the work machine:

#### Step 1 — Delete Derived Layers Only (DO NOT delete raw parquets)

We need to delete/reset the following folders:
```yaml
data/cleaned/spiders_daily/features/
data/cleaned/spiders_daily/stages/
data/cleaned/spiders_daily/gate/
data/cleaned/stocks_daily/features/
data/cleaned/stocks_daily/stages/
```
```yaml
data/cleaned/stocks_daily/features/_progress.jsonl
data/cleaned/stocks_daily/features/_errors.jsonl
data/cleaned/stocks_daily/stages/_progress.jsonl
data/cleaned/stocks_daily/stages/_errors.jsonl
```

- **DO NOT delete/reset:**

```yaml
data/raw/prices_daily/twelvedata/parquets/
data/metadata/spiders/
```

- **Recommended Order for progression:**

  1. 07A - build Spider memberships and Summary
     - optional to run for UI graphics - `zTester/03_spider_treemap.py`
  2. 07B - spiders OHLCV 
  3. 07C - spider features 
  4. 07D - spider stages 
  5. 08A - stock features ✅ (creates `stocks_daily/features/*.parquet`)
  6. 07E - attach sector stage to stocks ✅ 
  7. 08B - stock stages (optional whether before/after 07E — both fine)

If stock stage classifier later wants sector stage as an input feature, then do 07E before 08B.
If sector stage is purely a gate during backtest/signal evaluation, it can be done after 08B.

### b. Rebuild Order (Strict Execution Order)

#### From project root:

```
python research/experiments/07B_build_spider_ohlcv.py
python research/experiments/07C_build_spider_features.py
python research/experiments/07D_classify_spider_stages.py
python research/experiments/07G_build_spider_gate_daily.py
python research/experiments/08A_build_stock_features.py
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

```
python -c "import pandas as pd, glob; import os; 
paths=glob.glob(r'data/cleaned/stocks_daily/stages/*.parquet'); 
hit=0
for p in paths[:500]:
    df=pd.read_parquet(p, columns=['stage'])
    if (df['stage']==6).any(): hit+=1
print('tickers_with_stage6_in_first500=', hit)"
```

---

***End of Project Documentation***