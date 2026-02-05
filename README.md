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

Each stock is classified daily into **one of nine market stages**, based on price structure and trend behaviour:

1. Not Eligible  
2. Sharp Downtrend  
3. Downtrend  
4. Below Zone  
5. Lower Zone  
6. Breakout  
7. Breakout Confirmed  
8. In-Zone  
9. In-Zone (Fading)

### Indicators Used
- Donchian Channels (primary trigger)
- Bollinger Bands
- EMA stacks (EMA10 / EMA20 / EMA50 / EMA200)
- Volume behaviour
- Momentum indicators (confirmation only)

### Purpose
Market stages determine:
- Whether a stock is tradable
- When entries are allowed
- How positions are managed
- When exits are triggered

---

## 5. Sector “Spiders” (Macro Filter)

Stocks are grouped into normalized **sector baskets (“spiders”)**.

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

All filters and overlays are **toggleable** so their impact can be measured objectively.

---

## 7. Project Structure

ALGO-STOCKS/
│
├── data/ # Raw and cleaned datasets
│ ├── raw/ # Provider-native snapshots (Finviz, Nasdaq, etc.)
│ ├── cleaned/ # Normalized, research-safe datasets
│ └── metadata/ # Sector maps, exclusions, schema registry
│
├── features/ # Feature engineering
│ ├── technicals/ # EMA, Bollinger, Donchian, volume, momentum
│ └── spiders/ # Sector basket construction & regimes
│
├── stages/ # Market stage logic
│ ├── stage_definitions.md
│ ├── stage_classifier.py
│ └── stage_transitions.py
│
├── filters/ # Eligibility & overlay filters
│ ├── static_gates.py
│ ├── spider_gate.py
│ └── analyst_overlay.py
│
├── signals/ # Entry / exit logic
│ ├── entry_engine.py
│ ├── exit_engine.py
│ └── asymmetry_metrics.py
│
├── portfolio/ # Portfolio construction & constraints
│ ├── sizing.py
│ ├── constraints.py
│ └── rebalance.py
│
├── backtest/ # Backtesting & analysis
│ ├── engine.py
│ ├── metrics.py
│ ├── attribution.py
│ └── regime_analysis.py
│
├── research/ # Notebooks, experiments, reports
│
├── config/ # YAML-based strategy configuration
│ ├── indicators.yaml
│ ├── stages.yaml
│ ├── spiders.yaml
│ └── portfolio.yaml
│
├── run.py # Entry point / orchestration
├── requirements.txt
└── README.md


---

## 8. Configuration-Driven Design

All strategy assumptions are externalized via **YAML configuration**:

- Indicator settings
- Stage thresholds
- Sector (“spider”) rules
- Portfolio constraints

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

## 10. Project Status (as of 2026-02-05)

### Current Phase
**Data Foundation + Research Scaffolding.**

We have locked in a reproducible, replayable universe build process and validated the Twelve Data ingestion path using a single-ticker test (AAPL).

### Completed Milestones (so far)

**Universe construction (Finviz)**
- ✅ Stage 1: Finviz raw export capture (audit-safe, schema logged)
- ✅ Stage 2: Promote raw → cleaned snapshot (traceable baseline)
- ✅ Stage 3: Contract dataset creation (typed numeric fields, no drops)
- ✅ Stage 4: Trade-ready universe filtering (policy layer)

**Trade-ready universe (latest)**
- ✅ Filters applied:
  - `country == USA`
  - `market_cap >= 300M`
  - REIT exclusions enabled (sector/industry rules)
- ✅ Resulting universe size:
  - `rows_after = 2835` tickers (from 10892 total Finviz rows)

**Twelve Data validation**
- ✅ Single-ticker OHLCV test (AAPL) confirmed correct window coverage:
  - `start_date = 2023-01-01`
  - `end_date   = 2026-02-01`
  - `rows = 772` daily bars (trading days)
  - first/last dates align with US trading calendar

### In Progress (today)
- ⏳ Stage 6: Full-universe daily OHLCV ingestion (Twelve Data, free/basic plan)
  - Outputs per-ticker Parquet under:
    - `data/raw/prices_daily/twelvedata/{TICKER}.parquet`
  - Resumable logging:
    - `data/raw/prices_daily/twelvedata/_progress.jsonl`
    - `data/raw/prices_daily/twelvedata/_errors.jsonl`

---

## 11. Operational Notes (Important)

### Twelve Data free/basic limits
Twelve Data free/basic tier is rate/credit limited. The ingestion pipeline is designed to be:
- resumable across multiple days
- safe to interrupt and restart
- able to continue from last completed ticker

### Partial-history risk (to be hardened)
If an API response returns a truncated series for a ticker, the current ingestion script may still write the file.
A robustness patch will be added to:
- validate minimum expected row count per ticker
- mark incomplete downloads as `partial` and retry later
- only mark tickers as `ok` once coverage meets threshold

---

## 12. Next Steps (Locked Order)

### Stage 6 — Complete OHLCV ingestion (3+ years)
- Finish fetching daily OHLCV for all 2,835 tickers using Twelve Data.
- Confirm ingestion quality via:
  - random Parquet → CSV spot checks
  - missing-date/row-count audits
  - error + partial retry loop
- Stage 6 now writes status per ticker:
  - `status=ok` only if `rows >= TD_MIN_ROWS_OK` and `last_date >= TD_EXPECTED_LAST_DATE`
  - otherwise `partial` (auto-retried on rerun) 
- `_progress.jsonl` contains `first_date/last_date/rows` so completeness is auditable
- Audit script: *06B_audit_twelvedata_downloads.py* produces partial/missing lists.

### Stage 7 — Feature engineering (local compute)
Compute indicators locally (to reduce API credits and ensure reproducibility):
- EMA: 10, 20, 50, 100, 200
- Donchian: 20-day high/low (offset 0)
- Bollinger Bands: 20 length, 2 std dev (offset 0)
- MACD: (12, 26, 9)

Then trim to the strategy window:
- keep last 2 years for backtesting datasets
- retain the full 3y+ raw set for warmup integrity

### Stage 8 — Analyst overlay (research-only)
- Add analyst ratings / target price overlay (starting with Yahoo-based fields)
- Keep as a toggleable filter layer (do not hard-block trades until validated)

### Stage 9 — Stage classifier + backtest harness
- Formalise stage definitions & transitions
- Implement daily stage classification per ticker
- Build backtesting engine + reporting

