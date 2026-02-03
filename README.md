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

## 10. Project Status

**Current Phase**  
- Strategy design & research scaffolding

**Next Steps**
- Formalise stage definitions
- Define backtest assumptions
- Implement feature pipeline
- Begin controlled experiments

---
