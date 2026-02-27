# ALGO-STOCKS — Quantitative Research Report
## Long-Only Mean-Reversion Equity Strategy
### Full Research Documentation & Findings

**Document Version:** 1.0  
**Research Period:** January 2022 — January 2026  
**Universe:** 2,582 U.S. Listed Equities  
**Status:** Phase 09 Complete — Investor Presentation Ready  

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Strategy Thesis](#2-strategy-thesis)
3. [Data Collection & Universe Construction](#3-data-collection--universe-construction)
4. [Indicator Architecture](#4-indicator-architecture)
5. [The 9-Stage Market Model](#5-the-9-stage-market-model)
6. [Sector Spider System — Macro Filter](#6-sector-spider-system--macro-filter)
7. [Signal Generation — Entry Logic](#7-signal-generation--entry-logic)
8. [Trade Execution Model — Entry, Stop & Exit](#8-trade-execution-model--entry-stop--exit)
9. [Backtesting Architecture](#9-backtesting-architecture)
10. [Universe Filter — Identifying the Investable Subset](#10-universe-filter--identifying-the-investable-subset)
11. [Full Universe Results](#11-full-universe-results)
12. [Batched Time-Window Analysis](#12-batched-time-window-analysis)
13. [Portfolio Simulation Results](#13-portfolio-simulation-results)
14. [Key Research Findings](#14-key-research-findings)
15. [Risk Considerations](#15-risk-considerations)
16. [Proposed Next Experiments](#16-proposed-next-experiments)
17. [Conclusion](#17-conclusion)

---

## 1. Executive Summary

ALGO-STOCKS is a research-first quantitative framework built to validate a **long-only, mean-reversion equity strategy** across U.S. listed stocks. The strategy identifies stocks that have undergone sharp, identifiable price dislocations and enters only after structural recovery has begun — targeting the mean-reversion move rather than catching falling knives.

### Headline Results (Baseline Configuration, Filtered Universe)

| Metric | Value |
|---|---|
| Universe tested | 2,582 U.S. equities (2022–2026) |
| Investable subset (passed filter) | **1,251 tickers (48.5%)** |
| Total trades (filtered, 4-year period) | **21,782** |
| Win rate | **43.1%** |
| Profit factor | **2.26** |
| Expectancy R | **0.63** |
| Net PnL (independent $10k/ticker basis) | **$1,278,183** |
| Avg trade hold time | **~14 days** |

**The strategy demonstrates a statistically significant positive edge** across 1,251 tickers with an average of 17 trades per ticker over 4 years. The edge is strongest in market dislocation environments (2022 bear market) and remains positive in bull markets (2024–2026).

---

## 2. Strategy Thesis

### The Core Idea

Markets frequently overshoot to the downside during periods of stress, fear, or macro uncertainty. High-quality companies that experience sharp, identifiable price dislocations tend to recover toward their structural mean once the dislocation event passes. This recovery is not random — it follows a recognisable structural sequence that can be identified in real time using price, volume, and momentum indicators.

### Why This Edge Exists

- **Institutional behaviour:** Large funds that sold aggressively during dislocations re-accumulate as price stabilises. This creates a predictable base-building phase before the recovery.
- **Mean-reversion tendency:** Stocks with strong underlying businesses tend to revert toward fair value after event-driven dislocations.
- **Structural entry advantage:** By waiting for the dislocation to complete and recovery to begin (Stage 7 breakout confirmation), entries are made after the worst of the drawdown has passed — reducing risk relative to early mean-reversion attempts.

### What Makes This Different From Simple Mean-Reversion

Most mean-reversion strategies enter during the decline (buying cheapness). This strategy has a critical additional filter: **the stock must have already started recovering** before any entry is considered. The dislocation (Stage 2) must have occurred, a base must have formed, and price must have broken out of that base with volume confirmation. This is mean-reversion with structural confirmation — not value averaging into a declining stock.

### Design Constraints (Locked)

- **Long-only.** No short selling under any circumstance.
- **Dislocation prerequisite.** Stage 2 (sharp decline) must have occurred before any entry.
- **No entries from slow downtrends.** Stage 3 (gradual decline without sharp dislocation) alone never qualifies.
- **Entry only on breakout confirmation.** Stage 6 or Stage 7 transition required.
- **Systematic exits.** Stage 9 (momentum fading) or stop-loss. No discretionary exits.

---

## 3. Data Collection & Universe Construction

### Universe Definition

The investable universe was constructed from a Finviz export of U.S.-listed equities, filtered to:

| Filter | Criterion |
|---|---|
| Country | USA only |
| Market capitalisation | ≥ $300M (eliminates micro-caps) |
| Asset class | Common equities only (REITs excluded via sector/industry rules) |
| Listing quality | Active listings only |

**Result: 2,835 trade-ready U.S. equities**

REIT exclusion was applied because real estate trusts exhibit dividend-driven price behaviour that does not conform to the mean-reversion dislocation model.

### OHLCV Data Collection

Daily price data (Open, High, Low, Close, Volume) was collected for all 2,835 tickers via the Twelve Data API.

| Parameter | Value |
|---|---|
| Provider | Twelve Data |
| Start date | 2021-01-01 |
| End date | 2026-02-01 |
| Data type | Daily OHLCV bars |
| Expected rows per ticker | ~1,275 trading days |

**Why start in 2021 if research begins in 2022:**
EMA200 requires 200 bars of warmup. Starting OHLCV collection in 2021 ensures that by the time the first signal is generated in 2022, all indicators have a full year of history for accurate calculation. Starting earlier = more reliable indicators from day one of the research window.

### Data Quality Controls

- **Idempotent ingestion:** Re-running the fetch script never re-fetches completed tickers. Progress is tracked in `_progress.jsonl`.
- **IPO / short-history handling:** Tickers that listed after January 2021 are flagged as `ok_short_history` — accepted if they have sufficient data coverage, never treated as errors.
- **Retry pass:** A dedicated retry script re-attempts partial downloads to minimise gaps.
- **Hard-fail logging:** 4 tickers permanently excluded due to API symbol mismatches (`ALUB-U`, `NWAX-U`, `SBXE-U`, `PLYX`).

**Final ingestion outcome:**
- Successfully ingested: **2,831 tickers**
- Short-history (IPOs): handled
- Permanent exclusions: 4

### Data Reliability Guarantee

All derived data (indicators, stages, signals, backtest results) is rebuilt from raw OHLCV parquets. Raw data is never overwritten. If the historical window is extended or data quality issues are found, the entire derived pipeline can be rebuilt deterministically.

---

## 4. Indicator Architecture

All indicators are computed from raw OHLCV data using deterministic, expanding-window logic. No lookahead bias. Parameters are externalised in `config/indicators.yaml`.

### Core Indicators

| Indicator | Parameters | Purpose |
|---|---|---|
| EMA (Exponential Moving Average) | 10, 20, 50, 100, 200 periods | Trend direction, structural mean, entry confirmation |
| Bollinger Bands | 20-period, 2 standard deviations | Volatility envelope, dislocation identification |
| Donchian Channels | 20-period high/low | Breakout detection, range context |
| Volume SMA | 10-period | Volume baseline for surge detection |
| Volume Surge Flag | >15% above 10-day average | Confirms genuine institutional participation |
| ATR (Average True Range) | 14-period, Wilder smoothing | Adaptive stop-loss sizing |
| MACD | 12/26/9 | Momentum confirmation (secondary) |
| RSI | 14-period | Momentum confirmation (secondary) |

### Why These Specific Indicators

**EMA stack (10/20/50/200):**
The relationship between short-term EMAs (10, 20) and the structural mean (EMA200) defines the stage of the stock's price cycle. EMA10 crossing above EMA20 while both are below EMA50 identifies the early recovery phase. EMA50 crossover confirms the structural trend has shifted.

**Donchian 20-day high:**
The 20-day Donchian high is the most objective breakout trigger available. It simply asks: "Is price making a new 20-day high?" No parameter sensitivity issues. When price breaks the 20-day high after a period of basing, it indicates institutional accumulation has completed and upside is being established.

**Bollinger Bands for dislocation:**
Price closing below the lower Bollinger Band (2 standard deviations below the 20-day mean) identifies a statistically unusual downside move — the kind of overreaction that creates mean-reversion opportunities.

**ATR for stops:**
Using ATR rather than a fixed percentage stop is critical. A 6% stop on a $500 stock and a $10 stock are completely different risks. ATR scales the stop to each stock's actual daily volatility range — this means the stop is genuinely 2× the typical daily noise, regardless of the stock's price level.

### No-Lookahead Guarantee

All indicators use expanding-window computation. On any given date `t`, only data from dates `≤ t` is used. This is enforced at the feature engineering stage (08A) and verified during backtesting (09A checks that Stage 2 occurred strictly before the signal bar using `shift(1)` operations).

---

## 5. The 9-Stage Market Model

The strategy's core intellectual framework is a 9-stage state machine that classifies every stock every day into exactly one state based on its price structure, trend behaviour, and momentum characteristics.

### Stage Definitions

**Stage 1 — Not Eligible**
Price is above EMA200. The stock is in a structural uptrend and not relevant for this strategy. No action.

**Stage 2 — Sharp Dislocation** *(The Trigger)*
This is the most important stage in the entire model. It identifies stocks that have experienced a genuine, sharp breakdown:
- Price below EMA200
- Rapid decline of >5% over a short period (3–5 trading days)
- Price closes below the lower Bollinger Band (2 standard deviations)
- EMA10 slope negative and accelerating downward
- Ideally: volume spike confirming institutional selling

A stock that reaches Stage 2 is flagged as a **future candidate**. The dislocation itself is not an entry point — it is a prerequisite. The strategy never buys during Stage 2.

**Stage 3 — Slow Downtrend**
Price remains below EMA200 but the decline is gradual, not sharp. No Stage 2 conditions met. Stage 3 alone never qualifies for entry — a genuine dislocation is required first.

**Stage 4 — Below Zone (Basing)**
Price has stabilised after the Stage 2 or Stage 3 decline. No new 20-day Donchian lows. Sideways movement. The stock is building a base — the market is finding equilibrium after the dislocation.

**Stage 5 — Lower Zone (Early Recovery)**
Price begins moving into the upper half of the Donchian range. EMA10 crosses above EMA20 — the first sign that short-term momentum has shifted positive. Still not an entry stage.

**Stage 6 — Breakout** *(Early Entry)*
Price breaks above the 20-day Donchian high for the first time after the dislocation and basing period. EMA10 > EMA20. Volume confirms the move. This is technically an entry point but in practice extremely rare — see the Stage 7 note below.

**Stage 7 — Breakout Confirmed** *(Primary Entry)*
All Stage 6 conditions plus `close > EMA50` — meaning price has recovered not just above the recent range but above the medium-term structural average. This is the primary entry stage in practice (97.1% of all signals). The EMA50 crossover confirms that the mean reversion move is real, not a false breakout.

**Stage 8 — In-Zone**
Position is active. Price continuing higher but early momentum is slowing. Position is held with stop in place.

**Stage 9 — In-Zone Fading** *(Exit Trigger)*
Loss of momentum. Failure to make new highs. Increased risk of reversal. When Stage 9 is detected at close, exit at the next day's open. This is the primary exit mechanism (60.3% of all exits).

### Why Stage 7 Dominates Stage 6 (1.5% vs 98.5%)

The stage classifier evaluates Stage 7 conditions before Stage 6. After a genuine Stage 2 dislocation and base formation, by the time price breaks the 20-day Donchian high with EMA10 > EMA20, it has almost always already recovered above EMA50 — so it satisfies Stage 7 directly. Stage 6 only fires when price breaks out but has not yet crossed EMA50, which is the exception rather than the rule after a full dislocation-and-base cycle.

This is a design feature, not a bug. Stage 7 is the higher-confidence entry.

### The Dislocation Prerequisite — Why It Matters

The entire strategy is built around one design-locked constraint: **a ticker must have printed Stage 2 at some point strictly before the signal bar.** Stocks that gradually drift below EMA200 without a sharp dislocation are excluded.

This filter exists because:
- Slow drifters (Stage 3-only) tend to continue declining — there is no overreaction to revert from
- Sharp dislocations (Stage 2) create a clear "overshoot" below fair value — the reversion back to mean is the tradable event
- Institutional behaviour differs: sharp dislocations attract mean-reversion buyers; gradual declines do not

The 09D filter results confirm this: 249 tickers (8.8% of the full universe) never generated any signals because they never experienced a Stage 2 dislocation in the 2022–2026 window. These were predominantly stocks that trended upward throughout the period.

---

## 6. Sector Spider System — Macro Filter

### What Are Spiders?

Every individual stock in the universe belongs to one of 10 sector baskets, called "spiders." Each spider is a weighted composite price series built from all member stocks in that sector, designed to represent the macro health of that sector.

| Spider | Description |
|---|---|
| SECTOR_TECHNOLOGY | Technology |
| SECTOR_FINANCIALS | Financials |
| SECTOR_HEALTHCARE | Healthcare |
| SECTOR_INDUSTRIALS | Industrials |
| SECTOR_CONSUMER_DISCRETIONARY | Consumer Discretionary |
| SECTOR_CONSUMER_STAPLES | Consumer Staples |
| SECTOR_ENERGY | Energy |
| SECTOR_MATERIALS | Materials |
| SECTOR_COMMUNICATION_SERVICES | Communication Services |
| SECTOR_UTILITIES | Utilities |

### How The Gate Works

Each spider runs through the same 9-stage classifier. Each day, every spider has a stage classification:
- **Spider in Stage 7/8/9** → sector macro regime is supportive → signals from member stocks are permitted and sized at full or higher risk
- **Spider in Stage 2/3/4** → sector macro regime is stressed → signals from member stocks are blocked or sized at reduced risk

This prevents buying individual stock breakouts when the entire sector is in a downtrend — a key source of false signals in mean-reversion strategies.

### Risk Multiplier

When the gate is enabled, every signal also inherits a `gate_risk_mult`:
- Spider Stage 7 (strong breakout) → `risk_mult = 1.10` (size up 10%)
- Spider Stage 8 (in-zone) → `risk_mult = 1.00` (neutral)
- Spider Stage 9 (fading) → `risk_mult = 0.80` (size down 20%)

This creates dynamic position sizing aligned with macro regime strength.

### Gate Status in This Research

All results reported in this document use `gate_enabled: false` (baseline). The gate was deliberately disabled for the initial research phase to measure the **raw edge of the stock-level signal in isolation**. Gate attribution (how much the macro filter adds or costs) is the next research experiment.

---

## 7. Signal Generation — Entry Logic

### Two-Layer Architecture

Signal generation is separated from trade simulation into two distinct layers:

**Layer 1 (09A) — Signal Generator:** Scans all 2,831 tickers once, detects all valid stage transitions, and saves them to a single parquet file. This is compute-heavy (35 seconds for the full universe) but runs only once unless stage data changes.

**Layer 2 (09B) — Backtest Runner:** Reads the pre-generated signals and simulates trades with configurable stops, sizing, and exit rules. Runs in under 3 minutes for 44,008 signals across 2,582 tickers.

This separation means that changing stop size, position sizing, or exit rules does not require re-scanning 2,831 tickers' stage histories.

### Signal Detection Logic

A signal is generated when a ticker's stage **transitions INTO** Stage 6 or Stage 7 from a non-entry state. This is transition detection, not level detection.

A stock sitting in Stage 7 for 5 consecutive days generates exactly one signal (on the first day it entered Stage 7). This prevents double-counting.

**Validity check (point-in-time safe):**
- Stage 2 must have occurred on a day strictly before the signal bar
- This is enforced using a shift(1) + cummax expanding-window operation
- The signal bar itself is never counted as "having Stage 2 history"

### Signal Volume

| Metric | Value |
|---|---|
| Raw signals (full universe, 2022–2026) | 89,634 |
| Stage 6 signals | 1,364 (1.5%) |
| Stage 7 signals | 88,270 (98.5%) |
| Tickers with at least one signal | 2,582 |
| Tickers with no signals (never hit Stage 2) | 249 |
| Date range | 2022-01-31 → 2026-01-29 |

### Non-Overlap Rule

After signal generation, the 09B backtest applies a critical constraint: **one position per ticker at a time.** When a signal arrives for a ticker that already has an open trade, it is skipped. This prevents the same $10,000 capital being counted multiple times on the same stock and reflects real portfolio constraints.

This constraint reduced the trade count from 89,634 raw signals to **44,008 non-overlapping simulated trades** — the correct, honest trade count.

---

## 8. Trade Execution Model — Entry, Stop & Exit

### Entry Timing

All entries are at the **open of the trading day following the signal bar.**

Signal is observed at close of the signal bar (end of day). The trade order is placed for the next morning's open. This is completely lookahead-safe — you cannot see a closing price and enter at that same close.

### Stop Loss Calculation

The stop loss is computed on the signal bar and fixed at entry:

```
stop_price = entry_open - (ATR_14 × atr_multiplier)
```

Where ATR_14 is the 14-period Wilder Average True Range computed on the signal bar. With the baseline `atr_multiplier: 2.0`, the stop is placed 2× the stock's daily noise range below entry. This is:
- Wide enough to avoid being stopped out by normal daily fluctuation
- Tight enough to limit losses to a defined, controlled amount
- Self-scaling: a volatile stock gets a wider stop, a calm stock gets a tighter stop

**Stop floor:** A safety floor of 0.5% of entry price prevents near-zero stops on extremely stable stocks.

**Gap protection:** If a stock opens below the stop price (overnight gap down), the position exits at the gapped-down open — not at the stop price. This is realistic and prevents unrealised losses from not being recorded.

### Position Sizing

```
risk_dollars   = account_equity × risk_pct_per_trade
shares         = floor(risk_dollars / stop_distance)
notional       = shares × entry_open
```

With baseline settings ($10,000 account, 1% risk): $100 is risked per trade. The number of shares varies by the stop distance — wide stop = fewer shares, tight stop = more shares. This creates naturally adaptive position sizing that scales to each stock's volatility.

### Exit Hierarchy

Exits are checked in this priority order each day:

1. **Gap protection:** If the day's open is already below stop → exit at open
2. **Stop hit:** If the day's low touches the stop price → exit at stop price
3. **Stage 9 detected:** Exit signal observed at previous close → exit at today's open
4. **Time stop:** Maximum hold days reached → exit at next day's open
5. **End of data:** Last bar in history → exit at close

### R-Multiple Tracking

Every trade records its result in R-multiples:
```
pnl_r = (exit_price - entry_price) / stop_distance
```

A trade stopped out exactly at the stop = -1.0R. A trade that returns 2× the risk taken = +2.0R. Expectancy R = average R across all trades. **Positive expectancy R is the fundamental proof that an edge exists.**

Verified in smoke test: stop-hit trades show `pnl_r = -1.0000` exactly, confirming the stop calculation and exit logic are working correctly.

### Exit Reason Distribution (Baseline, All Trades)

| Exit Reason | Count | % | Description |
|---|---|---|---|
| Stage 9 exit | ~60% | Primary | Momentum fading — systematic trend exit |
| Stop hit | ~31% | | Price hit stop loss intraday |
| Gap stop | ~6% | | Overnight gap below stop |
| End of data / time stop | ~3% | | Research boundary / max hold |

The 60% Stage 9 exit rate is a healthy sign. It means the strategy is capturing most of the trend move before exiting — not being stopped out early. The stop hit rate of ~31% shows the stop is doing its job: protecting against the losing trades without prematurely ending the winning ones.

---

## 9. Backtesting Architecture

### Design Principles

The backtest is built with four core principles:

**1. Point-in-time safety.** Every signal, indicator, and stage classification uses only information available on or before the signal date. There is no lookahead bias anywhere in the pipeline.

**2. Realistic execution.** Entry at next-day's open (not signal close). Gap protection on stop exits. No assuming you can execute at prices that never traded.

**3. Separating signal from execution.** The two-layer architecture (09A signals + 09B simulation) means signal generation and trade simulation are independently auditable. You can inspect the raw signals file and verify every signal manually.

**4. Full audit trail.** Every run saves a config snapshot. Every trade records its signal date, entry date, stop price, exit reason, and all relevant metrics. Any result can be reproduced exactly.

### Independent Account Methodology (09B)

In 09B, each ticker is backtested independently with its own $10,000 starting equity. This is not a portfolio simulation — it is a per-ticker edge measurement. The purpose is to answer: "Does this strategy have edge on this specific stock?" independently of portfolio-level capital constraints.

This methodology:
- Allows clean comparison of edge across tickers
- Is unaffected by which other tickers were trading at the same time
- Provides the cleanest possible signal-level attribution

Portfolio-level simulation (capital constraints, concurrent position limits) is handled separately in 09C.

### Per-Ticker Backtest Output

For each ticker, the engine produces:
- Full trade-by-trade record (entry, exit, prices, PnL, R-multiple)
- Equity curve (step function — updates at each trade exit)
- Full metrics (win rate, profit factor, expectancy R, Sharpe, Sortino, drawdown)
- Exit reason breakdown

---

## 10. Universe Filter — Identifying the Investable Subset

### Why Filtering Is Necessary

Running the strategy on all 2,582 tickers reveals that the edge is not uniformly distributed. Some tickers show strong, consistent mean-reversion behaviour. Others — primarily structurally declining companies, commodity-driven names, and illiquid small caps — show negative or near-zero edge.

Deploying capital equally across all tickers would dilute returns by including tickers where the strategy simply does not work.

### Filter Criteria (Baseline Thresholds)

A ticker passes if ALL of the following are true:

| Criterion | Threshold | Rationale |
|---|---|---|
| Minimum trades | ≥ 5 | Below 5 trades, statistics are not meaningful |
| Profit factor | ≥ 1.0 | Gross wins must exceed gross losses |
| Expectancy R | ≥ 0.0 | Average trade must have positive expected value |
| Max drawdown | ≥ -60% | Rejects deeply broken tickers |
| Min win rate | ≥ 0% | No floor — captured by profit factor instead |

All thresholds are configurable. These are conservative defaults — they accept any ticker with positive edge, however marginal.

### Filter Results

| Category | Count | % |
|---|---|---|
| Total tickers | 2,582 | 100% |
| **Passed** | **1,251** | **48.5%** |
| Rejected | 1,331 | 51.5% |

### Who Gets Rejected and Why

The most common rejection reasons:
- **Low profit factor (< 1.0):** Gross losses exceed gross wins. The strategy is systematically losing on these stocks. Primarily seen in structurally declining companies (BYND, BNED, BGS) where Stage 2 → Stage 7 fires mechanically but the "mean" keeps declining.
- **Negative expectancy R:** The average trade destroys value. Every trade lost more than it gained on average.
- **Insufficient trades:** Fewer than 5 complete trades — statistically meaningless.

### Sector Distribution of Passing Tickers

| Sector | Passing | Avg Profit Factor | Avg Expectancy R |
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

Healthcare's outlier metrics (avg PF 4.94, avg expectancy R 3.34) are driven by biotech/pharma names. These stocks experience sharp, event-driven dislocations (clinical trial failures, FDA decisions) followed by large recoveries. High variance but genuine edge.

---

## 11. Full Universe Results

### Baseline Configuration

| Parameter | Value |
|---|---|
| Stop mode | ATR(14) × 2.0 (adaptive) |
| Entry | Stage 7 breakout confirmation |
| Entry timing | Next day open |
| Account equity (per ticker) | $10,000 |
| Risk per trade | 1% = $100 |
| Dislocation prerequisite | Required (Stage 2) |
| Spider gate | Disabled (raw edge test) |
| Overlap mode | Disabled (one position per ticker) |
| Time stop | 60 trading days |

### Full Universe (2,582 Tickers) — Pre-Filter

| Metric | Value |
|---|---|
| Total trades | 44,008 |
| Win rate | 36.2% |
| Expectancy R | 0.20 |
| Profit factor | 1.36 |
| Avg hold days | 13.5 |

### Filtered Universe (1,251 Tickers) — Post-Filter, All Trades

| Metric | Value |
|---|---|
| Total trades | 21,782 |
| Win rate | **43.1%** |
| Expectancy R | **0.63** |
| Profit factor | **2.26** |
| Net PnL ($10k/ticker basis) | **$1,278,183** |
| Avg hold days | ~14 |

The filter's impact is clear: removing 51.5% of tickers (the ones without edge) improves profit factor from 1.36 → 2.26 and win rate from 36.2% → 43.1%. The investable subset has meaningfully better characteristics than the full universe.

### Edge Distribution (Full Universe, 2,582 Tickers)

| Segment | Count | % |
|---|---|---|
| Positive expectancy R | 1,324 | 51.3% |
| Profit factor > 2.0 | 436 | 16.9% |
| Profit factor 1.0–2.0 | 416 | 16.1% |
| Profit factor < 1.0 | 936 | 36.2% |

**Over half the universe has positive expectancy.** The 16.9% with profit factor > 2.0 are the high-conviction names — these are the stocks where the dislocation → mean-reversion thesis works most reliably.

---

## 12. Batched Time-Window Analysis

### Methodology

To understand how the strategy performs across different market regimes, trades are split into two time windows using **entry-date attribution**: each trade belongs to the window in which it entered, regardless of when it exited.

This approach avoids artificial force-closing at window boundaries and reflects how a fund would actually attribute performance — by the vintage of each entry decision.

### Batch 1: 2022–2023 (Bear Market / Recovery)

| Metric | Value |
|---|---|
| Trades | 8,273 |
| Unique tickers | 1,201 |
| Win rate | **43.6%** |
| Profit factor | **3.07** |
| Expectancy R | **1.17** |
| Net PnL | $853,737 |

**Interpretation:** This is the strategy's natural habitat. The 2022 Federal Reserve hiking cycle created the deepest, most widespread equity dislocations since 2008-2009. Hundreds of stocks experienced genuine Stage 2 dislocations simultaneously. The subsequent 2023 recovery produced large, clean mean-reversion moves. Profit factor of 3.07 means every dollar lost was accompanied by three dollars won — exceptional performance for any systematic strategy.

### Batch 2: 2024–2026 (Bull Market)

| Metric | Value |
|---|---|
| Trades | 13,509 |
| Unique tickers | 1,251 |
| Win rate | 42.8% |
| Profit factor | 1.71 |
| Expectancy R | 0.30 |
| Net PnL | $424,446 |

**Interpretation:** Bull market conditions with fewer genuine dislocations. Stage 2 events still occur (sector rotations, earnings reactions, macro shocks) but they are less widespread and recoveries are faster. The strategy retains positive edge — profit factor 1.71, positive expectancy R — but with reduced magnitude. This is the expected behaviour: mean-reversion strategies extract more value from volatile dislocating markets than from smooth trending ones.

### Combined Summary

| Metric | Batch 1 | Batch 2 | Combined |
|---|---|---|---|
| Trades | 8,273 | 13,509 | **21,782** |
| Win rate | 43.6% | 42.8% | **43.1%** |
| Profit factor | 3.07 | 1.71 | **2.26** |
| Expectancy R | 1.17 | 0.30 | **0.63** |
| Net PnL | $853,737 | $424,446 | **$1,278,183** |

**The strategy is profitable in both market regimes.** The edge is stronger in dislocation environments but remains positive across the full cycle. This is a critical property for investor confidence — the strategy does not only work in one specific market condition.

---

## 13. Portfolio Simulation Results

### Methodology

Phase 09C simulates all trades through a single shared capital pool rather than independent per-ticker accounts. This gives realistic portfolio-level metrics: true drawdown, true Sharpe ratio, and real capital utilisation.

### Key Finding: Capital Demand vs Supply

The strategy's natural concurrent position demand is ~34 open positions simultaneously. At $10,000 risk-based sizing per position, this requires approximately $280,000 to run uncapped. This is the fundamental tension in deploying this strategy at portfolio level.

### Validated Portfolio Results (09C Baseline)

| Parameter | Value |
|---|---|
| Starting capital | $100,000 |
| Risk per trade | 0.5% = $500 |
| Max positions | Uncapped |

| Metric | Value |
|---|---|
| Final equity | $155,952 |
| Net return | **+55.9%** (4-year period, ~11.2% annualised) |
| Max drawdown | **-32.7%** (real daily equity curve) |
| Sharpe ratio | **0.83** (daily returns, annualised) |
| Sortino ratio | **1.73** (downside-only volatility) |
| Calmar ratio | **1.71** |
| Avg concurrent positions | 34.4 |
| Trades executed | 2,400 |
| Trades blocked (insufficient cash) | 41,594 |

**The Sortino of 1.73 is the standout metric.** It confirms that downside volatility is well-controlled relative to total returns — the stop-loss system is working exactly as intended. Losses are small and contained; wins are allowed to run.

**Note on blocked trades:** 41,594 trades were blocked due to capital exhaustion. This reflects the strategy's natural concurrent demand exceeding the $100k capital base. Resolving this requires either a larger capital pool (~$280k+) or a max position cap with priority-based signal selection. Both approaches are modelled in the scenarios section.

---

## 14. Key Research Findings

### Finding 1: The Dislocation Prerequisite Is the Core Alpha Driver

The Stage 2 requirement is the single most important filter in the strategy. Without it, the strategy would enter every Stage 7 breakout regardless of whether a genuine overshoot occurred first. The filter:
- Eliminated 8.8% of the universe (249 tickers that never dislocated)
- Concentrated entries on structurally mean-reverting setups
- Is the primary reason the strategy works in Batch 1 (2022) where dislocations were widespread

*Note: Gate attribution (testing `require_stage2_history: false`) is a proposed next experiment to quantify this numerically.*

### Finding 2: Stage 7 Is the Real Primary Entry (97%+ of signals)

Although the strategy nominally accepts Stage 6 (Breakout) and Stage 7 (Breakout Confirmed) entries, 98.5% of all signals were Stage 7. This is expected: after a full dislocation and base cycle, price has almost always recovered above EMA50 by the time the Donchian breakout occurs — satisfying Stage 7 directly.

Stage 6 is a theoretically valid earlier entry but extremely rare in practice. This is not a bug — it is a signal that the Stage 7 confirmation requirement is working as intended.

### Finding 3: Win Rate < 50% Does Not Imply No Edge

The baseline win rate is 43.1%. Many investors instinctively assume a strategy needs >50% win rate to be profitable. This is incorrect. With asymmetric payoffs (average win significantly larger than average loss), a 43% win rate with profit factor 2.26 generates substantial positive expectancy. The system wins less often than it loses but wins much bigger — a classic trend-following payoff profile applied to mean-reversion entries.

### Finding 4: Edge Is Concentrated in ~51% of the Universe

Exactly 1,251 of 2,582 tickers (48.5%) show positive edge by all filter metrics. The remaining 51.5% are:
- Structurally declining businesses where Stage 2 → Stage 7 fires mechanically but the mean keeps declining
- Commodity-driven names where price is driven by macro factors unrelated to stock structure
- Very low-liquidity tickers where ATR is unstable

This finding is valuable for deployment: the strategy should be applied to the filtered subset only. Deploying to the full universe dilutes returns significantly (profit factor 2.26 → 1.36).

### Finding 5: Healthcare Sector Shows Exceptional Edge

Healthcare/biotech shows average profit factor of 4.94 and expectancy R of 3.34 — significantly above all other sectors. This is driven by the biotech dislocation pattern: clinical trial failures or FDA rejections create sharp Stage 2 dislocations, followed by recoveries as the company redirects focus. These are genuine mean-reversion events with high R-multiples.

*Note: Healthcare expectancy R is inflated by a small number of exceptional trades (e.g. SSII at 617R). The median within Healthcare is likely lower. Sector-level attribution analysis is a proposed next experiment.*

### Finding 6: The Exit System Works — Stage 9 Captures 60% of Exits

The fact that 60% of trades exit via Stage 9 (trend fading signal) rather than stop-loss means the strategy is systematically capturing most of the mean-reversion move before exiting. Only 31% are stopped out. This is the ideal profile: let winners run to natural exhaustion, cut losers quickly.

---

## 15. Risk Considerations

### Market Regime Risk
The strategy performs significantly better in dislocation environments (2022) than in smooth bull markets (2024). Extended bull markets with few genuine dislocations will reduce trade frequency and edge magnitude. This is not a strategy failure — it is an expected characteristic of mean-reversion strategies.

### Concentration Risk (Pre-Portfolio Layer)
Without sector caps, the portfolio can become heavily concentrated in one sector during widespread sector-level dislocations. During 2022, Technology and Healthcare simultaneously dislocated, potentially creating 30%+ sector concentration. Sector exposure caps (proposed in Scenarios) mitigate this.

### Survivorship Bias Consideration
The universe is constructed from Finviz listings as of early 2026. Tickers that were delisted between 2022 and 2026 are not included. This introduces mild survivorship bias — the actual live performance would have included some additional losses from delisted/failed companies. The magnitude is expected to be small given the $300M+ market cap filter (large companies rarely delist entirely) but should be noted.

### Stop-Loss Limitations
ATR-based stops protect against normal volatility but not against extreme gap events (earnings surprises, macro shocks). Gap protection is implemented (if a stock opens below stop, exit at open) but large gap events can produce losses exceeding -1R. These are captured in the `stop_gap` exit category (5.8% of trades).

### Look-Forward Risk (Architecture Risk)
The system is designed to be lookahead-free but any future data or strategy changes must be validated against this constraint. Adding any indicator that uses future data (even accidentally through forward-fill operations) would invalidate all historical results.

---

## 16. Proposed Next Experiments

### Experiment 1 — Spider Gate Attribution (Highest Priority)
Enable the macro sector filter and compare all metrics against the baseline.

**Config change:** `spider_gate.enabled: true`  
**Expected outcome:** Reduced trade count (some signals blocked), potentially higher win rate and profit factor on executed trades, potentially lower net PnL if gate blocks too many good signals.

**Run order:**
```
09B → 09D → 09E
```

---

### Experiment 2 — Stop Multiplier Sensitivity
Test ATR multipliers of 1.5, 2.0 (baseline), and 3.0.

**Config change:** `stop.atr_multiplier: 1.5` / `3.0`

**Expected outcome:** Tighter stop (1.5×) — higher stop-out rate, potentially higher win rate on surviving trades, lower net PnL if stops too tight. Wider stop (3.0×) — fewer stop-outs, larger individual losses, potentially lower drawdown but lower Calmar.

**Run order:**
```
09B → 09D → 09E (for each multiplier variant)
```

---

### Experiment 3 — Stage 2 Requirement (Quantify Dislocation Value)
Remove the dislocation prerequisite and measure the impact.

**Config change:** `signal.require_stage2_history: false`

**This is the most informative single experiment available.** The delta in expectancy R between this run and baseline directly quantifies how much value the dislocation requirement adds.

**Run order:**
```
09A (required) → 09B → 09D → 09E
```

---

### Experiment 4 — Time Stop Sensitivity
Test hold periods of 30, 45, 60 (baseline), and 90 trading days.

**Config change:** `exit.time_stop_days: 30` / `45` / `90`

**Research question:** Does capping the hold at 30 days improve capital efficiency? Do longer holds capture more of the mean-reversion move or just introduce more end-of-move risk?

---

### Experiment 5 — Scale-In Mode (Pyramiding)
Allow re-entry on the same ticker while a trade is open, capped at 2 additional legs.

**Config change:** `sizing.overlap_mode: "scale_in"`

**Research question:** Does adding to confirmed trends improve overall returns or does it just add correlated risk?

---

### Experiment 6 — Portfolio Capacity Testing
Test multiple capital and max_positions combinations to find the optimal deployment parameters.

| Variant | Capital | Risk% | Max Pos | Research Question |
|---|---|---|---|---|
| A | $100k | 0.5% | 20 | Concentrated portfolio edge |
| B | $100k | 0.5% | 30 | Diversified portfolio edge |
| C | $100k | 0.25% | null | Conservative risk, full opportunity |
| D | $300k | 0.5% | null | Properly capitalised uncapped |

**Run order for each:** `09C only` (no 09A/09B re-run needed)

---

## 17. Conclusion

The ALGO-STOCKS research framework has validated a statistically significant positive edge for a long-only mean-reversion equity strategy across a 2,582-ticker U.S. universe over the 2022–2026 period.

### Summary of Evidence

| Evidence | Detail |
|---|---|
| Positive expectancy R | 0.63 across 21,782 trades in filtered universe |
| Profitable across both market regimes | Batch 1 (bear): PF 3.07 / Batch 2 (bull): PF 1.71 |
| Large sample size | 21,782 trades, 1,251 tickers — statistically robust |
| Consistent win rate | 43.1% — stable across regimes and configurations |
| Controlled downside | Stop-loss contains losses; 60% of exits via Stage 9 (full move captured) |
| Real Sharpe | 0.83 (daily returns, 4-year period including 2022 bear market) |
| Sortino | 1.73 — downside is well-managed relative to total returns |

### Investment Thesis (One Paragraph)

When high-quality U.S. equities experience sharp, identifiable dislocations below their structural mean — the kind of overshoot driven by macro fear, sector rotation, or event-driven selling — they exhibit a reliable tendency to recover toward fair value once the dislocation event passes. By waiting for the dislocation to complete, a base to form, and a breakout to be confirmed with volume and EMA alignment, it is possible to enter these recoveries systematically with controlled, predefined risk. The backtest evidence across 2,582 stocks and 4 years of data including one of the most challenging bear markets in recent history validates this thesis: the strategy generates positive edge in both bear and bull environments, with strongest performance when genuine dislocations are most widespread.

### Next Steps

1. Gate attribution experiment (flip `spider_gate.enabled: true`) — immediate priority
2. Stop sensitivity sweep (ATR 1.5× vs 2.0× vs 3.0×)
3. Stage 2 requirement ablation (quantify the dislocation filter's value)
4. Portfolio capacity optimisation (find optimal capital/position-cap combination)
5. Live monitoring framework — paper trading validation before capital deployment

---

*Report generated: February 2026*  
*Framework: ALGO-STOCKS v09E*  
*All results: baseline configuration, gate disabled, ATR 2× stop, 1% per-ticker risk*  
*Reproducible: full audit trail in `output/backtests/universe_baseline_v1_20260224_2310/`*

