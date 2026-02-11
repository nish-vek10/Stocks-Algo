# Market Stage Definitions  
### ALGO-STOCKS — Long-Only Mean-Reversion Framework

---

## 1. Purpose of Market Stages

The **market stage system** is the core decision engine of ALGO-STOCKS.

Every stock is classified into **exactly one stage per day**, based on its technical structure and trend behaviour.  
Stages determine:

- Whether a stock is **eligible for trading**
- When **entries** are allowed
- How **open positions** are managed
- When **exits** are triggered

This is a **state machine**, not a signal generator:

- Stages describe *context*
- Signals act *within* that context

---

## 2. Design Principles

- **Long-only**: no stage ever implies shorting
- **Deterministic**: same inputs → same stage
- **Point-in-time safe**: no future data
- **Hierarchical**: higher-risk stages override lower ones
- **Explainable**: each stage has explicit reasons
- **Research-first**: stages are validated by backtests; thresholds are configuration-driven

---

## 3. Core Indicators Used

The following indicators are used to determine stages (configuration-driven via `config/indicators.yaml`):

- **EMA Stack**
  - EMA10 / EMA20 / EMA50 / EMA100 / EMA200
- **Long-Term Mean**
  - EMA200 (configurable anchor)
- **Donchian Channels**
  - 20-day high / low (primary trigger)
- **Bollinger Bands**
  - 20-period, 2 standard deviations
- **Volume**
  - 10-day average volume + surge flag (e.g. > 1.15× average)
- **Momentum (optional confirmations)**
  - MACD (12, 26, 9), RSI(14)

> **Warmup note:** 
> - Indicators require sufficient lookback history (e.g. EMA200 + Donchian20 + BB20).  
> - IPO/short-history tickers are handled after warmup and may be flagged “insufficient history” in early periods.

---

## 4. Stage Overview

| Stage | Name                 | Trade Action |
|------:|----------------------|-------------|
| 1 | Not Eligible | Ignore |
| 2 | Sharp Downtrend | Block |
| 3 | Downtrend | Block |
| 4 | Below Zone | Watch |
| 5 | Lower Zone | Watch |
| 6 | Breakout | Entry Allowed |
| 7 | Breakout Confirmed | Entry Preferred |
| 8 | In-Zone | Hold / Manage |
| 9 | In-Zone (Fading) | Exit Candidate |

---

## 5. Detailed Stage Definitions

---

### **Stage 1 — Not Eligible**

**Description**  
Stock is not eligible because price remains above the long-term mean.

**Typical Characteristics**
- Price **above EMA200**
- Other signals are treated as informational only (eligibility gate dominates)

**Action**
- NO TRADE  
- NO WATCHLIST

**Hard rule**
- Remains Stage 1 until price closes below EMA200.

---

### **Stage 2 — Sharp Downtrend**

**Description**  
Strong bearish impulse with accelerating downside momentum. This stage establishes the **mandatory dislocation** required for later mean-reversion trades.

**Detection rule (important)**
- Stage 2 is detected over a rolling **5–7 day window**. Required markers do **not** need to align on the same day.

**Typical Characteristics (required markers within window)**
- Price below EMA200
- Sharp decline (e.g. **> 5% over ~3 trading days**)
- Close below **lower Bollinger Band** (20, 2 SD)
- EMA10 slope negative and falling meaningfully (e.g. ~1% per day)

**Optional Confirmations**
- Volume surge: **> 1.15× 10-day average**
- New Donchian 20-day low

**Action**
- NO TRADE  
- NO WATCHLIST  
- HIGH-RISK ENVIRONMENT

**State memory**
- If a ticker prints Stage 2 at any point, it becomes **eligible-in-principle** for future Stage 6 entries (subject to later rules).

---

### **Stage 3 — Downtrend**

**Description**  
Sustained bearish trend, but without the sharp acceleration required for a dislocation setup.

**Typical Characteristics**
- Price below EMA200
- Slower, grinding decline (lower highs / lower lows)
- Price often between Bollinger midline and lower band
- No persistent new Donchian lows (or weakness is less impulsive)
- Moderate volume

**Action**
- NO TRADE  
- NO WATCHLIST

**Eligibility constraint (design-locked)**
- Stage 3 alone is never tradable.  
- A ticker must have recorded **Stage 2 at least once historically** to become eligible for Stage 6 entry later.

---

### **Stage 4 — Below Zone**

**Description**  
Downtrend is slowing; price remains below the long-term mean but begins stabilising (early basing).

**Typical Characteristics**
- Price below EMA200
- No new Donchian lows (or lows stop expanding)
- Sideways or stabilising price action
- Bollinger Bands contracting (volatility compression)
- Selling pressure/volume normalising

**Action**
- EARLY WATCHLIST  
- NO ENTRY

---

### **Stage 5 — Lower Zone**

**Description**  
Base formation progresses; early recovery signals appear. This is the “setup maturation” zone before a breakout trigger.

**Typical Characteristics**
- Price still below EMA200 (often)
- Price enters the **upper half** of the Donchian range (relative positioning)
- Close above EMA10
- EMA10 crosses above EMA20 (or stack begins to improve)
- Volume stabilises, volatility reduces

**Action**
- WATCHLIST  
- NO ENTRY (unless explicitly tested as a research variant)

---

### **Stage 6 — Breakout (Primary Entry Stage)**

**Description**  
Initial confirmation that mean reversion has started. This is the first valid long entry stage.

**Typical Characteristics (entry requirements)**
- Break above **Donchian 20-day high** (breakout trigger)
- Price above EMA10, with **EMA10 > EMA20**
- Volume surge: **> 1.15× 10-day average**
- Structure improving after prior dislocation/basing (Stages 2 → 4/5 → 6)

**Action**
- ENTRY ALLOWED  
- REQUIRES SUPPORTIVE SECTOR (“SPIDER”) REGIME

**Hard constraint**
- Entries are only valid if the ticker has previously printed **Stage 2** at some point in its history.

---

### **Stage 7 — Breakout Confirmed**

**Description**  
Breakout holds and follow-through confirms trend quality. Prefer entries here when available.

**Typical Characteristics**
- Price holds above breakout level (no immediate failure)
- EMA10 > EMA20 > EMA50 (improving stack)
- Positive EMA slopes
- Elevated volume persists on advances
- Momentum confirms continuation (optional RSI/MACD confirmation)

**Action**
- PREFERRED ENTRY STAGE  
- CORE TRADE INITIATION ZONE

---

### **Stage 8 — In-Zone**

**Description**  
Mean reversion progresses; position is held and actively managed.

**Typical Characteristics**
- Price above EMA200 (often, but not strictly required)
- Trend stable; EMA alignment remains constructive
- Bollinger mid-to-upper band behaviour
- Momentum positive but may begin slowing

**Action**
- HOLD POSITION  
- MANAGE RISK / TRAIL RULES (as defined in exit framework)

---

### **Stage 9 — In-Zone (Fading)**

**Description**  
Mean reversion is mature; upside momentum deteriorates and the probability of fade increases.

**Typical Characteristics**
- EMA10 flattens or turns down
- Momentum divergence / loss of impulse
- Bollinger contraction or rejection
- Failure to make new highs
- Weakening volume

**Action**
- EXIT CANDIDATE / RISK OF FADE  
- Until exit logic is finalised, manage using **hard stops** and/or **time-based rules**.

---

## 6. Stage Transitions (High-Level)

- Stages generally progress **sequentially**, but may skip stages in strong moves (e.g. Stage 2 → Stage 4).
- Downward conditions override upward progression (risk-first).
- Exit conditions dominate entry logic (once exits are formalised).

**Non-negotiable rule**
- A ticker that never experienced a **Stage 2 sharp dislocation** is never eligible for a Stage 6/7 entry.

---

## 7. Relationship to Sector (“Spider”) Regime

Stage-based entries are only valid if the corresponding sector spider is supportive.

- Strong sector spider → full signal validity
- Neutral sector spider → reduced size / caution
- Weak sector spider → block entries

This ensures alignment between **micro stock setups** and **macro sector structure**.

---

## 8. Research Notes

- Not all stages are expected to generate alpha.
- Edge is expected primarily in:
  - Stage 6 (Breakout)
  - Stage 7 (Breakout Confirmed)
- Stage effectiveness must be validated via backtesting and sensitivity analysis.

---

## 9. Versioning

- Stage logic is **configuration-driven** (`config/stages.yaml`, `config/indicators.yaml`).
- Threshold changes must be documented and versioned.
- Any change requires re-running backtests to preserve audit integrity.

---

**End of Stage Definitions**
