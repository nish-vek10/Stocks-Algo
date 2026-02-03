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

---

## 3. Core Indicators Used

The following indicators are used to determine stages:

- **EMA Stack**
  - EMA10 / EMA20 / EMA50 / EMA200
- **Long-Term Mean**
  - EMA200 (configurable)
- **Donchian Channels**
  - 20-day high / low (primary trigger)
- **Bollinger Bands**
  - 20-period, 2 standard deviations
- **Volume**
  - Relative to 20-day average
- **Momentum**
  - RSI / MACD (confirmation only)

> **Note:** All lookbacks require **minimum 2 years of price history** per stock.

---

## 4. Stage Overview

| Stage | Name                  | Trade Action |
|------:|-----------------------|-------------|
| 1 | Not Eligible | Ignore |
| 2 | Sharp Downtrend | Block |
| 3 | Downtrend | Block |
| 4 | Below Zone | Watch |
| 5 | Lower Zone | Watch |
| 6 | Breakout | Entry Allowed |
| 7 | Breakout Confirmed | Entry Preferred |
| 8 | In-Zone | Hold / Manage |
| 9 | In-Zone (Fading) | Exit |

---

## 5. Detailed Stage Definitions

---

### **Stage 1 — Not Eligible**

**Description**  
Price shows no meaningful deviation or structure that supports mean reversion.

**Typical Characteristics**
- Price near EMA200
- Inside Bollinger Bands
- No Donchian breakout
- Flat or low volatility
- Weak or inconsistent volume

**Action**
- NO TRADE
- NO WATCHLIST

---

### **Stage 2 — Sharp Downtrend**

**Description**  
Strong bearish impulse with accelerating downside momentum.

**Typical Characteristics**
- Price below EMA200
- EMA stack strongly bearish (EMA10 < EMA20 < EMA50 < EMA200)
- New Donchian 20-day lows
- Expanding Bollinger Bands
- High downside volume

**Action**
- NO TRADE
- NO WATCHLIST
- HIGH-RISK ENVIRONMENT

---

### **Stage 3 — Downtrend**

**Description**  
Sustained bearish trend, but without sharp acceleration.

**Typical Characteristics**
- Price below EMA200
- Bearish EMA stack
- Lower highs / lower lows
- Donchian lows respected
- Moderate volume

**Action**
- NO TRADE
- NO WATCHLIST

---

### **Stage 4 — Below Zone**

**Description**  
Downtrend is slowing; price is deeply below long-term mean but beginning to stabilize.

**Typical Characteristics**
- Price below EMA200
- EMA slope flattening
- Bollinger Bands contracting
- Donchian lows no longer expanding
- Selling pressure weakening

**Action**
- EARLY WATCHLIST
- NO ENTRY

---

### **Stage 5 — Lower Zone**

**Description**  
Base formation phase; potential transition into mean reversion.

**Typical Characteristics**
- Price still below EMA200
- Higher lows forming
- EMA10 / EMA20 flattening or crossing upward
- Bollinger mid-band approached
- Volume stabilizing

**Action**
- WATCHLIST
- NO ENTRY UNLESS EXPLICITLY TESTED

---

### **Stage 6 — Breakout**

**Description**  
Initial confirmation that mean reversion has started.

**Typical Characteristics**
- Donchian 20-day **high breakout**
- Price moves above short-term EMAs
- Bollinger expansion upward
- Volume expansion
- Price still below or near EMA200

**Action**
- ENTRY ALLOWED
- REQUIRES SUPPORTIVE SECTOR ("SPIDER")

---

### **Stage 7 — Breakout Confirmed**

**Description**  
Breakout holds and trend quality improves.

**Typical Characteristics**
- Price holds above Donchian breakout level
- EMA10 > EMA20 > EMA50
- Positive EMA slopes
- Above-average volume on advances
- No immediate breakout failure

**Action**
- PREFERRED ENTRY STAGE
- CORE TRADE INITIATION ZONE

---

### **Stage 8 — In-Zone**

**Description**  
Price has entered the mean reversion “value zone”.

**Typical Characteristics**
- Price above EMA200
- Strong EMA alignment
- Stable trend
- Bollinger mid-to-upper band
- Momentum positive but slowing

**Action**
- HOLD POSITION
- MONITOR FOR FADING SIGNALS

---

### **Stage 9 — In-Zone (Fading)**

**Description**  
Mean reversion is mature; upside momentum deteriorates.

**Typical Characteristics**
- EMA10 flattens or turns down
- Momentum divergence
- Bollinger contraction or rejection
- Failure to make new highs
- Weakening volume

**Action**
- EXIT POSITION
- LOCK IN PROFITS

---

## 6. Stage Transitions

- Stages progress **sequentially**, but can skip stages in strong moves
- Downward transitions override upward bias
- Exit signals **always dominate** entry signals

---

## 7. Relationship to Sector (“Spider”) Regime

Stage-based entries are **only valid** if the corresponding sector spider is supportive.

- Strong sector → full signal validity
- Neutral sector → reduced size
- Weak sector → block entries

---

## 8. Research Notes

- Not all stages are expected to generate alpha
- Edge is expected primarily in:
  - Stage 6 (Breakout)
  - Stage 7 (Breakout Confirmed)
- Stage effectiveness must be validated via backtesting

---

## 9. Versioning

- Stage logic is **configuration-driven**
- All thresholds must be documented and versioned
- Changes require re-running backtests

---

**End of Stage Definitions**
