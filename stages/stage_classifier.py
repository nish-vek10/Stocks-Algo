# Path: stages/stage_classifier.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd

from features.technicals.ema import compute_ema
from features.technicals.donchian import compute_donchian
from features.technicals.bollinger import compute_bollinger
from features.technicals.volume import compute_relative_volume


@dataclass(frozen=True)
class StageResult:
    stage_id: int
    stage_name: str
    reasons: List[str]


STAGE_NAMES = {
    1: "Not Eligible",
    2: "Sharp Downtrend",
    3: "Downtrend",
    4: "Below Zone",
    5: "Lower Zone",
    6: "Breakout",
    7: "Breakout Confirmed",
    8: "In-Zone",
    9: "In-Zone (Fading)",
}


def classify_stage(df: pd.DataFrame, cfg_ind: Dict, cfg_stages: Dict) -> StageResult:
    """
    Minimal v1 stage classifier.
    - Deterministic
    - Long-only compatible
    - Designed to run end-to-end for smoke testing

    Expected df columns: time, open, high, low, close, volume (OHLCV)
    Uses the *latest* row (most recent bar).

    NOTE: This rule set is intentionally conservative and will be refined.
    """
    if df is None or df.empty:
        return StageResult(1, STAGE_NAMES[1], ["empty_df"])

    required = {"high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        return StageResult(1, STAGE_NAMES[1], [f"missing_cols={sorted(missing)}"])

    lb = cfg_ind["lookbacks"]
    close = df["close"]
    high = df["high"]
    low = df["low"]
    vol = df["volume"]

    # --- Indicators ---
    ema10 = compute_ema(close, lb["ema_fast"])
    ema20 = compute_ema(close, lb["ema_mid"])
    ema50 = compute_ema(close, lb["ema_slow"])
    ema200 = compute_ema(close, lb["ema_long"])

    don = compute_donchian(high, low, lb["donchian"])
    bb = compute_bollinger(close, lb["bollinger_period"], cfg_ind["bollinger"]["stdev"])
    relvol = compute_relative_volume(vol, lb["vol_avg_period"])

    # Latest values
    i = df.index[-1]
    c = float(close.loc[i])
    e10 = float(ema10.loc[i]) if pd.notna(ema10.loc[i]) else None
    e20 = float(ema20.loc[i]) if pd.notna(ema20.loc[i]) else None
    e50 = float(ema50.loc[i]) if pd.notna(ema50.loc[i]) else None
    e200 = float(ema200.loc[i]) if pd.notna(ema200.loc[i]) else None

    d_high = float(don["donchian_high"].loc[i]) if pd.notna(don["donchian_high"].loc[i]) else None
    d_low = float(don["donchian_low"].loc[i]) if pd.notna(don["donchian_low"].loc[i]) else None

    bb_mid = float(bb["bb_mid"].loc[i]) if pd.notna(bb["bb_mid"].loc[i]) else None
    bb_lower = float(bb["bb_lower"].loc[i]) if pd.notna(bb["bb_lower"].loc[i]) else None
    bb_upper = float(bb["bb_upper"].loc[i]) if pd.notna(bb["bb_upper"].loc[i]) else None

    rv = float(relvol.loc[i]) if pd.notna(relvol.loc[i]) else None

    # Not enough history → Not Eligible
    min_hist = int(lb["min_history_days"])
    if len(df) < min_hist:
        return StageResult(1, STAGE_NAMES[1], [f"insufficient_history={len(df)}<{min_hist}"])

    reasons: List[str] = []

    # Basic regime tags
    bearish_stack = (e10 is not None and e20 is not None and e50 is not None and e200 is not None
                     and (e10 < e20 < e50 < e200))
    bullish_stack = (e10 is not None and e20 is not None and e50 is not None and e200 is not None
                     and (e10 > e20 > e50))

    below_ema200 = (e200 is not None and c < e200)
    above_ema200 = (e200 is not None and c > e200)

    breakout = (d_high is not None and c > d_high)
    breakdown = (d_low is not None and c < d_low)

    vol_expansion = (rv is not None and rv >= float(cfg_ind["volume"]["rel_vol_threshold"]))

    # --- Stage logic (v1) ---
    # Stage 2: Sharp Downtrend
    if below_ema200 and bearish_stack and breakdown and vol_expansion:
        reasons += ["below_ema200", "bearish_stack", "donchian_breakdown", "relvol_expansion"]
        return StageResult(2, STAGE_NAMES[2], reasons)

    # Stage 3: Downtrend
    if below_ema200 and bearish_stack:
        reasons += ["below_ema200", "bearish_stack"]
        return StageResult(3, STAGE_NAMES[3], reasons)

    # Stage 4: Below Zone (deep below mean but stabilizing)
    if below_ema200 and bb_lower is not None and c <= bb_lower:
        reasons += ["below_ema200", "near_bb_lower"]
        return StageResult(4, STAGE_NAMES[4], reasons)

    # Stage 5: Lower Zone (base forming)
    if below_ema200 and bb_mid is not None and c > bb_lower and c < bb_mid:
        reasons += ["below_ema200", "between_bb_lower_and_mid"]
        return StageResult(5, STAGE_NAMES[5], reasons)

    # Stage 6: Breakout
    if breakout and bullish_stack:
        reasons += ["donchian_breakout", "bullish_stack"]
        if vol_expansion:
            reasons += ["relvol_expansion"]
        return StageResult(6, STAGE_NAMES[6], reasons)

    # Stage 7: Breakout Confirmed (v1 proxy: above EMA50 and holds above Donchian high)
    if d_high is not None and e50 is not None and c > d_high and c > e50 and bullish_stack:
        reasons += ["holding_breakout", "above_ema50", "bullish_stack"]
        return StageResult(7, STAGE_NAMES[7], reasons)

    # Stage 9: In-Zone (Fading)
    # v1 proxy: above EMA200 but close below EMA10
    if above_ema200 and e10 is not None and c < e10:
        reasons += ["above_ema200", "close_below_ema10"]
        return StageResult(9, STAGE_NAMES[9], reasons)

    # Stage 8: In-Zone
    if above_ema200:
        reasons += ["above_ema200"]
        if bb_upper is not None and c >= bb_mid:
            reasons += ["in_value_zone"]
        return StageResult(8, STAGE_NAMES[8], reasons)

    # Default: Not Eligible
    return StageResult(1, STAGE_NAMES[1], ["no_rule_matched"])
