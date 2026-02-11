# Path: stages/stage_classifier.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple
from pathlib import Path

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


# -------------------------------
# Config loading / normalization
# -------------------------------

def _load_yaml(path: "Path") -> dict:
    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError("PyYAML not installed. Install with: pip install pyyaml") from e

    if not path.exists():
        raise FileNotFoundError(f"Missing YAML: {path}")

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _normalize_indicators_cfg(ind_yaml: dict) -> dict:
    """
    Convert config/indicators.yaml (your current schema) into the legacy structure
    expected by classify_stage(): cfg_ind["lookbacks"][...], cfg_ind["bollinger"]["stdev"], etc.
    """
    t = (ind_yaml or {}).get("technicals", ind_yaml or {}) or {}

    ema_spans = t.get("ema_spans", [10, 20, 50, 100, 200]) or [10, 20, 50, 100, 200]
    # Choose fast/mid/slow/long from common defaults (or from list if present)
    def pick(default: int) -> int:
        return default if default in ema_spans else int(sorted(ema_spans)[0])

    bb = t.get("bollinger", {}) or {}
    dc = t.get("donchian", {}) or {}
    vol = t.get("volume", {}) or {}

    bb_window = int(bb.get("window", 20))
    bb_n_std = float(bb.get("n_std", 2.0))

    donch_window = int(dc.get("window", 20))

    vol_avg_window = int(vol.get("avg_window", 10))
    surge_mult = float(vol.get("surge_mult", 1.15))

    # This matches your compute_relative_volume() expectation:
    # relvol = volume / SMA(volume, vol_avg_window)
    # surge if relvol >= surge_mult
    out = {
        "lookbacks": {
            "ema_fast": pick(10),
            "ema_mid": pick(20),
            "ema_slow": pick(50),
            "ema_long": pick(200),
            "donchian": donch_window,
            "bollinger_period": bb_window,
            "vol_avg_period": vol_avg_window,
            # default gate; can be overridden by stages.yaml if you want
            "min_history_days": 260,
        },
        "bollinger": {"stdev": bb_n_std},
        "volume": {"rel_vol_threshold": surge_mult},
    }
    return out


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

    don_high_s = don["donchian_high"].shift(1)
    don_low_s = don["donchian_low"].shift(1)

    d_high = float(don_high_s.loc[i]) if pd.notna(don_high_s.loc[i]) else None
    d_low = float(don_low_s.loc[i]) if pd.notna(don_low_s.loc[i]) else None

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

    trend_turning_up = (e10 is not None and e20 is not None and e10 > e20)
    recovered = (e50 is not None and c > e50)

    # Stage 7: Breakout Confirmed
    if d_high is not None and recovered and c > d_high and trend_turning_up:
        reasons += ["holding_breakout", "above_ema50", "ema10_gt_ema20"]
        if vol_expansion:
            reasons += ["relvol_expansion"]
        return StageResult(7, STAGE_NAMES[7], reasons)

    # Stage 6: Breakout
    if breakout and trend_turning_up:
        reasons += ["donchian_breakout", "ema10_gt_ema20"]
        if vol_expansion:
            reasons += ["relvol_expansion"]
        return StageResult(6, STAGE_NAMES[6], reasons)

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


def run_stage_classifier(*, df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    """
    Produce a DAILY stage label series for an OHLCV dataframe.

    Output columns:
      date, stage, stage_name, stage_reason

    Notes:
    - Uses an expanding window up to each date (point-in-time safe).
    - Calls classify_stage() on each day using data available up to that day.
    - Works for BOTH stocks and spiders.
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "stage", "stage_name", "stage_reason"])

    # Ensure we have a date column
    work = df.copy()
    if "date" in work.columns:
        work["date"] = pd.to_datetime(work["date"])
        work = work.sort_values("date").reset_index(drop=True)
        work = work.set_index("date")
    else:
        if not isinstance(work.index, pd.DatetimeIndex):
            raise KeyError("Input df must have a 'date' column or a DatetimeIndex.")
        work = work.sort_index()

    # cfg may be:
    #  - stages.yaml only (from 07D)  -> we will load indicators.yaml automatically
    #  - combined dict containing indicators/stages -> we will use provided

    ROOT = Path(__file__).resolve().parents[1]
    indicators_path = ROOT / "config" / "indicators.yaml"

    cfg_stages = cfg if isinstance(cfg, dict) else {}
    cfg_ind_raw = cfg.get("indicators") if isinstance(cfg, dict) and "indicators" in cfg else None

    if cfg_ind_raw is None:
        ind_yaml = _load_yaml(indicators_path)
    else:
        ind_yaml = cfg_ind_raw

    cfg_ind = _normalize_indicators_cfg(ind_yaml)

    # Optional override: allow stages.yaml to set min_history_days if you include it
    # e.g. stages: { min_history_days: 260 }
    try:
        mh = cfg_stages.get("min_history_days", None)
        if mh is not None:
            cfg_ind["lookbacks"]["min_history_days"] = int(mh)
    except Exception:
        pass

    seen_breakout = False  # becomes True once we see Stage 6 or 7 at least once

    dates = work.index
    out_rows = []

    # Expanding window classification
    for i, dt in enumerate(dates):
        slice_df = work.iloc[: i + 1].copy()

        r = classify_stage(slice_df, cfg_ind, cfg_stages)

        # Update memory (breakout started)
        if int(r.stage_id) in (6, 7):
            seen_breakout = True

        stage_id = int(r.stage_id)
        stage_name = r.stage_name
        reasons = list(r.reasons)

        # Optional gate (configurable): require at least one breakout before allowing Stage 8/9.
        require_breakout_before_inzone = bool(
            (cfg_stages.get("stage_logic", {}) or {}).get("require_breakout_before_inzone", False)
        )

        if require_breakout_before_inzone and (stage_id in (8, 9)) and (not seen_breakout):
            stage_id = 1
            stage_name = STAGE_NAMES[1]
            reasons = reasons + ["pre_breakout_above_ema200->stage1"]

        out_rows.append(
            {
                "date": dt,
                "stage": stage_id,
                "stage_name": stage_name,
                "stage_reason": "|".join(reasons),
            }
        )

    out_df = pd.DataFrame(out_rows)
    # keep stable column order
    cols = ["date", "stage", "stage_name", "stage_reason"]
    for c in cols:
        if c not in out_df.columns:
            out_df[c] = None
    out_df = out_df[cols]
    return out_df


# ---------------------------------------------------------------------
# Public entrypoint used by BOTH:
# - stock pipelines
# - spider pipelines (07D)
# ---------------------------------------------------------------------

def classify_stages(*, df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    """
    Canonical entrypoint used by BOTH stocks and spiders.

    Input df:
      - must include: date (or DatetimeIndex), high, low, close, volume
      - open is optional for stage logic right now

    cfg:
      - can be stages.yaml dict (what 07D passes)
      - OR a dict that includes {"indicators": <indicators_yaml_dict>, ...}

    Returns:
      DataFrame with:
        date, stage, stage_name, stage_reason
    """
    return run_stage_classifier(df=df, cfg=cfg)

