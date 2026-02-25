# Path: backtest/engine.py
"""
ALGO-STOCKS Phase 09 — Trade Simulation Engine

Responsibilities:
- Simulate a single trade from a pre-validated signal row
- Deterministic and point-in-time safe
- Entry: open price of the day AFTER the signal bar
- Exits: stop loss (ATR or fixed %), Stage 9 detection, time stop, end-of-data

No lookahead: all forward simulation uses only data available after entry.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def simulate_trade(
    signal: pd.Series,
    fwd_features: pd.DataFrame,
    fwd_stages: pd.DataFrame,
    cfg: dict,
) -> Optional[dict]:
    """
    Simulate a single trade from entry to exit.

    Parameters
    ----------
    signal        : one row from raw_signals_all.parquet (output of 09A)
    fwd_features  : features parquet sliced to rows >= entry_date for this ticker
    fwd_stages    : stages parquet sliced to rows >= entry_date for this ticker
    cfg           : full backtest config dict (from config/backtest.yaml)

    Returns
    -------
    dict with the complete trade record, or None if trade cannot be entered
    (e.g. entry_date not found in forward data — delisted or data gap)
    """
    stop_cfg   = cfg.get("stop", {})
    exit_cfg   = cfg.get("exit", {})
    sizing_cfg = cfg.get("sizing", {})

    # ── Unpack signal fields ──────────────────────────────────────────────────
    ticker         = str(signal["ticker"])
    signal_date    = signal["signal_date"]
    signal_type    = str(signal["signal_type"])
    entry_date     = pd.Timestamp(signal["entry_date"])
    atr_14         = signal.get("atr_14", None)
    gate_risk_mult = float(signal.get("gate_risk_mult", 1.0) or 1.0)

    # ── Merge forward features + stages on date ───────────────────────────────
    fwd = fwd_features.copy()
    fwd["date"] = pd.to_datetime(fwd["date"])

    stg = fwd_stages[["date", "stage", "stage_name"]].copy()
    stg["date"] = pd.to_datetime(stg["date"])

    fwd = fwd.merge(stg, on="date", how="inner").sort_values("date").reset_index(drop=True)

    if fwd.empty:
        return None

    # ── Find entry row (open of entry_date) ───────────────────────────────────
    entry_mask = fwd["date"] == entry_date
    if not entry_mask.any():
        return None  # entry date missing — delisted or trading holiday

    entry_idx          = fwd.index[entry_mask][0]
    entry_open_actual  = float(fwd.loc[entry_idx, "open"])

    if pd.isna(entry_open_actual) or entry_open_actual <= 0:
        return None

    # ── Stop price calculation ────────────────────────────────────────────────
    stop_mode       = str(stop_cfg.get("mode", "atr"))
    atr_mult        = float(stop_cfg.get("atr_multiplier", 2.0))
    fixed_stop_pct  = float(stop_cfg.get("fixed_stop_pct", 0.06))
    gap_protection  = bool(stop_cfg.get("gap_protection", True))

    if stop_mode == "atr" and atr_14 is not None and not pd.isna(atr_14) and float(atr_14) > 0:
        stop_distance = float(atr_14) * atr_mult
    else:
        # Fallback to fixed % if ATR is missing or zero
        stop_distance = entry_open_actual * fixed_stop_pct

    # Safety floor: stop distance must be at least 0.5% of entry price
    # Prevents near-zero stops on stable low-volatility tickers
    min_stop_distance = entry_open_actual * 0.005
    stop_distance     = max(stop_distance, min_stop_distance)
    stop_price        = entry_open_actual - stop_distance

    # ── Position sizing (risk-based) ──────────────────────────────────────────
    # Risk per trade = account_equity * risk_pct * gate_risk_multiplier
    # Shares = floor(risk_dollars / stop_distance_per_share)
    account_equity     = float(sizing_cfg.get("account_equity", 10_000.0))
    risk_pct           = float(sizing_cfg.get("risk_pct_per_trade", 0.01))
    min_shares         = int(sizing_cfg.get("min_shares", 1))

    # Gate risk multiplier scales position size by sector regime strength
    effective_risk_pct = risk_pct * gate_risk_mult
    risk_dollars       = account_equity * effective_risk_pct

    shares   = int(risk_dollars / stop_distance)
    shares   = max(shares, min_shares)
    notional = shares * entry_open_actual

    # ── Exit configuration ────────────────────────────────────────────────────
    stage9_exit_enabled = bool(exit_cfg.get("stage9_exit_enabled", True))
    time_stop_enabled   = bool(exit_cfg.get("time_stop_enabled", True))
    time_stop_days      = int(exit_cfg.get("time_stop_days", 60))

    # ── Forward simulation ────────────────────────────────────────────────────
    # Slice from entry_idx onwards (inclusive of entry day for gap check)
    forward_slice = fwd.iloc[entry_idx:].copy().reset_index(drop=True)

    exit_date   = None
    exit_price  = None
    exit_reason = "end_of_data"
    hold_days   = 0

    for j, row in forward_slice.iterrows():
        current_date  = row["date"]
        current_open  = float(row["open"])  if not pd.isna(row.get("open",  np.nan)) else entry_open_actual
        current_high  = float(row["high"])  if not pd.isna(row.get("high",  np.nan)) else entry_open_actual
        current_low   = float(row["low"])   if not pd.isna(row.get("low",   np.nan)) else entry_open_actual
        current_close = float(row["close"]) if not pd.isna(row.get("close", np.nan)) else entry_open_actual
        current_stage = int(row["stage"])   if not pd.isna(row.get("stage", np.nan)) else 8

        # ── 1. Gap protection: open already below stop (e.g. overnight gap down) ──
        # Only applies from day 1 onwards (j=0 is the entry bar itself)
        if j > 0 and gap_protection and current_open <= stop_price:
            exit_date   = current_date
            exit_price  = current_open      # exit at the gapped-down open price
            exit_reason = "stop_gap"
            hold_days   = j
            break

        # ── 2. Intraday stop hit: low of the day touched the stop price ───────
        if j > 0 and current_low <= stop_price:
            exit_date   = current_date
            exit_price  = stop_price        # filled at the stop price
            exit_reason = "stop_hit"
            hold_days   = j
            break

        # ── 3. Stage 9 exit: detect fade on close, exit at next open ─────────
        # Signal observed at end of day; order sent for next morning's open
        if j > 0 and stage9_exit_enabled and current_stage == 9:
            if j + 1 < len(forward_slice):
                next_row   = forward_slice.iloc[j + 1]
                exit_date  = next_row["date"]
                exit_price = float(next_row["open"]) if not pd.isna(next_row.get("open", np.nan)) else current_close
                exit_reason = "stage9_exit"
                hold_days   = j + 1
            else:
                # Last bar of data: exit at current close
                exit_date   = current_date
                exit_price  = current_close
                exit_reason = "stage9_exit_eod"
                hold_days   = j
            break

        # ── 4. Time stop: max hold cap reached ───────────────────────────────
        # Count from j=1 (day after entry) to avoid counting the entry day
        if j > 0 and time_stop_enabled and j >= time_stop_days:
            if j + 1 < len(forward_slice):
                next_row   = forward_slice.iloc[j + 1]
                exit_date  = next_row["date"]
                exit_price = float(next_row["open"]) if not pd.isna(next_row.get("open", np.nan)) else current_close
                exit_reason = "time_stop"
                hold_days   = j + 1
            else:
                exit_date   = current_date
                exit_price  = current_close
                exit_reason = "time_stop_eod"
                hold_days   = j
            break

    else:
        # Loop exhausted without hitting any exit condition
        last_row    = forward_slice.iloc[-1]
        exit_date   = last_row["date"]
        exit_price  = float(last_row["close"]) if not pd.isna(last_row.get("close", np.nan)) else entry_open_actual
        exit_reason = "end_of_data"
        hold_days   = max(0, len(forward_slice) - 1)

    # ── PnL calculations ──────────────────────────────────────────────────────
    if exit_price is None or pd.isna(exit_price):
        exit_price = entry_open_actual

    pnl_per_share = exit_price - entry_open_actual
    pnl_dollar    = pnl_per_share * shares
    pnl_pct       = (pnl_per_share / entry_open_actual) * 100.0
    # R-multiple: how many R (risk units) did this trade return?
    pnl_r         = pnl_per_share / stop_distance if stop_distance > 0 else 0.0

    return {
        "ticker":            ticker,
        "signal_date":       signal_date,
        "signal_type":       signal_type,
        "entry_date":        entry_date,
        "entry_price":       round(entry_open_actual, 4),
        "stop_price":        round(stop_price, 4),
        "stop_mode":         stop_mode,
        "stop_distance":     round(stop_distance, 4),
        "shares":            shares,
        "notional":          round(notional, 2),
        "exit_date":         exit_date,
        "exit_price":        round(float(exit_price), 4),
        "exit_reason":       exit_reason,
        "hold_days":         hold_days,
        "pnl_dollar":        round(pnl_dollar, 2),
        "pnl_pct":           round(pnl_pct, 4),
        "pnl_r":             round(pnl_r, 4),
        "gate_risk_mult":    gate_risk_mult,
        "spider_id":         signal.get("spider_id", None),
        "atr_14":            round(float(atr_14), 4) if atr_14 is not None and not pd.isna(atr_14) else None,
    }
