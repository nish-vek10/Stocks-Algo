# Path: research/experiments/09A_generate_raw_signals.py
"""
ALGO-STOCKS Phase 09A — Raw Signal Generator (Layer 1)

PURPOSE
-------
Scans all ticker stage histories and extracts every valid entry signal.
This is the COMPUTE-HEAVY layer that runs once (or when stages change).
The output is a flat parquet file reused many times by 09B.

WHAT MAKES THIS THE RIGHT ARCHITECTURE
---------------------------------------
With 2,831 tickers and 1,275 daily bars each, regenerating signals on
every stop/sizing experiment is wasteful. Signals are a function of
stage transitions only — not of stop levels or position sizing.
Separating them enables fast Layer 2 iteration.

SIGNAL DETECTION LOGIC
-----------------------
1. Find every bar where stage transitions INTO Stage 6 or Stage 7
   from a non-entry state (transition detection, not level detection)
2. Enforce dislocation prerequisite: Stage 2 must have occurred at
   any point STRICTLY BEFORE the signal bar (expanding window, point-
   in-time safe — same bar does not count as "before")
3. Pre-compute ATR(14) on the signal bar for Layer 2 stop sizing
4. Pre-fetch entry_date (next bar's date) and entry_open (next bar's open)
5. Pre-join spider gate info from 07G for the entry_date

RE-RUN CONDITIONS
-----------------
Re-run 09A only if:
  - 08B stage classifications change (stages/*.parquet updated)
  - 07G spider gate changes (spider_gate_daily.parquet updated)
  - config/backtest.yaml entry_stages or require_stage2_history changes

OUTPUTS
-------
output/signals/raw_signals_all.parquet   — full signal table
output/signals/raw_signals_summary.json  — summary statistics + diagnostics

RUN FROM PROJECT ROOT:
  python research/experiments/09A_generate_raw_signals.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# ── Project root resolution ───────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURABLE PATHS — change here if directory layout changes
# ══════════════════════════════════════════════════════════════════════════════
FEATURES_DIR = ROOT / "data" / "cleaned" / "stocks_daily" / "features"
STAGES_DIR   = ROOT / "data" / "cleaned" / "stocks_daily" / "stages"
GATE_FILE    = ROOT / "data" / "cleaned" / "spiders_daily" / "gate" / "spider_gate_daily.parquet"
MEMBERSHIPS  = ROOT / "data" / "metadata" / "spiders" / "spider_memberships.csv"
BACKTEST_CFG = ROOT / "config" / "backtest.yaml"
OUTPUT_DIR   = ROOT / "output" / "signals"
# ══════════════════════════════════════════════════════════════════════════════


def _load_cfg(path: Path) -> dict:
    """Load and return a YAML config file as a dict."""
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Wilder's Average True Range using EWM smoothing.

    True Range = max(High - Low, |High - prev_Close|, |Low - prev_Close|)
    Smoothing  = EWM with alpha = 1/period (Wilder's method)

    ATR is computed on the SIGNAL bar and stored in the signals file
    so Layer 2 can compute stop prices without re-reading feature files.
    """
    high       = df["high"].astype(float)
    low        = df["low"].astype(float)
    close      = df["close"].astype(float)
    prev_close = close.shift(1)

    true_range = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    atr = true_range.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    return atr


def _extract_signals_for_ticker(
    ticker:          str,
    features_df:     pd.DataFrame,
    stages_df:       pd.DataFrame,
    entry_stages:    list,
    require_stage2:  bool,
    atr_period:      int,
) -> pd.DataFrame:
    """
    Extract all entry signals for a single ticker.

    Returns a DataFrame of signals (may be empty if no valid transitions found).

    Point-in-time safety guarantees:
    - stage2_ever_before uses shift(1) so the Stage 2 bar itself is not
      counted as "history" — only days AFTER it are eligible
    - entry_date/entry_open are the NEXT bar's values (next trading day open)
    - Signals on the last available bar are dropped (no next bar to enter on)
    """
    if features_df.empty or stages_df.empty:
        return pd.DataFrame()

    # Align dates
    feat = features_df.copy()
    stg  = stages_df[["date", "stage", "stage_name", "stage_reason"]].copy()
    feat["date"] = pd.to_datetime(feat["date"])
    stg["date"]  = pd.to_datetime(stg["date"])

    merged = (
        feat.merge(stg, on="date", how="inner")
        .sort_values("date")
        .reset_index(drop=True)
    )

    if merged.empty:
        return pd.DataFrame()

    # ── ATR(14) — computed on full history for valid warmup ──────────────────
    merged["atr_14"] = _compute_atr(merged, period=atr_period)

    # ── Stage 2 prerequisite (expanding, point-in-time safe) ─────────────────
    # shift(1): stage2_ever_before[i] is True iff stage==2 occurred on day j < i
    # cummax(): once True, stays True for all future rows
    merged["_stage2_flag"]    = (merged["stage"] == 2).astype(int)
    merged["stage2_ever_before"] = (
        merged["_stage2_flag"]
        .shift(1)
        .fillna(0)
        .astype(int)
        .cummax()
        .astype(bool)
    )

    # ── Transition detection: stage enters entry zone from outside ────────────
    # prev_stage = -1 sentinel on first row (never in entry_stages)
    merged["prev_stage"] = merged["stage"].shift(1).fillna(-1).astype(int)

    merged["is_signal"] = (
        merged["stage"].isin(entry_stages) &           # currently in entry stage
        (~merged["prev_stage"].isin(entry_stages))     # previous bar was NOT in entry stage
    )

    # Apply dislocation prerequisite if configured
    if require_stage2:
        merged["is_signal"] = merged["is_signal"] & merged["stage2_ever_before"]

    # ── Pre-fetch next bar's date and open (entry_date and entry_open) ────────
    # These are computed on the FULL merged frame before filtering signals
    # so shift(-1) correctly references the row immediately after each signal
    merged["_next_date"] = merged["date"].shift(-1)
    merged["_next_open"] = merged["open"].shift(-1)

    signal_rows = merged[merged["is_signal"]].copy()

    if signal_rows.empty:
        return pd.DataFrame()

    # Drop signals with no next bar (last row of history — no entry possible)
    signal_rows = signal_rows.dropna(subset=["_next_date", "_next_open"])

    if signal_rows.empty:
        return pd.DataFrame()

    # ── Build output DataFrame ────────────────────────────────────────────────
    out = pd.DataFrame({
        "ticker":             ticker,
        "signal_date":        signal_rows["date"].values,
        "signal_type":        signal_rows["stage"].apply(
                                  lambda s: "stage6_entry" if int(s) == 6 else "stage7_entry"
                              ).values,
        "entry_date":         signal_rows["_next_date"].values,
        "entry_open":         signal_rows["_next_open"].values,

        # Price context on the signal bar
        "close_on_signal":    signal_rows["close"].values,
        "atr_14":             signal_rows["atr_14"].values,
        "donch_high_20":      signal_rows["donch_high_20"].values,
        "ema10":              signal_rows["ema10"].values,
        "ema20":              signal_rows["ema20"].values,
        "ema50":              signal_rows["ema50"].values,
        "ema200":             signal_rows["ema200"].values,
        "rsi":                signal_rows["rsi"].values,
        "macd_hist":          signal_rows["macd_hist"].values,
        "vol_surge":          signal_rows["vol_surge"].values,

        # Transition context
        "stage_before":       signal_rows["prev_stage"].values,
        "stage_reason":       signal_rows["stage_reason"].values,
        "stage2_ever_before": signal_rows["stage2_ever_before"].values,
    })

    return out.reset_index(drop=True)


def main() -> None:
    print("=" * 70)
    print("09A — RAW SIGNAL GENERATOR (Layer 1)")
    print("=" * 70)
    ts_start = datetime.now()

    # ── Load config ───────────────────────────────────────────────────────────
    cfg         = _load_cfg(BACKTEST_CFG)
    run_cfg     = cfg.get("run", {})
    signal_cfg  = cfg.get("signal", {})
    stop_cfg    = cfg.get("stop", {})

    smoke_test     = bool(run_cfg.get("smoke_test", True))
    smoke_tickers  = list(run_cfg.get("smoke_tickers", ["AAPL", "MSFT", "NVDA", "JPM", "XOM"]))
    entry_stages   = list(signal_cfg.get("entry_stages", [6, 7]))
    require_stage2 = bool(signal_cfg.get("require_stage2_history", True))
    atr_period     = int(stop_cfg.get("atr_period", 14))

    print(f"Config loaded from : {BACKTEST_CFG}")
    print(f"Smoke test         : {smoke_test}")
    print(f"Entry stages       : {entry_stages}")
    print(f"Require Stage 2    : {require_stage2}")
    print(f"ATR period         : {atr_period}")

    # ── Determine ticker list ─────────────────────────────────────────────────
    if smoke_test:
        tickers = smoke_tickers
        print(f"\n[SMOKE] Processing {len(tickers)} tickers: {tickers}")
    else:
        tickers = sorted([
            f.stem for f in FEATURES_DIR.glob("*.parquet")
            if not f.stem.startswith("_")
        ])
        print(f"\n[FULL UNIVERSE] {len(tickers)} tickers found in {FEATURES_DIR}")

    # ── Load spider memberships (ticker → spider_id mapping) ──────────────────
    print(f"\nLoading spider memberships: {MEMBERSHIPS}")
    memberships      = pd.read_csv(MEMBERSHIPS)[["ticker", "spider_id"]]
    ticker_to_spider = dict(zip(memberships["ticker"], memberships["spider_id"]))
    print(f"  {len(ticker_to_spider)} ticker → spider_id mappings loaded")

    # ── Load spider gate table ────────────────────────────────────────────────
    gate_df = None
    if GATE_FILE.exists():
        print(f"Loading spider gate : {GATE_FILE}")
        gate_df             = pd.read_parquet(GATE_FILE)
        gate_df["date"]     = pd.to_datetime(gate_df["date"])
        print(f"  {len(gate_df)} rows, {gate_df['spider_id'].nunique()} spiders")
    else:
        print(f"[WARNING] Gate file not found — gate columns will be defaulted to allow/1.0")

    # ── Process each ticker ───────────────────────────────────────────────────
    all_signals      = []
    skipped_missing  = []
    skipped_no_signal = []
    errors           = []

    for i, ticker in enumerate(tickers, 1):
        feat_path  = FEATURES_DIR / f"{ticker}.parquet"
        stage_path = STAGES_DIR   / f"{ticker}.parquet"

        if not feat_path.exists() or not stage_path.exists():
            skipped_missing.append(ticker)
            continue

        try:
            feat_df  = pd.read_parquet(feat_path)
            stage_df = pd.read_parquet(stage_path)

            sigs = _extract_signals_for_ticker(
                ticker         = ticker,
                features_df    = feat_df,
                stages_df      = stage_df,
                entry_stages   = entry_stages,
                require_stage2 = require_stage2,
                atr_period     = atr_period,
            )

            if sigs.empty:
                skipped_no_signal.append(ticker)
                continue

            # Attach spider_id from membership lookup
            sigs["spider_id"] = ticker_to_spider.get(ticker, None)
            all_signals.append(sigs)

        except Exception as e:
            errors.append({"ticker": ticker, "error": str(e)})

        if i % 200 == 0:
            print(f"  ... {i}/{len(tickers)} tickers processed")

    print(f"\nExtraction complete:")
    print(f"  Processed        : {len(tickers)}")
    print(f"  With signals     : {len(all_signals)}")
    print(f"  No signals found : {len(skipped_no_signal)}")
    print(f"  Missing files    : {len(skipped_missing)}")
    print(f"  Errors           : {len(errors)}")

    if not all_signals:
        print("\n[ERROR] No signals generated. Check stage classifications (08B output).")
        sys.exit(1)

    # ── Save no-signal tickers for research audit ─────────────────────────────
    # These are tickers that either never hit Stage 2 (dislocation prerequisite
    # blocked them) or had insufficient history. Useful for understanding which
    # part of the universe the strategy ignores and why.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    no_signal_tickers = sorted(list(set(tickers) - set(
        [t for sigs in all_signals for t in [sigs["ticker"].iloc[0]]]
    )))
    no_signal_out = OUTPUT_DIR / "no_signal_tickers.json"
    with no_signal_out.open("w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "total_universe": len(tickers),
            "no_signal_count": len(no_signal_tickers),
            "note": "These tickers either never printed Stage 2 (dislocation prerequisite) or had insufficient OHLCV history for indicators to warm up.",
            "tickers": no_signal_tickers,
        }, f, indent=2)
    print(f"  No-signal tickers: {no_signal_out}  ({len(no_signal_tickers)} tickers)")

    # ── Concatenate all signals ───────────────────────────────────────────────
    signals_df                = pd.concat(all_signals, ignore_index=True)
    signals_df["signal_date"] = pd.to_datetime(signals_df["signal_date"])
    signals_df["entry_date"]  = pd.to_datetime(signals_df["entry_date"])

    print(f"\nTotal signals before gate join : {len(signals_df)}")

    # ── Join spider gate info on (entry_date, spider_id) ─────────────────────
    # Pre-joined here so Layer 2 (09B) does not need to re-read the gate file
    # at all — the decision info travels with each signal row
    if gate_df is not None:
        gate_slim = (
            gate_df[["date", "spider_id", "allowed", "risk_mult"]]
            .rename(columns={
                "date":      "entry_date",
                "allowed":   "gate_allowed",
                "risk_mult": "gate_risk_mult",
            })
        )
        signals_df = signals_df.merge(gate_slim, on=["entry_date", "spider_id"], how="left")

        # Default values for unmatched (e.g. ticker not in any spider)
        # infer_objects() suppresses pandas FutureWarning on boolean downcasting
        signals_df["gate_allowed"] = (
            signals_df["gate_allowed"]
            .fillna(True)
            .infer_objects(copy=False)
            .astype(bool)
        )
        signals_df["gate_risk_mult"] = (
            signals_df["gate_risk_mult"]
            .fillna(1.0)
            .infer_objects(copy=False)
            .astype(float)
        )

        matched = signals_df["gate_allowed"].notna().sum()
        print(f"Gate join: {matched}/{len(signals_df)} signals matched to gate data")
    else:
        signals_df["gate_allowed"]   = True
        signals_df["gate_risk_mult"] = 1.0

    # ── Write outputs ─────────────────────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    out_parquet = OUTPUT_DIR / "raw_signals_all.parquet"
    signals_df.to_parquet(out_parquet, index=False)

    # ── Print signal stats ────────────────────────────────────────────────────
    n_s6 = int((signals_df["signal_type"] == "stage6_entry").sum())
    n_s7 = int((signals_df["signal_type"] == "stage7_entry").sum())

    print(f"\n{'─' * 50}")
    print(f"  Output file     : {out_parquet}")
    print(f"  Total signals   : {len(signals_df)}")
    print(f"  Stage 6 entries : {n_s6}  ({n_s6/len(signals_df)*100:.1f}%)")
    print(f"  Stage 7 entries : {n_s7}  ({n_s7/len(signals_df)*100:.1f}%)")
    print(f"  Unique tickers  : {signals_df['ticker'].nunique()}")
    print(f"  Date range      : {signals_df['signal_date'].min().date()} → "
          f"{signals_df['signal_date'].max().date()}")
    print(f"{'─' * 50}")

    # ── Write summary JSON ────────────────────────────────────────────────────
    elapsed  = (datetime.now() - ts_start).total_seconds()
    summary  = {
        "generated_at":          datetime.now().isoformat(),
        "elapsed_seconds":       round(elapsed, 1),
        "smoke_test":            smoke_test,
        "tickers_requested":     len(tickers),
        "tickers_with_signals":  int(signals_df["ticker"].nunique()),
        "total_signals":         len(signals_df),
        "stage6_signals":        n_s6,
        "stage7_signals":        n_s7,
        "config": {
            "entry_stages":      entry_stages,
            "require_stage2":    require_stage2,
            "atr_period":        atr_period,
        },
        "signal_date_range": {
            "first": str(signals_df["signal_date"].min().date()),
            "last":  str(signals_df["signal_date"].max().date()),
        },
        "skipped_missing_files": skipped_missing,
        "skipped_no_signals":    len(skipped_no_signal),
        "no_signal_tickers_file": str(no_signal_out),
        "errors":                errors,
    }

    out_json = OUTPUT_DIR / "raw_signals_summary.json"
    with out_json.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"  Summary JSON    : {out_json}")
    print(f"  Elapsed         : {elapsed:.1f}s")
    print("=" * 70)
    print("09A COMPLETE — Run 09B to simulate trades")
    print("=" * 70)


if __name__ == "__main__":
    main()
