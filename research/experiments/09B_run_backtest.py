# Path: research/experiments/09B_run_backtest.py
"""
ALGO-STOCKS Phase 09B — Backtest Runner (Layer 2)

PURPOSE
-------
Reads pre-generated signals from 09A and simulates each trade.
This is the FAST layer — runs in seconds after signals are generated.
Change stop logic, sizing, or gate settings in config/backtest.yaml
and re-run this script without touching 09A.

WHAT THIS DOES
--------------
For each ticker with signals:
  1. Load features + stages parquet (once per ticker)
  2. For each signal, call engine.simulate_trade() with forward data
  3. Collect trades, build equity curve, compute metrics
  4. Write per-ticker outputs (single/) and universe aggregates (universe/)

OUTPUTS
-------
output/backtests/<run_tag>/single/<TICKER>/
    trades.parquet              — trade-by-trade record
    equity.parquet              — equity curve (step function at each exit)
    summary.json                — full metrics for this ticker
    debug_last_200_rows.parquet — last N rows of features+stages (audit)

output/backtests/<run_tag>/universe/
    trades_all.parquet          — all trades concatenated
    summary_by_ticker.csv       — per-ticker metric table
    failures.jsonl              — non-fatal failures with reasons
    universe_report.json        — aggregate metrics across all tickers

RUN FROM PROJECT ROOT:
  python research/experiments/09B_run_backtest.py

PREREQUISITE:
  python research/experiments/09A_generate_raw_signals.py  (must run first)
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

# ── Project root resolution ───────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backtest.engine  import simulate_trade
from backtest.metrics import compute_metrics, build_equity_curve

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURABLE PATHS — change here if directory layout changes
# ══════════════════════════════════════════════════════════════════════════════
FEATURES_DIR = ROOT / "data" / "cleaned" / "stocks_daily" / "features"
STAGES_DIR   = ROOT / "data" / "cleaned" / "stocks_daily" / "stages"
SIGNALS_FILE = ROOT / "output" / "signals" / "raw_signals_all.parquet"
BACKTEST_CFG = ROOT / "config" / "backtest.yaml"
# ══════════════════════════════════════════════════════════════════════════════


def _load_cfg(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def main() -> None:
    print("=" * 70)
    print("09B — BACKTEST RUNNER (Layer 2)")
    print("=" * 70)
    ts_start = datetime.now()

    # ── Load config ───────────────────────────────────────────────────────────
    cfg          = _load_cfg(BACKTEST_CFG)
    run_cfg      = cfg.get("run", {})
    gate_cfg     = cfg.get("spider_gate", {})
    sizing_cfg   = cfg.get("sizing", {})
    output_cfg   = cfg.get("output", {})

    smoke_test        = bool(run_cfg.get("smoke_test", True))
    smoke_tickers     = list(run_cfg.get("smoke_tickers", []))
    gate_enabled      = bool(gate_cfg.get("enabled", False))
    account_equity    = float(sizing_cfg.get("account_equity", 10_000.0))
    debug_rows        = int(output_cfg.get("debug_tail_rows", 200))
    base_dir          = ROOT / str(output_cfg.get("base_dir", "output/backtests"))
    run_tag_prefix    = str(run_cfg.get("run_tag_prefix", "baseline"))
    save_single_files = bool(output_cfg.get("save_single_ticker_files", True))

    # Auto-generate run tag (prefix + timestamp)
    run_tag = f"{run_tag_prefix}_{datetime.now().strftime('%Y%m%d_%H%M')}"

    print(f"Run tag         : {run_tag}")
    print(f"Smoke test      : {smoke_test}")
    print(f"Gate enabled    : {gate_enabled}")
    print(f"Account equity  : ${account_equity:,.2f}")
    print(f"Save per-ticker : {save_single_files}")

    # ── Load signals file (output of 09A) ─────────────────────────────────────
    if not SIGNALS_FILE.exists():
        print(f"\n[ERROR] Signals file not found: {SIGNALS_FILE}")
        print("  Please run 09A_generate_raw_signals.py first.")
        sys.exit(1)

    signals = pd.read_parquet(SIGNALS_FILE)
    signals["signal_date"] = pd.to_datetime(signals["signal_date"])
    signals["entry_date"]  = pd.to_datetime(signals["entry_date"])
    print(f"\nLoaded {len(signals)} signals from {SIGNALS_FILE.name}")

    # ── Smoke test filter (09B-level) ─────────────────────────────────────────
    # Useful when 09A was run for full universe but you want a quick 09B smoke run
    if smoke_test and smoke_tickers:
        before = len(signals)
        signals = signals[signals["ticker"].isin(smoke_tickers)].copy()
        print(f"[SMOKE] Filtered to {len(smoke_tickers)} tickers: "
              f"{len(signals)} signals (was {before})")

    # ── Spider gate filter ────────────────────────────────────────────────────
    signals_gated_out = 0
    if gate_enabled:
        mask              = signals["gate_allowed"] == True
        signals_gated_out = int((~mask).sum())
        signals           = signals[mask].copy()
        print(f"Gate filter: {signals_gated_out} signals blocked, "
              f"{len(signals)} remain")

    if signals.empty:
        print("[WARNING] No signals remain after filtering. Nothing to simulate.")
        return

    # ── Setup output directories ──────────────────────────────────────────────
    run_dir      = base_dir / run_tag
    single_dir   = run_dir / "single"
    universe_dir = run_dir / "universe"
    universe_dir.mkdir(parents=True, exist_ok=True)
    if save_single_files:
        single_dir.mkdir(parents=True, exist_ok=True)

    # Save the config snapshot used for this run (full audit trail)
    with (run_dir / "backtest_config_snapshot.yaml").open("w", encoding="utf-8") as f:
        yaml.dump(cfg, f, default_flow_style=False)

    # ── Per-ticker simulation ─────────────────────────────────────────────────
    tickers = signals["ticker"].unique()
    print(f"\nSimulating trades for {len(tickers)} tickers ...")
    print(f"{'─' * 50}")

    all_trades    = []
    all_summaries = []
    failures      = []

    for i, ticker in enumerate(tickers, 1):
        feat_path  = FEATURES_DIR / f"{ticker}.parquet"
        stage_path = STAGES_DIR   / f"{ticker}.parquet"

        if not feat_path.exists() or not stage_path.exists():
            failures.append({"ticker": ticker, "reason": "missing_features_or_stages_file"})
            continue

        try:
            # Load full ticker data once — forward-slice per signal below
            feat_df  = pd.read_parquet(feat_path)
            stage_df = pd.read_parquet(stage_path)
            feat_df["date"]  = pd.to_datetime(feat_df["date"])
            stage_df["date"] = pd.to_datetime(stage_df["date"])

            ticker_signals = signals[signals["ticker"] == ticker]

            ticker_trades = []
            last_exit_date = pd.Timestamp("1900-01-01")  # sentinel
            signals_skipped_overlap = 0

            # Read overlap mode from config
            overlap_mode = str(sizing_cfg.get("overlap_mode", "disabled"))
            max_scale_ins = int(sizing_cfg.get("max_scale_ins", 2))
            open_count = 0  # tracks how many scale-in legs are currently open

            # Sort signals chronologically
            ticker_signals_sorted = ticker_signals.sort_values("entry_date")

            for _, sig in ticker_signals_sorted.iterrows():
                entry_date = pd.Timestamp(sig["entry_date"])

                if overlap_mode == "disabled":
                    # ── Baseline: one position per ticker at a time ───────────
                    if entry_date <= last_exit_date:
                        signals_skipped_overlap += 1
                        continue

                elif overlap_mode == "scale_in":
                    # ── Scale-in: allow up to max_scale_ins add-ons ───────────
                    # Reset open count when prior cluster has fully closed
                    if entry_date > last_exit_date:
                        open_count = 0
                    if open_count >= (max_scale_ins + 1):
                        signals_skipped_overlap += 1
                        continue

                # Slice forward from entry_date — engine only sees future data
                fwd_feat = feat_df[feat_df["date"] >= entry_date].copy()
                fwd_stage = stage_df[stage_df["date"] >= entry_date].copy()

                if fwd_feat.empty:
                    # Signal at or near end of data — no forward bars to trade
                    continue

                trade = simulate_trade(
                    signal       = sig,
                    fwd_features = fwd_feat,
                    fwd_stages   = fwd_stage,
                    cfg          = cfg,
                )

                if trade is not None:
                    ticker_trades.append(trade)
                    # Track the furthest exit date seen in this cluster
                    trade_exit = pd.Timestamp(trade["exit_date"])
                    if trade_exit > last_exit_date:
                        last_exit_date = trade_exit
                    if overlap_mode == "scale_in":
                        open_count += 1

            # ── Write per-ticker outputs ──────────────────────────────────────
            if not ticker_trades:
                failures.append({"ticker": ticker, "reason": "no_trades_simulated"})
                continue

            trades_df = pd.DataFrame(ticker_trades)
            trades_df["entry_date"] = pd.to_datetime(trades_df["entry_date"])
            trades_df["exit_date"]  = pd.to_datetime(trades_df["exit_date"])

            equity_df = build_equity_curve(trades_df, account_equity)
            summary   = compute_metrics(trades_df, account_equity)
            summary["ticker"] = ticker

            if save_single_files:
                ticker_dir = single_dir / ticker
                ticker_dir.mkdir(parents=True, exist_ok=True)

                trades_df.to_parquet(ticker_dir / "trades.parquet", index=False)
                equity_df.to_parquet(ticker_dir / "equity.parquet", index=False)

                with (ticker_dir / "summary.json").open("w", encoding="utf-8") as f:
                    json.dump(summary, f, indent=2, default=str)

                # Debug file: last N rows of joined features + stages for audit
                debug_df = (
                    feat_df
                    .merge(
                        stage_df[["date", "stage", "stage_name", "stage_reason"]],
                        on="date", how="inner"
                    )
                    .tail(debug_rows)
                )
                debug_df.to_parquet(
                    ticker_dir / "debug_last_200_rows.parquet", index=False
                )

            all_trades.append(trades_df)
            all_summaries.append(summary)

            if i % 10 == 0 or i == len(tickers):
                wins = sum(1 for t in ticker_trades if t["pnl_dollar"] > 0)
                print(f"  [{i:4d}/{len(tickers)}] {ticker:<8s} "
                      f"trades={len(ticker_trades):2d}  "
                      f"wins={wins:2d}")

        except Exception as e:
            failures.append({"ticker": ticker, "reason": str(e)})

    # ── Universe-level outputs ────────────────────────────────────────────────
    print(f"\n{'─' * 50}")
    print(f"Simulation complete:")
    print(f"  Tickers with trades : {len(all_summaries)}")
    print(f"  Failures            : {len(failures)}")

    if all_trades:
        trades_all = pd.concat(all_trades, ignore_index=True)
        trades_all.to_parquet(universe_dir / "trades_all.parquet", index=False)
        print(f"  Total trades        : {len(trades_all)}")

        # Per-ticker summary table (easy to sort in Excel / pandas)
        summary_df = pd.DataFrame(all_summaries)
        # Flatten exit_reasons dict column → won't serialise cleanly to CSV
        summary_df = summary_df.drop(columns=["exit_reasons"], errors="ignore")
        summary_df.to_csv(universe_dir / "summary_by_ticker.csv", index=False)

        # Universe-level aggregate metrics
        universe_metrics = compute_metrics(trades_all, account_equity)
        universe_metrics.update({
            "run_tag":                run_tag,
            "smoke_test":             smoke_test,
            "gate_enabled":           gate_enabled,
            "signals_gated_out":      signals_gated_out,
            "tickers_with_trades":    len(all_summaries),
            "tickers_failed":         len(failures),
            "generated_at":           datetime.now().isoformat(),
            "elapsed_seconds":        round((datetime.now() - ts_start).total_seconds(), 1),
        })

        with (universe_dir / "universe_report.json").open("w", encoding="utf-8") as f:
            json.dump(universe_metrics, f, indent=2, default=str)

        # ── Print quick summary to console ────────────────────────────────────
        wins_total = int((trades_all["pnl_dollar"] > 0).sum())
        print(f"\n{'═' * 50}")
        print(f"  UNIVERSE SUMMARY — {run_tag}")
        print(f"{'═' * 50}")
        print(f"  Total trades    : {len(trades_all)}")
        print(f"  Win rate        : {wins_total/len(trades_all)*100:.1f}%")
        print(f"  Net PnL         : ${universe_metrics['net_pnl_usd']:,.2f}")
        print(f"  Expectancy R    : {universe_metrics['expectancy_r']:.4f}")
        print(f"  Profit factor   : {universe_metrics['profit_factor']}")
        print(f"  Max drawdown    : {universe_metrics['max_drawdown_pct']:.2f}%")
        print(f"  Sharpe (proxy)  : {universe_metrics['sharpe_ratio']:.4f}")
        print(f"  Avg hold days   : {universe_metrics['avg_hold_days']:.1f}")
        print(f"  Stage 6 entries : {universe_metrics['stage6_entries']}")
        print(f"  Stage 7 entries : {universe_metrics['stage7_entries']}")
        print(f"{'═' * 50}")

    # Write failures log (non-fatal — always written even if empty)
    with (universe_dir / "failures.jsonl").open("w", encoding="utf-8") as f:
        for rec in failures:
            f.write(json.dumps(rec) + "\n")

    elapsed = (datetime.now() - ts_start).total_seconds()
    print(f"\nAll outputs written to : {run_dir}")
    print(f"Elapsed               : {elapsed:.1f}s")
    print("=" * 70)
    print(f"09B COMPLETE — run_tag: {run_tag}")
    print("=" * 70)


if __name__ == "__main__":
    main()
