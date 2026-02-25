# Path: research/experiments/09C_run_portfolio.py
"""
ALGO-STOCKS Phase 09C — Portfolio Simulation Runner

PURPOSE
-------
Takes a completed 09B universe run and simulates all trades through a single
shared capital pool. Produces real portfolio metrics: true drawdown, true
Sharpe/Sortino, equity curve, sector exposure, and position log.

PREREQUISITE
------------
Must have run 09B with smoke_test: false first.
Set portfolio.source_run_tag in config/backtest.yaml to the 09B run folder name.

HOW IT RELATES TO 09A AND 09B
------------------------------
09A — generates signals (compute-heavy, run once)
09B — simulates each ticker independently (fast, run freely)
09C — combines all 09B trades into one portfolio (runs in seconds)

09C reads trades_all.parquet from 09B and re-sizes every trade using current
portfolio equity. It does NOT re-run signal detection or trade simulation.

RESEARCH WORKFLOW
-----------------
1. Run 09B with desired parameters → produces trades_all.parquet
2. Edit config/backtest.yaml portfolio section (capital, risk, caps)
3. Run this script → produces portfolio/ outputs in same run folder

This means you can run multiple 09C variants on the same 09B run without
re-running 09A or 09B. Just change the portfolio config and re-run 09C.

RUN FROM PROJECT ROOT:
  python research/experiments/09C_run_portfolio.py
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

from backtest.portfolio import run_portfolio_simulation

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURABLE PATHS — change here if directory layout changes
# ══════════════════════════════════════════════════════════════════════════════
BACKTEST_CFG = ROOT / "config" / "backtest.yaml"
BACKTESTS_DIR = ROOT / "output" / "backtests"
# ══════════════════════════════════════════════════════════════════════════════


def _load_cfg(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def main() -> None:
    print("=" * 70)
    print("09C — PORTFOLIO SIMULATION RUNNER")
    print("=" * 70)
    ts_start = datetime.now()

    # ── Load config ───────────────────────────────────────────────────────────
    cfg         = _load_cfg(BACKTEST_CFG)
    p_cfg       = cfg.get("portfolio", {})

    source_run_tag = str(p_cfg.get("source_run_tag", ""))
    capital        = float(p_cfg.get("capital",            100_000.0))
    risk_pct       = float(p_cfg.get("risk_pct_per_trade", 0.005))
    max_positions  = p_cfg.get("max_positions",  None)
    sector_cap_pct = p_cfg.get("sector_cap_pct", None)
    gate_enabled   = bool(p_cfg.get("gate_enabled", False))

    print(f"Source run tag      : {source_run_tag}")
    print(f"Starting capital    : ${capital:,.2f}")
    print(f"Risk per trade      : {risk_pct*100:.2f}% = ${capital*risk_pct:,.0f}")
    print(f"Max positions       : {max_positions if max_positions else 'uncapped'}")
    print(f"Sector cap          : {f'{sector_cap_pct*100:.0f}%' if sector_cap_pct else 'none'}")
    print(f"Gate enabled        : {gate_enabled}")

    # ── Locate source 09B trades_all.parquet ─────────────────────────────────
    if not source_run_tag:
        print("\n[ERROR] portfolio.source_run_tag not set in config/backtest.yaml")
        print("  Set it to the exact 09B run folder name, e.g.:")
        print("  source_run_tag: 'universe_baseline_v1_20260224_2310'")
        sys.exit(1)

    trades_path = BACKTESTS_DIR / source_run_tag / "universe" / "trades_all.parquet"
    if not trades_path.exists():
        print(f"\n[ERROR] trades_all.parquet not found: {trades_path}")
        print("  Check that:")
        print("  1. source_run_tag matches the exact folder name in output/backtests/")
        print("  2. 09B was run with smoke_test: false (full universe)")
        sys.exit(1)

    print(f"\nLoading trades from : {trades_path}")
    trades_df = pd.read_parquet(trades_path)
    print(f"  {len(trades_df):,} trades loaded from {trades_df['ticker'].nunique():,} tickers")

    # ── Drop any trades with invalid stop_distance ────────────────────────────
    before = len(trades_df)
    trades_df = trades_df[
        trades_df["stop_distance"].notna() &
        (trades_df["stop_distance"] > 0)
    ].copy()
    dropped = before - len(trades_df)
    if dropped > 0:
        print(f"  Dropped {dropped} trades with invalid stop_distance")

    # ── Setup output directory ────────────────────────────────────────────────
    portfolio_dir = BACKTESTS_DIR / source_run_tag / "portfolio"
    portfolio_dir.mkdir(parents=True, exist_ok=True)

    # ── Run portfolio simulation ──────────────────────────────────────────────
    print(f"\nRunning portfolio simulation ...")
    result = run_portfolio_simulation(trades_df, cfg)

    portfolio_trades_df  = result["portfolio_trades"]
    equity_df            = result["equity_df"]
    daily_equity_df      = result["daily_equity_df"]
    positions_log_df     = result["positions_log_df"]
    sector_exposure_df   = result["sector_exposure_df"]
    metrics              = result["metrics"]
    blocked_log          = result["blocked_log"]

    # ── Write outputs ─────────────────────────────────────────────────────────
    print(f"\nWriting outputs to  : {portfolio_dir}")

    # portfolio_trades.parquet — all executed trades with portfolio sizing
    if not portfolio_trades_df.empty:
        portfolio_trades_df.to_parquet(
            portfolio_dir / "portfolio_trades.parquet", index=False
        )
        print(f"  portfolio_trades.parquet   : {len(portfolio_trades_df):,} trades")

    # equity.parquet — equity curve at event dates
    if not equity_df.empty:
        equity_df.to_parquet(portfolio_dir / "equity.parquet", index=False)
        print(f"  equity.parquet             : {len(equity_df):,} snapshots")

    # daily_equity.parquet — daily equity curve (forward-filled)
    if not daily_equity_df.empty:
        daily_equity_df.to_parquet(portfolio_dir / "daily_equity.parquet", index=False)
        print(f"  daily_equity.parquet       : {len(daily_equity_df):,} days")

    # positions_log.parquet — open positions per event date
    if not positions_log_df.empty:
        positions_log_df.to_parquet(portfolio_dir / "positions_log.parquet", index=False)
        print(f"  positions_log.parquet      : {len(positions_log_df):,} rows")

    # sector_exposure.parquet — sector capital breakdown per event date
    if not sector_exposure_df.empty:
        sector_exposure_df.to_parquet(
            portfolio_dir / "sector_exposure.parquet", index=False
        )
        print(f"  sector_exposure.parquet    : {len(sector_exposure_df):,} rows")

    # blocked_log.jsonl — trades that were blocked and why
    with (portfolio_dir / "blocked_log.jsonl").open("w", encoding="utf-8") as f:
        for rec in blocked_log:
            f.write(json.dumps(rec, default=str) + "\n")
    print(f"  blocked_log.jsonl          : {len(blocked_log):,} blocked trades")

    # portfolio_report.json — full metrics
    elapsed = (datetime.now() - ts_start).total_seconds()
    metrics.update({
        "source_run_tag":  source_run_tag,
        "generated_at":    datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed, 1),
    })
    with (portfolio_dir / "portfolio_report.json").open("w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"  portfolio_report.json      : written")

    # ── Console summary ───────────────────────────────────────────────────────
    print(f"\n{'═' * 55}")
    print(f"  PORTFOLIO SUMMARY — {source_run_tag}")
    print(f"{'═' * 55}")
    print(f"  Starting capital         : ${capital:>12,.2f}")
    print(f"  Final equity             : ${metrics.get('final_equity', 0):>12,.2f}")
    print(f"  Net PnL                  : ${metrics.get('net_pnl_usd', 0):>12,.2f}")
    print(f"  Net return               : {metrics.get('net_return_pct', 0):>10.2f}%")
    print(f"{'─' * 55}")
    print(f"  Total trades executed    : {metrics.get('total_trades', 0):>10,}")
    print(f"  Trades blocked           : {metrics.get('total_blocked', 0):>10,}")
    print(f"  Win rate                 : {metrics.get('win_rate_pct', 0):>10.1f}%")
    print(f"  Profit factor            : {str(metrics.get('profit_factor', 0)):>10}")
    print(f"  Expectancy R             : {metrics.get('expectancy_r', 0):>10.4f}")
    print(f"{'─' * 55}")
    print(f"  Max drawdown (daily)     : {metrics.get('max_drawdown_pct', 0):>10.2f}%")
    print(f"  Sharpe ratio (daily)     : {metrics.get('sharpe_ratio', 0):>10.4f}")
    print(f"  Sortino ratio (daily)    : {metrics.get('sortino_ratio', 0):>10.4f}")
    print(f"  Calmar ratio             : {metrics.get('calmar_ratio', 0):>10.4f}")
    print(f"{'─' * 55}")
    print(f"  Avg hold days            : {metrics.get('avg_hold_days', 0):>10.1f}")
    print(f"  Avg concurrent positions : {metrics.get('avg_concurrent_positions', 0):>10.1f}")
    print(f"  Max concurrent positions : {metrics.get('max_concurrent_positions', 0):>10}")
    print(f"  Stage 6 entries          : {metrics.get('stage6_entries', 0):>10,}")
    print(f"  Stage 7 entries          : {metrics.get('stage7_entries', 0):>10,}")
    print(f"{'═' * 55}")

    # ── Exit reason breakdown ─────────────────────────────────────────────────
    exit_reasons = metrics.get("exit_reasons", {})
    if exit_reasons:
        print(f"\n  Exit reasons:")
        for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
            pct = count / metrics.get("total_trades", 1) * 100
            print(f"    {reason:<25s} : {count:>6,}  ({pct:.1f}%)")

    # ── Blocked trades breakdown ──────────────────────────────────────────────
    blocked_reasons = metrics.get("blocked_reasons", {})
    if blocked_reasons:
        print(f"\n  Blocked trade reasons:")
        for reason, count in sorted(blocked_reasons.items(), key=lambda x: -x[1]):
            print(f"    {reason:<25s} : {count:>6,}")

    print(f"\n  Elapsed                  : {elapsed:.1f}s")
    print(f"  Outputs                  : {portfolio_dir}")
    print("=" * 70)
    print("09C COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
