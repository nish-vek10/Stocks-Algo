# Path: backtest/metrics.py
"""
ALGO-STOCKS Phase 09 — Performance Metrics

Computes the full quant research metrics suite from a trades DataFrame.
Used by both per-ticker and universe-level reporting.

Metrics included:
  - Trade counts and win/loss split
  - Win rate, avg win/loss %, best/worst trade
  - R-multiples (profit factor, expectancy)
  - Net return, max drawdown
  - Sharpe ratio, Sortino ratio, Calmar ratio
  - Hold time statistics
  - Entry stage breakdown (Stage 6 vs Stage 7)
  - Exit reason breakdown

Notes on Sharpe/Sortino:
  These are computed at the trade level (per-trade return %, not daily).
  This is a proxy — daily-level ratios require a daily equity curve.
  With few trades per ticker the values are directional only.
  Use universe-level metrics for statistically meaningful Sharpe.
"""
from __future__ import annotations

from typing import Union

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Main metrics entry point
# ─────────────────────────────────────────────────────────────────────────────

def compute_metrics(trades: pd.DataFrame, account_equity: float = 10_000.0) -> dict:
    """
    Compute the full performance metrics suite from a trades DataFrame.

    Parameters
    ----------
    trades         : DataFrame with columns from engine.simulate_trade()
    account_equity : starting equity for return and drawdown calculations

    Returns
    -------
    dict of all metrics (safe to JSON-serialise)
    """
    if trades is None or trades.empty:
        return _empty_metrics()

    total_trades = len(trades)
    if total_trades == 0:
        return _empty_metrics()

    # ── PnL series ────────────────────────────────────────────────────────────
    pnl_dollar = trades["pnl_dollar"].fillna(0.0)
    pnl_pct    = trades["pnl_pct"].fillna(0.0)
    pnl_r      = trades["pnl_r"].fillna(0.0)

    # ── Win / Loss split ──────────────────────────────────────────────────────
    wins        = pnl_dollar[pnl_dollar > 0]
    losses      = pnl_dollar[pnl_dollar <= 0]
    win_pct_s   = pnl_pct[pnl_pct > 0]
    loss_pct_s  = pnl_pct[pnl_pct <= 0]
    win_r_s     = pnl_r[pnl_r > 0]
    loss_r_s    = pnl_r[pnl_r <= 0]

    win_count   = int(len(wins))
    loss_count  = int(len(losses))
    win_rate    = (win_count / total_trades * 100.0) if total_trades > 0 else 0.0

    # ── Return statistics ─────────────────────────────────────────────────────
    avg_win_pct  = float(win_pct_s.mean())  if not win_pct_s.empty  else 0.0
    avg_loss_pct = float(loss_pct_s.mean()) if not loss_pct_s.empty else 0.0
    best_trade   = float(pnl_pct.max())     if not pnl_pct.empty    else 0.0
    worst_trade  = float(pnl_pct.min())     if not pnl_pct.empty    else 0.0

    gross_wins   = float(wins.sum())         if not wins.empty       else 0.0
    gross_losses = float(abs(losses.sum()))  if not losses.empty     else 0.0

    # Profit factor: gross wins / gross losses (>1 is profitable system)
    profit_factor: Union[float, str] = (
        round(gross_wins / gross_losses, 4) if gross_losses > 0 else "inf"
    )

    # ── R-multiple statistics ─────────────────────────────────────────────────
    avg_win_r       = float(win_r_s.mean())   if not win_r_s.empty   else 0.0
    avg_loss_r      = float(loss_r_s.mean())  if not loss_r_s.empty  else 0.0
    expectancy_r    = float(pnl_r.mean())     if not pnl_r.empty     else 0.0  # avg R per trade

    # ── Net return ────────────────────────────────────────────────────────────
    net_pnl_dollar = float(pnl_dollar.sum())
    net_return_pct = (net_pnl_dollar / account_equity) * 100.0 if account_equity > 0 else 0.0

    # ── Drawdown ──────────────────────────────────────────────────────────────
    max_dd_pct = _compute_max_drawdown(trades, account_equity)

    # ── Risk-adjusted ratios ──────────────────────────────────────────────────
    sharpe  = _compute_sharpe(pnl_pct)
    sortino = _compute_sortino(pnl_pct)
    calmar: Union[float, str] = (
        round(net_return_pct / abs(max_dd_pct), 4)
        if max_dd_pct != 0 else "inf"
    )

    # ── Hold time statistics ──────────────────────────────────────────────────
    hold_days  = trades["hold_days"].dropna()
    avg_hold   = float(hold_days.mean()) if not hold_days.empty else 0.0
    max_hold   = int(hold_days.max())    if not hold_days.empty else 0
    min_hold   = int(hold_days.min())    if not hold_days.empty else 0

    # ── Breakdown by entry stage and exit reason ──────────────────────────────
    stage6_entries  = int((trades["signal_type"] == "stage6_entry").sum())
    stage7_entries  = int((trades["signal_type"] == "stage7_entry").sum())
    exit_reasons    = trades["exit_reason"].value_counts().to_dict()

    # ── Mark incomplete trades (end_of_data means no clean exit) ─────────────
    incomplete_mask  = trades["exit_reason"].isin(
        ["end_of_data", "time_stop_eod", "stage9_exit_eod"]
    )
    closed_trades    = int((~incomplete_mask).sum())
    incomplete_count = int(incomplete_mask.sum())

    return {
        # Trade counts
        "total_trades":       total_trades,
        "closed_trades":      closed_trades,
        "incomplete_trades":  incomplete_count,
        "winning_trades":     win_count,
        "losing_trades":      loss_count,

        # Win/loss metrics
        "win_rate_pct":       round(win_rate, 2),
        "avg_win_pct":        round(avg_win_pct, 4),
        "avg_loss_pct":       round(avg_loss_pct, 4),
        "best_trade_pct":     round(best_trade, 4),
        "worst_trade_pct":    round(worst_trade, 4),

        # R-multiple metrics
        "avg_win_r":          round(avg_win_r, 4),
        "avg_loss_r":         round(avg_loss_r, 4),
        "expectancy_r":       round(expectancy_r, 4),   # positive = edge exists

        # Dollar metrics
        "profit_factor":      profit_factor,
        "gross_wins_usd":     round(gross_wins, 2),
        "gross_losses_usd":   round(gross_losses, 2),
        "net_pnl_usd":        round(net_pnl_dollar, 2),
        "net_return_pct":     round(net_return_pct, 4),

        # Risk metrics
        "max_drawdown_pct":   round(max_dd_pct, 4),
        "sharpe_ratio":       round(sharpe, 4),
        "sortino_ratio":      round(sortino, 4),
        "calmar_ratio":       calmar,

        # Hold time
        "avg_hold_days":      round(avg_hold, 1),
        "max_hold_days":      max_hold,
        "min_hold_days":      min_hold,

        # Entry stage split (research: which entry stage performs better?)
        "stage6_entries":     stage6_entries,
        "stage7_entries":     stage7_entries,

        # Exit reason breakdown (research: what's driving exits?)
        "exit_reasons":       exit_reasons,

        # Context
        "account_equity":     account_equity,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Equity curve builder
# ─────────────────────────────────────────────────────────────────────────────

def build_equity_curve(trades: pd.DataFrame, account_equity: float = 10_000.0) -> pd.DataFrame:
    """
    Build an equity curve from the sequence of closed trades.

    Returns a step-function equity curve — equity updates at each trade exit.
    For Phase 09 this is sufficient for per-ticker and universe visualisation.

    Columns: date, equity, daily_return_pct, cumulative_return_pct
    """
    if trades is None or trades.empty:
        return pd.DataFrame(columns=["date", "equity", "daily_return_pct", "cumulative_return_pct"])

    sorted_t = trades.sort_values("exit_date").dropna(subset=["exit_date", "pnl_dollar"]).copy()

    if sorted_t.empty:
        return pd.DataFrame(columns=["date", "equity", "daily_return_pct", "cumulative_return_pct"])

    records = []
    equity  = account_equity
    for _, row in sorted_t.iterrows():
        equity += float(row["pnl_dollar"])
        records.append({
            "date":   row["exit_date"],
            "equity": round(equity, 2),
        })

    eq_df = pd.DataFrame(records)
    eq_df["date"]   = pd.to_datetime(eq_df["date"])
    eq_df           = eq_df.sort_values("date").reset_index(drop=True)

    # Daily return pct between trade exits (step function)
    prev_equity                  = pd.Series([account_equity] + eq_df["equity"].tolist()[:-1])
    eq_df["daily_return_pct"]    = ((eq_df["equity"] - prev_equity.values) / prev_equity.values * 100.0).round(4)
    eq_df["cumulative_return_pct"] = ((eq_df["equity"] - account_equity) / account_equity * 100.0).round(4)

    return eq_df


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _empty_metrics() -> dict:
    """Return a zeroed metrics dict for tickers with no trades."""
    return {
        "total_trades": 0, "closed_trades": 0, "incomplete_trades": 0,
        "winning_trades": 0, "losing_trades": 0,
        "win_rate_pct": 0.0, "avg_win_pct": 0.0, "avg_loss_pct": 0.0,
        "best_trade_pct": 0.0, "worst_trade_pct": 0.0,
        "avg_win_r": 0.0, "avg_loss_r": 0.0, "expectancy_r": 0.0,
        "profit_factor": 0.0, "gross_wins_usd": 0.0, "gross_losses_usd": 0.0,
        "net_pnl_usd": 0.0, "net_return_pct": 0.0,
        "max_drawdown_pct": 0.0, "sharpe_ratio": 0.0, "sortino_ratio": 0.0,
        "calmar_ratio": 0.0,
        "avg_hold_days": 0.0, "max_hold_days": 0, "min_hold_days": 0,
        "stage6_entries": 0, "stage7_entries": 0,
        "exit_reasons": {}, "account_equity": 0.0,
    }


def _compute_max_drawdown(trades: pd.DataFrame, start_equity: float) -> float:
    """
    Compute maximum drawdown % from the cumulative PnL series.
    Sequence is ordered by entry_date to simulate realistic trade ordering.
    """
    if trades.empty:
        return 0.0

    sorted_t   = trades.sort_values("entry_date").copy()
    cum_pnl    = sorted_t["pnl_dollar"].fillna(0.0).cumsum()
    equity_seq = pd.concat([
        pd.Series([start_equity]),
        pd.Series(start_equity + cum_pnl.values)
    ], ignore_index=True)

    running_max = equity_seq.cummax()
    drawdown    = (equity_seq - running_max) / running_max * 100.0
    return float(drawdown.min())


def _compute_sharpe(
    pnl_pct: pd.Series,
    risk_free: float = 0.0,
    ann_factor: float = 252.0,
) -> float:
    """
    Trade-level Sharpe ratio (annualised proxy).

    NOTE: This uses per-trade returns, not daily returns.
    The ann_factor of 252 is an approximation assuming ~1 trade per day
    on average. For low-trade-count tickers treat this as directional only.
    Universe-level Sharpe from 09B is more meaningful.
    """
    if pnl_pct.empty or len(pnl_pct) < 2:
        return 0.0
    std = pnl_pct.std()
    if std == 0:
        return 0.0
    excess_mean = pnl_pct.mean() - risk_free
    return float((excess_mean / std) * np.sqrt(ann_factor))


def _compute_sortino(
    pnl_pct: pd.Series,
    risk_free: float = 0.0,
    ann_factor: float = 252.0,
) -> float:
    """
    Trade-level Sortino ratio (annualised proxy).
    Uses only downside deviation (negative returns) in the denominator.
    Better measure for long-only strategies than Sharpe.
    """
    if pnl_pct.empty or len(pnl_pct) < 2:
        return 0.0
    downside = pnl_pct[pnl_pct < risk_free]
    if downside.empty or downside.std() == 0:
        return 0.0
    excess_mean   = pnl_pct.mean() - risk_free
    downside_std  = downside.std()
    return float((excess_mean / downside_std) * np.sqrt(ann_factor))
