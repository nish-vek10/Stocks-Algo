# Path: backtest/portfolio.py
"""
ALGO-STOCKS Phase 09C — Portfolio Capital Simulation Engine

PURPOSE
-------
Simulates all trades from a 09B universe run through a single shared capital pool.
This is what converts "2,582 independent $10k accounts" into a real portfolio.

WHAT THIS SOLVES
----------------
09B simulates each ticker independently with its own equity.
09C applies:
  - One shared capital pool (e.g. $100,000)
  - Shared position sizing: each trade risks X% of CURRENT portfolio equity
  - Capital tracking: positions consume cash; exits return cash + PnL
  - Concurrent position tracking: how many positions are open at once
  - Sector exposure tracking: capital concentration by spider
  - Optional position cap: block new entries if max_positions is reached
  - Optional sector cap: block if a sector would exceed X% of open notional

EQUITY CURVE METHODOLOGY (CASH BASIS)
--------------------------------------
Equity is computed as:
    equity = cash_balance + sum(entry_notional of all open positions)

This is a cash-basis approach — position value is marked at entry notional,
not at current market price. Equity updates when trades CLOSE (realized PnL).

This is intentional for Phase 09C:
  - Conservative: losses are not reflected until stop is hit
  - No need to load all daily prices for open positions
  - Standard methodology for this type of strategy backtest
  - Mark-to-market daily equity can be added in a future phase

PORTFOLIO TRADE RESIZING
------------------------
Trades from 09B were sized with $10,000 per ticker.
09C re-sizes every trade using:
    risk_dollars   = current_equity × risk_pct_per_trade
    shares         = floor(risk_dollars / stop_distance)
    notional       = shares × entry_price

stop_distance is taken directly from the 09B trade record (already calculated).
This preserves the stop logic exactly while changing only the sizing scale.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Main portfolio simulation entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_portfolio_simulation(trades_df: pd.DataFrame, cfg: dict) -> dict:
    """
    Simulate all trades through a shared capital pool.

    Parameters
    ----------
    trades_df : DataFrame from 09B trades_all.parquet
                Must contain: ticker, entry_date, exit_date, entry_price,
                              exit_price, stop_distance, pnl_pct, signal_type,
                              spider_id, gate_allowed, gate_risk_mult
    cfg       : full backtest config dict (from config/backtest.yaml)

    Returns
    -------
    dict with keys:
        portfolio_trades   : pd.DataFrame — all executed trades with portfolio context
        equity_df          : pd.DataFrame — equity curve (one row per event date)
        daily_equity_df    : pd.DataFrame — equity curve resampled to trading days
        positions_log_df   : pd.DataFrame — open positions snapshot per event date
        sector_exposure_df : pd.DataFrame — sector capital exposure per event date
        metrics            : dict — full portfolio performance metrics
        blocked_log        : list — records of trades that were blocked and why
    """
    p_cfg         = cfg.get("portfolio", {})
    gate_cfg      = cfg.get("spider_gate", {})

    capital         = float(p_cfg.get("capital",             100_000.0))
    risk_pct        = float(p_cfg.get("risk_pct_per_trade",  0.005))
    min_shares      = int(  p_cfg.get("min_shares",          1))
    max_positions   = p_cfg.get("max_positions",  None)   # None = uncapped
    sector_cap_pct  = p_cfg.get("sector_cap_pct", None)   # None = no cap
    gate_enabled    = bool(p_cfg.get("gate_enabled",         False))

    if max_positions is not None:
        max_positions = int(max_positions)
    if sector_cap_pct is not None:
        sector_cap_pct = float(sector_cap_pct)

    # positions_log can grow to millions of rows with large uncapped portfolios
    # disable it for high-capacity runs to avoid MemoryError
    save_positions_log = bool(p_cfg.get("save_positions_log", True))

    # ── Prepare trades ────────────────────────────────────────────────────────
    trades = trades_df.copy()
    trades["entry_date"] = pd.to_datetime(trades["entry_date"])
    trades["exit_date"]  = pd.to_datetime(trades["exit_date"])
    trades = trades.sort_values("entry_date").reset_index(drop=True)

    # ── Build chronological event list ────────────────────────────────────────
    # Exits are processed BEFORE entries on the same date so capital freed by
    # closing positions is available for new entries the same day
    events = []
    for idx, row in trades.iterrows():
        events.append((pd.Timestamp(row["entry_date"]), 1, "entry", idx))
        events.append((pd.Timestamp(row["exit_date"]),  0, "exit",  idx))

    # Sort by (date, priority) — exits (0) before entries (1)
    events.sort(key=lambda x: (x[0], x[1]))

    # ── Portfolio state ───────────────────────────────────────────────────────
    cash              = capital
    open_positions    = {}    # trade_idx → position dict
    portfolio_trades  = []    # all executed trades with portfolio sizing
    blocked_log       = []    # trades blocked with reasons
    equity_snapshots  = []    # (date, equity, cash, n_open)
    positions_log     = []    # snapshot of open positions per event date
    sector_snapshots  = []    # (date, spider_id, notional) per event date

    # ── Process events ────────────────────────────────────────────────────────
    for event_date, _, event_type, trade_idx in events:
        row = trades.iloc[trade_idx]

        if event_type == "exit":
            if trade_idx not in open_positions:
                continue  # Was blocked at entry — nothing to close

            pos       = open_positions.pop(trade_idx)
            exit_px   = float(row["exit_price"])
            pnl_trade = (exit_px - pos["entry_price"]) * pos["shares"]

            # Return cash: shares × exit_price
            cash += pos["shares"] * exit_px

            # Record completed portfolio trade
            pnl_pct = (exit_px - pos["entry_price"]) / pos["entry_price"] * 100.0
            pnl_r   = (exit_px - pos["entry_price"]) / pos["stop_distance"] \
                      if pos["stop_distance"] > 0 else 0.0

            portfolio_trades.append({
                # Identity
                "ticker":             pos["ticker"],
                "spider_id":          pos.get("spider_id"),
                "signal_type":        row.get("signal_type"),
                "signal_date":        row.get("signal_date"),

                # Trade dates
                "entry_date":         pos["entry_date"],
                "exit_date":          event_date,

                # Prices
                "entry_price":        round(pos["entry_price"], 4),
                "exit_price":         round(exit_px, 4),
                "stop_price":         round(pos["entry_price"] - pos["stop_distance"], 4),

                # Portfolio sizing (different from 09B sizing)
                "shares":             pos["shares"],
                "notional":           round(pos["notional"], 2),
                "risk_dollars":       round(pos["risk_dollars"], 2),
                "portfolio_equity_at_entry": round(pos["equity_at_entry"], 2),

                # PnL
                "pnl_dollar":         round(pnl_trade, 2),
                "pnl_pct":            round(pnl_pct, 4),
                "pnl_r":              round(pnl_r, 4),

                # Exit context
                "exit_reason":        row.get("exit_reason"),
                "hold_days":          row.get("hold_days"),
                "gate_risk_mult":     pos.get("gate_risk_mult", 1.0),
                "stop_distance":      round(pos["stop_distance"], 4),
            })

        elif event_type == "entry":
            # ── Gate check ────────────────────────────────────────────────────
            if gate_enabled and not bool(row.get("gate_allowed", True)):
                blocked_log.append({
                    "trade_idx": trade_idx,
                    "ticker":    row["ticker"],
                    "date":      event_date,
                    "reason":    "gate_blocked",
                })
                continue

            # ── Max positions check ───────────────────────────────────────────
            if max_positions is not None and len(open_positions) >= max_positions:
                blocked_log.append({
                    "trade_idx": trade_idx,
                    "ticker":    row["ticker"],
                    "date":      event_date,
                    "reason":    f"max_positions_cap_{max_positions}",
                })
                continue

            # ── Stop distance validation ──────────────────────────────────────
            stop_distance = float(row.get("stop_distance", 0))
            if stop_distance <= 0:
                blocked_log.append({
                    "trade_idx": trade_idx,
                    "ticker":    row["ticker"],
                    "date":      event_date,
                    "reason":    "invalid_stop_distance",
                })
                continue

            # ── Current equity and position sizing ───────────────────────────
            current_equity = _get_current_equity(cash, open_positions)

            # Apply gate risk multiplier to position size if gate is enabled
            gate_mult    = float(row.get("gate_risk_mult", 1.0)) if gate_enabled else 1.0
            risk_dollars = current_equity * risk_pct * gate_mult

            shares  = int(risk_dollars / stop_distance)
            shares  = max(shares, min_shares)
            notional = shares * float(row["entry_price"])

            # ── Cash sufficiency check ────────────────────────────────────────
            if notional > cash:
                # Scale down to available cash
                shares = int(cash / float(row["entry_price"]))
                if shares < min_shares:
                    blocked_log.append({
                        "trade_idx": trade_idx,
                        "ticker":    row["ticker"],
                        "date":      event_date,
                        "reason":    "insufficient_cash",
                    })
                    continue
                notional = shares * float(row["entry_price"])

            # ── Sector cap check ──────────────────────────────────────────────
            if sector_cap_pct is not None:
                spider_id = str(row.get("spider_id", "UNKNOWN"))
                total_open_notional = sum(
                    p["notional"] for p in open_positions.values()
                )
                sector_notional = sum(
                    p["notional"] for p in open_positions.values()
                    if str(p.get("spider_id", "")) == spider_id
                )
                projected_sector = (sector_notional + notional)
                projected_total  = (total_open_notional + notional)
                if projected_total > 0 and (projected_sector / projected_total) > sector_cap_pct:
                    blocked_log.append({
                        "trade_idx": trade_idx,
                        "ticker":    row["ticker"],
                        "date":      event_date,
                        "reason":    f"sector_cap_{spider_id}",
                    })
                    continue

            # ── Open position ─────────────────────────────────────────────────
            cash -= notional
            open_positions[trade_idx] = {
                "ticker":          row["ticker"],
                "spider_id":       row.get("spider_id"),
                "shares":          shares,
                "notional":        notional,
                "entry_price":     float(row["entry_price"]),
                "stop_distance":   stop_distance,
                "entry_date":      event_date,
                "equity_at_entry": current_equity,
                "risk_dollars":    risk_dollars,
                "gate_risk_mult":  gate_mult,
            }

        # ── Equity snapshot after every event ────────────────────────────────
        eq_now = _get_current_equity(cash, open_positions)
        equity_snapshots.append({
            "date":            event_date,
            "equity":          round(eq_now, 2),
            "cash":            round(cash, 2),
            "open_positions":  len(open_positions),
        })

        # ── Sector exposure snapshot (guarded by same flag as positions log) ─
        if save_positions_log:
            for pos in open_positions.values():
                sector_snapshots.append({
                    "date": event_date,
                    "spider_id": pos.get("spider_id", "UNKNOWN"),
                    "notional": pos["notional"],
                })

        # ── Positions log snapshot (only if enabled — can be memory-heavy) ───
        if save_positions_log:
            for pos in open_positions.values():
                positions_log.append({
                    "date": event_date,
                    "ticker": pos["ticker"],
                    "spider_id": pos.get("spider_id"),
                    "shares": pos["shares"],
                    "notional": round(pos["notional"], 2),
                    "entry_price": round(pos["entry_price"], 4),
                    "entry_date": pos["entry_date"],
                })

    # ── Build output DataFrames ───────────────────────────────────────────────
    portfolio_trades_df = pd.DataFrame(portfolio_trades)
    if not portfolio_trades_df.empty:
        portfolio_trades_df["entry_date"] = pd.to_datetime(portfolio_trades_df["entry_date"])
        portfolio_trades_df["exit_date"]  = pd.to_datetime(portfolio_trades_df["exit_date"])

    # Equity curve — deduplicate to one row per date (last event of each day)
    equity_df = pd.DataFrame(equity_snapshots)
    if not equity_df.empty:
        equity_df["date"] = pd.to_datetime(equity_df["date"])
        equity_df = (
            equity_df.sort_values("date")
            .groupby("date")
            .last()
            .reset_index()
        )
        # Add return metrics
        equity_df["daily_pnl"]     = equity_df["equity"].diff().fillna(0.0).round(2)
        equity_df["daily_ret_pct"] = (
            equity_df["equity"].pct_change().fillna(0.0) * 100.0
        ).round(4)
        equity_df["cum_ret_pct"]   = (
            (equity_df["equity"] - capital) / capital * 100.0
        ).round(4)

    # Daily equity — reindex to all trading days (fill forward for days without events)
    daily_equity_df = _build_daily_equity(equity_df, capital)

    # Positions log — deduplicate to one row per (date, ticker)
    positions_log_df = pd.DataFrame(positions_log)
    if not positions_log_df.empty:
        positions_log_df["date"]       = pd.to_datetime(positions_log_df["date"])
        positions_log_df["entry_date"] = pd.to_datetime(positions_log_df["entry_date"])
        positions_log_df = (
            positions_log_df
            .sort_values(["date", "ticker"])
            .drop_duplicates(subset=["date", "ticker"], keep="last")
            .reset_index(drop=True)
        )

    # Sector exposure — sum notional by (date, spider_id)
    sector_exposure_df = pd.DataFrame(sector_snapshots)
    if not sector_exposure_df.empty:
        sector_exposure_df["date"] = pd.to_datetime(sector_exposure_df["date"])
        sector_exposure_df = (
            sector_exposure_df
            .groupby(["date", "spider_id"])["notional"]
            .sum()
            .reset_index()
            .rename(columns={"notional": "sector_notional"})
        )
        # Add pct of total open notional on that date
        date_totals = sector_exposure_df.groupby("date")["sector_notional"].sum().rename("total_notional")
        sector_exposure_df = sector_exposure_df.merge(date_totals, on="date")
        sector_exposure_df["sector_pct"] = (
            sector_exposure_df["sector_notional"] / sector_exposure_df["total_notional"] * 100.0
        ).round(2)

    # ── Compute metrics ───────────────────────────────────────────────────────
    metrics = compute_portfolio_metrics(
        daily_equity_df    = daily_equity_df,
        portfolio_trades_df = portfolio_trades_df,
        capital            = capital,
        blocked_log        = blocked_log,
        max_positions      = max_positions,
        sector_cap_pct     = sector_cap_pct,
        gate_enabled       = gate_enabled,
    )

    return {
        "portfolio_trades":   portfolio_trades_df,
        "equity_df":          equity_df,
        "daily_equity_df":    daily_equity_df,
        "positions_log_df":   positions_log_df,
        "sector_exposure_df": sector_exposure_df,
        "metrics":            metrics,
        "blocked_log":        blocked_log,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio metrics
# ─────────────────────────────────────────────────────────────────────────────

def compute_portfolio_metrics(
    daily_equity_df:    pd.DataFrame,
    portfolio_trades_df: pd.DataFrame,
    capital:            float,
    blocked_log:        list,
    max_positions:      Optional[int],
    sector_cap_pct:     Optional[float],
    gate_enabled:       bool,
) -> dict:
    """
    Compute portfolio-level performance metrics.

    Unlike 09B's trade-level proxy metrics, these use the DAILY equity curve
    for Sharpe/Sortino/drawdown — giving statistically meaningful risk metrics.
    """
    if portfolio_trades_df is None or portfolio_trades_df.empty:
        return {"error": "no_trades_executed"}

    trades   = portfolio_trades_df
    total_t  = len(trades)
    wins     = trades[trades["pnl_dollar"] > 0]
    losses   = trades[trades["pnl_dollar"] <= 0]

    win_count  = int(len(wins))
    loss_count = int(len(losses))
    win_rate   = win_count / total_t * 100.0 if total_t > 0 else 0.0

    avg_win_pct  = float(trades.loc[trades["pnl_dollar"] > 0,  "pnl_pct"].mean()) if win_count  > 0 else 0.0
    avg_loss_pct = float(trades.loc[trades["pnl_dollar"] <= 0, "pnl_pct"].mean()) if loss_count > 0 else 0.0
    best_trade   = float(trades["pnl_pct"].max()) if total_t > 0 else 0.0
    worst_trade  = float(trades["pnl_pct"].min()) if total_t > 0 else 0.0

    gross_wins   = float(wins["pnl_dollar"].sum())   if win_count  > 0 else 0.0
    gross_losses = float(losses["pnl_dollar"].abs().sum()) if loss_count > 0 else 0.0
    net_pnl      = float(trades["pnl_dollar"].sum())

    profit_factor = round(gross_wins / gross_losses, 4) if gross_losses > 0 else float("inf")
    expectancy_r  = float(trades["pnl_r"].mean()) if total_t > 0 else 0.0
    net_ret_pct   = (net_pnl / capital) * 100.0 if capital > 0 else 0.0

    hold_days     = trades["hold_days"].dropna()
    avg_hold      = float(hold_days.mean()) if not hold_days.empty else 0.0
    max_hold      = int(hold_days.max())    if not hold_days.empty else 0

    s6 = int((trades["signal_type"] == "stage6_entry").sum())
    s7 = int((trades["signal_type"] == "stage7_entry").sum())

    exit_reasons = trades["exit_reason"].value_counts().to_dict() if "exit_reason" in trades.columns else {}

    # ── Risk metrics from DAILY equity curve (proper, not trade-level proxy) ──
    max_dd_pct = 0.0
    sharpe     = 0.0
    sortino    = 0.0
    calmar     = 0.0
    peak_equity    = capital
    trough_equity  = capital
    final_equity   = capital
    trading_days   = 0

    if daily_equity_df is not None and not daily_equity_df.empty and "daily_ret_pct" in daily_equity_df.columns:
        eq_series   = daily_equity_df["equity"]
        ret_series  = daily_equity_df["daily_ret_pct"].dropna()
        trading_days = len(daily_equity_df)

        # Max drawdown from daily equity curve
        running_max = eq_series.cummax()
        dd_series   = (eq_series - running_max) / running_max * 100.0
        max_dd_pct  = float(dd_series.min())

        peak_equity   = float(eq_series.max())
        trough_equity = float(eq_series.min())
        final_equity  = float(eq_series.iloc[-1])

        # Sharpe and Sortino from DAILY returns (annualised, risk-free = 0)
        if len(ret_series) >= 2 and ret_series.std() > 0:
            sharpe = float((ret_series.mean() / ret_series.std()) * np.sqrt(252))

        downside = ret_series[ret_series < 0]
        if len(downside) >= 2 and downside.std() > 0:
            sortino = float((ret_series.mean() / downside.std()) * np.sqrt(252))

        if max_dd_pct != 0:
            calmar = round(net_ret_pct / abs(max_dd_pct), 4)

    # ── Blocked trades breakdown ──────────────────────────────────────────────
    blocked_reasons = {}
    for b in blocked_log:
        r = b.get("reason", "unknown")
        blocked_reasons[r] = blocked_reasons.get(r, 0) + 1
    total_blocked = len(blocked_log)

    # ── Concurrent position stats ─────────────────────────────────────────────
    avg_open = 0.0
    max_open = 0
    if daily_equity_df is not None and not daily_equity_df.empty and "open_positions" in daily_equity_df.columns:
        avg_open = float(daily_equity_df["open_positions"].mean())
        max_open = int(daily_equity_df["open_positions"].max())

    return {
        # Trade counts
        "total_trades":       total_t,
        "winning_trades":     win_count,
        "losing_trades":      loss_count,
        "total_blocked":      total_blocked,

        # Win/loss metrics
        "win_rate_pct":       round(win_rate, 2),
        "avg_win_pct":        round(avg_win_pct, 4),
        "avg_loss_pct":       round(avg_loss_pct, 4),
        "best_trade_pct":     round(best_trade, 4),
        "worst_trade_pct":    round(worst_trade, 4),

        # R-multiple metrics
        "expectancy_r":       round(expectancy_r, 4),
        "profit_factor":      profit_factor,
        "gross_wins_usd":     round(gross_wins, 2),
        "gross_losses_usd":   round(gross_losses, 2),
        "net_pnl_usd":        round(net_pnl, 2),
        "net_return_pct":     round(net_ret_pct, 4),

        # Capital
        "starting_capital":   capital,
        "final_equity":       round(final_equity, 2),
        "peak_equity":        round(peak_equity, 2),
        "trough_equity":      round(trough_equity, 2),

        # Risk metrics — FROM DAILY EQUITY CURVE (statistically meaningful)
        "max_drawdown_pct":   round(max_dd_pct, 4),
        "sharpe_ratio":       round(sharpe, 4),   # annualised, daily returns
        "sortino_ratio":      round(sortino, 4),  # annualised, downside only
        "calmar_ratio":       round(calmar, 4),   # net return / max drawdown

        # Hold time
        "avg_hold_days":      round(avg_hold, 1),
        "max_hold_days":      max_hold,

        # Position stats
        "avg_concurrent_positions": round(avg_open, 1),
        "max_concurrent_positions": max_open,
        "trading_days_covered":     trading_days,

        # Entry stage split
        "stage6_entries":     s6,
        "stage7_entries":     s7,

        # Exit reasons
        "exit_reasons":       exit_reasons,

        # Blocked trades
        "blocked_reasons":    blocked_reasons,

        # Config snapshot
        "config": {
            "max_positions":      max_positions,
            "sector_cap_pct":     sector_cap_pct,
            "gate_enabled":       gate_enabled,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_current_equity(cash: float, open_positions: dict) -> float:
    """
    Current equity = cash + sum of entry notionals of open positions.
    Cash-basis valuation: open positions are valued at cost, not current price.
    """
    open_notional = sum(p["notional"] for p in open_positions.values())
    return cash + open_notional


def _build_daily_equity(equity_df: pd.DataFrame, capital: float) -> pd.DataFrame:
    """
    Resample equity curve to one row per calendar day.
    Forward-fill equity on days without events (no trades opened or closed).
    This gives a continuous daily equity series for proper Sharpe/drawdown.
    """
    if equity_df is None or equity_df.empty:
        return pd.DataFrame(columns=[
            "date", "equity", "cash", "open_positions",
            "daily_ret_pct", "cum_ret_pct"
        ])

    # Set date index and forward-fill between events
    df = equity_df.set_index("date").sort_index()

    # Fill only equity and open_positions forward; cash is less critical
    df["equity"]         = df["equity"].ffill()
    df["open_positions"] = df["open_positions"].ffill().fillna(0).astype(int)
    df["cash"]           = df["cash"].ffill()

    df = df.reset_index()
    df["daily_ret_pct"] = (df["equity"].pct_change().fillna(0.0) * 100.0).round(4)
    df["cum_ret_pct"]   = ((df["equity"] - capital) / capital * 100.0).round(4)

    return df
