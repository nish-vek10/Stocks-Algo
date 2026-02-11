# features/technicals/pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd

from features.technicals.indicators import (
    ema, bollinger_bands, donchian_channels, macd, rsi, sma
)


@dataclass(frozen=True)
class IndicatorConfig:
    ema_spans: List[int]
    bb_window: int
    bb_n_std: float
    donch_window: int
    vol_avg_window: int
    vol_surge_mult: float
    macd_fast: int
    macd_slow: int
    macd_signal: int
    rsi_period: int
    compute_macd: bool = True
    compute_rsi: bool = True


def apply_indicators(df: pd.DataFrame, cfg: IndicatorConfig) -> pd.DataFrame:
    """
    Input df must have columns: date, open, high, low, close, volume
    Output: df + indicator columns (aligned, NaNs during warmup)
    """
    out = df.copy()

    # Ensure sort
    if "date" in out.columns:
        out = out.sort_values("date").reset_index(drop=True)

    close = out["close"].astype(float)
    high = out["high"].astype(float)
    low = out["low"].astype(float)

    # EMA stack
    for span in cfg.ema_spans:
        out[f"ema{span}"] = ema(close, span)

    # Bollinger
    bb = bollinger_bands(close, window=cfg.bb_window, n_std=cfg.bb_n_std)
    bb = bb.rename(columns={
        "bb_mid": f"bb_mid_{cfg.bb_window}",
        "bb_upper": f"bb_upper_{cfg.bb_window}_{int(cfg.bb_n_std)}",
        "bb_lower": f"bb_lower_{cfg.bb_window}_{int(cfg.bb_n_std)}",
    })
    out = pd.concat([out, bb], axis=1)

    # Donchian
    dc = donchian_channels(high, low, window=cfg.donch_window)
    dc = dc.rename(columns={
        "donch_high": f"donch_high_{cfg.donch_window}",
        "donch_low": f"donch_low_{cfg.donch_window}",
        "donch_mid": f"donch_mid_{cfg.donch_window}",
    })
    out = pd.concat([out, dc], axis=1)

    # Volume avg + surge flag
    vol = out["volume"].astype(float).fillna(0.0)
    out[f"vol_sma_{cfg.vol_avg_window}"] = sma(vol, cfg.vol_avg_window)
    out["vol_surge"] = vol > (out[f"vol_sma_{cfg.vol_avg_window}"] * cfg.vol_surge_mult)

    # Optional momentum overlays (compute now, used later if desired)
    if cfg.compute_macd:
        m = macd(close, fast=cfg.macd_fast, slow=cfg.macd_slow, signal=cfg.macd_signal)
        out = pd.concat([out, m], axis=1)

    if cfg.compute_rsi:
        out["rsi"] = rsi(close, period=cfg.rsi_period)

    return out
