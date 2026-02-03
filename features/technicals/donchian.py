# Path: features/technicals/donchian.py
from __future__ import annotations

import pandas as pd


def compute_donchian(high: pd.Series, low: pd.Series, lookback: int) -> pd.DataFrame:
    """
    Donchian channel: rolling highest high and lowest low.

    Returns a DataFrame with:
      - donchian_high
      - donchian_low
    """
    high = high.astype(float)
    low = low.astype(float)

    out = pd.DataFrame(index=high.index)
    out["donchian_high"] = high.rolling(lookback, min_periods=lookback).max()
    out["donchian_low"] = low.rolling(lookback, min_periods=lookback).min()
    return out
