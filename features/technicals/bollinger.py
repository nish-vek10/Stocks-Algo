# Path: features/technicals/bollinger.py
from __future__ import annotations

import pandas as pd


def compute_bollinger(close: pd.Series, period: int, stdev: float = 2.0) -> pd.DataFrame:
    """
    Bollinger bands:
      mid = SMA(period)
      upper/lower = mid +/- stdev * rolling_std(period)
    """
    close = close.astype(float)

    mid = close.rolling(period, min_periods=period).mean()
    sd = close.rolling(period, min_periods=period).std(ddof=0)

    out = pd.DataFrame(index=close.index)
    out["bb_mid"] = mid
    out["bb_upper"] = mid + stdev * sd
    out["bb_lower"] = mid - stdev * sd
    out["bb_width"] = (out["bb_upper"] - out["bb_lower"]) / out["bb_mid"]
    return out
