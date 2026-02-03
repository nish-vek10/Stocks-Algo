# Path: features/technicals/volume.py
from __future__ import annotations

import pandas as pd


def compute_relative_volume(volume: pd.Series, period: int) -> pd.Series:
    """
    Relative volume = volume / SMA(volume, period)
    """
    volume = volume.astype(float)
    vavg = volume.rolling(period, min_periods=period).mean()
    return volume / vavg
