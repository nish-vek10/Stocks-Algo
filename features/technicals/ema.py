# Path: features/technicals/ema.py
from __future__ import annotations

import pandas as pd


def compute_ema(close: pd.Series, span: int) -> pd.Series:
    """
    Exponential moving average using pandas ewm.

    Args:
        close: price series
        span: EMA span

    Returns:
        EMA series (same index)
    """
    close = close.astype(float)
    return close.ewm(span=span, adjust=False).mean()
