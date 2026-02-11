# features/spiders/build_features.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Dict, Any
import pandas as pd


def _ensure_cols(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns: {missing}")


def build_spider_features(
    *,
    spider_parquet: Path,
    out_parquet: Path,
    indicators_cfg: Dict[str, Any],
    trim_last_n_days: Optional[int] = None,
) -> pd.DataFrame:
    """
    Build feature-enriched spider dataframe from raw spider OHLCV parquet.

    Expected schema (raw spiders):
      date, open, high, low, close, volume, members_used

    Returns the enriched dataframe and writes it to out_parquet.
    """
    if not spider_parquet.exists():
        raise FileNotFoundError(f"Missing spider parquet: {spider_parquet}")

    df = pd.read_parquet(spider_parquet)

    # Normalize date column
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        df = df.set_index("date")
    else:
        # If index is date-like, keep but validate
        if not isinstance(df.index, pd.DatetimeIndex):
            raise KeyError("Spider parquet must contain 'date' column or DatetimeIndex.")
        df = df.sort_index()

    _ensure_cols(df, ["open", "high", "low", "close"])
    if "volume" not in df.columns:
        df["volume"] = 0.0  # optional; keep consistent

    # Apply canonical indicator pipeline
    try:
        from features.technicals.pipeline import apply_indicators
    except Exception as e:
        raise RuntimeError(
            "Could not import apply_indicators from features.technicals.pipeline. "
            "Ensure features/technicals/pipeline.py defines apply_indicators()."
        ) from e

    out = apply_indicators(df.copy(), indicators_cfg)

    # Optional trim (research/backtest window later). Keep None for now.
    if trim_last_n_days is not None and trim_last_n_days > 0:
        out = out.tail(int(trim_last_n_days)).copy()

    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    out.reset_index().to_parquet(out_parquet, index=False)

    return out.reset_index()
