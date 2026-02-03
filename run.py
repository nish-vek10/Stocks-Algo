# Path: run.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
import yaml

from stages.stage_classifier import classify_stage


ROOT = Path(__file__).resolve().parent


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def smoke_make_synthetic_ohlcv(n: int = 600, seed: int = 7) -> pd.DataFrame:
    """
    Create synthetic OHLCV series for wiring tests.
    n=600 ~ enough for EMA200 + 2y minimum checks.
    """
    rng = np.random.default_rng(seed)
    rets = rng.normal(loc=0.0004, scale=0.01, size=n)  # slight drift up
    price = 100 * np.exp(np.cumsum(rets))

    close = pd.Series(price)
    high = close * (1 + rng.uniform(0.000, 0.01, size=n))
    low = close * (1 - rng.uniform(0.000, 0.01, size=n))
    open_ = close.shift(1).fillna(close.iloc[0])
    volume = pd.Series(rng.integers(low=2_000_000, high=10_000_000, size=n))

    df = pd.DataFrame(
        {
            "open": open_.astype(float),
            "high": high.astype(float),
            "low": low.astype(float),
            "close": close.astype(float),
            "volume": volume.astype(float),
        }
    )
    df.index = pd.RangeIndex(start=0, stop=n, step=1)
    return df


def main() -> None:
    print("\n=== ALGO-STOCKS :: Smoke Test Runner ===")
    print(f"[ROOT] {ROOT}")

    cfg_ind = load_yaml(ROOT / "config" / "indicators.yaml")
    cfg_stg = load_yaml(ROOT / "config" / "stages.yaml")

    print("[CONFIG] Loaded:")
    print(f"  indicators.yaml version={cfg_ind.get('version')}")
    print(f"  stages.yaml     version={cfg_stg.get('version')}")

    df = smoke_make_synthetic_ohlcv(n=620)
    print(f"[DATA] Synthetic OHLCV rows={len(df)} cols={list(df.columns)}")

    res = classify_stage(df, cfg_ind, cfg_stg)
    print("[STAGE] Result:")
    print(f"  stage_id   = {res.stage_id}")
    print(f"  stage_name = {res.stage_name}")
    print(f"  reasons    = {res.reasons}")

    print("\n[OK] Smoke test complete: imports, configs, and stage pipeline are working.")


if __name__ == "__main__":
    main()
