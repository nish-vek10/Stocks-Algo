# research/experiments/08A_build_stock_features.py
"""
08A :: Build Stock Daily Features
--------------------------------

Goal:
- For each ticker OHLCV parquet in:
    data/raw/prices_daily/twelvedata/parquets/*.parquet
  compute canonical indicators and write:
    data/cleaned/stocks_daily/features/{TICKER}.parquet

Inputs:
- Raw OHLCV (per ticker parquet):
    data/raw/prices_daily/twelvedata/parquets/{TICKER}.parquet

- Indicators config:
    config/indicators.yaml

Pipeline:
- Uses canonical feature pipeline:
    features/technicals/pipeline.py -> apply_indicators(df, cfg)

Outputs:
- data/cleaned/stocks_daily/features/{TICKER}.parquet

Logs:
- data/cleaned/stocks_daily/features/_progress.jsonl
- data/cleaned/stocks_daily/features/_errors.jsonl
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from types import SimpleNamespace

import pandas as pd
import yaml

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

RAW_DIR = ROOT / "data" / "raw" / "prices_daily" / "twelvedata" / "parquets"
OUT_DIR = ROOT / "data" / "cleaned" / "stocks_daily" / "features"
PROGRESS_JSONL = OUT_DIR / "_progress.jsonl"
ERRORS_JSONL = OUT_DIR / "_errors.jsonl"

INDICATORS_YAML = ROOT / "config" / "indicators.yaml"

# -----------------------------------------------------------------------------
# Smoke mode
# -----------------------------------------------------------------------------
SMOKE_N: Optional[int] = None   # e.g. 5 or None for all
SMOKE_TICKERS: Optional[str] = None  # e.g. "AAPL,MSFT" or None for all

# -----------------------------------------------------------------------------
# Imports from your canonical pipeline
# -----------------------------------------------------------------------------
from features.technicals.pipeline import apply_indicators  # noqa: E402


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def load_done_set(progress_path: Path) -> set[str]:
    done = set()
    if not progress_path.exists():
        return done
    with progress_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
                if j.get("status") == "ok" and j.get("ticker"):
                    done.add(str(j["ticker"]))
            except Exception:
                continue
    return done


def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing YAML: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def to_ns(x: Any) -> Any:
    """Convert nested dicts to SimpleNamespace for dot-access configs."""
    if isinstance(x, dict):
        return SimpleNamespace(**{k: to_ns(v) for k, v in x.items()})
    if isinstance(x, list):
        return [to_ns(v) for v in x]
    return x


def build_legacy_indicator_cfg(cfg_yaml: dict) -> SimpleNamespace:
    """
    Your apply_indicators() expects a flat namespace:
      ema_spans, bb_window, bb_n_std, donchian_window,
      vol_avg_window, vol_surge_mult,
      macd_enabled, macd_fast, macd_slow, macd_signal,
      rsi_enabled, rsi_period
    We translate config/indicators.yaml (nested under technicals) into that.
    """
    t = (cfg_yaml.get("technicals") or cfg_yaml) or {}

    ema_spans = t.get("ema_spans", [10, 20, 50, 100, 200])

    bb = t.get("bollinger", {}) or {}
    bb_window = int(bb.get("window", 20))
    bb_n_std = float(bb.get("n_std", 2.0))

    dc = t.get("donchian", {}) or {}
    donchian_window = int(dc.get("window", 20))

    vol = t.get("volume", {}) or {}
    vol_avg_window = int(vol.get("avg_window", 10))
    vol_surge_mult = float(vol.get("surge_mult", 1.15))

    macd = t.get("macd", {}) or {}
    macd_enabled = bool(macd.get("enabled", True))
    macd_fast = int(macd.get("fast", 12))
    macd_slow = int(macd.get("slow", 26))
    macd_signal = int(macd.get("signal", 9))

    rsi = t.get("rsi", {}) or {}
    rsi_enabled = bool(rsi.get("enabled", True))
    rsi_period = int(rsi.get("period", 14))

    return SimpleNamespace(
        ema_spans=ema_spans,
        bb_window=bb_window,
        bb_n_std=bb_n_std,
        donch_window=donchian_window,
        vol_avg_window=vol_avg_window,
        vol_surge_mult=vol_surge_mult,
        macd_fast=macd_fast,
        macd_slow=macd_slow,
        macd_signal=macd_signal,
        rsi_period=rsi_period,
        compute_macd=macd_enabled,
        compute_rsi=rsi_enabled,
    )


def list_raw_files() -> List[Path]:
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Missing RAW_DIR: {RAW_DIR}")
    files = sorted(RAW_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No raw parquets found in: {RAW_DIR}")
    return files


def read_ohlcv(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    # Expect either date column or DatetimeIndex
    if "date" in df.columns:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        return df
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.copy().sort_index()
        df = df.reset_index().rename(columns={"index": "date"})
        df["date"] = pd.to_datetime(df["date"])
        return df
    raise KeyError(f"{path.name}: needs 'date' column or DatetimeIndex")


def validate_ohlcv_cols(df: pd.DataFrame, path: Path) -> None:
    needed = {"open", "high", "low", "close", "volume"}
    missing = needed - set(df.columns)
    if missing:
        raise KeyError(f"{path.name} missing OHLCV columns: {sorted(missing)}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    print("\n=== 08A :: Build Stock Daily Features ===")
    print(f"[ROOT] {ROOT}")
    print(f"[RAW]  {RAW_DIR}")
    print(f"[OUT]  {OUT_DIR}")
    print(f"[CFG]  {INDICATORS_YAML}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cfg_yaml = load_yaml(INDICATORS_YAML)
    cfg = build_legacy_indicator_cfg(cfg_yaml)
    print("[CFG_FLAT]", cfg)

    done = load_done_set(PROGRESS_JSONL)

    files = list_raw_files()

    # Smoke selection
    if SMOKE_TICKERS:
        want = {t.strip().upper() for t in SMOKE_TICKERS.split(",") if t.strip()}
        files = [p for p in files if p.stem.upper() in want]
    if SMOKE_N is not None:
        files = files[: int(SMOKE_N)]

    remaining = [p for p in files if p.stem not in done]
    print(f"[RUN] total={len(files)} done={len(done)} remaining={len(remaining)}")

    ok_n = 0
    err_n = 0

    for p in files:
        ticker = p.stem
        if ticker in done:
            continue

        t0 = datetime.now(timezone.utc)
        try:
            df = read_ohlcv(p)
            validate_ohlcv_cols(df, p)

            # Compute indicators
            feat = apply_indicators(df, cfg)

            # Basic sanity
            if "date" not in feat.columns:
                raise RuntimeError(f"{ticker}: features output missing 'date' column")
            feat = feat.sort_values("date").reset_index(drop=True)

            out_path = OUT_DIR / f"{ticker}.parquet"
            feat.to_parquet(out_path, index=False)

            append_jsonl(PROGRESS_JSONL, {
                "ts": utc_now(),
                "status": "ok",
                "ticker": ticker,
                "rows": int(len(feat)),
                "first_date": str(pd.to_datetime(feat["date"].iloc[0]).date()),
                "last_date": str(pd.to_datetime(feat["date"].iloc[-1]).date()),
                "out": str(out_path),
                "elapsed_s": round((datetime.now(timezone.utc) - t0).total_seconds(), 3),
            })

            ok_n += 1
            if ok_n % 100 == 0:
                print(f"[OK] processed={ok_n} last={ticker}")

        except Exception as e:
            append_jsonl(ERRORS_JSONL, {
                "ts": utc_now(),
                "status": "error",
                "ticker": ticker,
                "file": str(p),
                "error": repr(e),
            })
            print(f"[ERROR] {ticker}: {e}")
            err_n += 1

    print(f"\n[SUMMARY] ok={ok_n} error={err_n}")
    if ok_n == 0 and err_n > 0:
        raise SystemExit("08A failed: zero tickers processed successfully.")
    print("[OK] 08A complete.")


if __name__ == "__main__":
    main()
