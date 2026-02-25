# research/experiments/08B_retry_failed_stock_stages_from_tickers.py

"""
08B_retry_failed_stock_stages_from_tickers.py

Retry stock stage classification for tickers recorded in:
  data/cleaned/stocks_daily/stages/_errors.jsonl

Key difference vs old retry:
- DO NOT trust the logged absolute "file" path (often machine-specific).
- Instead, rebuild the features path from ticker:
    data/cleaned/stocks_daily/features/<TICKER>.parquet

Writes:
  data/cleaned/stocks_daily/stages/<TICKER>.parquet   (overwrites only those tickers)

Logs (separate; does not touch original run logs):
  data/cleaned/stocks_daily/stages/_retry_progress.jsonl
  data/cleaned/stocks_daily/stages/_retry_errors.jsonl
"""

from __future__ import annotations

import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import sys
import numpy as np
import pandas as pd

# ensure repo root is on PYTHONPATH (so "stages" resolves)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from stages.stage_classifier import classify_stages

STAGES_DIR = os.path.join(ROOT, "data", "cleaned", "stocks_daily", "stages")
FEATURES_DIR = os.path.join(ROOT, "data", "cleaned", "stocks_daily", "features")

DEFAULT_ERRORS = os.path.join(STAGES_DIR, "_errors.jsonl")

RETRY_PROGRESS = os.path.join(STAGES_DIR, "_retry_progress.jsonl")
RETRY_ERRORS = os.path.join(STAGES_DIR, "_retry_errors.jsonl")

# Match your pipeline config
CLASSIFY_CFG = {"stage_logic": {"require_breakout_before_inzone": True}}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def read_error_records(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Errors jsonl not found: {path}")

    recs: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                recs.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return recs


def to_scalar(x: Any) -> Any:
    """
    Convert problematic cell types into scalars or NaN.
    Defensive guard against the "Series -> float" error.
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return x

    if isinstance(x, (pd.Series, np.ndarray, list, tuple)):
        if len(x) == 0:
            return np.nan
        return x[0]

    if isinstance(x, dict):
        return np.nan

    return x


def sanitize_features_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make DF safe for stage logic:
      - ensure date column exists
      - drop duplicate dates (keep last)
      - coerce non-scalar cells
      - coerce numeric columns to numeric
      - sort by date
    """
    if "date" not in df.columns:
        raise ValueError("features df missing required column: 'date'")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype("datetime64[ns]")

    # remove duplicate dates
    df = df.sort_values("date")
    df = df.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    # non-scalar cleanup
    non_date_cols = [c for c in df.columns if c != "date"]
    for c in non_date_cols:
        s = df[c].dropna()
        if not s.empty:
            sample = s.head(25).tolist()
            if any(isinstance(v, (pd.Series, np.ndarray, list, tuple, dict)) for v in sample):
                df[c] = df[c].map(to_scalar)

    # coerce non-numeric -> numeric (others become NaN; acceptable)
    for c in non_date_cols:
        if pd.api.types.is_numeric_dtype(df[c]):
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def infer_output_columns() -> Optional[List[str]]:
    """
    Try to read one existing successful stages parquet to match schema.
    """
    if not os.path.isdir(STAGES_DIR):
        return None
    for fn in os.listdir(STAGES_DIR):
        if fn.endswith(".parquet") and not fn.startswith("_"):
            try:
                df = pd.read_parquet(os.path.join(STAGES_DIR, fn))
                return list(df.columns)
            except Exception:
                continue
    return None


def write_stage_parquet(ticker: str, out_df: pd.DataFrame, out_cols: Optional[List[str]]) -> str:
    out_path = os.path.join(STAGES_DIR, f"{ticker}.parquet")
    os.makedirs(STAGES_DIR, exist_ok=True)

    df = out_df.copy()

    if out_cols and "ticker" in out_cols and "ticker" not in df.columns:
        df["ticker"] = ticker

    if out_cols:
        keep = [c for c in out_cols if c in df.columns]
        extra = [c for c in df.columns if c not in keep]
        df = df[keep + extra]

    df.to_parquet(out_path, index=False)
    return out_path


def feature_path_for_ticker(ticker: str) -> str:
    return os.path.join(FEATURES_DIR, f"{ticker}.parquet")


def retry_one(ticker: str, feature_path: str, out_cols: Optional[List[str]]) -> Tuple[bool, str]:
    t0 = time.time()

    df = pd.read_parquet(feature_path)
    df = sanitize_features_df(df)

    out = classify_stages(df=df, cfg=CLASSIFY_CFG)

    if "date" not in out.columns or "stage" not in out.columns:
        raise ValueError(f"classify_stages output missing required cols for {ticker}: {out.columns.tolist()}")

    _ = write_stage_parquet(ticker=ticker, out_df=out, out_cols=out_cols)

    elapsed = time.time() - t0
    stages_present = sorted(pd.Series(out["stage"]).dropna().unique().tolist())

    msg = (
        f"ok ticker={ticker} rows={len(out)} "
        f"first={str(out['date'].iloc[0])} last={str(out['date'].iloc[-1])} "
        f"stages={stages_present} elapsed_s={elapsed:.3f}"
    )
    return True, msg


def main(errors_path: str = DEFAULT_ERRORS) -> None:
    recs = read_error_records(errors_path)

    # Collect tickers from error records (ignore machine-specific 'file' paths)
    tickers: List[str] = []
    seen = set()

    for r in recs:
        if r.get("status") != "error":
            continue
        t = r.get("ticker")
        if not t:
            continue
        t = str(t).strip().upper()
        if not t or t in seen:
            continue
        seen.add(t)
        tickers.append(t)

    if not tickers:
        print(f"[08B retry] No tickers found in: {errors_path}")
        return

    out_cols = infer_output_columns()

    print(f"[08B retry] errors_path={errors_path}")
    print(f"[08B retry] retry_count={len(tickers)}")
    print(f"[08B retry] features_dir={FEATURES_DIR}")
    if out_cols:
        print(f"[08B retry] detected_stage_schema_cols={out_cols}")
    else:
        print("[08B retry] could not infer schema (no existing stage parquet found)")

    for ticker in tickers:
        fpath = feature_path_for_ticker(ticker)

        if not os.path.exists(fpath):
            append_jsonl(
                RETRY_ERRORS,
                {
                    "ts": utc_now_iso(),
                    "status": "error",
                    "ticker": ticker,
                    "feature_file": fpath,
                    "error": f"FileNotFoundError: {fpath}",
                },
            )
            print(f"[ERR] ticker={ticker} :: missing features parquet: {fpath}")
            continue

        try:
            ok, msg = retry_one(ticker, fpath, out_cols)
            append_jsonl(
                RETRY_PROGRESS,
                {
                    "ts": utc_now_iso(),
                    "status": "ok",
                    "ticker": ticker,
                    "feature_file": fpath,
                    "message": msg,
                },
            )
            print(f"[OK] {msg}")
        except Exception as e:
            append_jsonl(
                RETRY_ERRORS,
                {
                    "ts": utc_now_iso(),
                    "status": "error",
                    "ticker": ticker,
                    "feature_file": fpath,
                    "error": repr(e),
                },
            )
            print(f"[ERR] ticker={ticker} :: {repr(e)}")

    print(f"[08B retry] done. logs:\n  {RETRY_PROGRESS}\n  {RETRY_ERRORS}")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_ERRORS
    main(path)