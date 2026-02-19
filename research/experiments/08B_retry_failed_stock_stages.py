# research/experiments/08B_retry_failed_stock_stages.py

"""
08B_retry_failed_stock_stages.py

Retry stock stage classification ONLY for tickers recorded in:
  data/cleaned/stocks_daily/stages/_errors.jsonl

Writes:
  data/cleaned/stocks_daily/stages/<TICKER>.parquet   (overwrites only those tickers)
Logs (separate, so we don't break existing run logs):
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
DEFAULT_ERRORS = os.path.join(STAGES_DIR, "_errors.jsonl")

RETRY_PROGRESS = os.path.join(STAGES_DIR, "_retry_progress.jsonl")
RETRY_ERRORS = os.path.join(STAGES_DIR, "_retry_errors.jsonl")

# Match your pipeline config (you used this in your test)
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
                # ignore bad lines rather than stopping the whole retry
                continue
    return recs


def to_scalar(x: Any) -> Any:
    """
    Convert problematic cell types into scalars or NaN.
    This is a defensive guard against the "Series -> float" error.
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return x

    # common bad types
    if isinstance(x, (pd.Series, np.ndarray, list, tuple)):
        if len(x) == 0:
            return np.nan
        # take first element
        return x[0]

    if isinstance(x, dict):
        # can't reliably convert dict -> scalar
        return np.nan

    return x


def sanitize_features_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make the DF safe for indicator/stage logic:
      - ensure date column exists
      - drop duplicate dates (keep last)
      - coerce any non-scalar cells
      - coerce numeric columns to numeric
      - sort by date
    """
    if "date" not in df.columns:
        raise ValueError("features df missing required column: 'date'")

    # normalize date
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date.astype("datetime64[ns]")

    # critical: remove duplicate dates
    df = df.sort_values("date")
    df = df.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    # non-scalar cleanup (rare, but it exactly produces Series->float errors)
    # only apply to non-date cols for speed
    non_date_cols = [c for c in df.columns if c != "date"]
    for c in non_date_cols:
        # if column contains lists/series/arrays, sanitize it
        # cheap heuristic: sample a few non-null values
        s = df[c].dropna()
        if not s.empty:
            sample = s.head(25).tolist()
            if any(isinstance(v, (pd.Series, np.ndarray, list, tuple, dict)) for v in sample):
                df[c] = df[c].map(to_scalar)

    # coerce numerics where appropriate
    # (skip clearly non-numeric columns; most of your features should be numeric)
    for c in non_date_cols:
        if pd.api.types.is_numeric_dtype(df[c]):
            continue
        # attempt conversion; if it's truly categorical, it will become NaN (fine)
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def infer_output_columns() -> Optional[List[str]]:
    """
    Try to read one existing successful stages parquet to match schema.
    If none found, return None and we just write what classify_stages returns.
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

    # try to add ticker column if schema expects it
    if out_cols and "ticker" in out_cols and "ticker" not in df.columns:
        df["ticker"] = ticker

    # reorder to match existing schema if possible
    if out_cols:
        keep = [c for c in out_cols if c in df.columns]
        extra = [c for c in df.columns if c not in keep]
        df = df[keep + extra]

    df.to_parquet(out_path, index=False)
    return out_path


def retry_one(ticker: str, feature_path: str, out_cols: Optional[List[str]]) -> Tuple[bool, str]:
    t0 = time.time()

    df = pd.read_parquet(feature_path)
    df = sanitize_features_df(df)

    # classify
    out = classify_stages(df=df, cfg=CLASSIFY_CFG)

    # ensure core cols exist (date/stage/stage_name/stage_reason)
    if "date" not in out.columns or "stage" not in out.columns:
        raise ValueError(f"classify_stages output missing required cols for {ticker}: {out.columns.tolist()}")

    out_path = write_stage_parquet(ticker=ticker, out_df=out, out_cols=out_cols)

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

    # only take records that have ticker+file (your errors.jsonl has both)
    items: List[Tuple[str, str]] = []
    seen = set()

    for r in recs:
        if r.get("status") != "error":
            continue
        ticker = r.get("ticker")
        fpath = r.get("file")
        if not ticker or not fpath:
            continue

        # Some logs store absolute Windows paths; keep them as-is if they exist.
        # If the file isn't found, try making it relative to repo root.
        if not os.path.exists(fpath):
            maybe = os.path.join(ROOT, fpath)
            if os.path.exists(maybe):
                fpath = maybe

        if not os.path.exists(fpath):
            append_jsonl(
                RETRY_ERRORS,
                {
                    "ts": utc_now_iso(),
                    "status": "error",
                    "ticker": ticker,
                    "file": fpath,
                    "error": f"FileNotFoundError: {fpath}",
                },
            )
            continue

        key = (ticker, fpath)
        if key in seen:
            continue
        seen.add(key)
        items.append(key)

    if not items:
        print(f"[08B retry] No valid error records found in: {errors_path}")
        return

    out_cols = infer_output_columns()

    print(f"[08B retry] errors_path={errors_path}")
    print(f"[08B retry] retry_count={len(items)}")
    if out_cols:
        print(f"[08B retry] detected_stage_schema_cols={out_cols}")
    else:
        print("[08B retry] could not infer schema (no existing stage parquet found)")

    for ticker, feature_path in items:
        try:
            ok, msg = retry_one(ticker, feature_path, out_cols)
            append_jsonl(
                RETRY_PROGRESS,
                {
                    "ts": utc_now_iso(),
                    "status": "ok",
                    "ticker": ticker,
                    "feature_file": feature_path,
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
                    "feature_file": feature_path,
                    "error": repr(e),
                },
            )
            print(f"[ERR] ticker={ticker} :: {repr(e)}")

    print(f"[08B retry] done. logs:\n  {RETRY_PROGRESS}\n  {RETRY_ERRORS}")


if __name__ == "__main__":
    # You can also pass a custom errors jsonl path:
    #   python research/experiments/08B_retry_failed_stock_stages.py "path/to/_errors.jsonl"
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_ERRORS
    main(path)
