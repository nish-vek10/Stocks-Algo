# Path: research/experiments/05_test_twelvedata_single.py
from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from twelvedata import TDClient


# =============================================================================
# CONFIG (zero-arg runnable)
# =============================================================================
ROOT = Path(__file__).resolve().parents[2]

load_dotenv(ROOT / ".env")

API_KEY = os.getenv("TWELVEDATA_API_KEY", "").strip()
INTERVAL = os.getenv("TD_INTERVAL", "1day").strip()
START_DATE = os.getenv("TD_START_DATE", "2023-01-01").strip()
END_DATE = os.getenv("TD_END_DATE", "2026-02-01").strip()
TZ = os.getenv("TD_TIMEZONE", "UTC").strip()

OUTPUTSIZE = int(os.getenv("TD_OUTPUTSIZE", "5000"))

TEST_TICKER = "AAPL"

OUT_DIR = ROOT / "data" / "raw" / "prices_daily" / "twelvedata" / "parquets"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    TwelveData pandas output typically includes a 'datetime' column or index.
    Standardize to:
      date (datetime64[ns]), open/high/low/close (float), volume (float)
    Sorted ASC.
    """
    df = df.copy()

    if "datetime" in df.columns:
        df["date"] = pd.to_datetime(df["datetime"], errors="coerce", utc=False)
        df = df.drop(columns=["datetime"])
    else:
        # sometimes returned as index
        if df.index.name in ("datetime", "date"):
            df = df.reset_index()
            if "datetime" in df.columns:
                df["date"] = pd.to_datetime(df["datetime"], errors="coerce", utc=False)
                df = df.drop(columns=["datetime"])
            elif "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=False)

    # Ensure required cols exist
    for c in ["open", "high", "low", "close", "volume"]:
        if c not in df.columns:
            df[c] = None

    # Types
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df[["date", "open", "high", "low", "close", "volume"]]


def main() -> None:
    if not API_KEY:
        raise RuntimeError("TWELVEDATA_API_KEY missing in .env")

    print("\n=== Twelve Data :: Single Ticker Test ===")
    print(f"[ROOT] {ROOT}")
    print(f"[TICK] {TEST_TICKER}")
    print(f"[WIN]  {START_DATE} → {END_DATE}  [{INTERVAL}]  tz={TZ}")

    td = TDClient(apikey=API_KEY)

    ts = td.time_series(
        symbol=TEST_TICKER,
        interval=INTERVAL,
        start_date=START_DATE,
        end_date=END_DATE,
        outputsize=OUTPUTSIZE,
        timezone=TZ,
        order="asc",
    )

    df = ts.as_pandas()
    if df is None or len(df) == 0:
        raise RuntimeError("No data returned for test ticker.")

    df2 = normalize_ohlcv(df)

    out_path = OUT_DIR / f"{TEST_TICKER}.parquet"
    df2.to_parquet(out_path, index=False)

    meta = {
        "asof_utc": utc_now_iso(),
        "provider": "twelvedata",
        "ticker": TEST_TICKER,
        "interval": INTERVAL,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "timezone": TZ,
        "rows": int(len(df2)),
        "first_date": str(df2["date"].iloc[0]) if len(df2) else None,
        "last_date": str(df2["date"].iloc[-1]) if len(df2) else None,
        "output": str(out_path),
    }
    meta_path = OUT_DIR / f"{TEST_TICKER}.meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print("\n=== Output ===")
    print(f"[PARQ] {out_path}")
    print(f"[META] {meta_path}")
    print(f"[INFO] rows={len(df2)} first={meta['first_date']} last={meta['last_date']}")
    print("\n[OK] Single ticker test complete.")


if __name__ == "__main__":
    main()
