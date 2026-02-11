# Path: zTester/01_parquet_to_csv.py
from __future__ import annotations

from pathlib import Path
import pandas as pd


# =============================================================================
# CONFIG (zero-arg runnable)
# =============================================================================
ROOT = Path(__file__).resolve().parents[1]  # ALGO-Stocks/

TICKER = "AAMI"

# Default input (AAPL test output)
# IN_PARQUET = ROOT / "data" / "raw" / "prices_daily" / "twelvedata" / "parquets" / f"{TICKER}.parquet"

# Output dir for human inspection
# OUT_DIR = ROOT / "data" / "raw" / "prices_daily" / "twelvedata" / "csv"
# OUT_DIR.mkdir(parents=True, exist_ok=True)

# Output file name
# OUT_CSV = OUT_DIR / f"{TICKER}.csv"

# ==== FOR GENERAL FILES ====
PARQUET_FILE = "spider_gate_daily"

IN_PARQUET = ROOT / "data" / "cleaned" / "spiders_daily" / "gate" / f"{PARQUET_FILE}.parquet"
OUT_DIR = ROOT / "data" / "cleaned" / "spiders_daily" / "gate" / "csv"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / f"{PARQUET_FILE}.csv"

def main() -> None:
    print("\n=== Parquet → CSV ===")
    print(f"[ROOT] {ROOT}")
    print(f"[IN]   {IN_PARQUET}")

    if not IN_PARQUET.exists():
        raise FileNotFoundError(f"Input parquet not found: {IN_PARQUET}")

    df = pd.read_parquet(IN_PARQUET)

    # Sort + standardize for viewing
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date")

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    df.to_csv(OUT_CSV, index=False)

    print("\n=== Output ===")
    print(f"[CSV]  {OUT_CSV}")
    print(f"[INFO] rows={len(df)} cols={df.shape[1]}")
    print("\n[OK] Done.")


if __name__ == "__main__":
    main()
