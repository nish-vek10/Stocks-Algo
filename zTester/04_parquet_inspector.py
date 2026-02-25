# Path: zTester/04_parquet_inspector.py
"""
ALGO-STOCKS — Parquet Inspector & Exporter

PURPOSE
-------
Convert any parquet file in the project to CSV or XLSX for manual inspection.
Useful for auditing trades_all.parquet, signals, features, stages, or gate data
without needing to write one-liner scripts each time.

USAGE
-----
1. Set INPUT_PARQUET to the parquet file you want to inspect
2. Set OUTPUT_FORMAT to "csv" or "xlsx"
3. Set FILTER_TICKER if you want only one ticker's rows (leave "" for all rows)
4. Run from project root:
   python zTester/04_parquet_inspector.py

OUTPUT
------
All exports land in: output/exports/
File is named after the source parquet automatically.

SAFE TO RUN REPEATEDLY
-----------------------
Never modifies the source parquet. Read-only operation.
Output directory is created automatically if it does not exist.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION — edit these lines only
# ══════════════════════════════════════════════════════════════════════════════

ROOT = Path(__file__).resolve().parents[1]  # ALGO-Stocks/

# ── Input: path to any parquet file in the project ───────────────────────────
# Examples (uncomment the one needed):

# Full universe trades (most common use case)
INPUT_PARQUET = ROOT / "output" / "backtests" / "universe_baseline_v1_20260224_2200" / "universe" / "trades_all.parquet"

# Single ticker trades
# INPUT_PARQUET = ROOT / "output" / "backtests" / "baseline_v1_20260224_2149" / "single" / "AAPL" / "trades.parquet"

# Raw signals
# INPUT_PARQUET = ROOT / "output" / "signals" / "raw_signals_all.parquet"

# Stock features
# INPUT_PARQUET = ROOT / "data" / "cleaned" / "stocks_daily" / "features" / "AAPL.parquet"

# Stock stages
# INPUT_PARQUET = ROOT / "data" / "cleaned" / "stocks_daily" / "stages" / "AAPL.parquet"

# Spider gate
# INPUT_PARQUET = ROOT / "data" / "cleaned" / "spiders_daily" / "gate" / "spider_gate_daily.parquet"

# Spider features
# INPUT_PARQUET = ROOT / "data" / "cleaned" / "spiders_daily" / "features" / "SECTOR_TECHNOLOGY.parquet"

# ── Output format ─────────────────────────────────────────────────────────────
OUTPUT_FORMAT = "csv"      # "csv" or "xlsx"

# ── Optional: filter to a single ticker ──────────────────────────────────────
# Leave as "" to export all rows.
# Set to e.g. "AAPL" to export only that ticker's rows.
FILTER_TICKER = ""         # e.g. "NVDA" or "" for all

# ── Output directory ──────────────────────────────────────────────────────────
OUTPUT_DIR = ROOT / "output" / "exports"

# ── Row limit (safety cap for large files) ────────────────────────────────────
# Set to None to export all rows.
# Set to e.g. 10000 to cap at 10,000 rows (useful for huge parquets).
MAX_ROWS = None

# ══════════════════════════════════════════════════════════════════════════════


def main() -> None:
    print("\n" + "=" * 60)
    print("PARQUET INSPECTOR & EXPORTER")
    print("=" * 60)
    print(f"Input  : {INPUT_PARQUET}")
    print(f"Format : {OUTPUT_FORMAT.upper()}")
    print(f"Filter : {FILTER_TICKER if FILTER_TICKER else '(all rows)'}")

    if not INPUT_PARQUET.exists():
        print(f"\n[ERROR] Parquet not found: {INPUT_PARQUET}")
        print("  Check INPUT_PARQUET path at top of script.")
        return

    print("\nLoading parquet ...")
    df = pd.read_parquet(INPUT_PARQUET)
    print(f"  Loaded  : {len(df):,} rows x {df.shape[1]} columns")
    print(f"  Columns : {df.columns.tolist()}")

    if FILTER_TICKER:
        if "ticker" not in df.columns:
            print(f"\n[WARNING] FILTER_TICKER='{FILTER_TICKER}' set but no 'ticker' column found.")
        else:
            before = len(df)
            df = df[df["ticker"] == FILTER_TICKER].copy()
            print(f"  Filtered: {len(df):,} rows for ticker '{FILTER_TICKER}' (was {before:,})")
            if df.empty:
                print(f"\n[WARNING] No rows found for ticker '{FILTER_TICKER}'.")
                return

    for date_col in ["date", "entry_date", "signal_date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.sort_values(date_col).reset_index(drop=True)
            print(f"  Sorted  : by '{date_col}'")
            break

    if MAX_ROWS is not None and len(df) > MAX_ROWS:
        print(f"  Capped  : {MAX_ROWS:,} rows (MAX_ROWS limit applied)")
        df = df.head(MAX_ROWS)

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stem = INPUT_PARQUET.stem
    suffix = f"_{FILTER_TICKER}" if FILTER_TICKER else ""
    out_name = f"{stem}{suffix}.{OUTPUT_FORMAT}"
    out_path = OUTPUT_DIR / out_name

    if OUTPUT_FORMAT == "xlsx":
        print(f"\nWriting XLSX ...")
        try:
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="data")
                ws = writer.sheets["data"]
                for col_idx, col_name in enumerate(df.columns, 1):
                    max_len = max(
                        len(str(col_name)),
                        df[col_name].astype(str).str.len().max() if len(df) > 0 else 0
                    )
                    ws.column_dimensions[
                        ws.cell(row=1, column=col_idx).column_letter
                    ].width = min(max_len + 2, 40)
        except ImportError:
            print("[ERROR] openpyxl not installed.")
            print("  Install with: pip install openpyxl --break-system-packages")
            print("  Falling back to CSV.")
            out_path = OUTPUT_DIR / f"{stem}{suffix}.csv"
            df.to_csv(out_path, index=False)
    else:
        print(f"\nWriting CSV ...")
        df.to_csv(out_path, index=False)

    file_size_kb = out_path.stat().st_size / 1024
    print(f"\n{'─' * 50}")
    print(f"  Output  : {out_path}")
    print(f"  Rows    : {len(df):,}")
    print(f"  Columns : {df.shape[1]}")
    print(f"  Size    : {file_size_kb:.1f} KB")
    print(f"{'─' * 50}")

    print("\nPreview (first 5 rows):")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    pd.set_option("display.max_colwidth", 20)
    print(df.head(5).to_string(index=False))
    print("\n[OK] Done.")


if __name__ == "__main__":
    main()