# Path: research/experiments/02_promote_finviz_raw_to_cleaned.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


# =============================================================================
# CONFIG (zero-arg runnable)
# =============================================================================
ROOT = Path(__file__).resolve().parents[2]  # ALGO-STOCKS/

RAW_DIR = ROOT / "data" / "raw" / "finviz"
CLEAN_DIR = ROOT / "data" / "cleaned" / "universe"

REPORTS_DIR = ROOT / "research" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def find_latest_parsed_csv() -> Path:
    """
    Picks the latest *.parsed.csv in data/raw/finviz by filename timestamp then mtime fallback.
    """
    candidates = sorted(RAW_DIR.glob("*.parsed.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No *.parsed.csv found in {RAW_DIR}. Run 01_fetch_finviz_export.py first."
        )

    # Filenames include timestamps; sorting usually works. Fallback: mtime.
    candidates = sorted(
        candidates,
        key=lambda p: (p.name, p.stat().st_mtime),
        reverse=True,
    )
    return candidates[0]


def main() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    latest = find_latest_parsed_csv()
    print("\n=== Promote Finviz Raw → Cleaned (NO FILTERS) ===")
    print(f"[ROOT]   {ROOT}")
    print(f"[INPUT]  {latest}")

    df = pd.read_csv(latest)

    # --- Minimal, safe normalization (NO drops) ---
    # Ensure a stable ticker column name if present.
    if "ticker" not in df.columns:
        # some exports might use "symbol"
        if "symbol" in df.columns:
            df = df.rename(columns={"symbol": "ticker"})

    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

    # Add metadata columns (safe, no strategy assumptions)
    df.insert(0, "asof_utc", utc_now_iso())
    df.insert(1, "source", "finviz_elite_export")
    df.insert(2, "raw_file", latest.name)

    # Output file
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = CLEAN_DIR / f"universe_finviz_rawpromote_{ts}.csv"
    df.to_csv(out_path, index=False)

    # Create a columns report for you to review quickly
    col_report = {
        "asof_utc": utc_now_iso(),
        "input_file": str(latest),
        "output_file": str(out_path),
        "rows": int(len(df)),
        "cols": int(df.shape[1]),
        "columns": list(df.columns),
    }
    report_path = REPORTS_DIR / f"finviz_columns_{ts}.json"
    report_path.write_text(json.dumps(col_report, indent=2), encoding="utf-8")

    # Also write a human-readable txt list
    txt_path = REPORTS_DIR / f"finviz_columns_{ts}.txt"
    txt_path.write_text("\n".join(col_report["columns"]), encoding="utf-8")

    print("\n=== Output ===")
    print(f"[CLEAN]  {out_path}")
    print(f"[REPORT] {report_path}")
    print(f"[TXT]    {txt_path}")
    print(f"[INFO]   rows={len(df)} cols={df.shape[1]}")
    print("\n[OK] Done. No rows dropped. No filtering. No sector mapping.")


if __name__ == "__main__":
    main()
