# Path: research/experiments/06B_audit_twelvedata_downloads.py
from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

EXPECTED_LAST_DATE = os.getenv("TD_EXPECTED_LAST_DATE", "2026-01-30").strip()
MIN_ROWS_OK = int(os.getenv("TD_MIN_ROWS_OK", "700"))

UNIVERSE_DIR = ROOT / "data" / "cleaned" / "universe"
PRICES_DIR = ROOT / "data" / "raw" / "prices_daily" / "twelvedata"
PARQUETS_DIR = PRICES_DIR / "parquets"

REPORTS_DIR = ROOT / "research" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def latest_trade_ready_universe() -> Path:
    files = sorted(UNIVERSE_DIR.glob("universe_trade_ready_*.csv"))
    if not files:
        raise FileNotFoundError("No universe_trade_ready_*.csv found.")
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def main() -> None:
    uni_path = latest_trade_ready_universe()
    uni = pd.read_csv(uni_path)
    tickers = sorted(uni["ticker"].astype(str).str.upper().str.strip().unique().tolist())

    exp_dt = pd.to_datetime(EXPECTED_LAST_DATE)

    rows = []
    missing = []

    for t in tickers:
        fp = PARQUETS_DIR / f"{t}.parquet"
        if not fp.exists():
            missing.append(t)
            continue

        try:
            df = pd.read_parquet(fp, columns=["date"])
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date")
            n = int(len(df))
            first = df["date"].iloc[0].strftime("%Y-%m-%d") if n else None
            last = df["date"].iloc[-1].strftime("%Y-%m-%d") if n else None
            last_dt = df["date"].iloc[-1] if n else pd.NaT

            ok = (n >= MIN_ROWS_OK) and (pd.notna(last_dt)) and (last_dt >= exp_dt)
            status = "ok" if ok else "partial"

            rows.append({
                "ticker": t,
                "status": status,
                "rows": n,
                "first_date": first,
                "last_date": last,
                "expected_last_date": EXPECTED_LAST_DATE,
                "file": str(fp),
            })
        except Exception as e:
            rows.append({
                "ticker": t,
                "status": "error",
                "rows": None,
                "first_date": None,
                "last_date": None,
                "expected_last_date": EXPECTED_LAST_DATE,
                "file": str(fp),
                "error": str(e),
            })

    df_rep = pd.DataFrame(rows)
    ok_n = int((df_rep["status"] == "ok").sum()) if not df_rep.empty else 0
    partial_n = int((df_rep["status"] == "partial").sum()) if not df_rep.empty else 0
    error_n = int((df_rep["status"] == "error").sum()) if not df_rep.empty else 0

    report = {
        "asof_utc": utc_now_iso(),
        "universe_file": str(uni_path),
        "prices_dir": str(PARQUETS_DIR),
        "expected_last_date": EXPECTED_LAST_DATE,
        "min_rows_ok": MIN_ROWS_OK,
        "tickers_total": len(tickers),
        "files_present": len(tickers) - len(missing),
        "missing_files": len(missing),
        "ok": ok_n,
        "partial": partial_n,
        "error": error_n,
    }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_json = REPORTS_DIR / f"twelvedata_audit_{stamp}.json"
    out_csv = REPORTS_DIR / f"twelvedata_audit_{stamp}.csv"
    out_partial_csv = REPORTS_DIR / f"twelvedata_partials_{stamp}.csv"
    out_missing_txt = REPORTS_DIR / f"twelvedata_missing_{stamp}.txt"

    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    df_rep.to_csv(out_csv, index=False)

    if not df_rep.empty:
        df_rep[df_rep["status"].isin(["partial", "error"])].to_csv(out_partial_csv, index=False)

    out_missing_txt.write_text("\n".join(missing), encoding="utf-8")

    print("\n=== Twelve Data Download Audit ===")
    print(f"[UNI] {uni_path.name}")
    print(f"[DIR] {PARQUETS_DIR}")
    print(f"[EXP] expected_last_date={EXPECTED_LAST_DATE}  min_rows_ok={MIN_ROWS_OK}")
    print(f"[TOT] tickers={len(tickers)} present={len(tickers)-len(missing)} missing={len(missing)}")
    print(f"[OK ] ok={ok_n} partial={partial_n} error={error_n}")
    print("\n=== Outputs ===")
    print(f"[JSON] {out_json}")
    print(f"[CSV]  {out_csv}")
    print(f"[PART] {out_partial_csv}")
    print(f"[MISS] {out_missing_txt}")
    print("\n[OK] Audit complete.")


if __name__ == "__main__":
    main()
