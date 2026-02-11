# Path: research/experiments/06C_retry_twelvedata_errors_1by1.py
from __future__ import annotations

import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from dotenv import load_dotenv
from twelvedata import TDClient


# =============================================================================
# CONFIG — EDIT THESE ONLY
# =============================================================================
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

# Where your TwelveData outputs already live
OUT_DIR = ROOT / "data" / "raw" / "prices_daily" / "twelvedata"
PARQUETS_DIR = OUT_DIR / "parquets"
META_DIR = OUT_DIR / "meta"

# Input: your converted CSV (from _errors.jsonl -> _errors.csv)
ERRORS_CSV = OUT_DIR / "_errors.csv"

# Retry error output (separate file so you can see what's still failing)
RETRY_ERRORS_JSONL = OUT_DIR / "_errors_retry.jsonl"

# Writes to your existing progress log (same as main pipeline)
PROGRESS_JSONL = OUT_DIR / "_progress.jsonl"

# TwelveData request settings (same as your main script)
API_KEY = os.getenv("TWELVEDATA_API_KEY", "").strip()
INTERVAL = os.getenv("TD_INTERVAL", "1day").strip()
START_DATE = os.getenv("TD_START_DATE", "2023-01-01").strip()
END_DATE = os.getenv("TD_END_DATE", "2026-02-01").strip()
TZ = os.getenv("TD_TIMEZONE", "UTC").strip()
OUTPUTSIZE = int(os.getenv("TD_OUTPUTSIZE", "5000"))

EXPECTED_LAST_DATE = os.getenv("TD_EXPECTED_LAST_DATE", "2026-01-30").strip()
MIN_ROWS_OK = int(os.getenv("TD_MIN_ROWS_OK", "700"))

# Throttle between calls (seconds). Keep small but non-zero.
SLEEP_BETWEEN_CALLS = float(os.getenv("TD_RETRY_SLEEP_SEC", "0.25"))


# =============================================================================
# Helpers
# =============================================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def append_jsonl(path: Path, obj: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")


def is_minute_credit_error(e: Exception) -> bool:
    msg = str(e).lower()
    return ("run out of api credits for the current minute" in msg) or ("out of api credits" in msg)


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "datetime" in df.columns:
        df["date"] = pd.to_datetime(df["datetime"], errors="coerce", utc=False)
        df = df.drop(columns=["datetime"])
    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=False)
    else:
        for candidate in ("time", "timestamp", "level_1", "index"):
            if candidate in df.columns:
                df["date"] = pd.to_datetime(df[candidate], errors="coerce", utc=False)
                df = df.drop(columns=[candidate])
                break

    if "date" not in df.columns:
        raise KeyError(f"normalize_ohlcv: no datetime/date column found. cols={list(df.columns)}")

    for c in ["open", "high", "low", "close", "volume"]:
        if c not in df.columns:
            df[c] = None
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df[["date", "open", "high", "low", "close", "volume"]]


def coverage_status(sub: pd.DataFrame) -> Tuple[bool, str, str, int, str]:
    rows = int(len(sub))
    if rows == 0 or "date" not in sub.columns:
        return (False, None, None, rows, "empty_or_missing_date")

    first_dt = pd.to_datetime(sub["date"].iloc[0], errors="coerce")
    last_dt = pd.to_datetime(sub["date"].iloc[-1], errors="coerce")
    first_s = first_dt.strftime("%Y-%m-%d") if pd.notna(first_dt) else None
    last_s = last_dt.strftime("%Y-%m-%d") if pd.notna(last_dt) else None

    exp = pd.to_datetime(EXPECTED_LAST_DATE, errors="coerce")
    if pd.isna(last_dt) or pd.isna(exp):
        return (False, first_s, last_s, rows, "bad_dates")

    if last_dt >= exp:
        reason = "ok_short_history" if rows < MIN_ROWS_OK else "ok"
        return (True, first_s, last_s, rows, reason)

    return (False, first_s, last_s, rows, "partial")


def read_done_set_from_progress() -> set:
    """Skip tickers already marked ok / ok_short_history in _progress.jsonl."""
    done = set()
    if PROGRESS_JSONL.exists():
        for line in PROGRESS_JSONL.read_text(encoding="utf-8").splitlines():
            try:
                obj = json.loads(line)
                if obj.get("status") in ("ok", "ok_short_history"):
                    t = obj.get("ticker")
                    if t:
                        done.add(str(t).upper().strip())
            except Exception:
                continue
    return done


def symbol_candidates(ticker: str) -> List[str]:
    """
    TwelveData often prefers dot-notation for class shares / units:
      BF-A -> BF.A
      BRK-B -> BRK.B
      ALUB-U -> ALUB.U

    We try a few safe transforms.
    """
    t = ticker.strip().upper()
    cands = [t]

    # common: class shares & preferred use dot instead of hyphen
    if "-" in t:
        cands.append(t.replace("-", "."))

    # common unit / warrant suffixes
    if t.endswith("-U"):
        cands.append(t[:-2] + ".U")
    if t.endswith("-WS"):
        cands.append(t[:-3] + ".WS")
    if t.endswith("-W"):
        cands.append(t[:-2] + ".W")

    # de-dupe while preserving order
    out = []
    seen = set()
    for x in cands:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    if not API_KEY:
        raise RuntimeError("TWELVEDATA_API_KEY missing in .env")

    if not ERRORS_CSV.exists():
        raise FileNotFoundError(f"Missing input CSV: {ERRORS_CSV}")

    df_err = pd.read_csv(ERRORS_CSV)
    if "ticker" not in df_err.columns:
        raise KeyError(f"{ERRORS_CSV} must contain a 'ticker' column. cols={list(df_err.columns)}")

    # unique tickers from errors
    tickers = sorted(df_err["ticker"].astype(str).str.upper().str.strip().unique().tolist())

    done = read_done_set_from_progress()
    remaining = [t for t in tickers if t not in done]

    print("\n=== TwelveData Retry (errors → one-by-one) ===")
    print(f"[ROOT] {ROOT}")
    print(f"[IN ] {ERRORS_CSV}  tickers={len(tickers)} remaining={len(remaining)}")
    print(f"[OUT] {OUT_DIR}")
    print(f"[CFG] interval={INTERVAL} window={START_DATE}→{END_DATE} tz={TZ} outputsize={OUTPUTSIZE}")
    print(f"[GATE] expected_last={EXPECTED_LAST_DATE} min_rows_ok={MIN_ROWS_OK}")
    print(f"[SLEEP] {SLEEP_BETWEEN_CALLS}s between calls")

    if not remaining:
        print("\n[OK] Nothing to retry (0 remaining). Exiting.")
        return

    td = TDClient(apikey=API_KEY)

    ok_n = 0
    partial_n = 0
    err_n = 0

    for k, ticker in enumerate(remaining, start=1):
        print(f"[TRY] {k}/{len(remaining)} ticker={ticker}")

        success = False
        last_exception = None

        for sym_try in symbol_candidates(ticker):
            try:
                # ---- IMPORTANT: handle minute-credit errors by sleeping and retrying SAME symbol ----
                while True:
                    try:
                        ts = td.time_series(
                            symbol=sym_try,  # IMPORTANT: single symbol string
                            interval=INTERVAL,
                            start_date=START_DATE,
                            end_date=END_DATE,
                            outputsize=OUTPUTSIZE,
                            timezone=TZ,
                            order="asc",
                        )
                        raw = ts.as_pandas()
                        break
                    except Exception as e_req:
                        if is_minute_credit_error(e_req):
                            print(
                                f"  [RATE] minute credits hit; sleeping ~65s then retrying {ticker} (symbol={sym_try})")
                            time.sleep(65)
                            continue
                        raise

                if raw is None or len(raw) == 0:
                    raise RuntimeError("Empty response")

                # single-symbol responses can be a normal index, handle both
                sub = raw.reset_index(drop=False)
                if "datetime" not in sub.columns and "date" not in sub.columns and len(sub.columns) > 0:
                    sub = sub.rename(columns={sub.columns[0]: "datetime"})

                sub = normalize_ohlcv(sub)

                out_path = PARQUETS_DIR / f"{ticker}.parquet"
                PARQUETS_DIR.mkdir(parents=True, exist_ok=True)
                sub.to_parquet(out_path, index=False)

                ok, first_s, last_s, rows, ok_reason = coverage_status(sub)
                status = ok_reason if ok else "partial"

                append_jsonl(PROGRESS_JSONL, {
                    "asof_utc": utc_now_iso(),
                    "status": status,
                    "ticker": ticker,
                    "queried_symbol": sym_try,
                    "rows": rows,
                    "first_date": first_s,
                    "last_date": last_s,
                    "expected_last_date": EXPECTED_LAST_DATE,
                    "min_rows_ok": MIN_ROWS_OK,
                    "start_date": START_DATE,
                    "end_date": END_DATE,
                    "interval": INTERVAL,
                    "timezone": TZ,
                    "source": "retry_from_errors_csv",
                })

                meta_path = META_DIR / f"{ticker}.meta.json"
                META_DIR.mkdir(parents=True, exist_ok=True)
                meta_path.write_text(json.dumps({
                    "asof_utc": utc_now_iso(),
                    "provider": "twelvedata",
                    "ticker": ticker,
                    "queried_symbol": sym_try,
                    "interval": INTERVAL,
                    "start_date": START_DATE,
                    "end_date": END_DATE,
                    "timezone": TZ,
                    "rows": rows,
                    "first_date": first_s,
                    "last_date": last_s,
                    "expected_last_date": EXPECTED_LAST_DATE,
                    "min_rows_ok": MIN_ROWS_OK,
                    "status": status,
                    "output": str(out_path),
                }, indent=2), encoding="utf-8")

                if status in ("ok", "ok_short_history"):
                    ok_n += 1
                else:
                    partial_n += 1

                print(f"  [OK] queried={sym_try} status={status} rows={rows} last={last_s}")
                success = True
                break


            except Exception as e:
                last_exception = e
                # If it was a minute-credit issue, we would have retried above,
                # so anything here is a real symbol/candles failure -> try next candidate.
                continue

        if not success:
            err_n += 1
            append_jsonl(RETRY_ERRORS_JSONL, {
                "asof_utc": utc_now_iso(),
                "status": "error",
                "ticker": ticker,
                "error": str(last_exception) if last_exception else "unknown_error",
                "candidates": symbol_candidates(ticker),
                "source": "retry_from_errors_csv",
            })
            print(f"  [ERR] {ticker}: {last_exception}")

        time.sleep(SLEEP_BETWEEN_CALLS)

        if k % 25 == 0 or k == len(remaining):
            print(f"[PROG] {k}/{len(remaining)} ok={ok_n} partial={partial_n} err={err_n}")

    print("\n[DONE] Retry pass complete.")
    print(f"       ok={ok_n} partial={partial_n} err={err_n}")
    print(f"       retry_errors={RETRY_ERRORS_JSONL}")


if __name__ == "__main__":
    main()
