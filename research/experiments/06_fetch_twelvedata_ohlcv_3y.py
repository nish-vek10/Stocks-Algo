# Path: research/experiments/06_fetch_twelvedata_ohlcv_3y.py
from __future__ import annotations

import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Tuple

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
START_DATE = os.getenv("TD_START_DATE", "2021-01-01").strip()
END_DATE = os.getenv("TD_END_DATE", "2026-02-01").strip()
TZ = os.getenv("TD_TIMEZONE", "UTC").strip()

CREDITS_PER_MIN = int(os.getenv("TD_CREDITS_PER_MIN", os.getenv("TD_REQUESTS_PER_MIN", "8")))
BATCH_SIZE = int(os.getenv("TD_BATCH_SIZE", "8"))
OUTPUTSIZE = int(os.getenv("TD_OUTPUTSIZE", "5000"))

SMOKE_N = int(os.getenv("TD_SMOKE_N", "0"))
SMOKE_TICKERS = os.getenv("TD_SMOKE_TICKERS", "").strip()

# Completeness gates (prevents marking partial tickers as OK)
EXPECTED_LAST_DATE = os.getenv("TD_EXPECTED_LAST_DATE", "2026-01-30").strip()
MIN_ROWS_OK = int(os.getenv("TD_MIN_ROWS_OK", "1200"))


UNIVERSE_DIR = ROOT / "data" / "cleaned" / "universe"
OUT_DIR = ROOT / "data" / "raw" / "prices_daily" / "twelvedata"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PARQUETS_DIR = OUT_DIR / "parquets"
META_DIR = OUT_DIR / "meta"
PARQUETS_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

PROGRESS_JSONL = OUT_DIR / "_progress.jsonl"
ERRORS_JSONL = OUT_DIR / "_errors.jsonl"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def latest_trade_ready_universe() -> Path:
    files = sorted(UNIVERSE_DIR.glob("universe_trade_ready_20260205*.csv"))
    if not files:
        raise FileNotFoundError("No universe_trade_ready_20260205*.csv found. Run Stage 4 first.")
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[0]


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ---- standardize time column into df["date"] ----
    if "datetime" in df.columns:
        df["date"] = pd.to_datetime(df["datetime"], errors="coerce", utc=False)
        df = df.drop(columns=["datetime"])
    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=False)
    else:
        # common names after reset_index() on MultiIndex frames
        for candidate in ("time", "timestamp", "level_1", "index"):
            if candidate in df.columns:
                df["date"] = pd.to_datetime(df[candidate], errors="coerce", utc=False)
                df = df.drop(columns=[candidate])
                break

    # If still no date, fail loudly with context (this is what was happening)
    if "date" not in df.columns:
        raise KeyError(f"normalize_ohlcv: no datetime/date column found. cols={list(df.columns)}")

    # ---- ensure numeric OHLCV ----
    for c in ["open", "high", "low", "close", "volume"]:
        if c not in df.columns:
            df[c] = None
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df[["date", "open", "high", "low", "close", "volume"]]


def coverage_status(sub: pd.DataFrame) -> Tuple[bool, str, str, int, str]:
    """
    Returns:
      (is_ok, first_date_str, last_date_str, rows, ok_reason)

    RULE:
      - OK if last_date >= EXPECTED_LAST_DATE
      - If OK but rows < MIN_ROWS_OK => ok_short_history (still OK / should be skipped next run)
      - Else OK => ok
      - Else partial
    """
    rows = int(len(sub))
    if rows == 0 or "date" not in sub.columns:
        return (False, None, None, rows, "empty_or_missing_date")

    first_dt = pd.to_datetime(sub["date"].iloc[0], errors="coerce")
    last_dt  = pd.to_datetime(sub["date"].iloc[-1], errors="coerce")

    first_s = first_dt.strftime("%Y-%m-%d") if pd.notna(first_dt) else None
    last_s  = last_dt.strftime("%Y-%m-%d") if pd.notna(last_dt) else None

    exp = pd.to_datetime(EXPECTED_LAST_DATE, errors="coerce")
    if pd.isna(last_dt) or pd.isna(exp):
        return (False, first_s, last_s, rows, "bad_dates")

    if last_dt >= exp:
        reason = "ok_short_history" if rows < MIN_ROWS_OK else "ok"
        return (True, first_s, last_s, rows, reason)

    return (False, first_s, last_s, rows, "partial")


def read_done_set() -> set:
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


def append_jsonl(path: Path, obj: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")


def chunk(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]


def sleep_for_rate_limit(last_req_time: float, batch_credits: int) -> float:
    """
    TwelveData minute limit is CREDIT-based.
    Assume: 1 symbol ~= 1 credit for time_series batch.
    We space calls so credits/min isn't exceeded.

    If credits_per_min=8:
      - batch=8 => 1 call per 60s
      - batch=4 => 1 call per 30s (2 calls/min)
    """
    if batch_credits <= 0:
        return time.time()

    # Required spacing to stay under credits/min
    min_gap = 60.0 * (batch_credits / max(1, CREDITS_PER_MIN))

    now = time.time()
    gap = now - last_req_time
    if gap < min_gap:
        time.sleep(min_gap - gap)

    return time.time()


def main() -> None:
    if not API_KEY:
        raise RuntimeError("TWELVEDATA_API_KEY missing in .env")

    uni_path = latest_trade_ready_universe()
    uni = pd.read_csv(uni_path)
    tickers = sorted(uni["ticker"].astype(str).str.upper().str.strip().unique().tolist())

    done = read_done_set()
    remaining = [t for t in tickers if t not in done]

    # ---- optional smoke-test limiting ----
    if SMOKE_TICKERS:
        forced = [x.strip().upper() for x in SMOKE_TICKERS.split(",") if x.strip()]
        remaining = [t for t in forced if t in remaining]
    elif SMOKE_N > 0:
        remaining = remaining[:SMOKE_N]

    if SMOKE_TICKERS or SMOKE_N > 0:
        print(f"[SMOKE] enabled n={SMOKE_N} tickers='{SMOKE_TICKERS}'")

    if not remaining:
        print("\n[OK] Nothing to fetch (0 remaining). Exiting without API calls.")
        return

    # Ensure we never request more symbols than credits allowed per minute
    if BATCH_SIZE > CREDITS_PER_MIN:
        print(f"[WARN] BATCH_SIZE={BATCH_SIZE} > credits_per_min={CREDITS_PER_MIN}. Capping batch size.")
        batch_size_eff = CREDITS_PER_MIN
    else:
        batch_size_eff = BATCH_SIZE

    print("\n=== Twelve Data :: Fetch Daily OHLCV (3y) ===")
    print(f"[ROOT] {ROOT}")
    print(f"[UNI]  {uni_path.name}  tickers={len(tickers)} remaining={len(remaining)}")
    print(f"[WIN]  {START_DATE} → {END_DATE}  [{INTERVAL}] tz={TZ}")
    print(f"[CFG]  batch={batch_size_eff} credits_per_min={CREDITS_PER_MIN}")
    print(f"[GATE] expected_last={EXPECTED_LAST_DATE} (rows<{MIN_ROWS_OK} => ok_short_history, still skipped next run)")
    print(f"[OUT]  {OUT_DIR}  (parquets/, meta/, _progress.jsonl)")

    td = TDClient(apikey=API_KEY)

    last_req_time = 0.0
    batches = chunk(remaining, max(1, batch_size_eff))

    ok_n = 0
    partial_n = 0
    err_n = 0
    total_remaining = len(remaining)
    processed_n = 0

    for i, batch in enumerate(batches, start=1):
        print(f"[BATCH] {i}/{len(batches)} size={len(batch)} first={batch[0]} last={batch[-1]}")
        last_req_time = sleep_for_rate_limit(last_req_time, batch_credits=len(batch))

        try:
            # Retry the SAME batch if TwelveData says "out of credits for the current minute"
            while True:
                try:
                    ts = td.time_series(
                        symbol=batch,
                        interval=INTERVAL,
                        start_date=START_DATE,
                        end_date=END_DATE,
                        outputsize=OUTPUTSIZE,
                        timezone=TZ,
                        order="asc",
                    )
                    df = ts.as_pandas()
                    last_req_time = time.time()
                    break
                except Exception as e_req:
                    msg = str(e_req).lower()
                    if "run out of api credits for the current minute" in msg or "out of api credits" in msg:
                        print(f"[RATE] minute credits hit; sleeping ~65s then retrying batch {i}/{len(batches)} (size={len(batch)})")
                        time.sleep(65)
                        last_req_time = time.time()
                        continue
                    raise

            if df is None or len(df) == 0:
                raise RuntimeError("Empty response")

            # Batch .as_pandas usually returns MultiIndex (symbol, datetime)
            if isinstance(df.index, pd.MultiIndex):
                for sym in batch:
                    try:
                        if sym not in df.index.get_level_values(0):
                            raise KeyError(f"Symbol missing from batch response: {sym}")
                        sub = df.xs(sym, level=0).reset_index()

                        # If neither datetime nor date exist, assume first col is time and rename it.
                        if "datetime" not in sub.columns and "date" not in sub.columns and len(sub.columns) > 0:
                            sub = sub.rename(columns={sub.columns[0]: "datetime"})

                        sub = normalize_ohlcv(sub)

                        out_path = PARQUETS_DIR / f"{sym}.parquet"
                        sub.to_parquet(out_path, index=False)

                        ok, first_s, last_s, rows, ok_reason = coverage_status(sub)
                        status = ok_reason if ok else "partial"

                        append_jsonl(PROGRESS_JSONL, {
                            "asof_utc": utc_now_iso(),
                            "status": status,
                            "ticker": sym,
                            "rows": rows,
                            "first_date": first_s,
                            "last_date": last_s,
                            "expected_last_date": EXPECTED_LAST_DATE,
                            "min_rows_ok": MIN_ROWS_OK,
                            "batch_i": i,
                            "batch_size": len(batch),
                            "start_date": START_DATE,
                            "end_date": END_DATE,
                        })

                        meta_path = META_DIR / f"{sym}.meta.json"
                        meta = {
                            "asof_utc": utc_now_iso(),
                            "provider": "twelvedata",
                            "ticker": sym,
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
                        }
                        meta_path.parent.mkdir(parents=True, exist_ok=True)
                        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                        processed_n += 1
                        if status in ("ok", "ok_short_history"):
                            ok_n += 1
                        else:
                            partial_n += 1

                    except Exception as e_sym:
                        try:
                            idx_names = list(df.index.names) if isinstance(df.index, pd.MultiIndex) else [df.index.name]
                        except Exception:
                            idx_names = ["<unknown>"]

                        append_jsonl(ERRORS_JSONL, {
                            "asof_utc": utc_now_iso(),
                            "status": "error",
                            "ticker": sym,
                            "batch_i": i,
                            "error": str(e_sym),
                            "df_index_names": idx_names,
                        })

                        processed_n += 1
                        err_n += 1

            else:
                # Some responses might come single-frame; handle defensively
                if len(batch) != 1:
                    raise RuntimeError("Non-multiindex response for a batch > 1")

                sym = batch[0]
                sub = normalize_ohlcv(df.reset_index(drop=False))

                out_path = PARQUETS_DIR / f"{sym}.parquet"
                sub.to_parquet(out_path, index=False)

                ok, first_s, last_s, rows, ok_reason = coverage_status(sub)
                status = ok_reason if ok else "partial"

                append_jsonl(PROGRESS_JSONL, {
                    "asof_utc": utc_now_iso(),
                    "status": status,
                    "ticker": sym,
                    "rows": rows,
                    "first_date": first_s,
                    "last_date": last_s,
                    "expected_last_date": EXPECTED_LAST_DATE,
                    "min_rows_ok": MIN_ROWS_OK,
                    "batch_i": i,
                    "batch_size": len(batch),
                    "start_date": START_DATE,
                    "end_date": END_DATE,
                })

                meta_path = META_DIR / f"{sym}.meta.json"
                meta = {
                    "asof_utc": utc_now_iso(),
                    "provider": "twelvedata",
                    "ticker": sym,
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
                }
                meta_path.parent.mkdir(parents=True, exist_ok=True)
                meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

                processed_n += 1
                if status in ("ok", "ok_short_history"):
                    ok_n += 1
                else:
                    partial_n += 1

            if i % 5 == 0 or i == len(batches):
                pct = (processed_n / total_remaining * 100.0) if total_remaining else 100.0
                print(
                    f"[PROG] batch={i}/{len(batches)}  "
                    f"done={processed_n}/{total_remaining} ({pct:.1f}%)  "
                    f"ok={ok_n} partial={partial_n} err={err_n}"
                )

        except Exception as e:
            for sym in batch:
                append_jsonl(ERRORS_JSONL, {
                    "asof_utc": utc_now_iso(),
                    "status": "error",
                    "ticker": sym,
                    "batch_i": i,
                    "error": str(e),
                })
                processed_n += 1
                err_n += 1

            print(f"[WARN] batch {i}/{len(batches)} failed: {e}")

    print("\n[OK] Fetch run complete. You can re-run the script anytime; it will skip completed tickers.")


if __name__ == "__main__":
    main()