# research/experiments/07E_attach_sector_stage_to_stocks.py
"""
07E :: Attach Sector Spider Stage to Stock Daily Features
---------------------------------------------------------

Goal:
- For each stock daily features parquet, join the corresponding sector "spider stage" by date.

Inputs:
- Universe mapping (ticker -> sector):
    data/cleaned/universe/universe_trade_ready_*.csv

- Spider stages (output of 07D):
    data/cleaned/spiders_daily/stages/SECTOR_*.parquet
    columns: date, stage, stage_name, stage_reason, spider_id

- Stock daily features parquets (YOU must set STOCKS_IN_DIR):
    e.g. data/cleaned/stocks_daily/features/*.parquet
    must contain at least: date (or DatetimeIndex)

Outputs:
- Enriched stock parquets:
    data/cleaned/stocks_daily/features_with_sector_stage/*.parquet

Logs:
- data/cleaned/stocks_daily/features_with_sector_stage/_progress.jsonl
- data/cleaned/stocks_daily/features_with_sector_stage/_errors.jsonl
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd

# ----------------------------
# Paths (edit STOCKS_IN_DIR)
# ----------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

UNIVERSE_CSV = ROOT / "data" / "cleaned" / "universe" / "universe_trade_ready_20260205_133048.csv"

SPIDER_STAGES_DIR = ROOT / "data" / "cleaned" / "spiders_daily" / "stages"

STOCKS_IN_DIR = ROOT / "data" / "cleaned" / "stocks_daily" / "features"

OUT_DIR = ROOT / "data" / "cleaned" / "stocks_daily" / "features_with_sector_stage"
PROGRESS_JSONL = OUT_DIR / "_progress.jsonl"
ERRORS_JSONL = OUT_DIR / "_errors.jsonl"

SMOKE_N: Optional[int] = None  # set e.g. 5 for quick test


# ----------------------------
# Helpers
# ----------------------------
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


def sector_to_spider_id(sector: str) -> str:
    """
    Universe sectors look like: Financials, Healthcare, Technology, etc.
    Your spider ids are: SECTOR_FINANCIALS, SECTOR_HEALTHCARE, ...

    We map by:
      - upper
      - spaces -> underscores
      - ampersands -> AND
      - strip
    """
    s = (sector or "").strip()
    s = s.replace("&", "AND")
    s = s.replace("-", "_")
    s = s.replace(" ", "_")
    s = s.upper()
    return f"SECTOR_{s}"


def load_universe_map(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing universe CSV: {path}")
    uni = pd.read_csv(path)
    # required
    for c in ("ticker", "sector"):
        if c not in uni.columns:
            raise KeyError(f"Universe CSV missing column '{c}'")

    uni = uni.copy()
    uni["ticker"] = uni["ticker"].astype(str).str.strip()
    uni["sector"] = uni["sector"].astype(str).str.strip()
    uni["spider_id"] = uni["sector"].apply(sector_to_spider_id)
    return uni[["ticker", "sector", "spider_id"]]


def load_all_spider_stages(stages_dir: Path) -> pd.DataFrame:
    if not stages_dir.exists():
        raise FileNotFoundError(f"Missing spider stages dir: {stages_dir}")

    files = sorted(stages_dir.glob("SECTOR_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No spider stage parquets found in: {stages_dir}")

    parts = []
    for p in files:
        df = pd.read_parquet(p)
        # normalize expected columns
        needed = {"date", "stage", "stage_name", "stage_reason", "spider_id"}
        missing = needed - set(df.columns)
        if missing:
            raise KeyError(f"{p.name} missing columns: {sorted(missing)}")
        d = df.copy()
        d["date"] = pd.to_datetime(d["date"])
        parts.append(d[["date", "spider_id", "stage", "stage_name", "stage_reason"]])

    out = pd.concat(parts, ignore_index=True)
    # rename to avoid collisions with stock columns
    out = out.rename(
        columns={
            "stage": "sector_stage",
            "stage_name": "sector_stage_name",
            "stage_reason": "sector_stage_reason",
        }
    )
    return out


def list_stock_files(in_dir: Path) -> List[Path]:
    return sorted(in_dir.glob("*.parquet"))


def read_stock_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    # accept either a 'date' column or datetime index
    if "date" in df.columns:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        return df
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.copy()
        df = df.sort_index()
        df = df.reset_index().rename(columns={"index": "date"})
        df["date"] = pd.to_datetime(df["date"])
        return df
    raise KeyError(f"{path.name}: needs 'date' column or DatetimeIndex")


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    print("\n=== 07E :: Attach Sector Spider Stage to Stocks ===")
    print(f"[ROOT] {ROOT}")
    print(f"[UNI]  {UNIVERSE_CSV}")
    print(f"[SPDR] {SPIDER_STAGES_DIR}")
    print(f"[IN ]  {STOCKS_IN_DIR}")
    print(f"[OUT]  {OUT_DIR}")

    if not STOCKS_IN_DIR.exists():
        raise FileNotFoundError(
            f"Missing STOCKS_IN_DIR: {STOCKS_IN_DIR}\n"
            "Edit STOCKS_IN_DIR at the top of this script to your actual stock features folder."
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    uni = load_universe_map(UNIVERSE_CSV)
    spiders = load_all_spider_stages(SPIDER_STAGES_DIR)

    # For quick lookups
    uni_map = dict(zip(uni["ticker"], uni[["sector", "spider_id"]].to_dict(orient="records")))

    done = load_done_set(PROGRESS_JSONL)

    stock_files = list_stock_files(STOCKS_IN_DIR)
    if SMOKE_N is not None:
        stock_files = stock_files[: int(SMOKE_N)]

    print(f"[RUN] stocks_total={len(stock_files)} done={len(done)} remaining={len([p for p in stock_files if p.stem not in done])}")

    ok_n = 0
    err_n = 0

    for p in stock_files:
        ticker = p.stem  # assumes file name is TICKER.parquet
        if ticker in done:
            continue

        t0 = datetime.now(timezone.utc)
        try:
            stock = read_stock_parquet(p)

            # Map sector/spider_id
            meta = uni_map.get(ticker)
            if not meta:
                raise KeyError(f"{ticker}: not found in universe CSV (ticker column).")
            sector = meta["sector"]
            spider_id = meta["spider_id"]

            # Subset spider stages for this sector to speed up merge
            sp = spiders.loc[spiders["spider_id"] == spider_id].copy()
            if sp.empty:
                raise KeyError(f"{ticker}: spider stages not found for spider_id={spider_id}")

            # Merge on date
            merged = stock.merge(sp, on="date", how="left")

            # Attach sector metadata
            merged["sector"] = sector
            merged["spider_id"] = spider_id

            # Sanity: if all sector_stage are null, date alignment is broken
            if merged["sector_stage"].isna().all():
                raise RuntimeError(
                    f"{ticker}: merge produced all-NaN sector_stage. "
                    f"Check date alignment + timezone + spider stages date range for {spider_id}."
                )

            out_path = OUT_DIR / f"{ticker}.parquet"
            merged.to_parquet(out_path, index=False)

            append_jsonl(PROGRESS_JSONL, {
                "ts": utc_now(),
                "ticker": ticker,
                "status": "ok",
                "sector": sector,
                "spider_id": spider_id,
                "rows": int(len(merged)),
                "out": str(out_path),
                "elapsed_s": round((datetime.now(timezone.utc) - t0).total_seconds(), 3),
            })

            ok_n += 1
            if ok_n % 100 == 0:
                print(f"[OK] processed={ok_n} last={ticker}")

        except Exception as e:
            append_jsonl(ERRORS_JSONL, {
                "ts": utc_now(),
                "ticker": ticker,
                "status": "error",
                "error": repr(e),
                "file": str(p),
            })
            print(f"[ERROR] {ticker}: {e}")
            err_n += 1

    print(f"\n[SUMMARY] ok={ok_n} error={err_n}")
    if ok_n == 0 and err_n > 0:
        raise SystemExit("07E failed: zero stocks processed successfully.")
    print("[OK] 07E complete.")


if __name__ == "__main__":
    main()
