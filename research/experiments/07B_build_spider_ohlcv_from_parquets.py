# research/experiments/07B_build_spider_ohlcv_from_parquets.py
"""
Stage 7A (07B) — Build Spider OHLCV series from member parquets (market-cap weighted).

Inputs:
- data/metadata/spiders/spider_memberships.csv   (required)
- data/raw/prices_daily/twelvedata/parquets/{TICKER}.parquet  (required)

Outputs:
- data/raw/spiders_daily/{SPIDER_ID}.parquet
- data/raw/spiders_daily/_progress.jsonl
- data/raw/spiders_daily/_errors.jsonl

Key rules (LOCKED):
- OHLC = weighted average of member OHLC (weights from spider_memberships)
- Only include members that have data for that date
- Renormalize automatically per date via divide by weight_coverage (sum of available weights)
- Sector spiders are regime filters, NOT tradable portfolios (no capping, no equal-weighting)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json
import time

import pandas as pd


# =========================
# CONFIG (EDIT HERE ONLY)
# =========================
ROOT = Path(__file__).resolve().parents[2]  # research/experiments/.. -> project root

MEMBERSHIPS_CSV = ROOT / "data" / "metadata" / "spiders" / "spider_memberships.csv"

# Stage 6 output location (adjust ONLY if your tree differs)
PRICES_DIR = ROOT / "data" / "raw" / "prices_daily" / "twelvedata"
PRICES_PARQUETS_DIR = PRICES_DIR / "parquets"

OUT_DIR = ROOT / "data" / "raw" / "spiders_daily"
PROGRESS_JSONL = OUT_DIR / "_progress.jsonl"
ERRORS_JSONL = OUT_DIR / "_errors.jsonl"

# Optional: process only these spiders (None = all)
ONLY_SPIDERS: Optional[List[str]] = None  # e.g. ["SECTOR_TECHNOLOGY"]

# Optional: quick smoke mode (limits tickers per spider)
SMOKE_MAX_TICKERS_PER_SPIDER: Optional[int] = None  # e.g. 25

# Minimum acceptable weight_coverage on a day (below this, row is dropped)
MIN_DAILY_COVERAGE = 0.10

# Liveness logging
PRINT_EVERY_TICKERS = 25


# =========================
# Utilities
# =========================
def append_jsonl(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def try_read_member_parquet(ticker: str) -> Optional[pd.DataFrame]:
    """
    Return dataframe if parquet exists + is readable; otherwise return None.
    Missing/failed members are logged by caller.
    """
    p = PRICES_PARQUETS_DIR / f"{ticker}.parquet"
    if not p.exists():
        return None
    try:
        return safe_read_member_parquet(ticker)
    except Exception:
        return None


def read_progress_done(progress_path: Path) -> set[str]:
    done = set()
    if not progress_path.exists():
        return done
    with progress_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if obj.get("status") == "ok" and obj.get("spider_id"):
                done.add(str(obj["spider_id"]))
    return done


def safe_read_member_parquet(ticker: str) -> pd.DataFrame:
    p = PRICES_PARQUETS_DIR / f"{ticker}.parquet"
    if not p.exists():
        raise FileNotFoundError(f"Missing parquet for {ticker}: {p}")

    df = pd.read_parquet(p)

    # Accept common schemas; normalize to required columns.
    # We expect at least: date + open/high/low/close + volume
    # Date column could be "date" or "time" depending on writer.
    date_col = None
    for c in ["date", "time", "datetime", "time_utc"]:
        if c in df.columns:
            date_col = c
            break
    if date_col is None:
        raise KeyError(f"{ticker} parquet missing a date/time column. cols={list(df.columns)}")

    # Normalize date index
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df = df.dropna(subset=[date_col])
    df = df.rename(columns={date_col: "date"})

    needed = ["open", "high", "low", "close"]
    for c in needed:
        if c not in df.columns:
            raise KeyError(f"{ticker} parquet missing column '{c}'. cols={list(df.columns)}")
    if "volume" not in df.columns:
        # allow volume missing; fill with 0
        df["volume"] = 0.0

    # Keep only required cols
    df = df[["date", "open", "high", "low", "close", "volume"]].drop_duplicates(subset=["date"])
    df = df.sort_values("date")
    return df


@dataclass
class SpiderBuildResult:
    spider_id: str
    rows: int
    first_date: Optional[str]
    last_date: Optional[str]
    members_total: int
    members_used_median: float
    coverage_median: float


def build_spider_series(spider_id: str, members_df: pd.DataFrame) -> Tuple[pd.DataFrame, SpiderBuildResult]:
    """
    Build one spider OHLCV series from member parquets.

    members_df columns: ticker, weight
    """
    t0 = time.time()

    members_df = members_df.copy()
    members_df["ticker"] = members_df["ticker"].astype(str)
    members_df["weight"] = members_df["weight"].astype(float)

    # Optional smoke mode
    if SMOKE_MAX_TICKERS_PER_SPIDER is not None:
        members_df = members_df.head(int(SMOKE_MAX_TICKERS_PER_SPIDER))

    tickers = members_df["ticker"].tolist()
    weights = dict(zip(members_df["ticker"], members_df["weight"]))
    members_total = len(tickers)

    # Pass 1: gather union of dates across members (small & safe)
    all_dates: set = set()
    for i, tkr in enumerate(tickers, 1):

        df_t = try_read_member_parquet(tkr)
        if df_t is None:
            continue
        all_dates.update(df_t["date"].tolist())

        if i % PRINT_EVERY_TICKERS == 0 or i == members_total:
            print(f"  [{spider_id}] scanned dates {i}/{members_total}")

    if not all_dates:
        out = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "members_used", "weight_coverage"])
        res = SpiderBuildResult(spider_id, 0, None, None, members_total, 0.0, 0.0)
        return out, res

    idx = pd.Index(sorted(all_dates), name="date")

    # Aggregators (numerators)
    agg_open = pd.Series(0.0, index=idx)
    agg_high = pd.Series(0.0, index=idx)
    agg_low  = pd.Series(0.0, index=idx)
    agg_close= pd.Series(0.0, index=idx)
    agg_vol  = pd.Series(0.0, index=idx)

    # Denominator (weight coverage) + members used
    w_cov = pd.Series(0.0, index=idx)
    members_used = pd.Series(0, index=idx, dtype="int32")

    # Pass 2: accumulate weighted OHLC + volume
    for i, tkr in enumerate(tickers, 1):
        w = float(weights.get(tkr, 0.0))
        if w <= 0:
            continue

        df_t0 = try_read_member_parquet(tkr)
        if df_t0 is None:
            continue
        df_t = df_t0.set_index("date")

        # Align to spider index without wide merges
        close = df_t["close"].reindex(idx)
        mask = close.notna()
        if not mask.any():
            continue

        open_ = df_t["open"].reindex(idx)
        high  = df_t["high"].reindex(idx)
        low   = df_t["low"].reindex(idx)
        vol   = df_t["volume"].reindex(idx).fillna(0.0)

        # Use mask on close (if close present, we assume the bar is present)
        agg_open.loc[mask]  += w * open_.loc[mask].astype(float)
        agg_high.loc[mask]  += w * high.loc[mask].astype(float)
        agg_low.loc[mask]   += w * low.loc[mask].astype(float)
        agg_close.loc[mask] += w * close.loc[mask].astype(float)

        # Volume: sum (unweighted)
        agg_vol.loc[mask] += vol.loc[mask].astype(float)

        w_cov.loc[mask] += w
        members_used.loc[mask] += 1

        if i % PRINT_EVERY_TICKERS == 0 or i == members_total:
            print(f"  [{spider_id}] aggregated {i}/{members_total}")

    # Finalize: renormalize by coverage
    valid = w_cov >= float(MIN_DAILY_COVERAGE)
    if not valid.any():
        out = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "members_used", "weight_coverage"])
        res = SpiderBuildResult(spider_id, 0, None, None, members_total, 0.0, 0.0)
        return out, res

    out = pd.DataFrame({
        "date": idx[valid].astype(str),
        "open":  (agg_open[valid]  / w_cov[valid]).values,
        "high":  (agg_high[valid]  / w_cov[valid]).values,
        "low":   (agg_low[valid]   / w_cov[valid]).values,
        "close": (agg_close[valid] / w_cov[valid]).values,
        "volume": agg_vol[valid].values,
        "members_used": members_used[valid].values.astype(int),
        "weight_coverage": w_cov[valid].values,
    })

    out = out.sort_values("date").reset_index(drop=True)

    first_date = out["date"].iloc[0] if len(out) else None
    last_date = out["date"].iloc[-1] if len(out) else None

    res = SpiderBuildResult(
        spider_id=spider_id,
        rows=int(len(out)),
        first_date=str(first_date) if first_date else None,
        last_date=str(last_date) if last_date else None,
        members_total=int(members_total),
        members_used_median=float(out["members_used"].median()) if len(out) else 0.0,
        coverage_median=float(out["weight_coverage"].median()) if len(out) else 0.0,
    )

    dt = time.time() - t0
    print(f"[DONE] {spider_id}: rows={res.rows} first={res.first_date} last={res.last_date} "
          f"members={res.members_total} med_used={res.members_used_median:.1f} med_cov={res.coverage_median:.3f} "
          f"({dt:.1f}s)")
    return out, res


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not MEMBERSHIPS_CSV.exists():
        raise FileNotFoundError(f"Missing memberships CSV: {MEMBERSHIPS_CSV}")

    if not PRICES_PARQUETS_DIR.exists():
        raise FileNotFoundError(f"Missing parquets dir: {PRICES_PARQUETS_DIR}")

    mem = pd.read_csv(MEMBERSHIPS_CSV)

    req = {"spider_id", "sector", "ticker", "market_cap_usd", "weight"}
    missing = req - set(mem.columns)
    if missing:
        raise KeyError(f"Missing columns in spider_memberships.csv: {sorted(missing)}")

    mem["spider_id"] = mem["spider_id"].astype(str)
    mem["ticker"] = mem["ticker"].astype(str)

    # Optional subset
    spiders = sorted(mem["spider_id"].unique().tolist())
    if ONLY_SPIDERS:
        only = set(map(str, ONLY_SPIDERS))
        spiders = [s for s in spiders if s in only]

    done = read_progress_done(PROGRESS_JSONL)
    remaining = [s for s in spiders if s not in done]

    print("\n=== 07B :: Build Spider OHLCV from Member Parquets ===")
    print(f"[ROOT] {ROOT}")
    print(f"[IN]   memberships={MEMBERSHIPS_CSV}")
    print(f"[IN]   parquets_dir={PRICES_PARQUETS_DIR}")
    print(f"[OUT]  spiders_dir={OUT_DIR}")
    print(f"[RUN]  spiders_total={len(spiders)} done={len(done)} remaining={len(remaining)}")
    print(f"[CFG]  smoke_max_tickers={SMOKE_MAX_TICKERS_PER_SPIDER} min_daily_coverage={MIN_DAILY_COVERAGE}")

    if not remaining:
        print("[OK] Nothing to do.")
        return

    for spider_id in remaining:
        t0 = time.time()
        try:
            sub = mem[mem["spider_id"] == spider_id].sort_values("weight", ascending=False)

            missing_members = []
            for tkr in sub["ticker"].astype(str).tolist():
                if not (PRICES_PARQUETS_DIR / f"{tkr}.parquet").exists():
                    missing_members.append(tkr)

            if missing_members:
                append_jsonl(ERRORS_JSONL, {
                    "ts": pd.Timestamp.utcnow().isoformat(),
                    "spider_id": spider_id,
                    "status": "missing_member_parquet",
                    "missing_count": len(missing_members),
                    "missing": missing_members[:50],  # cap for log hygiene
                })

            # Build series
            out_df, res = build_spider_series(spider_id, sub)

            # Write parquet
            out_path = OUT_DIR / f"{spider_id}.parquet"
            out_df.to_parquet(out_path, index=False)

            append_jsonl(PROGRESS_JSONL, {
                "ts": pd.Timestamp.utcnow().isoformat(),
                "spider_id": spider_id,
                "status": "ok",
                "rows": res.rows,
                "first_date": res.first_date,
                "last_date": res.last_date,
                "members_total": res.members_total,
                "missing_members_count": len(missing_members),
                "members_used_median": res.members_used_median,
                "coverage_median": res.coverage_median,
                "elapsed_s": round(time.time() - t0, 3),
                "out": str(out_path),
            })

        except Exception as e:
            append_jsonl(ERRORS_JSONL, {
                "ts": pd.Timestamp.utcnow().isoformat(),
                "spider_id": spider_id,
                "status": "error",
                "error": repr(e),
            })
            print(f"[ERROR] {spider_id}: {e}")

    print("\n[OK] 07B complete.")


if __name__ == "__main__":
    main()
