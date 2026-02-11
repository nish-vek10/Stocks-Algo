# Path: research/experiments/07A_build_spider_memberships.py
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd


# =============================================================================
# CONFIG — EDIT THESE ONLY
# =============================================================================
ROOT = Path(__file__).resolve().parents[2]

# You confirmed this exact file:
UNIVERSE_CSV = ROOT / "data" / "cleaned" / "universe" / "universe_trade_ready_20260205_133048.csv"

# Outputs
OUT_DIR = ROOT / "data" / "metadata" / "spiders"
OUT_MEMBERSHIPS = OUT_DIR / "spider_memberships.csv"
OUT_SUMMARY = OUT_DIR / "spider_summary.csv"

# Column names (we’ll auto-detect market cap column if this doesn’t match)
TICKER_COL_CANDIDATES = ["ticker", "symbol", "Ticker", "Symbol"]
SECTOR_COL_CANDIDATES = ["sector", "Sector"]
MCAP_COL_CANDIDATES = ["market_cap", "market_cap_usd", "Market Cap", "marketcap", "mcap"]


# =============================================================================
# Helpers
# =============================================================================
def pick_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def parse_market_cap_to_usd(x) -> Optional[float]:
    """
    Accepts:
      - numeric already
      - strings like '300M', '1.2B', '950K'
      - strings like '$1.2B'
      - strings like '1,234,567,890'
    Returns float USD or None.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None

    # already numeric
    if isinstance(x, (int, float)):
        v = float(x)
        if pd.isna(v):
            return None
        return v

    s = str(x).strip()
    if not s:
        return None

    s = s.replace("$", "").replace(",", "").upper()

    # Pure numeric
    try:
        return float(s)
    except Exception:
        pass

    # Suffix pattern
    m = re.match(r"^([0-9]*\.?[0-9]+)\s*([KMBT])$", s)
    if not m:
        return None

    num = float(m.group(1))
    suf = m.group(2)
    mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}.get(suf, None)
    if mult is None:
        return None
    return num * mult


def make_spider_id(sector: str) -> str:
    s = str(sector).strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return f"SECTOR_{s}"


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    if not UNIVERSE_CSV.exists():
        raise FileNotFoundError(f"Universe file not found: {UNIVERSE_CSV}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    uni = pd.read_csv(UNIVERSE_CSV)

    ticker_col = pick_col(uni, TICKER_COL_CANDIDATES)
    sector_col = pick_col(uni, SECTOR_COL_CANDIDATES)
    mcap_col = pick_col(uni, MCAP_COL_CANDIDATES)

    if ticker_col is None:
        raise KeyError(f"Could not find ticker column. candidates={TICKER_COL_CANDIDATES} cols={list(uni.columns)}")
    if sector_col is None:
        raise KeyError(f"Could not find sector column. candidates={SECTOR_COL_CANDIDATES} cols={list(uni.columns)}")
    if mcap_col is None:
        raise KeyError(
            f"Could not find market cap column. candidates={MCAP_COL_CANDIDATES} cols={list(uni.columns)}"
        )

    df = uni[[ticker_col, sector_col, mcap_col]].copy()
    df.columns = ["ticker", "sector", "market_cap_raw"]

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["sector"] = df["sector"].astype(str).str.strip()

    # Parse market cap into USD float
    df["market_cap_usd"] = df["market_cap_raw"].apply(parse_market_cap_to_usd)

    # Drop junk rows
    df = df[df["ticker"].notna() & (df["ticker"] != "")]
    df = df[df["sector"].notna() & (df["sector"] != "")]

    # If market cap missing, we can’t weight reliably → drop (or set equal weights later)
    missing_mcap_n = int(df["market_cap_usd"].isna().sum())
    if missing_mcap_n > 0:
        print(f"[WARN] market_cap_usd missing for {missing_mcap_n} rows. Dropping them for spider weights.")
        df = df.dropna(subset=["market_cap_usd"])

    # Build spider_id
    df["spider_id"] = df["sector"].apply(make_spider_id)

    # Compute weights per sector spider
    df["spider_mcap_sum"] = df.groupby("spider_id")["market_cap_usd"].transform("sum")
    df["weight"] = df["market_cap_usd"] / df["spider_mcap_sum"]

    # Sanity: remove any spiders with bad sums (e.g. zero)
    df = df[df["spider_mcap_sum"] > 0].copy()

    # Final memberships table
    out = df[["spider_id", "sector", "ticker", "market_cap_usd", "weight"]].copy()
    out = out.sort_values(["spider_id", "weight"], ascending=[True, False]).reset_index(drop=True)

    # Summary table for quick checks
    summary = (
        out.groupby(["spider_id", "sector"], as_index=False)
        .agg(
            members=("ticker", "count"),
            mcap_sum_usd=("market_cap_usd", "sum"),
            weight_sum=("weight", "sum"),
            top1_weight=("weight", "max"),
        )
        .sort_values("mcap_sum_usd", ascending=False)
        .reset_index(drop=True)
    )

    out.to_csv(OUT_MEMBERSHIPS, index=False)
    summary.to_csv(OUT_SUMMARY, index=False)

    # Print key checks
    worst = float((summary["weight_sum"] - 1.0).abs().max()) if len(summary) else 0.0
    print("\n=== Spider Memberships (Sector-only) ===")
    print(f"[IN ] {UNIVERSE_CSV}")
    print(f"[OUT] {OUT_MEMBERSHIPS}")
    print(f"[SUM] {OUT_SUMMARY}")
    print(f"[OK ] spiders={len(summary)} members={len(out)} worst_abs_weight_sum_error={worst:.6f}")

    # Warn if any sector is dominated by one stock
    dom = summary[summary["top1_weight"] >= 0.35]
    if len(dom):
        print(f"[WARN] {len(dom)} spiders have top1_weight >= 0.35 (dominance risk). See spider_summary.csv.")


if __name__ == "__main__":
    main()
