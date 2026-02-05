# Path: research/experiments/04_apply_universe_filters.py
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

import pandas as pd


# =============================================================================
# CONFIG (zero-arg runnable)
# =============================================================================
ROOT = Path(__file__).resolve().parents[2]

IN_DIR = ROOT / "data" / "cleaned" / "universe"
OUT_DIR = ROOT / "data" / "cleaned" / "universe"
META_DIR = ROOT / "data" / "metadata"
REPORT_DIR = ROOT / "research" / "reports"

EXCLUSION_RULES_PATH = META_DIR / "reit_exclusion.csv"

# --- Core filters ---
MIN_MARKET_CAP_USD = 300_000_000   # 300M
COUNTRY_KEEP = "USA"

# --- Toggle exclusions (CSV-driven) ---
EXCLUSIONS_ENABLED = True

# --- Columns to remove in trade-ready dataset ---
DROP_COLS = {
    "asof_utc",
    "source",
    "raw_file",
    "sector_raw",
    "industry_raw",
    "has_sector",
    "has_industry",
    "row_no"
}

REPORT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Utilities
# =============================================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def latest_contract_file() -> Path:
    files = sorted(IN_DIR.glob("universe_finviz_contract_*.csv"))
    if not files:
        raise FileNotFoundError("No contract universe file found. Run script 03 first.")
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)[0]


# =============================================================================
# Market cap parsing & formatting
# =============================================================================
def parse_market_cap_usd(x: str) -> float:
    """
    Finviz export:
    - Plain numbers are in MILLIONS
    - Supports M / B / T / K suffixes if present
    """
    if x is None:
        return float("nan")

    s = str(x).strip()
    if s in ("", "-", "nan"):
        return float("nan")

    if re.fullmatch(r"[\d\.]+", s):
        return float(s) * 1e6  # assume MILLIONS

    m = re.match(r"^([\d\.]+)\s*([TtBbMmKk])$", s)
    if not m:
        return float("nan")

    val = float(m.group(1))
    suf = m.group(2).upper()
    mult = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}[suf]
    return val * mult


def fmt_market_cap(usd: float) -> str:
    """
    Pretty formatting:
    - 1.2345T
    - 38.2937B
    - 287.5400M
    """
    if usd != usd:
        return ""
    if usd >= 1e12:
        return f"{usd/1e12:.4f}T"
    if usd >= 1e9:
        return f"{usd/1e9:.4f}B"
    if usd >= 1e6:
        return f"{usd/1e6:.4f}M"
    if usd >= 1e3:
        return f"{usd/1e3:.4f}K"
    return f"{usd:.4f}"


# =============================================================================
# Exclusion engine
# =============================================================================
def load_exclusion_rules() -> List[Dict]:
    if not EXCLUSION_RULES_PATH.exists():
        return []
    df = pd.read_csv(EXCLUSION_RULES_PATH)
    return df.to_dict("records")


def apply_exclusions(df: pd.DataFrame) -> pd.DataFrame:
    if not EXCLUSIONS_ENABLED:
        return df

    rules = load_exclusion_rules()
    if not rules:
        return df

    keep = pd.Series(True, index=df.index)

    for r in rules:
        rule = r["rule_type"]
        pattern = str(r["pattern"]).strip()

        if rule == "sector_equals":
            keep &= df["sector"] != pattern

        elif rule == "sector_in":
            sectors = [s.strip() for s in pattern.split(",")]
            keep &= ~df["sector"].isin(sectors)

        elif rule == "industry_equals":
            keep &= df["industry"] != pattern

        elif rule == "industry_contains":
            keep &= ~df["industry"].str.contains(pattern, case=False, na=False)

        elif rule == "ticker_in":
            tickers = [t.strip().upper() for t in pattern.split(",")]
            keep &= ~df["ticker"].isin(tickers)

    return df.loc[keep].copy()


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    in_path = latest_contract_file()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print("\n=== Apply Universe Filters (Trade-Ready) ===")
    print(f"[IN]  {in_path}")

    df = pd.read_csv(in_path)

    # Market cap handling
    df["market_cap_usd"] = df["market_cap"].apply(parse_market_cap_usd)
    df["market_cap_fmt"] = df["market_cap_usd"].apply(fmt_market_cap)

    # Core filters
    before = len(df)
    df = df[df["country"] == COUNTRY_KEEP]
    df = df[df["market_cap_usd"] >= MIN_MARKET_CAP_USD]

    # Optional exclusions
    df = apply_exclusions(df)
    after = len(df)

    # Build trade-ready view
    cols = [
        "ticker", "company", "sector", "industry", "country",
        "market_cap_usd", "market_cap_fmt",
        "pe_num", "price_num", "change_pct", "volume_num"
    ]
    trade_df = df[[c for c in cols if c in df.columns]].copy()
    trade_df.drop(columns=[c for c in DROP_COLS if c in trade_df.columns], inplace=True, errors="ignore")

    out_path = OUT_DIR / f"universe_trade_ready_{ts}.csv"
    trade_df.to_csv(out_path, index=False)

    # Report
    report = {
        "asof_utc": utc_now_iso(),
        "input": str(in_path),
        "output": str(out_path),
        "rows_before": int(before),
        "rows_after": int(after),
        "filters": {
            "country": COUNTRY_KEEP,
            "min_market_cap_usd": MIN_MARKET_CAP_USD,
            "exclusions_enabled": EXCLUSIONS_ENABLED,
            "exclusion_rules_file": str(EXCLUSION_RULES_PATH),
        },
        "columns": list(trade_df.columns),
    }

    rep_path = REPORT_DIR / f"universe_trade_ready_report_{ts}.json"
    rep_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"[OUT] {out_path}")
    print(f"[REP] {rep_path}")
    print(f"[INFO] rows {before} → {after}")
    print("\n[OK] Trade-ready universe generated.")


if __name__ == "__main__":
    main()
