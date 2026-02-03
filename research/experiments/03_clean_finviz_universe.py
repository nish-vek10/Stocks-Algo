# Path: research/experiments/03_clean_finviz_universe.py
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd


# =============================================================================
# CONFIG (zero-arg runnable)
# =============================================================================
ROOT = Path(__file__).resolve().parents[2]  # ALGO-STOCKS/

CLEAN_UNIVERSE_DIR = ROOT / "data" / "cleaned" / "universe"
OUT_DIR = ROOT / "data" / "cleaned" / "universe"  # keep in same folder for now
REPORT_DIR = ROOT / "research" / "reports"
META_DIR = ROOT / "data" / "metadata"

SECTOR_MAP_PATH = META_DIR / "sector_mapping.csv"  # optional, safe mapping only

REPORT_DIR.mkdir(parents=True, exist_ok=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def find_latest_promoted_csv() -> Path:
    """
    Finds the latest promoted file created by 02_promote_finviz_raw_to_cleaned.py
    """
    candidates = sorted(CLEAN_UNIVERSE_DIR.glob("universe_finviz_rawpromote_*.csv"))
    if not candidates:
        raise FileNotFoundError(
            f"No promoted universe file found in {CLEAN_UNIVERSE_DIR}. Run 02_promote... first."
        )
    candidates = sorted(candidates, key=lambda p: (p.name, p.stat().st_mtime), reverse=True)
    return candidates[0]


# -----------------------------
# Parsers / Normalizers
# -----------------------------
def parse_market_cap(x: str) -> float:
    """
    Finviz market cap comes like: 2.34T, 950.1B, 120.5M, 800K, or '-' / empty.
    Returns market cap in USD (absolute number).
    """
    if x is None:
        return float("nan")
    s = str(x).strip()
    if s in ("", "-", "nan", "None"):
        return float("nan")

    m = re.match(r"^([\d\.]+)\s*([TtBbMmKk])?$", s)
    if not m:
        return float("nan")

    val = float(m.group(1))
    suf = (m.group(2) or "").upper()

    mult = 1.0
    if suf == "T":
        mult = 1e12
    elif suf == "B":
        mult = 1e9
    elif suf == "M":
        mult = 1e6
    elif suf == "K":
        mult = 1e3

    return val * mult


def parse_pe(x: str) -> float:
    if x is None:
        return float("nan")
    s = str(x).strip()
    if s in ("", "-", "nan", "None"):
        return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def parse_price(x: str) -> float:
    if x is None:
        return float("nan")
    s = str(x).strip().replace("$", "")
    if s in ("", "-", "nan", "None"):
        return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def parse_change_pct(x: str) -> float:
    """
    Finviz 'change' is often like: 1.23% or -0.54%
    Returns percent as float (e.g., 1.23, -0.54)
    """
    if x is None:
        return float("nan")
    s = str(x).strip().replace("%", "")
    if s in ("", "-", "nan", "None"):
        return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def parse_volume(x: str) -> float:
    if x is None:
        return float("nan")
    s = str(x).replace(",", "").strip()
    if s in ("", "-", "nan", "None"):
        return float("nan")
    try:
        return float(s)
    except ValueError:
        return float("nan")


def load_sector_map() -> Dict[str, str]:
    """
    Optional sector normalization map.
    If file missing, mapping is empty and we keep raw sectors.
    """
    if not SECTOR_MAP_PATH.exists():
        return {}
    df = pd.read_csv(SECTOR_MAP_PATH)
    out = {}
    for _, r in df.iterrows():
        out[str(r["source_sector"]).strip()] = str(r["canonical_sector"]).strip()
    return out


def profile_categoricals(df: pd.DataFrame, cols: Tuple[str, ...], topn: int = 30) -> Dict:
    rep = {}
    for c in cols:
        if c not in df.columns:
            continue
        vc = (
            df[c]
            .fillna("")
            .astype(str)
            .str.strip()
            .replace({"": "∅"})
            .value_counts()
            .head(topn)
        )
        rep[c] = vc.to_dict()
    return rep


def main() -> None:
    in_path = find_latest_promoted_csv()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print("\n=== Clean Finviz Universe (Standardize, NO DROPS) ===")
    print(f"[IN]   {in_path}")

    df = pd.read_csv(in_path)

    # --- Rename columns to canonical internal names (keep original too if useful) ---
    # 'no.' isn't useful downstream, but we keep it for traceability for now.
    rename = {
        "no.": "row_no",
        "p/e": "pe",
    }
    df = df.rename(columns=rename)

    # Ensure ticker clean
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()

    # Add raw copies for audit
    if "sector" in df.columns:
        df["sector_raw"] = df["sector"]
    if "industry" in df.columns:
        df["industry_raw"] = df["industry"]

    # Optional sector mapping (no drops)
    sector_map = load_sector_map()
    if sector_map and "sector" in df.columns:
        df["sector"] = df["sector"].astype(str).str.strip().map(sector_map).fillna(df["sector"].astype(str).str.strip())

    # --- Type conversions (safe) ---
    if "market_cap" in df.columns:
        df["market_cap_usd"] = df["market_cap"].apply(parse_market_cap)
    if "pe" in df.columns:
        df["pe_num"] = df["pe"].apply(parse_pe)
    if "price" in df.columns:
        df["price_num"] = df["price"].apply(parse_price)
    if "change" in df.columns:
        df["change_pct"] = df["change"].apply(parse_change_pct)
    if "volume" in df.columns:
        df["volume_num"] = df["volume"].apply(parse_volume)

    # --- Basic integrity flags (no drops) ---
    df["has_sector"] = df.get("sector", pd.Series([None] * len(df))).notna()
    df["has_industry"] = df.get("industry", pd.Series([None] * len(df))).notna()

    # --- Write contract dataset ---
    out_path = OUT_DIR / f"universe_finviz_contract_{ts}.csv"
    df.to_csv(out_path, index=False)

    # --- Profiling reports for you to decide what to drop later ---
    missingness = (df.isna().mean().sort_values(ascending=False) * 100).round(2).to_dict()
    categoricals = profile_categoricals(df, cols=("sector", "industry", "country"), topn=50)

    rep = {
        "asof_utc": utc_now_iso(),
        "input_file": str(in_path),
        "output_file": str(out_path),
        "rows": int(len(df)),
        "cols": int(df.shape[1]),
        "columns": list(df.columns),
        "missingness_pct": missingness,
        "top_categoricals": categoricals,
    }
    rep_path = REPORT_DIR / f"finviz_universe_profile_{ts}.json"
    rep_path.write_text(json.dumps(rep, indent=2), encoding="utf-8")

    print("\n=== Output ===")
    print(f"[OUT]   {out_path}")
    print(f"[REP]   {rep_path}")
    print(f"[INFO]  rows={len(df)} cols={df.shape[1]}")
    print("\n[OK] Done. Standardization only. No rows dropped. No filters applied.")


if __name__ == "__main__":
    main()
