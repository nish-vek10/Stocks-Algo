# research/experiments/08B_classify_stock_stages.py
"""
08B :: Classify Stock Daily Stages
----------------------------------

Goal:
- For each ticker features parquet in:
    data/cleaned/stocks_daily/features/*.parquet
  run the canonical stage classifier and write:
    data/cleaned/stocks_daily/stages/{TICKER}.parquet

Inputs:
- Stock features parquets:
    data/cleaned/stocks_daily/features/{TICKER}.parquet

- Stage config:
    config/stages.yaml

Classifier (canonical):
- stages/stage_classifier.py :: classify_stages(df, cfg)

Outputs:
- data/cleaned/stocks_daily/stages/{TICKER}.parquet

Logs:
- data/cleaned/stocks_daily/stages/_progress.jsonl
- data/cleaned/stocks_daily/stages/_errors.jsonl
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

IN_DIR = ROOT / "data" / "cleaned" / "stocks_daily" / "features"
OUT_DIR = ROOT / "data" / "cleaned" / "stocks_daily" / "stages"
PROGRESS_JSONL = OUT_DIR / "_progress.jsonl"
ERRORS_JSONL = OUT_DIR / "_errors.jsonl"

STAGES_YAML = ROOT / "config" / "stages.yaml"

# -----------------------------------------------------------------------------
# Smoke mode
# -----------------------------------------------------------------------------
SMOKE_N: Optional[int] = None        # e.g. 5 or None for all
SMOKE_TICKERS: Optional[str] = None  # e.g. "AAPL,MSFT,NVDA" or None for all

# -----------------------------------------------------------------------------
# Canonical classifier
# -----------------------------------------------------------------------------
from stages.stage_classifier import classify_stages  # noqa: E402


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing YAML: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


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


def list_feature_files(in_dir: Path) -> List[Path]:
    if not in_dir.exists():
        raise FileNotFoundError(f"Missing IN_DIR: {in_dir}")
    files = sorted(in_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No feature parquets found in: {in_dir}")
    return files


def read_features(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    # accept either a 'date' column or datetime index
    if "date" in df.columns:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        return df
    if isinstance(df.index, pd.DatetimeIndex):
        df = df.copy().sort_index()
        df = df.reset_index().rename(columns={"index": "date"})
        df["date"] = pd.to_datetime(df["date"])
        return df
    raise KeyError(f"{path.name}: needs 'date' column or DatetimeIndex")


def sanity_check_min_cols(df: pd.DataFrame, ticker: str) -> None:
    # classifier typically needs close/high/low/volume + key EMAs / Donchian / BB
    required = {"date", "close", "high", "low", "volume", "ema200"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"{ticker}: features missing required columns: {sorted(missing)}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    print("\n=== 08B :: Classify Stock Stages ===")
    print(f"[ROOT] {ROOT}")
    print(f"[IN ] {IN_DIR}")
    print(f"[OUT] {OUT_DIR}")
    print(f"[CFG] {STAGES_YAML}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_yaml(STAGES_YAML)

    done = load_done_set(PROGRESS_JSONL)

    files = list_feature_files(IN_DIR)

    # Smoke selection
    if SMOKE_TICKERS:
        want = {t.strip().upper() for t in SMOKE_TICKERS.split(",") if t.strip()}
        files = [p for p in files if p.stem.upper() in want]
    if SMOKE_N is not None:
        files = files[: int(SMOKE_N)]

    remaining = [p for p in files if p.stem not in done]
    print(f"[RUN] total={len(files)} done={len(done)} remaining={len(remaining)}")

    ok_n = 0
    err_n = 0

    for p in files:
        ticker = p.stem
        if ticker in done:
            continue

        t0 = datetime.now(timezone.utc)
        try:
            df = read_features(p)
            sanity_check_min_cols(df, ticker)

            out = classify_stages(df=df, cfg=cfg)

            # Ensure expected outputs exist
            if "stage" not in out.columns:
                raise RuntimeError(f"{ticker}: classifier output missing 'stage'")
            if "stage_name" not in out.columns:
                # allow older classifier versions, but keep it explicit
                out["stage_name"] = None
            if "stage_reason" not in out.columns:
                out["stage_reason"] = None

            out = out.sort_values("date").reset_index(drop=True)

            out_path = OUT_DIR / f"{ticker}.parquet"
            out.to_parquet(out_path, index=False)

            # Quick metrics for audit
            stage_counts = out["stage"].value_counts().sort_index()
            stages_present = sorted(int(x) for x in stage_counts.index.tolist())

            append_jsonl(PROGRESS_JSONL, {
                "ts": utc_now(),
                "status": "ok",
                "ticker": ticker,
                "rows": int(len(out)),
                "first_date": str(pd.to_datetime(out["date"].iloc[0]).date()),
                "last_date": str(pd.to_datetime(out["date"].iloc[-1]).date()),
                "stages_present": stages_present,
                "out": str(out_path),
                "elapsed_s": round((datetime.now(timezone.utc) - t0).total_seconds(), 3),
            })

            ok_n += 1
            if ok_n % 100 == 0:
                print(f"[OK] processed={ok_n} last={ticker}")

        except Exception as e:
            append_jsonl(ERRORS_JSONL, {
                "ts": utc_now(),
                "status": "error",
                "ticker": ticker,
                "file": str(p),
                "error": repr(e),
            })
            print(f"[ERROR] {ticker}: {e}")
            err_n += 1

    print(f"\n[SUMMARY] ok={ok_n} error={err_n}")
    if ok_n == 0 and err_n > 0:
        raise SystemExit("08B failed: zero tickers processed successfully.")
    print("[OK] 08B complete.")


if __name__ == "__main__":
    main()
