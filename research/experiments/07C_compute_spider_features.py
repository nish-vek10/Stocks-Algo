# research/experiments/07C_compute_spider_features.py
"""
07C :: Compute Spider Features (Indicators)
-------------------------------------------

Reads spider OHLCV series created by 07B and applies canonical indicator pipeline.

IN:
  data/raw/spiders_daily/SECTOR_*.parquet

OUT:
  data/cleaned/spiders_daily/features/SECTOR_*.parquet

Logs:
  data/cleaned/spiders_daily/features/_progress.jsonl
  data/cleaned/spiders_daily/features/_errors.jsonl
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List
from types import SimpleNamespace
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SPIDERS_DIR = ROOT / "data" / "raw" / "spiders_daily"
OUT_DIR = ROOT / "data" / "cleaned" / "spiders_daily" / "features"
PROGRESS_JSONL = OUT_DIR / "_progress.jsonl"
ERRORS_JSONL = OUT_DIR / "_errors.jsonl"

INDICATORS_YAML = ROOT / "config" / "indicators.yaml"

# Optional: smoke mode to run only first N spiders
SMOKE_N = None  # e.g. 2 for quick test; None for all


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
                if j.get("status") == "ok" and j.get("spider_id"):
                    done.add(str(j["spider_id"]))
            except Exception:
                continue
    return done


def normalize_technicals_cfg(raw: dict) -> dict:
    """
    Convert nested YAML under 'technicals' into the flat attribute schema
    expected by features.technicals.pipeline.apply_indicators().

    Input (your YAML):
      {
        "ema_spans": [...],
        "bollinger": {"window": 20, "n_std": 2.0},
        "donchian": {"window": 20},
        "volume": {"avg_window": 10, "surge_mult": 1.15},
        "macd": {"enabled": True, "fast": 12, "slow": 26, "signal": 9},
        "rsi": {"enabled": True, "period": 14},
      }

    Output (flat):
      ema_spans, bb_window, bb_n_std, donchian_window, vol_avg_window, vol_surge_mult,
      macd_enabled, macd_fast, macd_slow, macd_signal,
      rsi_enabled, rsi_period
    """
    t = raw.get("technicals", raw) or {}

    bb = t.get("bollinger", {}) or {}
    dc = t.get("donchian", {}) or {}
    vol = t.get("volume", {}) or {}
    macd = t.get("macd", {}) or {}
    rsi = t.get("rsi", {}) or {}

    flat = {
        # EMA
        "ema_spans": t.get("ema_spans", [10, 20, 50, 100, 200]),

        # Bollinger
        "bb_window": bb.get("window", 20),
        "bb_n_std": bb.get("n_std", 2.0),

        # Donchian (pipeline expects donch_window)
        "donch_window": dc.get("window", 20),

        # Volume
        "vol_avg_window": vol.get("avg_window", 10),
        "vol_surge_mult": vol.get("surge_mult", 1.15),

        # MACD (pipeline expects compute_macd + macd_fast/slow/signal)
        "compute_macd": bool(macd.get("enabled", False)),
        "macd_fast": macd.get("fast", 12),
        "macd_slow": macd.get("slow", 26),
        "macd_signal": macd.get("signal", 9),

        # RSI (pipeline expects compute_rsi + rsi_period)
        "compute_rsi": bool(rsi.get("enabled", False)),
        "rsi_period": rsi.get("period", 14),
    }

    return flat


def load_indicators_cfg(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError("PyYAML not installed. Install with: pip install pyyaml") from e

    if not path.exists():
        raise FileNotFoundError(f"Missing indicators config: {path}")

    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    flat = normalize_technicals_cfg(cfg)
    return dict_to_ns(flat)


def dict_to_ns(x: Any) -> Any:
    """
    Recursively convert dict -> SimpleNamespace so we can access cfg.key as attributes.
    Lists are preserved (with items converted).
    """
    if isinstance(x, dict):
        return SimpleNamespace(**{k: dict_to_ns(v) for k, v in x.items()})
    if isinstance(x, list):
        return [dict_to_ns(v) for v in x]
    return x


def list_spider_ids(spiders_dir: Path) -> List[str]:
    files = sorted(spiders_dir.glob("SECTOR_*.parquet"))
    return [p.stem for p in files]  # stem is spider_id


def main() -> None:
    print("\n=== 07C :: Compute Spider Features (Indicators) ===")
    print(f"[ROOT] {ROOT}")
    print(f"[IN]   spiders_dir={SPIDERS_DIR}")
    print(f"[OUT]  out_dir={OUT_DIR}")
    print(f"[CFG]  indicators={INDICATORS_YAML}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not SPIDERS_DIR.exists():
        raise FileNotFoundError(f"Missing spiders dir: {SPIDERS_DIR}")

    indicators_cfg = load_indicators_cfg(INDICATORS_YAML)
    done = load_done_set(PROGRESS_JSONL)

    spider_ids = list_spider_ids(SPIDERS_DIR)
    if SMOKE_N is not None:
        spider_ids = spider_ids[: int(SMOKE_N)]

    remaining = [sid for sid in spider_ids if sid not in done]

    ok_n = 0
    err_n = 0

    print(f"[RUN] spiders_total={len(spider_ids)} done={len(done)} remaining={len(remaining)}")
    if not remaining:
        print("[OK] Nothing to do.")
        return

    # Import builder
    from features.spiders.build_features import build_spider_features

    for spider_id in remaining:
        src = SPIDERS_DIR / f"{spider_id}.parquet"
        out = OUT_DIR / f"{spider_id}.parquet"

        t0 = datetime.now(timezone.utc)
        try:
            df = build_spider_features(
                spider_parquet=src,
                out_parquet=out,
                indicators_cfg=indicators_cfg,
                trim_last_n_days=None,
            )

            first_date = str(pd.to_datetime(df["date"].iloc[0]).date())
            last_date = str(pd.to_datetime(df["date"].iloc[-1]).date())
            rows = int(len(df))

            append_jsonl(
                PROGRESS_JSONL,
                {
                    "ts": utc_now(),
                    "spider_id": spider_id,
                    "status": "ok",
                    "rows": rows,
                    "first_date": first_date,
                    "last_date": last_date,
                    "out": str(out),
                    "elapsed_s": round((datetime.now(timezone.utc) - t0).total_seconds(), 3),
                },
            )
            print(f"[DONE] {spider_id}: rows={rows} first={first_date} last={last_date}")
            ok_n += 1

        except Exception as e:
            append_jsonl(
                ERRORS_JSONL,
                {"ts": utc_now(), "spider_id": spider_id, "status": "error", "error": repr(e)},
            )
            print(f"[ERROR] {spider_id}: {e}")
            err_n += 1

    print(f"\n[SUMMARY] ok={ok_n} error={err_n}")

    if ok_n == 0 and err_n > 0:
        raise SystemExit("07C failed: zero spiders processed successfully.")

    print("[OK] 07C complete.")


if __name__ == "__main__":
    main()
