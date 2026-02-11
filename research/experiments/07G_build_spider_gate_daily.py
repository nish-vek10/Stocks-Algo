# research/experiments/07G_build_spider_gate_daily.py
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from filters.spider_gate import (
    SpiderStageStore,
    load_spider_gate_config,
    spider_gate_decision,
    spider_risk_multiplier,
)

OUT_DIR = ROOT / "data" / "cleaned" / "spiders_daily" / "gate"
OUT_PARQUET = OUT_DIR / "spider_gate_daily.parquet"
PROGRESS_JSONL = OUT_DIR / "_progress.jsonl"
ERRORS_JSONL = OUT_DIR / "_errors.jsonl"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main() -> None:
    print("\n=== 07G :: Build Spider Gate Daily Table ===")
    print(f"[ROOT] {ROOT}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_spider_gate_config(ROOT)
    store = SpiderStageStore(ROOT)
    stages_df = store.load().reset_index()  # columns: spider_id, date, stage, stage_name, stage_reason

    if stages_df.empty:
        raise SystemExit("No spider stages loaded.")

    # Build decision row-by-row (only ~10 spiders * ~1000 days => tiny)
    rows = []
    for r in stages_df.itertuples(index=False):
        spider_id = str(r.spider_id)
        date = pd.to_datetime(r.date).normalize()
        stage = int(r.stage)

        d = spider_gate_decision(store, spider_id, date, cfg)
        rows.append({
            "date": date,
            "spider_id": spider_id,
            "sector_stage": stage,
            "sector_stage_name": r.stage_name,
            "allowed": bool(d["allowed"]),
            "reason": d.get("reason"),
            "risk_mult": float(spider_risk_multiplier(stage, cfg)),
        })

    out = pd.DataFrame(rows).sort_values(["date", "spider_id"]).reset_index(drop=True)
    out.to_parquet(OUT_PARQUET, index=False)

    append_jsonl(PROGRESS_JSONL, {
        "ts": utc_now(),
        "status": "ok",
        "rows": int(len(out)),
        "out": str(OUT_PARQUET),
        "spiders_n": int(out["spider_id"].nunique()),
        "dates_n": int(out["date"].nunique()),
    })

    print(f"[OK] wrote {OUT_PARQUET}")
    print(out.tail(5))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        append_jsonl(ERRORS_JSONL, {"ts": utc_now(), "status": "error", "error": repr(e)})
        raise
