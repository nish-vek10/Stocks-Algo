# research/experiments/07D_classify_spider_stages.py
"""
07D :: Classify Spider Stages (Same Stock Stage Logic)
------------------------------------------------------

Reads spider feature parquets created by 07C and applies the SAME stage logic
as individual stocks (9-stage state machine).

IN:
  data/cleaned/spiders_daily/features/SECTOR_*.parquet

OUT:
  data/cleaned/spiders_daily/stages/SECTOR_*.parquet

Logs:
  data/cleaned/spiders_daily/stages/_progress.jsonl
  data/cleaned/spiders_daily/stages/_errors.jsonl
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

IN_DIR = ROOT / "data" / "cleaned" / "spiders_daily" / "features"
OUT_DIR = ROOT / "data" / "cleaned" / "spiders_daily" / "stages"

PROGRESS_JSONL = OUT_DIR / "_progress.jsonl"
ERRORS_JSONL = OUT_DIR / "_errors.jsonl"

STAGES_YAML = ROOT / "config" / "stages.yaml"

SMOKE_N = None  # set to 2 for quick test, else None for all


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


def load_yaml(path: Path) -> dict:
    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError("PyYAML not installed. Install with: pip install pyyaml") from e

    if not path.exists():
        raise FileNotFoundError(f"Missing YAML: {path}")

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def list_spider_ids(in_dir: Path) -> List[str]:
    return [p.stem for p in sorted(in_dir.glob("SECTOR_*.parquet"))]


def _call_stock_stage_classifier(df: pd.DataFrame, stages_cfg: dict) -> pd.DataFrame:
    """
    Calls your EXISTING stock stage logic.

    This function is intentionally defensive: it tries common public entrypoints.
    You should wire ONE canonical function in stages/stage_classifier.py:

      classify_stages(df: pd.DataFrame, cfg: dict) -> pd.DataFrame

    It must return a dataframe with at least:
      - 'stage' (int 1..9) OR 'stage_id' OR 'market_stage'
    """
    from stages import stage_classifier as sc

    # 1) Preferred: a single public function
    if hasattr(sc, "classify_stages"):
        return sc.classify_stages(df=df, cfg=stages_cfg)

    # 2) Other common names (fallbacks)
    for fn_name in ("classify_market_stages", "classify_stage_df", "run_stage_classifier"):
        if hasattr(sc, fn_name):
            fn = getattr(sc, fn_name)
            return fn(df=df, cfg=stages_cfg)

    raise AttributeError(
        "No stage classifier entrypoint found in stages/stage_classifier.py.\n"
        "Add: classify_stages(df: pd.DataFrame, cfg: dict) -> pd.DataFrame\n"
        "so spiders can reuse the SAME logic as stocks."
    )


def main() -> None:
    print("\n=== 07D :: Classify Spider Stages (Same Stock Logic) ===")
    print(f"[ROOT] {ROOT}")
    print(f"[IN]   spiders_features_dir={IN_DIR}")
    print(f"[OUT]  spiders_stages_dir={OUT_DIR}")
    print(f"[CFG]  stages={STAGES_YAML}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not IN_DIR.exists():
        raise FileNotFoundError(f"Missing input dir: {IN_DIR}")

    stages_cfg = load_yaml(STAGES_YAML)
    done = load_done_set(PROGRESS_JSONL)

    spider_ids = list_spider_ids(IN_DIR)
    if SMOKE_N is not None:
        spider_ids = spider_ids[: int(SMOKE_N)]

    remaining = [sid for sid in spider_ids if sid not in done]

    print(f"[RUN] spiders_total={len(spider_ids)} done={len(done)} remaining={len(remaining)}")
    if not remaining:
        print("[OK] Nothing to do.")
        return

    ok_n = 0
    err_n = 0

    for spider_id in remaining:
        src = IN_DIR / f"{spider_id}.parquet"
        out = OUT_DIR / f"{spider_id}.parquet"

        t0 = datetime.now(timezone.utc)
        try:
            df = pd.read_parquet(src)

            # Ensure canonical sort
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"])
                df = df.sort_values("date").reset_index(drop=True)
            else:
                raise KeyError(f"{spider_id} missing 'date' column")

            staged = _call_stock_stage_classifier(df=df, stages_cfg=stages_cfg)

            # Normalise stage column name for downstream use
            stage_col = None
            for c in ("stage", "stage_id", "market_stage"):
                if c in staged.columns:
                    stage_col = c
                    break
            if stage_col is None:
                raise KeyError(
                    f"{spider_id}: stage classifier output missing stage column. "
                    "Expected one of: stage, stage_id, market_stage"
                )

            staged = staged.copy()
            if stage_col != "stage":
                staged["stage"] = staged[stage_col].astype(int)

            staged["spider_id"] = spider_id

            # Keep this lean (but include stage_name for readability/debug)
            keep_cols = [c for c in staged.columns if c in (
                "date", "spider_id", "stage", "stage_name", "stage_reason", "stage_flags"
            )]

            # If stage_name is missing, derive it from STAGE_NAMES mapping
            if "stage_name" not in staged.columns:
                from stages.stage_classifier import STAGE_NAMES
                staged["stage_name"] = staged["stage"].map(STAGE_NAMES).fillna("Unknown")
            if "date" not in keep_cols:
                keep_cols = ["date"] + keep_cols
            if "spider_id" not in keep_cols:
                keep_cols = keep_cols + ["spider_id"]
            if "stage" not in keep_cols:
                keep_cols = keep_cols + ["stage"]

            staged_out = staged[keep_cols].copy()

            # Stable column order (audit-friendly)
            ordered = ["date", "stage", "stage_name", "stage_reason", "spider_id"]
            extras = [c for c in staged_out.columns if c not in ordered]
            staged_out = staged_out[[c for c in ordered if c in staged_out.columns] + extras]

            out.parent.mkdir(parents=True, exist_ok=True)
            staged_out.to_parquet(out, index=False)

            first_date = str(pd.to_datetime(staged_out["date"].iloc[0]).date())
            last_date = str(pd.to_datetime(staged_out["date"].iloc[-1]).date())
            rows = int(len(staged_out))

            append_jsonl(PROGRESS_JSONL, {
                "ts": utc_now(),
                "spider_id": spider_id,
                "status": "ok",
                "rows": rows,
                "first_date": first_date,
                "last_date": last_date,
                "out": str(out),
                "elapsed_s": round((datetime.now(timezone.utc) - t0).total_seconds(), 3),
            })

            print(f"[DONE] {spider_id}: rows={rows} first={first_date} last={last_date}")
            ok_n += 1

        except Exception as e:
            append_jsonl(ERRORS_JSONL, {
                "ts": utc_now(),
                "spider_id": spider_id,
                "status": "error",
                "error": repr(e),
            })
            print(f"[ERROR] {spider_id}: {e}")
            err_n += 1

    print(f"\n[SUMMARY] ok={ok_n} error={err_n}")
    if ok_n == 0 and err_n > 0:
        raise SystemExit("07D failed: zero spiders processed successfully.")
    print("[OK] 07D complete.")


if __name__ == "__main__":
    main()
