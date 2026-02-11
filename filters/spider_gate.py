from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, Any

import pandas as pd
import yaml


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class SpiderGateConfig:
    enabled: bool = True
    allow_stages: Tuple[int, ...] = (7, 8, 9)
    block_stages: Tuple[int, ...] = (2, 3, 4)
    on_missing: str = "block"  # "block" | "allow"
    min_consecutive_days_in_allow: int = 1
    stage_risk_multiplier: Optional[Dict[str, float]] = None


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing config: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_spider_gate_config(root: Path) -> SpiderGateConfig:
    cfg_path = root / "config" / "spiders.yaml"
    y = _load_yaml(cfg_path)
    sg = (y.get("spider_gate") or {})

    stage_rm = sg.get("stage_risk_multiplier") or {}
    # normalize keys to strings
    stage_rm = {str(k): float(v) for k, v in stage_rm.items()}

    return SpiderGateConfig(
        enabled=bool(sg.get("enabled", True)),
        allow_stages=tuple(int(x) for x in sg.get("allow_stages", [7, 8, 9])),
        block_stages=tuple(int(x) for x in sg.get("block_stages", [2, 3, 4])),
        on_missing=str(sg.get("on_missing", "block")).lower(),
        min_consecutive_days_in_allow=int(sg.get("min_consecutive_days_in_allow", 1)),
        stage_risk_multiplier=stage_rm,
    )


# -----------------------------------------------------------------------------
# Loader / Cache
# -----------------------------------------------------------------------------
class SpiderStageStore:
    """
    Loads spider stages from:
      data/cleaned/spiders_daily/stages/SECTOR_*.parquet
    Exposes fast lookups: (spider_id, date) -> stage (+ meta)
    """

    def __init__(self, root: Path):
        self.root = root
        self.stages_dir = root / "data" / "cleaned" / "spiders_daily" / "stages"
        self._df: Optional[pd.DataFrame] = None

    def load(self) -> pd.DataFrame:
        if self._df is not None:
            return self._df

        if not self.stages_dir.exists():
            raise FileNotFoundError(f"Missing spider stages dir: {self.stages_dir}")

        files = sorted(self.stages_dir.glob("SECTOR_*.parquet"))
        if not files:
            raise FileNotFoundError(f"No spider stage parquets found in: {self.stages_dir}")

        parts = []
        for p in files:
            d = pd.read_parquet(p)
            needed = {"date", "spider_id", "stage"}
            missing = needed - set(d.columns)
            if missing:
                raise KeyError(f"{p.name} missing columns: {sorted(missing)}")

            x = d.copy()
            x["date"] = pd.to_datetime(x["date"]).dt.normalize()
            x["spider_id"] = x["spider_id"].astype(str)
            x["stage"] = x["stage"].astype("int16")
            # keep optional meta if present
            for col in ("stage_name", "stage_reason"):
                if col not in x.columns:
                    x[col] = None
            parts.append(x[["date", "spider_id", "stage", "stage_name", "stage_reason"]])

        df = pd.concat(parts, ignore_index=True)
        # index for fast lookup
        df = df.sort_values(["spider_id", "date"])
        df = df.set_index(["spider_id", "date"])
        self._df = df
        return df

    def get_stage_row(self, spider_id: str, date: pd.Timestamp) -> Optional[dict]:
        df = self.load()
        key = (str(spider_id), pd.to_datetime(date).normalize())
        if key not in df.index:
            return None
        row = df.loc[key]
        # row can be Series
        return {
            "spider_id": key[0],
            "date": key[1],
            "stage": int(row["stage"]),
            "stage_name": row.get("stage_name"),
            "stage_reason": row.get("stage_reason"),
        }


# -----------------------------------------------------------------------------
# Gate Logic
# -----------------------------------------------------------------------------
def _is_allowed_by_stage(stage: int, cfg: SpiderGateConfig) -> bool:
    if stage in cfg.block_stages:
        return False
    if stage in cfg.allow_stages:
        return True
    # anything else defaults to block (conservative)
    return False


def _consecutive_allow_ok(
    store: SpiderStageStore,
    spider_id: str,
    date: pd.Timestamp,
    cfg: SpiderGateConfig,
) -> bool:
    n = max(1, int(cfg.min_consecutive_days_in_allow))
    if n == 1:
        return True

    # Require the spider to be in allow stages for N consecutive trading days ending at `date`.
    # We approximate with calendar-day stepping and presence checks (robust enough for daily data).
    # If you want strict "trading day" stepping, we can later use the spider's own date index.
    cur = pd.to_datetime(date).normalize()
    ok = 0
    checked = 0
    # loop back up to 10*n days to survive weekends/holidays
    for _ in range(10 * n):
        row = store.get_stage_row(spider_id, cur)
        if row is not None:
            checked += 1
            if int(row["stage"]) in cfg.allow_stages and int(row["stage"]) not in cfg.block_stages:
                ok += 1
            else:
                ok = 0
            if ok >= n:
                return True
        cur = cur - pd.Timedelta(days=1)
    # If we couldn't find enough datapoints, treat as missing
    return False


def spider_gate_decision(
    store: SpiderStageStore,
    spider_id: str,
    date: pd.Timestamp,
    cfg: SpiderGateConfig,
) -> dict:
    """
    Returns a structured decision dict (audit-friendly).
    """
    if not cfg.enabled:
        return {"allowed": True, "reason": "gate_disabled"}

    row = store.get_stage_row(spider_id, date)
    if row is None:
        allowed = (cfg.on_missing == "allow")
        return {
            "allowed": allowed,
            "reason": "missing_spider_stage",
            "spider_id": spider_id,
            "date": str(pd.to_datetime(date).date()),
        }

    stage = int(row["stage"])
    allowed = _is_allowed_by_stage(stage, cfg)
    if allowed and cfg.min_consecutive_days_in_allow > 1:
        allowed = _consecutive_allow_ok(store, spider_id, date, cfg)
        if not allowed:
            return {
                "allowed": False,
                "reason": "not_enough_consecutive_allow_days",
                "spider_id": spider_id,
                "date": str(pd.to_datetime(date).date()),
                "stage": stage,
            }

    return {
        "allowed": bool(allowed),
        "reason": "stage_allowed" if allowed else "stage_blocked",
        "spider_id": spider_id,
        "date": str(pd.to_datetime(date).date()),
        "stage": stage,
        "stage_name": row.get("stage_name"),
    }


def spider_risk_multiplier(stage: Optional[int], cfg: SpiderGateConfig) -> float:
    rm = cfg.stage_risk_multiplier or {}
    if stage is None:
        return float(rm.get("default", 1.0))
    return float(rm.get(str(int(stage)), rm.get("default", 1.0)))


# Convenience one-liner for later engine use
def is_spider_allowed(root: Path, spider_id: str, date: pd.Timestamp) -> bool:
    cfg = load_spider_gate_config(root)
    store = SpiderStageStore(root)
    d = spider_gate_decision(store, spider_id, date, cfg)
    return bool(d["allowed"])
