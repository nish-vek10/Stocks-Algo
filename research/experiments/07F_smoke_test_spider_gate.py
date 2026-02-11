# research/experiments/07F_smoke_test_spider_gate.py
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from filters.spider_gate import SpiderStageStore, load_spider_gate_config, spider_gate_decision


def main():
    cfg = load_spider_gate_config(ROOT)
    print("\n[CFG]", cfg)

    store = SpiderStageStore(ROOT)

    spider_id = "SECTOR_TECHNOLOGY"
    date = pd.Timestamp("2026-01-30")

    d = spider_gate_decision(store, spider_id, date, cfg)
    print("\n=== Spider Gate Smoke ===")
    print(d)


if __name__ == "__main__":
    main()
