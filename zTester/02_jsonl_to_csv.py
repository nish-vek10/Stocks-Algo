# zTester/02_jsonl_to_csv.py

"""
Generic JSON / JSONL → CSV converter

USAGE:
- Set SOURCE_DIR (folder containing the file)
- Set FILE_BASENAME (no extension)

The script will:
- Look for <FILE_BASENAME>.json OR <FILE_BASENAME>.jsonl
- Write <FILE_BASENAME>.csv in the SAME folder
"""

from pathlib import Path
import json
import pandas as pd


# ============================================================
# CONFIG
# ============================================================
ROOT = Path(__file__).resolve().parents[1]  # ALGO-STOCKS root
SOURCE_DIR = ROOT / "data" / "raw" / "prices_daily" / "twelvedata"
FILE_BASENAME = "_errors"

# ============================================================
# SCRIPT
# ============================================================

json_path = SOURCE_DIR / f"{FILE_BASENAME}.json"
jsonl_path = SOURCE_DIR / f"{FILE_BASENAME}.jsonl"
csv_path = SOURCE_DIR / f"{FILE_BASENAME}.csv"

if json_path.exists():
    input_path = json_path
elif jsonl_path.exists():
    input_path = jsonl_path
else:
    raise FileNotFoundError(
        f"No {FILE_BASENAME}.json or {FILE_BASENAME}.jsonl found in {SOURCE_DIR}"
    )

rows = []

with input_path.open("r", encoding="utf-8") as f:
    for i, line in enumerate(f, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as e:
            print(f"[WARN] Line {i} skipped (invalid JSON): {e}")

if not rows:
    raise RuntimeError("No valid JSON rows found — CSV not written.")

df = pd.DataFrame(rows)
df.to_csv(csv_path, index=False)

print(f"[OK] Converted {input_path} → {csv_path}")
print(f"[INFO] Rows: {len(df)}, Columns: {len(df.columns)}")
