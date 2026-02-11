import pandas as pd
from pathlib import Path

root = Path("data/cleaned/spiders_daily/stages")
for p in sorted(root.glob("SECTOR_*.parquet")):
    df = pd.read_parquet(p)
    vc = df["stage"].value_counts().sort_index()
    print(p.stem, dict(vc))
