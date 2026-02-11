# Path: zTester/03_spider_treemap.py
"""
Spider Treemap Visualiser (Sector-only)

Creates:
- Interactive HTML treemap with hover tooltips (recommended for presenting)
- Optional PNG export (for reports; requires kaleido)

Inputs:
- data/metadata/spiders/spider_memberships.csv  (required)
- data/metadata/spiders/spider_summary.csv      (optional)

Notes:
- Sector "headers" are parent labels (not extra tiles).
- RGB color scale is fixed to 0%..35% for stronger transitions.
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd

# -------------------------
# CONFIG (EDIT THESE ONLY)
# -------------------------
ROOT = Path(__file__).resolve().parents[1]  # zTester/.. -> project root
MEMBERSHIPS_CSV = ROOT / "data" / "metadata" / "spiders" / "spider_memberships.csv"
SUMMARY_CSV     = ROOT / "data" / "metadata" / "spiders" / "spider_summary.csv"  # optional

OUT_DIR  = ROOT / "zTester" / "output"
OUT_HTML = OUT_DIR / "spider_treemap.html"
OUT_PNG  = OUT_DIR / "spider_treemap.png"   # optional export

# Show only top N tickers per sector (None = all)
TOP_N_PER_SECTOR = None

# Show extra inside-tile text only when weight >= threshold
MIN_WEIGHT_TEXT     = 0.01   # 1%
MIN_WEIGHT_TEXT_BIG = 0.05   # 5%

# Star tickers above this weight
STAR_WEIGHT = 0.07  # 7%

# COLOR SCALE: force 0%..35%
WEIGHT_MAX = 0.35

# -------------------------
# THEME / STYLE (DARK)
# -------------------------
TITLE_TEXT = "ALGO-STOCKS — Sector Spiders (Market Cap Weighted)"

PAPER_BG = "#0b0f19"
PLOT_BG  = "#0b0f19"

FONT_FAMILY = "Inter, Segoe UI, Arial"
TEXT_COLOR  = "#000000"  # <- black text (your request)

# Borders
TILE_BORDER_COLOR = "#f4d35e"
TILE_BORDER_WIDTH = 1.6

# RGB continuous scale: Blue -> Green -> Red
RGB_SCALE = [
    (0.00, "rgb(0, 120, 255)"),   # blue
    (0.50, "rgb(0, 220, 120)"),   # green
    (1.00, "rgb(255, 70, 70)"),   # red
]

# -------------------------
# Helpers
# -------------------------
def fmt_mcap(x: float) -> str:
    """Format market cap into M/B/T with 2dp."""
    try:
        v = float(x)
    except Exception:
        return "NA"
    abs_v = abs(v)
    if abs_v >= 1e12:
        return f"{v/1e12:.2f}T"
    if abs_v >= 1e9:
        return f"{v/1e9:.2f}B"
    if abs_v >= 1e6:
        return f"{v/1e6:.2f}M"
    return f"{v:.0f}"

def fmt_pct(x: float) -> str:
    try:
        return f"{float(x)*100:.2f}%"
    except Exception:
        return "NA"

def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not MEMBERSHIPS_CSV.exists():
        raise FileNotFoundError(f"Missing memberships CSV: {MEMBERSHIPS_CSV}")

    df = pd.read_csv(MEMBERSHIPS_CSV)

    required = {"spider_id", "sector", "ticker", "market_cap_usd", "weight"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Missing columns in memberships CSV: {sorted(missing)}")

    # Optional: order sectors by total mcap (nice layout ordering)
    if SUMMARY_CSV.exists():
        s = pd.read_csv(SUMMARY_CSV)
        if {"spider_id", "mcap_sum_usd"}.issubset(set(s.columns)):
            spider_order = (
                s.sort_values("mcap_sum_usd", ascending=False)["spider_id"].astype(str).tolist()
            )
            df["spider_id"] = pd.Categorical(df["spider_id"].astype(str), categories=spider_order, ordered=True)
            df = df.sort_values(["spider_id", "weight"], ascending=[True, False])
        else:
            df = df.sort_values(["spider_id", "weight"], ascending=[True, False])
    else:
        df = df.sort_values(["spider_id", "weight"], ascending=[True, False])

    if TOP_N_PER_SECTOR is not None:
        df = df.groupby("spider_id", group_keys=False).head(int(TOP_N_PER_SECTOR))

    # Format columns
    df["mcap_fmt"] = df["market_cap_usd"].apply(fmt_mcap)
    df["w_fmt"] = df["weight"].apply(fmt_pct)

    # Star tickers >= 7%
    w = df["weight"].fillna(0.0)
    df["ticker_label"] = df["ticker"].astype(str)
    df.loc[w >= float(STAR_WEIGHT), "ticker_label"] = df.loc[w >= float(STAR_WEIGHT), "ticker_label"] + " ★"

    # Sector header text (as a PARENT label, not a tile)
    sector_stats = (
        df.groupby("sector", as_index=False)
          .agg(members=("ticker", "nunique"), mcap_sum=("market_cap_usd", "sum"))
    )
    sector_stats["mcap_sum_fmt"] = sector_stats["mcap_sum"].apply(fmt_mcap)
    sector_to_header = dict(
        zip(
            sector_stats["sector"].astype(str),
            (
                sector_stats["sector"].astype(str).str.upper()
                + "<br><span style='font-size:11px'>Members "
                + sector_stats["members"].astype(int).astype(str)
                + " • MCap "
                + sector_stats["mcap_sum_fmt"].astype(str)
                + "</span>"
            ),
        )
    )
    df["sector_header"] = df["sector"].astype(str).map(sector_to_header)

    # Hover tooltip (for ALL ticker tiles)
    df["hover"] = (
        "<b>" + df["ticker"].astype(str) + "</b>"
        + "<br>Sector: " + df["sector"].astype(str)
        + "<br>Market Cap: " + df["mcap_fmt"].astype(str)
        + "<br>Weight: " + df["w_fmt"].astype(str)
    )

    # Inside-tile text: keep minimal for small tiles
    df["box_text"] = ""
    mask_med = w >= float(MIN_WEIGHT_TEXT)
    df.loc[mask_med, "box_text"] = "W " + df.loc[mask_med, "w_fmt"].astype(str)

    mask_big = w >= float(MIN_WEIGHT_TEXT_BIG)
    df.loc[mask_big, "box_text"] = (
        "MCap " + df.loc[mask_big, "mcap_fmt"].astype(str)
        + "<br>W " + df.loc[mask_big, "w_fmt"].astype(str)
    )

    # Plotly
    try:
        import plotly.express as px
    except Exception as e:
        raise RuntimeError("Plotly not installed. Install with: pip install plotly") from e

    fig = px.treemap(
        df,
        path=["sector_header", "ticker_label"],   # sector is a parent header, ticker is leaf
        values="market_cap_usd",
        color="weight",
        color_continuous_scale=RGB_SCALE,
        range_color=(0.0, float(WEIGHT_MAX)),     # <-- force 0..35%
        custom_data=["hover", "box_text"],
        maxdepth=2,
        branchvalues="total",
    )

    # IMPORTANT:
    # - Use black text
    # - Make it "feel" bold by using slightly larger default font + no uniformtext lock
    fig.update_traces(
        hovertemplate="%{customdata[0]}<extra></extra>",
        texttemplate="<b>%{label}</b><br><b>%{customdata[1]}</b>",
        textinfo="text",
        marker=dict(
            line=dict(color=TILE_BORDER_COLOR, width=TILE_BORDER_WIDTH),
            pad=dict(t=3, l=3, r=3, b=3),
        ),
        tiling=dict(packing="squarify"),
        insidetextfont=dict(color=TEXT_COLOR, family=FONT_FAMILY, size=13),
        textfont=dict(color=TEXT_COLOR, family=FONT_FAMILY, size=13),
    )

    fig.update_layout(
        title=dict(
            text=TITLE_TEXT,
            x=0.02,
            xanchor="left",
            font=dict(size=22, family=FONT_FAMILY, color="#e6edf7"),
        ),
        template="plotly_dark",
        paper_bgcolor=PAPER_BG,
        plot_bgcolor=PLOT_BG,
        font=dict(family=FONT_FAMILY, color="#e6edf7"),
        margin=dict(t=70, l=8, r=55, b=8),

        coloraxis_colorbar=dict(
            title="Weight",
            tickformat=".0%",
            outlinewidth=0,
            len=0.85,
        ),

        hoverlabel=dict(
            bgcolor="#0f172a",
            font=dict(color="#ffffff", family=FONT_FAMILY, size=13),
            bordercolor=TILE_BORDER_COLOR,
        ),
    )

    fig.write_html(str(OUT_HTML), include_plotlyjs="cdn")
    print(f"[OK] Wrote interactive treemap: {OUT_HTML}")

    try:
        fig.write_image(str(OUT_PNG), scale=2)
        print(f"[OK] Wrote PNG: {OUT_PNG}")
    except Exception:
        print("[INFO] PNG export skipped (install kaleido to enable): pip install -U kaleido")

if __name__ == "__main__":
    main()
