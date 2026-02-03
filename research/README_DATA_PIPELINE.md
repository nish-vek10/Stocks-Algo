# ALGO-STOCKS — Data Pipeline SOP  
### Finviz → Clean → Contract → Trade-Ready Universe

This document records the **exact data pipeline design, intent, and operating procedure**  
for the ALGO-STOCKS project.

It is written as **research notes**, not marketing documentation, and is intended to be
useful **years later** when re-running, extending, or auditing the system.

---

## 0. High-Level Philosophy

The pipeline is intentionally split into **independent, replayable stages**.

Key principles:

- Raw data is **never mutated**
- Cleaning ≠ filtering ≠ trading eligibility
- Every stage produces a durable artifact
- Policy decisions (exclusions, thresholds) are **config-driven**
- Scripts are **zero-argument runnable**
- Re-running any stage does not break downstream stages

The result is a **quant-grade, auditable data pipeline**.

---

## 1. Pipeline Overview

| Stage | Script | Purpose |
|-----|------|-------|
| Stage 1 | `01_fetch_finviz_export.py` | Capture raw Finviz export (audit layer) |
| Stage 2 | `02_promote_finviz_raw_to_cleaned.py` | Promote raw → structured cleaned snapshot |
| Stage 3 | `03_clean_finviz_universe.py` | Standardize schema & numeric types (contract) |
| Stage 4 | `04_apply_universe_filters.py` | Apply trading eligibility rules |

Each stage can be run independently.

---

## 2. Stage 1 — Raw Finviz Capture (Audit Layer)

**Script:**
*research/experiments/01_fetch_finviz_export.py*


**Purpose**

- Download the **full Finviz Elite export**
- Preserve *exactly* what Finviz delivers
- Enable replay if Finviz changes schema in the future

**Inputs**

- `.env`
```env
FINVIZ_EXPORT_URL=https://elite.finviz.com/export.ashx?v=111&auth=MY_TOKEN
FINVIZ_EXPORT_TAG=v111_all
HTTP_TIMEOUT=60
```

**Outputs**
```commandline
data/raw/finviz/export_*.csv
data/raw/finviz/export_*.parsed.csv
data/raw/finviz/export_*.meta.json
data/metadata/schema_registry.json
```

**Notes**

- No filtering
- No cleaning
- No row drops 
- Schema fingerprint is recorded 
- This is the source-of-truth layer

*RUN (from project root):*
```cmd
.\.venv\Scripts\python research\experiments\01_fetch_finviz_export.py
```

---

## 3. Stage 2 — Promote Raw → Cleaned Snapshot

**Script:**
*research/experiments/02_promote_finviz_raw_to_cleaned.py*


**Purpose**

- Move the latest raw export into a stable cleaned directory
- Add minimal metadata
- Generate column inspection reports

**Outputs**
```commandline
data/cleaned/universe/universe_finviz_rawpromote_*.csv
research/reports/finviz_columns_*.txt
research/reports/finviz_columns_*.json
```

**Notes**

- Still no filtering
- Still no assumptions 
- Exists for **structure & traceability**
- Makes downstream processing deterministic

*RUN (from project root):*
```cmd
.\.venv\Scripts\python research\experiments/02_promote_finviz_raw_to_cleaned.py
```

---

## 4. Stage 3 — Contract Standardization (Typed Dataset)

**Script:**
*research/experiments/03_clean_finviz_universe.py*


**Purpose**

- Standardise column names 
- Create numeric, typed fields 
- Preserve **all rows** for research flexibility

**Key Derived Columns**

- market_cap_usd — absolute USD value 
- price_num 
- pe_num 
- volume_num 
- change_pct

**Market Cap Interpretation**

Finviz export provides plain numbers representing MILLIONS
(e.g. 38293.66 → 38,293.66M → 38.29366B)

This stage converts everything into absolute USD values safely.

**Outputs**
```root
data/cleaned/universe/universe_finviz_contract_*.csv
research/reports/finviz_universe_profile_*.json
```

**Notes**

- No filters applied 
- No exclusions applied 
- This is the **research contract dataset** 
- All later filters operate on this file

*RUN (from project root):*
```cmd
.\.venv\Scripts\python research\experiments/03_clean_finviz_universe.py
```

---

## 5. Stage 4 — Trade-Ready Universe (Policy Layer)

**Script:**
*research/experiments/04_apply_universe_filters.py*


**Purpose**

- Apply trading eligibility rules 
- Produce a **minimal, production-safe universe**
- Separate research data from trading inputs

**Current Hard Filters**

- country == "USA"
- market_cap >= 300M

**Optional Exclusions (CSV-Driven)**

Controlled via a toggle in the script:

*EXCLUSIONS_ENABLED = True*


**Rules defined in:**

*data/metadata/reit_exclusion.csv*

**Supported Exclusion Rules**: could be copy-pasted in Notepad++

```csv
rule_type,pattern,notes
sector_equals,Healthcare,Drop healthcare
sector_in,Healthcare,Financials,Drop multiple sectors
industry_equals,Asset Management,Exact industry match
industry_contains,REIT,Substring match
ticker_in,AAPL,TSLA,MSFT,Explicit tickers
```

**Market Cap Formatting**

Trade-ready output includes:
- market_cap_usd (numeric)
- market_cap_fmt (pretty string)

**Examples:**
- 1.2345T 
- 38.2937B 
- 287.5400M

*Always formatted to 4 decimal places.*

**Outputs**
```root
data/cleaned/universe/universe_trade_ready_*.csv
research/reports/universe_trade_ready_report_*.json
```

**Notes**

- Audit columns removed
- Clean, minimal schema 
- Safe for backtests and live systems

*RUN (from project root):*
```cmd
.\.venv\Scripts\python research\experiments/04_apply_universe_filters.py
```

---

## 6. Metadata Files

**Sector Mapping (Spiders)**

```commandline
data/metadata/sector_mapping.csv
```

Used to normalise Finviz sector names into canonical “spider” buckets.
```
source_sector,canonical_sector
Consumer Defensive,Consumer Staples
Consumer Cyclical,Consumer Discretionary
...
```

---

**Exclusion Rules**

```commandline
data/metadata/reit_exclusion.csv
```

Used **only** when exclusions are enabled.

Designed so policy can change **without code changes.**

---

## 7. Reproducibility & Audit Notes

- Raw exports are never modified 
- Every stage creates timestamped outputs 
- Reports are saved for historical inspection 
- `.env` is never committed 
- `.env.example` documents required configuration

---

## 8. Future Planned Extensions

- Analyst rating overlays (Yahoo / Zacks)
- Sector “spider” regime detection 
- 2-year OHLCV ingestion per ticker 
- Mean-reversion signal generation 
- Backtesting & execution integration

---
