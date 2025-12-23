###_______Week2_DAY1_______####

Today’s assignment builds the **Day 1 baseline** for Week 2: a clean repo scaffold + a runnable ETL script that writes **typed, idempotent** processed outputs (Parquet).

## What I built today
- Standard project layout for offline-first data work (`data/`, `src/`, `scripts/`, `reports/`, `notebooks/`)
- Centralized paths via `src/bootcamp_data/config.py` (`make_paths` + `Path` objects)
- Centralized I/O via `src/bootcamp_data/io.py`
  - CSV readers with explicit dtypes (IDs as **string**)
  - Parquet read/write helpers
- Minimal schema enforcement via `src/bootcamp_data/transforms.py` (`enforce_schema`)
  - `amount` parsed to `Float64` and invalid values coerced to missing
  - `quantity` parsed to `Int64` (nullable)
- End-to-end run script: `scripts/run_day1_load.py`
  - Loads raw CSVs → applies schema enforcement → writes processed Parquet
  - Logs evidence (row counts, dtypes, output paths)
  - Writes a small run metadata file: `data/processed/_run_meta.json`
  

# Week 2 — Day 1: Offline-First ETL Foundations

This repo builds the **Day 1 baseline** for Week 2: an offline-first ETL scaffold + a runnable pipeline that produces **typed, idempotent** processed outputs (**Parquet**).

---

## Main requirements (Day 1 checklist)

- Repo scaffold exists: `data/`, `src/`, `scripts/`, `reports/`, `notebooks/`
- Virtual environment created + deps installed: `pandas`, `pyarrow`, `httpx`
- `requirements.txt` frozen
- Sample raw data exists:
  - `data/raw/orders.csv`
  - `data/raw/users.csv`
- Core package implemented under `src/bootcamp_data/`:
  - `config.py` → centralized paths (`Paths` + `make_paths`)
  - `io.py` → typed CSV readers (IDs as **string**) + Parquet read/write helpers
  - `transforms.py` → `enforce_schema()` for minimal schema enforcement
- Run script works end-to-end:
  - `scripts/run_day1_load.py` loads raw → enforces schema → writes Parquet to `data/processed/`
  - logs evidence (row counts, dtypes, output paths)
  - writes run metadata: `data/processed/_run_meta.json`
- Verification passes: can read `orders.parquet` back and confirm dtypes + sample rows
- Git commit pushed to GitHub

---
## How to run (Day 1)
```bash
python scripts/run_day1_load.py

##Checklist##
✅ All directories are created
✅ pyproject.toml has correct dependencies
✅  README.md has required sections
✅ .gitignore excludes .venv and __pycache__
✅ config.py has Paths class and make_paths function
✅ io.py has all 4 functions with correct signatures
✅ transforms.py has enforce_schema function
✅ scripts/run_day1_load.py runs without errors
✅ Raw data files exist in data/raw/
✅ Script creates data/processed/orders.parquet
__________________________________________________________________________


# Week 2 — Day 2: Cleaning + Data Quality Checks

Day 2 builds on the Day 1 baseline by producing a **clean analytics-ready table** + adding **lightweight data quality checks** so bad inputs fail fast.

-------------

## Main requirements (Day 2 checklist)

- A runnable Day 2 script exists: `scripts/run_day2_clean.py`
- Data quality helpers exist under `src/bootcamp_data/`:
  - `quality.py` (example helpers: `require_columns`, `assert_non_empty`)
- Cleaning logic exists under `src/bootcamp_data/`:
  - `cleaning.py` or `transforms_day2.py` (any file name is fine, but it must be centralized)
- The Day 2 pipeline:
  - reads Day 1 processed inputs from `data/processed/`
  - applies cleaning + validation
  - writes **idempotent** cleaned outputs back to `data/processed/`
  - logs evidence (row counts, dtypes, key value counts)
- Outputs must exist after run:
  - `data/processed/orders_clean.parquet` (minimum)
  - optional but recommended: `data/processed/_run_meta_day2.json`
  - optional but recommended: a small quality report under `reports/` (e.g., `reports/day2_quality.md`)

---

## What Day 2 does

Typical Day 2 “analytics-ready” improvements:

- **Validations (fail fast)**
  - Required columns exist (no silent missing columns)
  - Dataset is not empty
  - Optional: unique key checks / null thresholds (if your rubric asks)

- **Cleaning**
  - Standardize categorical fields (e.g., normalize `status` → `status_clean`)
  - Add missingness flags for key metrics (e.g., `amount__isna`, `quantity__isna`)
  - Ensure types stay consistent with Day 1 contract (IDs as string; numeric as nullable)

---

## How to run (Day 2)

```bash
python scripts/run_day2_clean.py

##Checklist##

✅quality.py has all 4 functions
✅transforms.py has all Day 2 functions
✅scripts/run_day2_clean.py runs without errors
✅Creates data/processed/orders_clean.parquet
✅Creates reports/missingness_orders.csv
__________________________________________________________

##Day 3: Datetimes, Outliers, and Joins##
