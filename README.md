# AI Professionals Bootcamp - Week 2
---
## Progress Log

### ✅ Day 1: Offline-First ETL Foundations
**Goal:** Build a robust scaffold and idempotent pipeline.
- **Scaffold:** Established `data/`, `src/`, `scripts/`, `reports/`, and `notebooks/`.
- **Centralized Config:** Implemented `config.py` for path management and `io.py` for typed CSV/Parquet operations.
- **Schema Enforcement:** Enforced IDs as strings and numeric fields as nullable (Int64/Float64) via `transforms.py`.
- **Output:** Produced initial `orders.parquet` and `_run_meta.json`.

### ✅ Day 2: Cleaning & Data Quality Checks
**Goal:** Implement "fail-fast" validations and standardize data.
- **Quality Assurance:** Created `quality.py` to assert non-empty datasets and required columns.
- **Standardization:** Normalized categorical fields (e.g., `status` → `status_clean`) and added missingness flags (`amount__isna`).
- **Audit:** Generated `reports/missingness_orders.csv` to track data gaps.
- **Output:** Produced `orders_clean.parquet`.

### 🚀 Day 3: Datetime, Outliers, and Safe Joins
**Goal:** Transform clean data into an "Analytics-Ready" table.
- **Datetime Engineering:** - Safe parsing with `errors="coerce"` and UTC standardization.
    - Extracted temporal features: `month`, `hour`, and `day_of_week`.
- **Outlier Management:** - Implemented IQR bounds detection.
    - Added `is_outlier` flags and applied Winsorization (capping at 1st/99th percentiles) for stable visualizations.
- **Safe Joins:** - Merged `orders` and `users` using `many_to_one` validation to prevent row duplication.
    - Audited match rates to ensure join integrity.
- **Output:** Final consolidated `analytics_table.parquet`.

