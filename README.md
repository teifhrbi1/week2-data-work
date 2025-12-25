# AI Professionals Bootcamp - Week 2
## Progress Log

### Day 1: Offline-First ETL Foundations
**Goal:** Set up the project and produce the first processed output.
- Created the project folders: `data/`, `src/`, `scripts/`, `reports/`, `notebooks/`.
- Added path utilities and typed IO helpers.
- Standardized types (IDs as strings, numeric fields as nullable).
- Output: processed Parquet + `_run_meta.json`.

### Day 2: Cleaning & Data Quality Checks
**Goal:** Validate inputs and clean the data.
- Added required-column checks and non-empty validations.
- Cleaned categorical values (e.g., `status` → `status_clean`) and added missingness flags (e.g., `amount__isna`).
- Produced missingness summaries for review.
- Output: `orders_clean.parquet`.

### Day 3: Datetime, Outliers, and Safe Joins
**Goal:** Build an analytics-ready table.
- Parsed datetime safely (`errors="coerce"`) and extracted time features (e.g., `month`, `day_of_week`).
- Added outlier flags and applied winsorization for analysis.
- Joined `orders` and `users` with validation and match-rate checks.
- Output: `analytics_table.parquet`.

### Day 4: EDA + Visualizations + Bootstrap
**Goal:** Explore the data and export figures.
- Completed `notebooks/eda.ipynb` using `data/processed/`.
- Built key visuals (trend, distribution, breakdown).
- Used bootstrap sampling to estimate confidence intervals for key metrics.
- Exported figures to `reports/figures/`.
- Output: EDA notebook + PNG figures.

### Day 5: Reporting + Reproducibility Docs
**Goal:** Document results and make the workflow reproducible.
- Wrote `reports/summary.md` (findings, definitions, caveats, next questions).
- Updated README with setup + ETL + outputs + EDA steps.
- Verified the pipeline produces the same outputs when re-run.
- Output: complete documentation.

---