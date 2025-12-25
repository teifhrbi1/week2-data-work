# Summary of Findings and Caveats

_Source used: **analytics_table.parquet (joined, analysis-ready)**_

## Key Findings
- **Finding 1 (quantified)**: SA accounts for 100.0% of total revenue with $145.50
- **Finding 2 (quantified)**: N/A
- **Finding 3 (quantified)**: Average order value (AOV) is $36.38, with median $18.75
- **Finding 4 (quantified)**: Refund rate differs by 100.0 percentage points between AE and SA

## Definitions
- **Revenue**: Sum of `amount` over orders (refunds excluded if `status_clean == "refund"` is available)
- **AOV (Average Order Value)**: Mean of `amount`
- **Refund rate**: Proportion of orders where `status_clean == "refund"` (or derived from `status`)
- **Time window**: 2025-12-01 → 2025-12-03 (UTC)
- **Winsorized amount**: Amount capped at 1st and 99th percentiles to reduce outlier impact on visualizations

## Data Quality Caveats

### Missingness
- 20.0% of rows have missing/invalid created_at (coerced to NaT)

### Duplicates
- No duplicate order_id rows detected

### Join Coverage
- country_match_rate = 1.00

### Outliers
- 1 rows above the 99th percentile amount ($97.75) flagged as outliers
- Winsorized amount caps values at p01=$8.13 and p99=$97.75 for cleaner charts

### Other Issues
- Status normalization was applied (lowercasing + mapping refunded→refund) if `status_clean` was not present.

## Next Questions
- How does refund rate vary by month?
- Are there seasonal patterns in order volume or revenue?
- Which segments (country, signup cohort) drive high-value orders?
- What features predict refunds or high AOV?

## Technical Notes
- **Run Metadata**: `data/processed/_run_meta.json`
- **Processed outputs**: data/processed/analytics_table.parquet
- **EDA Notebook**: `notebooks/eda.ipynb` reads from processed only
