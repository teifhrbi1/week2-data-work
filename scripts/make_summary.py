from __future__ import annotations

from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
REPORTS = ROOT / "reports"
OUT_MD = REPORTS / "summary.md"


def _fmt_money(x: float) -> str:
    try:
        return f"${x:,.2f}"
    except Exception:
        return "N/A"


def _normalize_status(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure status_clean exists if possible."""
    out = df.copy()
    if "status_clean" in out.columns:
        return out
    if "status" in out.columns:
        s = out["status"].astype("string").str.strip().str.lower()
        s = s.replace({"refunded": "refund"})
        out["status_clean"] = s
    return out


def _load_run_meta() -> dict:
    meta_path = PROCESSED / "_run_meta.json"
    if meta_path.exists():
        return json.loads(meta_path.read_text(encoding="utf-8"))
    return {}


def main() -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)

    meta = _load_run_meta()

    # Prefer analytics table if present (already joined)
    analytics_path = PROCESSED / "analytics_table.parquet"
    orders_clean_path = PROCESSED / "orders_clean.parquet"
    orders_path = PROCESSED / "orders.parquet"
    users_path = PROCESSED / "users.parquet"

    if analytics_path.exists():
        df = pd.read_parquet(analytics_path)
        source_note = "analytics_table.parquet (joined, analysis-ready)"
    else:
        # Fallback: merge orders + users, but enforce user_id types first
        if orders_clean_path.exists():
            orders = pd.read_parquet(orders_clean_path)
            source_note = "orders_clean.parquet + users.parquet (merged)"
        elif orders_path.exists():
            orders = pd.read_parquet(orders_path)
            source_note = "orders.parquet + users.parquet (merged)"
        else:
            raise FileNotFoundError("Missing processed orders file (orders_clean.parquet or orders.parquet).")

        if not users_path.exists():
            raise FileNotFoundError("Missing processed users.parquet.")
        users = pd.read_parquet(users_path)

        # Force merge key dtype to string on both sides
        if "user_id" in orders.columns:
            orders["user_id"] = orders["user_id"].astype("string")
        if "user_id" in users.columns:
            users["user_id"] = users["user_id"].astype("string")

        orders = _normalize_status(orders)
        df = orders.merge(users, on="user_id", how="left", suffixes=("", "_user"))

    df = _normalize_status(df)

    # Parse created_at safely (if exists)
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)

    # Ensure amount numeric (if exists)
    if "amount" in df.columns and not pd.api.types.is_numeric_dtype(df["amount"]):
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Refund filtering
    non_refund = df
    if "status_clean" in df.columns:
        non_refund = df[df["status_clean"] != "refund"].copy()

    # Time window
    time_window = "N/A"
    if "created_at" in df.columns:
        dt_min = df["created_at"].min()
        dt_max = df["created_at"].max()
        if pd.notna(dt_min) and pd.notna(dt_max):
            time_window = f"{dt_min.date().isoformat()} → {dt_max.date().isoformat()} (UTC)"

    # Core metrics
    total_revenue = float(non_refund["amount"].sum(skipna=True)) if "amount" in non_refund.columns else float("nan")
    aov_mean = float(non_refund["amount"].mean(skipna=True)) if "amount" in non_refund.columns else float("nan")
    aov_median = float(non_refund["amount"].median(skipna=True)) if "amount" in non_refund.columns else float("nan")

    # Revenue by country
    top_country_line = "N/A"
    if "country" in non_refund.columns and "amount" in non_refund.columns and pd.notna(total_revenue) and total_revenue != 0:
        rev_by_country = non_refund.groupby("country", dropna=False)["amount"].sum().sort_values(ascending=False)
        if len(rev_by_country) > 0:
            top_country = rev_by_country.index[0]
            top_rev = float(rev_by_country.iloc[0])
            share = (top_rev / total_revenue) * 100
            top_country_line = f"{top_country} accounts for {share:.1f}% of total revenue with {_fmt_money(top_rev)}"

    # Monthly revenue trend
    monthly_growth_line = "N/A"
    if "created_at" in non_refund.columns and "amount" in non_refund.columns:
        tmp = non_refund.dropna(subset=["created_at"]).copy()
        if len(tmp) > 0:
            tmp["month"] = tmp["created_at"].dt.to_period("M").astype(str)
            rev_by_month = tmp.groupby("month")["amount"].sum().sort_index()
            if len(rev_by_month) >= 2:
                m1, m2 = rev_by_month.index[-2], rev_by_month.index[-1]
                v1, v2 = float(rev_by_month.iloc[-2]), float(rev_by_month.iloc[-1])
                if v1 != 0:
                    pct = ((v2 - v1) / v1) * 100
                    monthly_growth_line = f"Monthly revenue changed by {pct:+.1f}% from {m1} to {m2}"
                else:
                    monthly_growth_line = f"Monthly revenue moved from {_fmt_money(v1)} in {m1} to {_fmt_money(v2)} in {m2}"

    # Refund rate
    refund_rate_line = "N/A"
    refund_country_line = "N/A"
    if "status_clean" in df.columns:
        total_orders = len(df)
        refund_orders = int((df["status_clean"] == "refund").sum())
        if total_orders > 0:
            refund_rate = (refund_orders / total_orders) * 100
            refund_rate_line = f"Overall refund rate is {refund_rate:.1f}% ({refund_orders}/{total_orders})"

        if "country" in df.columns:
            joined = df[df["country"].notna()].copy()
            if len(joined) > 0:
                grp = joined.groupby("country")["status_clean"].apply(lambda s: (s == "refund").mean() * 100).sort_values(ascending=False)
                if len(grp) >= 2:
                    c_hi, c_lo = grp.index[0], grp.index[-1]
                    diff = float(grp.iloc[0] - grp.iloc[-1])
                    refund_country_line = f"Refund rate differs by {diff:.1f} percentage points between {c_hi} and {c_lo}"

    # Data quality caveats from meta if present
    missing_created_at_line = "N/A"
    if "created_at" in df.columns:
        miss = float(df["created_at"].isna().mean() * 100)
        missing_created_at_line = f"{miss:.1f}% of rows have missing/invalid created_at (coerced to NaT)"
    elif meta.get("missing_timestamps", {}).get("analytics.created_at_missing") is not None:
        missing_created_at_line = f"analytics.created_at_missing = {meta['missing_timestamps']['analytics.created_at_missing']}"

    join_coverage_line = "N/A"
    if meta.get("join_match_rate", {}).get("country_match_rate") is not None:
        join_coverage_line = f"country_match_rate = {float(meta['join_match_rate']['country_match_rate']):.2f}"
    elif "country" in df.columns:
        matched = int(df["country"].notna().sum())
        total = len(df)
        join_coverage_line = f"{(matched/total*100 if total else 0.0):.1f}% country non-null after join"

    # Duplicates (if order_id exists)
    duplicates_line = "N/A"
    if "order_id" in df.columns:
        dup_n = int(df["order_id"].duplicated().sum())
        duplicates_line = "No duplicate order_id rows detected" if dup_n == 0 else f"Found {dup_n} duplicate order_id rows"

    # Outliers + winsorization
    outliers_line = "N/A"
    winsor_line = "N/A"
    if "amount" in non_refund.columns:
        s = non_refund["amount"].dropna()
        if len(s) > 0:
            p01 = float(s.quantile(0.01))
            p99 = float(s.quantile(0.99))
            out_n = int((s > p99).sum())
            outliers_line = f"{out_n} rows above the 99th percentile amount ({_fmt_money(p99)}) flagged as outliers"
            winsor_line = f"Winsorized amount caps values at p01={_fmt_money(p01)} and p99={_fmt_money(p99)} for cleaner charts"

    md = f"""# Summary of Findings and Caveats

_Source used: **{source_note}**_

## Key Findings
- **Finding 1 (quantified)**: {top_country_line}
- **Finding 2 (quantified)**: {monthly_growth_line}
- **Finding 3 (quantified)**: Average order value (AOV) is {_fmt_money(aov_mean)}, with median {_fmt_money(aov_median)}
- **Finding 4 (quantified)**: {refund_country_line if refund_country_line != "N/A" else refund_rate_line}

## Definitions
- **Revenue**: Sum of `amount` over orders (refunds excluded if `status_clean == "refund"` is available)
- **AOV (Average Order Value)**: Mean of `amount`
- **Refund rate**: Proportion of orders where `status_clean == "refund"` (or derived from `status`)
- **Time window**: {time_window}
- **Winsorized amount**: Amount capped at 1st and 99th percentiles to reduce outlier impact on visualizations

## Data Quality Caveats

### Missingness
- {missing_created_at_line}

### Duplicates
- {duplicates_line}

### Join Coverage
- {join_coverage_line}

### Outliers
- {outliers_line}
- {winsor_line}

### Other Issues
- Status normalization was applied (lowercasing + mapping refunded→refund) if `status_clean` was not present.

## Next Questions
- How does refund rate vary by month?
- Are there seasonal patterns in order volume or revenue?
- Which segments (country, signup cohort) drive high-value orders?
- What features predict refunds or high AOV?

## Technical Notes
- **Run Metadata**: `data/processed/_run_meta.json`
- **Processed outputs**: {meta.get("paths", {}).get("analytics", "data/processed/analytics_table.parquet")}
- **EDA Notebook**: `notebooks/eda.ipynb` reads from processed only
"""
    OUT_MD.write_text(md, encoding="utf-8")
    print(f"✅ wrote: {OUT_MD}")


if __name__ == "__main__":
    main()
