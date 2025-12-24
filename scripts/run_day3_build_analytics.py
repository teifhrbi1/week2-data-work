import sys
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.transforms import parse_datetime, add_time_parts, winsorize, add_outlier_flag
from bootcamp_data.joins import safe_left_join

def main():
    orders = pd.read_parquet("data/processed/orders_clean.parquet")
    users = pd.read_parquet("data/processed/users.parquet")

    orders = parse_datetime(orders, col="created_at")
    orders = add_time_parts(orders, col="created_at")

    analytics_df = safe_left_join(
        orders, 
        users, 
        on="user_id", 
        validate="many_to_one",
        suffixes=("", "_user")
    )

    analytics_df["amount_winsor"] = winsorize(analytics_df["amount"])
    analytics_df = add_outlier_flag(analytics_df, "amount")

    output_path = "data/processed/analytics_table.parquet"
    analytics_df.to_parquet(output_path, index=False)
    
    summary = (
        analytics_df.groupby("country", dropna=False)
        .agg(
            order_count=("order_id", "size"),
            total_revenue=("amount", "sum")
        )
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    print(summary.to_string(index=False))

    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    summary.to_csv(reports_dir / "revenue_by_country.csv", index=False)

if __name__ == "__main__":
    main()
