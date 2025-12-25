from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path
    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path


def run_etl(cfg: ETLConfig) -> None:
    cfg.out_orders_clean.parent.mkdir(parents=True, exist_ok=True)

    orders = pd.read_csv(cfg.raw_orders)
    users = pd.read_csv(cfg.raw_users)

    orders.columns = [str(c).strip().lower() for c in orders.columns]
    users.columns = [str(c).strip().lower() for c in users.columns]

    if "status" in orders.columns:
        s = orders["status"].astype(str).str.strip().str.lower()
        orders["status_clean"] = s.replace(
            {
                "refunded": "refund",
                "refund": "refund",
                "returned": "refund",
                "cancelled": "cancel",
                "canceled": "cancel",
            }
        )

    for col in ("amount", "quantity"):
        if col in orders.columns:
            orders[col] = pd.to_numeric(orders[col], errors="coerce")
            orders[f"{col}__isna"] = orders[col].isna()

    orders.to_parquet(cfg.out_orders_clean, index=False)
    users.to_parquet(cfg.out_users, index=False)

    join_key = None
    for k in ("user_id", "customer_id", "userid", "id"):
        if k in orders.columns and k in users.columns:
            join_key = k
            break

    if join_key is not None:
        merged = orders.merge(users, on=join_key, how="left", suffixes=("", "_user"))
    else:
        merged = orders.copy()

    refund_mask = merged["status_clean"].eq("refund") if "status_clean" in merged.columns else pd.Series(False, index=merged.index)

    if "amount" in merged.columns:
        merged["_net_amount"] = pd.to_numeric(merged["amount"], errors="coerce").fillna(0)
        merged.loc[refund_mask, "_net_amount"] = 0
    else:
        merged["_net_amount"] = 0

    if "country" in merged.columns:
        analytics = (
            merged.groupby("country", dropna=False)
            .agg(orders=("country", "size"), revenue=("_net_amount", "sum"))
            .reset_index()
        )
    else:
        analytics = pd.DataFrame(
            [
                {
                    "orders": int(len(merged)),
                    "revenue": float(merged["_net_amount"].sum()),
                }
            ]
        )

    analytics.to_parquet(cfg.out_analytics, index=False)

    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "row_counts": {
            "orders_raw": int(len(orders)),
            "users_raw": int(len(users)),
            "orders_clean": int(len(orders)),
            "users_clean": int(len(users)),
            "analytics_table": int(len(analytics)),
        },
        "missing": {
            "orders_raw": {c: int(orders[c].isna().sum()) for c in orders.columns},
            "users_raw": {c: int(users[c].isna().sum()) for c in users.columns},
        },
    }
    cfg.run_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

def run_etl(cfg: ETLConfig) -> None:
    import json
    from datetime import datetime, timezone

    import pandas as pd

    cfg.out_orders_clean.parent.mkdir(parents=True, exist_ok=True)

    orders = pd.read_csv(cfg.raw_orders)
    users = pd.read_csv(cfg.raw_users)

    orders.columns = [str(c).strip().lower() for c in orders.columns]
    users.columns = [str(c).strip().lower() for c in users.columns]

    if "status" in orders.columns:
        s = orders["status"].astype(str).str.strip().str.lower()
        orders["status_clean"] = (
            s.replace(
                {
                    "refunded": "refund",
                    "refund": "refund",
                    "returned": "refund",
                    "cancelled": "cancel",
                    "canceled": "cancel",
                }
            )
        )

    for col in ("amount", "quantity"):
        if col in orders.columns:
            orders[col] = pd.to_numeric(orders[col], errors="coerce")
            orders[f"{col}__isna"] = orders[col].isna()

    orders.to_parquet(cfg.out_orders_clean, index=False)
    users.to_parquet(cfg.out_users, index=False)

    join_keys = [k for k in ("user_id", "customer_id", "userid", "id") if k in orders.columns and k in users.columns]
    if join_keys:
        key = join_keys[0]
        merged = orders.merge(users, on=key, how="left", suffixes=("", "_user"))
        unique_users = int(orders[key].nunique(dropna=True))
    else:
        merged = orders.copy()
        unique_users = None

    total_orders = int(len(orders))

    if "amount" in orders.columns:
        refund_mask = orders["status_clean"].eq("refund") if "status_clean" in orders.columns else pd.Series(False, index=orders.index)
        total_revenue = float(orders.loc[~refund_mask, "amount"].fillna(0).sum())
        refund_rate = float(refund_mask.mean()) if total_orders else 0.0
    else:
        total_revenue = None
        refund_rate = None

    analytics = pd.DataFrame(
        [
            {
                "total_orders": total_orders,
                "unique_users": unique_users,
                "total_revenue": total_revenue,
                "refund_rate": refund_rate,
            }
        ]
    )
    analytics.to_parquet(cfg.out_analytics, index=False)

    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "row_counts": {
            "orders_raw": int(len(orders)),
            "users_raw": int(len(users)),
            "orders_clean": int(len(orders)),
            "analytics_table": int(len(analytics)),
        },
        "missing": {
            "orders_raw": {c: int(orders[c].isna().sum()) for c in orders.columns},
            "users_raw": {c: int(users[c].isna().sum()) for c in users.columns},
        },
    }
    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
