import pandas as pd

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string"),
        user_id=df["user_id"].astype("string"),
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"),
    )

def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n == 0:
        # empty df -> avoid division by zero
        return pd.DataFrame({"n_missing": df.isna().sum(), "p_missing": 0.0}).sort_values("n_missing", ascending=False)

    return (
        df.isna().sum()
          .rename("n_missing")
          .to_frame()
          .assign(p_missing=lambda t: t["n_missing"] / n)
          .sort_values("p_missing", ascending=False)
    )

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out

def normalize_text(s: pd.Series) -> pd.Series:
    # normalize: strip + lower, keep pandas string dtype
    return s.astype("string").str.strip().str.lower()

def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    # map known values; keep unknown values unchanged
    s2 = s.astype("string")
    mapped = s2.map(mapping)
    return mapped.fillna(s2).astype("string")
