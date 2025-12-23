import pandas as pd

def require_columns(df: pd.DataFrame, required: list[str]) -> None:
    """Fail fast if required columns are missing."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

def assert_non_empty(df: pd.DataFrame, name: str = "df") -> None:
    """Fail fast if a DataFrame is empty."""
    if df is None or len(df) == 0:
        raise ValueError(f"{name} is empty (0 rows).")
