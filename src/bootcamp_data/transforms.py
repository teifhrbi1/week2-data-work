import pandas as pd

def enforce_schema(df):
    df["order_id"] = df["order_id"].astype("string")
    df["user_id"] = df["user_id"].astype("string")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    return df

def parse_datetime(df, col="created_at"):
    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df

def add_time_parts(df, col="created_at"):
    df["year"] = df[col].dt.year
    df["month"] = df[col].dt.month
    df["day"] = df[col].dt.day
    df["hour"] = df[col].dt.hour
    df["weekday"] = df[col].dt.weekday
    df["date_only"] = df[col].dt.date
    return df

def iqr_bounds(s, k=1.5):
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - k * iqr
    upper_bound = q3 + k * iqr
    return lower_bound, upper_bound

def winsorize(s, lo=0.01, hi=0.99):
    lower_limit = s.quantile(lo)
    upper_limit = s.quantile(hi)
    return s.clip(lower=lower_limit, upper=upper_limit)

def add_outlier_flag(df, col, k=1.5):
    low, high = iqr_bounds(df[col], k=k)
    new_col_name = col + "__is_outlier"
    df[new_col_name] = (df[col] < low) | (df[col] > high)
    return df
