import pandas as pd
from datetime import timedelta

sources = [
    "/dataset/transformed/fact_orders.parquet",
    "/dataset/transformed/fact_campaign_transactions.parquet"
]

dates = []

for path in sources:
    try:
        df = pd.read_parquet(path)
        for col in df.columns:
            if col.endswith("_date_key"):
                dates.append(
                    pd.to_datetime(df[col], errors="coerce")
                )
    except FileNotFoundError:
        pass

if not dates:
    raise ValueError("No date keys found in transformed facts")

all_dates = pd.concat(dates).dropna()

min_date = all_dates.min() - pd.DateOffset(years=1)
max_date = all_dates.max() + pd.DateOffset(years=1)

date_range = pd.date_range(start=min_date, end=max_date, freq="D")

df = pd.DataFrame({"date_actual": date_range})

df["date_key"] = df["date_actual"].dt.strftime("%Y-%m-%d")
df["year_actual"] = df["date_actual"].dt.year
df["quarter_actual"] = df["date_actual"].dt.quarter
df["month_actual"] = df["date_actual"].dt.month
df["month_name"] = df["date_actual"].dt.month_name()
df["week_of_year"] = df["date_actual"].dt.isocalendar().week.astype(int)
df["day_name"] = df["date_actual"].dt.day_name()
df["day_of_week"] = df["date_actual"].dt.weekday + 1
df["is_weekend"] = df["day_of_week"].isin([6, 7])
df["is_holiday"] = False  

df = df[
    [
        "date_key",
        "date_actual",
        "year_actual",
        "quarter_actual",
        "month_actual",
        "month_name",
        "week_of_year",
        "day_name",
        "day_of_week",
        "is_weekend",
        "is_holiday",
    ]
]

df.to_parquet("/dataset/transformed/dim_date.parquet", index=False)

print(
    f"[SUCCESS] dim_date generated: {df.shape[0]} rows "
    f"({min_date.date()} → {max_date.date()})"
)
