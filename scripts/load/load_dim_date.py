import pandas as pd
import psycopg2, psycopg2.extras

PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 1000

df = pd.read_parquet("/dataset/transformed/dim_date.parquet")

conn = psycopg2.connect(
    host=PG_HOST,
    database=PG_DB,
    user=PG_USER,
    password=PG_PASS
)
cur = conn.cursor()

cols = [
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
    "is_holiday"
]

rows = df[cols].where(pd.notnull(df), None).values.tolist()

psycopg2.extras.execute_values(
    cur,
    f"""
    INSERT INTO dim_date ({",".join(cols)})
    VALUES %s
    ON CONFLICT (date_key) DO NOTHING
    """,
    rows,
    page_size=BATCH_SIZE
)

conn.commit()
cur.close()
conn.close()

print(f"[SUCCESS] dim_date loaded ({len(rows)} rows)")
