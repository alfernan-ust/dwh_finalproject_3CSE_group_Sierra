import pandas as pd
import psycopg2, psycopg2.extras

PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 1000

# -------------------------------------------------
# LOAD TRANSFORMED FACT
# -------------------------------------------------
df = pd.read_parquet("/dataset/transformed/fact_orders.parquet")

# ---- HARD GUARDS (fail fast, clear errors)
required_cols = [
    'order_id',
    'user_id',
    'transaction_date_key',
    'is_duplicate',
    'is_incomplete',
    'incomplete_reason'
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise ValueError(f"[ERROR] Missing required columns: {missing}")

# -------------------------------------------------
# DATABASE CONNECTION
# -------------------------------------------------
conn = psycopg2.connect(
    host=PG_HOST,
    database=PG_DB,
    user=PG_USER,
    password=PG_PASS
)
cur = conn.cursor()

# -------------------------------------------------
# INFER CUSTOMER DIMENSION (NATURAL KEY = user_id)
# -------------------------------------------------
cur.execute("SELECT user_id FROM dim_customer")
existing = set(r[0] for r in cur.fetchall())

missing_users = (
    df.loc[~df['user_id'].isin(existing), 'user_id']
      .dropna()
      .unique()
)

if len(missing_users):
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO dim_customer (
            user_id,
            is_duplicate,
            is_inferred,
            is_incomplete,
            incomplete_reason
        ) VALUES %s
        """,
        [(uid, False, True, True, 'Inferred by Fact') for uid in missing_users]
    )

conn.commit()

# -------------------------------------------------
# MAP SURROGATE KEYS
# -------------------------------------------------
cur.execute("SELECT user_id, customer_key FROM dim_customer")
cust_map = dict(cur.fetchall())
df['customer_key'] = df['user_id'].map(cust_map)

# merchant is OPTIONAL — handle safely
df['merchant_key'] = None
if 'merchant_id' in df.columns:
    cur.execute("SELECT merchant_id, merchant_key FROM dim_merchant")
    merch_map = dict(cur.fetchall())
    df['merchant_key'] = df['merchant_id'].map(merch_map)

# -------------------------------------------------
# ENSURE NUMERIC FACT MEASURES EXIST
# -------------------------------------------------
for col in ['gross_total', 'discount_total', 'net_total', 'delay_in_days']:
    if col not in df.columns:
        df[col] = None

# -------------------------------------------------
# LOAD FACT TABLE (NO DEDUPLICATION)
# -------------------------------------------------
cols = [
    'order_id',
    'customer_key',
    'merchant_key',
    'transaction_date_key',
    'estimated_arrival_date_key',
    'delay_in_days',
    'gross_total',
    'discount_total',
    'net_total',
    'is_duplicate',
    'is_incomplete',
    'incomplete_reason'
]

rows = df[cols].where(pd.notnull(df), None).values.tolist()

psycopg2.extras.execute_values(
    cur,
    f"INSERT INTO fact_orders ({','.join(cols)}) VALUES %s",
    rows,
    page_size=BATCH_SIZE
)

conn.commit()
cur.close()
conn.close()

print("[SUCCESS] fact_orders loaded")
