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

# staff is OPTIONAL — handle safely
df['staff_key'] = None
if 'staff_id' in df.columns:
    cur.execute("SELECT staff_id, staff_key FROM dim_staff")
    staff_map = dict(cur.fetchall())
    df['staff_key'] = df['staff_id'].map(staff_map)

# -------------------------------------------------
# ENSURE NUMERIC FACT MEASURES EXIST
# -------------------------------------------------
for col in ['gross_total', 'discount_total', 'net_total', 'delay_in_days']:
    if col not in df.columns:
        df[col] = None

# -------------------------------------------------
# VALIDATE INTEGER RANGES (PostgreSQL INT limits)
# -------------------------------------------------
INT_MAX = 2147483647
INT_MIN = -2147483648

# Validate and fix integer columns
for col in ['customer_key', 'merchant_key', 'staff_key', 'delay_in_days']:
    if col in df.columns:
        # Convert to numeric, coerce errors to NaN
        df[col] = pd.to_numeric(df[col], errors='coerce')
        # Check for out of range values
        mask = (df[col] > INT_MAX) | (df[col] < INT_MIN)
        if mask.any():
            print(f"[WARNING] Found {mask.sum()} out-of-range values in {col}")
            print(f"[WARNING] Range: {df.loc[mask, col].min()} to {df.loc[mask, col].max()}")
            # Set out of range values to None
            df.loc[mask, col] = None
        # Convert to nullable integer type
        df[col] = df[col].astype('Int64')

# -------------------------------------------------
# LOAD FACT TABLE (NO DEDUPLICATION)
# -------------------------------------------------
cols = [
    'order_id',
    'customer_key',
    'merchant_key',
    'staff_key',
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

# Convert DataFrame to list of tuples, handling pandas NA properly
rows = []
for idx, row in df[cols].iterrows():
    row_data = []
    for col in cols:
        val = row[col]
        # Convert pandas NA to None
        if pd.isna(val):
            row_data.append(None)
        else:
            row_data.append(val)
    rows.append(tuple(row_data))

print(f"[INFO] Inserting {len(rows)} rows into fact_orders")
print(f"[DEBUG] Sample row: {rows[0] if rows else 'No data'}")

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
