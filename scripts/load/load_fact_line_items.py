import pandas as pd
import psycopg2, psycopg2.extras

PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 1000

df = pd.read_parquet("/dataset/transformed/fact_line_items.parquet")

# Handle empty DataFrame gracefully
if df.empty:
    print("[SUCCESS] fact_line_items loaded (0 rows - empty input)")
    exit(0)

df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)

for c in ['gross_total', 'discount_total', 'net_total']:
    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).round(2)

conn = psycopg2.connect(
    host=PG_HOST,
    database=PG_DB,
    user=PG_USER,
    password=PG_PASS
)
cur = conn.cursor()

cur.execute("SELECT product_id, product_key FROM dim_product")
product_map = dict(cur.fetchall())

cur.execute("SELECT order_id, order_key FROM fact_orders")
order_map = dict(cur.fetchall())

df['product_key'] = df['product_id'].map(product_map)
df['order_key'] = df['order_id'].map(order_map)

# Deduplicate by order_key and product_key combination
original_count = len(df)
df = df.drop_duplicates(subset=['order_key', 'product_key'], keep='last')
if len(df) < original_count:
    print(f"[INFO] Removed {original_count - len(df)} duplicate (order_key, product_key) combinations")

cols = [
    'order_key',
    'product_key',
    'quantity',
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
    f"""
    INSERT INTO fact_line_items ({",".join(cols)})
    VALUES %s
    ON CONFLICT (order_key, product_key) DO NOTHING
    """,
    rows,
    page_size=BATCH_SIZE
)

conn.commit()
cur.close()
conn.close()

print(f"[SUCCESS] fact_line_items loaded ({len(rows)} rows)")
