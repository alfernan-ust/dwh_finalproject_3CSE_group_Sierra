import pandas as pd
import psycopg2
import os

# --- Step 1: File paths ---
order_file = "/dataset/order_merchant_data.parquet"
delay_file = "/dataset/order_delays.parquet"

for f in [order_file, delay_file]:
    if not os.path.exists(f):
        raise FileNotFoundError(f"{f} not found")

# --- Step 2: Read Parquet files ---
df_orders = pd.read_parquet(order_file)
df_delays = pd.read_parquet(delay_file)

# --- Step 3: Merge delays with orders ---
df = df_orders.merge(df_delays, on="order_id", how="left")
df = df.where(pd.notnull(df), None)  # Replace NaN with None for psycopg2

# Optional: Map your column names from your Parquet files to the table columns
# Adjust these if your Parquet columns are named differently
df.rename(columns={
    'order_date': 'transaction_date',
    'delay_minutes': 'delay_in_days'  # or calculate as needed
}, inplace=True)

# --- Step 4: Connect to PostgreSQL ---
conn = psycopg2.connect(
    host="postgres",
    database="kestra",
    user="kestra",
    password="k3str4"
)
cur = conn.cursor()

# --- Step 5: Create fact table if it does not exist ---
cur.execute("""
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id varchar PRIMARY KEY,
    user_id varchar,
    merchant_id varchar,
    staff_id varchar,
    transaction_date timestamp,
    estimated_arrival timestamp,
    delay_in_days int
);
""")
conn.commit()

# --- Step 6: Insert data ---
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO fact_orders (
            order_id, user_id, merchant_id, staff_id, transaction_date,
            estimated_arrival, delay_in_days
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE
        SET user_id = EXCLUDED.user_id,
            merchant_id = EXCLUDED.merchant_id,
            staff_id = EXCLUDED.staff_id,
            transaction_date = EXCLUDED.transaction_date,
            estimated_arrival = EXCLUDED.estimated_arrival,
            delay_in_days = EXCLUDED.delay_in_days;
    """, (
        row['order_id'],
        row.get('user_id'),
        row.get('merchant_id'),
        row.get('staff_id'),
        row.get('transaction_date'),
        row.get('estimated_arrival'),
        row.get('delay_in_days')
    ))

# --- Step 7: Commit and close ---
conn.commit()
cur.close()
conn.close()

print(f"Loaded {len(df)} rows into fact_orders")
