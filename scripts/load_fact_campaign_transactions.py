import pandas as pd
import psycopg2
import os

# --- Step 1: File path ---
file_path = "/dataset/transactional_campaign_data.parquet"
if not os.path.exists(file_path):
    raise FileNotFoundError(f"{file_path} not found")

# --- Step 2: Load Parquet ---
df = pd.read_parquet(file_path)
df = df.where(pd.notnull(df), None)

# --- Step 3: Convert timestamp columns if needed ---
for col in ['transaction_date', 'estimated_arrival']:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

# Ensure 'availed' is boolean
if 'availed' in df.columns:
    df['availed'] = df['availed'].astype(bool)
else:
    df['availed'] = True  # default True if missing

# --- Step 4: Connect to PostgreSQL ---
conn = psycopg2.connect(
    host="postgres",
    database="kestra",
    user="kestra",
    password="k3str4"
)
cur = conn.cursor()

# --- Step 5: Create fact_campaign_transactions table ---
cur.execute("""
CREATE TABLE IF NOT EXISTS fact_campaign_transactions (
    transaction_id serial PRIMARY KEY,
    order_id varchar REFERENCES fact_orders(order_id),
    campaign_id varchar REFERENCES dim_campaign(campaign_id),
    transaction_date timestamp,
    estimated_arrival timestamp,
    availed boolean
);
""")
conn.commit()

# --- Step 6: Insert data ---
for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO fact_campaign_transactions (
            order_id, campaign_id, transaction_date, estimated_arrival, availed
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO UPDATE
        SET order_id = EXCLUDED.order_id,
            campaign_id = EXCLUDED.campaign_id,
            transaction_date = EXCLUDED.transaction_date,
            estimated_arrival = EXCLUDED.estimated_arrival,
            availed = EXCLUDED.availed;
    """, (
        row.get('order_id'),
        row.get('campaign_id'),
        row.get('transaction_date'),
        row.get('estimated_arrival'),
        row.get('availed')
    ))

conn.commit()
cur.close()
conn.close()

print(f"Loaded {len(df)} rows into fact_campaign_transactions")
