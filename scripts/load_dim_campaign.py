import pandas as pd
import psycopg2
import os

file_path = "/dataset/campaign_data.parquet"
if not os.path.exists(file_path):
    raise FileNotFoundError(f"{file_path} not found")

df = pd.read_parquet(file_path)
df = df.where(pd.notnull(df), None)

# --- Fix discount column safely ---
if 'discount' in df.columns:
    # Remove any % or text, convert to float, coerce errors to NaN
    df['discount'] = pd.to_numeric(
        df['discount'].astype(str).str.replace(r'[^0-9.]', '', regex=True),
        errors='coerce'
    )

conn = psycopg2.connect(host="postgres", database="kestra", user="kestra", password="k3str4")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_id varchar PRIMARY KEY,
    campaign_name varchar,
    campaign_description varchar,
    discount decimal(5,2)
);
""")
conn.commit()

# --- Insert safely, skip bad rows ---
for _, row in df.iterrows():
    try:
        cur.execute("""
            INSERT INTO dim_campaign (campaign_id, campaign_name, campaign_description, discount)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (campaign_id) DO UPDATE
            SET campaign_name = EXCLUDED.campaign_name,
                campaign_description = EXCLUDED.campaign_description,
                discount = EXCLUDED.discount;
        """, (row['campaign_id'], row['campaign_name'], row['campaign_description'], row['discount']))
    except Exception as e:
        print(f"Skipping campaign_id {row['campaign_id']} due to error: {e}")

conn.commit()
cur.close()
conn.close()
print(f"Loaded {len(df)} campaigns into dim_campaign")
