import pandas as pd
import psycopg2
import os

# --- File path ---
file_path = "/dataset/product_list.parquet"
if not os.path.exists(file_path):
    raise FileNotFoundError(f"{file_path} not found")

df = pd.read_parquet(file_path)

# --- Clean ---
df = df.where(pd.notnull(df), None)

# --- Connect to PostgreSQL ---
conn = psycopg2.connect(
    host="postgres",
    database="kestra",
    user="kestra",
    password="k3str4"
)
cur = conn.cursor()

# --- Create table if not exists ---
cur.execute("""
CREATE TABLE IF NOT EXISTS dim_product (
    product_id varchar PRIMARY KEY,
    product_name varchar,
    product_type varchar,
    price decimal(10,2)
);
""")
conn.commit()

# --- Insert data ---
for _, row in df.iterrows():
    cur.execute("""
    INSERT INTO dim_product (product_id, product_name, product_type, price)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (product_id) DO UPDATE
    SET product_name = EXCLUDED.product_name,
        product_type = EXCLUDED.product_type,
        price = EXCLUDED.price;
    """, (row['product_id'], row['product_name'], row['product_type'], row['price']))

conn.commit()
cur.close()
conn.close()
print(f"Loaded {len(df)} products into dim_product")
