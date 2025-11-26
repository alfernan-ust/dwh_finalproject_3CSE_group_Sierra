import pandas as pd
import psycopg2
import os

# --- Step 1: File paths ---
line_items_products_file = "/dataset/line_item_data_products.parquet"
line_items_prices_file = "/dataset/line_item_data_prices.parquet"

# Check if files exist
for f in [line_items_products_file, line_items_prices_file]:
    if not os.path.exists(f):
        raise FileNotFoundError(f"{f} not found")

# --- Step 2: Load Parquet files ---
df_products = pd.read_parquet(line_items_products_file)  # columns: order_id, product_name, product_id
df_prices = pd.read_parquet(line_items_prices_file)      # columns: order_id, price, quantity

# --- Step 3: Merge DataFrames on order_id ---
df_line_items = df_products.merge(df_prices, on="order_id", how="inner")

# Replace NaN with None for PostgreSQL
df_line_items = df_line_items.where(pd.notnull(df_line_items), None)

print(f"Merged DataFrame shape: {df_line_items.shape}")
print(f"Columns: {df_line_items.columns.tolist()}")

# --- Step 4: Connect to PostgreSQL ---
conn = psycopg2.connect(
    host="postgres",
    database="kestra",
    user="kestra",
    password="k3str4"
)
cur = conn.cursor()

# --- Step 5: Create fact_line_items table if not exists ---
cur.execute("""
CREATE TABLE IF NOT EXISTS fact_line_items (
    line_item_id serial PRIMARY KEY,
    order_id varchar REFERENCES fact_orders(order_id),
    product_id varchar REFERENCES dim_product(product_id),
    price decimal(10,2),
    quantity int
);
""")
conn.commit()

# --- Step 6: Insert data into PostgreSQL ---
for _, row in df_line_items.iterrows():
    cur.execute("""
        INSERT INTO fact_line_items (order_id, product_id, price, quantity)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """, (row['order_id'], row['product_id'], row['price'], row['quantity']))

conn.commit()
cur.close()
conn.close()

print(f"Loaded {len(df_line_items)} rows into fact_line_items")
