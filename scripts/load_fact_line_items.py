"""
load_fact_line_items.py

- Uses line_item_data_products.parquet + line_item_data_prices.parquet
- Aligns product rows and quantity rows positionally per order_id
- Joins with Product and Campaign data to calculate financials
- Loads a clean order-line fact table:

    fact_line_items
        line_item_id (PK)
        order_id   -> FK to fact_orders(order_id)
        product_id -> FK to dim_product(product_id)
        quantity
        gross_total
        discount_total
        net_total
"""

import os
import sys
import logging

import pandas as pd
import psycopg2
import psycopg2.extras

# Input Data
LINE_PRODUCTS = "/dataset/line_item_data_products.parquet"
LINE_PRICES   = "/dataset/line_item_data_prices.parquet"
PRODUCT_FILE  = "/dataset/product_list.parquet"
CAMPAIGN_TXN  = "/dataset/transactional_campaign_data.parquet"
CAMPAIGN_DIM  = "/dataset/campaign_data.parquet"

PG = dict(
    host="postgres",
    database="kestra",
    user="kestra",
    password="k3str4",
)

BATCH = 500

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


def require_file(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(path)


def clean_str_series(s: pd.Series) -> pd.Series:
    s = s.astype(object).where(s.notnull(), None)

    def norm(v):
        if v is None:
            return None
        if isinstance(v, str):
            v2 = v.strip()
            return v2 if v2 != "" else None
        return v

    return s.apply(norm)


def align_products_quantities(df_p: pd.DataFrame, df_q: pd.DataFrame) -> pd.DataFrame:
    """
    Align product rows and quantity rows positionally per order_id.
    """
    logging.info("Aligning products & quantities positionally per order_id")

    df_p = df_p.copy()
    df_q = df_q.copy()

    if "order_id" not in df_p.columns or "order_id" not in df_q.columns:
        raise Exception("Both line item files must contain order_id")

    df_p["order_id"] = clean_str_series(df_p["order_id"])
    df_q["order_id"] = clean_str_series(df_q["order_id"])

    p_groups = df_p.groupby("order_id", sort=False).indices
    q_groups = df_q.groupby("order_id", sort=False).indices

    combined_rows = []
    dropped_unmatched = 0
    matched_pairs = 0

    order_ids = set(p_groups.keys()).union(set(q_groups.keys()))
    for oid in order_ids:
        p_idx = p_groups.get(oid, [])
        q_idx = q_groups.get(oid, [])

        n_p = len(p_idx)
        n_q = len(q_idx)
        n = min(n_p, n_q)

        if n == 0:
            dropped_unmatched += max(n_p, n_q)
            continue

        p_slice = df_p.iloc[p_idx[:n]].reset_index(drop=True)
        q_slice = df_q.iloc[q_idx[:n]].reset_index(drop=True)

        for i in range(n):
            prod_row = p_slice.iloc[i]
            qty_row = q_slice.iloc[i]

            combined_rows.append(
                {
                    "order_id": oid,
                    "product_id": prod_row.get("product_id"),
                    "quantity": qty_row.get("quantity"),
                }
            )

        matched_pairs += n
        if n_p != n_q:
            dropped_unmatched += abs(n_p - n_q)

    logging.info("Aligned pairs: %d, dropped unmatched rows: %d", matched_pairs, dropped_unmatched)

    df = pd.DataFrame(combined_rows)
    # Convert quantity to numeric immediately for calculations
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    return df

def calculate_financials(df_line):
    """
    Enrich line items with prices and calculate gross, discount, net.
    """
    logging.info("Loading reference data for calculations...")
    
    # 1. Load Products (for Price)
    if os.path.exists(PRODUCT_FILE):
        df_prod = pd.read_parquet(PRODUCT_FILE)
        # Ensure price is numeric
        df_prod['price'] = pd.to_numeric(df_prod['price'], errors='coerce').fillna(0.0)
        price_map = df_prod.set_index('product_id')['price'].to_dict()
    else:
        logging.warning("Product file not found. Prices will be 0.")
        price_map = {}

    # 2. Load Campaign Info (for Discounts)
    # We need: Order -> Campaign -> Discount % (if availed)
    order_discount_map = {} # Maps order_id -> discount_rate (e.g. 0.10)
    
    if os.path.exists(CAMPAIGN_TXN) and os.path.exists(CAMPAIGN_DIM):
        df_ctxn = pd.read_parquet(CAMPAIGN_TXN)
        df_cdim = pd.read_parquet(CAMPAIGN_DIM)
        
        # Clean discount string "10%" or "0.1" -> float
        if 'discount' in df_cdim.columns:
             df_cdim['discount'] = pd.to_numeric(
                df_cdim['discount'].astype(str).str.replace(r'[^0-9.]', '', regex=True),
                errors='coerce'
            ).fillna(0.0)
        
        # Join txn to dim
        df_campaigns = df_ctxn.merge(df_cdim, on='campaign_id', how='left')
        
        # Filter for availed only
        # Handle boolean or string 'true' in availed
        def is_true(x):
            return str(x).lower() in ['true', '1', 'yes', 't']
        
        df_campaigns['availed'] = df_campaigns['availed'].apply(is_true)
        df_availed = df_campaigns[df_campaigns['availed'] == True]
        
        # Create map: order_id -> discount (taking max if multiple, or first)
        # Assuming discount is a percentage (e.g. 0.20). If data is "20", might need /100 logic
        # For safety, if > 1, assume it's like 10 (=10%), if <=1, assume 0.10 (=10%)
        # Adjust logic based on your specific data range. 
        # Using direct value for now.
        for _, row in df_availed.iterrows():
            d = float(row.get('discount', 0))
            order_discount_map[row['order_id']] = d

    logging.info("Applying calculations...")
    
    def calc_row(row):
        pid = row['product_id']
        oid = row['order_id']
        qty = row['quantity']
        
        unit_price = price_map.get(pid, 0.0)
        gross = qty * unit_price
        
        disc_rate = order_discount_map.get(oid, 0.0)
        # Heuristic: if discount is > 1 (e.g. 15), treat as percentage (0.15)
        # if disc_rate > 1: disc_rate = disc_rate / 100.0 
        
        disc_total = gross * disc_rate
        net = gross - disc_total
        
        return pd.Series([gross, disc_total, net])

    df_line[['gross_total', 'discount_total', 'net_total']] = df_line.apply(calc_row, axis=1)
    
    return df_line

def ensure_fact_line_items_table(conn):
    cur = conn.cursor()
    
    # 1. Create table with new columns
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_line_items (
            line_item_id SERIAL PRIMARY KEY, -- NEW PK
            order_id     varchar REFERENCES fact_orders(order_id),
            product_id   varchar REFERENCES dim_product(product_id),
            quantity     int,
            gross_total    decimal(12,2),
            discount_total decimal(12,2),
            net_total      decimal(12,2)
        );
        """
    )
    
    # 2. Truncate
    logging.info("Truncating fact_line_items...")
    cur.execute("TRUNCATE TABLE fact_line_items CASCADE;")
    
    conn.commit()
    cur.close()


def insert_fact_line_items(conn, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        logging.info("No fact_line_items rows to insert")
        return 0

    cur = conn.cursor()

    insert_sql = """
        INSERT INTO fact_line_items (
            order_id, product_id, quantity, gross_total, discount_total, net_total
        ) VALUES (%s, %s, %s, %s, %s, %s);
    """

    rows = []
    for _, r in df.iterrows():
        rows.append(
            (
                r.get("order_id"),
                r.get("product_id"),
                r.get("quantity"),
                r.get("gross_total"),
                r.get("discount_total"),
                r.get("net_total"),
            )
        )

    psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH)
    conn.commit()
    cur.close()

    logging.info("Inserted %d rows into fact_line_items", len(rows))
    return len(rows)


def main():
    logging.info("Starting load_fact_line_items...")
    
    require_file(LINE_PRODUCTS)
    require_file(LINE_PRICES)

    logging.info("Reading line item parquet files")
    df_p = pd.read_parquet(LINE_PRODUCTS)  
    df_q = pd.read_parquet(LINE_PRICES)    

    # 1. Align
    df_line = align_products_quantities(df_p, df_q)
    
    # 2. Calculate Financials
    df_line = calculate_financials(df_line)
    
    logging.info("Combined rows to load: %d", len(df_line))

    conn = psycopg2.connect(**PG)
    try:
        ensure_fact_line_items_table(conn)
        inserted = insert_fact_line_items(conn, df_line)
        logging.info("DONE load_fact_line_items. Inserted rows: %d", inserted)
    finally:
        conn.close()


if __name__ == "__main__":
    main()  