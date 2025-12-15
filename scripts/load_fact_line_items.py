import os
import sys
import logging
import pandas as pd
import psycopg2
import psycopg2.extras

LINE_PRODUCTS = "/dataset/line_item_data_products.parquet"
LINE_PRICES   = "/dataset/line_item_data_prices.parquet"
PRODUCT_FILE  = "/dataset/product_list.parquet"
CAMPAIGN_TXN  = "/dataset/transactional_campaign_data.parquet"
CAMPAIGN_DIM  = "/dataset/campaign_data.parquet"

PG = dict(host="postgres", database="kestra", user="kestra", password="k3str4")
BATCH = 500

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def require_file(path):
    if not os.path.exists(path): raise FileNotFoundError(path)

def clean_str_series(s):
    s = s.astype(object).where(s.notnull(), None)
    return s.apply(lambda v: v.strip() if isinstance(v, str) and v.strip() != "" else None)

def get_db_mapping(conn, query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        return dict(cur.fetchall())
    except Exception as e:
        logging.warning(f"Mapping fetch failed: {e}")
        return {}
    finally:
        cur.close()

def align_products_quantities(df_p, df_q):
    logging.info("Aligning products...")
    df_p = df_p.copy(); df_q = df_q.copy()
    
    df_p["order_id"] = clean_str_series(df_p["order_id"])
    df_q["order_id"] = clean_str_series(df_q["order_id"])

    p_groups = df_p.groupby("order_id", sort=False).indices
    q_groups = df_q.groupby("order_id", sort=False).indices
    
    combined_rows = []
    order_ids = set(p_groups.keys()).union(set(q_groups.keys()))
    
    for oid in order_ids:
        p_idx = p_groups.get(oid, [])
        q_idx = q_groups.get(oid, [])
        n = min(len(p_idx), len(q_idx))
        p_slice = df_p.iloc[p_idx[:n]].reset_index(drop=True)
        q_slice = df_q.iloc[q_idx[:n]].reset_index(drop=True)
        
        for i in range(n):
            combined_rows.append({
                "order_id": oid,
                "product_id": p_slice.iloc[i].get("product_id"),
                "quantity": q_slice.iloc[i].get("quantity"),
            })
            
    df = pd.DataFrame(combined_rows)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)
    return df

def calculate_financials(df_line):
    logging.info("Calculating financials...")
    if os.path.exists(PRODUCT_FILE):
        df_prod = pd.read_parquet(PRODUCT_FILE)
        df_prod['price'] = pd.to_numeric(df_prod['price'], errors='coerce').fillna(0.0)
        price_map = df_prod.set_index('product_id')['price'].to_dict()
    else: price_map = {}

    order_discount_map = {}
    if os.path.exists(CAMPAIGN_TXN) and os.path.exists(CAMPAIGN_DIM):
        df_ctxn = pd.read_parquet(CAMPAIGN_TXN)
        df_cdim = pd.read_parquet(CAMPAIGN_DIM)
        if 'discount' in df_cdim.columns:
             df_cdim['discount'] = pd.to_numeric(
                df_cdim['discount'].astype(str).str.replace(r'[^0-9.]', '', regex=True),
                errors='coerce'
            ).fillna(0.0) / 100.0
        
        df_camp = df_ctxn.merge(df_cdim, on='campaign_id', how='left')
        df_camp = df_camp[df_camp['availed'].apply(lambda x: str(x).lower() in ['true','1','yes','t'])]
        for _, row in df_camp.iterrows():
            order_discount_map[row['order_id']] = float(row.get('discount', 0))

    def calc_row(row):
        gross = row['quantity'] * price_map.get(row['product_id'], 0.0)
        disc_total = gross * order_discount_map.get(row['order_id'], 0.0)
        return pd.Series([gross, disc_total, gross - disc_total])

    df_line[['gross_total', 'discount_total', 'net_total']] = df_line.apply(calc_row, axis=1)
    return df_line

def main():
    require_file(LINE_PRODUCTS); require_file(LINE_PRICES)
    
    df_p = pd.read_parquet(LINE_PRODUCTS)
    df_q = pd.read_parquet(LINE_PRICES)
    
    df_line = align_products_quantities(df_p, df_q)
    df_line['is_duplicate'] = df_line.duplicated(subset=['order_id', 'product_id'], keep='last')
    df_line = calculate_financials(df_line)
    
    conn = psycopg2.connect(**PG)
    
    # Get Keys
    logging.info("Fetching surrogate keys...")
    # Map Order Key
    order_map = get_db_mapping(conn, "SELECT order_id, order_key FROM fact_orders WHERE is_duplicate = false")
    # Map Product Key
    prod_map = get_db_mapping(conn, "SELECT product_id, product_key FROM dim_product WHERE is_duplicate = false")
    
    df_line['order_key'] = df_line['order_id'].map(order_map).astype('Int64')
    df_line['product_key'] = df_line['product_id'].map(prod_map).astype('Int64')
    
    df_line = df_line.dropna(subset=['order_key', 'product_key'])
    df_line = df_line.where(pd.notnull(df_line), None)

    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS fact_line_items CASCADE;")
    conn.commit()
    
    cur.execute("""
        CREATE TABLE fact_line_items (
            line_item_id SERIAL PRIMARY KEY,
            order_key    INT REFERENCES fact_orders(order_key),
            product_key  INT REFERENCES dim_product(product_key),
            quantity     int,
            gross_total    decimal(12,2),
            discount_total decimal(12,2),
            net_total      decimal(12,2),
            is_duplicate   boolean
        );
    """)
    conn.commit()

    rows = []
    for _, r in df_line.iterrows():
        rows.append((
            r.get("order_key"), r.get("product_key"), r.get("quantity"),
            r.get("gross_total"), r.get("discount_total"), r.get("net_total"),
            r.get("is_duplicate")
        ))

    psycopg2.extras.execute_batch(cur, """
        INSERT INTO fact_line_items (order_key, product_key, quantity, gross_total, discount_total, net_total, is_duplicate)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, rows, page_size=BATCH)
    
    conn.commit(); cur.close(); conn.close()
    logging.info(f"Loaded {len(rows)} rows into fact_line_items")

if __name__ == "__main__":
    main()