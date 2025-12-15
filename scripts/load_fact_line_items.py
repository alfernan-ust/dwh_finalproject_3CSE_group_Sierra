import os
import sys
import logging
import pandas as pd
import psycopg2
import psycopg2.extras
import gc 


LINE_PRODUCTS = "/dataset/line_item_data_products.parquet"
LINE_PRICES   = "/dataset/line_item_data_prices.parquet"
PRODUCT_FILE  = "/dataset/product_list.parquet"
CAMPAIGN_TXN  = "/dataset/transactional_campaign_data.parquet"
CAMPAIGN_DIM  = "/dataset/campaign_data.parquet"

PG = dict(host="postgres", database="kestra", user="kestra", password="k3str4")
BATCH = 1000 

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def require_file(path):
    if not os.path.exists(path): raise FileNotFoundError(path)

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
    """
    Aligns products and quantities by order_id using vectorized merge.
    Faster and strictly memory efficient compared to looping.
    """
    logging.info("Aligning products & quantities (Vectorized)...")
    
   
    df_p['order_id'] = df_p['order_id'].astype(str).str.strip()
    df_q['order_id'] = df_q['order_id'].astype(str).str.strip()

  
    df_p['pos_idx'] = df_p.groupby('order_id').cumcount()
    df_q['pos_idx'] = df_q.groupby('order_id').cumcount()
    

    df_aligned = pd.merge(
        df_p[['order_id', 'pos_idx', 'product_id']], 
        df_q[['order_id', 'pos_idx', 'quantity']], 
        on=['order_id', 'pos_idx'], 
        how='inner'
    )
    

    del df_p, df_q
    gc.collect()
    
    df_aligned['quantity'] = pd.to_numeric(df_aligned['quantity'], errors='coerce').fillna(0)
    
    return df_aligned[['order_id', 'product_id', 'quantity']]

def calculate_financials(df_line):
    logging.info("Calculating financials (Vectorized)...")
    

    price_map = {}
    if os.path.exists(PRODUCT_FILE):
        df_prod = pd.read_parquet(PRODUCT_FILE)
        df_prod['price'] = pd.to_numeric(df_prod['price'], errors='coerce').fillna(0.0)
        price_map = df_prod.set_index('product_id')['price'].to_dict()
        del df_prod
    

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
        
        df_camp['availed'] = df_camp['availed'].astype(str).str.lower().isin(['true','1','yes','t'])
        df_camp = df_camp[df_camp['availed']]
        
        df_camp = df_camp.drop_duplicates(subset=['order_id'])
        
        order_discount_map = df_camp.set_index('order_id')['discount'].to_dict()
        del df_ctxn, df_cdim, df_camp
        gc.collect()

    logging.info("Mapping prices...")
    df_line['unit_price'] = df_line['product_id'].map(price_map).fillna(0.0)
    
    logging.info("Mapping discounts...")
    df_line['discount_rate'] = df_line['order_id'].map(order_discount_map).fillna(0.0)
    
    logging.info("Computing totals...")
    df_line['gross_total'] = df_line['quantity'] * df_line['unit_price']
    df_line['discount_total'] = df_line['gross_total'] * df_line['discount_rate']
    df_line['net_total'] = df_line['gross_total'] - df_line['discount_total']
    
    df_line.drop(columns=['unit_price', 'discount_rate'], inplace=True)
    gc.collect()
    
    return df_line

def main():
    require_file(LINE_PRODUCTS); require_file(LINE_PRICES)
    
    df_p = pd.read_parquet(LINE_PRODUCTS)
    df_q = pd.read_parquet(LINE_PRICES)
    
    df_line = align_products_quantities(df_p, df_q)
    
    logging.info("Flagging duplicates...")
    df_line['is_duplicate'] = df_line.duplicated(subset=['order_id', 'product_id'], keep='last')
    
    df_line = calculate_financials(df_line)
    
    logging.info("Connecting to DB...")
    conn = psycopg2.connect(**PG)
    
    logging.info("Fetching surrogate keys...")
    order_map = get_db_mapping(conn, "SELECT order_id, order_key FROM fact_orders WHERE is_duplicate = false")
    
    prod_map = get_db_mapping(conn, "SELECT product_id, product_key FROM dim_product WHERE is_duplicate = false")
    
    logging.info("Mapping keys to dataframe...")
    df_line['order_key'] = df_line['order_id'].map(order_map).astype('Int64')
    df_line['product_key'] = df_line['product_id'].map(prod_map).astype('Int64')
    
    before_count = len(df_line)
    df_line = df_line.dropna(subset=['order_key', 'product_key'])
    if len(df_line) < before_count:
        logging.warning(f"Dropped {before_count - len(df_line)} rows due to missing keys.")
        
    df_line = df_line.where(pd.notnull(df_line), None)

    cur = conn.cursor()
    logging.info("Recreating table...")
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

    logging.info("Inserting data...")
    rows = []
    for row in df_line.itertuples(index=False):
        rows.append((
            int(row.order_key) if row.order_key is not None else None, 
            int(row.product_key) if row.product_key is not None else None, 
            int(row.quantity) if row.quantity is not None else None,
            float(row.gross_total) if row.gross_total is not None else None, 
            float(row.discount_total) if row.discount_total is not None else None, 
            float(row.net_total) if row.net_total is not None else None,
            bool(row.is_duplicate) if row.is_duplicate is not None else None
        ))

    psycopg2.extras.execute_batch(cur, """
        INSERT INTO fact_line_items (order_key, product_key, quantity, gross_total, discount_total, net_total, is_duplicate)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, rows, page_size=BATCH)
    
    conn.commit(); cur.close(); conn.close()
    logging.info(f"Loaded {len(rows)} rows into fact_line_items")

if __name__ == "__main__":
    main()