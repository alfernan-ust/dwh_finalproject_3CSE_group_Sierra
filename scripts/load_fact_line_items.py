import os
import sys
import logging
import pandas as pd
import psycopg2
import psycopg2.extras
import gc 

# --- CONFIG ---
LINE_PRODUCTS = "/dataset/line_item_data_products.parquet"
LINE_PRICES   = "/dataset/line_item_data_prices.parquet"
PRODUCT_FILE  = "/dataset/product_list.parquet"
CAMPAIGN_TXN  = "/dataset/transactional_campaign_data.parquet"
CAMPAIGN_DIM  = "/dataset/campaign_data.parquet"

PG_HOST = "postgres"; PG_DB = "kestra"; PG_USER = "kestra"; PG_PASS = "k3str4"
BATCH = 1000 

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def require_file(path):
    if not os.path.exists(path): 
        logging.warning(f"File not found: {path}")
        return False
    return True

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

def get_existing_lines(conn):
    """Fetches existing (order_key, product_key) tuples to avoid re-insertion."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT order_key, product_key FROM fact_line_items")
        # Return as a set of tuples for fast lookup
        return set(cur.fetchall())
    except: return set()
    finally: cur.close()

def ensure_product_exists(conn, ids):
    unique_ids = list(set([str(x) for x in ids if pd.notna(x) and str(x) != '']))
    if not unique_ids: return
    cur = conn.cursor()
    cur.execute("SELECT product_id FROM dim_product WHERE product_id IN %s", (tuple(unique_ids),))
    existing = set(r[0] for r in cur.fetchall())
    missing = [x for x in unique_ids if x not in existing]
    if missing:
        logging.info(f"Inferring {len(missing)} missing products...")
        psycopg2.extras.execute_values(
            cur, 
            "INSERT INTO dim_product (product_id, is_referred) VALUES %s ON CONFLICT (product_id) DO NOTHING", 
            [(x, True) for x in missing]
        )
        conn.commit()
    cur.close()

def align_products_quantities(df_p, df_q):
    logging.info("Aligning products & quantities (Vectorized)...")
    df_p['order_id'] = df_p['order_id'].astype(str).str.strip()
    df_q['order_id'] = df_q['order_id'].astype(str).str.strip()
    df_p['pos_idx'] = df_p.groupby('order_id').cumcount()
    df_q['pos_idx'] = df_q.groupby('order_id').cumcount()
    df_aligned = pd.merge(
        df_p[['order_id', 'pos_idx', 'product_id']], 
        df_q[['order_id', 'pos_idx', 'quantity']], 
        on=['order_id', 'pos_idx'], how='inner'
    )
    del df_p, df_q; gc.collect()
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
             df_cdim['discount'] = pd.to_numeric(df_cdim['discount'].astype(str).str.replace(r'[^0-9.]', '', regex=True), errors='coerce').fillna(0.0) / 100.0
        df_camp = df_ctxn.merge(df_cdim, on='campaign_id', how='left')
        df_camp['availed'] = df_camp['availed'].astype(str).str.lower().isin(['true','1','yes','t'])
        df_camp = df_camp[df_camp['availed']].drop_duplicates(subset=['order_id'])
        order_discount_map = df_camp.set_index('order_id')['discount'].to_dict()
        del df_ctxn, df_cdim, df_camp; gc.collect()

    df_line['unit_price'] = df_line['product_id'].map(price_map).fillna(0.0)
    df_line['discount_rate'] = df_line['order_id'].map(order_discount_map).fillna(0.0)
    df_line['gross_total'] = df_line['quantity'] * df_line['unit_price']
    df_line['discount_total'] = df_line['gross_total'] * df_line['discount_rate']
    df_line['net_total'] = df_line['gross_total'] - df_line['discount_total']
    df_line.drop(columns=['unit_price', 'discount_rate'], inplace=True)
    gc.collect()
    return df_line

def main():
    logging.info("Loading Fact Line Items...")
    if not require_file(LINE_PRODUCTS) or not require_file(LINE_PRICES): return
    
    df_p = pd.read_parquet(LINE_PRODUCTS)
    df_q = pd.read_parquet(LINE_PRICES)
    df_line = align_products_quantities(df_p, df_q)
    
    df_line['is_duplicate'] = df_line.duplicated(subset=['order_id', 'product_id'], keep='last')
    df_line = calculate_financials(df_line)
    
    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    ensure_product_exists(conn, df_line['product_id'].tolist())
    
    logging.info("Mapping keys...")
    order_map = get_db_mapping(conn, "SELECT order_id, order_key FROM fact_orders")
    prod_map = get_db_mapping(conn, "SELECT product_id, product_key FROM dim_product")
    
    df_line['order_key'] = df_line['order_id'].map(order_map).astype('Int64')
    df_line['product_key'] = df_line['product_id'].map(prod_map).astype('Int64')
    df_line = df_line.where(pd.notnull(df_line), None)

    # --- INCREMENTAL CHECK ---
    # We filter out rows where (order_key, product_key) already exists in the DB
    logging.info("Checking for existing records...")
    existing_pairs = get_existing_lines(conn) # set of (order_key, product_key)
    
    # Create tuple column for filtering
    # Note: Int64 can be NaN (pd.NA), ensure we handle that before tuple creation or allow it (None)
    # existing_pairs will likely contain int/None.
    
    initial_len = len(df_line)
    
    # Filter function: keep if keys are missing (to go to reject) OR if pair not in existing
    def is_new(row):
        if pd.isna(row['order_key']) or pd.isna(row['product_key']):
            return True # Let reject logic handle it
        return (row['order_key'], row['product_key']) not in existing_pairs

    # Apply filter efficiently
    mask = df_line.apply(is_new, axis=1)
    df_line = df_line[mask]
    
    logging.info(f"Skipped {initial_len - len(df_line)} existing line items. Processing {len(df_line)} new items.")

    if df_line.empty:
        logging.info("No new line items to load.")
        return

    # --- REJECT vs VALID ---
    # Reject if keys are missing
    reject_mask = df_line['order_key'].isnull() | df_line['product_key'].isnull()
    
    # Or if attributes missing (e.g. quantity is None?) - optional strictness
    # Adding strictness on gross_total as per instruction "attributes are missing"
    attr_missing = df_line['gross_total'].isnull()
    
    final_reject = reject_mask | attr_missing
    
    df_reject = df_line[final_reject].copy()
    df_valid = df_line[~final_reject].copy()

    cur = conn.cursor()
    try:
        if not df_reject.empty:
            logging.warning(f"Rejecting {len(df_reject)} rows.")
            reject_rows = []
            for _, r in df_reject.iterrows():
                reason = "Missing Keys" if (pd.isna(r.order_key) or pd.isna(r.product_key)) else "Missing Attributes"
                reject_rows.append((r.order_id, r.product_id, r.quantity, r.gross_total, reason))
                
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO reject_fact_line_items (order_id, product_id, quantity, gross_total, rejection_reason)
                VALUES (%s, %s, %s, %s, %s)
            """, reject_rows, page_size=BATCH)

        if not df_valid.empty:
            logging.info(f"Inserting {len(df_valid)} valid rows...")
            valid_rows = []
            for row in df_valid.itertuples(index=False):
                valid_rows.append((
                    int(row.order_key), int(row.product_key), int(row.quantity),
                    float(row.gross_total), float(row.discount_total), float(row.net_total),
                    bool(row.is_duplicate)
                ))
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO fact_line_items (order_key, product_key, quantity, gross_total, discount_total, net_total, is_duplicate)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, valid_rows, page_size=BATCH)
        
        conn.commit()
        logging.info("Fact Line Items Load Complete.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Load failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()