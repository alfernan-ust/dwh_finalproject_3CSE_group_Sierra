import os
import sys
import time
import pandas as pd
import psycopg2
import psycopg2.extras

ORDER_FILE = "/dataset/order_merchant_data.parquet"
OUTPUT_FILE = "/dataset/output_order_data.parquet"
DELAY_FILE = "/dataset/order_delays.parquet"

LINE_PRODUCTS = "/dataset/line_item_data_products.parquet"
LINE_PRICES   = "/dataset/line_item_data_prices.parquet"
PRODUCT_FILE  = "/dataset/product_list.parquet"
CAMPAIGN_TXN  = "/dataset/transactional_campaign_data.parquet"
CAMPAIGN_DIM  = "/dataset/campaign_data.parquet"

PG_HOST = "postgres"; PG_DB = "kestra"; PG_USER = "kestra"; PG_PASS = "k3str4"
BATCH_SIZE = 200

def require_files(*paths):
    for p in paths:
        if not os.path.exists(p): raise FileNotFoundError(f"{p} not found")

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None

def ensure_dim_exists(conn, table, col, ids):
    """Infers missing dimensions (Customer, Merchant, Staff) to allow fact loading."""
    unique_ids = list(set([str(x) for x in ids if pd.notna(x) and str(x) != '' and str(x) != 'nan']))
    if not unique_ids: return

    cur = conn.cursor()
    cur.execute(f"SELECT {col} FROM {table} WHERE {col} IN %s", (tuple(unique_ids),))
    existing = set(r[0] for r in cur.fetchall())
    missing = [x for x in unique_ids if x not in existing]
    
    if missing:
        print(f"Inferring {len(missing)} missing items in {table}...")
        insert_query = f"INSERT INTO {table} ({col}, is_referred) VALUES %s ON CONFLICT ({col}) DO NOTHING"
        psycopg2.extras.execute_values(cur, insert_query, [(x, True) for x in missing])
        conn.commit()
    cur.close()

def get_dimension_map(conn, table, business_col, key_col):
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT {business_col}, {key_col} FROM {table}")
        return dict(cur.fetchall())
    except Exception as e:
        print(f"Error fetching mapping: {e}")
        return {}
    finally:
        cur.close()

def get_existing_orders(conn):
    """Fetches existing order_ids to avoid re-loading data on re-runs."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT order_id FROM fact_orders")
        return set(r[0] for r in cur.fetchall())
    except: return set()
    finally: cur.close()

def calculate_order_financials():
    if not (os.path.exists(LINE_PRODUCTS) and os.path.exists(LINE_PRICES)): 
        return pd.DataFrame()
    df_p = pd.read_parquet(LINE_PRODUCTS); df_q = pd.read_parquet(LINE_PRICES)
    df_p['order_id'] = df_p['order_id'].astype(str); df_q['order_id'] = df_q['order_id'].astype(str)
    df_p['idx'] = df_p.groupby('order_id').cumcount(); df_q['idx'] = df_q.groupby('order_id').cumcount()
    df_lines = pd.merge(df_p, df_q, on=['order_id', 'idx'], how='inner')
    df_lines['quantity'] = pd.to_numeric(df_lines['quantity'], errors='coerce').fillna(0)
    
    if 'price' in df_lines.columns: df_lines.drop(columns=['price'], inplace=True)
    if os.path.exists(PRODUCT_FILE):
        df_prod = pd.read_parquet(PRODUCT_FILE)
        df_prod['price'] = pd.to_numeric(df_prod['price'], errors='coerce').fillna(0)
        df_lines = df_lines.merge(df_prod[['product_id', 'price']], on='product_id', how='left')
    else: 
        df_lines['price'] = 0.0
    df_lines['price'] = df_lines['price'].fillna(0.0)
    df_lines['line_gross'] = df_lines['quantity'] * df_lines['price']
    df_agg = df_lines.groupby('order_id')['line_gross'].sum().reset_index().rename(columns={'line_gross': 'gross_total'})
    
    if os.path.exists(CAMPAIGN_TXN) and os.path.exists(CAMPAIGN_DIM):
        df_ctxn = pd.read_parquet(CAMPAIGN_TXN); df_cdim = pd.read_parquet(CAMPAIGN_DIM)
        if 'discount' in df_cdim.columns:
             df_cdim['discount'] = pd.to_numeric(df_cdim['discount'].astype(str).str.replace(r'[^0-9.]','',regex=True), errors='coerce').fillna(0.0) / 100.0
        df_camp = df_ctxn.merge(df_cdim, on='campaign_id', how='left')
        df_camp = df_camp[df_camp['availed'].apply(lambda x: str(x).lower() in ['true','1','yes','t'])]
        df_camp = df_camp.drop_duplicates(subset=['order_id']) 
        df_agg = df_agg.merge(df_camp[['order_id', 'discount']], on='order_id', how='left')
        df_agg['discount'] = df_agg['discount'].fillna(0.0)
    else: df_agg['discount'] = 0.0
    
    df_agg['discount_total'] = df_agg['gross_total'] * df_agg['discount']
    df_agg['net_total'] = df_agg['gross_total'] - df_agg['discount_total']
    return df_agg[['order_id', 'gross_total', 'discount_total', 'net_total']]

def wait_for_date_keys(conn, required_keys, max_retries=10):
    required_keys = set(k for k in required_keys if k is not None)
    if not required_keys: return
    cur = conn.cursor()
    for _ in range(max_retries):
        cur.execute(f"SELECT unnest(array{list(required_keys)}) EXCEPT SELECT date_key FROM dim_date;")
        if not cur.fetchall(): 
            cur.close()
            return
        time.sleep(5)
    cur.close()
    print("Warning: Missing date keys in dim_date.")

def main():
    print("Loading Fact Orders...")
    require_files(ORDER_FILE, OUTPUT_FILE)
    df_orders = pd.read_parquet(ORDER_FILE); df_output = pd.read_parquet(OUTPUT_FILE)
    
    orders_map = {}; oid = pick_col(df_orders, ['order_id','id','order_uuid']); orders_map[oid] = 'order_id'
    mid = pick_col(df_orders, ['merchant_id','seller_id']); orders_map[mid] = 'merchant_id'
    sid = pick_col(df_orders, ['staff_id','employee_id']); orders_map[sid] = 'staff_id'
    df_orders = df_orders.rename(columns=orders_map)

    output_map = {}; oid = pick_col(df_output, ['order_id','id']); output_map[oid] = 'order_id'
    uid = pick_col(df_output, ['user_id','customer_id']); output_map[uid] = 'user_id'
    td = pick_col(df_output, ['transaction_date','order_date']); output_map[td] = 'transaction_date'
    eta = pick_col(df_output, ['estimated_arrival','estimated arrival']); output_map[eta] = 'estimated_arrival'
    df_output = df_output.rename(columns=output_map)
    
    df = df_orders.merge(df_output, on='order_id', how='left')
    if os.path.exists(DELAY_FILE):
        df_delays = pd.read_parquet(DELAY_FILE)
        del_map = {}; oid = pick_col(df_delays, ['order_id']); del_map[oid] = 'order_id'
        dm = pick_col(df_delays, ['delay_minutes','delay']); del_map[dm] = 'delay_minutes'
        df_delays = df_delays.rename(columns=del_map)
        df = df.merge(df_delays, on='order_id', how='left')
    
    df_fin = calculate_order_financials()
    if not df_fin.empty: df = df.merge(df_fin, on='order_id', how='left')
    
    for col in ['gross_total', 'discount_total', 'net_total']:
        if col not in df.columns: df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    # --- DATE PARSING & RECOVERY ---
    # 1. Parse Transaction Date
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

    # 2. Parse Estimated Arrival with Fallback
    # First pass: standard datetime parse
    estimated_series = pd.to_datetime(df['estimated_arrival'], errors='coerce')
    
    # Check for rows where date parse failed but data exists (likely offsets/ints)
    # mask: where parsed is NaT AND original was NOT null
    mask_nat = estimated_series.isna() & df['estimated_arrival'].notna()
    
    if mask_nat.any():
        print(f"Detected {mask_nat.sum()} invalid estimated_arrival dates. Attempting offset recovery...")
        # Try converting original values to numeric (int offsets)
        offsets = pd.to_numeric(df.loc[mask_nat, 'estimated_arrival'], errors='coerce')
        valid_offsets_mask = offsets.notna()
        indices_to_fix = valid_offsets_mask.index[valid_offsets_mask]
        
        if not indices_to_fix.empty:
            # Calculate: transaction_date + offset days
            # Note: transaction_date must be valid for this to work
            base_dates = df.loc[indices_to_fix, 'transaction_date']
            recovered_dates = base_dates + pd.to_timedelta(offsets[indices_to_fix], unit='D')
            
            # Fill the recovered dates back into the main series
            estimated_series.loc[indices_to_fix] = recovered_dates
            print(f"Recovered {len(indices_to_fix)} dates using transaction_date + offset.")

    df['estimated_arrival'] = estimated_series

    # Format keys
    df['transaction_date_key'] = df['transaction_date'].dt.strftime('%Y-%m-%d').where(df['transaction_date'].notnull(), None)
    df['estimated_arrival_date_key'] = df['estimated_arrival'].dt.strftime('%Y-%m-%d').where(df['estimated_arrival'].notnull(), None)
    
    # --- FIX for AttributeError: 'float' object has no attribute 'fillna' ---
    if 'delay_minutes' not in df.columns:
        df['delay_minutes'] = 0
    # Ensure Series operation by forcing to_numeric on the column series
    delay_series = pd.to_numeric(df['delay_minutes'], errors='coerce')
    df['delay_in_days'] = (delay_series / (60*24)).fillna(0).astype(int)

    df['order_id'] = df['order_id'].astype(str)
    df['is_duplicate'] = df.duplicated(subset=['order_id'], keep='last')

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    
    # --- INCREMENTAL CHECK ---
    print("Checking for existing orders...")
    existing_ids = get_existing_orders(conn)
    initial_count = len(df)
    # Filter: keep only order_ids NOT in existing_ids
    df = df[~df['order_id'].isin(existing_ids)]
    print(f"Skipped {initial_count - len(df)} existing orders. Processing {len(df)} new orders.")

    if df.empty:
        print("No new data to load.")
        return

    # --- INFERENCE ---
    print("Inferring missing dimensions...")
    ensure_dim_exists(conn, 'dim_customer', 'user_id', df['user_id'].tolist())
    ensure_dim_exists(conn, 'dim_merchant', 'merchant_id', df['merchant_id'].tolist())
    ensure_dim_exists(conn, 'dim_staff', 'staff_id', df['staff_id'].tolist())
    
    # --- MAPPING ---
    cust_map = get_dimension_map(conn, 'dim_customer', 'user_id', 'customer_key')
    merch_map = get_dimension_map(conn, 'dim_merchant', 'merchant_id', 'merchant_key')
    staff_map = get_dimension_map(conn, 'dim_staff', 'staff_id', 'staff_key')
    
    df['customer_key'] = df['user_id'].map(cust_map).astype('Int64')
    df['merchant_key'] = df['merchant_id'].map(merch_map).astype('Int64')
    df['staff_key'] = df['staff_id'].map(staff_map).astype('Int64')
    df = df.where(pd.notnull(df), None)

    # --- REJECT LOGIC ---
    reject_mask = df['customer_key'].isnull() | df['merchant_key'].isnull() | df['staff_key'].isnull() | df['transaction_date_key'].isnull()
    df_reject = df[reject_mask].copy()
    df_valid = df[~reject_mask].copy()
    
    cur = conn.cursor()
    try:
        if not df_reject.empty:
            print(f"Rejecting {len(df_reject)} rows (Missing Keys or Date).")
            reject_rows = []
            for _, r in df_reject.iterrows():
                reject_rows.append((
                    r.get('order_id'), r.get('user_id'), r.get('merchant_id'), r.get('staff_id'),
                    r.get('transaction_date_key'), r.get('gross_total'), "Missing Keys/Date"
                ))
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO reject_fact_orders (order_id, user_id, merchant_id, staff_id, transaction_date_key, gross_total, rejection_reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, reject_rows, page_size=BATCH_SIZE)

        if not df_valid.empty:
            wait_for_date_keys(conn, list(df_valid['transaction_date_key'].dropna().unique()))
            valid_rows = []
            for _, r in df_valid.iterrows():
                valid_rows.append((
                    r.get('order_id'), r.get('is_duplicate'),
                    r.get('customer_key'), r.get('merchant_key'), r.get('staff_key'),
                    r.get('transaction_date_key'), r.get('estimated_arrival_date_key'), 
                    r.get('delay_in_days'), 
                    r.get('gross_total'), r.get('discount_total'), r.get('net_total')
                ))
            
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO fact_orders (
                    order_id, is_duplicate, customer_key, merchant_key, staff_key, 
                    transaction_date_key, estimated_arrival_date_key, 
                    delay_in_days, gross_total, discount_total, net_total
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, valid_rows, page_size=BATCH_SIZE)

        conn.commit()
        print("Fact Orders Loaded.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()