import os
import sys
import time
import pandas as pd
import psycopg2
import psycopg2.extras

ORDER_FILE = "/dataset/order_merchant_data.parquet"
OUTPUT_FILE = "/dataset/output_order_data.parquet"
DELAY_FILE = "/dataset/order_delays.parquet"
ORDER_COST_FILE = "/dataset/order_cost.parquet"

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

def safe_int(v):
    if v is None or pd.isna(v): return None
    try: return int(float(v))
    except: return None

def get_dimension_map(conn, table, business_col, key_col):
    print(f"Fetching mapping for {table}...")
    cur = conn.cursor()
    # Surrogate Key 
    query = f"SELECT {business_col}, {key_col} FROM {table} WHERE is_duplicate = false"
    try:
        cur.execute(query)
        return dict(cur.fetchall())
    except Exception as e:
        print(f"Error fetching mapping: {e}")
        return {}
    finally:
        cur.close()

def calculate_order_financials():
    print("Calculating order financials...")
    if not (os.path.exists(LINE_PRODUCTS) and os.path.exists(LINE_PRICES)): return pd.DataFrame()

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
    else: df_lines['price'] = 0.0
        
    df_lines['price'] = df_lines['price'].fillna(0.0)
    df_lines['line_gross'] = df_lines['quantity'] * df_lines['price']
    
    df_agg = df_lines.groupby('order_id')['line_gross'].sum().reset_index()
    df_agg.rename(columns={'line_gross': 'gross_total'}, inplace=True)
    
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
    print(f"Waiting for {len(required_keys)} unique date keys...")
    cur = conn.cursor()
    for _ in range(max_retries):
        cur.execute(f"SELECT unnest(array{list(required_keys)}) EXCEPT SELECT date_key FROM dim_date;")
        if not cur.fetchall(): cur.close(); return
        time.sleep(5)
    cur.close()
    print("Warning: Missing date keys.")

def main():
    require_files(ORDER_FILE, OUTPUT_FILE)
    df_orders = pd.read_parquet(ORDER_FILE); df_output = pd.read_parquet(OUTPUT_FILE)
    
    # Mapping
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
    df = df.merge(df_fin, on='order_id', how='left')
    
    for col in ['gross_total', 'discount_total', 'net_total']:
        if col not in df.columns: df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    for col in ['transaction_date','estimated_arrival']:
        if col in df.columns: df[col] = pd.to_datetime(df[col], errors='coerce')

    df['transaction_date_key'] = df['transaction_date'].dt.strftime('%Y-%m-%d').where(df['transaction_date'].notnull(), None) if 'transaction_date' in df.columns else None
    df['estimated_arrival_date_key'] = df['estimated_arrival'].dt.strftime('%Y-%m-%d').where(df['estimated_arrival'].notnull(), None) if 'estimated_arrival' in df.columns else None
    
    df['delay_in_days'] = pd.to_numeric(df['delay_minutes'], errors='coerce') / (60*24) if 'delay_minutes' in df.columns else 0
    df['delay_in_days'] = df['delay_in_days'].fillna(0).astype(int)

    df['is_duplicate'] = df.duplicated(subset=['order_id'], keep='last')

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    
    # KEys
    cust_map = get_dimension_map(conn, 'dim_customer', 'user_id', 'customer_key')
    merch_map = get_dimension_map(conn, 'dim_merchant', 'merchant_id', 'merchant_key')
    staff_map = get_dimension_map(conn, 'dim_staff', 'staff_id', 'staff_key')
    
    df['customer_key'] = df['user_id'].map(cust_map).astype('Int64')
    df['merchant_key'] = df['merchant_id'].map(merch_map).astype('Int64')
    df['staff_key'] = df['staff_id'].map(staff_map).astype('Int64')
    
    df = df.where(pd.notnull(df), None)

    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS fact_orders CASCADE;")
    conn.commit()
    
    cur.execute("""
    CREATE TABLE fact_orders (
        order_key SERIAL PRIMARY KEY,
        order_id varchar, 
        is_duplicate boolean,
        
        customer_key INT REFERENCES dim_customer(customer_key),
        merchant_key INT REFERENCES dim_merchant(merchant_key),
        staff_key    INT REFERENCES dim_staff(staff_key),
        
        transaction_date_key VARCHAR(10) REFERENCES dim_date(date_key),
        estimated_arrival_date_key VARCHAR(10) REFERENCES dim_date(date_key),
        
        delay_in_days INT,
        gross_total DECIMAL(12,2),
        discount_total DECIMAL(12,2),
        net_total DECIMAL(12,2)
    );
    """)
    conn.commit()
    
    required_keys = list(df['transaction_date_key'].dropna().unique())
    wait_for_date_keys(conn, required_keys)

    rows = []
    for _, r in df.iterrows():
        rows.append((
            r.get('order_id'), r.get('is_duplicate'),
            r.get('customer_key'), r.get('merchant_key'), r.get('staff_key'),
            r.get('transaction_date_key'), r.get('estimated_arrival_date_key'), 
            r.get('delay_in_days'), 
            r.get('gross_total'), r.get('discount_total'), r.get('net_total')
        ))

    insert_sql = """
    INSERT INTO fact_orders (
        order_id, is_duplicate, customer_key, merchant_key, staff_key, 
        transaction_date_key, estimated_arrival_date_key, 
        delay_in_days, gross_total, discount_total, net_total
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    try:
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Loaded {len(rows)} rows into fact_orders")
    except Exception as e:
        conn.rollback()
        print("Insert failed:", e, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()