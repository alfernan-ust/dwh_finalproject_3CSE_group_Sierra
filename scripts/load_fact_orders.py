import os
import sys
import time
import pandas as pd
import psycopg2
import psycopg2.extras

# --- File Paths ---
ORDER_FILE = "/dataset/order_merchant_data.parquet"
OUTPUT_FILE = "/dataset/output_order_data.parquet"
DELAY_FILE = "/dataset/order_delays.parquet"
ORDER_COST_FILE = "/dataset/order_cost.parquet"

# Files needed for calculations
LINE_PRODUCTS = "/dataset/line_item_data_products.parquet"
LINE_PRICES   = "/dataset/line_item_data_prices.parquet"
PRODUCT_FILE  = "/dataset/product_list.parquet"
CAMPAIGN_TXN  = "/dataset/transactional_campaign_data.parquet"
CAMPAIGN_DIM  = "/dataset/campaign_data.parquet"

PG_HOST = "postgres"
PG_DB = "kestra" 
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 200

# ---------- HELPERS ----------

def require_files(*paths):
    for p in paths:
        if not os.path.exists(p):
            raise FileNotFoundError(f"{p} not found")

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def safe_int(v):
    if v is None or pd.isna(v): return None
    try: return int(float(v))
    except: return None

# ---------- CALCULATION LOGIC ----------

def calculate_order_financials():
    """
    Aggregates line items to calculate Gross, Discount, and Net totals per Order ID.
    Returns a DataFrame: [order_id, gross_total, discount_total, net_total]
    """
    print("Calculating order financials from line items...")
    
    # 1. Load Line Items (Products & Quantities)
    if not (os.path.exists(LINE_PRODUCTS) and os.path.exists(LINE_PRICES)):
        print("Warning: Line item files missing. Financials will be 0.")
        return pd.DataFrame(columns=['order_id', 'gross_total', 'discount_total', 'net_total'])

    df_p = pd.read_parquet(LINE_PRODUCTS)
    df_q = pd.read_parquet(LINE_PRICES)
    
    # Align Logic (Simplified version of line item loader logic for aggregation)
    # Ensure order_id is string
    df_p['order_id'] = df_p['order_id'].astype(str)
    df_q['order_id'] = df_q['order_id'].astype(str)
    
    # We need to join positional. 
    # For aggregation, simple merge might duplicate if multiple lines.
    # Grouping is safer.
    df_p['idx'] = df_p.groupby('order_id').cumcount()
    df_q['idx'] = df_q.groupby('order_id').cumcount()
    
    df_lines = pd.merge(df_p, df_q, on=['order_id', 'idx'], how='inner')
    df_lines['quantity'] = pd.to_numeric(df_lines['quantity'], errors='coerce').fillna(0)
    
    # FIX: Ensure no existing 'price' column causes a collision (price_x, price_y)
    if 'price' in df_lines.columns:
        df_lines.drop(columns=['price'], inplace=True)

    # 2. Join Price
    if os.path.exists(PRODUCT_FILE):
        df_prod = pd.read_parquet(PRODUCT_FILE)
        df_prod['price'] = pd.to_numeric(df_prod['price'], errors='coerce').fillna(0)
        # Merge product price info
        df_lines = df_lines.merge(df_prod[['product_id', 'price']], on='product_id', how='left')
    else:
        df_lines['price'] = 0.0
        
    df_lines['price'] = df_lines['price'].fillna(0.0)
    
    # 3. Calculate Line Gross
    df_lines['line_gross'] = df_lines['quantity'] * df_lines['price']
    
    # 4. Aggregate Gross by Order
    df_agg = df_lines.groupby('order_id')['line_gross'].sum().reset_index()
    df_agg.rename(columns={'line_gross': 'gross_total'}, inplace=True)
    
    # 5. Apply Discounts
    # Load Campaign Data
    if os.path.exists(CAMPAIGN_TXN) and os.path.exists(CAMPAIGN_DIM):
        df_ctxn = pd.read_parquet(CAMPAIGN_TXN)
        df_cdim = pd.read_parquet(CAMPAIGN_DIM)
        
        # Clean discount
        if 'discount' in df_cdim.columns:
             df_cdim['discount'] = pd.to_numeric(
                df_cdim['discount'].astype(str).str.replace(r'[^0-9.]', '', regex=True),
                errors='coerce'
            ).fillna(0.0)
             
             # FIX: Convert percentage points (e.g. 1.00) to decimal rate (0.01)
             df_cdim['discount'] = df_cdim['discount'] / 100.0
            
        df_camp = df_ctxn.merge(df_cdim, on='campaign_id', how='left')
        
        # Check availed
        def is_true(x): return str(x).lower() in ['true', '1', 'yes', 't']
        df_camp = df_camp[df_camp['availed'].apply(is_true)]
        
        # Merge discount rate to orders
        # If multiple campaigns, take max discount? defaulting to first found.
        df_camp = df_camp.drop_duplicates(subset=['order_id']) 
        
        df_agg = df_agg.merge(df_camp[['order_id', 'discount']], on='order_id', how='left')
        df_agg['discount'] = df_agg['discount'].fillna(0.0)
    else:
        df_agg['discount'] = 0.0
        
    # 6. Final Totals
    df_agg['discount_total'] = df_agg['gross_total'] * df_agg['discount']
    df_agg['net_total'] = df_agg['gross_total'] - df_agg['discount_total']
    
    return df_agg[['order_id', 'gross_total', 'discount_total', 'net_total']]

# ---------- WAIT FUNCTION ----------
def wait_for_date_keys(conn, required_keys, max_retries=10, delay_sec=5):
    required_keys = set(k for k in required_keys if k is not None)
    if not required_keys: return

    print(f"Waiting for {len(required_keys)} unique date keys...")
    for attempt in range(max_retries):
        cur = conn.cursor()
        missing_keys_query = f"SELECT unnest(array{list(required_keys)}) EXCEPT SELECT date_key FROM dim_date;"
        cur.execute(missing_keys_query)
        missing_keys = {row[0] for row in cur.fetchall()}
        cur.close()

        if not missing_keys: return
        
        if attempt < max_retries - 1:
            print(f"Attempt {attempt + 1}: Missing keys found. Retrying...")
            time.sleep(delay_sec)
        else:
            print("Warning: Missing date keys. Foreign Key constraint may fail.")
            return

# ---------- MAIN ----------

def main():
    require_files(ORDER_FILE, OUTPUT_FILE)

    # 1. Load Main Order Data
    df_orders = pd.read_parquet(ORDER_FILE)
    df_output = pd.read_parquet(OUTPUT_FILE)
    
    # 2. Standard Column Mapping
    orders_map = {}
    oid = pick_col(df_orders, ['order_id','id','order_uuid'])
    if oid: orders_map[oid] = 'order_id'
    mid = pick_col(df_orders, ['merchant_id','seller_id'])
    if mid: orders_map[mid] = 'merchant_id'
    sid = pick_col(df_orders, ['staff_id','employee_id'])
    if sid: orders_map[sid] = 'staff_id'
    df_orders = df_orders.rename(columns=orders_map)

    output_map = {}
    oid = pick_col(df_output, ['order_id','id'])
    if oid: output_map[oid] = 'order_id'
    uid = pick_col(df_output, ['user_id','customer_id'])
    if uid: output_map[uid] = 'user_id'
    td = pick_col(df_output, ['transaction_date','order_date'])
    if td: output_map[td] = 'transaction_date'
    eta = pick_col(df_output, ['estimated_arrival','estimated arrival'])
    if eta: output_map[eta] = 'estimated_arrival'
    df_output = df_output.rename(columns=output_map)
    
    # 3. Merge Basic Data
    df = df_orders.merge(df_output, on='order_id', how='left')
    
    # 4. Load Delays (Optional)
    if os.path.exists(DELAY_FILE):
        df_delays = pd.read_parquet(DELAY_FILE)
        del_map = {}
        oid = pick_col(df_delays, ['order_id'])
        if oid: del_map[oid] = 'order_id'
        dm = pick_col(df_delays, ['delay_minutes','delay'])
        if dm: del_map[dm] = 'delay_minutes'
        df_delays = df_delays.rename(columns=del_map)
        df = df.merge(df_delays, on='order_id', how='left')
    
    # 5. NEW: Calculate Financials
    df_fin = calculate_order_financials()
    # Merge financials into main DF
    df = df.merge(df_fin, on='order_id', how='left')
    
    # Fill NaN financials with 0
    for col in ['gross_total', 'discount_total', 'net_total']:
        if col not in df.columns: df[col] = 0.0
        df[col] = df[col].fillna(0.0)

    # 6. Process Dates
    for col in ['transaction_date','estimated_arrival']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    if 'transaction_date' in df.columns:
        df['transaction_date_key'] = df['transaction_date'].dt.strftime('%Y-%m-%d').where(df['transaction_date'].notnull(), None)
    else: df['transaction_date_key'] = None
        
    if 'estimated_arrival' in df.columns:
        df['estimated_arrival_date_key'] = df['estimated_arrival'].dt.strftime('%Y-%m-%d').where(df['estimated_arrival'].notnull(), None)
    else: df['estimated_arrival_date_key'] = None
    
    if 'delay_minutes' in df.columns:
        df['delay_in_days'] = pd.to_numeric(df['delay_minutes'], errors='coerce') / (60*24)
        df['delay_in_days'] = df['delay_in_days'].fillna(0).astype(int)
    else:
        df['delay_in_days'] = 0

    # 7. Postgres Load
    required_keys = list(df['transaction_date_key'].dropna().unique())
    
    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()
    
    # --- FIX: Create Table BEFORE Truncate ---
    # Updated DDL with Financial Columns
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_orders (
        order_id varchar PRIMARY KEY,
        user_id varchar,
        merchant_id varchar,
        staff_id varchar,
        transaction_date_key VARCHAR(10) REFERENCES dim_date(date_key),
        estimated_arrival_date_key VARCHAR(10) REFERENCES dim_date(date_key),
        delay_in_days INT,
        gross_total DECIMAL(12,2),
        discount_total DECIMAL(12,2),
        net_total DECIMAL(12,2)
    );
    """)
    conn.commit()
    
    # Now safe to truncate
    cur.execute("TRUNCATE TABLE fact_orders CASCADE;")
    conn.commit()
    
    wait_for_date_keys(conn, required_keys)

    rows = []
    for _, r in df.iterrows():
        rows.append((
            r.get('order_id'), r.get('user_id'), r.get('merchant_id'), r.get('staff_id'),
            r.get('transaction_date_key'), r.get('estimated_arrival_date_key'), 
            r.get('delay_in_days'), 
            r.get('gross_total'), r.get('discount_total'), r.get('net_total')
        ))

    insert_sql = """
    INSERT INTO fact_orders (
        order_id, user_id, merchant_id, staff_id, 
        transaction_date_key, estimated_arrival_date_key, 
        delay_in_days, gross_total, discount_total, net_total
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (order_id) DO UPDATE SET
      gross_total = EXCLUDED.gross_total,
      discount_total = EXCLUDED.discount_total,
      net_total = EXCLUDED.net_total;
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