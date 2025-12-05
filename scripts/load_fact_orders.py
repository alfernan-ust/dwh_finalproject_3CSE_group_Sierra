
"""
load_fact_orders.py — loader with correct numeric-offset rules and integrated order cost.

Integrates total_price from order_cost.parquet based on order_id.
"""
import os
import sys
from datetime import datetime
import pandas as pd
import psycopg2
import psycopg2.extras


ORDER_FILE = "/dataset/order_merchant_data.parquet"
OUTPUT_FILE = "/dataset/output_order_data.parquet"
DELAY_FILE = "/dataset/order_delays.parquet"
ORDER_COST_FILE = "/dataset/order_cost.parquet"  


PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 200


def require_files(*paths):
    for p in paths:
        if not os.path.exists(p):
            raise FileNotFoundError(f"{p} not found")

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def is_numeric_like(v):

    if v is None:
        return False
    if pd.isna(v):
        return False
    if isinstance(v, (int, float)):
        return True
    if isinstance(v, str):
        try:
            float(v)
            return True
        except:
            return False
    return False

def safe_int(v):
    if v is None or pd.isna(v):
        return None
    try:
        return int(float(v))
    except:
        return None


def apply_est_offset(row):
    raw = row.get('estimated_arrival_raw')
    trans_dt = row.get('transaction_date')
    if is_numeric_like(raw) and pd.notna(trans_dt):
        try:
            n = float(raw)
            return pd.to_datetime(trans_dt) + pd.to_timedelta(n, unit='D')
        except:
            return row.get('estimated_arrival')
    return row.get('estimated_arrival')

def apply_trans_offset(row):
    raw = row.get('transaction_date_raw')
    est_dt = row.get('estimated_arrival')
    if is_numeric_like(raw) and pd.notna(est_dt):
        try:
            n = float(raw)
            return pd.to_datetime(est_dt) - pd.to_timedelta(n, unit='D')
        except:
            return row.get('transaction_date')
    return row.get('transaction_date')


def main():

    require_files(ORDER_FILE, OUTPUT_FILE, DELAY_FILE, ORDER_COST_FILE)


    df_orders = pd.read_parquet(ORDER_FILE)
    df_output = pd.read_parquet(OUTPUT_FILE)
    df_delays = pd.read_parquet(DELAY_FILE)
    df_cost = pd.read_parquet(ORDER_COST_FILE) # <-- READ NEW COST FILE

    

    orders_map = {}
    oid = pick_col(df_orders, ['order_id','id','order_uuid'])
    if oid: orders_map[oid] = 'order_id'
    mid = pick_col(df_orders, ['merchant_id','seller_id','merchant'])
    if mid: orders_map[mid] = 'merchant_id'
    sid = pick_col(df_orders, ['staff_id','employee_id','staff'])
    if sid: orders_map[sid] = 'staff_id'
    df_orders = df_orders.rename(columns=orders_map)


    output_map = {}

    output_map = {}
    oid = pick_col(df_output, ['order_id','id','order_uuid'])
    if oid: output_map[oid] = 'order_id'
    uid = pick_col(df_output, ['user_id','customer_id','buyer_id'])
    if uid: output_map[uid] = 'user_id'
    td = pick_col(df_output, ['transaction_date','order_date','created_at','order_ts'])
    if td: output_map[td] = 'transaction_date'
    if 'estimated arrival' in df_output.columns:
        output_map['estimated arrival'] = 'estimated_arrival'
    else:
        eta = pick_col(df_output, ['estimated_arrival','eta','estimated_arrival_at'])
        if eta: output_map[eta] = 'estimated_arrival'
    df_output = df_output.rename(columns=output_map)


    cost_map = {}
    oid = pick_col(df_cost, ['order_id','id','order_uuid'])
    if oid: cost_map[oid] = 'order_id'

    cost_col = pick_col(df_cost, ['total_price','price','cost','order_total'])
    if cost_col: cost_map[cost_col] = 'total_price'
    df_cost = df_cost.rename(columns=cost_map)

    df_cost = df_cost[['order_id', 'total_price']].drop_duplicates(subset=['order_id'], keep='last')
    
    del_map = {}
    oid = pick_col(df_delays, ['order_id','id','order_uuid'])
    if oid: del_map[oid] = 'order_id'
    if 'delay in days' in df_delays.columns:
        del_map['delay in days'] = 'delay_in_days'
    else:
        dm = pick_col(df_delays, ['delay_minutes','delay_min','delay'])
        if dm: del_map[dm] = 'delay_minutes'
    df_delays = df_delays.rename(columns=del_map)
    
    
    if 'estimated_arrival' in df_output.columns:
        df_output_raw_est = df_output['estimated_arrival'].copy()
    else:
        df_output_raw_est = pd.Series([None] * len(df_output), index=df_output.index)

    if 'transaction_date' in df_output.columns:
        df_output_raw_trans = df_output['transaction_date'].copy()
    else:
        df_output_raw_trans = pd.Series([None] * len(df_output), index=df_output.index)
    
    df = df_orders.merge(df_output, on='order_id', how='left')
    df = df.merge(df_delays, on='order_id', how='left', suffixes=('','_del'))
    
    df = df.merge(df_cost, on='order_id', how='left')
    
    raw_est_map = dict(zip(df_output['order_id'].astype(str), df_output_raw_est)) if 'order_id' in df_output.columns else {}
    raw_trans_map = dict(zip(df_output['order_id'].astype(str), df_output_raw_trans)) if 'order_id' in df_output.columns else {}

    if raw_est_map:
        df['estimated_arrival_raw'] = df['order_id'].astype(str).map(raw_est_map)
    else:
        df['estimated_arrival_raw'] = pd.Series([None] * len(df), index=df.index)

    if raw_trans_map:
        df['transaction_date_raw'] = df['order_id'].astype(str).map(raw_trans_map)
    else:
        df['transaction_date_raw'] = pd.Series([None] * len(df), index=df.index)
     
    for col in ['transaction_date','estimated_arrival']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    if 'delay_in_days' not in df.columns:
        if 'delay_minutes' in df.columns:
            df['delay_in_days'] = pd.to_numeric(df['delay_minutes'], errors='coerce') / (60*24)
        else:
            df['delay_in_days'] = None
    df['delay_in_days'] = pd.to_numeric(df['delay_in_days'], errors='coerce').fillna(0)

    if 'estimated_arrival' in df.columns and 'estimated_arrival_raw' in df.columns:
        df['estimated_arrival'] = df.apply(apply_est_offset, axis=1)

    if 'transaction_date' in df.columns and 'transaction_date_raw' in df.columns:
        df['transaction_date'] = df.apply(apply_trans_offset, axis=1)

    if 'estimated_arrival_raw' in df.columns:
        df.drop(columns=['estimated_arrival_raw'], inplace=True)
    if 'transaction_date_raw' in df.columns:
        df.drop(columns=['transaction_date_raw'], inplace=True)

    for col in ['transaction_date','estimated_arrival']:
        if col in df.columns:
            df[col] = df[col].astype('object').where(df[col].notnull(), None)

    if 'total_price' in df.columns:
        df['total_price'] = pd.to_numeric(df['total_price'], errors='coerce')

    df = df.where(pd.notnull(df), None)

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_orders (
        order_id varchar PRIMARY KEY,
        user_id varchar,
        merchant_id varchar,
        staff_id varchar,
        transaction_date timestamp,
        estimated_arrival timestamp,
        delay_in_days int,
        total_price decimal(10, 2) -- <-- ADDED TOTAL PRICE DDL
    );
    """)
    conn.commit()

    rows = []
    for _, r in df.iterrows():
        rows.append((
            r.get('order_id'),
            r.get('user_id'),
            r.get('merchant_id'),
            r.get('staff_id'),
            r.get('transaction_date'),
            r.get('estimated_arrival'),
            safe_int(r.get('delay_in_days')),
            r.get('total_price') 
        ))

    insert_sql = """
    INSERT INTO fact_orders (
        order_id, user_id, merchant_id, staff_id, transaction_date,
        estimated_arrival, delay_in_days, total_price -- <-- ADDED TOTAL PRICE COLUMN
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (order_id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      merchant_id = EXCLUDED.merchant_id,
      staff_id = EXCLUDED.staff_id,
      transaction_date = EXCLUDED.transaction_date,
      estimated_arrival = EXCLUDED.estimated_arrival,
      delay_in_days = EXCLUDED.delay_in_days,
      total_price = EXCLUDED.total_price; -- <-- ADDED TOTAL PRICE UPDATE
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