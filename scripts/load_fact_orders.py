#!/usr/bin/env python3
"""
load_fact_orders.py — loader with correct numeric-offset rules.

Key rules implemented exactly:
- If estimated_arrival_raw is numeric (n) and transaction_date is datetime:
    estimated_arrival = transaction_date + n days
- If transaction_date_raw is numeric (n) and estimated_arrival is datetime:
    transaction_date = estimated_arrival - n days
- DO NOT use delay_in_days to compute estimated_arrival.
"""
import os
import sys
from datetime import datetime
import pandas as pd
import psycopg2
import psycopg2.extras

# ---------- CONFIG ----------
ORDER_FILE = "/dataset/order_merchant_data.parquet"
OUTPUT_FILE = "/dataset/output_order_data.parquet"
DELAY_FILE = "/dataset/order_delays.parquet"

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

def is_numeric_like(v):
    # returns True for int/float, and numeric strings like "13" or "13.0"
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

# ---------- MAIN ----------
def main():
    require_files(ORDER_FILE, OUTPUT_FILE, DELAY_FILE)

    # read raw parquet files
    df_orders = pd.read_parquet(ORDER_FILE)
    df_output = pd.read_parquet(OUTPUT_FILE)
    df_delays = pd.read_parquet(DELAY_FILE)

    # normalize column names (same logic as before)
    # ORDERS
    orders_map = {}
    oid = pick_col(df_orders, ['order_id','id','order_uuid'])
    if oid: orders_map[oid] = 'order_id'
    mid = pick_col(df_orders, ['merchant_id','seller_id','merchant'])
    if mid: orders_map[mid] = 'merchant_id'
    sid = pick_col(df_orders, ['staff_id','employee_id','staff'])
    if sid: orders_map[sid] = 'staff_id'
    df_orders = df_orders.rename(columns=orders_map)

    # OUTPUT
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

    # DELAYS
    del_map = {}
    oid = pick_col(df_delays, ['order_id','id','order_uuid'])
    if oid: del_map[oid] = 'order_id'
    if 'delay in days' in df_delays.columns:
        del_map['delay in days'] = 'delay_in_days'
    else:
        dm = pick_col(df_delays, ['delay_minutes','delay_min','delay'])
        if dm: del_map[dm] = 'delay_minutes'
    df_delays = df_delays.rename(columns=del_map)

    # Keep raw copies (before coercion) for numeric-detection
    # If column missing, create series of None to keep alignment
    if 'estimated_arrival' in df_output.columns:
        df_output_raw_est = df_output['estimated_arrival'].copy()
    else:
        df_output_raw_est = pd.Series([None] * len(df_output), index=df_output.index)

    if 'transaction_date' in df_output.columns:
        df_output_raw_trans = df_output['transaction_date'].copy()
    else:
        df_output_raw_trans = pd.Series([None] * len(df_output), index=df_output.index)

    # Merge
    df = df_orders.merge(df_output, on='order_id', how='left')
    df = df.merge(df_delays, on='order_id', how='left', suffixes=('','_del'))

    # Attach the raw values back aligned by order_id (safer if merges reorder)
    # Build small lookup dicts from df_output original file (use last occurrence if duplicates)
    raw_est_map = dict(zip(df_output['order_id'].astype(str), df_output_raw_est)) if 'order_id' in df_output.columns else {}
    raw_trans_map = dict(zip(df_output['order_id'].astype(str), df_output_raw_trans)) if 'order_id' in df_output.columns else {}

    # create full-series aligned with df index
    if raw_est_map:
        df['estimated_arrival_raw'] = df['order_id'].astype(str).map(raw_est_map)
    else:
        df['estimated_arrival_raw'] = pd.Series([None] * len(df), index=df.index)

    if raw_trans_map:
        df['transaction_date_raw'] = df['order_id'].astype(str).map(raw_trans_map)
    else:
        df['transaction_date_raw'] = pd.Series([None] * len(df), index=df.index)

    # Coerce datetimes (safe)
    for col in ['transaction_date','estimated_arrival']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Prepare delay_in_days but DO NOT use it to compute estimated_arrival.
    if 'delay_in_days' not in df.columns:
        if 'delay_minutes' in df.columns:
            df['delay_in_days'] = pd.to_numeric(df['delay_minutes'], errors='coerce') / (60*24)
        else:
            df['delay_in_days'] = None
    # preserve numeric but fill missing with 0 (you can change this if you prefer NULL)
    df['delay_in_days'] = pd.to_numeric(df['delay_in_days'], errors='coerce').fillna(0)

    # ---- NOW: apply numeric-offset rules using the RAW values ----
    # Case 1: estimated_arrival_raw is numeric-like AND transaction_date is a datetime -> estimated_arrival = transaction_date + n days
    def apply_est_offset(row):
        raw = row.get('estimated_arrival_raw')
        trans_dt = row.get('transaction_date')
        if is_numeric_like(raw) and pd.notna(trans_dt):
            try:
                n = float(raw)
                return pd.to_datetime(trans_dt) + pd.to_timedelta(n, unit='D')
            except:
                return row.get('estimated_arrival')  # fallback
        return row.get('estimated_arrival')

    # Case 2: transaction_date_raw is numeric-like AND estimated_arrival is a datetime -> transaction_date = estimated_arrival - n days
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

    # Apply row-wise adjustments (only if the columns exist)
    if 'estimated_arrival' in df.columns and 'estimated_arrival_raw' in df.columns:
        df['estimated_arrival'] = df.apply(apply_est_offset, axis=1)

    if 'transaction_date' in df.columns and 'transaction_date_raw' in df.columns:
        df['transaction_date'] = df.apply(apply_trans_offset, axis=1)

    # After adjustments, coerce to datetime again
    for col in ['transaction_date','estimated_arrival']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # IMPORTANT: do NOT compute estimated_arrival using delay_in_days
    # Leave estimated_arrival NULL if it was not present or computed by numeric offsets.

    # Final cleanup: drop helper raw columns, convert datetimes to objects for DB insertion
    if 'estimated_arrival_raw' in df.columns:
        df.drop(columns=['estimated_arrival_raw'], inplace=True)
    if 'transaction_date_raw' in df.columns:
        df.drop(columns=['transaction_date_raw'], inplace=True)

    for col in ['transaction_date','estimated_arrival']:
        if col in df.columns:
            df[col] = df[col].astype('object').where(df[col].notnull(), None)

    df = df.where(pd.notnull(df), None)

    # sample log
    sample_cols = ['order_id','user_id','merchant_id','staff_id','transaction_date','estimated_arrival','delay_in_days']
    present_cols = [c for c in sample_cols if c in df.columns]
    print("SAMPLE (first 10):")
    print(df[present_cols].head(10).to_string(index=False))

    # Upsert to Postgres (same schema; include user_id, merchant_id, staff_id)
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
        delay_in_days int
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
            safe_int(r.get('delay_in_days'))
        ))

    insert_sql = """
    INSERT INTO fact_orders (
        order_id, user_id, merchant_id, staff_id, transaction_date,
        estimated_arrival, delay_in_days
    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (order_id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      merchant_id = EXCLUDED.merchant_id,
      staff_id = EXCLUDED.staff_id,
      transaction_date = EXCLUDED.transaction_date,
      estimated_arrival = EXCLUDED.estimated_arrival,
      delay_in_days = EXCLUDED.delay_in_days;
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
