#!/usr/bin/env python3
import os
import sys
from datetime import datetime
import pandas as pd
import psycopg2
import psycopg2.extras

ORDER_FILE = "/dataset/order_merchant_data.parquet"
OUTPUT_FILE = "/dataset/output_order_data.parquet"
DELAY_FILE = "/dataset/order_delays.parquet"

PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 500

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

def ensure_order_id_col(df, candidates=('order_id','id','order_uuid')):
    col = pick_col(df, candidates)
    if not col:
        raise KeyError(f"none of {candidates} found")
    df = df.rename(columns={col: 'order_id'})
    df['order_id'] = df['order_id'].astype(str).str.strip()
    return df

def main():
    require_files(ORDER_FILE, OUTPUT_FILE, DELAY_FILE)

    df_orders_src = pd.read_parquet(ORDER_FILE)          
    df_output = pd.read_parquet(OUTPUT_FILE)           
    df_delays = pd.read_parquet(DELAY_FILE)            

    df_orders_src = ensure_order_id_col(df_orders_src)
    df_output = ensure_order_id_col(df_output)
    df_delays = ensure_order_id_col(df_delays)

    raw_est_series = None
    raw_trans_series = None
    if 'estimated arrival' in df_output.columns:
        raw_est_series = df_output['estimated arrival'].copy()
        df_output = df_output.rename(columns={'estimated arrival': 'estimated_arrival'})
    else:
        est_col = pick_col(df_output, ['estimated_arrival','eta','estimated_arrival_at'])
        raw_est_series = df_output[est_col].copy() if est_col else pd.Series([None]*len(df_output), index=df_output.index)
        if est_col:
            df_output = df_output.rename(columns={est_col: 'estimated_arrival'})

    trans_col = pick_col(df_output, ['transaction_date','order_date','created_at','order_ts'])
    raw_trans_series = df_output[trans_col].copy() if trans_col else pd.Series([None]*len(df_output), index=df_output.index)
    if trans_col:
        df_output = df_output.rename(columns={trans_col: 'transaction_date'})

    if 'delay in days' in df_delays.columns:
        df_delays = df_delays.rename(columns={'delay in days': 'delay_in_days'})
    else:
        dm = pick_col(df_delays, ['delay_minutes','delay_min','delay','delay_mins'])
        if dm:
            df_delays = df_delays.rename(columns={dm: 'delay_minutes'})

    df = df_orders_src[['order_id']].drop_duplicates().merge(
        df_output[['order_id','transaction_date','estimated_arrival']], on='order_id', how='left'
    ).merge(
        df_delays[['order_id'] + ([c for c in ['delay_in_days','delay_minutes'] if c in df_delays.columns])],
        on='order_id', how='left'
    )

    raw_est_map = dict(zip(df_output['order_id'].astype(str), raw_est_series)) if raw_est_series is not None else {}
    raw_trans_map = dict(zip(df_output['order_id'].astype(str), raw_trans_series)) if raw_trans_series is not None else {}

    df['estimated_arrival_raw'] = df['order_id'].astype(str).map(raw_est_map)
    df['transaction_date_raw'] = df['order_id'].astype(str).map(raw_trans_map)

    for col in ['transaction_date','estimated_arrival']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    if 'delay_in_days' not in df.columns:
        if 'delay_minutes' in df.columns:
            df['delay_in_days'] = pd.to_numeric(df['delay_minutes'], errors='coerce') / (60*24)
        else:
            df['delay_in_days'] = None

    df['delay_in_days'] = pd.to_numeric(df['delay_in_days'], errors='coerce').fillna(0)

    def compute_est_val(row):
        raw = row.get('estimated_arrival_raw')
        trans = row.get('transaction_date')
        if is_numeric_like(raw) and pd.notna(trans):
            try:
                n = float(raw)
                return pd.to_datetime(trans) + pd.to_timedelta(n, unit='D')
            except:
                return row.get('estimated_arrival')
        return row.get('estimated_arrival')

    def compute_trans_val(row):
        raw = row.get('transaction_date_raw')
        est = row.get('estimated_arrival')
        if is_numeric_like(raw) and pd.notna(est):
            try:
                n = float(raw)
                return pd.to_datetime(est) - pd.to_timedelta(n, unit='D')
            except:
                return row.get('transaction_date')
        return row.get('transaction_date')

    if 'estimated_arrival' in df.columns and 'estimated_arrival_raw' in df.columns:
        df['estimated_arrival'] = df.apply(compute_est_val, axis=1)

    if 'transaction_date' in df.columns and 'transaction_date_raw' in df.columns:
        df['transaction_date'] = df.apply(compute_trans_val, axis=1)

    for col in ['transaction_date','estimated_arrival']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')


    for col in ['transaction_date','estimated_arrival']:
        if col in df.columns:
            df[col] = df[col].dt.date.where(df[col].notnull(), None)

    df = df.drop(columns=[c for c in ['estimated_arrival_raw','transaction_date_raw','delay_minutes'] if c in df.columns], errors='ignore')

    final_cols = ['order_id','transaction_date','estimated_arrival','delay_in_days']
    df_final = df[[c for c in final_cols if c in df.columns]].copy()

    df_final = df_final.where(pd.notnull(df_final), None)

    print("SAMPLE (first 10):")
    print(df_final.head(10).to_string(index=False))

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_orders (
        order_id varchar PRIMARY KEY,
        transaction_date date,
        estimated_arrival date,
        delay_in_days int
    );
    """)
    conn.commit()

    rows = []
    for _, r in df_final.iterrows():
        rows.append((
            r.get('order_id'),
            r.get('transaction_date'),
            r.get('estimated_arrival'),
            safe_int(r.get('delay_in_days'))
        ))

    insert_sql = """
    INSERT INTO dim_orders (order_id, transaction_date, estimated_arrival, delay_in_days)
    VALUES (%s,%s,%s,%s)
    ON CONFLICT (order_id) DO UPDATE SET
      transaction_date = EXCLUDED.transaction_date,
      estimated_arrival = EXCLUDED.estimated_arrival,
      delay_in_days = EXCLUDED.delay_in_days;
    """

    try:
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Loaded {len(rows)} rows into dim_orders")
    except Exception as e:
        conn.rollback()
        print("Insert failed:", e, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
