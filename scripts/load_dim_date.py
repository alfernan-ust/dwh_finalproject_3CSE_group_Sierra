import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import date

OUTPUT_FILE = "/dataset/output_order_data.parquet"
PG_HOST = "postgres"
PG_DB = "kestra" 
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 500
MIN_DATE_PADDING_YEARS = 1 
MAX_DATE_PADDING_YEARS = 1

def require_file(path):
    if not os.path.exists(path):
        return False
    return True

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None

def main():
    print("Loading Date Dimension...")
    if not require_file(OUTPUT_FILE): return
    
    try:
        df_output = pd.read_parquet(OUTPUT_FILE)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return

    output_map = {}
    td_col = pick_col(df_output, ['transaction_date', 'order_date', 'created_at', 'order_ts'])
    if td_col: output_map[td_col] = 'transaction_date'
    
    ea_col = pick_col(df_output, ['estimated arrival', 'estimated_arrival', 'eta', 'estimated_arrival_at'])
    if ea_col: output_map[ea_col] = 'estimated_arrival'
        
    df_output = df_output.rename(columns=output_map)

    date_series = pd.concat([
        df_output.get('transaction_date', pd.Series(dtype='object')),
        df_output.get('estimated_arrival', pd.Series(dtype='object'))
    ]).rename('date_column').dropna()

    date_series = pd.to_datetime(date_series, errors='coerce').dropna().dt.normalize()

    if date_series.empty:
        start_date = date(2020, 1, 1)
        end_date = date(2025, 12, 31)
    else:
        min_date = date_series.min().date()
        max_date = date_series.max().date()
        start_date = date(min_date.year - MIN_DATE_PADDING_YEARS, 1, 1)
        end_date = date(max_date.year + MAX_DATE_PADDING_YEARS, 12, 31)

    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    df_dates = pd.DataFrame({'date': date_range})

    df_dates['date_key'] = df_dates['date'].dt.strftime('%Y-%m-%d')
    df_dates['year'] = df_dates['date'].dt.year
    df_dates['quarter'] = df_dates['date'].dt.quarter
    df_dates['month'] = df_dates['date'].dt.month
    df_dates['month_name'] = df_dates['date'].dt.strftime('%B')
    df_dates['day'] = df_dates['date'].dt.day
    df_dates['weekday'] = df_dates['date'].dt.weekday + 1 
    df_dates['weekday_name'] = df_dates['date'].dt.strftime('%A')
    df_dates['is_weekend'] = df_dates['date'].dt.dayofweek.isin([5, 6])
    
    cols = ['date_key', 'year', 'quarter', 'month', 'month_name', 'day', 'weekday', 'weekday_name', 'is_weekend']
    df_insert = df_dates[cols].copy()
    df_insert['is_weekend'] = df_insert['is_weekend'].astype(bool)
    df_insert = df_insert.where(pd.notnull(df_insert), None) 

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    rows = [tuple(row) for row in df_insert.values]
    insert_sql = """
    INSERT INTO dim_date (
        date_key, year, quarter, month, month_name, day, weekday, weekday_name, is_weekend
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (date_key) DO NOTHING;
    """

    try:
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Loaded {len(rows)} dates.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()