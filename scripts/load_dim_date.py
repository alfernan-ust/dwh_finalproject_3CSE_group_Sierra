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
MIN_DATE_PADDING_YEARS = 1  # Pad start date backward by 1 year for safety
MAX_DATE_PADDING_YEARS = 1  # Pad end date forward by 1 year for safety

def require_file(path):
    """Ensures the required input file exists."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")

def pick_col(df, candidates):
    """Finds the first column in the DataFrame that matches a candidate name."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

def main():
    require_file(OUTPUT_FILE)
    
    # -------------------------------------------------------------
    # 1. Load Data and Find Date Range
    # -------------------------------------------------------------
    try:
        df_output = pd.read_parquet(OUTPUT_FILE)
    except Exception as e:
        print(f"Error reading Parquet file: {e}", file=sys.stderr)
        sys.exit(1)

    # Map columns in the source data (df_output) for consistency
    output_map = {}
    
    # Find transaction date column name
    td_col = pick_col(df_output, ['transaction_date', 'order_date', 'created_at', 'order_ts'])
    if td_col: output_map[td_col] = 'transaction_date'
    
    # Find estimated arrival column name
    ea_col = pick_col(df_output, ['estimated arrival', 'estimated_arrival', 'eta', 'estimated_arrival_at'])
    if ea_col: output_map[ea_col] = 'estimated_arrival'
        
    df_output = df_output.rename(columns=output_map)

    # Collect all unique dates from the two relevant columns
    date_series = pd.concat([
        df_output.get('transaction_date', pd.Series(dtype='object')),
        df_output.get('estimated_arrival', pd.Series(dtype='object'))
    ]).rename('date_column').dropna()

    # Convert to datetime and find the min/max dates
    date_series = pd.to_datetime(date_series, errors='coerce').dropna().dt.normalize()

    if date_series.empty:
        print("No valid dates found in the Parquet file. Aborting.", file=sys.stderr)
        return

    min_date = date_series.min().date()
    max_date = date_series.max().date()
    
    # Calculate padded start/end dates for the dimension table
    # This ensures the Dim_Date table spans full years, which is best practice.
    start_date = date(min_date.year - MIN_DATE_PADDING_YEARS, 1, 1)
    end_date = date(max_date.year + MAX_DATE_PADDING_YEARS, 12, 31)

    # -------------------------------------------------------------
    # 2. Generate CONTINUOUS Date Dimension
    # FIX: Creates a continuous range (the primary fix for missing dates)
    # -------------------------------------------------------------
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    df_dates = pd.DataFrame({'date': date_range})

    df_dates['date_key'] = df_dates['date'].dt.strftime('%Y-%m-%d')
    df_dates['year'] = df_dates['date'].dt.year
    df_dates['quarter'] = df_dates['date'].dt.quarter
    df_dates['month'] = df_dates['date'].dt.month
    df_dates['month_name'] = df_dates['date'].dt.strftime('%B')
    df_dates['day'] = df_dates['date'].dt.day
    df_dates['weekday'] = df_dates['date'].dt.weekday + 1 # 1=Monday, 7=Sunday
    df_dates['weekday_name'] = df_dates['date'].dt.strftime('%A')
    df_dates['is_weekend'] = df_dates['date'].dt.dayofweek.isin([5, 6]) # Saturday=5, Sunday=6
    
    cols = ['date_key', 'year', 'quarter', 'month', 'month_name', 'day', 'weekday', 'weekday_name', 'is_weekend']
    
    df_insert = df_dates[cols].copy()
    
    # Ensures True/False Python objects for BOOLEAN database field, replacing NaNs with None
    df_insert['is_weekend'] = df_insert['is_weekend'].astype('object').where(df_insert['is_weekend'].notnull(), None)
    df_insert = df_insert.where(pd.notnull(df_insert), None) 

    # -------------------------------------------------------------
    # 3. POSTGRES UPSERT
    # -------------------------------------------------------------
    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    # --- DROP TABLE: Implemented to drop the table before creation ---
    cur.execute("DROP TABLE IF EXISTS dim_date CASCADE;")
    conn.commit()

    # Create the table structure
    cur.execute("""
    CREATE TABLE dim_date (
        date_key VARCHAR(10) PRIMARY KEY,
        year INT,
        quarter INT,
        month INT,
        month_name VARCHAR(50),
        day INT,
        weekday INT,
        weekday_name VARCHAR(50),
        is_weekend BOOLEAN
    );
    """)
    conn.commit()

    rows = [tuple(row) for row in df_insert.values]

    # Simple INSERT is used as the table is guaranteed to be empty
    insert_sql = """
    INSERT INTO dim_date (
        date_key, year, quarter, month, month_name, day, weekday, weekday_name, is_weekend
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

    try:
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Successfully generated and loaded {len(rows)} continuous date records into dim_date (from {start_date} to {end_date}).")
    except Exception as e:
        conn.rollback()
        print("Insert failed:", e, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()