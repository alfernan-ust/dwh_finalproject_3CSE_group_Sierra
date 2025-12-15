import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras

FILE = "/dataset/transactional_campaign_data.parquet"
PG_HOST = "postgres"; PG_DB = "kestra"; PG_USER = "kestra"; PG_PASS = "k3str4"
BATCH_SIZE = 200

def require_file(path):
    if not os.path.exists(path): raise FileNotFoundError(path)

def safe_bool(v):
    return str(v).lower() in ['true', '1', 'yes', 't'] if v is not None else True

def get_db_mapping(conn, query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        return dict(cur.fetchall())
    except: return {}
    finally: cur.close()

def main():
    require_file(FILE)
    df = pd.read_parquet(FILE)
    if 'transaction_date' in df.columns: df.drop(columns=['transaction_date'], inplace=True)

    df['is_duplicate'] = df.duplicated(subset=['order_id', 'campaign_id'], keep='last')
    df["availed"] = df["availed"].apply(safe_bool) if "availed" in df.columns else True
    
    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    
    print("Getting Keys...")
    order_map = get_db_mapping(conn, "SELECT order_id, order_key FROM fact_orders WHERE is_duplicate = false")
    camp_map = get_db_mapping(conn, "SELECT campaign_id, campaign_key FROM dim_campaign WHERE is_duplicate = false")
    
    df['order_key'] = df['order_id'].map(order_map).astype('Int64')
    df['campaign_key'] = df['campaign_id'].map(camp_map).astype('Int64')
    
    df = df.dropna(subset=['order_key', 'campaign_key'])
    df = df.where(pd.notnull(df), None)

    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS fact_campaign_transactions CASCADE;")
    conn.commit()

    cur.execute("""
    CREATE TABLE fact_campaign_transactions (
        transaction_id serial PRIMARY KEY,
        order_key INT REFERENCES fact_orders(order_key),
        campaign_key INT REFERENCES dim_campaign(campaign_key),
        availed boolean,
        is_duplicate boolean
    );
    """)
    conn.commit()
            
    rows = []
    for _, r in df.iterrows():
        rows.append((r.get("order_key"), r.get("campaign_key"), r.get("availed"), r.get("is_duplicate")))

    psycopg2.extras.execute_batch(cur, """
        INSERT INTO fact_campaign_transactions (order_key, campaign_key, availed, is_duplicate)
        VALUES (%s, %s, %s, %s)
    """, rows, page_size=BATCH_SIZE)
    
    conn.commit(); cur.close(); conn.close()
    print(f"Loaded {len(rows)} rows into fact_campaign_transactions")

if __name__ == "__main__":
    main()