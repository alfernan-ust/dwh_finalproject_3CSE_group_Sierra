import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras

FILE = "/dataset/transactional_campaign_data.parquet"
PG_HOST = "postgres"; PG_DB = "kestra"; PG_USER = "kestra"; PG_PASS = "k3str4"
BATCH_SIZE = 500

def require_file(path):
    if not os.path.exists(path): return False
    return True

def safe_bool(v):
    return str(v).lower() in ['true', '1', 'yes', 't'] if v is not None else True

def get_db_mapping(conn, query):
    cur = conn.cursor()
    try:
        cur.execute(query)
        return dict(cur.fetchall())
    except: return {}
    finally: cur.close()

def get_existing_txns(conn):
    cur = conn.cursor()
    try:
        cur.execute("SELECT order_key, campaign_key FROM fact_campaign_transactions")
        return set(cur.fetchall())
    except: return set()
    finally: cur.close()

def ensure_campaign_exists(conn, ids):
    unique_ids = list(set([str(x) for x in ids if pd.notna(x) and str(x) != '']))
    if not unique_ids: return
    cur = conn.cursor()
    cur.execute("SELECT campaign_id FROM dim_campaign WHERE campaign_id IN %s", (tuple(unique_ids),))
    existing = set(r[0] for r in cur.fetchall())
    missing = [x for x in unique_ids if x not in existing]
    if missing:
        psycopg2.extras.execute_values(
            cur, 
            "INSERT INTO dim_campaign (campaign_id, is_referred) VALUES %s ON CONFLICT (campaign_id) DO NOTHING", 
            [(x, True) for x in missing]
        )
        conn.commit()
    cur.close()

def main():
    print("Loading Campaign Transactions...")
    if not require_file(FILE): return
    try:
        df = pd.read_parquet(FILE)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return

    if 'transaction_date' in df.columns: df.drop(columns=['transaction_date'], inplace=True)
    
    df['order_id'] = df['order_id'].astype(str)
    df['campaign_id'] = df['campaign_id'].astype(str)
    df['is_duplicate'] = df.duplicated(subset=['order_id', 'campaign_id'], keep='last')
    df["availed"] = df["availed"].apply(safe_bool) if "availed" in df.columns else True
    
    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    ensure_campaign_exists(conn, df['campaign_id'].tolist())
    
    order_map = get_db_mapping(conn, "SELECT order_id, order_key FROM fact_orders")
    camp_map = get_db_mapping(conn, "SELECT campaign_id, campaign_key FROM dim_campaign")
    
    df['order_key'] = df['order_id'].map(order_map).astype('Int64')
    df['campaign_key'] = df['campaign_id'].map(camp_map).astype('Int64')
    df = df.where(pd.notnull(df), None)
    
    # --- INCREMENTAL CHECK ---
    print("Checking for existing transactions...")
    existing_txns = get_existing_txns(conn)
    initial_count = len(df)
    
    def is_new(row):
        if pd.isna(row['order_key']) or pd.isna(row['campaign_key']): return True
        return (row['order_key'], row['campaign_key']) not in existing_txns

    df = df[df.apply(is_new, axis=1)]
    print(f"Skipped {initial_count - len(df)} existing transactions. Processing {len(df)} new.")

    if df.empty: return

    # --- REJECT vs VALID ---
    # Reject if keys are missing
    reject_mask = df['order_key'].isnull() | df['campaign_key'].isnull()
    # Reject if attribute 'availed' is null? (Likely not null due to boolean, but safe to check if requirement is strict)
    # The prompt said "if attributes are missing", availed is the main attribute here.
    
    df_reject = df[reject_mask].copy()
    df_valid = df[~reject_mask].copy()

    cur = conn.cursor()
    try:
        if not df_reject.empty:
            reject_rows = []
            for _, r in df_reject.iterrows():
                reject_rows.append((r.get("order_id"), r.get("campaign_id"), r.get("availed"), 'Missing Keys'))
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO reject_fact_campaign_transactions (order_id, campaign_id, availed, rejection_reason)
                VALUES (%s, %s, %s, %s)
            """, reject_rows, page_size=BATCH_SIZE)

        if not df_valid.empty:
            valid_rows = []
            for _, r in df_valid.iterrows():
                valid_rows.append((r.get("order_key"), r.get("campaign_key"), r.get("availed"), r.get("is_duplicate")))
            psycopg2.extras.execute_batch(cur, """
                INSERT INTO fact_campaign_transactions (order_key, campaign_key, availed, is_duplicate)
                VALUES (%s, %s, %s, %s)
            """, valid_rows, page_size=BATCH_SIZE)
        
        conn.commit()
        print("Fact Campaign Transactions Load Complete.")
    except Exception as e:
        conn.rollback()
        print("Load failed:", e, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()