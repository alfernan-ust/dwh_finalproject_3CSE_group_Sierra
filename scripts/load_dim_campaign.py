import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras

FILE = "/dataset/campaign_data.parquet"
PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 500

def main():
    print("Loading Campaign Data...")
    if not os.path.exists(FILE):
        print(f"Warning: {FILE} not found.")
        return
    
    try:
        df = pd.read_parquet(FILE)
    except Exception as e:
        print(f"Error reading parquet: {e}", file=sys.stderr)
        return

    # 1. MARK DUPLICATES
    # keep='last' marks the last occurrence as False (0), all others as True (1)
    df['is_duplicate'] = df.duplicated(subset=['campaign_id'], keep='last')
    
    if 'discount' in df.columns:
        df['discount'] = pd.to_numeric(df['discount'].astype(str).str.replace(r'[^0-9.]', '', regex=True), errors='coerce')

    df['campaign_id'] = df['campaign_id'].astype(str)
    df['is_referred'] = False
    df = df.where(pd.notnull(df), None)

    reject_mask = df['campaign_id'].isnull() | (df['campaign_id'] == 'nan') | (df['campaign_id'] == '')
    df_reject = df[reject_mask].copy()
    df_valid = df[~reject_mask].copy()

    # 2. DEDUPLICATE FOR UPSERT
    # We drop duplicates from the batch to prevent "ON CONFLICT" errors.
    # We keep='last' to ensure the row we insert is the one marked is_duplicate=False
    if not df_valid.empty:
        df_valid = df_valid.drop_duplicates(subset=['campaign_id'], keep='last')

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    try:
        if not df_reject.empty:
            df_reject['rejection_reason'] = 'Missing campaign_id'
            cols = ['campaign_id', 'is_duplicate', 'campaign_name', 'campaign_description', 'discount', 'is_referred', 'rejection_reason']
            for c in cols:
                if c not in df_reject.columns: df_reject[c] = None
            query = f"INSERT INTO reject_dim_campaign ({','.join(cols)}) VALUES %s"
            psycopg2.extras.execute_values(cur, query, df_reject[cols].values.tolist())

        if not df_valid.empty:
            cols = ['campaign_id', 'is_duplicate', 'campaign_name', 'campaign_description', 'discount', 'is_referred']
            for c in cols:
                if c not in df_valid.columns: df_valid[c] = None
            
            upsert = """
                INSERT INTO dim_campaign (campaign_id, is_duplicate, campaign_name, campaign_description, discount, is_referred)
                VALUES %s
                ON CONFLICT (campaign_id) DO UPDATE SET
                    is_duplicate = EXCLUDED.is_duplicate,
                    campaign_name = EXCLUDED.campaign_name,
                    campaign_description = EXCLUDED.campaign_description,
                    discount = EXCLUDED.discount,
                    is_referred = FALSE;
            """
            psycopg2.extras.execute_values(cur, upsert, df_valid[cols].values.tolist())

        conn.commit()
        print("Dim Campaign Loaded.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()