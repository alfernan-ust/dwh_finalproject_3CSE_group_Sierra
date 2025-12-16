import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras

FILE = "/dataset/product_list.parquet"
PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 500

def require_file(path):
    if not os.path.exists(path):
        print(f"Warning: {path} not found.")
        return False
    return True

def main():
    print("Loading Product Data...")
    if not require_file(FILE): return
    
    try:
        df = pd.read_parquet(FILE)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return
    
    # 1. MARK DUPLICATES
    # keep='last' marks the last occurrence as False (0), all others as True (1)
    df['is_duplicate'] = df.duplicated(subset=['product_id'], keep='last')
    
    df['is_referred'] = False
    df = df.where(pd.notnull(df), None)

    reject_mask = df['product_id'].isnull() | (df['product_id'] == '')
    df_reject = df[reject_mask].copy()
    df_valid = df[~reject_mask].copy()

    # 2. DEDUPLICATE FOR UPSERT
    # We drop duplicates from the batch to prevent "ON CONFLICT" errors.
    # We keep='last' to ensure the row we insert is the one marked is_duplicate=False
    if not df_valid.empty:
        df_valid = df_valid.drop_duplicates(subset=['product_id'], keep='last')

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    try:
        # Handle Rejects
        if not df_reject.empty:
            df_reject['rejection_reason'] = 'Missing product_id'
            reject_cols = ['product_id', 'is_duplicate', 'product_name', 'product_type', 'price', 'is_referred', 'rejection_reason']
            for c in reject_cols:
                if c not in df_reject.columns: df_reject[c] = None
            
            query = f"INSERT INTO reject_dim_product ({','.join(reject_cols)}) VALUES %s"
            psycopg2.extras.execute_values(cur, query, df_reject[reject_cols].values.tolist(), page_size=BATCH_SIZE)

        # Handle Valid Upserts
        if not df_valid.empty:
            valid_cols = ['product_id', 'is_duplicate', 'product_name', 'product_type', 'price', 'is_referred']
            for c in valid_cols:
                 if c not in df_valid.columns: df_valid[c] = None

            upsert = """
                INSERT INTO dim_product (
                    product_id, is_duplicate, product_name, product_type, price, is_referred
                ) VALUES %s
                ON CONFLICT (product_id) DO UPDATE SET
                    is_duplicate = EXCLUDED.is_duplicate,
                    product_name = EXCLUDED.product_name,
                    product_type = EXCLUDED.product_type,
                    price = EXCLUDED.price,
                    is_referred = FALSE;
            """
            psycopg2.extras.execute_values(cur, upsert, df_valid[valid_cols].values.tolist(), page_size=BATCH_SIZE)

        conn.commit()
        print("Dim Product Loaded.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()