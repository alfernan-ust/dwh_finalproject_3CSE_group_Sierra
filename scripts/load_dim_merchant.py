import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras

FILE = "/dataset/merchant_data.parquet"
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
    print("Loading Merchant Data...")
    if not require_file(FILE): return
    
    try:
        df = pd.read_parquet(FILE)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return
    
    df['is_duplicate'] = df.duplicated(subset=['merchant_id'], keep='last')
    df['is_referred'] = False
    df = df.where(pd.notnull(df), None)

    reject_mask = df['merchant_id'].isnull() | (df['merchant_id'] == '')
    df_reject = df[reject_mask].copy()
    df_valid = df[~reject_mask].copy()

    # DEDUPLICATION
    if not df_valid.empty:
        df_valid = df_valid.drop_duplicates(subset=['merchant_id'], keep='last')

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    try:
        if not df_reject.empty:
            df_reject['rejection_reason'] = 'Missing merchant_id'
            cols = ['merchant_id', 'is_duplicate', 'name', 'creation_date', 'age', 'street', 'state', 'city', 'country', 'contact_number', 'is_referred', 'rejection_reason']
            for c in cols:
                if c not in df_reject.columns: df_reject[c] = None
            
            query = f"INSERT INTO reject_dim_merchant ({','.join(cols)}) VALUES %s"
            psycopg2.extras.execute_values(cur, query, df_reject[cols].values.tolist(), page_size=BATCH_SIZE)

        if not df_valid.empty:
            cols = ['merchant_id', 'is_duplicate', 'name', 'creation_date', 'age', 'street', 'state', 'city', 'country', 'contact_number', 'is_referred']
            for c in cols:
                 if c not in df_valid.columns: df_valid[c] = None

            upsert = """
                INSERT INTO dim_merchant (
                    merchant_id, is_duplicate, name, creation_date, age, street, state, city, country, contact_number, is_referred
                ) VALUES %s
                ON CONFLICT (merchant_id) DO UPDATE SET
                    is_duplicate = EXCLUDED.is_duplicate,
                    name = EXCLUDED.name,
                    creation_date = EXCLUDED.creation_date,
                    age = EXCLUDED.age,
                    street = EXCLUDED.street,
                    state = EXCLUDED.state,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    contact_number = EXCLUDED.contact_number,
                    is_referred = FALSE;
            """
            psycopg2.extras.execute_values(cur, upsert, df_valid[cols].values.tolist(), page_size=BATCH_SIZE)

        conn.commit()
        print("Dim Merchant Loaded.")
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()