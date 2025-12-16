import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras

FILE = "/dataset/staff_data.parquet"
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
    print("Loading Staff Data...")
    if not require_file(FILE): return
    
    try:
        df = pd.read_parquet(FILE)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return
    
    # 1. MARK DUPLICATES
    # keep='last' marks the last occurrence as False (0), all others as True (1)
    df['is_duplicate'] = df.duplicated(subset=['staff_id'], keep='last')
    
    df['is_referred'] = False
    df = df.where(pd.notnull(df), None)

    reject_mask = df['staff_id'].isnull() | (df['staff_id'] == '')
    df_reject = df[reject_mask].copy()
    df_valid = df[~reject_mask].copy()

    # 2. DEDUPLICATE FOR UPSERT
    # We drop duplicates from the batch to prevent "ON CONFLICT" errors.
    # We keep='last' to ensure the row we insert is the one marked is_duplicate=False
    if not df_valid.empty:
        df_valid = df_valid.drop_duplicates(subset=['staff_id'], keep='last')

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    try:
        # --- Handle Rejects ---
        if not df_reject.empty:
            df_reject['rejection_reason'] = 'Missing staff_id'
            reject_cols = [
                'staff_id', 'is_duplicate', 'name', 'job_level', 'street', 
                'state', 'city', 'country', 'contact_number', 'creation_date', 
                'age', 'is_referred', 'rejection_reason'
            ]
            for c in reject_cols:
                if c not in df_reject.columns: df_reject[c] = None
            
            query_reject = f"INSERT INTO reject_dim_staff ({','.join(reject_cols)}) VALUES %s"
            psycopg2.extras.execute_values(cur, query_reject, df_reject[reject_cols].values.tolist(), page_size=BATCH_SIZE)

        # --- Handle Valid (Upsert) ---
        if not df_valid.empty:
            print(f"Upserting {len(df_valid)} valid staff rows.")
            valid_cols = [
                'staff_id', 'is_duplicate', 'name', 'job_level', 'street', 
                'state', 'city', 'country', 'contact_number', 'creation_date', 
                'age', 'is_referred'
            ]
            for c in valid_cols:
                 if c not in df_valid.columns: df_valid[c] = None

            upsert_sql = """
                INSERT INTO dim_staff (
                    staff_id, is_duplicate, name, job_level, street, state, city, 
                    country, contact_number, creation_date, age, is_referred
                ) VALUES %s
                ON CONFLICT (staff_id) DO UPDATE SET
                    is_duplicate = EXCLUDED.is_duplicate,
                    name = EXCLUDED.name,
                    job_level = EXCLUDED.job_level,
                    street = EXCLUDED.street,
                    state = EXCLUDED.state,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    contact_number = EXCLUDED.contact_number,
                    creation_date = EXCLUDED.creation_date,
                    age = EXCLUDED.age,
                    is_referred = FALSE;
            """
            psycopg2.extras.execute_values(cur, upsert_sql, df_valid[valid_cols].values.tolist(), page_size=BATCH_SIZE)

        conn.commit()
        print("Dim Staff Loaded.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()