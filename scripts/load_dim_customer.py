import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras

# --- CONFIG ---
USER_FILE = "/dataset/user_data.parquet"
CREDIT_FILE = "/dataset/credit_card.parquet"
JOB_FILE = "/dataset/user_job.parquet"

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
    print("Loading Customer Data...")
    if not all(require_file(f) for f in [USER_FILE, CREDIT_FILE, JOB_FILE]):
        return

    # 1. Load and Merge
    try:
        df_users = pd.read_parquet(USER_FILE)
        df_credit = pd.read_parquet(CREDIT_FILE)
        df_jobs = pd.read_parquet(JOB_FILE)
    except Exception as e:
        print(f"Error reading Parquet files: {e}", file=sys.stderr)
        return

    df = df_users.merge(df_credit, on="user_id", how="left")
    df = df.merge(df_jobs, on="user_id", how="left")

    # 2. Transform
    # Duplicate Logic: keep='last' marks the last occurrence as False (0), all others as True (1)
    df['is_duplicate'] = df.duplicated(subset=['user_id'], keep='last')

    for col in ['creation_date', 'birthdate']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    df['is_referred'] = False
    df = df.where(pd.notnull(df), None)

    # 3. Reject vs Valid Split
    reject_mask = df['user_id'].isnull() | (df['user_id'] == '')
    df_reject = df[reject_mask].copy()
    df_valid = df[~reject_mask].copy()

    # 4. DEDUPLICATE FOR UPSERT
    # We drop duplicates from the batch to prevent "ON CONFLICT" errors.
    # We keep='last' to ensure the row we insert is the one marked is_duplicate=False
    if not df_valid.empty:
        df_valid = df_valid.drop_duplicates(subset=['user_id'], keep='last')

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    try:
        # --- Handle Rejects ---
        if not df_reject.empty:
            print(f"Rejecting {len(df_reject)} rows.")
            df_reject['rejection_reason'] = 'Missing user_id'
            
            cols = [
                'user_id', 'is_duplicate', 'name', 'creation_date', 'street', 
                'state', 'city', 'country', 'birthdate', 'gender', 'device_address', 
                'user_type', 'job_title', 'job_level', 'credit_card_number', 
                'issuing_bank', 'age', 'is_referred', 'rejection_reason'
            ]
            for c in cols:
                if c not in df_reject.columns: df_reject[c] = None

            query = f"INSERT INTO reject_dim_customer ({','.join(cols)}) VALUES %s"
            psycopg2.extras.execute_values(cur, query, df_reject[cols].values.tolist(), page_size=BATCH_SIZE)

        # --- Handle Valid (Upsert) ---
        if not df_valid.empty:
            print(f"Upserting {len(df_valid)} valid rows.")
            
            cols = [
                'user_id', 'is_duplicate', 'name', 'creation_date', 'street', 
                'state', 'city', 'country', 'birthdate', 'gender', 'device_address', 
                'user_type', 'job_title', 'job_level', 'credit_card_number', 
                'issuing_bank', 'age', 'is_referred'
            ]
            for c in cols:
                if c not in df_valid.columns: df_valid[c] = None

            upsert_sql = """
                INSERT INTO dim_customer (
                    user_id, is_duplicate, name, creation_date, street, state, city, country,
                    birthdate, gender, device_address, user_type, job_title, job_level,
                    credit_card_number, issuing_bank, age, is_referred
                ) VALUES %s
                ON CONFLICT (user_id) DO UPDATE SET
                    is_duplicate = EXCLUDED.is_duplicate,
                    name = EXCLUDED.name,
                    creation_date = EXCLUDED.creation_date,
                    street = EXCLUDED.street,
                    state = EXCLUDED.state,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country,
                    birthdate = EXCLUDED.birthdate,
                    gender = EXCLUDED.gender,
                    device_address = EXCLUDED.device_address,
                    user_type = EXCLUDED.user_type,
                    job_title = EXCLUDED.job_title,
                    job_level = EXCLUDED.job_level,
                    credit_card_number = EXCLUDED.credit_card_number,
                    issuing_bank = EXCLUDED.issuing_bank,
                    age = EXCLUDED.age,
                    is_referred = FALSE;
            """
            psycopg2.extras.execute_values(cur, upsert_sql, df_valid[cols].values.tolist(), page_size=BATCH_SIZE)

        conn.commit()
        print("Dim Customer Loaded.")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()