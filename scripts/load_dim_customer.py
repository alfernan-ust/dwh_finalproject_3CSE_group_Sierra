import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras

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
        raise FileNotFoundError(f"{path} not found")

def main():
    for f in [USER_FILE, CREDIT_FILE, JOB_FILE]:
        require_file(f)

    df_users = pd.read_parquet(USER_FILE)
    df_credit = pd.read_parquet(CREDIT_FILE)
    df_jobs = pd.read_parquet(JOB_FILE)

    df = df_users.merge(df_credit, on="user_id", how="left")
    df = df.merge(df_jobs, on="user_id", how="left")

    # Duplicate Logic
    df['is_duplicate'] = df.duplicated(subset=['user_id'], keep='last')

    # Date Cleaning
    for col in ['creation_date', 'birthdate']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    df = df.where(pd.notnull(df), None)

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS dim_customer CASCADE;")
    conn.commit()

    cur.execute("""
    CREATE TABLE dim_customer (
        customer_key SERIAL PRIMARY KEY, 
        user_id varchar,                 
        is_duplicate boolean,
        name varchar,
        creation_date timestamp,
        street varchar,
        state varchar,
        city varchar,
        country varchar,
        birthdate date,
        gender varchar,
        device_address varchar,
        user_type varchar,
        job_title varchar,
        job_level varchar,
        credit_card_number varchar,
        issuing_bank varchar,
        age int
    );
    """)
    conn.commit()

    rows = []
    for _, row in df.iterrows():
        rows.append((
            row.get('user_id'),
            row.get('is_duplicate'),
            row.get('name'),
            row.get('creation_date'),
            row.get('street'),
            row.get('state'),
            row.get('city'),
            row.get('country'),
            row.get('birthdate'),
            row.get('gender'),
            row.get('device_address'),
            row.get('user_type'),
            row.get('job_title'),
            row.get('job_level'),
            row.get('credit_card_number'),
            row.get('issuing_bank'),
            row.get('age')
        ))

    insert_sql = """
        INSERT INTO dim_customer (
            user_id, is_duplicate, name, creation_date, street, state, city, country,
            birthdate, gender, device_address, user_type, job_title, job_level,
            credit_card_number, issuing_bank, age
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Loaded {len(rows)} customers into dim_customer")
    except Exception as e:
        conn.rollback()
        print("Insert failed:", e, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()