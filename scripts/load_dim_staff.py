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
        raise FileNotFoundError(f"{path} not found")

def main():
    require_file(FILE)
    df = pd.read_parquet(FILE)
    
    # Duplicate Logic
    df['is_duplicate'] = df.duplicated(subset=['staff_id'], keep='last')
    df = df.where(pd.notnull(df), None)

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS dim_staff CASCADE;")
    conn.commit()

    cur.execute("""
    CREATE TABLE dim_staff (
        staff_key SERIAL PRIMARY KEY, 
        staff_id varchar,             
        is_duplicate boolean,
        name varchar,
        job_level varchar,
        street varchar,
        state varchar,
        city varchar,
        country varchar,
        contact_number varchar,
        creation_date timestamp,
        age int
    );
    """)
    conn.commit()

    rows = []
    for _, row in df.iterrows():
        rows.append((
            row.get('staff_id'),
            row.get('is_duplicate'),
            row.get('name'),
            row.get('job_level'),
            row.get('street'),
            row.get('state'),
            row.get('city'),
            row.get('country'),
            row.get('contact_number'),
            row.get('creation_date'),
            row.get('age')
        ))

    insert_sql = """
        INSERT INTO dim_staff (
            staff_id, is_duplicate, name, job_level, street, state, city, country, contact_number, creation_date, age
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Loaded {len(rows)} staff into dim_staff")
    except Exception as e:
        conn.rollback()
        print("Insert failed:", e, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()