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
        raise FileNotFoundError(f"{path} not found")

def main():
    require_file(FILE)

    df = pd.read_parquet(FILE)
    df = df.where(pd.notnull(df), None)

    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_merchant (
        merchant_id varchar PRIMARY KEY,
        name varchar,
        creation_date timestamp,
        age int,
        street varchar,
        state varchar,
        city varchar,
        country varchar,
        contact_number varchar
    );
    """)
    conn.commit()

    cur.execute("TRUNCATE TABLE dim_merchant CASCADE;")
    conn.commit()

    rows = []
    for _, row in df.iterrows():
        rows.append((
            row.get('merchant_id'),
            row.get('name'),
            row.get('creation_date'),
            row.get('age'),
            row.get('street'),
            row.get('state'),
            row.get('city'),
            row.get('country'),
            row.get('contact_number')
        ))

    insert_sql = """
        INSERT INTO dim_merchant (
            merchant_id, name, creation_date, age, street, state, city, country, contact_number
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (merchant_id) DO UPDATE
        SET name = EXCLUDED.name,
            creation_date = EXCLUDED.creation_date,
            age = EXCLUDED.age,
            street = EXCLUDED.street,
            state = EXCLUDED.state,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            contact_number = EXCLUDED.contact_number;
    """

    try:
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Loaded {len(rows)} merchants into dim_merchant")
    except Exception as e:
        conn.rollback()
        print("Insert failed:", e, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()