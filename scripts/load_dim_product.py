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
        raise FileNotFoundError(f"{path} not found")

def main():
    require_file(FILE)
    df = pd.read_parquet(FILE)
    
    # Duplicate Logic
    df['is_duplicate'] = df.duplicated(subset=['product_id'], keep='last')
    df = df.where(pd.notnull(df), None)

    conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS dim_product CASCADE;")
    conn.commit()

    cur.execute("""
    CREATE TABLE dim_product (
        product_key SERIAL PRIMARY KEY, -- Surrogate Key
        product_id varchar,             -- Business Key
        is_duplicate boolean,
        product_name varchar,
        product_type varchar,
        price decimal(10,2)
    );
    """)
    conn.commit()

    rows = []
    for _, row in df.iterrows():
        rows.append((
            row.get('product_id'),
            row.get('is_duplicate'),
            row.get('product_name'),
            row.get('product_type'),
            row.get('price')
        ))

    insert_sql = """
        INSERT INTO dim_product (product_id, is_duplicate, product_name, product_type, price)
        VALUES (%s, %s, %s, %s, %s)
    """

    try:
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Loaded {len(rows)} products into dim_product")
    except Exception as e:
        conn.rollback()
        print("Insert failed:", e, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()