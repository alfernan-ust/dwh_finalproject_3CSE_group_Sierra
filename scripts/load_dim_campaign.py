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

def require_file(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")

def main():
    require_file(FILE)

    df = pd.read_parquet(FILE)
    df = df.where(pd.notnull(df), None)

    # Duplicate Logic    
    df['is_duplicate'] = df.duplicated(subset=['campaign_id'], keep='last')

    if 'discount' in df.columns:
        df['discount'] = pd.to_numeric(
            df['discount'].astype(str).str.replace(r'[^0-9.]', '', regex=True),
            errors='coerce'
        )

    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS dim_campaign CASCADE;")
    conn.commit()

    cur.execute("""
    CREATE TABLE dim_campaign (
        campaign_key SERIAL PRIMARY KEY,
        campaign_id varchar,
        is_duplicate boolean,
        campaign_name varchar,
        campaign_description varchar,
        discount decimal(5,2)
    );
    """)
    conn.commit()

    rows = []
    for _, row in df.iterrows():
        rows.append((
            row.get('campaign_id'),
            row.get('is_duplicate'),
            row.get('campaign_name'),
            row.get('campaign_description'),
            row.get('discount')
        ))

    insert_sql = """
        INSERT INTO dim_campaign (
            campaign_id, is_duplicate, campaign_name, campaign_description, discount
        )
        VALUES (%s, %s, %s, %s, %s)
    """

    try:
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Loaded {len(rows)} campaigns into dim_campaign")
    except Exception as e:
        conn.rollback()
        print("Insert failed:", e, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()