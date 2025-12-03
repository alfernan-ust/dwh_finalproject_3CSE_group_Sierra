#!/usr/bin/env python3
import os
import sys
import pandas as pd
import psycopg2
import psycopg2.extras

FILE = "/dataset/transactional_campaign_data.parquet"

PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 200

def require_file(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found")

def safe_bool(v):
    if v is None:
        return True
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)

    s = str(v).strip().lower()
    if s in ("true", "t", "1", "yes", "y"):
        return True
    if s in ("false", "f", "0", "no", "n"):
        return False

    return True

def main():

    require_file(FILE)

    df = pd.read_parquet(FILE)
    df = df.where(pd.notnull(df), None)

    if "availed" in df.columns:
        df["availed"] = df["availed"].apply(safe_bool)
    else:
        df["availed"] = True

    print("=== SAMPLE (first 10 rows) ===")
    print(df[["order_id", "campaign_id", "availed"]].head(10).to_string(index=False))

    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_campaign_transactions (
        transaction_id serial PRIMARY KEY,
        order_id varchar REFERENCES fact_orders(order_id),
        campaign_id varchar REFERENCES dim_campaign(campaign_id),
        availed boolean
    );
    """)
    conn.commit()

    rows = []
    for _, r in df.iterrows():
        rows.append((
            r.get("order_id"),
            r.get("campaign_id"),
            r.get("availed")
        ))

    insert_sql = """
        INSERT INTO fact_campaign_transactions (
            order_id, campaign_id, availed
        ) VALUES (%s, %s, %s)
    """

    try:
        psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=BATCH_SIZE)
        conn.commit()
        print(f"Loaded {len(rows)} rows into fact_campaign_transactions")
    except Exception as e:
        conn.rollback()
        print("Insert failed:", e, file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
