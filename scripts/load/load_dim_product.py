import pandas as pd
import psycopg2, psycopg2.extras

PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 1000

df = pd.read_parquet("/dataset/transformed/dim_product.parquet")

conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
cur = conn.cursor()

cols = [
    'product_id','product_name','product_type','price',
    'is_duplicate','is_incomplete','incomplete_reason'
]

rows = df[cols].where(pd.notnull(df), None).values.tolist()

psycopg2.extras.execute_values(
    cur,
    f"INSERT INTO dim_product ({','.join(cols)}) VALUES %s",
    rows,
    page_size=BATCH_SIZE
)

conn.commit()
cur.close()
conn.close()
