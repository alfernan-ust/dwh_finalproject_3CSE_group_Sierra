import pandas as pd
import psycopg2, psycopg2.extras

PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 1000

df = pd.read_parquet("/dataset/transformed/fact_campaign_transactions.parquet")

# Handle empty DataFrame gracefully
if df.empty:
    print("[SUCCESS] fact_campaign_transactions loaded (0 rows - empty input)")
    exit(0)

conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
cur = conn.cursor()

cur.execute("SELECT campaign_id FROM dim_campaign")
existing = set(r[0] for r in cur.fetchall())

missing = df.loc[~df['campaign_id'].isin(existing), 'campaign_id'].dropna().unique()

if len(missing):
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO dim_campaign (
            campaign_id,is_duplicate,is_inferred,is_incomplete,incomplete_reason
        ) VALUES %s
        """,
        [(x, False, True, True, 'Inferred by Fact') for x in missing]
    )

conn.commit()

cur.execute("SELECT order_id, order_key FROM fact_orders")
order_map = dict(cur.fetchall())

cur.execute("SELECT campaign_id, campaign_key FROM dim_campaign")
camp_map = dict(cur.fetchall())

df['order_key'] = df['order_id'].map(order_map)
df['campaign_key'] = df['campaign_id'].map(camp_map)

cols = [
    'order_key','campaign_key','availed',
    'is_duplicate','is_incomplete','incomplete_reason'
]

rows = df[cols].where(pd.notnull(df), None).values.tolist()

psycopg2.extras.execute_values(
    cur,
    f"INSERT INTO fact_campaign_transactions ({','.join(cols)}) VALUES %s",
    rows,
    page_size=BATCH_SIZE
)

conn.commit()
cur.close()
conn.close()
