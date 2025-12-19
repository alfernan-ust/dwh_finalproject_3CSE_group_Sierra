import pandas as pd
import psycopg2, psycopg2.extras

PG_HOST = "postgres"
PG_DB = "kestra"
PG_USER = "kestra"
PG_PASS = "k3str4"
BATCH_SIZE = 1000

df = pd.read_parquet("/dataset/transformed/dim_campaign.parquet")

# Handle empty DataFrame gracefully
if df.empty:
    print("[SUCCESS] dim_campaign loaded (0 rows - empty input)")
    exit(0)

# Deduplicate by campaign_id, keeping the last occurrence
original_count = len(df)
df = df.drop_duplicates(subset=['campaign_id'], keep='last')
if len(df) < original_count:
    print(f"[INFO] Removed {original_count - len(df)} duplicate campaign_id values")

conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
cur = conn.cursor()

cols = [
    'campaign_id','campaign_name','campaign_description','discount',
    'is_duplicate','is_inferred','is_incomplete','incomplete_reason'
]

rows = df[cols].where(pd.notnull(df), None).values.tolist()

# Upsert: update existing records or insert new ones
update_cols = [c for c in cols if c != 'campaign_id']
update_set = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_cols])

psycopg2.extras.execute_values(
    cur,
    f"""
    INSERT INTO dim_campaign ({','.join(cols)})
    VALUES %s
    ON CONFLICT (campaign_id) DO UPDATE SET {update_set}
    """,
    rows,
    page_size=BATCH_SIZE
)

conn.commit()
cur.close()
conn.close()

print(f"[SUCCESS] dim_campaign loaded ({len(rows)} rows)")
