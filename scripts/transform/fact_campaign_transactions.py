import pandas as pd

df = pd.read_parquet("/dataset/extracted/transactional_campaign_data.parquet")

# Handle empty DataFrame gracefully
if df.empty:
    print("Warning: Empty input data for fact_campaign_transactions. Creating empty output.")
    pd.DataFrame().to_parquet("/dataset/transformed/fact_campaign_transactions.parquet", index=False)
    print("[SUCCESS] fact_campaign_transactions completed (empty)")
    exit(0)

df['order_id'] = df.get('order_id').astype(str).replace({'nan': None, '': None})
df['campaign_id'] = df.get('campaign_id').astype(str).replace({'nan': None, '': None})

def parse_bool(x):
    if pd.isna(x) or x == '':
        return None
    return str(x).lower() in ['true', '1', 'yes', 't']

if 'availed' in df.columns:
    df['availed'] = df['availed'].apply(parse_bool)
else:
    df['availed'] = None

df['is_duplicate'] = df.duplicated(
    subset=['order_id', 'campaign_id'],
    keep='last'
)

required = ['order_id', 'campaign_id', 'availed']

for col in required:
    if col not in df.columns:
        df[col] = None

df[required] = df[required].replace('', None)

df['is_incomplete'] = df[required].isnull().any(axis=1)
df['incomplete_reason'] = None
df.loc[df['is_incomplete'], 'incomplete_reason'] = 'Missing Required Attributes'

df.to_parquet(
    "/dataset/transformed/fact_campaign_transactions.parquet",
    index=False
)

print("[SUCCESS] fact_campaign_transactions transformed")
