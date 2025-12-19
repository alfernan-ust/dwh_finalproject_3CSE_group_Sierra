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

# Mark all duplicates (including the one we'll keep) to track that duplicates existed
has_duplicates = df.duplicated(subset=['order_id', 'campaign_id'], keep=False)
df['is_duplicate'] = has_duplicates

required = ['order_id', 'campaign_id', 'availed']

for col in required:
    if col not in df.columns:
        df[col] = None

df[required] = df[required].replace('', None)

# Check for null values in ALL fields (excluding quality flags)
data_cols = [c for c in df.columns if c not in ['is_duplicate', 'is_incomplete', 'incomplete_reason']]
df['is_incomplete'] = df[data_cols].isnull().any(axis=1)

# Set incomplete_reason based on what's missing
incomplete_reasons = []
for idx, row in df.iterrows():
    if not row['is_incomplete']:
        incomplete_reasons.append(None)
    else:
        missing = [col for col in data_cols if pd.isnull(row[col])]
        incomplete_reasons.append(f"Missing: {', '.join(missing)}")

df['incomplete_reason'] = incomplete_reasons

df.to_parquet(
    "/dataset/transformed/fact_campaign_transactions.parquet",
    index=False
)

print("[SUCCESS] fact_campaign_transactions transformed")
