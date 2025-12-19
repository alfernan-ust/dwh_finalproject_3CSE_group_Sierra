import pandas as pd

df = pd.read_parquet("/dataset/extracted/campaign_data.parquet")

# Handle empty DataFrame gracefully
if df.empty:
    print("Warning: Empty campaign_data for dim_campaign. Creating empty output.")
    pd.DataFrame().to_parquet("/dataset/transformed/dim_campaign.parquet", index=False)
    print("[SUCCESS] dim_campaign completed (empty)")
    exit(0)

df['campaign_id'] = df['campaign_id'].astype(str).replace({'nan': None, '': None})
df['discount'] = pd.to_numeric(
    df['discount'].astype(str).str.replace(r'[^0-9.]','',regex=True),
    errors='coerce'
)

df = df.sort_values(by=['campaign_id'])

# Mark all duplicates (including the one we'll keep) to track that duplicates existed
has_duplicates = df['campaign_id'].duplicated(keep=False)
df['is_duplicate'] = has_duplicates

required = ['campaign_id','campaign_name','campaign_description','discount']
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

# All records from source data are not inferred (only inferred when created by fact load)
df['is_inferred'] = False

df.to_parquet("/dataset/transformed/dim_campaign.parquet", index=False)
print("[SUCCESS] dim_campaign completed")
