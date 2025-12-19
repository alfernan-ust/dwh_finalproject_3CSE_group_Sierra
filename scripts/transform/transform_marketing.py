import pandas as pd

# Process campaign_data
df = pd.read_parquet('/dataset/extracted/campaign_data.parquet')

# Handle empty DataFrame gracefully
if df.empty:
    print("Warning: Empty campaign_data. Creating empty output.")
    pd.DataFrame().to_parquet('/dataset/transformed/campaign_data.parquet', index=False)
else:
    df['discount'] = (
        df['discount']
        .astype(str)
        .str.replace(r'[^0-9.]', '', regex=True)
        .astype(float)
    )
    df.to_parquet('/dataset/transformed/campaign_data.parquet', index=False)

# Process transactional_campaign_data
df = pd.read_parquet('/dataset/extracted/transactional_campaign_data.parquet')
df.to_parquet('/dataset/transformed/transactional_campaign_data.parquet', index=False)

print("[SUCCESS] transform_marketing completed")
