import pandas as pd

df = pd.read_parquet('/dataset/extracted/campaign_data.parquet')
df['discount'] = (
    df['discount']
    .astype(str)
    .str.replace(r'[^0-9.]', '', regex=True)
    .astype(float)
)

df.to_parquet('/dataset/transformed/campaign_data.parquet', index=False)

df = pd.read_parquet('/dataset/extracted/transactional_campaign_data.parquet')
df.to_parquet('/dataset/transformed/transactional_campaign_data.parquet', index=False)
