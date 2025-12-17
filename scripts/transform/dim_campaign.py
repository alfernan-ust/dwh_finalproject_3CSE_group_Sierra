import pandas as pd

df = pd.read_parquet("/dataset/extracted/campaign_data.parquet")

df['campaign_id'] = df['campaign_id'].astype(str).replace({'nan': None, '': None})
df['discount'] = pd.to_numeric(
    df['discount'].astype(str).str.replace(r'[^0-9.]','',regex=True),
    errors='coerce'
)

df = df.sort_values(by=['campaign_id'])
df['is_duplicate'] = df.duplicated(subset=['campaign_id'], keep='last')

required = ['campaign_id','campaign_name','campaign_description','discount']
df[required] = df[required].replace('', None)

df['is_incomplete'] = df[required].isnull().any(axis=1)
df['incomplete_reason'] = 'Missing Required Attributes'

df.to_parquet("/dataset/transformed/dim_campaign.parquet", index=False)
