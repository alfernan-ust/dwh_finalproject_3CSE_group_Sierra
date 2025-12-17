import pandas as pd
from datetime import date

df = pd.read_parquet("/dataset/extracted/merchant_data.parquet")

df['merchant_id'] = df['merchant_id'].astype(str).replace({'nan': None, '': None})
df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')

def calc_age(d):
    if pd.isna(d): return None
    today = date.today()
    return today.year - d.year - ((today.month, today.day) < (d.month, d.day))

df['age'] = df['creation_date'].apply(calc_age)

df = df.sort_values(by='creation_date')

df['is_duplicate'] = df.duplicated(subset=['merchant_id'], keep='last')

required = [
    'merchant_id','name','creation_date','age',
    'street','state','city','country','contact_number'
]
for c in required:
    if c not in df.columns:
        df[c] = None

missing_attr = df[required].isnull().any(axis=1)
missing_id = df['merchant_id'].isnull()

df['is_incomplete'] = missing_attr | missing_id
df['incomplete_reason'] = None
df.loc[missing_id, 'incomplete_reason'] = 'Missing ID'
df.loc[missing_attr & ~missing_id, 'incomplete_reason'] = 'Missing Attributes'

df.to_parquet("/dataset/transformed/dim_merchant.parquet", index=False)

