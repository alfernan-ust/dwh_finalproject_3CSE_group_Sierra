import pandas as pd
from datetime import date

u = pd.read_parquet("/dataset/extracted/user_data.parquet")
c = pd.read_parquet("/dataset/extracted/credit_card.parquet")
j = pd.read_parquet("/dataset/extracted/user_job.parquet")

# Handle empty DataFrames gracefully
if u.empty or c.empty or j.empty:
    print("Warning: Empty input data for dim_customer. Creating empty output.")
    pd.DataFrame().to_parquet("/dataset/transformed/dim_customer.parquet", index=False)
    print("[SUCCESS] dim_customer completed (empty)")
    exit(0)

df = u.merge(c, on="user_id", how="left").merge(j, on="user_id", how="left")

df['user_id'] = df['user_id'].astype(str).replace({'nan': None, '': None})
df['birthdate'] = pd.to_datetime(df['birthdate'], errors='coerce')
df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')

def age(b):
    if pd.isna(b): return None
    t = date.today()
    return t.year - b.year - ((t.month, t.day) < (b.month, b.day))

df['age'] = df['birthdate'].apply(age)

df = df.sort_values(by=['user_id', 'creation_date'])
df['is_duplicate'] = df.duplicated(subset=['user_id'], keep='last')

required = [
    'user_id','name','creation_date','street','state','city','country',
    'birthdate','gender','age'
]
df[required] = df[required].replace('', None)

df['is_incomplete'] = df[required].isnull().any(axis=1)
df['incomplete_reason'] = 'Missing Required Attributes'

df.to_parquet("/dataset/transformed/dim_customer.parquet", index=False)
print("[SUCCESS] dim_customer completed")
