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

# Mark all duplicates (including the one we'll keep) to track that duplicates existed
has_duplicates = df['user_id'].duplicated(keep=False)
df['is_duplicate'] = has_duplicates

required = [
    'user_id','name','creation_date','street','state','city','country',
    'birthdate','gender','age'
]
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

df.to_parquet("/dataset/transformed/dim_customer.parquet", index=False)
print("[SUCCESS] dim_customer completed")
