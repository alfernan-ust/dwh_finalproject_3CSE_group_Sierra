import pandas as pd
from datetime import date

df = pd.read_parquet("/dataset/extracted/staff_data.parquet")

# Handle empty DataFrame gracefully
if df.empty:
    print("Warning: Empty staff_data for dim_staff. Creating empty output.")
    pd.DataFrame().to_parquet("/dataset/transformed/dim_staff.parquet", index=False)
    print("[SUCCESS] dim_staff completed (empty)")
    exit(0)

df['staff_id'] = df['staff_id'].astype(str).replace({'nan': None, '': None})
df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')

def calc_age(d):
    if pd.isna(d): return None
    today = date.today()
    return today.year - d.year - ((today.month, today.day) < (d.month, d.day))

df['age'] = df['creation_date'].apply(calc_age)

df = df.sort_values(by='creation_date')

# Mark all duplicates (including the one we'll keep) to track that duplicates existed
has_duplicates = df['staff_id'].duplicated(keep=False)
df['is_duplicate'] = has_duplicates

required = [
    'staff_id','name','job_level','street','state','city',
    'country','contact_number','creation_date','age'
]
for c in required:
    if c not in df.columns:
        df[c] = None

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

df.to_parquet("/dataset/transformed/dim_staff.parquet", index=False)
print("[SUCCESS] dim_staff completed")
