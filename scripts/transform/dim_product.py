import pandas as pd

df = pd.read_parquet("/dataset/extracted/product_list.parquet")

# Handle empty DataFrame gracefully
if df.empty:
    print("Warning: Empty product_list data for dim_product. Creating empty output.")
    pd.DataFrame().to_parquet("/dataset/transformed/dim_product.parquet", index=False)
    print("[SUCCESS] dim_product completed (empty)")
    exit(0)

df['product_id'] = df['product_id'].astype(str).replace({'nan': None, '': None})
df['price'] = pd.to_numeric(
    df['price'].astype(str).str.replace(r'[^0-9.]', '', regex=True),
    errors='coerce'
)

df = df.sort_values(by=['product_id'])

# Mark all duplicates (including the one we'll keep) to track that duplicates existed
has_duplicates = df['product_id'].duplicated(keep=False)
df['is_duplicate'] = has_duplicates

required = ['product_id', 'product_name', 'product_type', 'price']
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

df.to_parquet("/dataset/transformed/dim_product.parquet", index=False)
print("[SUCCESS] dim_product completed")
