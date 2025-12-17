import pandas as pd

p = pd.read_parquet("/dataset/extracted/line_item_data_products.parquet")
q = pd.read_parquet("/dataset/extracted/line_item_data_prices.parquet")
prod = pd.read_parquet("/dataset/transformed/dim_product.parquet")

# Normalize column names
for df_ in [p, q, prod]:
    df_.columns = (
        df_.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
    )

# Pair rows
p['idx'] = p.groupby('order_id').cumcount()
q['idx'] = q.groupby('order_id').cumcount()

df = p.merge(q, on=['order_id','idx'], how='inner')

# Numeric quantity
df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

# 🔥 REMOVE ANY EXISTING PRICE BEFORE DIM MERGE
if 'price' in df.columns:
    df = df.drop(columns=['price'])

# Merge canonical dimension price
df = df.merge(
    prod[['product_id','price']],
    on='product_id',
    how='left'
)

# Totals
df['gross_total'] = df['quantity'] * df['price']
df['discount_total'] = 0
df['net_total'] = df['gross_total']

# Duplicate logic (DO NOT DROP)
df = df.sort_values(by=['order_id','product_id'])
df['is_duplicate'] = df.duplicated(
    subset=['order_id','product_id'],
    keep='last'
)

# Incomplete logic
required = ['order_id','product_id','quantity','gross_total']
df[required] = df[required].replace('', None)

df['is_incomplete'] = df[required].isnull().any(axis=1)
df['incomplete_reason'] = None
df.loc[df['is_incomplete'], 'incomplete_reason'] = 'Missing Required Attributes'

df.to_parquet("/dataset/transformed/fact_line_items.parquet", index=False)

print("[SUCCESS] fact_line_items transformed")
