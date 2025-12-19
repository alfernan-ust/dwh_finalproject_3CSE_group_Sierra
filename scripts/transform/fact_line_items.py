import pandas as pd

p = pd.read_parquet("/dataset/extracted/line_item_data_products.parquet")
q = pd.read_parquet("/dataset/extracted/line_item_data_prices.parquet")
prod = pd.read_parquet("/dataset/transformed/dim_product.parquet")
campaign_data = pd.read_parquet("/dataset/extracted/campaign_data.parquet")
transactional_campaign = pd.read_parquet("/dataset/extracted/transactional_campaign_data.parquet")

# Handle empty DataFrames gracefully
if p.empty or q.empty:
    print("Warning: Empty input data for fact_line_items. Creating empty output.")
    pd.DataFrame().to_parquet("/dataset/transformed/fact_line_items.parquet", index=False)
    print("[SUCCESS] fact_line_items completed (empty)")
    exit(0)

for df_ in [p, q, prod]:
    df_.columns = (
        df_.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
    )

p['idx'] = p.groupby('order_id').cumcount()
q['idx'] = q.groupby('order_id').cumcount()

df = p.merge(q, on=['order_id','idx'], how='inner')

df['quantity'] = df['quantity'].astype(str).str.extract(r'(\d+)', expand=False)

df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

if 'price' in df.columns:
    df = df.drop(columns=['price'])

df = df.merge(
    prod[['product_id','price']],
    on='product_id',
    how='left'
)

df['gross_total'] = df['quantity'] * df['price']

for df_ in [campaign_data, transactional_campaign]:
    df_.columns = (
        df_.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
    )

availed_campaigns = transactional_campaign[transactional_campaign['availed'] == 1].copy()

availed_campaigns = availed_campaigns.merge(
    campaign_data[['campaign_id', 'discount']],
    on='campaign_id',
    how='left'
)

availed_campaigns['discount'] = availed_campaigns['discount'].astype(str).str.extract(r'(\d+)', expand=False)
availed_campaigns['discount'] = pd.to_numeric(availed_campaigns['discount'], errors='coerce')

order_discounts = availed_campaigns.groupby('order_id').agg({
    'discount': 'max'
}).reset_index()

df = df.merge(order_discounts, on='order_id', how='left')

df['discount'] = pd.to_numeric(df['discount'], errors='coerce').fillna(0)

df['discount_total'] = (df['gross_total'] * df['discount'] / 100).fillna(0)

df['net_total'] = df['gross_total'] - df['discount_total']

df = df.drop(columns=['discount'], errors='ignore')

df = df.sort_values(by=['order_id','product_id'])

# Mark all duplicates (including the one we'll keep) to track that duplicates existed
has_duplicates = df.duplicated(subset=['order_id','product_id'], keep=False)
df['is_duplicate'] = has_duplicates

required = ['order_id','product_id','quantity','gross_total']
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

print(f"\n[INFO] Transformed {len(df)} line items")
print(f"[INFO] Complete line items: {(~df['is_incomplete']).sum()}")
print(f"[INFO] Incomplete line items: {df['is_incomplete'].sum()}")
print(f"[DEBUG] Quantity range: {df['quantity'].min()} to {df['quantity'].max()}")
print(f"[DEBUG] Gross total range: {df['gross_total'].min()} to {df['gross_total'].max()}")
print(f"[DEBUG] Sample line items:")
print(df[['order_id', 'product_id', 'quantity', 'price', 'gross_total']].head(3).to_string())

df.to_parquet("/dataset/transformed/fact_line_items.parquet", index=False)

print("\n[SUCCESS] fact_line_items transformed")
