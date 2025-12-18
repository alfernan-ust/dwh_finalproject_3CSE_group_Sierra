import pandas as pd

p = pd.read_parquet("/dataset/extracted/line_item_data_products.parquet")
q = pd.read_parquet("/dataset/extracted/line_item_data_prices.parquet")
prod = pd.read_parquet("/dataset/transformed/dim_product.parquet")
campaign_data = pd.read_parquet("/dataset/extracted/campaign_data.parquet")
transactional_campaign = pd.read_parquet("/dataset/extracted/transactional_campaign_data.parquet")

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

# Clean quantity column - extract only digits from strings like "6px", "4pcs", etc.
df['quantity'] = df['quantity'].astype(str).str.extract(r'(\d+)', expand=False)

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

# Calculate gross total
df['gross_total'] = df['quantity'] * df['price']

# -------------------------------
# Calculate discount from campaigns (same logic as fact_orders)
# -------------------------------
# Normalize column names for campaign data
for df_ in [campaign_data, transactional_campaign]:
    df_.columns = (
        df_.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
    )

# Filter for availed campaigns only
availed_campaigns = transactional_campaign[transactional_campaign['availed'] == 1].copy()

# Join with campaign_data to get discount percentage
availed_campaigns = availed_campaigns.merge(
    campaign_data[['campaign_id', 'discount']],
    on='campaign_id',
    how='left'
)

# Clean discount column - extract only digits from strings like "1pct", "20pct", "10%%"
availed_campaigns['discount'] = availed_campaigns['discount'].astype(str).str.extract(r'(\d+)', expand=False)
availed_campaigns['discount'] = pd.to_numeric(availed_campaigns['discount'], errors='coerce')

# For orders with multiple campaigns, take the maximum discount
order_discounts = availed_campaigns.groupby('order_id').agg({
    'discount': 'max'
}).reset_index()

# Join discount info with line items
df = df.merge(order_discounts, on='order_id', how='left')

# Convert discount to numeric and fill missing with 0
df['discount'] = pd.to_numeric(df['discount'], errors='coerce').fillna(0)

# Calculate discount_total as percentage of gross_total
df['discount_total'] = (df['gross_total'] * df['discount'] / 100).fillna(0)

# Calculate net_total
df['net_total'] = df['gross_total'] - df['discount_total']

# Drop the intermediate discount column
df = df.drop(columns=['discount'], errors='ignore')

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

print(f"\n[INFO] Transformed {len(df)} line items")
print(f"[INFO] Complete line items: {(~df['is_incomplete']).sum()}")
print(f"[INFO] Incomplete line items: {df['is_incomplete'].sum()}")
print(f"[DEBUG] Quantity range: {df['quantity'].min()} to {df['quantity'].max()}")
print(f"[DEBUG] Gross total range: {df['gross_total'].min()} to {df['gross_total'].max()}")
print(f"[DEBUG] Sample line items:")
print(df[['order_id', 'product_id', 'quantity', 'price', 'gross_total']].head(3).to_string())

df.to_parquet("/dataset/transformed/fact_line_items.parquet", index=False)

print("\n[SUCCESS] fact_line_items transformed")
