import pandas as pd

# -------------------------------
# Load all source data
# -------------------------------
orders = pd.read_parquet("/dataset/extracted/output_order_data.parquet")
delays = pd.read_parquet("/dataset/extracted/order_delays.parquet")
order_merchant = pd.read_parquet("/dataset/extracted/order_merchant_data.parquet")
line_items_prices = pd.read_parquet("/dataset/extracted/line_item_data_prices.parquet")
campaign_data = pd.read_parquet("/dataset/extracted/campaign_data.parquet")
transactional_campaign = pd.read_parquet("/dataset/extracted/transactional_campaign_data.parquet")

print("ORDERS COLUMNS:", orders.columns.tolist())
print("DELAYS COLUMNS:", delays.columns.tolist())
print("ORDER_MERCHANT COLUMNS:", order_merchant.columns.tolist())
print("CAMPAIGN COLUMNS:", campaign_data.columns.tolist())
print("TRANSACTIONAL CAMPAIGN COLUMNS:", transactional_campaign.columns.tolist())

# -------------------------------
# Calculate financial totals from line items
# -------------------------------
print(f"\n[DEBUG] Line items before processing: {len(line_items_prices)} rows")
print(f"[DEBUG] Line items columns: {line_items_prices.columns.tolist()}")
print(f"[DEBUG] Sample line items:")
print(line_items_prices.head(3).to_string())

# Clean quantity column - extract only digits from strings like "6px", "4pcs", etc.
line_items_prices['quantity'] = line_items_prices['quantity'].astype(str).str.extract(r'(\d+)', expand=False)

# Convert to numeric
line_items_prices['quantity'] = pd.to_numeric(line_items_prices['quantity'], errors='coerce')
line_items_prices['price'] = pd.to_numeric(line_items_prices['price'], errors='coerce')

print(f"\n[DEBUG] After numeric conversion:")
print(f"[DEBUG] Price range: {line_items_prices['price'].min()} to {line_items_prices['price'].max()}")
print(f"[DEBUG] Quantity range: {line_items_prices['quantity'].min()} to {line_items_prices['quantity'].max()}")
print(f"[DEBUG] Non-null prices: {line_items_prices['price'].notna().sum()}")
print(f"[DEBUG] Non-null quantities: {line_items_prices['quantity'].notna().sum()}")

# Calculate gross total per order
line_items_prices['line_total'] = line_items_prices['price'] * line_items_prices['quantity']

print(f"\n[DEBUG] After line_total calculation:")
print(f"[DEBUG] Line total range: {line_items_prices['line_total'].min()} to {line_items_prices['line_total'].max()}")
print(f"[DEBUG] Non-null line totals: {line_items_prices['line_total'].notna().sum()}")
print(f"[DEBUG] Sample with line_total:")
print(line_items_prices[['order_id', 'price', 'quantity', 'line_total']].head(3).to_string())

order_totals = line_items_prices.groupby('order_id').agg({
    'line_total': 'sum'
}).reset_index()
order_totals = order_totals.rename(columns={'line_total': 'gross_total'})

print(f"\n[DEBUG] Order totals after aggregation: {len(order_totals)} orders")
print(f"[DEBUG] Gross total range: {order_totals['gross_total'].min()} to {order_totals['gross_total'].max()}")
print(f"[DEBUG] Sample order totals:")
print(order_totals.head(3).to_string())

# -------------------------------
# Join all data together
# -------------------------------
df = orders.merge(order_merchant, on='order_id', how='left')
df = df.merge(delays, on='order_id', how='left')
df = df.merge(order_totals, on='order_id', how='left')

# Fill missing gross_total with 0 (orders with no line items)
df['gross_total'] = df['gross_total'].fillna(0)

# -------------------------------
# Normalize column names
# -------------------------------
df = df.rename(columns={
    'estimated arrival': 'estimated_arrival',
    'delay in days': 'delay_in_days'
})

# Fill NULL delay_in_days with 0
if 'delay_in_days' in df.columns:
    df['delay_in_days'] = df['delay_in_days'].fillna(0)

# -------------------------------
# Handle dates
# -------------------------------
df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

# Handle estimated_arrival: the raw value is days offset as integer
# Convert estimated_arrival to numeric (it's currently integer days offset)
df['estimated_arrival_days'] = pd.to_numeric(df['estimated_arrival'], errors='coerce')

# Calculate estimated_arrival date by adding days to transaction_date
df['estimated_arrival'] = df['transaction_date'] + pd.to_timedelta(df['estimated_arrival_days'].fillna(0), unit='D')

# Create date keys
df['transaction_date_key'] = df['transaction_date'].dt.strftime('%Y-%m-%d')
df['estimated_arrival_date_key'] = df['estimated_arrival'].dt.strftime('%Y-%m-%d')

print(f"\n[DEBUG] Date processing:")
print(f"[DEBUG] Transaction dates with NaT: {df['transaction_date'].isna().sum()}")
print(f"[DEBUG] Estimated arrival dates with NaT: {df['estimated_arrival'].isna().sum()}")
print(f"[DEBUG] Estimated arrival date keys with NULL: {df['estimated_arrival_date_key'].isna().sum()}")
print(f"[DEBUG] Sample dates:")
print(df[['order_id', 'transaction_date', 'estimated_arrival_days', 'estimated_arrival', 'estimated_arrival_date_key']].head(3).to_string())

# Clean up temporary column
df = df.drop(columns=['estimated_arrival_days'], errors='ignore')

# -------------------------------
# Calculate discount from campaigns
# -------------------------------
# Join with transactional_campaign data to get campaign info for orders
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

print(f"\n[DEBUG] Campaign discounts:")
print(f"[DEBUG] Orders with campaigns: {len(order_discounts)}")
print(f"[DEBUG] Sample order discounts:")
print(order_discounts.head(3).to_string())

# Join discount info with main dataframe
df = df.merge(order_discounts, on='order_id', how='left')

# Convert discount to numeric and fill missing with 0
df['discount'] = pd.to_numeric(df['discount'], errors='coerce').fillna(0)

# Calculate discount_total as percentage of gross_total
df['discount_total'] = (df['gross_total'] * df['discount'] / 100).fillna(0)

# Calculate net_total
df['net_total'] = df['gross_total'] - df['discount_total']

# Drop the intermediate discount column
df = df.drop(columns=['discount'], errors='ignore')

print(f"\n[DEBUG] Discount calculation:")
print(f"[DEBUG] Orders with discount > 0: {(df['discount_total'] > 0).sum()}")
print(f"[DEBUG] Discount range: {df['discount_total'].min()} to {df['discount_total'].max()}")

# -------------------------------
# Validate numeric ranges (prevent integer overflow)
# -------------------------------
# Cap delay_in_days to reasonable range (-365 to 365)
if 'delay_in_days' in df.columns:
    df['delay_in_days'] = pd.to_numeric(df['delay_in_days'], errors='coerce')
    df['delay_in_days'] = df['delay_in_days'].clip(lower=-365, upper=365)

# Validate financial totals (cap at reasonable maximum)
for col in ['gross_total', 'discount_total', 'net_total']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        # Cap at 10 billion (well within DECIMAL(12,2) range)
        df[col] = df[col].clip(upper=9999999999.99)

# -------------------------------
# Handle duplicates
# -------------------------------
df = df.sort_values(by=['order_id', 'transaction_date'])
df['is_duplicate'] = df.duplicated(subset=['order_id'], keep='last')

# -------------------------------
# Completeness checks
# -------------------------------
required = ['order_id', 'user_id', 'transaction_date_key']
df['is_incomplete'] = df[required].isnull().any(axis=1)

incomplete_reasons = []
for idx, row in df.iterrows():
    reasons = []
    if pd.isnull(row['user_id']):
        reasons.append('Missing user_id')
    if pd.isnull(row['merchant_id']):
        reasons.append('Missing merchant_id')
    if pd.isnull(row['staff_id']):
        reasons.append('Missing staff_id')
    # Note: gross_total is now always filled with 0, so no check needed
    # Orders with 0 gross_total just have no line items

    incomplete_reasons.append(', '.join(reasons) if reasons else None)

df['incomplete_reason'] = incomplete_reasons

# Update is_incomplete based on any missing critical fields
df['is_incomplete'] = df['incomplete_reason'].notna()

print(f"\n[INFO] Processed {len(df)} orders")
print(f"[INFO] Complete orders: {(~df['is_incomplete']).sum()}")
print(f"[INFO] Incomplete orders: {df['is_incomplete'].sum()}")

# Debug: Check for any extreme values
if 'delay_in_days' in df.columns:
    print(f"[DEBUG] delay_in_days range: {df['delay_in_days'].min()} to {df['delay_in_days'].max()}")
if 'gross_total' in df.columns:
    print(f"[DEBUG] gross_total range: {df['gross_total'].min()} to {df['gross_total'].max()}")

# Show sample of data
print("\n[DEBUG] Sample rows:")
print(df[['order_id', 'user_id', 'merchant_id', 'staff_id', 'gross_total', 'delay_in_days']].head(3).to_string())

df.to_parquet("/dataset/transformed/fact_orders.parquet", index=False)
