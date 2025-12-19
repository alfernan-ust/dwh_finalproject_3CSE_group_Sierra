import pandas as pd

orders = pd.read_parquet("/dataset/extracted/output_order_data.parquet")
delays = pd.read_parquet("/dataset/extracted/order_delays.parquet")
order_merchant = pd.read_parquet("/dataset/extracted/order_merchant_data.parquet")
line_items_prices = pd.read_parquet("/dataset/extracted/line_item_data_prices.parquet")
campaign_data = pd.read_parquet("/dataset/extracted/campaign_data.parquet")
transactional_campaign = pd.read_parquet("/dataset/extracted/transactional_campaign_data.parquet")

# Handle empty DataFrames gracefully
if orders.empty or line_items_prices.empty:
    print("Warning: Empty input data for fact_orders. Creating empty output.")
    pd.DataFrame().to_parquet("/dataset/transformed/fact_orders.parquet", index=False)
    print("[SUCCESS] fact_orders completed (empty)")
    exit(0)

print("ORDERS COLUMNS:", orders.columns.tolist())
print("DELAYS COLUMNS:", delays.columns.tolist())
print("ORDER_MERCHANT COLUMNS:", order_merchant.columns.tolist())
print("CAMPAIGN COLUMNS:", campaign_data.columns.tolist())
print("TRANSACTIONAL CAMPAIGN COLUMNS:", transactional_campaign.columns.tolist())

print(f"\n[DEBUG] Line items before processing: {len(line_items_prices)} rows")
print(f"[DEBUG] Line items columns: {line_items_prices.columns.tolist()}")
print(f"[DEBUG] Sample line items:")
print(line_items_prices.head(3).to_string())

line_items_prices['quantity'] = line_items_prices['quantity'].astype(str).str.extract(r'(\d+)', expand=False)

line_items_prices['quantity'] = pd.to_numeric(line_items_prices['quantity'], errors='coerce')
line_items_prices['price'] = pd.to_numeric(line_items_prices['price'], errors='coerce')

print(f"\n[DEBUG] After numeric conversion:")
print(f"[DEBUG] Price range: {line_items_prices['price'].min()} to {line_items_prices['price'].max()}")
print(f"[DEBUG] Quantity range: {line_items_prices['quantity'].min()} to {line_items_prices['quantity'].max()}")
print(f"[DEBUG] Non-null prices: {line_items_prices['price'].notna().sum()}")
print(f"[DEBUG] Non-null quantities: {line_items_prices['quantity'].notna().sum()}")

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

df = orders.merge(order_merchant, on='order_id', how='left')
df = df.merge(delays, on='order_id', how='left')
df = df.merge(order_totals, on='order_id', how='left')

df['gross_total'] = df['gross_total'].fillna(0)

df = df.rename(columns={
    'estimated arrival': 'estimated_arrival',
    'delay in days': 'delay_in_days'
})

if 'delay_in_days' in df.columns:
    df['delay_in_days'] = df['delay_in_days'].fillna(0)

df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

print(f"\n[DEBUG] Columns after merge and rename: {df.columns.tolist()}")
print(f"[DEBUG] 'estimated_arrival' in columns: {'estimated_arrival' in df.columns}")
if 'estimated_arrival' in df.columns:
    print(f"[DEBUG] estimated_arrival null count: {df['estimated_arrival'].isna().sum()}/{len(df)}")
    print(f"[DEBUG] Sample estimated_arrival values: {df['estimated_arrival'].head(10).tolist()}")

def parse_estimated_arrival(row):
    if 'estimated_arrival' not in row.index:
        return pd.NaT

    est_arrival = row['estimated_arrival']
    transaction_date = row['transaction_date']

    if pd.isna(est_arrival) or pd.isna(transaction_date):
        return pd.NaT

    est_arrival_str = str(est_arrival).strip()

    try:
        days = float(est_arrival_str)
        return transaction_date + pd.Timedelta(days=days)
    except (ValueError, TypeError):
        pass

    import re
    match = re.search(r'(\d+)', est_arrival_str)
    if match:
        try:
            days = float(match.group(1))
            return transaction_date + pd.Timedelta(days=days)
        except (ValueError, TypeError):
            pass

    try:
        return pd.to_datetime(est_arrival_str, errors='raise')
    except:
        pass

    return pd.NaT

if 'estimated_arrival' in df.columns:
    print("[DEBUG] Processing estimated_arrival column...")
    df['estimated_arrival'] = df.apply(parse_estimated_arrival, axis=1)
else:
    print("[WARNING] 'estimated_arrival' column not found! Setting all to NaT")
    df['estimated_arrival'] = pd.NaT

df['transaction_date_key'] = df['transaction_date'].dt.strftime('%Y-%m-%d')
df['estimated_arrival_date_key'] = df['estimated_arrival'].dt.strftime('%Y-%m-%d')

print(f"\n[DEBUG] Date processing:")
print(f"[DEBUG] Transaction dates with NaT: {df['transaction_date'].isna().sum()}")
print(f"[DEBUG] Estimated arrival dates with NaT: {df['estimated_arrival'].isna().sum()}")
print(f"[DEBUG] Estimated arrival date keys with NULL: {df['estimated_arrival_date_key'].isna().sum()}")
print(f"[DEBUG] Sample dates:")
print(df[['order_id', 'transaction_date', 'estimated_arrival', 'estimated_arrival_date_key']].head(3).to_string())

df = df.drop(columns=['estimated_arrival_days'], errors='ignore')

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

print(f"\n[DEBUG] Campaign discounts:")
print(f"[DEBUG] Orders with campaigns: {len(order_discounts)}")
print(f"[DEBUG] Sample order discounts:")
print(order_discounts.head(3).to_string())

df = df.merge(order_discounts, on='order_id', how='left')

df['discount'] = pd.to_numeric(df['discount'], errors='coerce').fillna(0)

df['discount_total'] = (df['gross_total'] * df['discount'] / 100).fillna(0)

df['net_total'] = df['gross_total'] - df['discount_total']

df = df.drop(columns=['discount'], errors='ignore')

print(f"\n[DEBUG] Discount calculation:")
print(f"[DEBUG] Orders with discount > 0: {(df['discount_total'] > 0).sum()}")
print(f"[DEBUG] Discount range: {df['discount_total'].min()} to {df['discount_total'].max()}")

if 'delay_in_days' in df.columns:
    df['delay_in_days'] = pd.to_numeric(df['delay_in_days'], errors='coerce')
    df['delay_in_days'] = df['delay_in_days'].clip(lower=-365, upper=365)

for col in ['gross_total', 'discount_total', 'net_total']:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].clip(upper=9999999999.99)

df = df.sort_values(by=['order_id', 'transaction_date'])
df['is_duplicate'] = df.duplicated(subset=['order_id'], keep='last')

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

    incomplete_reasons.append(', '.join(reasons) if reasons else None)

df['incomplete_reason'] = incomplete_reasons

df['is_incomplete'] = df['incomplete_reason'].notna()

print(f"\n[INFO] Processed {len(df)} orders")
print(f"[INFO] Complete orders: {(~df['is_incomplete']).sum()}")
print(f"[INFO] Incomplete orders: {df['is_incomplete'].sum()}")

if 'delay_in_days' in df.columns:
    print(f"[DEBUG] delay_in_days range: {df['delay_in_days'].min()} to {df['delay_in_days'].max()}")
if 'gross_total' in df.columns:
    print(f"[DEBUG] gross_total range: {df['gross_total'].min()} to {df['gross_total'].max()}")

print("\n[DEBUG] Sample rows:")
print(df[['order_id', 'user_id', 'merchant_id', 'staff_id', 'gross_total', 'delay_in_days']].head(3).to_string())

df.to_parquet("/dataset/transformed/fact_orders.parquet", index=False)
