import pandas as pd

orders = pd.read_parquet("/dataset/extracted/output_order_data.parquet")
delays = pd.read_parquet("/dataset/extracted/order_delays.parquet")
print("ORDERS COLUMNS:", orders.columns.tolist())
print("DELAYS COLUMNS:", delays.columns.tolist())

df = orders.merge(delays, on='order_id', how='left')
# -------------------------------
# Normalize column names
# -------------------------------
df = df.rename(columns={
    'estimated arrival': 'estimated_arrival',
    'delay in days': 'estimated_arrival_days'
})

df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')

# --- ETA HANDLING ---
# Case 1: estimated_arrival is date-like
df['estimated_arrival'] = pd.to_datetime(df.get('estimated_arrival'), errors='coerce')

# Case 2: estimated_arrival is integer (days offset)
mask = df['estimated_arrival'].isnull() & df.get('estimated_arrival_days', pd.Series()).notnull()
df.loc[mask, 'estimated_arrival'] = (
    df.loc[mask, 'transaction_date'] +
    pd.to_timedelta(df.loc[mask, 'estimated_arrival_days'], unit='D')
)

df['transaction_date_key'] = df['transaction_date'].dt.strftime('%Y-%m-%d')
df['estimated_arrival_date_key'] = df['estimated_arrival'].dt.strftime('%Y-%m-%d')

df = df.sort_values(by=['order_id', 'transaction_date'])
df['is_duplicate'] = df.duplicated(subset=['order_id'], keep='last')

required = ['order_id','transaction_date_key']
df[required] = df[required].replace('', None)

df['is_incomplete'] = df[required].isnull().any(axis=1)
df['incomplete_reason'] = 'Missing Required Attributes'

df.to_parquet("/dataset/transformed/fact_orders.parquet", index=False)
