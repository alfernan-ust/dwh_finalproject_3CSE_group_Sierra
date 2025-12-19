import pandas as pd

prices = pd.read_parquet('/dataset/extracted/line_item_data_prices.parquet')
products = pd.read_parquet('/dataset/extracted/line_item_data_products.parquet')

# Handle empty DataFrames gracefully
if prices.empty or products.empty:
    print("Warning: Empty input data. Creating empty output files.")
    pd.DataFrame().to_parquet('/dataset/transformed/order_product_list.parquet', index=False)
    pd.DataFrame().to_parquet('/dataset/transformed/order_cost.parquet', index=False)
    pd.read_parquet('/dataset/extracted/output_order_data.parquet').to_parquet('/dataset/transformed/output_order_data.parquet', index=False)
    pd.read_parquet('/dataset/extracted/order_delays.parquet').to_parquet('/dataset/transformed/order_delays.parquet', index=False)
    print("[SUCCESS] transform_operations completed (with empty data)")
    exit(0)

prices['quantity'] = prices['quantity'].astype(str).str.replace(r'[^0-9]', '', regex=True).astype(float)
prices = prices.sort_values('order_id')
products = products.sort_values('order_id')

df = prices.join(products, lsuffix='_l', rsuffix='_r')
df = df.drop(columns='order_id_r')
df.rename(columns={'order_id_l':'order_id'}, inplace=True)

agg = df.groupby(['order_id','product_id','product_name','price'], as_index=False)['quantity'].sum()
agg['total_price'] = agg['quantity'] * agg['price']

agg.to_parquet('/dataset/transformed/order_product_list.parquet', index=False)

order_cost = agg.groupby('order_id', as_index=False)['total_price'].sum()
order_cost.to_parquet('/dataset/transformed/order_cost.parquet', index=False)

orders = pd.read_parquet('/dataset/extracted/output_order_data.parquet')
orders.to_parquet('/dataset/transformed/output_order_data.parquet', index=False)

delays = pd.read_parquet('/dataset/extracted/order_delays.parquet')
delays.to_parquet('/dataset/transformed/order_delays.parquet', index=False)
