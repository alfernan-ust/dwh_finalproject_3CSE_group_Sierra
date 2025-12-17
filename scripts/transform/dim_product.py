import pandas as pd

df = pd.read_parquet("/dataset/extracted/product_list.parquet")

df['product_id'] = df['product_id'].astype(str).replace({'nan': None, '': None})
df['price'] = pd.to_numeric(
    df['price'].astype(str).str.replace(r'[^0-9.]', '', regex=True),
    errors='coerce'
)

df = df.sort_values(by=['product_id'])
df['is_duplicate'] = df.duplicated(subset=['product_id'], keep='last')

required = ['product_id', 'product_name', 'product_type', 'price']
df[required] = df[required].replace('', None)

df['is_incomplete'] = df[required].isnull().any(axis=1)
df['incomplete_reason'] = 'Missing Required Attributes'

df.to_parquet("/dataset/transformed/dim_product.parquet", index=False)
