from dataframe import set_frame, append_files
import pandas as pd
import pickle
import json
import glob
import os

# <-- LEGACY DATA
            # # Line item data prices
            # line_item_data_prices = glob.glob("/dataset/Operations Department/line_item_data_prices*")
            # line_item_data_prices.sort()
            # df = set_frame(line_item_data_prices[0])
            # line_item_data_prices.pop(0)
            # df = append_files(df, line_item_data_prices)

            # # removes non-numbers in 'quantity' column
            # df.replace(to_replace={'quantity': '[^0-9]'}, value="", inplace=True, regex=True)
            # df.to_parquet("line_item_data_prices.parquet")

            # # Line item data products
            # line_item_data_products = glob.glob("/dataset/Operations Department/line_item_data_products*")
            # line_item_data_products.sort()
            # df = set_frame(line_item_data_products[0])
            # line_item_data_products.pop(0)
            # df = append_files(df, line_item_data_products)

            # df.to_parquet("line_item_data_products.parquet")
# -->            

line_item_data_prices = glob.glob("/dataset/Operations Department/line_item_data_prices*")
line_item_data_prices.sort()
df = set_frame(line_item_data_prices[0])
line_item_data_prices.pop(0)
df = append_files(df, line_item_data_prices)

# removes non-numbers in 'quantity' column, convert to integer
df.replace(to_replace={'quantity': '[^0-9]'}, value="", inplace=True, regex=True)
df.sort_values(by='order_id', inplace=True, ignore_index=True)
df['quantity'] = pd.to_numeric(df['quantity'], downcast='integer', errors='coerce')

line_item_data_products = glob.glob("/dataset/Operations Department/line_item_data_products*")
line_item_data_products.sort()
df2 = set_frame(line_item_data_products[0])
line_item_data_products.pop(0)
df2 = append_files(df2, line_item_data_products)
df2.sort_values(by='order_id', inplace=True, ignore_index=True)

new_df = df.join(df2, lsuffix='_caller', rsuffix='_other')
new_df = new_df.drop(columns='order_id_other')

agg_df = new_df.groupby(['order_id_caller', 'product_id', 'product_name', 'price']).agg({
    'quantity': 'sum'    
}).reset_index()

agg_df.rename(columns={"order_id_caller":"order_id"}, inplace=True)
agg_df.sort_values(by='order_id', inplace=True, ignore_index=True)

agg_df.to_parquet('order_product_list.parquet') # Aggregated line_item_data_prices and line_item_data_prodcuts

# Order Data
order_data = glob.glob("/dataset/Operations Department/order_data*")
order_data.sort()
df = set_frame(order_data[0])
order_data.pop(0)
df = append_files(df, order_data)
ctr = 1

# Removes non/numerical data in 'estimated arrival' column
df.replace(to_replace={'estimated arrival': '[^0-9]'}, value="", inplace=True, regex=True)

df.to_parquet('output_order_data.parquet')

# Order Delays
order_delays = glob.glob("/dataset/Operations Department/order_delay*")
order_delays.sort()
df = set_frame(order_delays[0])

if len(order_delays) > 1:
    order_delays.pop(0)
    df = append_files(df, order_delays)

df.to_parquet("order_delays.parquet")
