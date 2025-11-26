from dataframe import set_frame, append_files
import pandas as pd
import pickle
import json
import glob
import os

# Line item data prices
line_item_data_prices = glob.glob("/dataset/OperationsDepartment/line_item_data_prices*")
line_item_data_prices.sort()
df = set_frame(line_item_data_prices[0])
line_item_data_prices.pop(0)
df = append_files(df, line_item_data_prices)

# removes non-numbers in 'quantity' column
df.replace(to_replace={'quantity': '[^0-9]'}, value="", inplace=True, regex=True)
df.to_parquet("/dataset/line_item_data_prices.parquet")

# Line item data products
line_item_data_products = glob.glob("/dataset/OperationsDepartment/line_item_data_products*")
line_item_data_products.sort()
df = set_frame(line_item_data_products[0])
line_item_data_products.pop(0)
df = append_files(df, line_item_data_products)

df.to_parquet("/dataset/line_item_data_products.parquet")

# Order Data
order_data = glob.glob("/dataset/OperationsDepartment/order_data*")
order_data.sort()
df = set_frame(order_data[0])
order_data.pop(0)
df = append_files(df, order_data)
ctr = 1

# Removes non/numerical data in 'estimated arrival' column
df.replace(to_replace={'estimated arrival': '[^0-9]'}, value="", inplace=True, regex=True)

df.to_parquet('/dataset/output_order_data.parquet')

# Order Delays
order_delays = glob.glob("/dataset/OperationsDepartment/order_delay*")
order_delays.sort()
df = set_frame(order_delays[0])

if len(order_delays) > 1:
    order_delays.pop(0)
    df = append_files(df, order_delays)

df.to_parquet("/dataset/order_delays.parquet")