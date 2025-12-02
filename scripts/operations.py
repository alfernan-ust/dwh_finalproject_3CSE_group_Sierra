from dataframe import set_frame, append_files
import pandas as pd
import pickle
import json
import glob
import os
import datetime

def isWeekend(d):
    # d is a Date object
    day = int(d.strftime('%w'))
    if day==0 or day==6:
        return 1
    else:
        return 0

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

# order_product_list
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

# order_cost
total_price_list = []
for index, row in agg_df.iterrows():
    total_price_list.append(row['quantity'] * row['price'])
agg_df.insert(loc=5, column='total_price', value=total_price_list)

order_cost_df = agg_df.groupby(['order_id']).agg({
    'total_price': 'sum'
}).reset_index()

order_cost_df.to_parquet('order_cost.parquet')

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

year_list = []
quarter_list = []
month_list = []
month_name_list = []
day_list = []
weekday_list = []
weekday_name_list = []
is_weekend_list = []
for index, row in df.iterrows():
    date_row = row['transaction_date']
    d = datetime.date(int(date_row[:4]), int(date_row[5:7]), int(date_row[8:]))
    year_list.append(d.year)
    quarter_list.append( d.month//4 + 1 )
    month_list.append(d.month)
    month_name_list.append( d.strftime('%B') )
    day_list.append(d.day)
    weekday_list.append( d.strftime('%w') ) # 0 = sunday, 6 = saturday
    weekday_name_list.append( d.strftime('%A') )
    is_weekend_list.append( isWeekend(d) )
new_columns = {'year':year_list,
              'quarter':quarter_list,
              'month':month_list,
              'month_name_list':month_name_list,
                'day':day_list,
              'weekday':weekday_list,
              'weekday_name_list':weekday_name_list,
              'is_weekend':is_weekend_list}
df = df.assign(**new_columns)
df.to_parquet('output_order_data.parquet')

# Order Delays
order_delays = glob.glob("/dataset/Operations Department/order_delay*")
order_delays.sort()
df = set_frame(order_delays[0])

if len(order_delays) > 1:
    order_delays.pop(0)
    df = append_files(df, order_delays)

df.to_parquet("order_delays.parquet")
