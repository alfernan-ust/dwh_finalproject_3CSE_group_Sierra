from dataframe import set_frame, append_files
import pandas as pd
import pyarrow as pa
import lxml
import html5lib
from bs4 import BeautifulSoup
import os
import glob
import datetime
from datetime import date

def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

order_merchant_data = glob.glob("/dataset/EnterpriseDepartment/order_with_merchant_data*")
order_merchant_data.sort()
df = set_frame(order_merchant_data[0])
order_merchant_data.pop(0)
df = append_files(df, order_merchant_data)
df.to_parquet("/dataset/order_merchant_data.parquet")

# Merchant Data
merchant_data = glob.glob("/dataset/EnterpriseDepartment/merchant_data*")
merchant_data.sort()
df = set_frame(merchant_data[0])
if len(merchant_data) > 1:
    merchant_data.pop(0)
    df = append_files(df, merchant_data)
    
age_list = []
for index, row in df.iterrows():
    date_raw = row['creation_date']
    creation_date = datetime.date(int(date_raw[:4]), int(date_raw[5:7]), int(date_raw[8:10]))
    age_list.append(calculate_age(creation_date))
    
df.insert(loc=2, column='age', value=age_list)
    
df.to_parquet("/dataset/merchant_data.parquet")

# Staff Data
staff_data = glob.glob("/dataset/EnterpriseDepartment/staff_data*")
staff_data.sort()
df = set_frame(staff_data[0])
if len(staff_data) > 1:
    staff_data.pop(0)
    df = append_files(df, staff_data)
    
age_list = []
for index, row in df.iterrows():
    date_raw = row['creation_date']
    creation_date = datetime.date(int(date_raw[:4]), int(date_raw[5:7]), int(date_raw[8:10]))
    age_list.append(calculate_age(creation_date))
    
df.insert(loc=9, column='age', value=age_list)
    
df.to_parquet("/dataset/staff_data.parquet")