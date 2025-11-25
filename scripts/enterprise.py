from dataframe import set_frame, append_files
import pandas as pd
import pyarrow as pa
import lxml
import html5lib
from bs4 import BeautifulSoup
import os
import glob

order_merchant_data = glob.glob("/dataset/Enterprise Department/order_with_merchant_data*")
order_merchant_data.sort()
df = set_frame(order_merchant_data[0])
order_merchant_data.pop(0)
df = append_files(df, order_merchant_data)
df.to_parquet("order_merchant_data.parquet")

merchant_data = glob.glob("/dataset/Enterprise Department/merchant_data*")
merchant_data.sort()
df = set_frame(merchant_data[0])
if len(merchant_data) > 1:
    merchant_data.pop(0)
    df = append_files(df, merchant_data)
df.to_parquet("merchant_data.parquet")

staff_data = glob.glob("/dataset/Enterprise Department/staff_data*")
staff_data.sort()
df = set_frame(staff_data[0])
if len(staff_data) > 1:
    staff_data.pop(0)
    df = append_files(df, staff_data)
df.to_parquet("staff_data.parquet")