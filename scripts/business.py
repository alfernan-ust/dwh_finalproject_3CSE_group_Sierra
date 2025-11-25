from dataframe import set_frame, append_files
import pandas as pd
import glob
import pickle
import os

product_list = glob.glob("/dataset/BusinessDepartment/product_list*")
product_list.sort()
df = set_frame(product_list[0])

if len(product_list) > 1:
    product_list.pop(0)
    df = append_files(df, product_list)

df.to_parquet("product_list.parquet")