
from dataframe import set_frame, append_files
import glob

product_list = glob.glob("/dataset/BusinessDepartment/product_list*")
if not product_list:
    raise FileNotFoundError("No product_list files found in /dataset/BusinessDepartment")
product_list.sort()
df = set_frame(product_list[0])

if len(product_list) > 1:
    product_list.pop(0)
    df = append_files(df, product_list)

df.to_parquet("/dataset/product_list.parquet")
