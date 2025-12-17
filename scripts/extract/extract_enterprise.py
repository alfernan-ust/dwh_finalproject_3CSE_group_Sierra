from dataframe import set_frame, append_files
import glob

# Order–Merchant
order_merchant_data = glob.glob("/dataset/EnterpriseDepartment/order_with_merchant_data*")
order_merchant_data.sort()
df = set_frame(order_merchant_data[0])
order_merchant_data.pop(0)
df = append_files(df, order_merchant_data)

df.to_parquet("/dataset/extracted/order_merchant_data.parquet")

# Merchant Data
merchant_data = glob.glob("/dataset/EnterpriseDepartment/merchant_data*")
merchant_data.sort()
df = set_frame(merchant_data[0])
if len(merchant_data) > 1:
    merchant_data.pop(0)
    df = append_files(df, merchant_data)

df.to_parquet("/dataset/extracted/merchant_data.parquet")

# Staff Data
staff_data = glob.glob("/dataset/EnterpriseDepartment/staff_data*")
staff_data.sort()
df = set_frame(staff_data[0])
if len(staff_data) > 1:
    staff_data.pop(0)
    df = append_files(df, staff_data)

df.to_parquet("/dataset/extracted/staff_data.parquet")
