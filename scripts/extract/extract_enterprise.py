from dataframe import set_frame, append_files
import glob
import os

if os.path.exists("/dataset"):
    dataset_path = "/dataset"
else:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dataset_path = os.path.join(project_root, "dataset")

order_merchant_data = glob.glob(os.path.join(dataset_path, "EnterpriseDepartment/order_with_merchant_data*"))
order_merchant_data.sort()
df = set_frame(order_merchant_data[0])
order_merchant_data.pop(0)
df = append_files(df, order_merchant_data)

df.to_parquet(os.path.join(dataset_path, "extracted/order_merchant_data.parquet"))

merchant_data = glob.glob(os.path.join(dataset_path, "EnterpriseDepartment/merchant_data*"))
merchant_data.sort()
df = set_frame(merchant_data[0])
if len(merchant_data) > 1:
    merchant_data.pop(0)
    df = append_files(df, merchant_data)

df.to_parquet(os.path.join(dataset_path, "extracted/merchant_data.parquet"))

staff_data = glob.glob(os.path.join(dataset_path, "EnterpriseDepartment/staff_data*"))
staff_data.sort()
df = set_frame(staff_data[0])
if len(staff_data) > 1:
    staff_data.pop(0)
    df = append_files(df, staff_data)

df.to_parquet(os.path.join(dataset_path, "extracted/staff_data.parquet"))
