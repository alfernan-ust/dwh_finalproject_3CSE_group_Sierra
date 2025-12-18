from dataframe import set_frame, append_files
import glob
        
from dataframe import set_frame, append_files
import glob
import os

if os.path.exists("/dataset"):
    dataset_path = "/dataset"
else:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dataset_path = os.path.join(project_root, "dataset")

prices = glob.glob(os.path.join(dataset_path, "OperationsDepartment/line_item_data_prices*"))
prices.sort()
df = set_frame(prices[0])
prices.pop(0)
df = append_files(df, prices)

df.to_parquet(os.path.join(dataset_path, 'extracted/line_item_data_prices.parquet'))

products = glob.glob(os.path.join(dataset_path, "OperationsDepartment/line_item_data_products*"))
products.sort()
df = set_frame(products[0])
products.pop(0)
df = append_files(df, products)

df.to_parquet(os.path.join(dataset_path, 'extracted/line_item_data_products.parquet'))

orders = glob.glob(os.path.join(dataset_path, "OperationsDepartment/order_data*"))
orders.sort()
df = set_frame(orders[0])
orders.pop(0)
df = append_files(df, orders)

df.to_parquet(os.path.join(dataset_path, 'extracted/output_order_data.parquet'))

delays = glob.glob(os.path.join(dataset_path, "OperationsDepartment/order_delay*"))
delays.sort()
df = set_frame(delays[0])
if len(delays) > 1:
    delays.pop(0)
    df = append_files(df, delays)

df.to_parquet(os.path.join(dataset_path, "extracted/order_delays.parquet"), index=False)
