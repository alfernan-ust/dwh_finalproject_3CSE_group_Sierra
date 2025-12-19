from dataframe import set_frame, append_files
import glob
import os

if os.path.exists("/dataset"):
    dataset_path = "/dataset"
    workspace_path = "/opt/kestra/workspace"
else:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dataset_path = os.path.join(project_root, "dataset")
    workspace_path = os.path.join(project_root, "scripts")

product_list = glob.glob(os.path.join(dataset_path, "Business Department/product_list*"))
if not product_list:
    raise FileNotFoundError("No product_list files found in Business Department")

product_list.sort()
df = set_frame(product_list[0])

if len(product_list) > 1:
    product_list.pop(0)
    df = append_files(df, product_list)

df.to_parquet(os.path.join(dataset_path, "extracted/product_list.parquet"), index=False)
