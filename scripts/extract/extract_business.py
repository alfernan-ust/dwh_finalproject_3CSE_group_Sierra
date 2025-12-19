from dataframe import extract_with_archiving
import os

# Determine paths based on environment
if os.path.exists("/dataset"):
    dataset_path = "/dataset"
    workspace_path = "/opt/kestra/workspace"
else:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dataset_path = os.path.join(project_root, "dataset")
    workspace_path = os.path.join(project_root, "scripts")

# Extract product_list files with archiving
print("=" * 50)
print("Extracting Business Department: product_list")
print("=" * 50)

product_list_pattern = os.path.join(dataset_path, "Business Department/product_list*")
product_list_output = os.path.join(dataset_path, "extracted/product_list.parquet")

df = extract_with_archiving(product_list_pattern, product_list_output, dataset_path)

print(f"\nExtraction complete. Total rows: {len(df)}")
print("=" * 50)
