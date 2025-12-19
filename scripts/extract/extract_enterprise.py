from dataframe import extract_with_archiving
import os

# Determine paths based on environment
if os.path.exists("/dataset"):
    dataset_path = "/dataset"
else:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dataset_path = os.path.join(project_root, "dataset")

print("=" * 50)
print("Extracting Enterprise Department Data")
print("=" * 50)

# Extract order_with_merchant_data
print("\n1. Extracting order_with_merchant_data...")
order_merchant_pattern = os.path.join(dataset_path, "EnterpriseDepartment/order_with_merchant_data*")
order_merchant_output = os.path.join(dataset_path, "extracted/order_merchant_data.parquet")
df_order_merchant = extract_with_archiving(order_merchant_pattern, order_merchant_output, dataset_path)
print(f"   Rows extracted: {len(df_order_merchant)}")

# Extract merchant_data
print("\n2. Extracting merchant_data...")
merchant_pattern = os.path.join(dataset_path, "EnterpriseDepartment/merchant_data*")
merchant_output = os.path.join(dataset_path, "extracted/merchant_data.parquet")
df_merchant = extract_with_archiving(merchant_pattern, merchant_output, dataset_path)
print(f"   Rows extracted: {len(df_merchant)}")

# Extract staff_data
print("\n3. Extracting staff_data...")
staff_pattern = os.path.join(dataset_path, "EnterpriseDepartment/staff_data*")
staff_output = os.path.join(dataset_path, "extracted/staff_data.parquet")
df_staff = extract_with_archiving(staff_pattern, staff_output, dataset_path)
print(f"   Rows extracted: {len(df_staff)}")

print("\n" + "=" * 50)
print("Enterprise Department extraction complete")
print("=" * 50)
