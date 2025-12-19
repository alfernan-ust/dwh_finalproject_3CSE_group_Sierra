from dataframe import extract_with_archiving
import os

# Determine paths based on environment
if os.path.exists("/dataset"):
    dataset_path = "/dataset"
else:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dataset_path = os.path.join(project_root, "dataset")

print("=" * 50)
print("Extracting Operations Department Data")
print("=" * 50)

# Extract line_item_data_prices
print("\n1. Extracting line_item_data_prices...")
prices_pattern = os.path.join(dataset_path, "OperationsDepartment/line_item_data_prices*")
prices_output = os.path.join(dataset_path, "extracted/line_item_data_prices.parquet")
df_prices = extract_with_archiving(prices_pattern, prices_output, dataset_path)
print(f"   Rows extracted: {len(df_prices)}")

# Extract line_item_data_products
print("\n2. Extracting line_item_data_products...")
products_pattern = os.path.join(dataset_path, "OperationsDepartment/line_item_data_products*")
products_output = os.path.join(dataset_path, "extracted/line_item_data_products.parquet")
df_products = extract_with_archiving(products_pattern, products_output, dataset_path)
print(f"   Rows extracted: {len(df_products)}")

# Extract order_data
print("\n3. Extracting order_data...")
orders_pattern = os.path.join(dataset_path, "OperationsDepartment/order_data*")
orders_output = os.path.join(dataset_path, "extracted/output_order_data.parquet")
df_orders = extract_with_archiving(orders_pattern, orders_output, dataset_path)
print(f"   Rows extracted: {len(df_orders)}")

# Extract order_delay
print("\n4. Extracting order_delay...")
delays_pattern = os.path.join(dataset_path, "OperationsDepartment/order_delay*")
delays_output = os.path.join(dataset_path, "extracted/order_delays.parquet")
df_delays = extract_with_archiving(delays_pattern, delays_output, dataset_path)
print(f"   Rows extracted: {len(df_delays)}")

print("\n" + "=" * 50)
print("Operations Department extraction complete")
print("=" * 50)
