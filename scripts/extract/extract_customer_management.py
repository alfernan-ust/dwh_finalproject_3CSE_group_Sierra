from dataframe import extract_with_archiving
import os

# Determine paths based on environment
if os.path.exists("/dataset"):
    dataset_path = "/dataset"
else:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dataset_path = os.path.join(project_root, "dataset")

print("=" * 50)
print("Extracting Customer Management Department Data")
print("=" * 50)

# Extract user_credit_card
print("\n1. Extracting user_credit_card...")
credit_card_pattern = os.path.join(dataset_path, "CustomerManagementDepartment/user_credit_card*")
credit_card_output = os.path.join(dataset_path, "extracted/credit_card.parquet")
df_credit_card = extract_with_archiving(credit_card_pattern, credit_card_output, dataset_path)
print(f"   Rows extracted: {len(df_credit_card)}")

# Extract user_data
print("\n2. Extracting user_data...")
user_data_pattern = os.path.join(dataset_path, "CustomerManagementDepartment/user_data*")
user_data_output = os.path.join(dataset_path, "extracted/user_data.parquet")
df_user_data = extract_with_archiving(user_data_pattern, user_data_output, dataset_path)
print(f"   Rows extracted: {len(df_user_data)}")

# Extract user_job
print("\n3. Extracting user_job...")
user_job_pattern = os.path.join(dataset_path, "CustomerManagementDepartment/user_job*")
user_job_output = os.path.join(dataset_path, "extracted/user_job.parquet")
df_user_job = extract_with_archiving(user_job_pattern, user_job_output, dataset_path)
print(f"   Rows extracted: {len(df_user_job)}")

print("\n" + "=" * 50)
print("Customer Management Department extraction complete")
print("=" * 50)
