from dataframe import extract_with_archiving
import os

# Determine paths based on environment
if os.path.exists("/dataset"):
    dataset_path = "/dataset"
else:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dataset_path = os.path.join(project_root, "dataset")

print("=" * 50)
print("Extracting Marketing Department Data")
print("=" * 50)

# Extract campaign_data
print("\n1. Extracting campaign_data...")
campaign_pattern = os.path.join(dataset_path, "MarketingDepartment/campaign_data*")
campaign_output = os.path.join(dataset_path, "extracted/campaign_data.parquet")
df_campaign = extract_with_archiving(campaign_pattern, campaign_output, dataset_path)
print(f"   Rows extracted: {len(df_campaign)}")

# Extract transactional_campaign_data
print("\n2. Extracting transactional_campaign_data...")
transactional_pattern = os.path.join(dataset_path, "MarketingDepartment/transactional_campaign_data*")
transactional_output = os.path.join(dataset_path, "extracted/transactional_campaign_data.parquet")
df_transactional = extract_with_archiving(transactional_pattern, transactional_output, dataset_path)
print(f"   Rows extracted: {len(df_transactional)}")

print("\n" + "=" * 50)
print("Marketing Department extraction complete")
print("=" * 50)
