from dataframe import set_frame, append_files
import glob
import os

if os.path.exists("/dataset"):
    dataset_path = "/dataset"
else:
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    dataset_path = os.path.join(project_root, "dataset")

campaign_data = glob.glob(os.path.join(dataset_path, 'MarketingDepartment/campaign_data*'))
df = set_frame(campaign_data[0])
if len(campaign_data) > 1:
    campaign_data.pop(0)
    df = append_files(df, campaign_data)
df.to_parquet(os.path.join(dataset_path, 'extracted/campaign_data.parquet'))

transactional_campaign_data = glob.glob(os.path.join(dataset_path, 'MarketingDepartment/transactional_campaign_data*'))
df = set_frame(transactional_campaign_data[0])
if len(transactional_campaign_data) > 1:
    transactional_campaign_data.pop(0)
    df = append_files(df, transactional_campaign_data)
df.to_parquet(os.path.join(dataset_path, 'extracted/transactional_campaign_data.parquet'))
