from dataframe import set_frame, append_files
import glob

campaign_data = glob.glob('/dataset/MarketingDepartment/campaign_data*')
df = set_frame(campaign_data[0])
if len(campaign_data) > 1:
    campaign_data.pop(0)
    df = append_files(df, campaign_data)
df.to_parquet('/dataset/extracted/campaign_data.parquet')

transactional_campaign_data = glob.glob('/dataset/MarketingDepartment/transactional_campaign_data*')
df = set_frame(transactional_campaign_data[0])
if len(transactional_campaign_data) > 1:
    transactional_campaign_data.pop(0)
    df = append_files(df, transactional_campaign_data)
df.to_parquet('/dataset/extracted/transactional_campaign_data.parquet')
