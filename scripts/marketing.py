from dataframe import set_frame, append_files
import pandas as pd
import glob
import os

campaign_data = glob.glob('/dataset/MarketingDepartment/campaign_data*')
with open(campaign_data[0], 'r') as file:
    filedata = file.read()
# Replace the target string
filedata = filedata.replace('\t', ',')
# Write the file out again
with open(campaign_data[0], 'w') as file:  
    file.write(filedata)
    
df = set_frame(campaign_data[0])
df.to_parquet('/dataset/campaign_data.parquet')

if len(campaign_data) > 1:
    campaign_data.pop(0)
df = append_files(df, campaign_data)
df.replace(to_replace={'discount': '[^0-9]'}, value="", inplace=True, regex=True)

transactional_campaign_data = glob.glob('/dataset/MarketingDepartment/transactional_campaign_data*')
df = set_frame(transactional_campaign_data[0])
if len(transactional_campaign_data) > 1:
    transactional_campaign_data.pop(0)
    df = append_files(df, transactional_campaign_data)
df.to_parquet('/dataset/transactional_campaign_data.parquet')