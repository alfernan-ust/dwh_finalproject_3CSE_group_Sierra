from dataframe import set_frame, append_files
import glob

# Credit card
credit_card_files = glob.glob('/dataset/CustomerManagementDepartment/user_credit_card*')
if not credit_card_files:
    raise FileNotFoundError("No credit_card files found in /dataset/CustomerManagementDepartment")
df = set_frame(credit_card_files[0])

if len(credit_card_files) > 1:
    credit_card_files.pop(0)
    df = append_files(df, credit_card_files)

df.to_parquet('/dataset/credit_card.parquet')

# User data
user_data_files = glob.glob('/dataset/CustomerManagementDepartment/user_data*')
if not user_data_files:
    raise FileNotFoundError("No user_data files found in /dataset/CustomerManagementDepartment")
df = set_frame(user_data_files[0])

if len(user_data_files) > 1:
    user_data_files.pop(0)
    df = append_files(df, user_data_files)

df.to_parquet('/dataset/user_data.parquet')

# User job
user_job_files = glob.glob('/dataset/CustomerManagementDepartment/user_job*')
if not user_job_files:
    raise FileNotFoundError("No user_job files found in /dataset/CustomerManagementDepartment")
df = set_frame(user_job_files[0])

if len(user_job_files) > 1:
    user_job_files.pop(0)
    df = append_files(df, user_job_files)

df.to_parquet('/dataset/user_job.parquet')
