from dataframe import set_frame, append_files
import pandas as pd
import glob
import pickle
import os

credit_card = glob.glob('/dataset/Customer Management Department/user_credit_card*')
df = set_frame(credit_card[0])

if len(credit_card) > 1:
  credit_card.pop(0)
  df = append_files(df, credit_card)

df.to_parquet('credit_card.parquet')

user_data = glob.glob('/dataset/Customer Management Department/user_data*')
df = set_frame(user_data[0])

if len(user_data) > 1:
  user_data.pop(0)
  df = append_files(df, user_data)

df.to_parquet('user_data.parquet')

user_job = glob.glob('/dataset/Customer Management Department/user_job*')
df = set_frame(user_job[0])

if len(user_job) > 1:
  user_job.pop(0)
  df = append_files(df, user_job)

df.to_parquet('user_job.parquet')