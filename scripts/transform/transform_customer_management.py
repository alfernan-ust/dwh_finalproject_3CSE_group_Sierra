import pandas as pd
import datetime
from datetime import date

df = pd.read_parquet('/dataset/extracted/user_data.parquet')

def calculate_age(born):
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

age_list = []
for _, row in df.iterrows():
    try:
        birth = row['birthdate']
        birth = datetime.date(int(birth[:4]), int(birth[5:7]), int(birth[8:10]))
        age_list.append(calculate_age(birth))
    except:
        age_list.append(None)

df['age'] = age_list
df.to_parquet('/dataset/transformed/user_data.parquet', index=False)
