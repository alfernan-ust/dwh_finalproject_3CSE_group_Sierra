import pandas as pd
import datetime
from datetime import date

def calculate_age(d):
    today = date.today()
    return today.year - d.year - ((today.month, today.day) < (d.month, d.day))

# Process merchant_data
df = pd.read_parquet('/dataset/extracted/merchant_data.parquet')

# Handle empty DataFrame gracefully
if df.empty:
    print("Warning: Empty merchant_data. Creating empty output.")
    pd.DataFrame().to_parquet('/dataset/transformed/merchant_data.parquet', index=False)
else:
    ages = []
    for _, r in df.iterrows():
        try:
            d = datetime.date(int(r['creation_date'][:4]), int(r['creation_date'][5:7]), int(r['creation_date'][8:10]))
            ages.append(calculate_age(d))
        except:
            ages.append(None)

    df.insert(loc=2, column='age', value=ages)
    df.to_parquet('/dataset/transformed/merchant_data.parquet', index=False)

# Process staff_data
df = pd.read_parquet('/dataset/extracted/staff_data.parquet')

# Handle empty DataFrame gracefully
if df.empty:
    print("Warning: Empty staff_data. Creating empty output.")
    pd.DataFrame().to_parquet('/dataset/transformed/staff_data.parquet', index=False)
else:
    ages = []
    for _, r in df.iterrows():
        try:
            d = datetime.date(int(r['creation_date'][:4]), int(r['creation_date'][5:7]), int(r['creation_date'][8:10]))
            ages.append(calculate_age(d))
        except:
            ages.append(None)

    df.insert(loc=9, column='age', value=ages)
    df.to_parquet('/dataset/transformed/staff_data.parquet', index=False)

print("[SUCCESS] transform_enterprise completed")
