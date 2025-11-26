import pandas as pd
import psycopg2
import os

# --- File paths ---
user_file = "/dataset/user_data.parquet"
credit_file = "/dataset/credit_card.parquet"
job_file = "/dataset/user_job.parquet"

for f in [user_file, credit_file, job_file]:
    if not os.path.exists(f):
        raise FileNotFoundError(f"{f} not found")

df_users = pd.read_parquet(user_file)
df_credit = pd.read_parquet(credit_file)
df_jobs = pd.read_parquet(job_file)

df = df_users.merge(df_credit, on="user_id", how="left")
df = df.merge(df_jobs, on="user_id", how="left")

# Convert dates
for col in ['creation_date', 'birthdate']:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

df = df.where(pd.notnull(df), None)

conn = psycopg2.connect(
    host="postgres",
    database="kestra",
    user="kestra",
    password="k3str4"
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS dim_customer (
    user_id varchar PRIMARY KEY,
    name varchar,
    creation_date timestamp,
    street varchar,
    state varchar,
    city varchar,
    country varchar,
    birthdate date,
    gender varchar,
    device_address varchar,
    user_type varchar,
    job_title varchar,
    job_level varchar,
    credit_card_number varchar,
    issuing_bank varchar
);
""")
conn.commit()

for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO dim_customer (
            user_id, name, creation_date, street, state, city, country,
            birthdate, gender, device_address, user_type, job_title, job_level,
            credit_card_number, issuing_bank
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE
        SET name = EXCLUDED.name,
            creation_date = EXCLUDED.creation_date,
            street = EXCLUDED.street,
            state = EXCLUDED.state,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            birthdate = EXCLUDED.birthdate,
            gender = EXCLUDED.gender,
            device_address = EXCLUDED.device_address,
            user_type = EXCLUDED.user_type,
            job_title = EXCLUDED.job_title,
            job_level = EXCLUDED.job_level,
            credit_card_number = EXCLUDED.credit_card_number,
            issuing_bank = EXCLUDED.issuing_bank;
    """, (
        row.get('user_id'), row.get('name'), row.get('creation_date'), row.get('street'),
        row.get('state'), row.get('city'), row.get('country'), row.get('birthdate'),
        row.get('gender'), row.get('device_address'), row.get('user_type'),
        row.get('job_title'), row.get('job_level'), row.get('credit_card_number'),
        row.get('issuing_bank')
    ))

conn.commit()
cur.close()
conn.close()
print(f"Loaded {len(df)} customers into dim_customer")
