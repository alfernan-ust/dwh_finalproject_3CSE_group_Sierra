import pandas as pd
import psycopg2
import os

file_path = "/dataset/staff_data.parquet"
if not os.path.exists(file_path):
    raise FileNotFoundError(f"{file_path} not found")

df = pd.read_parquet(file_path)
df = df.where(pd.notnull(df), None)

conn = psycopg2.connect(host="postgres", database="kestra", user="kestra", password="k3str4")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS dim_staff (
    staff_id varchar PRIMARY KEY,
    name varchar,
    job_level varchar,
    street varchar,
    state varchar,
    city varchar,
    country varchar,
    contact_number varchar,
    creation_date timestamp
);
""")
conn.commit()

for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO dim_staff (staff_id, name, job_level, street, state, city, country, contact_number, creation_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (staff_id) DO UPDATE
        SET name = EXCLUDED.name,
            job_level = EXCLUDED.job_level,
            street = EXCLUDED.street,
            state = EXCLUDED.state,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            contact_number = EXCLUDED.contact_number,
            creation_date = EXCLUDED.creation_date;
    """, (row['staff_id'], row['name'], row['job_level'], row['street'], row['state'],
          row['city'], row['country'], row['contact_number'], row['creation_date']))

conn.commit()
cur.close()
conn.close()
print(f"Loaded {len(df)} staff into dim_staff")
