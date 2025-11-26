import pandas as pd
import psycopg2
import os

file_path = "/dataset/merchant_data.parquet"
if not os.path.exists(file_path):
    raise FileNotFoundError(f"{file_path} not found")

df = pd.read_parquet(file_path)
df = df.where(pd.notnull(df), None)

conn = psycopg2.connect(host="postgres", database="kestra", user="kestra", password="k3str4")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_id varchar PRIMARY KEY,
    name varchar,
    creation_date timestamp,
    street varchar,
    state varchar,
    city varchar,
    country varchar,
    contact_number varchar
);
""")
conn.commit()

for _, row in df.iterrows():
    cur.execute("""
        INSERT INTO dim_merchant (merchant_id, name, creation_date, street, state, city, country, contact_number)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (merchant_id) DO UPDATE
        SET name = EXCLUDED.name,
            creation_date = EXCLUDED.creation_date,
            street = EXCLUDED.street,
            state = EXCLUDED.state,
            city = EXCLUDED.city,
            country = EXCLUDED.country,
            contact_number = EXCLUDED.contact_number;
    """, (row['merchant_id'], row['name'], row['creation_date'], row['street'],
          row['state'], row['city'], row['country'], row['contact_number']))

conn.commit()
cur.close()
conn.close()
print(f"Loaded {len(df)} merchants into dim_merchant")
