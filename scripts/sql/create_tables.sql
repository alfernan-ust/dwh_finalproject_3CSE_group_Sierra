DROP TABLE IF EXISTS fact_campaign_transactions CASCADE;
DROP TABLE IF EXISTS fact_line_items CASCADE;
DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS dim_campaign CASCADE;
DROP TABLE IF EXISTS dim_staff CASCADE;
DROP TABLE IF EXISTS dim_merchant CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    date_key VARCHAR(10) PRIMARY KEY,
    date_actual DATE,
    
    year_actual INT,
    quarter_actual INT,
    month_actual INT,
    month_name VARCHAR(20),
    week_of_year INT,
    day_name VARCHAR(20),
    day_of_week INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR, 
    product_name VARCHAR,
    product_type VARCHAR,
    price DECIMAL(10, 2),
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    user_id VARCHAR, 
    name VARCHAR,
    creation_date TIMESTAMP,
    street VARCHAR,
    state VARCHAR,
    city VARCHAR,
    country VARCHAR,
    birthdate DATE,
    gender VARCHAR,
    device_address VARCHAR,
    user_type VARCHAR,
    job_title VARCHAR,
    job_level VARCHAR,
    credit_card_number VARCHAR,
    issuing_bank VARCHAR,
    age INT,
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

CREATE TABLE dim_merchant (
    merchant_key SERIAL PRIMARY KEY,
    merchant_id VARCHAR, 
    name VARCHAR,
    creation_date TIMESTAMP,
    age INT,
    street VARCHAR,
    state VARCHAR,
    city VARCHAR,
    country VARCHAR,
    contact_number VARCHAR,
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

CREATE TABLE dim_staff (
    staff_key SERIAL PRIMARY KEY,
    staff_id VARCHAR, 
    name VARCHAR,
    job_level VARCHAR,
    street VARCHAR,
    state VARCHAR,
    city VARCHAR,
    country VARCHAR,
    contact_number VARCHAR,
    creation_date TIMESTAMP,
    age INT,
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

CREATE TABLE dim_campaign (
    campaign_key SERIAL PRIMARY KEY,
    campaign_id VARCHAR, 
    campaign_name VARCHAR,
    campaign_description VARCHAR,
    discount DECIMAL(5, 2),
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

CREATE TABLE fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR, 
    customer_key INT REFERENCES dim_customer(customer_key),
    merchant_key INT REFERENCES dim_merchant(merchant_key),
    staff_key    INT REFERENCES dim_staff(staff_key),
    transaction_date_key VARCHAR(10) REFERENCES dim_date(date_key),
    estimated_arrival_date_key VARCHAR(10) REFERENCES dim_date(date_key),
    delay_in_days INT,
    gross_total DECIMAL(12,2),
    discount_total DECIMAL(12,2),
    net_total DECIMAL(12,2),
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

CREATE TABLE fact_line_items (
    line_item_id SERIAL PRIMARY KEY,
    order_key    INT REFERENCES fact_orders(order_key),
    product_key  INT REFERENCES dim_product(product_key),
    quantity     INT,
    gross_total  DECIMAL(12,2),
    discount_total DECIMAL(12,2),
    net_total    DECIMAL(12,2),
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

CREATE TABLE fact_campaign_transactions (
    transaction_id SERIAL PRIMARY KEY,
    order_key    INT REFERENCES fact_orders(order_key),
    campaign_key INT REFERENCES dim_campaign(campaign_key),
    availed      BOOLEAN,
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);