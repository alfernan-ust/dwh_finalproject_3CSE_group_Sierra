-- Tables will persist across ETL runs; data will be appended, not replaced

CREATE TABLE IF NOT EXISTS dim_date (
    date_key VARCHAR(10) PRIMARY KEY UNIQUE,
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

CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR UNIQUE,
    product_name VARCHAR,
    product_type VARCHAR,
    price DECIMAL(10, 2),
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
    user_id VARCHAR UNIQUE,
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

CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_key SERIAL PRIMARY KEY,
    merchant_id VARCHAR UNIQUE,
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

CREATE TABLE IF NOT EXISTS dim_staff (
    staff_key SERIAL PRIMARY KEY,
    staff_id VARCHAR UNIQUE,
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

CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_key SERIAL PRIMARY KEY,
    campaign_id VARCHAR UNIQUE,
    campaign_name VARCHAR,
    campaign_description VARCHAR,
    discount DECIMAL(5, 2),
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR UNIQUE,
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

CREATE TABLE IF NOT EXISTS fact_line_items (
    line_item_id SERIAL PRIMARY KEY,
    order_key    INT REFERENCES fact_orders(order_key),
    product_key  INT REFERENCES dim_product(product_key),
    quantity     INT,
    gross_total  DECIMAL(12,2),
    discount_total DECIMAL(12,2),
    net_total    DECIMAL(12,2),
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT,
    UNIQUE(order_key, product_key)
);

CREATE TABLE IF NOT EXISTS fact_campaign_transactions (
    transaction_id SERIAL PRIMARY KEY,
    order_key    INT REFERENCES fact_orders(order_key),
    campaign_key INT REFERENCES dim_campaign(campaign_key),
    availed      BOOLEAN,
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT,
    UNIQUE(order_key, campaign_key)
);