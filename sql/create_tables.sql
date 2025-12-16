-- =============================================================
-- SHOPZADA DATA WAREHOUSE SCHEMA
-- =============================================================
-- Features:
-- 1. Surrogate Keys (_key) used for internal joins.
-- 2. Natural Keys (_id) retained for ETL/Upsert logic.
-- 3. is_referred: Boolean flag for late-arriving/inferred dimensions.
-- 4. is_duplicate: Boolean flag for data quality auditing.
-- 5. Reject Tables: Shadow tables for every entity to catch invalid rows.
-- =============================================================

-- =============================================================
-- 1. DIMENSION: DATE
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key VARCHAR(10) PRIMARY KEY, -- YYYY-MM-DD
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(50),
    day INT,
    weekday INT,
    weekday_name VARCHAR(50),
    is_weekend BOOLEAN
);

-- =============================================================
-- 2. DIMENSION: PRODUCT
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY, -- Surrogate Key
    product_id VARCHAR UNIQUE,      -- Natural Key
    is_duplicate BOOLEAN,
    product_name VARCHAR,
    product_type VARCHAR,
    price DECIMAL(10, 2),
    is_referred BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS reject_dim_product (
    reject_id SERIAL PRIMARY KEY,
    product_id VARCHAR,
    is_duplicate BOOLEAN,
    product_name VARCHAR,
    product_type VARCHAR,
    price DECIMAL(10, 2),
    is_referred BOOLEAN,
    rejection_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- 3. DIMENSION: CUSTOMER
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY, -- Surrogate Key
    user_id VARCHAR UNIQUE,          -- Natural Key
    is_duplicate BOOLEAN,
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
    is_referred BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS reject_dim_customer (
    reject_id SERIAL PRIMARY KEY,
    user_id VARCHAR,
    is_duplicate BOOLEAN,
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
    is_referred BOOLEAN,
    rejection_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- 4. DIMENSION: MERCHANT
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_key SERIAL PRIMARY KEY, -- Surrogate Key
    merchant_id VARCHAR UNIQUE,      -- Natural Key
    is_duplicate BOOLEAN,
    name VARCHAR,
    creation_date TIMESTAMP,
    age INT,
    street VARCHAR,
    state VARCHAR,
    city VARCHAR,
    country VARCHAR,
    contact_number VARCHAR,
    is_referred BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS reject_dim_merchant (
    reject_id SERIAL PRIMARY KEY,
    merchant_id VARCHAR,
    is_duplicate BOOLEAN,
    name VARCHAR,
    creation_date TIMESTAMP,
    age INT,
    street VARCHAR,
    state VARCHAR,
    city VARCHAR,
    country VARCHAR,
    contact_number VARCHAR,
    is_referred BOOLEAN,
    rejection_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- 5. DIMENSION: STAFF
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_staff (
    staff_key SERIAL PRIMARY KEY, -- Surrogate Key
    staff_id VARCHAR UNIQUE,      -- Natural Key
    is_duplicate BOOLEAN,
    name VARCHAR,
    job_level VARCHAR,
    street VARCHAR,
    state VARCHAR,
    city VARCHAR,
    country VARCHAR,
    contact_number VARCHAR,
    creation_date TIMESTAMP,
    age INT,
    is_referred BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS reject_dim_staff (
    reject_id SERIAL PRIMARY KEY,
    staff_id VARCHAR,
    is_duplicate BOOLEAN,
    name VARCHAR,
    job_level VARCHAR,
    street VARCHAR,
    state VARCHAR,
    city VARCHAR,
    country VARCHAR,
    contact_number VARCHAR,
    creation_date TIMESTAMP,
    age INT,
    is_referred BOOLEAN,
    rejection_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- 6. DIMENSION: CAMPAIGN
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_key SERIAL PRIMARY KEY, -- Surrogate Key
    campaign_id VARCHAR UNIQUE,      -- Natural Key
    is_duplicate BOOLEAN,
    campaign_name VARCHAR,
    campaign_description VARCHAR,
    discount DECIMAL(5, 2),
    is_referred BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS reject_dim_campaign (
    reject_id SERIAL PRIMARY KEY,
    campaign_id VARCHAR,
    is_duplicate BOOLEAN,
    campaign_name VARCHAR,
    campaign_description VARCHAR,
    discount DECIMAL(5, 2),
    is_referred BOOLEAN,
    rejection_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- 7. FACT: ORDER
-- =============================================================
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key SERIAL PRIMARY KEY, -- Surrogate Key
    order_id VARCHAR,             -- Natural Key
    is_duplicate BOOLEAN,

    -- Foreign Keys point to Surrogate Keys in Dimensions
    customer_key INT REFERENCES dim_customer(customer_key),
    merchant_key INT REFERENCES dim_merchant(merchant_key),
    staff_key    INT REFERENCES dim_staff(staff_key),

    -- Date Keys point to the String Date Key
    transaction_date_key VARCHAR(10) REFERENCES dim_date(date_key),
    estimated_arrival_date_key VARCHAR(10) REFERENCES dim_date(date_key),

    delay_in_days INT,
    gross_total DECIMAL(12,2),
    discount_total DECIMAL(12,2),
    net_total DECIMAL(12,2)
);

CREATE TABLE IF NOT EXISTS reject_fact_orders (
    reject_id SERIAL PRIMARY KEY,
    order_id VARCHAR,     -- Natural ID
    user_id VARCHAR,      -- Natural ID
    merchant_id VARCHAR,  -- Natural ID
    staff_id VARCHAR,     -- Natural ID
    transaction_date_key VARCHAR,
    gross_total DECIMAL(12,2),
    rejection_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- 8. FACT: LINE ITEMS
-- =============================================================
CREATE TABLE IF NOT EXISTS fact_line_items (
    line_item_id SERIAL PRIMARY KEY,
    order_key    INT REFERENCES fact_orders(order_key),
    product_key  INT REFERENCES dim_product(product_key),
    quantity     INT,
    gross_total  DECIMAL(12,2),
    discount_total DECIMAL(12,2),
    net_total    DECIMAL(12,2),
    is_duplicate BOOLEAN
);

CREATE TABLE IF NOT EXISTS reject_fact_line_items (
    reject_id SERIAL PRIMARY KEY,
    order_id VARCHAR,    -- Natural ID
    product_id VARCHAR,  -- Natural ID
    quantity INT,
    gross_total DECIMAL(12,2),
    rejection_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================
-- 9. FACT: CAMPAIGN TRANSACTIONS
-- =============================================================
CREATE TABLE IF NOT EXISTS fact_campaign_transactions (
    transaction_id SERIAL PRIMARY KEY,
    order_key    INT REFERENCES fact_orders(order_key),
    campaign_key INT REFERENCES dim_campaign(campaign_key),
    availed      BOOLEAN,
    is_duplicate BOOLEAN
);

CREATE TABLE IF NOT EXISTS reject_fact_campaign_transactions (
    reject_id SERIAL PRIMARY KEY,
    order_id VARCHAR,    -- Natural ID
    campaign_id VARCHAR, -- Natural ID
    availed BOOLEAN,
    rejection_reason TEXT,
    rejected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);