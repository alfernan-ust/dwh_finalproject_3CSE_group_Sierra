-- =============================================================
-- SHOPZADA DATA WAREHOUSE SCHEMA (FRESH INSTALL)
-- =============================================================
-- NOTES:
-- 1. Business Keys (e.g., user_id, order_id) are NOT UNIQUE to allow history.
-- 2. Surrogate Keys (e.g., customer_key) are the PRIMARY KEYS.
-- 3. Flags:
--    - is_duplicate: FALSE for the most recent/active row, TRUE for older versions.
--    - is_inferred: TRUE if created by a Fact loader (missing in Dim source).
--    - is_incomplete: TRUE if any required attribute is null/missing.

-- =============================================================
-- 1. DIMENSION: DATE
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_date (
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

-- =============================================================
-- 2. DIMENSION: PRODUCT
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR, -- Not Unique, allows history
    product_name VARCHAR,
    product_type VARCHAR,
    price DECIMAL(10, 2),
    -- Flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

-- =============================================================
-- 3. DIMENSION: CUSTOMER
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
    user_id VARCHAR, -- Not Unique, allows history
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
    -- Flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

-- =============================================================
-- 4. DIMENSION: MERCHANT
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_key SERIAL PRIMARY KEY,
    merchant_id VARCHAR, -- Not Unique, allows history
    name VARCHAR,
    creation_date TIMESTAMP,
    age INT,
    street VARCHAR,
    state VARCHAR,
    city VARCHAR,
    country VARCHAR,
    contact_number VARCHAR,
    -- Flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

-- =============================================================
-- 5. DIMENSION: STAFF
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_staff (
    staff_key SERIAL PRIMARY KEY,
    staff_id VARCHAR, -- Not Unique, allows history
    name VARCHAR,
    job_level VARCHAR,
    street VARCHAR,
    state VARCHAR,
    city VARCHAR,
    country VARCHAR,
    contact_number VARCHAR,
    creation_date TIMESTAMP,
    age INT,
    -- Flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

-- =============================================================
-- 6. DIMENSION: CAMPAIGN
-- =============================================================
CREATE TABLE IF NOT EXISTS dim_campaign (
    campaign_key SERIAL PRIMARY KEY,
    campaign_id VARCHAR, -- Not Unique, allows history
    campaign_name VARCHAR,
    campaign_description VARCHAR,
    discount DECIMAL(5, 2),
    -- Flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_inferred BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

-- =============================================================
-- 7. FACT: ORDER
-- =============================================================
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key SERIAL PRIMARY KEY,
    order_id VARCHAR, -- Not Unique, allows status updates/history
    -- Foreign keys are nullable to allow for incomplete rows
    customer_key INT REFERENCES dim_customer(customer_key),
    merchant_key INT REFERENCES dim_merchant(merchant_key),
    staff_key    INT REFERENCES dim_staff(staff_key),
    transaction_date_key VARCHAR(10) REFERENCES dim_date(date_key),
    estimated_arrival_date_key VARCHAR(10) REFERENCES dim_date(date_key),
    delay_in_days INT,
    gross_total DECIMAL(12,2),
    discount_total DECIMAL(12,2),
    net_total DECIMAL(12,2),
    -- Flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

-- =============================================================
-- 8. FACT: LINE ITEMS
-- =============================================================
CREATE TABLE IF NOT EXISTS fact_line_items (
    line_item_id SERIAL PRIMARY KEY,
    -- Composite Business Key would be order_id + product_id (not unique in history)
    order_key    INT REFERENCES fact_orders(order_key),
    product_key  INT REFERENCES dim_product(product_key),
    quantity     INT,
    gross_total  DECIMAL(12,2),
    discount_total DECIMAL(12,2),
    net_total    DECIMAL(12,2),
    -- Flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);

-- =============================================================
-- 9. FACT: CAMPAIGN TRANSACTIONS
-- =============================================================
CREATE TABLE IF NOT EXISTS fact_campaign_transactions (
    transaction_id SERIAL PRIMARY KEY,
    -- Composite Business Key would be order_id + campaign_id
    order_key    INT REFERENCES fact_orders(order_key),
    campaign_key INT REFERENCES dim_campaign(campaign_key),
    availed      BOOLEAN,
    -- Flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_incomplete BOOLEAN DEFAULT FALSE,
    incomplete_reason TEXT
);