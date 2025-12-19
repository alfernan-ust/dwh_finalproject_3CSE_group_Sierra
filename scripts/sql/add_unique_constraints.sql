-- Add unique constraints to existing tables
-- This is needed to support ON CONFLICT in the load scripts

-- Drop existing constraints if they exist (ignore errors if not found)
ALTER TABLE dim_date DROP CONSTRAINT IF EXISTS dim_date_date_key_key CASCADE;
ALTER TABLE dim_product DROP CONSTRAINT IF EXISTS dim_product_product_id_key CASCADE;
ALTER TABLE dim_customer DROP CONSTRAINT IF EXISTS dim_customer_user_id_key CASCADE;
ALTER TABLE dim_merchant DROP CONSTRAINT IF EXISTS dim_merchant_merchant_id_key CASCADE;
ALTER TABLE dim_staff DROP CONSTRAINT IF EXISTS dim_staff_staff_id_key CASCADE;
ALTER TABLE dim_campaign DROP CONSTRAINT IF EXISTS dim_campaign_campaign_id_key CASCADE;
ALTER TABLE fact_orders DROP CONSTRAINT IF EXISTS fact_orders_order_id_key CASCADE;
ALTER TABLE fact_line_items DROP CONSTRAINT IF EXISTS fact_line_items_order_key_product_key_key CASCADE;
ALTER TABLE fact_campaign_transactions DROP CONSTRAINT IF EXISTS fact_campaign_transactions_order_key_campaign_key_key CASCADE;

-- Add unique constraints
ALTER TABLE dim_date ADD CONSTRAINT dim_date_date_key_key UNIQUE (date_key);
ALTER TABLE dim_product ADD CONSTRAINT dim_product_product_id_key UNIQUE (product_id);
ALTER TABLE dim_customer ADD CONSTRAINT dim_customer_user_id_key UNIQUE (user_id);
ALTER TABLE dim_merchant ADD CONSTRAINT dim_merchant_merchant_id_key UNIQUE (merchant_id);
ALTER TABLE dim_staff ADD CONSTRAINT dim_staff_staff_id_key UNIQUE (staff_id);
ALTER TABLE dim_campaign ADD CONSTRAINT dim_campaign_campaign_id_key UNIQUE (campaign_id);
ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_order_id_key UNIQUE (order_id);
ALTER TABLE fact_line_items ADD CONSTRAINT fact_line_items_order_key_product_key_key UNIQUE (order_key, product_key);
ALTER TABLE fact_campaign_transactions ADD CONSTRAINT fact_campaign_transactions_order_key_campaign_key_key UNIQUE (order_key, campaign_key);
