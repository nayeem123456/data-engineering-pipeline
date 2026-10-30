-- ==============================================================================
-- 02_init_gold_schema.sql
-- Analytics Serving and Dimensional Star Schema (Gold Layer)
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- Date Dimension
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER PRIMARY KEY, -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(16) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(16) NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Customer Dimension
CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_sk VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL UNIQUE,
    first_name VARCHAR(64),
    last_name VARCHAR(64),
    email VARCHAR(128),
    city VARCHAR(64),
    state VARCHAR(64),
    country VARCHAR(64),
    customer_segment VARCHAR(32), -- VIP, REGULAR, NEW, DORMANT
    signup_date DATE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS gold.dim_products (
    product_sk VARCHAR(64) PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL UNIQUE,
    product_name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    brand VARCHAR(64),
    cost_price NUMERIC(12, 2) NOT NULL,
    retail_price NUMERIC(12, 2) NOT NULL,
    price_band VARCHAR(32), -- BUDGET, MID_RANGE, PREMIUM, LUXURY
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE
);

-- Orders Fact
CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_sk VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL UNIQUE,
    order_date_key INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    customer_sk VARCHAR(64) NOT NULL REFERENCES gold.dim_customers(customer_sk),
    order_status VARCHAR(32) NOT NULL, -- COMPLETED, SHIPPED, PROCESSING, CANCELLED, REFUNDED
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    subtotal_amount NUMERIC(12, 2) NOT NULL,
    tax_amount NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    shipping_amount NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    discount_amount NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    total_amount NUMERIC(12, 2) NOT NULL,
    item_count INTEGER NOT NULL DEFAULT 1,
    order_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    order_updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Order Items Fact
CREATE TABLE IF NOT EXISTS gold.fact_order_items (
    order_item_sk VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    order_sk VARCHAR(64) NOT NULL REFERENCES gold.fact_orders(order_sk),
    product_sk VARCHAR(64) NOT NULL REFERENCES gold.dim_products(product_sk),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    total_price NUMERIC(12, 2) NOT NULL,
    estimated_margin NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Payments Fact
CREATE TABLE IF NOT EXISTS gold.fact_payments (
    payment_sk VARCHAR(64) PRIMARY KEY,
    payment_id VARCHAR(64) NOT NULL UNIQUE,
    order_sk VARCHAR(64) NOT NULL REFERENCES gold.fact_orders(order_sk),
    payment_method VARCHAR(32) NOT NULL, -- CREDIT_CARD, PAYPAL, APPLE_PAY, BANK_TRANSFER
    payment_status VARCHAR(32) NOT NULL, -- CAPTURED, PENDING, FAILED, REFUNDED
    amount NUMERIC(12, 2) NOT NULL,
    is_refund BOOLEAN NOT NULL DEFAULT FALSE,
    payment_date_key INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Analytical Mart: Daily Sales Summary
CREATE TABLE IF NOT EXISTS gold.mart_daily_sales (
    date_key INTEGER PRIMARY KEY,
    sale_date DATE NOT NULL UNIQUE,
    total_orders INTEGER NOT NULL,
    completed_orders INTEGER NOT NULL,
    gross_revenue NUMERIC(14, 2) NOT NULL,
    tax_collected NUMERIC(12, 2) NOT NULL,
    shipping_collected NUMERIC(12, 2) NOT NULL,
    discounts_given NUMERIC(12, 2) NOT NULL,
    refund_count INTEGER NOT NULL DEFAULT 0,
    refund_amount NUMERIC(14, 2) NOT NULL DEFAULT 0.00,
    net_revenue NUMERIC(14, 2) NOT NULL,
    unique_customers INTEGER NOT NULL,
    average_order_value NUMERIC(10, 2) NOT NULL
);

-- Analytical Mart: Customer Lifetime Value (LTV)
CREATE TABLE IF NOT EXISTS gold.mart_customer_lifetime_value (
    customer_sk VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL UNIQUE,
    customer_name VARCHAR(128),
    customer_segment VARCHAR(32),
    tenure_days INTEGER NOT NULL,
    first_order_date DATE,
    last_order_date DATE,
    total_orders INTEGER NOT NULL,
    total_spend NUMERIC(14, 2) NOT NULL,
    total_items_purchased INTEGER NOT NULL,
    average_order_value NUMERIC(10, 2) NOT NULL,
    is_repeat_customer BOOLEAN NOT NULL
);

-- Analytical Mart: Product Performance
CREATE TABLE IF NOT EXISTS gold.mart_product_performance (
    product_sk VARCHAR(64) PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL UNIQUE,
    product_name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    price_band VARCHAR(32),
    units_sold INTEGER NOT NULL,
    gross_revenue NUMERIC(14, 2) NOT NULL,
    total_margin NUMERIC(14, 2) NOT NULL,
    margin_percentage NUMERIC(5, 2) NOT NULL,
    unique_buyers INTEGER NOT NULL
);

-- Analytical Mart: Refund Analysis
CREATE TABLE IF NOT EXISTS gold.mart_refund_analysis (
    refund_date DATE NOT NULL,
    payment_method VARCHAR(32) NOT NULL,
    refund_count INTEGER NOT NULL,
    refund_amount NUMERIC(14, 2) NOT NULL,
    avg_refund_size NUMERIC(10, 2) NOT NULL,
    PRIMARY KEY (refund_date, payment_method)
);

-- Indexes for analytical query optimization
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON gold.fact_orders(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON gold.fact_orders(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_product ON gold.fact_order_items(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_payments_order ON gold.fact_payments(order_sk);
