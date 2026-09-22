CREATE TABLE IF NOT EXISTS dim_location(
    location_key SERIAL PRIMARY KEY,
    zip_code_prefix VARCHAR(10) UNIQUE NOT NULL,
    city VARCHAR(100),
    state VARCHAR(5),
    avg_lat FLOAT,
    avg_lng FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_customer(
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_unique_id VARCHAR(50),
    zip_code_prefix VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_seller(
    seller_key SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE NOT NULL,
    zip_code_prefix VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_product(
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm  FLOAT
);

CREATE TABLE IF NOT EXISTS dim_time (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      VARCHAR(20),
    week_of_year    INT,
    day_of_month    INT,
    day_of_week     INT,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_order(
    order_item_sk SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    order_item_id BIGINT NOT NULL,
    customer_key INT REFERENCES dim_customer(customer_key),
    product_key INT REFERENCES dim_product(product_key),
    seller_key INT REFERENCES dim_seller(seller_key),
    customer_location_key INT REFERENCES dim_location(location_key),
    seller_location_key INT REFERENCES dim_location(location_key),
    order_date_key INT REFERENCES dim_time(date_key),
    approved_date_key INT REFERENCES dim_time(date_key),
    delivered_date_key INT REFERENCES dim_time(date_key),
    price FLOAT,
    freight_value FLOAT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value FLOAT,
    review_score FLOAT,
    order_status VARCHAR(30),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(order_id, order_item_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_order_date ON fact_order(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_order(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_order(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_seller ON fact_order(seller_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer_location ON fact_order(customer_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_seller_location ON fact_order(seller_location_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_id ON fact_order(order_id);
CREATE INDEX IF NOT EXISTS idx_dim_location_zip ON dim_location(zip_code_prefix);
CREATE INDEX IF NOT EXISTS idx_dim_customer_zip ON dim_customer(zip_code_prefix);
CREATE INDEX IF NOT EXISTS idx_dim_seller_zip ON dim_seller(zip_code_prefix);