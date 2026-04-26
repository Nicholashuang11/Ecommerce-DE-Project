CREATE OR REPLACE VIEW vw_monthly_revenue AS
SELECT
    dt.year,
    dt.month,
    dt.month_name,
    COUNT(DISTINCT f.order_id)                              AS total_orders,
    COUNT(DISTINCT f.customer_key)                          AS unique_customers,
    ROUND(SUM(f.price + f.freight_value)::NUMERIC, 2)       AS gross_revenue,
    ROUND(AVG(f.price + f.freight_value)::NUMERIC, 2)       AS avg_order_value
FROM fact_order_items f
JOIN dim_time dt ON f.order_date_key = dt.date_key
WHERE f.order_status = 'delivered'
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year, dt.month;

CREATE OR REPLACE VIEW vw_sales_by_region AS
SELECT
    l.state,
    l.city,
    l.avg_lat,
    l.avg_lng,
    COUNT(DISTINCT f.order_id)                              AS total_orders,
    ROUND(SUM(f.price + f.freight_value)::NUMERIC, 2)       AS total_revenue,
    COUNT(DISTINCT f.customer_key)                          AS unique_customers
FROM fact_order_items f
JOIN dim_customer c  ON f.customer_key   = c.customer_key
JOIN dim_location l  ON c.location_key   = l.location_key
GROUP BY l.state, l.city, l.avg_lat, l.avg_lng
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW vw_seller_performance AS
SELECT
    s.seller_id,
    l.city                                                  AS seller_city,
    l.state                                                 AS seller_state,
    COUNT(DISTINCT f.order_id)                              AS total_orders,
    ROUND(SUM(f.price)::NUMERIC, 2)                         AS total_revenue,
    ROUND(AVG(f.review_score)::NUMERIC, 2)                  AS avg_review_score,
    COUNT(DISTINCT f.customer_key)                          AS unique_customers
FROM fact_order_items f
JOIN dim_seller   s ON f.seller_key    = s.seller_key
JOIN dim_location l ON s.location_key  = l.location_key
GROUP BY s.seller_id, l.city, l.state;

CREATE OR REPLACE VIEW vw_product_performance AS
SELECT
    COALESCE(p.product_category_name_english, 'unknown')   AS category,
    COUNT(f.order_item_sk)                                  AS total_items_sold,
    ROUND(SUM(f.price)::NUMERIC, 2)                         AS total_revenue,
    ROUND(AVG(f.review_score)::NUMERIC, 2)                  AS avg_review_score,
    ROUND(AVG(f.price)::NUMERIC, 2)                         AS avg_price
FROM fact_order_items f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_category_name_english
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW vw_customer_rfm AS
WITH rfm_base AS (
    SELECT
        c.customer_unique_id,
        MAX(dt.full_date)                                   AS last_order_date,
        COUNT(DISTINCT f.order_id)                          AS frequency,
        ROUND(SUM(f.price + f.freight_value)::NUMERIC, 2)  AS monetary
    FROM fact_order_items f
    JOIN dim_customer c  ON f.customer_key   = c.customer_key
    JOIN dim_time     dt ON f.order_date_key  = dt.date_key
    GROUP BY c.customer_unique_id
),
rfm_scored AS (
    SELECT *,
        (CURRENT_DATE - last_order_date)                                        AS recency_days,
        NTILE(5) OVER (ORDER BY (CURRENT_DATE - last_order_date) ASC)           AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)                                 AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)                                  AS m_score
    FROM rfm_base
)
SELECT
    customer_unique_id,
    last_order_date,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    CONCAT(r_score::TEXT, f_score::TEXT, m_score::TEXT) AS rfm_segment
FROM rfm_scored;
