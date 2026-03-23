-- ============================================================
-- Q1. Customer Segmentation & LTV by Purchase Behavior
-- ============================================================

WITH snapshot AS (
    SELECT MAX(order_purchase_timestamp)::date + 1 AS snapshot_date
    FROM public.olist_orders_dataset
),

customer_metrics AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(p.payment_value) AS total_spent,
        MAX(o.order_purchase_timestamp)::date AS last_order_date
    FROM public.olist_customers_dataset c
    JOIN public.olist_orders_dataset o
        ON c.customer_id = o.customer_id
    JOIN public.olist_order_payments_dataset p
        ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),

rfm_base AS (
    SELECT 
        cm.*,
        (s.snapshot_date - cm.last_order_date) AS recency_days
    FROM customer_metrics cm
    CROSS JOIN snapshot s
),

rfm_scores AS (
    SELECT *,
        NTILE(4) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(4) OVER (ORDER BY frequency DESC)    AS f_score,
        NTILE(4) OVER (ORDER BY total_spent DESC)  AS m_score
    FROM rfm_base
),

rfm_segments AS (
    SELECT *,
        CASE 
            WHEN frequency = 1                        THEN 'Single Purchase'
            WHEN r_score >= 3 AND f_score >= 3        THEN 'Champions'
            WHEN r_score <= 2 AND f_score >= 3        THEN 'At Risk Loyal'
            WHEN r_score >= 3 AND f_score <= 2        THEN 'Recent Low Frequency'
            ELSE 'Regular Repeat Buyers'
        END AS customer_segment
    FROM rfm_scores
)

SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spent)::numeric, 2)                 AS avg_ltv,
    ROUND(AVG(frequency)::numeric, 2)                   AS avg_orders,
    ROUND(AVG(total_spent / frequency)::numeric, 2)     AS avg_order_value,  -- FIX: added
    ROUND(AVG(recency_days)::numeric, 0)                AS avg_recency_days
FROM rfm_segments
GROUP BY customer_segment
ORDER BY avg_ltv DESC;




-- ============================================================
-- Q2. Geographic Patterns: LTV and Delivery Performance by State
-- ============================================================

WITH customer_ltv AS (
    SELECT
        c.customer_state,
        c.customer_unique_id,
        SUM(p.payment_value) AS total_spent,
        AVG(oi.freight_value) AS avg_freight,
        AVG(
            o.order_delivered_customer_date::date 
            - o.order_estimated_delivery_date::date
        ) AS avg_delivery_delay
    FROM public.olist_customers_dataset c
    JOIN public.olist_orders_dataset o
        ON c.customer_id = o.customer_id
    JOIN public.olist_order_payments_dataset p
        ON o.order_id = p.order_id
    JOIN public.olist_order_items_dataset oi
        ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_state, c.customer_unique_id
)

SELECT
    customer_state,
    COUNT(DISTINCT customer_unique_id)          AS customer_count,
    ROUND(AVG(total_spent)::numeric, 2)         AS avg_ltv,
    ROUND(AVG(avg_delivery_delay)::numeric, 1)  AS avg_delivery_delay_days,
    ROUND(AVG(avg_freight)::numeric, 2)         AS avg_freight_cost
FROM customer_ltv
GROUP BY customer_state
ORDER BY avg_ltv DESC;




-- ============================================================
-- Q3. Churn Definition & First-Purchase Retention Rate
-- ============================================================

WITH customer_first_last AS (
    SELECT 
        c.customer_unique_id,
        MIN(o.order_purchase_timestamp::timestamp) AS first_order,
        MAX(o.order_purchase_timestamp::timestamp) AS last_order
    FROM public.olist_customers_dataset c
    JOIN public.olist_orders_dataset o 
        ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),

max_order AS (
    SELECT MAX(order_purchase_timestamp::timestamp) AS max_date
    FROM public.olist_orders_dataset
    WHERE order_status = 'delivered'
),

churn_analysis AS (
    SELECT 
        cfl.*,
        CASE 
            WHEN last_order < (SELECT max_date FROM max_order) - INTERVAL '6 months' 
            THEN 'Churned' 
            ELSE 'Active' 
        END AS churn_status,
        CASE 
            WHEN first_order = last_order THEN 'One-time Buyer' 
            ELSE 'Repeat Buyer' 
        END AS buyer_type
    FROM customer_first_last cfl
)

SELECT
    buyer_type,
    churn_status,
    COUNT(*) AS customer_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM churn_analysis), 2) AS pct_of_total
FROM churn_analysis
GROUP BY buyer_type, churn_status
ORDER BY buyer_type, churn_status;




-- ============================================================
-- Q4. First Purchase Experience: Review Score & Delivery Time Impact
-- ============================================================

WITH first_order_details AS (
    SELECT DISTINCT ON (c.customer_unique_id)
        c.customer_unique_id,
        o.order_id,
        o.order_purchase_timestamp::timestamp AS order_purchase_timestamp,
        r.review_score,

        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date::timestamp 
            - o.order_purchase_timestamp::timestamp
        )) / 86400 AS delivery_days,

        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date::timestamp 
            - o.order_estimated_delivery_date::timestamp
        )) / 86400 AS delay_days

    FROM public.olist_customers_dataset c
    JOIN public.olist_orders_dataset o 
        ON c.customer_id = o.customer_id
    LEFT JOIN public.olist_order_reviews_dataset r 
        ON o.order_id = r.order_id
    WHERE o.order_status = 'delivered'
      AND o.order_delivered_customer_date IS NOT NULL
    ORDER BY c.customer_unique_id, o.order_purchase_timestamp
),

customer_orders AS (
    SELECT 
        c.customer_unique_id,
        COUNT(*) AS total_orders
    FROM public.olist_customers_dataset c
    JOIN public.olist_orders_dataset o 
        ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
)

SELECT 
    f.review_score,
    ROUND(AVG(f.delivery_days)::numeric, 1) AS avg_delivery_days,
    ROUND(AVG(f.delay_days)::numeric, 1)    AS avg_delay_days,
    COUNT(*) AS customers_in_group,
    ROUND((
        100.0 * SUM(CASE WHEN co.total_orders > 1 THEN 1 ELSE 0 END) / COUNT(*)
    )::numeric, 2) AS pct_repeat
FROM first_order_details f
JOIN customer_orders co 
    ON f.customer_unique_id = co.customer_unique_id
WHERE f.review_score IS NOT NULL
GROUP BY f.review_score
ORDER BY f.review_score;




-- ============================================================
-- Q5. Product Category Impact on Repeat Purchase Rate
-- ============================================================

WITH customer_category AS (
    SELECT 
        c.customer_unique_id,
        TRIM(p.product_category_name) AS product_category_name,
        COUNT(DISTINCT o.order_id) AS orders_in_category
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o 
        ON c.customer_id = o.customer_id
    JOIN olist_order_items_dataset i 
        ON o.order_id = i.order_id
    JOIN olist_products_dataset p 
        ON i.product_id = p.product_id
    WHERE o.order_status = 'delivered'
      AND p.product_category_name IS NOT NULL
      AND TRIM(p.product_category_name) <> ''
    GROUP BY c.customer_unique_id, TRIM(p.product_category_name)
),

customer_total_orders AS (
    SELECT 
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o 
        ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
)

SELECT 
    COALESCE(t.product_category_name_english, cc.product_category_name) AS category,
    COUNT(DISTINCT cc.customer_unique_id) AS customers,
    ROUND(
        100.0 * SUM(
            CASE WHEN cto.total_orders > 1 THEN 1 ELSE 0 END
        ) / COUNT(DISTINCT cc.customer_unique_id),
        2
    ) AS pct_repeat
FROM customer_category cc
JOIN customer_total_orders cto 
    ON cc.customer_unique_id = cto.customer_unique_id
LEFT JOIN product_category_name_translation t
    ON cc.product_category_name = t.product_category_name
GROUP BY COALESCE(t.product_category_name_english, cc.product_category_name)
HAVING COUNT(DISTINCT cc.customer_unique_id) > 50
ORDER BY pct_repeat DESC;




-- ============================================================
-- Q6. Seller Distance vs. Delivery Time & Churn
-- ============================================================

WITH max_date AS (
    SELECT MAX(order_purchase_timestamp::timestamp) AS max_purchase_date
    FROM public.olist_orders_dataset
    WHERE order_status = 'delivered'
),

churned_customers AS (
    SELECT 
        c.customer_unique_id
    FROM public.olist_customers_dataset c
    JOIN public.olist_orders_dataset o 
        ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
    HAVING MAX(o.order_purchase_timestamp::timestamp) 
        < (SELECT max_purchase_date FROM max_date) - INTERVAL '6 months'
),

geo_unique AS (
    SELECT DISTINCT ON (geolocation_zip_code_prefix)
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng
    FROM public.olist_geolocation_dataset
),

order_distance AS (
    SELECT 
        o.order_id,
        c.customer_unique_id,

        CASE 
            WHEN cc.customer_unique_id IS NOT NULL THEN 1
            ELSE 0
        END AS churn_flag,

        2 * 6371 * asin(
            sqrt(
                sin(radians((g2.geolocation_lat - g1.geolocation_lat)/2))^2 +
                cos(radians(g1.geolocation_lat)) *
                cos(radians(g2.geolocation_lat)) *
                sin(radians((g2.geolocation_lng - g1.geolocation_lng)/2))^2
            )
        ) AS distance_km,

        EXTRACT(EPOCH FROM (
            o.order_delivered_customer_date::timestamp -
            o.order_estimated_delivery_date::timestamp
        )) / 86400 AS delay_days

    FROM public.olist_orders_dataset o
    JOIN public.olist_order_items_dataset i 
        ON o.order_id = i.order_id
    JOIN public.olist_customers_dataset c 
        ON o.customer_id = c.customer_id
    JOIN public.olist_sellers_dataset s 
        ON i.seller_id = s.seller_id
    JOIN geo_unique g1 
        ON c.customer_zip_code_prefix = g1.geolocation_zip_code_prefix
    JOIN geo_unique g2 
        ON s.seller_zip_code_prefix = g2.geolocation_zip_code_prefix
    LEFT JOIN churned_customers cc
        ON c.customer_unique_id = cc.customer_unique_id
    WHERE o.order_status = 'delivered'
      AND o.order_delivered_customer_date IS NOT NULL
      AND o.order_estimated_delivery_date IS NOT NULL
)

SELECT 
    CASE 
        WHEN distance_km < 100               THEN 'Local'
        WHEN distance_km BETWEEN 100 AND 500 THEN 'Regional'
        ELSE 'Long Distance'
    END AS distance_band,

    ROUND(AVG(delay_days)::numeric, 1) AS avg_delay_days,
    COUNT(*) AS total_orders,

    ROUND(
        100.0 * SUM(churn_flag) / COUNT(*),
        2
    ) AS pct_orders_from_churned_customers

FROM order_distance
GROUP BY distance_band
ORDER BY avg_delay_days;




-- ============================================================
-- Q7. Seller Rating Effect on Customer Retention
-- ============================================================

WITH seller_reviews AS (
    SELECT 
        i.seller_id,
        AVG(r.review_score) AS avg_review_score
    FROM olist_order_items_dataset i
    JOIN olist_order_reviews_dataset r 
        ON i.order_id = r.order_id
    GROUP BY i.seller_id
),

customer_order_sequence AS (
    SELECT 
        o.order_id,
        c.customer_unique_id,
        o.order_purchase_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY o.order_purchase_timestamp
        ) AS order_number
    FROM olist_orders_dataset o
    JOIN olist_customers_dataset c 
        ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
),

seller_orders AS (
    SELECT 
        i.seller_id,
        cos.order_id,
        cos.order_number
    FROM olist_order_items_dataset i
    JOIN customer_order_sequence cos 
        ON i.order_id = cos.order_id
),

seller_data AS (
    SELECT 
        sr.seller_id,
        sr.avg_review_score,
        COUNT(DISTINCT so.order_id) AS total_orders,
        ROUND(
            100.0 *
            COUNT(DISTINCT CASE WHEN so.order_number > 1 THEN so.order_id END)
            / NULLIF(COUNT(DISTINCT so.order_id), 0),
            2
        ) AS repeat_purchase_rate
    FROM seller_reviews sr
    JOIN seller_orders so 
        ON sr.seller_id = so.seller_id
    GROUP BY sr.seller_id, sr.avg_review_score
)

SELECT
    CASE
        WHEN avg_review_score < 2 THEN '1-2 (poor)'
        WHEN avg_review_score < 3 THEN '2-3 (below avg)'
        WHEN avg_review_score < 4 THEN '3-4 (good)'
        ELSE                           '4-5 (excellent)'
    END AS rating_band,
    COUNT(*)                                     AS total_sellers,
    ROUND(AVG(repeat_purchase_rate)::numeric, 2) AS avg_repeat_rate,
    ROUND(AVG(total_orders)::numeric, 0)         AS avg_orders_per_seller
FROM seller_data
GROUP BY rating_band
ORDER BY rating_band;




-- ============================================================
-- Q8. Payment Method Influence on LTV and Loyalty
-- ============================================================

WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS total_orders
    FROM public.olist_customers_dataset c
    JOIN public.olist_orders_dataset o
        ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),

customer_payment AS (
    SELECT DISTINCT
        c.customer_unique_id,
        p.payment_type,
        CASE 
            WHEN p.payment_installments = 1              THEN 'Single'
            WHEN p.payment_installments BETWEEN 2 AND 6  THEN 'Installments (2-6)'
            ELSE 'Installments (7+)'
        END AS installment_group
    FROM public.olist_customers_dataset c
    JOIN public.olist_orders_dataset o
        ON c.customer_id = o.customer_id
    JOIN public.olist_order_payments_dataset p
        ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
      AND p.payment_installments > 0
),

customer_ltv AS (
    SELECT
        c.customer_unique_id,
        SUM(p.payment_value) AS total_ltv
    FROM public.olist_customers_dataset c
    JOIN public.olist_orders_dataset o
        ON c.customer_id = o.customer_id
    JOIN public.olist_order_payments_dataset p
        ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
)

SELECT
    cp.payment_type,
    cp.installment_group,
    COUNT(DISTINCT cp.customer_unique_id) AS customers,
    ROUND(AVG(cl.total_ltv)::numeric, 2) AS avg_ltv,

    ROUND(
        100.0 *
        COUNT(DISTINCT CASE WHEN co.total_orders > 1 THEN cp.customer_unique_id END)
        / NULLIF(COUNT(DISTINCT cp.customer_unique_id), 0),
        2
    ) AS repeat_purchase_rate_pct

FROM customer_payment cp
JOIN customer_orders co
    ON cp.customer_unique_id = co.customer_unique_id
JOIN customer_ltv cl
    ON cp.customer_unique_id = cl.customer_unique_id
GROUP BY cp.payment_type, cp.installment_group
ORDER BY avg_ltv DESC;




-- ============================================================
-- Q9. Price Sensitivity: Average Order Value vs. Retention
-- ============================================================

WITH order_value AS (
    SELECT
        o.order_id,
        c.customer_unique_id,
        SUM(p.payment_value) AS order_total
    FROM olist_orders_dataset o
    JOIN olist_customers_dataset c 
        ON o.customer_id = c.customer_id
    JOIN olist_order_payments_dataset p 
        ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.order_id, c.customer_unique_id
),

customer_aov AS (
    SELECT
        customer_unique_id,
        AVG(order_total) AS avg_order_value,
        COUNT(order_id)  AS order_count
    FROM order_value
    GROUP BY customer_unique_id
)

SELECT 
    CASE 
        WHEN avg_order_value < 100               THEN 'Low AOV (<100)'
        WHEN avg_order_value BETWEEN 100 AND 300 THEN 'Medium AOV (100-300)'
        ELSE 'High AOV (>300)'
    END AS aov_segment,

    COUNT(*) AS customers,

    ROUND(
        100.0 * SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS repeat_purchase_rate,

    ROUND(
        100.0 * SUM(CASE WHEN order_count = 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS churn_rate,                          -- FIX: added

    ROUND(AVG(order_count), 2) AS avg_orders

FROM customer_aov
GROUP BY aov_segment
ORDER BY repeat_purchase_rate DESC;




-- ============================================================
-- BONUS: Monthly Revenue Trend
-- ============================================================

SELECT 
    DATE_TRUNC('month', o.order_purchase_timestamp::timestamp) AS order_month,
    ROUND(SUM(p.payment_value)::numeric, 2) AS monthly_revenue
FROM olist_orders_dataset o
JOIN olist_order_payments_dataset p
    ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY order_month
ORDER BY order_month;




-- ============================================================
-- BONUS: CREATE VIEW — RFM Customer Segments (all customers)
-- ============================================================

CREATE VIEW rfm_customer_segments AS
WITH snapshot AS (
    SELECT MAX(order_purchase_timestamp)::date + 1 AS snapshot_date
    FROM olist_orders_dataset
),

customer_metrics AS (
    SELECT
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(p.payment_value) AS total_spent,
        MAX(o.order_purchase_timestamp)::date AS last_order_date
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o
        ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset p
        ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),

rfm_base AS (
    SELECT 
        cm.*,
        (s.snapshot_date - cm.last_order_date) AS recency_days
    FROM customer_metrics cm
    CROSS JOIN snapshot s
),

rfm_scores AS (
    SELECT *,
        NTILE(4) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(4) OVER (ORDER BY frequency DESC)    AS f_score,
        NTILE(4) OVER (ORDER BY total_spent DESC)  AS m_score
    FROM rfm_base
)

SELECT
    customer_unique_id,
    CASE 
        WHEN frequency = 1                        THEN 'Single Purchase'
        WHEN r_score >= 3 AND f_score >= 3        THEN 'Champions'
        WHEN r_score <= 2 AND f_score >= 3        THEN 'At Risk Loyal'
        WHEN r_score >= 3 AND f_score <= 2        THEN 'Recent Low Frequency'
        ELSE 'Regular Repeat Buyers'
    END AS customer_segment
FROM rfm_scores;