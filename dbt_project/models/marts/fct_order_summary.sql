-- Mart model: aggregated order summary per customer.
-- Why a fact table: provides pre-computed metrics for analytics dashboards
-- and reporting, avoiding expensive aggregations at query time.

WITH orders AS (
    SELECT
        order_id
        , customer_id
        , order_date
        , total_amount
        , status
    FROM {{ ref('stg_orders') }}
)

, customers AS (
    SELECT
        customer_id
        , email
        , first_name
        , last_name
        , country
    FROM {{ ref('stg_customers') }}
)

, order_aggregates AS (
    SELECT
        customer_id
        , COUNT(order_id) AS total_orders
        , SUM(total_amount) AS total_revenue
        , AVG(total_amount) AS avg_order_value
        , MIN(order_date) AS first_order_date
        , MAX(order_date) AS last_order_date
        , SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_orders
        , SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders
        , SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded_orders
    FROM orders
    GROUP BY customer_id
)

SELECT
    c.customer_id
    , c.email
    , c.first_name
    , c.last_name
    , c.country
    , COALESCE(oa.total_orders, 0) AS total_orders
    , COALESCE(oa.total_revenue, 0) AS total_revenue
    , COALESCE(oa.avg_order_value, 0) AS avg_order_value
    , oa.first_order_date
    , oa.last_order_date
    , COALESCE(oa.completed_orders, 0) AS completed_orders
    , COALESCE(oa.cancelled_orders, 0) AS cancelled_orders
    , COALESCE(oa.refunded_orders, 0) AS refunded_orders
    , CURRENT_TIMESTAMP AS generated_at
FROM customers c
LEFT JOIN order_aggregates oa
    ON c.customer_id = oa.customer_id
