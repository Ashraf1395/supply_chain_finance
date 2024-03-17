{{
    config(
        materialized='table'
    )
}}

fraud_detection AS (
    SELECT
        customer_id,
        AVG(order_total) AS avg_order_total,
        COUNT(*) AS num_orders
    FROM {{ ref('dim_order') }}
    GROUP BY customer_id
    ORDER BY avg_order_total DESC
    LIMIT 10
)