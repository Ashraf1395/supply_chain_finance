{{
    config(
        materialized='table'
    )
}}

WITH overall_sales_performance AS (
    SELECT DATE_TRUNC('month', order_date) AS month,
           SUM(order_item_total) AS total_sales
    FROM {{ ref('dim_order') }}
    GROUP BY month
),
profit_margin_analysis AS (
    SELECT DATE_TRUNC('month', order_date) AS month,
           AVG(order_profit_per_order) AS avg_profit_margin
    FROM {{ ref('dim_order') }}
    GROUP BY month
),
operational_efficiency_metrics AS (
    SELECT AVG(days_for_shipment_real) AS avg_actual_shipment_days,
           AVG(days_for_shipment_scheduled) AS avg_scheduled_shipment_days
    FROM {{ ref('dim_shipping') }}
)
SELECT 
    os.month,
    os.total_sales,
    pma.avg_profit_margin,
    oem.avg_actual_shipment_days,
    oem.avg_scheduled_shipment_days
FROM 
    overall_sales_performance os
JOIN 
    profit_margin_analysis pma ON os.month = pma.month
CROSS JOIN 
    overall_customer_satisfaction ocs
CROSS JOIN 
    operational_efficiency_metrics oem
