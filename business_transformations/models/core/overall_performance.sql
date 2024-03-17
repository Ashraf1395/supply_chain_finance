{{
    config(
        materialized='table'
    )
}}

WITH month_names AS (
    SELECT 1 AS month_num, 'January' AS month_name UNION ALL
    SELECT 2, 'February' UNION ALL
    SELECT 3, 'March' UNION ALL
    SELECT 4, 'April' UNION ALL
    SELECT 5, 'May' UNION ALL
    SELECT 6, 'June' UNION ALL
    SELECT 7, 'July' UNION ALL
    SELECT 8, 'August' UNION ALL
    SELECT 9, 'September' UNION ALL
    SELECT 10, 'October' UNION ALL
    SELECT 11, 'November' UNION ALL
    SELECT 12, 'December'
),
overall_sales_performance AS (
    SELECT DATE_TRUNC('month', order_date) AS month,
           SUM(order_item_total) AS total_sales
    FROM {{ ref('dim_order') }}
    GROUP BY month
),
profit_margin_analysis AS (
    SELECT EXTRACT(MONTH FROM DATE_TRUNC(PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date), MONTH)) AS month,
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
    o.month_name,
    os.total_sales,
    pma.avg_profit_margin,
    oem.avg_actual_shipment_days,
    oem.avg_scheduled_shipment_days
FROM 
    overall_sales_performance os
JOIN 
    profit_margin_analysis pma ON os.month = pma.month
JOIN
    month_names m os.month=o.month_num
CROSS JOIN 
    overall_customer_satisfaction ocs
CROSS JOIN 
    operational_efficiency_metrics oem
