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
-- Total inventory value over time
total_inventory_value AS (
    SELECT EXTRACT(MONTH FROM DATE_TRUNC(PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date), MONTH)) AS month,
           SUM(order_item_product_price * order_item_quantity) AS total_inventory_value
    FROM {{ ref('dim_order') }}
    GROUP BY month
),
-- Inventory turnover ratio
inventory_turnover AS (
    SELECT 
        EXTRACT(MONTH FROM DATE_TRUNC(PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date), MONTH)) AS month_num,
        (SUM(sales) / AVG(inventory)) AS inventory_turnover_ratio
    FROM (
        SELECT 
            order_date,
            order_item_total AS sales,
            order_item_quantity AS inventory
        FROM {{ ref('dim_order') }}
    )
    GROUP BY month_num
),
-- Inventory aging analysis
inventory_aging AS (
    SELECT 
        order_date,
        CASE
           WHEN TIMESTAMP_DIFF('2018-01-01', PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date), DAY) <= 30 THEN '0-30 days'
           WHEN TIMESTAMP_DIFF('2018-01-01', PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date), DAY) <= 60 THEN '31-60 days'
           WHEN TIMESTAMP_DIFF('2018-01-01', PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date), DAY) <= 90 THEN '61-90 days'
           ELSE 'Over 90 days'
       END AS age_range,
       SUM(order_item_product_price * order_item_quantity) AS inventory_value
    FROM {{ ref('dim_order') }}
    GROUP BY order_date, age_range
)

SELECT 
    month_names.month_name,
    total_inventory_value.total_inventory_value,
    inventory_turnover.inventory_turnover_ratio,
    inventory_aging.age_range,
    inventory_aging.inventory_value
FROM 
    total_inventory_value
JOIN 
    month_names ON total_inventory_value.month = month_names.month_num
JOIN 
    inventory_turnover ON total_inventory_value.month = inventory_turnover.month_num
JOIN
    inventory_aging ON total_inventory_value.month = EXTRACT(MONTH FROM DATE_TRUNC(PARSE_TIMESTAMP('%m/%d/%Y %H:%M', inventory_aging.order_date), MONTH))

