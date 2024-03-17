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
monthly_customers AS (
    SELECT
        EXTRACT(YEAR FROM PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date)) AS year,
        EXTRACT(MONTH FROM PARSE_TIMESTAMP('%m/%d/%Y %H:%M', order_date)) AS month,
        COUNT(DISTINCT order_customer_id) AS distinct_customers
    FROM
        {{ ref('dim_order') }}
    GROUP BY
        year, month
)
SELECT
    year,
    mo.month_name,
    distinct_customers,
    SUM(distinct_customers) OVER (ORDER BY year, month) AS rolling_customers,
    round(distinct_customers/(SUM(distinct_customers) OVER (ORDER BY year, month)),2) as customer_retention_rate
FROM
    monthly_customers m
JOIN month_names mo on m.month =mo.month_num
ORDER BY
    year, month