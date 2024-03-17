-- Inventory management insights
{{
    config(
        materialized='table'
    )
}}

SELECT product_card_id as product_id, SUM(order_item_quantity) AS total_quantity_sold
FROM {{ ref('dim_order') }}
GROUP BY product_card_id
ORDER BY total_quantity_sold ASC
LIMIT 10