WITH order_aggregates AS (
    SELECT
        department_id,
        product_card_id,
        SUM(order_item_total) AS total_committed_funds,
        SUM(CASE WHEN order_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_orders,
        COUNT(*) AS total_orders
    FROM {{ ref('dim_order') }}
    GROUP BY department_id, product_card_id
),
department_market_category AS (
    SELECT
        d.department_name,
        d.market,
        p.category_name,
        oa.total_committed_funds,
        oa.completed_orders,
        oa.total_orders
    FROM order_aggregates oa
    JOIN {{ ref('dim_department') }} d ON oa.department_id = d.department_id
    JOIN {{ ref('dim_product') }} p ON oa.product_card_id = p.product_card_id
)
SELECT
    department_name,
    market,
    category_name,
    total_committed_funds,
    completed_orders / total_orders AS commitment_fulfillment_rate
FROM department_market_category;


