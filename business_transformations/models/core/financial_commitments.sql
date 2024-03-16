WITH financial_commitments AS (
    -- Financial commitments by department, market, or product category
    SELECT department_name, market, product_category,
           SUM(order_item_total) AS total_committed_funds,
           (SUM(CASE WHEN order_status = 'COMPLETED' THEN 1 ELSE 0 END) / COUNT(*)) AS commitment_fulfillment_rate
    FROM {{ ref('dim_order') }} 
    GROUP BY department_name, market, product_category
)
SELECT * FROM financial_commitments;
