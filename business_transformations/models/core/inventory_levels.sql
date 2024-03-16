{{
    config(
        materialized='table'
    )
}}

-- Total inventory value over time
with total_inventory_value as (
    select date_trunc('month', order_date) as month,
           sum(product_price * order_item_quantity) as total_inventory_value
    from from {{ ref('dim_order') }}
    group by month
)

-- Inventory turnover ratio
, inventory_turnover as (
    select (sum(sales) / avg(inventory)) as inventory_turnover_ratio
    from (
        select sum(order_item_total) as sales,
               avg(order_item_quantity) as inventory
        from dim_order
        group by date_trunc('month', order_date)
    ) as turnover
)

-- Inventory aging analysis
, inventory_aging as (
    select case
               when datediff(current_date(), order_date) <= 30 then '0-30 days'
               when datediff(current_date(), order_date) <= 60 then '31-60 days'
               when datediff(current_date(), order_date) <= 90 then '61-90 days'
               else 'Over 90 days'
           end as age_range,
           sum(product_price * order_item_quantity) as inventory_value
    from dim_order
    group by age_range
)

select total_inventory_value.month,
       total_inventory_value.total_inventory_value,
       inventory_turnover.inventory_turnover_ratio,
       inventory_aging.age_range,
       inventory_aging.inventory_value
from total_inventory_value
cross join inventory_turnover
cross join inventory_aging;
