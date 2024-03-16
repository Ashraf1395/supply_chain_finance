{{
    config(
        materialized='view'
    )
}}

select
    -- attributes
    {{ dbt.safe_cast("Shipping_date", api.Column.translate_type("string")) }} as shipping_date,
    {{ dbt.safe_cast("Days_for_shipping_real", api.Column.translate_type("integer")) }} as days_for_shipping_real,
    {{ dbt.safe_cast("Days_for_shipping_scheduled", api.Column.translate_type("integer")) }} as days_for_shipping_scheduled,
    {{ dbt.safe_cast("Shipping_Mode", api.Column.translate_type("string")) }} as shipping_mode,
    {{ dbt.safe_cast("Delivery_Status", api.Column.translate_type("string")) }} as delivery_status

from {{ source('staging','dim_shipping') }}
