{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    {{ dbt.safe_cast("`Customer Id`", api.Column.translate_type("integer")) }} as customer_id,
    {{ dbt.safe_cast("`Customer Email`", api.Column.translate_type("string")) }} as customer_email,
    {{ dbt.safe_cast("`Customer Fname`", api.Column.translate_type("string")) }} as customer_fname,
    {{ dbt.safe_cast("`Customer Lname`", api.Column.translate_type("string")) }} as customer_lname,
    {{ dbt.safe_cast("`Customer Segment`", api.Column.translate_type("string")) }} as customer_segment,
    {{ dbt.safe_cast("`Customer City`", api.Column.translate_type("string")) }} as customer_city,
    {{ dbt.safe_cast("`Customer Country`", api.Column.translate_type("string")) }} as customer_country,
    {{ dbt.safe_cast("`Customer State`", api.Column.translate_type("string")) }} as customer_state,
    {{ dbt.safe_cast("`Customer Street`", api.Column.translate_type("string")) }} as customer_street,
    {{ dbt.safe_cast("`Customer Zipcode`", api.Column.translate_type("string")) }} as customer_zipcode

from {{ source('staging','dim_customer') }}
