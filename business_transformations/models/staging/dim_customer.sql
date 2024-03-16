{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    {{ dbt.safe_cast("Customer_Id", api.Column.translate_type("integer")) }} as customer_id,
    {{ dbt.safe_cast("Customer_Email", api.Column.translate_type("string")) }} as customer_email,
    {{ dbt.safe_cast("Customer_Fname", api.Column.translate_type("string")) }} as customer_fname,
    {{ dbt.safe_cast("Customer_Lname", api.Column.translate_type("string")) }} as customer_lname,
    {{ dbt.safe_cast("Customer_Segment", api.Column.translate_type("string")) }} as customer_segment,
    {{ dbt.safe_cast("Customer_City", api.Column.translate_type("string")) }} as customer_city,
    {{ dbt.safe_cast("Customer_Country", api.Column.translate_type("string")) }} as customer_country,
    {{ dbt.safe_cast("Customer_State", api.Column.translate_type("string")) }} as customer_state,
    {{ dbt.safe_cast("Customer_Street", api.Column.translate_type("string")) }} as customer_street,
    {{ dbt.safe_cast("Customer_Zipcode", api.Column.translate_type("string")) }} as customer_zipcode
    

from {{ source('staging','dim_customer') }}
