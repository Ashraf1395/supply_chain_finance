{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    {{ dbt.safe_cast("Department_Id", api.Column.translate_type("integer")) }} as department_id,
    
    -- attributes
    {{ dbt.safe_cast("Department_Name", api.Column.translate_type("string")) }} as department_name,
    {{ dbt.safe_cast("Market", api.Column.translate_type("string")) }} as market

from {{ source('staging','dim_department') }}
