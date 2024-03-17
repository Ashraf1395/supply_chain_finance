{{
    config(
        materialized='view'
    )
}}

select
    -- attributes
    {{ dbt.safe_cast("`key`", api.Column.translate_type("string")) }} as metadata_key,
    {{ dbt.safe_cast("`offset`", api.Column.translate_type("integer")) }} as metadata_offset,
    {{ dbt.safe_cast("`partition`", api.Column.translate_type("integer")) }} as metadata_partition,
    {{ dbt.safe_cast("`time`", api.Column.translate_type("integer")) }} as metadata_time,
    {{ dbt.safe_cast("`topic`", api.Column.translate_type("string")) }} as metadata_topic

from {{ source('staging','dim_metadata') }}
