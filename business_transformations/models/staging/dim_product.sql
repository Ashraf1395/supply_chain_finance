{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    {{ dbt.safe_cast("Product_Card_Id", api.Column.translate_type("integer")) }} as product_card_id,
    {{ dbt.safe_cast("Product_Category_Id", api.Column.translate_type("integer")) }} as product_category_id,
    
    -- attributes
    {{ dbt.safe_cast("Category_Name", api.Column.translate_type("string")) }} as category_name,
    {{ dbt.safe_cast("Product_Description", api.Column.translate_type("string")) }} as product_description,
    {{ dbt.safe_cast("Product_Image", api.Column.translate_type("string")) }} as product_image,
    {{ dbt.safe_cast("Product_Name", api.Column.translate_type("string")) }} as product_name,
    {{ dbt.safe_cast("Product_Price", api.Column.translate_type("float")) }} as product_price,
    {{ dbt.safe_cast("Product_Status", api.Column.translate_type("string")) }} as product_status

from {{ source('staging','dim_product') }}
