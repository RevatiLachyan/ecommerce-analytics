{{ config(
    materialized='view',
    schema='staging'
) }}
with source as (
    select * from {{ source('raw_ecommerce', 'order_items') }}
),

cleaned as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        
        -- Total item value (product + shipping)
        price + freight_value as total_item_value
        
    from source
)

select * from cleaned