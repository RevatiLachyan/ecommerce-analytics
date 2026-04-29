{{ config(
    materialized='table',
    schema='analytics'
) }}
with sellers as (
    select * from {{ ref('stg_sellers') }}
),

-- Count products per seller
seller_product_counts as (
    select
        seller_id,
        count(distinct product_id) as products_sold
    from {{ ref('stg_order_items') }}
    group by seller_id
),

final as (
    select
        s.seller_id,
        s.city,
        s.state,
        s.zip_code,
        coalesce(sp.products_sold, 0) as total_products_sold
        
    from sellers s
    left join seller_product_counts sp on s.seller_id = sp.seller_id
)

select * from final
