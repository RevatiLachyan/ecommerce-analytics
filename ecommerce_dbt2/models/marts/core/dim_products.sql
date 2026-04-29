{{ config(
    materialized='table',
    schema='analytics'
) }}
with products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        product_id,
        category,
        weight_grams,
        
        -- Categorize by weight
        case
            when weight_grams < 500 then 'Light'
            when weight_grams < 2000 then 'Medium'
            when weight_grams < 10000 then 'Heavy'
            else 'Very Heavy'
        end as weight_category
        
    from products
)

select * from final