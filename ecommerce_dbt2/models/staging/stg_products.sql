with source as (
    select * from {{ source('raw_ecommerce', 'products') }}
),

cleaned as (
    select
        product_id,
        
        -- Handle missing category names
        coalesce(product_category_name, 'unknown') as category,
        
        product_name_length,
        product_description_length,
        product_photos_qty as photo_count,
        product_weight_g as weight_grams,
        product_length_cm as length_cm,
        product_height_cm as height_cm,
        product_width_cm as width_cm,
        
        -- Calculate volume (might be useful for shipping analysis)
        product_length_cm * product_height_cm * product_width_cm as volume_cubic_cm
        
    from source
)

select * from cleaned