{{ config(
    materialized='view',
    schema='staging'
) }}
with source as (
    select * from {{ source('raw_ecommerce', 'orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        order_status,
        
        -- Dates
        order_purchase_timestamp as purchase_date,
        order_approved_at as approved_date,
        order_delivered_carrier_date as shipped_date,
        order_delivered_customer_date as delivered_date,
        order_estimated_delivery_date as estimated_delivery_date,
        
        -- Calculate delivery time (in days)
        datediff(
            day, 
            order_purchase_timestamp, 
            order_delivered_customer_date
        ) as delivery_days,
        
        -- Was it late?
        case 
            when order_delivered_customer_date > order_estimated_delivery_date 
            then true
            else false
        end as is_late_delivery,
        
        -- Is the order complete?
        case 
            when order_status = 'delivered' then true
            else false  
        end as is_delivered
        
    from source
)

select * from cleaned