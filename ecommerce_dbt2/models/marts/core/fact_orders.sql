{{ config(
    materialized='table',
    schema='analytics'
) }}
with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

reviews as (
    select * from {{ ref('stg_reviews') }}
),

-- Aggregate order items to order level
order_totals as (
    select
        order_id,
        count(*) as item_count,
        sum(price) as total_product_value,
        sum(freight_value) as total_freight_value,
        sum(price + freight_value) as total_order_value
    from order_items
    group by order_id
),

-- Aggregate payments to order level
payment_totals as (
    select
        order_id,
        sum(amount) as total_payment,
        listagg(distinct payment_type, ', ') as payment_types
    from payments
    group by order_id
),

-- Get review info
order_reviews as (
    select
        order_id,
        avg(rating) as avg_rating,
        count(*) as review_count
    from reviews
    group by order_id
),

final as (
    select
        -- Order ID (grain of the fact table)
        o.order_id,
        
        -- Foreign keys to dimensions
        o.customer_id,
        
        -- Order dates
        o.purchase_date,
        o.approved_date,
        o.delivered_date,
        date(o.purchase_date) as order_date,
        
        -- Order status
        o.order_status,
        o.is_delivered,
        o.is_late_delivery,
        
        -- Order metrics
        coalesce(ot.item_count, 0) as item_count,
        coalesce(ot.total_product_value, 0) as product_value,
        coalesce(ot.total_freight_value, 0) as freight_value,
        coalesce(ot.total_order_value, 0) as order_value,
        
        -- Payment info
        coalesce(pt.total_payment, 0) as payment_amount,
        pt.payment_types,
        
        -- Review metrics
        r.avg_rating,
        coalesce(r.review_count, 0) as review_count,
        
        -- Delivery performance
        o.delivery_days,
        case
            when o.delivery_days <= 7 then 'Fast'
            when o.delivery_days <= 14 then 'Normal'
            when o.delivery_days <= 30 then 'Slow'
            else 'Very Slow'
        end as delivery_speed,
        
        -- Date parts for analysis
        year(o.purchase_date) as order_year,
        month(o.purchase_date) as order_month,
        dayofweek(o.purchase_date) as order_day_of_week,
        quarter(o.purchase_date) as order_quarter
        
    from orders o
    left join order_totals ot on o.order_id = ot.order_id
    left join payment_totals pt on o.order_id = pt.order_id
    left join order_reviews r on o.order_id = r.order_id
)

select * from final