{{ config(
    materialized='table',
    schema='analytics'
) }}
with customers as (
    select * from {{ ref('stg_customers') }}
),

-- Get customer's first order date
customer_first_orders as (
    select
        customer_id,
        min(purchase_date) as first_order_date
    from {{ ref('stg_orders') }}
    group by customer_id
),

-- Count total orders per customer
customer_order_counts as (
    select
        customer_id,
        count(*) as total_orders
    from {{ ref('stg_orders') }}
    where is_delivered = true
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.customer_unique_id,
        c.city,
        c.state,
        c.zip_code,
        
        -- Add customer metrics
        coalesce(fo.first_order_date, null) as first_order_date,
        coalesce(oc.total_orders, 0) as lifetime_orders,
        
        -- Customer segment based on order count
        case
            when coalesce(oc.total_orders, 0) >= 5 then 'VIP'
            when coalesce(oc.total_orders, 0) >= 2 then 'Repeat'
            when coalesce(oc.total_orders, 0) = 1 then 'One-time'
            else 'New'
        end as customer_segment
        
    from customers c
    left join customer_first_orders fo on c.customer_id = fo.customer_id
    left join customer_order_counts oc on c.customer_id = oc.customer_id
)

select * from final