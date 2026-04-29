{{ config(
    materialized='view',
    schema='staging'
) }}
with source as (
    select * from {{ source('raw_ecommerce', 'payments') }}
),

cleaned as (
    select
        order_id,
        payment_sequential as payment_number,
        payment_type,
        payment_installments as installments,
        payment_value as amount
    from source
)

select * from cleaned