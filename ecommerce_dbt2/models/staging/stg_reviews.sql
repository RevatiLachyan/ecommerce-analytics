{{ config(
    materialized='view',
    schema='staging'
) }}
with source as (
    select * from {{ source('raw_ecommerce', 'reviews') }}
),

cleaned as (
    select
        review_id,
        order_id,
        review_score as rating,
        review_comment_title as comment_title,
        
        -- Some comments are null - that's ok
        review_comment_message as comment_text,
        
        review_creation_date as created_date,
        review_answer_timestamp as answered_date,
        
        -- Did they leave a written comment?
        case 
            when review_comment_message is not null then true
            else false
        end as has_comment
        
    from source
)

select * from cleaned