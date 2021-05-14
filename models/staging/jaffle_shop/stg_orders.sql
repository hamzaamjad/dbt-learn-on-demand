WITH orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from {{source('jaffle_shop', 'orders')}}
    -- completed, shipped, returned, placed, return_pending
    {{ limit_data_in_dev('order_date', 1000) }}
)

SELECT * FROM orders