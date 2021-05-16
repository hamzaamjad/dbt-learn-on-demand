{{ config(
    materialized='ephemeral'
)}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders')}}
),

payments AS (
    SELECT * FROM {{ ref('stg_payments')}}
),

order_payments AS (
    SELECT
        order_id,
        SUM(CASE WHEN status = 'success' THEN amount END) AS amount,
    FROM
        payments
    GROUP BY 1
),

final as (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        COALESCE(p.amount, 0) AS amount
    FROM
        orders AS o
    LEFT JOIN
        order_payments as p
        USING (order_id)
)

SELECT * FROM final