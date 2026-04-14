with payments as (

    select
        order_id,
        sum(ifNull(payment_value, 0)) as payment_value_total,
        max(payment_installments) as max_payment_installments,
        count() as payment_records_count
    from {{ ref('stg_payments') }}
    group by order_id

),

reviews as (

    select
        order_id,
        max(review_score) as review_score,
        max(review_created_ts) as review_created_ts
    from {{ ref('stg_reviews') }}
    group by order_id

),

items as (

    select
        order_id,
        count() as items_count,
        sum(ifNull(price, 0)) as items_price_total,
        sum(ifNull(freight_value, 0)) as freight_total
    from {{ ref('stg_order_items') }}
    group by order_id

)

select
    o.order_id as order_id,
    o.customer_id as customer_id,
    o.order_status as order_status,
    o.order_purchase_ts as order_purchase_ts,
    o.order_purchase_date as order_purchase_date,
    o.order_approved_ts as order_approved_ts,
    o.order_delivered_carrier_ts as order_delivered_carrier_ts,
    o.order_delivered_customer_ts as order_delivered_customer_ts,
    o.order_estimated_delivery_ts as order_estimated_delivery_ts,

    i.items_count,
    i.items_price_total,
    i.freight_total,

    p.payment_value_total,
    p.max_payment_installments,
    p.payment_records_count,

    r.review_score,
    r.review_created_ts,

    if(
        o.order_delivered_customer_ts is not null
        and o.order_estimated_delivery_ts is not null
        and o.order_delivered_customer_ts > o.order_estimated_delivery_ts,
        1,
        0
    ) as is_delayed,

    if(
        o.order_purchase_ts is not null
        and o.order_delivered_customer_ts is not null,
        dateDiff('day', o.order_purchase_ts, o.order_delivered_customer_ts),
        null
    ) as delivery_days

from {{ ref('stg_orders') }} o
left join items i on o.order_id = i.order_id
left join payments p on o.order_id = p.order_id
left join reviews r on o.order_id = r.order_id