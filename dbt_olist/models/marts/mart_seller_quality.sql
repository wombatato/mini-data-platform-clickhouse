with seller_order_stats as (

    select
        seller_id,
        countDistinct(order_id) as orders_count,
        count() as items_count,
        round(sum(ifNull(price, 0)), 2) as items_revenue_total,
        round(sum(ifNull(freight_value, 0)), 2) as freight_total
    from {{ ref('fct_order_items') }}
    group by seller_id

),

seller_order_flags as (

    select distinct
        i.seller_id as seller_id,
        i.order_id as order_id,
        o.review_score as review_score,
        o.is_delayed as is_delayed
    from {{ ref('fct_order_items') }} i
    left join {{ ref('fct_orders') }} o
        on i.order_id = o.order_id

),

seller_quality as (

    select
        seller_id,
        round(avg(review_score), 2) as avg_review_score,
        sum(if(is_delayed = 1, 1, 0)) as delayed_orders_count
    from seller_order_flags
    group by seller_id

)

select
    s.seller_id as seller_id,
    s.seller_city as seller_city,
    s.seller_state as seller_state,
    sos.orders_count as orders_count,
    sos.items_count as items_count,
    sos.items_revenue_total as items_revenue_total,
    sos.freight_total as freight_total,
    sq.avg_review_score as avg_review_score,
    sq.delayed_orders_count as delayed_orders_count
from {{ ref('dim_sellers') }} s
left join seller_order_stats sos
    on s.seller_id = sos.seller_id
left join seller_quality sq
    on s.seller_id = sq.seller_id