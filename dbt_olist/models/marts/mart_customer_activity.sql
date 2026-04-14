with customer_orders as (

    select
        c.customer_unique_id,
        any(c.customer_state) as customer_state,
        countDistinct(o.order_id) as orders_count,
        min(o.order_purchase_date) as first_order_date,
        max(o.order_purchase_date) as last_order_date,
        round(sum(ifNull(o.payment_value_total, 0)), 2) as total_revenue,
        round(avg(ifNull(o.payment_value_total, 0)), 2) as avg_order_value,
        round(avg(o.review_score), 2) as avg_review_score,
        sum(if(o.is_delayed = 1, 1, 0)) as delayed_orders_count
    from {{ ref('fct_orders') }} o
    left join {{ ref('dim_customers') }} c
        on o.customer_id = c.customer_id
    group by c.customer_unique_id

)

select
    customer_unique_id,
    customer_state,
    orders_count,
    first_order_date,
    last_order_date,
    total_revenue,
    avg_order_value,
    avg_review_score,
    delayed_orders_count,
    dateDiff('day', first_order_date, last_order_date) as customer_lifetime_days
from customer_orders