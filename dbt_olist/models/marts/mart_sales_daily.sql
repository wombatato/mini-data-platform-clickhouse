select
    order_purchase_date,
    count() as orders_count,
    countDistinct(customer_id) as customers_count,
    round(sum(ifNull(payment_value_total, 0)), 2) as revenue_total,
    round(sum(ifNull(items_price_total, 0)), 2) as items_revenue_total,
    round(sum(ifNull(freight_total, 0)), 2) as freight_total,
    round(avg(ifNull(payment_value_total, 0)), 2) as avg_order_value,
    sum(if(is_delayed = 1, 1, 0)) as delayed_orders_count
from {{ ref('fct_orders') }}
group by order_purchase_date