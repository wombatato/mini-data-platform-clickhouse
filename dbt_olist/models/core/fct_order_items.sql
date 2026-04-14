select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_ts,
    price,
    freight_value,
    ifNull(price, 0) + ifNull(freight_value, 0) as item_total_value
from {{ ref('stg_order_items') }}