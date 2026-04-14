select
    order_id,
    order_item_id,
    price,
    freight_value
from {{ ref('fct_order_items') }}
where ifNull(price, 0) < 0
   or ifNull(freight_value, 0) < 0