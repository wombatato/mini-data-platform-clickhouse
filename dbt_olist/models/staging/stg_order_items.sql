select
    order_id,
    CAST(order_item_id, 'Nullable(UInt16)') as order_item_id,
    product_id,
    seller_id,
    parseDateTimeBestEffortOrNull(toString(shipping_limit_date)) as shipping_limit_ts,
    CAST(price, 'Nullable(Float64)') as price,
    CAST(freight_value, 'Nullable(Float64)') as freight_value
from {{ source('raw', 'order_items_raw') }}