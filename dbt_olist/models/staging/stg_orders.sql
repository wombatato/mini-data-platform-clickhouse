select
    order_id,
    customer_id,
    nullIf(order_status, '') as order_status,
    parseDateTimeBestEffortOrNull(toString(order_purchase_timestamp)) as order_purchase_ts,
    toDate(parseDateTimeBestEffortOrNull(toString(order_purchase_timestamp))) as order_purchase_date,
    parseDateTimeBestEffortOrNull(toString(order_approved_at)) as order_approved_ts,
    parseDateTimeBestEffortOrNull(toString(order_delivered_carrier_date)) as order_delivered_carrier_ts,
    parseDateTimeBestEffortOrNull(toString(order_delivered_customer_date)) as order_delivered_customer_ts,
    parseDateTimeBestEffortOrNull(toString(order_estimated_delivery_date)) as order_estimated_delivery_ts
from {{ source('raw', 'orders_raw') }}