select
    order_id,
    CAST(payment_sequential, 'Nullable(UInt16)') as payment_sequential,
    nullIf(payment_type, '') as payment_type,
    CAST(payment_installments, 'Nullable(UInt16)') as payment_installments,
    CAST(payment_value, 'Nullable(Float64)') as payment_value
from {{ source('raw', 'payments_raw') }}