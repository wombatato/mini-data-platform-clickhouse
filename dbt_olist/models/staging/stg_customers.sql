select
    customer_id,
    customer_unique_id,
    toUInt32OrNull(customer_zip_code_prefix) as customer_zip_code_prefix,
    nullIf(customer_city, '') as customer_city,
    nullIf(customer_state, '') as customer_state
from {{ source('raw', 'customers_raw') }}