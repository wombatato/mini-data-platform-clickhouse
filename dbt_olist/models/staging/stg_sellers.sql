select
    seller_id,
    toUInt32OrNull(seller_zip_code_prefix) as seller_zip_code_prefix,
    nullIf(seller_city, '') as seller_city,
    nullIf(seller_state, '') as seller_state
from {{ source('raw', 'sellers_raw') }}