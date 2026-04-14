select
    product_id,
    nullIf(product_category_name, '') as product_category_name,
    CAST(product_name_lenght, 'Nullable(UInt32)') as product_name_length,
    CAST(product_description_lenght, 'Nullable(UInt32)') as product_description_length,
    CAST(product_photos_qty, 'Nullable(UInt16)') as product_photos_qty,
    CAST(product_weight_g, 'Nullable(Float64)') as product_weight_g,
    CAST(product_length_cm, 'Nullable(Float64)') as product_length_cm,
    CAST(product_height_cm, 'Nullable(Float64)') as product_height_cm,
    CAST(product_width_cm, 'Nullable(Float64)') as product_width_cm
from {{ source('raw', 'products_raw') }}