select
    review_id,
    order_id,
    CAST(review_score, 'Nullable(UInt8)') as review_score,
    nullIf(review_comment_title, '') as review_comment_title,
    nullIf(review_comment_message, '') as review_comment_message,
    parseDateTimeBestEffortOrNull(toString(review_creation_date)) as review_created_ts,
    parseDateTimeBestEffortOrNull(toString(review_answer_timestamp)) as review_answer_ts
from {{ source('raw', 'reviews_raw') }}