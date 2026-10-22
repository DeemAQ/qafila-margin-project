-- One row per order. created_at is naive UTC text; the business day is the Riyadh date (UTC+3, no DST).
select
    order_id,
    customer_id,
    store_id,
    status,
    cast(created_at as timestamp)                                as created_at_utc,
    cast(cast(created_at as timestamp) + interval 3 hour as date) as order_date
from {{ source('raw', 'orders') }}
