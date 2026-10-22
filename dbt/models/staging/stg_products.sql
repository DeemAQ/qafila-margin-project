-- One row per product. SKUs trimmed so they match the supplier files.
select
    product_id,
    trim(sku)  as sku,
    category,
    name_en,
    unit_price,
    is_active = 1 as is_active
from {{ source('raw', 'products') }}
