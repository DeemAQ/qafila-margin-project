-- One row per row of every supplier price file. Costs and dates parsed, SKUs trimmed; nothing filtered.
with source as (
    select * from {{ source('raw', 'supplier_prices') }}
),

parsed as (
    select
        source_file,
        source_row,
        supplier_id,
        trim(sku)                                 as sku,
        try_cast(trim(valid_from) as date)        as valid_from,
        -- the cost column is unit_cost until week 38 and cost_sar from week 39 on
        coalesce(cost_sar, unit_cost)             as raw_cost,
        {{ parse_cost('coalesce(cost_sar, unit_cost)') }} as parsed_cost
    from source
)

select
    source_file,
    source_row,
    supplier_id,
    sku,
    valid_from,
    raw_cost,
    case when isfinite(parsed_cost) then round(parsed_cost, 2) end as unit_cost_sar
from parsed
