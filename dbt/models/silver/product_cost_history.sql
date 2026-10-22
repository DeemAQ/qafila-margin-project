-- SCD2: one row per SKU per cost version (rule 6).
-- A weekly file only lists the SKUs quoted that week, so a missing week simply adds no row and the
-- previous version stays open (rule 4). A quote with the same cost as the one before it doesn't
-- start a version. valid_to is the next version's valid_from (exclusive), NULL for the current one.
with quotes as (
    select
        sku,
        valid_from,
        unit_cost_sar,
        lag(unit_cost_sar) over (partition by sku order by valid_from) as previous_cost
    from {{ ref('supplier_cost_quotes') }}
),

versions as (
    select sku, valid_from, unit_cost_sar
    from quotes
    where previous_cost is null or unit_cost_sar <> previous_cost
)

select
    sku,
    unit_cost_sar,
    valid_from,
    lead(valid_from) over by_sku         as valid_to,
    lead(valid_from) over by_sku is null as is_current
from versions
window by_sku as (partition by sku order by valid_from)
