{{ config(materialized='view') }}
-- One row per (sku, valid_from): the cost a supplier quoted for that week, after quarantine.
-- When several files quote the same SKU and week, the file whose name sorts last wins (rule 5),
-- so prices_2026-W39_fix.csv replaces prices_2026-W39.csv. Inside one file the later row wins.
with usable as (
    select * from {{ ref('supplier_price_checks') }}
    where reject_reason is null
)

select
    sku,
    valid_from,
    unit_cost_sar,
    source_file
from usable
qualify row_number() over (
    partition by sku, valid_from
    order by source_file desc, source_row desc
) = 1
