-- Each version ends exactly where the next one starts (no gap, no overlap), starts before it
-- ends, and differs in cost from the version before it (otherwise it shouldn't exist).
with history as (
    select
        *,
        lead(valid_from)   over (partition by sku order by valid_from) as next_valid_from,
        lag(unit_cost_sar) over (partition by sku order by valid_from) as previous_cost
    from {{ ref('product_cost_history') }}
)

select *
from history
where valid_to is distinct from next_valid_from
   or valid_to <= valid_from
   or unit_cost_sar = previous_cost
