-- One row per counted order line, costed at the cost version valid on its order date (rule 9):
-- valid_from <= order_date < valid_to (open-ended for the current version). A line with no such
-- version (SKU never quoted, or quoted only after the order) keeps a NULL cost: it's uncosted.
-- Money is summed as DECIMAL(18, 2) downstream so totals are exact and identical on every run.
with lines as (
    select * from {{ ref('order_lines') }}
),

history as (
    select * from {{ ref('product_cost_history') }}
)

select
    lines.order_id,
    lines.line_no,
    lines.order_date,
    lines.iso_week,
    lines.city,
    lines.category,
    lines.sku,
    lines.quantity,
    cast(lines.line_revenue_sar as decimal(18, 2))                       as line_revenue_sar,
    history.valid_from                                                    as cost_valid_from,
    cast(history.unit_cost_sar as decimal(18, 2))                         as unit_cost_sar,
    lines.quantity * cast(history.unit_cost_sar as decimal(18, 2))        as line_cost_sar
from lines
left join history
    on  history.sku = lines.sku
    and lines.order_date >= history.valid_from
    and (history.valid_to is null or lines.order_date < history.valid_to)
