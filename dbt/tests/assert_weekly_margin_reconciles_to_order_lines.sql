-- The weekly table neither drops nor double-counts lines: its totals equal the counted order
-- lines, which are re-derived here straight from staging with rule 7.
with expected as (
    select count(*) as lines, round(sum(items.line_revenue_sar), 2) as revenue_sar
    from {{ ref('stg_order_items') }} as items
    inner join {{ ref('stg_orders') }} as orders using (order_id)
    where orders.status = 'delivered'
      and items.quantity > 0
      and orders.order_date >= date '{{ var("margin_start_date") }}'
),

actual as (
    select sum(lines) as lines, round(sum(revenue_sar), 2) as revenue_sar
    from {{ ref('weekly_margin') }}
)

select *
from expected cross join actual
where expected.lines <> actual.lines
   or abs(expected.revenue_sar - actual.revenue_sar) > 0.05
