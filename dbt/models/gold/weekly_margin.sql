-- One row per iso_week x city x category with at least one counted line.
-- Uncosted lines count in lines and revenue_sar but not in the cost columns (rule 9).
with lines as (
    select * from {{ ref('order_line_margin') }}
),

weekly as (
    select
        iso_week,
        city,
        category,
        count(*)                                                                  as lines,
        sum(line_revenue_sar)                                                     as revenue_sar,
        coalesce(sum(line_revenue_sar) filter (where line_cost_sar is not null), 0) as costed_revenue_sar,
        coalesce(sum(line_cost_sar), 0)                                           as cost_sar,
        count(*) filter (where line_cost_sar is null)                             as uncosted_lines
    from lines
    group by iso_week, city, category
)

select
    iso_week,
    city,
    category,
    cast(lines as integer)                                    as lines,
    cast(round(revenue_sar, 2) as double)                     as revenue_sar,
    cast(round(costed_revenue_sar, 2) as double)              as costed_revenue_sar,
    cast(round(cost_sar, 2) as double)                        as cost_sar,
    cast(round(costed_revenue_sar - cost_sar, 2) as double)   as margin_sar,
    cast(uncosted_lines as integer)                           as uncosted_lines
from weekly
