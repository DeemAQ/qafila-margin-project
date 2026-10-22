-- Internal consistency of each weekly row: margin = costed revenue - cost, costed revenue is part
-- of revenue, and uncosted lines are part of lines.
select *
from {{ ref('weekly_margin') }}
where abs(costed_revenue_sar - cost_sar - margin_sar) > 0.005
   or costed_revenue_sar > revenue_sar + 0.005
   or uncosted_lines > lines
   or uncosted_lines < 0
   or (uncosted_lines = lines and (costed_revenue_sar <> 0 or cost_sar <> 0))
