-- One row per counted order line (rule 7): delivered order, quantity > 0, Riyadh order date on or
-- after the first supplier week. Carries what the margin needs: date, week, city, category, SKU, revenue.
with orders as (
    select * from {{ ref('stg_orders') }}
    where status = 'delivered'
      and order_date >= date '{{ var("margin_start_date") }}'
),

items as (
    select * from {{ ref('stg_order_items') }}
    where quantity > 0
)

select
    items.order_id,
    items.line_no,
    orders.order_date,
    cast(isoyear(orders.order_date) as integer) as iso_year,
    cast(week(orders.order_date) as integer)    as iso_week,  -- DuckDB's week() is the ISO week
    stores.city,
    products.category,
    products.sku,
    items.quantity,
    items.line_revenue_sar
from items
inner join orders           using (order_id)
left join {{ ref('stg_stores') }}   as stores   using (store_id)
left join {{ ref('stg_products') }} as products using (product_id)
