{{ config(materialized='view') }}
-- One row per supplier file row, labelled with why it can't be used (rule 3), or NULL when it can.
-- price_quarantine and supplier_cost_quotes both read this, so the rule lives in one place.
with prices as (
    select * from {{ ref('stg_supplier_prices') }}
),

known_skus as (
    select distinct sku from {{ ref('stg_products') }}
)

select
    prices.*,
    case
        when known_skus.sku is null                                       then 'unknown_sku'
        when prices.unit_cost_sar is null or prices.unit_cost_sar <= 0    then 'invalid_cost'
    end as reject_reason
from prices
left join known_skus using (sku)
