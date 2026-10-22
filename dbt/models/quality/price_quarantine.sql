-- One row per rejected supplier row (rule 3). Every file is kept, so a row that a later
-- correction replaces still shows here: it was still a bad row in the file it came in.
select
    source_file,
    sku,
    valid_from,
    raw_cost,
    reject_reason as reason,
    source_row
from {{ ref('supplier_price_checks') }}
where reject_reason is not null
