# Qafila gross margin

Submitted by: Deem Alqudaimi — academy: @SDAIAAcademy

The task, rules and grading are in [BRIEF.md](BRIEF.md).

```bash
uv sync
uv run python tools/make_data.py          # data/
uv run python run.py                      # data/ -> warehouse/ (DuckDB + Iceberg)
uv run python tools/check.py              # contract self-check
uv run python tools/snapshots.py          # Iceberg history of the three tables (re-run / time-travel evidence)
```

## Design

**Flow:** SQLite + supplier CSVs → `raw` (Python `ingest()`, DuckDB) → `staging` → `silver` → `gold` / `quality` (dbt-duckdb, `dbt build` runs models and tests) → Iceberg (pyiceberg `publish()`, SQL catalog).

| Model | Layer | Grain (one row per ...) | Materialization |
|---|---|---|---|
| `raw.orders`, `order_items`, `products`, `stores` | raw | source row, untouched | table (replaced on every run) |
| `raw.supplier_prices` | raw | row of any `prices_*.csv`, all values as text, + `source_file`, `source_row` | table (replaced on every run) |
| `stg_supplier_prices` | staging | supplier file row: cost parsed, SKU trimmed, `valid_from` as date | view |
| `stg_orders`, `stg_order_items`, `stg_products`, `stg_stores` | staging | order (+ Riyadh `order_date`), order line (+ revenue), product (trimmed SKU), store | view |
| `supplier_price_checks` | silver | supplier file row + `reject_reason` (NULL = usable) | view |
| `supplier_cost_quotes` | silver | `sku` × `valid_from` (last file wins) | view |
| `product_cost_history` | silver | `sku` × cost version (SCD2) | table |
| `order_lines` | silver | counted order line (delivered, qty > 0, from 2026-08-31) | table |
| `order_line_margin` | silver | counted order line + the cost version valid on its date | table |
| `weekly_margin` | gold | `iso_week` × `city` × `category` | table |
| `price_quarantine` | quality | rejected supplier file row | table |

**How the SCD2 history is built:** Rows from every file are labelled once (`supplier_price_checks`), so quarantine and history share one rule. Quarantined rows are dropped first. Then each (`sku`, `valid_from`) keeps the row from the file whose name sorts last, which is how `prices_2026-W39_fix.csv` replaces `prices_2026-W39.csv`. Within a file, the later row wins. A missing week just adds no quote, so the previous version stays open. A quote whose cost equals the previous quote (`lag`) is dropped, and the remaining rows are the versions. `valid_to = lead(valid_from)` (exclusive) and `is_current = valid_to IS NULL`. Everything is rebuilt from all files on every run, so a late correction for any week flows through.

**How each order line finds its cost:** `order_line_margin` left-joins the history on `sku` and `valid_from <= order_date < valid_to` (open-ended for the current version), with `order_date` = `created_at` (UTC) + 3 h. No match means the line is uncosted. It still counts in `lines` and `revenue_sar`, but not in the cost columns. Money is summed as `DECIMAL(18,2)` so totals are exact and identical on every run.

**Publishing:** each table is written to Iceberg in place. A new table is created and appended. If the rows equal the current snapshot, nothing is written, so a re-run adds no snapshot. Otherwise one `OVERWRITE` snapshot removes the old data files and adds the new ones. pyiceberg's `Table.overwrite()` commits an empty `DELETE` snapshot followed by an `APPEND`. I avoided it so that the snapshot before the newest one is never an empty table.

## Data problems I found

Counts are for the course dataset (seed 2026). The pipeline finds them by rule, not by name, and gives the same results as an independent reference on seeds 3, 11, 77 and 4242.

| Problem | Where | Rows affected | What I did |
|---|---|---|---|
| Byte-order mark before the header | `prices_2026-W38.csv` | 1 file (the first column would read `﻿supplier_id`) | read every file as `utf-8-sig` |
| Decimal commas (`"9,49"`) | W38 `unit_cost` | 126 | `parse_cost` macro: `,` → `.` |
| Arabic-Indic digits (`٩٫٤٩`) | W38 `unit_cost` | 34 | `parse_cost` maps `٠-٩`/`۰-۹` → `0-9` and `٫` → `.` |
| Column renamed `unit_cost` → `cost_sar`, columns reordered, `currency` added | W39 and the fix file | 169 rows | columns matched by name on ingest; `coalesce(cost_sar, unit_cost)` |
| Zero / negative cost (`0`, `-1`, `-5.5`) | W39 | 4 | quarantined `invalid_cost` |
| SKUs Qafila doesn't sell (`QF-NEW-*`) | W39 | 3 | quarantined `unknown_sku` |
| Trailing spaces in SKUs | `products` (`QF-FRU-1107`, `QF-PAN-1331`) | 2 products (0 supplier rows in this seed) | `trim()` on both sides before matching |
| Late correction | `prices_2026-W39_fix.csv` | 9 rows: costs for the 4 bad rows + 5 renegotiated prices | last file name wins per (sku, week): history 633 → 637 versions. The 4 bad W39 rows stay in quarantine, because they were still rejected rows in the file they came in. |
| Negative quantities | `order_items` | 142 lines (69 in delivered orders from 2026-08-31) | excluded (`quantity > 0`) |
| Timestamps are UTC, business day is Riyadh | `orders.created_at` | 1,273 counted lines fall on another date in Riyadh, 183 of them in another ISO week | `order_date = created_at + 3 h`, then filter and week |
| Products sold before their first quote, or never quoted | order lines vs history | 20,626 of 82,404 counted lines uncosted (10,160 of them in W36; 27 SKUs never quoted) | kept as uncosted: in `lines`/`revenue_sar`, not in cost |

## Tests

`dbt build` runs 38 data tests and 7 unit tests. Any failure stops `run.py` before anything is published.

| Test | On | What it protects against |
|---|---|---|
| `unique` (sku, valid_from) | `product_cost_history`, `supplier_cost_quotes` | two quotes for the same week surviving the "last file wins" step |
| `assert_history_one_current_version_per_sku` | `product_cost_history` | a SKU with no current version, or two of them |
| `assert_history_versions_chain_without_gaps` | `product_cost_history` | gaps or overlaps between versions, `valid_to` not exclusive, a "version" with an unchanged cost |
| `positive`, `not_null` on `unit_cost_sar`, `relationships` sku → `stg_products` | `product_cost_history` | a quarantined row (bad cost or unknown SKU) leaking into the history |
| `accepted_values` + `not_null` on `reason`, `unique` (file, row) | `price_quarantine` | wrong reasons, a row quarantined twice |
| `unique` (order_id, line_no) + `equal_row_count` vs `order_lines` | `order_line_margin` | the point-in-time join matching two versions (double cost) or dropping lines |
| `unique` (iso_week, city, category), `not_null` | `weekly_margin` | grain broken, lines with a missing store or product |
| `assert_weekly_margin_adds_up` | `weekly_margin` | margin ≠ costed revenue − cost, costed revenue > revenue, uncosted > lines |
| `assert_weekly_margin_reconciles_to_order_lines` | `weekly_margin` vs staging | lines or revenue lost or double-counted (rule 7 re-derived independently) |
| `unique` keys, `not_null` dates | staging models | duplicated source rows, unreadable dates (`valid_from` is a warning) |
| unit tests (7) | `stg_supplier_prices`, `stg_orders`, `supplier_cost_quotes`, `product_cost_history`, `order_line_margin`, `weekly_margin` | each format the costs come in. The Riyadh day boundary. Fix file beats W39 but never with a quarantined row. Missing week, same cost, and a cost that goes back (5 → 6 → 5 is 3 versions). An order on the day a version starts gets the new cost. Uncosted lines in revenue but not in cost. |

## Re-runs and the correction

On the course dataset: run, run again, add the correction, run, run again (`tools/snapshots.py` prints the history).

```
run 1                silver.product_cost_history: created, 633 rows   gold.weekly_margin: created, 165 rows   quality.price_quarantine: created, 7 rows
run 2 (same data)    silver.product_cost_history: unchanged           gold.weekly_margin: unchanged           quality.price_quarantine: unchanged
run 3 (correction)   silver.product_cost_history: overwritten, 637    gold.weekly_margin: overwritten, 165    quality.price_quarantine: unchanged
run 4 (same data)    silver.product_cost_history: unchanged           gold.weekly_margin: unchanged           quality.price_quarantine: unchanged
```

`gold.weekly_margin` snapshots:

| Snapshot | Operation | Week 39 cost_sar | Week 39 margin_sar | All weeks margin_sar |
|---|---|---|---|---|
| `8904716638585545609` (before the correction) | append | 519,954.96 | 248,962.65 | 801,016.88 |
| `5060397403693557223` (after the correction) | overwrite | 518,152.53 | 250,765.08 | 803,515.80 |

The older snapshot still returns the pre-correction numbers: `catalog.load_table("gold.weekly_margin").scan(snapshot_id=8904716638585545609)`. Week 40 changes too (cost 191,580.79 → 190,884.30) because the corrected W39 cost is still current on 1 October. Weeks 36–38 don't change. The second and fourth runs add no snapshot. Snapshot ids differ on each fresh warehouse.

## What I'd do next

- **100× more data:** `ingest()` reads all of SQLite and every CSV into memory and replaces `raw` on each run. Every model is a full rebuild, and `publish()` rewrites and re-scans whole tables. That breaks first. Next steps: incremental landing, meaning a manifest of loaded files with checksums, and orders read by `updated_at`. Incremental dbt models for `order_lines`/`order_line_margin`. `weekly_margin` partitioned by `iso_week` in Iceberg, overwriting only the weeks whose costs or orders changed, with a content hash in the snapshot summary instead of a full comparison.
- **Daily supplier files:** the SCD2 and point-in-time logic already work per date. But "the file name that sorts last wins" stops being safe once names mix schemes (`prices_2026-09-21.csv` vs `prices_2026-W39_fix.csv`). I'd record each file's arrival time and order by that. A late correction to an old day can also change every later version of that SKU, so recomputation has to start from the earliest corrected date.
- Alert when quarantine grows or the uncosted share jumps, instead of only recording it.

## AI assistance

I used an AI assistant (Claude) in these ways:

- **`run.py`:** I co-wrote `ingest()` and `publish()` with AI. After an AI review, `publish()` was changed to use only public pyiceberg functions.
- **dbt tests:** I co-wrote the data tests and unit tests with AI.
- **Review:** I used AI to check the project against the brief on new datasets and give me a score. I acted on its feedback: moving `order_line_margin` to silver and fixing this README.
