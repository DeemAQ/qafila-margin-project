"""Show the Iceberg history of the three published tables: the evidence for re-runs and time travel.

    uv run python tools/snapshots.py --warehouse warehouse

For every snapshot it prints the operation, the row count and, for gold.weekly_margin, the totals
read from that snapshot. A re-run on the same data adds no snapshot; the correction adds one, and
the older snapshot still holds the numbers from before it.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog

TABLES = ["silver.product_cost_history", "gold.weekly_margin", "quality.price_quarantine"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    wh = ap.parse_args().warehouse.resolve()
    catalog = SqlCatalog("qafila", uri=f"sqlite:///{(wh / 'catalog.db').as_posix()}", warehouse=wh.as_posix())

    for name in TABLES:
        table = catalog.load_table(name)
        print(name)
        for snap in table.snapshots():
            df = table.scan(snapshot_id=snap.snapshot_id).to_pandas()
            when = datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            line = f"  {snap.snapshot_id:>20}  {when}  {snap.summary.operation.value:<9}  {len(df):>4} rows"
            if name == "gold.weekly_margin":
                line += f"  cost_sar={df['cost_sar'].sum():,.2f}  margin_sar={df['margin_sar'].sum():,.2f}"
                for week, w in df.groupby("iso_week"):
                    line += f"\n{'':26}week {week}: cost_sar={w['cost_sar'].sum():,.2f}  margin_sar={w['margin_sar'].sum():,.2f}"
            print(line)


if __name__ == "__main__":
    main()
