"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import csv
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import _dataframe_to_data_files
from pyiceberg.table import Table

DBT_DIR = Path(__file__).resolve().parent / "dbt"

OLTP_TABLES = ["orders", "order_items", "products", "stores"]
SUPPLIER_GLOB = "prices_*.csv"

# What publish() writes: Iceberg table -> (DuckDB relation built by dbt, contract schema).
PUBLISHED = {
    "silver.product_cost_history": ("silver.product_cost_history", pa.schema([
        ("sku", pa.string()),
        ("unit_cost_sar", pa.float64()),
        ("valid_from", pa.date32()),
        ("valid_to", pa.date32()),
        ("is_current", pa.bool_()),
    ])),
    "gold.weekly_margin": ("gold.weekly_margin", pa.schema([
        ("iso_week", pa.int32()),
        ("city", pa.string()),
        ("category", pa.string()),
        ("lines", pa.int32()),
        ("revenue_sar", pa.float64()),
        ("costed_revenue_sar", pa.float64()),
        ("cost_sar", pa.float64()),
        ("margin_sar", pa.float64()),
        ("uncosted_lines", pa.int32()),
    ])),
    "quality.price_quarantine": ("quality.price_quarantine", pa.schema([
        ("source_file", pa.string()),
        ("sku", pa.string()),
        ("valid_from", pa.date32()),
        ("raw_cost", pa.string()),
        ("reason", pa.string()),
    ])),
}


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`, exactly as they arrive.

    - From data_dir/qafila_oltp.sqlite: orders, order_items, products, stores.
    - Every data_dir/suppliers/prices_*.csv, keeping the file name of each row.

    Running this twice must not duplicate anything.
    """
    sqlite_path = data_dir / "qafila_oltp.sqlite"
    if not sqlite_path.exists():
        raise SystemExit(f"missing {sqlite_path}: run tools/make_data.py first")
    supplier_files = sorted((data_dir / "suppliers").glob(SUPPLIER_GLOB))
    if not supplier_files:
        raise SystemExit(f"no {SUPPLIER_GLOB} files in {data_dir / 'suppliers'}")

    with duckdb.connect(str(db)) as con:  # closes the file before dbt opens it
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")
        # Every run replaces the raw tables with what is in data_dir now, so a re-run never duplicates rows.
        for name in OLTP_TABLES:
            land(con, name, read_sqlite_table(sqlite_path, name))
        land(con, "supplier_prices", read_supplier_files(supplier_files))
        for name in [*OLTP_TABLES, "supplier_prices"]:
            n = con.sql(f"SELECT count(*) FROM raw.{name}").fetchone()[0]
            print(f"raw.{name}: {n:,} rows")


def land(con: duckdb.DuckDBPyConnection, name: str, table: pa.Table) -> None:
    """Replace raw.<name> with an Arrow table."""
    con.register("incoming", table)
    con.sql(f"CREATE OR REPLACE TABLE raw.{name} AS SELECT * FROM incoming")
    con.unregister("incoming")


def read_sqlite_table(path: Path, name: str) -> pa.Table:
    """One app table, column names and values untouched (SQLite's own types decide the Arrow types)."""
    with closing(sqlite3.connect(path)) as con:
        cur = con.execute(f'SELECT * FROM "{name}"')
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pa.table({c: pa.array([r[i] for r in rows]) for i, c in enumerate(cols)})


def read_supplier_files(paths: list[Path]) -> pa.Table:
    """All supplier rows as text, exactly as written, plus the file name and the row's position in it.

    Files change shape over time (renamed and added columns, a byte-order mark), so columns are
    matched by name and a column a file doesn't have is NULL for its rows. Nothing is parsed here:
    staging decides what the text means.
    """
    rows: list[dict[str, str | int | None]] = []
    columns: list[str] = []
    for path in paths:
        with path.open(encoding="utf-8-sig", newline="") as f:  # utf-8-sig drops a byte-order mark
            reader = csv.reader(f)
            header = [h.strip() for h in next(reader, [])]
            columns += [h for h in header if h not in columns]
            for row_number, values in enumerate(reader, start=1):
                if not any(v.strip() for v in values):
                    continue  # blank line
                rows.append({"source_file": path.name, "source_row": row_number, **dict(zip(header, values))})
    data = {
        "source_file": pa.array([r["source_file"] for r in rows], pa.string()),
        "source_row": pa.array([r["source_row"] for r in rows], pa.int32()),
    }
    for c in columns:
        data[c] = pa.array([r.get(c) for r in rows], pa.string())
    return pa.table(data)


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def publish(db: Path, warehouse: Path) -> None:
    """Step 5: publish three dbt models to Iceberg (see the brief for the exact columns).

        silver.product_cost_history   gold.weekly_margin   quality.price_quarantine

    Update existing tables in place, never drop and recreate them: the table's snapshots are
    how we see what a number was before the supplier's correction arrived.
    """
    catalog = get_catalog(warehouse)
    with duckdb.connect(str(db), read_only=True) as con:
        for name, (relation, schema) in PUBLISHED.items():
            cols = ", ".join(schema.names)
            data = con.sql(f"SELECT {cols} FROM {relation} ORDER BY ALL").arrow()
            if isinstance(data, pa.RecordBatchReader):  # newer duckdb returns a reader
                data = data.read_all()
            print(f"{name}: {publish_table(catalog, name, data.cast(schema))}")


def publish_table(catalog: SqlCatalog, name: str, data: pa.Table) -> str:
    """Make Iceberg table `name` hold exactly `data`, keeping its history.

    - New table: create it and append (snapshot 1).
    - Same rows as the current snapshot: do nothing, so a re-run adds no snapshot.
    - Different rows: one OVERWRITE snapshot that removes the old data files and adds the new ones.
      Table.overwrite() would commit a DELETE snapshot (an empty table) and then an APPEND, so the
      snapshot just before the newest one would be empty; doing it in one snapshot avoids that.
    """
    catalog.create_namespace_if_not_exists(name.split(".")[0])
    if not catalog.table_exists(name):
        table = catalog.create_table(name, schema=data.schema)
        table.append(data)
        return f"created, {len(data)} rows"

    table = catalog.load_table(name)
    with table.update_schema() as update:  # adds any new contract column; never drops one
        update.union_by_name(data.schema)
    table = catalog.load_table(name)
    if same_rows(table, data):
        return f"unchanged, {len(data)} rows"

    old_files = [task.file for task in table.scan().plan_files()]
    with table.transaction() as tx:
        with tx.update_snapshot().overwrite() as snapshot:
            for f in old_files:
                snapshot.delete_data_file(f)
            for f in _dataframe_to_data_files(tx.table_metadata, data, table.io, snapshot.commit_uuid):
                snapshot.append_data_file(f)
    return f"overwritten, {len(data)} rows (snapshot {catalog.load_table(name).current_snapshot().snapshot_id})"


def same_rows(table: Table, data: pa.Table) -> bool:
    """True when the table's current snapshot holds exactly these rows (order ignored)."""
    if table.current_snapshot() is None:
        return False
    current = table.scan(selected_fields=tuple(data.schema.names)).to_arrow()
    if current.num_rows != data.num_rows:
        return False
    keys = [(c, "ascending") for c in data.schema.names]
    return current.cast(data.schema).sort_by(keys).equals(data.sort_by(keys))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
