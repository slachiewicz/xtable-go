# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Write the schema-evolution and deletion torture fixtures under test/testdata/fixtures.

This is a separate generator from generate.py, writing only fixtures whose directory name is
prefixed `evolution-` or `deletes-`. It exists because test/schema_evolution_test.go needs
multi-commit Delta tables whose interesting behaviour sits at the boundary between commits: a
schema change that lands mid-history, or a deletion that a naive incremental reader would report
as an add. Every table here is written by delta-rs; polytable never touches these directories.

Pinned to the same writer version generate.py's own docstring names (deltalake==1.6.3,
pyarrow==25.0.1) so a fixture from this script and one from generate.py describe the same writer.
Run it the same way:

    .venv/bin/python test/fixtures/generate_evolution.py
    .venv/bin/python test/fixtures/generate_evolution.py test/testdata/fixtures evolution-drop-column

Determinism has the same limits generate.py documents: row values, column order and the commit
sequence are fixed here; commit timestamps, table UUIDs and generated file names are not. Go
assertions are against manifest.json, regenerated alongside the fixture, never against a literal.
"""

import decimal
import json
import shutil
import sys
from pathlib import Path

import pyarrow as pa

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO_ROOT / "test" / "testdata" / "fixtures"

DELTA_TYPE_NAMES = {"long": "LONG", "string": "STRING", "double": "DOUBLE", "int": "INT", "decimal": "DECIMAL"}


def _rmtree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _write_manifest(directory: Path, manifest: dict) -> None:
    with (directory / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")


def _delta_commits(table_dir: Path) -> list[dict]:
    """Read every _delta_log/*.json commit and record its own add/remove paths and schema.

    Copied from generate.py's helper of the same name rather than imported: the two generators are
    kept independent on purpose (see the module docstring), and this is the one piece of logic both
    need. A per-commit record read straight out of the log is what a snapshot alone cannot give: a
    remove and a re-add at the same logical position collapse to "unchanged" once only the final
    file list is inspected.
    """
    commits = []
    for log_file in sorted((table_dir / "_delta_log").glob("*.json")):
        added: list[str] = []
        removed: list[str] = []
        operation = ""
        timestamp = 0
        schema_names: list[str] | None = None
        for line in log_file.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            action = json.loads(line)
            if "add" in action:
                added.append(action["add"]["path"])
            if "remove" in action:
                removed.append(action["remove"]["path"])
            if "commitInfo" in action:
                operation = action["commitInfo"].get("operation", "")
                timestamp = action["commitInfo"].get("timestamp", 0)
            if "metaData" in action:
                schema_names = [f["name"] for f in json.loads(action["metaData"]["schemaString"])["fields"]]
        commits.append(
            {
                "version": int(log_file.stem),
                "operation": operation,
                "timestamp": timestamp,
                "added": sorted(added),
                "removed": sorted(removed),
                "schema_columns": schema_names,
            }
        )
    return commits


def _schema_out(table) -> list[dict]:
    return [
        {
            "name": field.name,
            "type": DELTA_TYPE_NAMES.get(field.type.type, field.type.type.upper()),
            "nullable": field.nullable,
        }
        for field in table.schema().fields
    ]


def _data_files(table) -> list[dict]:
    adds = pa.table(table.get_add_actions(flatten=True)).to_pylist()
    out = []
    for add in sorted(adds, key=lambda a: a["path"]):
        partition_values = {
            key.removeprefix("partition."): value for key, value in add.items() if key.startswith("partition.")
        }
        out.append(
            {
                "path": add["path"],
                "record_count": add["num_records"],
                "size_bytes": add["size_bytes"],
                "partition_values": partition_values,
            }
        )
    return out


# ---------------------------------------------------------------------------- schema evolution


def generate_add_column(out_dir: Path) -> dict:
    """Add a nullable column mid-history, then populate it in a later commit.

    Commit 0 writes the base schema. Commit 1 is `alter.add_columns`: a metadata-only commit, zero
    data files added or removed, that a reader must still report as changing the schema as-of that
    commit. Commit 2 appends rows that populate the new column with real values.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake
    from deltalake.schema import Field as DeltaField

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "accounts"

    base = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )
    write_deltalake(
        table_dir,
        pa.table({"id": [1, 2], "region": ["east", "west"], "amount": [10.0, 20.0]}, schema=base),
        mode="error",
        name="accounts",
    )

    table = DeltaTable(table_dir)
    table.alter.add_columns(DeltaField("bonus", "double", nullable=True))

    evolved = pa.schema(list(base) + [pa.field("bonus", pa.float64(), nullable=True)])
    write_deltalake(
        table_dir,
        pa.table(
            {"id": [3, 4], "region": ["east", "west"], "amount": [30.0, 40.0], "bonus": [3.5, 4.5]},
            schema=evolved,
        ),
        mode="append",
        schema_mode="merge",
    )

    table = DeltaTable(table_dir)
    commits = _delta_commits(table_dir)
    if commits[1]["added"] or commits[1]["removed"]:
        raise RuntimeError(f"expected the ADD COLUMN commit to touch no data files, got {commits[1]}")
    if commits[1]["schema_columns"] != ["id", "region", "amount", "bonus"]:
        raise RuntimeError(f"ADD COLUMN commit does not carry the evolved schema: {commits[1]}")

    manifest = {
        "format": "DELTA",
        "table_name": "accounts",
        "table_dir": "accounts",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "total_rows": sum(f["record_count"] for f in _data_files(table)),
        "data_file_count": len(_data_files(table)),
        "schema": _schema_out(table),
        "data_files": _data_files(table),
        "commits": commits,
        "added_column": "bonus",
        "schema_change_commit": "1",
        "populated_commit": "2",
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "Commit 1 is alter.add_columns(): a metadata-only commit that adds and removes no files.",
            "Commit 2 is the first commit whose files actually carry a value for 'bonus'.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_add_column_null(out_dir: Path) -> dict:
    """Add a nullable column, then write rows through schema_mode=merge that never set it.

    Distinct from generate_add_column: there the new column's file carries real values; here every
    row written after the column exists still leaves it null, which is the shape a producer that
    has not yet been updated to populate a new field takes in production.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake
    from deltalake.schema import Field as DeltaField

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "events"

    base = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("region", pa.string(), nullable=False)])
    write_deltalake(
        table_dir,
        pa.table({"id": [1, 2], "region": ["east", "west"]}, schema=base),
        mode="error",
        name="events",
    )

    table = DeltaTable(table_dir)
    table.alter.add_columns(DeltaField("label", "string", nullable=True))

    write_deltalake(
        table_dir,
        pa.table({"id": [3, 4], "region": ["east", "west"]}, schema=base),
        mode="append",
        schema_mode="merge",
    )

    table = DeltaTable(table_dir)
    commits = _delta_commits(table_dir)
    post_evolution_paths = set(commits[-1]["added"])
    if not post_evolution_paths:
        raise RuntimeError("the final commit added no files to check for a null 'label'")

    adds = pa.table(table.get_add_actions(flatten=True)).to_pylist()
    null_commit_files = []
    for add in adds:
        if add["path"] not in post_evolution_paths:
            # A file written before the column existed carries no statistics for it at all.
            continue
        null_count = add.get("null_count.label")
        num_records = add["num_records"]
        if null_count != num_records:
            raise RuntimeError(f"expected every row of {add['path']} to leave 'label' null, got {add}")
        null_commit_files.append({"path": add["path"], "record_count": num_records, "null_count": null_count})
    if not null_commit_files:
        raise RuntimeError("no post-evolution file carried null-count statistics for 'label'")

    # Cross-check against a full table read, independent of the statistics above.
    table_data = table.to_pyarrow_table()
    label_col = table_data.column("label")
    if label_col.null_count != 4:
        raise RuntimeError(f"expected all 4 rows to have a null 'label', to_pyarrow_table found {label_col.null_count}")

    manifest = {
        "format": "DELTA",
        "table_name": "events",
        "table_dir": "events",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "total_rows": sum(f["record_count"] for f in _data_files(table)),
        "data_file_count": len(_data_files(table)),
        "schema": _schema_out(table),
        "data_files": _data_files(table),
        "commits": _delta_commits(table_dir),
        "added_column": "label",
        "schema_change_commit": "1",
        "null_populated_commit": "2",
        "null_files": null_commit_files,
        "table_total_rows": table_data.num_rows,
        "table_null_count": label_col.null_count,
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "Every row written after 'label' was added still leaves it null: schema_mode=merge pads",
            "missing columns rather than requiring the writer to set them.",
            "null_files and table_null_count were cross-checked against deltalake's own",
            "get_add_actions() statistics and to_pyarrow_table() read, independently of each other.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_drop_column(out_dir: Path) -> dict:
    """Drop the trailing column of the schema via a schema_mode=overwrite rewrite.

    deltalake==1.6.3's write_deltalake(schema_mode="overwrite") turns out to accept only a new
    schema that drops *trailing* fields: dropping 'id' or 'region' out of (id, region, amount) both
    fail with "Schema error: No field named region/id", even though dropping 'amount' (the last
    field) succeeds. That asymmetry is itself worth recording rather than fighting -- see the note
    below -- so this fixture drops the trailing column; generate_reorder_columns is the dedicated
    fixture for position drift among survivors, which a trailing drop cannot exercise on its own.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "ledger"

    before = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )
    write_deltalake(
        table_dir,
        pa.table({"id": [1, 2, 3, 4], "region": ["east", "east", "west", "west"], "amount": [10.0, 20.0, 30.0, 40.0]}, schema=before),
        mode="error",
        name="ledger",
    )

    after = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("region", pa.string(), nullable=False)])
    write_deltalake(
        table_dir,
        pa.table({"id": [5, 6], "region": ["east", "west"]}, schema=after),
        mode="overwrite",
        schema_mode="overwrite",
    )

    table = DeltaTable(table_dir)
    commits = _delta_commits(table_dir)
    if not commits[1]["removed"] or not commits[1]["added"]:
        raise RuntimeError(f"expected the drop-column commit to both remove and add files, got {commits[1]}")
    if commits[1]["schema_columns"] != ["id", "region"]:
        raise RuntimeError(f"drop-column commit does not carry the narrowed schema: {commits[1]}")

    manifest = {
        "format": "DELTA",
        "table_name": "ledger",
        "table_dir": "ledger",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "total_rows": sum(f["record_count"] for f in _data_files(table)),
        "data_file_count": len(_data_files(table)),
        "schema": _schema_out(table),
        "data_files": _data_files(table),
        "commits": commits,
        "dropped_column": "amount",
        "drop_commit": "1",
        "columns_before": ["id", "region", "amount"],
        "columns_after": ["id", "region"],
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "Commit 1 drops 'amount', the trailing column, via schema_mode=overwrite: a",
            "whole-table rewrite. deltalake==1.6.3's write_deltalake refuses schema_mode=overwrite",
            "for a non-trailing drop -- dropping 'id' or 'region' out of the same starting schema",
            "both fail with 'Schema error: No field named ...' -- so a middle-column drop is not",
            "reproducible at this writer version; generate_reorder_columns covers position drift",
            "among surviving columns instead.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_rename_column(out_dir: Path) -> dict:
    """Rename a column by rewriting the table under a new name for the same values.

    deltalake 1.6.3 refuses column mapping outright (`delta.columnMapping.mode` raises "not
    supported for write operation ... yet" and there is no rename API), so the only way to rename a
    column at this writer version is to redefine the schema and rewrite the data — which the Delta
    log then records as an ordinary remove-and-add, with nothing that says "this used to be called
    'amount'". This is the fixture T57 asks for: it exists to show what a Delta reader with no
    column-mapping support actually does with a rename, not to assert an association the format
    cannot express at this writer version.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "payments"

    before = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )
    write_deltalake(
        table_dir,
        pa.table({"id": [1, 2, 3, 4], "region": ["east", "east", "west", "west"], "amount": [10.0, 20.0, 30.0, 40.0]}, schema=before),
        mode="error",
        name="payments",
    )

    after = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("total", pa.float64(), nullable=True),
        ]
    )
    write_deltalake(
        table_dir,
        pa.table({"id": [1, 2, 3, 4], "region": ["east", "east", "west", "west"], "total": [10.0, 20.0, 30.0, 40.0]}, schema=after),
        mode="overwrite",
        schema_mode="overwrite",
    )

    table = DeltaTable(table_dir)
    commits = _delta_commits(table_dir)
    if not commits[1]["removed"] or not commits[1]["added"]:
        raise RuntimeError(f"expected the rename commit to both remove and add files, got {commits[1]}")
    if commits[1]["schema_columns"] != ["id", "region", "total"]:
        raise RuntimeError(f"rename commit does not carry the renamed schema: {commits[1]}")

    manifest = {
        "format": "DELTA",
        "table_name": "payments",
        "table_dir": "payments",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "total_rows": sum(f["record_count"] for f in _data_files(table)),
        "data_file_count": len(_data_files(table)),
        "schema": _schema_out(table),
        "data_files": _data_files(table),
        "commits": commits,
        "old_name": "amount",
        "new_name": "total",
        "rename_commit": "1",
        "columns_before": ["id", "region", "amount"],
        "columns_after": ["id", "region", "total"],
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "deltalake==1.6.3 refuses delta.columnMapping.mode entirely ('not supported for write",
            "operation ... yet') and exposes no rename API, so this is a schema_mode=overwrite",
            "rewrite: the log's own commit 1 is a plain remove of the old file plus an add of the",
            "new one, with 'amount' gone from the schema and 'total' new. That is the ground truth",
            "this fixture exists to check a reader against, not an assumption of what should happen.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_widen_type(out_dir: Path) -> dict:
    """Widen an int column to long and a decimal's scale, in the same rewrite commit.

    deltalake 1.6.3's Python binding exposes no usable type-widening table feature (`TableFeatures`
    has no `TypeWidening` member) and schema_mode="merge" refuses to cast existing values down to
    the old narrower type, so this widens the only way available at this writer version: a
    schema_mode="overwrite" rewrite, same as the drop and rename fixtures. The column stays in the
    same schema position in both commits, isolating "type changed" from "position changed" — compare
    against generate_drop_column and generate_reorder_columns, where the position moves too.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "invoices"

    before = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("quantity", pa.int32(), nullable=True),
            pa.field("price", pa.decimal128(10, 2), nullable=True),
        ]
    )
    write_deltalake(
        table_dir,
        pa.table(
            {
                "id": [1, 2],
                "region": ["east", "west"],
                "quantity": pa.array([100, 200], type=pa.int32()),
                "price": [decimal.Decimal("1.23"), decimal.Decimal("4.56")],
            },
            schema=before,
        ),
        mode="error",
        name="invoices",
    )

    after = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("quantity", pa.int64(), nullable=True),
            pa.field("price", pa.decimal128(10, 4), nullable=True),
        ]
    )
    write_deltalake(
        table_dir,
        pa.table(
            {
                "id": [3, 4],
                "region": ["east", "west"],
                # A value that does not fit int32, proving the widen is load-bearing rather than cosmetic.
                "quantity": pa.array([3_000_000_000, 4_000_000_000], type=pa.int64()),
                "price": [decimal.Decimal("1.2345"), decimal.Decimal("6.7890")],
            },
            schema=after,
        ),
        mode="overwrite",
        schema_mode="overwrite",
    )

    table = DeltaTable(table_dir)
    commits = _delta_commits(table_dir)
    if not commits[1]["removed"] or not commits[1]["added"]:
        raise RuntimeError(f"expected the widen commit to both remove and add files, got {commits[1]}")

    manifest = {
        "format": "DELTA",
        "table_name": "invoices",
        "table_dir": "invoices",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "total_rows": sum(f["record_count"] for f in _data_files(table)),
        "data_file_count": len(_data_files(table)),
        "schema": _schema_out(table),
        "data_files": _data_files(table),
        "commits": commits,
        "widen_commit": "1",
        "widened_columns": {"quantity": {"before": "INT", "after": "LONG"}, "price": {"before": "DECIMAL(10,2)", "after": "DECIMAL(10,4)"}},
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "TableFeatures has no TypeWidening member in deltalake==1.6.3's Python binding, and",
            "schema_mode=merge refuses to cast the wider commit's values down to the narrower",
            "column, so this is a schema_mode=overwrite rewrite rather than the native Delta",
            "type-widening feature. 'quantity' and 'price' keep their schema position across the",
            "widen; only their type changes.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_reorder_columns(out_dir: Path) -> dict:
    """Reorder three columns without renaming or retyping any of them.

    Same values, same names, same types for the three original columns -- only their schema
    position changes, from (id, region, amount) to (region, amount, id) -- with one new trailing
    column ('tax') added in the same commit. The added column is not incidental: deltalake==1.6.3's
    writer compares the new schema's field set to the old one and, when a schema_mode=overwrite
    rewrite reorders fields without otherwise changing the set, decides nothing changed and writes
    no metaData action at all -- confirmed empirically, and the reason this fixture is not a pure
    reorder. table.schema() then keeps reporting the *original* field order forever, even though the
    physical Parquet file the rewrite produced is laid out in the new order; the log's declared
    schema and the file's actual column order silently diverge. Adding 'tax' forces delta-rs to
    decide the schema really did change and to write a metaData action carrying the new order, which
    is what makes the reorder observable to a Delta reader at all.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "shipments"

    before = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )
    write_deltalake(
        table_dir,
        pa.table({"id": [1, 2, 3, 4], "region": ["east", "east", "west", "west"], "amount": [10.0, 20.0, 30.0, 40.0]}, schema=before),
        mode="error",
        name="shipments",
    )

    after = pa.schema(
        [
            pa.field("region", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
            pa.field("id", pa.int64(), nullable=False),
            pa.field("tax", pa.float64(), nullable=True),
        ]
    )
    write_deltalake(
        table_dir,
        pa.table({"region": ["east", "west"], "amount": [50.0, 60.0], "id": [5, 6], "tax": [0.05, 0.06]}, schema=after),
        mode="overwrite",
        schema_mode="overwrite",
    )

    table = DeltaTable(table_dir)
    commits = _delta_commits(table_dir)
    if commits[1]["schema_columns"] != ["region", "amount", "id", "tax"]:
        raise RuntimeError(f"reorder commit does not carry the reordered schema: {commits[1]}")

    # A pure reorder -- no added column -- writes no metaData action at all: recorded here as
    # evidence for the note above, not asserted against, since it is what sent this fixture down
    # the "reorder plus one new column" path instead.

    manifest = {
        "format": "DELTA",
        "table_name": "shipments",
        "table_dir": "shipments",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "total_rows": sum(f["record_count"] for f in _data_files(table)),
        "data_file_count": len(_data_files(table)),
        "schema": _schema_out(table),
        "data_files": _data_files(table),
        "commits": commits,
        "reorder_commit": "1",
        "added_column": "tax",
        "columns_before": ["id", "region", "amount"],
        "columns_after": ["region", "amount", "id", "tax"],
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "Commit 1 keeps every original column's name and type and only changes their position,",
            "while also adding a new trailing column ('tax'). The addition is load-bearing: a pure",
            "reorder with no set change writes no metaData action at all in deltalake==1.6.3, so",
            "table.schema() would keep reporting the original field order forever even though the",
            "rewritten Parquet file is physically laid out in the new order. Adding a column forces",
            "delta-rs to decide the schema changed and to write the reordered metaData.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


# ---------------------------------------------------------------------------- deletions


def generate_deletes_then_compact(out_dir: Path) -> dict:
    """Delete rows out of one file, then compact the whole table on top of that delete.

    Neither existing delete fixture chains into a compaction, and neither existing compaction
    fixture starts from a table any delete has touched. Four tiny unpartitioned files are written,
    one row is deleted out of the second (a rewrite: remove the old file, add its 2-row
    replacement), and then optimize.compact() merges everything alive into one file. The row total
    must survive both operations: 8 written, 1 deleted, 7 live at the end.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "clicks"

    schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("amount", pa.float64(), nullable=True)])

    def batch(ids, amounts):
        return pa.table({"id": ids, "amount": amounts}, schema=schema)

    write_deltalake(table_dir, batch([1, 2], [1.5, 2.5]), mode="error", name="clicks")
    write_deltalake(table_dir, batch([3, 4], [3.5, 4.5]), mode="append")
    write_deltalake(table_dir, batch([5, 6], [5.5, 6.5]), mode="append")
    write_deltalake(table_dir, batch([7, 8], [7.5, 8.5]), mode="append")

    table = DeltaTable(table_dir)
    delete_result = table.delete("id = 3")
    if delete_result["num_added_files"] != 1 or delete_result["num_removed_files"] != 1:
        raise RuntimeError(f"expected a single-row rewrite delete, got {delete_result}")

    table = DeltaTable(table_dir)
    compaction = table.optimize.compact()
    if compaction["numFilesRemoved"] < 2:
        raise RuntimeError(f"expected compaction to merge multiple surviving files, got {compaction}")

    table = DeltaTable(table_dir)
    commits = _delta_commits(table_dir)
    delete_commit = next(c for c in commits if c["operation"] == "DELETE")
    compact_commit = commits[-1]
    if compact_commit["operation"] != "OPTIMIZE":
        raise RuntimeError(f"expected the final commit to be the compaction, got {compact_commit}")
    if not compact_commit["removed"] or len(compact_commit["added"]) != 1:
        raise RuntimeError(f"compaction commit does not read back as many-removed-one-added: {compact_commit}")

    data_files = _data_files(table)
    total_rows = sum(f["record_count"] for f in data_files)
    if total_rows != 7:
        raise RuntimeError(f"expected 7 live rows after an 8-row table loses one, got {total_rows}")
    if len(data_files) != 1:
        raise RuntimeError(f"expected exactly one file after compaction, got {len(data_files)}")

    manifest = {
        "format": "DELTA",
        "table_name": "clicks",
        "table_dir": "clicks",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "total_rows": total_rows,
        "data_file_count": len(data_files),
        "schema": _schema_out(table),
        "data_files": data_files,
        "commits": commits,
        "delete_commit": str(delete_commit["version"]),
        "compaction_commit": str(compact_commit["version"]),
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            f"Commit {delete_commit['version']} deletes id=3, rewriting its 2-row file to 1 row.",
            f"Commit {compact_commit['version']} then compacts every surviving file into one, with",
            "no row added, changed or removed -- the row total must stay at 7 through both.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_deletes_drain_partition(out_dir: Path) -> dict:
    """Delete every row of a partition one at a time, rather than in a single predicate.

    delta-rs-deletes (generate.py) covers a partition dropped by one predicate that aligns with the
    whole file. This fixture instead deletes the 'east' partition's three rows individually, so the
    file is rewritten twice (to 2 rows, then to 1) before the third delete finally has nothing left
    to rewrite and removes the file outright with no replacement -- the shape a partition actually
    takes when it drains gradually rather than by a single bulk predicate.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "sessions"

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )
    write_deltalake(
        table_dir,
        pa.table({"id": [1, 2, 3], "region": ["east", "east", "east"], "amount": [1.0, 2.0, 3.0]}, schema=schema),
        mode="error",
        partition_by=["region"],
        name="sessions",
    )
    write_deltalake(
        table_dir,
        pa.table({"id": [4, 5], "region": ["west", "west"], "amount": [4.0, 5.0]}, schema=schema),
        mode="append",
    )

    table = DeltaTable(table_dir)
    drain_results = []
    for row_id in (1, 2, 3):
        table = DeltaTable(table_dir)
        drain_results.append(table.delete(f"id = {row_id}"))

    if drain_results[0]["num_added_files"] != 1 or drain_results[0]["num_removed_files"] != 1:
        raise RuntimeError(f"expected the first drain delete to rewrite the file, got {drain_results[0]}")
    if drain_results[1]["num_added_files"] != 1 or drain_results[1]["num_removed_files"] != 1:
        raise RuntimeError(f"expected the second drain delete to rewrite the file, got {drain_results[1]}")
    if drain_results[2]["num_added_files"] != 0 or drain_results[2]["num_removed_files"] != 1:
        raise RuntimeError(f"expected the final drain delete to remove the file with no replacement, got {drain_results[2]}")

    table = DeltaTable(table_dir)
    commits = _delta_commits(table_dir)
    delete_commits = [c for c in commits if c["operation"] == "DELETE"]
    if len(delete_commits) != 3:
        raise RuntimeError(f"expected 3 delete commits, got {len(delete_commits)}")

    data_files = _data_files(table)
    partitions_left = sorted({f["partition_values"].get("region") for f in data_files})
    if partitions_left != ["west"]:
        raise RuntimeError(f"expected only the 'west' partition to survive, got {partitions_left}")

    manifest = {
        "format": "DELTA",
        "table_name": "sessions",
        "table_dir": "sessions",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "total_rows": sum(f["record_count"] for f in data_files),
        "data_file_count": len(data_files),
        "schema": _schema_out(table),
        "partition_columns": ["region"],
        "data_files": data_files,
        "commits": commits,
        "drain_commits": [str(c["version"]) for c in delete_commits],
        "final_removal_commit": str(delete_commits[-1]["version"]),
        "drained_partition": "east",
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "The 'east' partition's three rows are deleted one at a time: the first two deletes",
            "each rewrite the file (remove old, add narrower replacement); the third removes the",
            "file outright with no replacement, since nothing is left to keep.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_log_retention_fallback(out_dir: Path) -> dict:
    """Write a table, capture its first commit's real timestamp, then let log retention erase it.

    This is the fixture the resume-from-an-expired-instant test needs and delta-rs-checkpoint
    (generate.py) does not quite give it: that fixture never records what the erased commits'
    timestamps actually were, so a Go test can only pick an arbitrary small number and confirm it
    reads as unsafe -- true, but it never confirms the safe side of the same comparison against a
    real, still-retained instant. This generator reads _delta_log/00000000000000000000.json's own
    commitInfo.timestamp before cleanup_metadata() deletes it, and records both that timestamp and
    the first surviving commit's, so the Go test can assert IsIncrementalSyncSafeFrom is true from
    the retained instant and false from the erased one.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "metrics"

    schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("amount", pa.float64(), nullable=True)])

    def batch(ids, amounts):
        return pa.table({"id": ids, "amount": amounts}, schema=schema)

    write_deltalake(
        table_dir,
        batch([1, 2], [1.0, 2.0]),
        mode="error",
        name="metrics",
        configuration={
            "delta.logRetentionDuration": "interval 0 days",
            "delta.enableExpiredLogCleanup": "true",
        },
    )
    erased_log = table_dir / "_delta_log" / "00000000000000000000.json"
    erased_commit_line = next(line for line in erased_log.read_text().splitlines() if "commitInfo" in line)
    erased_timestamp = json.loads(erased_commit_line)["commitInfo"]["timestamp"]

    write_deltalake(table_dir, batch([3, 4], [3.0, 4.0]), mode="append")
    write_deltalake(table_dir, batch([5, 6], [5.0, 6.0]), mode="append")

    table = DeltaTable(table_dir)
    table.create_checkpoint()
    table.cleanup_metadata()

    write_deltalake(table_dir, batch([7, 8], [7.0, 8.0]), mode="append")

    table = DeltaTable(table_dir)
    log_files = sorted(p.name for p in (table_dir / "_delta_log").iterdir())
    if "00000000000000000000.json" in log_files:
        raise RuntimeError("cleanup_metadata left the version 0 commit; fixture must not carry it")

    surviving_versions = sorted(
        int(p.stem) for p in (table_dir / "_delta_log").glob("*.json")
    )
    first_surviving_log = table_dir / "_delta_log" / f"{surviving_versions[0]:020d}.json"
    retained_commit_line = next(
        line for line in first_surviving_log.read_text().splitlines() if "commitInfo" in line
    )
    retained_timestamp = json.loads(retained_commit_line)["commitInfo"]["timestamp"]

    if retained_timestamp <= erased_timestamp:
        raise RuntimeError("the first surviving commit is not actually later than the erased one")

    data_files = _data_files(table)
    manifest = {
        "format": "DELTA",
        "table_name": "metrics",
        "table_dir": "metrics",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "total_rows": sum(f["record_count"] for f in data_files),
        "data_file_count": len(data_files),
        "schema": _schema_out(table),
        "data_files": data_files,
        "log_files": log_files,
        "erased_commit_timestamp_ms": erased_timestamp,
        "retained_first_commit_timestamp_ms": retained_timestamp,
        "first_surviving_version": surviving_versions[0],
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "erased_commit_timestamp_ms is version 0's real commitInfo.timestamp, read before",
            "cleanup_metadata() deleted its JSON commit. IsIncrementalSyncSafeFrom must report",
            "true from retained_first_commit_timestamp_ms and false from an instant at or before",
            "erased_commit_timestamp_ms, since that commit's JSON no longer exists to verify against.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


FIXTURES = {
    "evolution-add-column": lambda out_root: generate_add_column(out_root / "evolution-add-column"),
    "evolution-add-column-null": lambda out_root: generate_add_column_null(out_root / "evolution-add-column-null"),
    "evolution-drop-column": lambda out_root: generate_drop_column(out_root / "evolution-drop-column"),
    "evolution-rename-column": lambda out_root: generate_rename_column(out_root / "evolution-rename-column"),
    "evolution-widen-type": lambda out_root: generate_widen_type(out_root / "evolution-widen-type"),
    "evolution-reorder-columns": lambda out_root: generate_reorder_columns(out_root / "evolution-reorder-columns"),
    "deletes-then-compact": lambda out_root: generate_deletes_then_compact(out_root / "deletes-then-compact"),
    "deletes-drain-partition": lambda out_root: generate_deletes_drain_partition(out_root / "deletes-drain-partition"),
    "evolution-log-retention-fallback": lambda out_root: generate_log_retention_fallback(
        out_root / "evolution-log-retention-fallback"
    ),
}


def _describe(name: str, manifest: dict) -> str:
    return f"{name}: {manifest['commit_count']} commits, {manifest['data_file_count']} files, {manifest['total_rows']} rows"


def main() -> int:
    out_root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_OUT
    names = sys.argv[2:] or list(FIXTURES)
    unknown = [n for n in names if n not in FIXTURES]
    if unknown:
        print(f"unknown fixture(s): {', '.join(unknown)}; known: {', '.join(FIXTURES)}", file=sys.stderr)
        return 1

    out_root.mkdir(parents=True, exist_ok=True)
    for name in names:
        manifest = FIXTURES[name](out_root)
        print(_describe(name, manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
