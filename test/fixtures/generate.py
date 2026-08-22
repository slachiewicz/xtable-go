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

"""Write the table fixtures under test/testdata/fixtures with real engines.

Every other test in this repository reads a table polytable itself wrote, so a reader that agrees
with polytable's own writer passes even when it disagrees with the format. These fixtures come from
delta-rs and pyiceberg instead, and `test/foreign_fixtures_test.go` asserts polytable's readers
against the `manifest.json` this script emits next to each one.

Run it from a virtualenv that is not committed:

    python3 -m venv .venv
    .venv/bin/pip install deltalake==1.6.3 pyarrow==25.0.1 'pyiceberg[sql-sqlite]==0.11.1'
    .venv/bin/python test/fixtures/generate.py

With no arguments this rewrites every fixture under `test/testdata/fixtures`. Each generator mints
its own table UUIDs, file names and commit timestamps per run, so a full run churns every fixture
already committed to the tree even when only one of them changed. Pass the output directory and one
or more fixture names (see `FIXTURES` below) to regenerate only those:

    .venv/bin/python test/fixtures/generate.py test/testdata/fixtures delta-rs-deletes

The install line is pinned to what every fixture in the tree was regenerated with most recently,
and is the writer version each new fixture should be added at unless the point of the fixture is a
version boundary — `delta-rs/sales` predates the pin and still records `deltalake` 1.6.2 in its own
manifest.json for exactly that reason: regenerating it would erase evidence of a version this repo
has already tested against, for no benefit. Re-running this script never rewrites the pin to match
whatever is installed; it is a separate, deliberate edit when the fixtures move to a newer writer.

Determinism has limits the writers impose. Row values, row counts, column order, partition values
and the commit sequence are fixed here, so the metadata a rerun produces describes the same table.
Commit timestamps, table UUIDs, snapshot IDs and the generated data file names are not: both
writers mint them per run. The Go tests therefore assert against manifest.json, which is regenerated
with the fixture, and never against a literal from a previous run.

The Iceberg fixture records absolute locations, which Iceberg's metadata format requires. Every
occurrence of the generation-time warehouse path in a *.metadata.json is rewritten to the
PATH_PLACEHOLDER token below, and the Go test substitutes the directory it copied the fixture into.
The paths embedded in the Avro manifests are left as generated: they cannot be rewritten without
re-encoding the file. `relocateAvroManifests` in test/foreign_fixtures_test.go does exactly that
re-encoding when it loads the fixture, under the file's own schema and header.
"""

import datetime
import json
import math
import os
import shutil
import sys
import tempfile
import uuid
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa

PATH_PLACEHOLDER = "file:///__POLYTABLE_FIXTURE_ROOT__"

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO_ROOT / "test" / "testdata" / "fixtures"

# Canonical type names shared by both manifests, so one Go assertion covers both fixtures. They are
# the names of polytable's model.Type constants; the mapping from each writer's own type names is
# spelled out below rather than inferred.
DELTA_TYPE_NAMES = {"long": "LONG", "string": "STRING", "double": "DOUBLE"}
ICEBERG_TYPE_NAMES = {"long": "LONG", "string": "STRING", "double": "DOUBLE"}


def _rmtree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


def _write_manifest(directory: Path, manifest: dict) -> None:
    with (directory / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")


# ---------------------------------------------------------------------------- Delta (delta-rs)


def generate_delta(out_dir: Path) -> dict:
    """Write a three-commit partitioned Delta table with a mid-history column addition."""
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "sales"

    base_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )
    # region values stay URL-safe on purpose: delta-rs percent-encodes the partition directory in
    # the add action's path and polytable's reader does not decode it, so a value needing an escape
    # would fold a second, unrelated question into this fixture.
    commit_one = pa.table(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "region": ["east", "east", "east", "west", "west", "west"],
            "amount": [10.5, 20.25, 30.0, 40.75, 50.5, 60.0],
        },
        schema=base_schema,
    )
    commit_two = pa.table(
        {
            "id": [7, 8, 9, 10],
            "region": ["east", "east", "west", "west"],
            "amount": [70.25, 80.0, 90.5, 100.75],
        },
        schema=base_schema,
    )
    evolved_schema = pa.schema(list(base_schema) + [pa.field("discount", pa.float64(), nullable=True)])
    commit_three = pa.table(
        {
            "id": [11, 12, 13, 14],
            "region": ["east", "east", "west", "west"],
            "amount": [110.0, 120.5, 130.25, 140.0],
            "discount": [1.5, 2.5, 3.5, 4.5],
        },
        schema=evolved_schema,
    )

    write_deltalake(table_dir, commit_one, mode="error", partition_by=["region"], name="sales")
    write_deltalake(table_dir, commit_two, mode="append", partition_by=["region"])
    write_deltalake(
        table_dir, commit_three, mode="append", partition_by=["region"], schema_mode="merge"
    )

    table = DeltaTable(table_dir)
    # deltalake 1.x returns an arro3 table; the Arrow PyCapsule interface carries it into pyarrow.
    adds = pa.table(table.get_add_actions(flatten=True)).to_pylist()

    data_files = []
    for add in sorted(adds, key=lambda a: a["path"]):
        data_files.append(
            {
                "path": add["path"],
                "record_count": add["num_records"],
                "size_bytes": add["size_bytes"],
                "partition_values": {"region": add["partition.region"]},
            }
        )

    schema = [
        {
            "name": field.name,
            "type": DELTA_TYPE_NAMES[field.type.type],
            "nullable": field.nullable,
        }
        for field in table.schema().fields
    ]

    def _bounds(column: str) -> dict:
        lows = [a[f"min.{column}"] for a in adds if a.get(f"min.{column}") is not None]
        highs = [a[f"max.{column}"] for a in adds if a.get(f"max.{column}") is not None]
        return {"min": min(lows), "max": max(highs)}

    manifest = {
        "format": "DELTA",
        "table_name": "sales",
        "table_dir": "sales",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "latest_commit_id": str(table.version()),
        "total_rows": sum(f["record_count"] for f in data_files),
        "data_file_count": len(data_files),
        "schema": schema,
        "partition_columns": ["region"],
        "partition_values": sorted({f["partition_values"]["region"] for f in data_files}),
        "column_bounds": {"id": _bounds("id"), "amount": _bounds("amount")},
        "data_files": data_files,
        "schema_evolution": {
            "added_column": "discount",
            "added_at_commit": str(table.version()),
        },
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "Commit 2 adds the 'discount' column, so the files of commits 0 and 1 predate it.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_delta_checkpoint(out_dir: Path) -> dict:
    """Write a Delta table whose pre-checkpoint JSON commits have been cleaned up.

    This is the shape a production table takes once Spark or delta-rs expires old log
    entries: the only complete copy of the table state (metaData and protocol included)
    lives in the Parquet checkpoint, and a reader that only replays JSON commits cannot
    reconstruct it.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "orders"

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )

    def batch(ids, regions, amounts):
        return pa.table({"id": ids, "region": regions, "amount": amounts}, schema=schema)

    write_deltalake(
        table_dir,
        batch([1, 2], ["east", "west"], [10.5, 20.0]),
        mode="error",
        partition_by=["region"],
        name="orders",
        configuration={
            "delta.logRetentionDuration": "interval 0 days",
            "delta.enableExpiredLogCleanup": "true",
        },
    )
    write_deltalake(table_dir, batch([3, 4], ["east", "south"], [30.0, 40.0]), mode="append")
    write_deltalake(table_dir, batch([5], ["west"], [50.0]), mode="append")

    table = DeltaTable(table_dir)
    table.create_checkpoint()
    table.cleanup_metadata()

    # One commit after the checkpoint, so a reader must both load the checkpoint and
    # replay the JSON tail on top of it.
    write_deltalake(table_dir, batch([6], ["north"], [60.0]), mode="append")

    table = DeltaTable(table_dir)
    log_files = sorted(p.name for p in (table_dir / "_delta_log").iterdir())
    removed = [f"{v:020d}.json" for v in (0, 1)]
    for name in removed:
        if name in log_files:
            raise RuntimeError(f"cleanup_metadata left {name}; fixture must not carry it")

    adds = pa.table(table.get_add_actions(flatten=True)).to_pylist()
    data_files = [
        {
            "path": add["path"],
            "record_count": add["num_records"],
            "size_bytes": add["size_bytes"],
            "partition_values": {"region": add["partition.region"]},
        }
        for add in sorted(adds, key=lambda a: a["path"])
    ]

    manifest = {
        "format": "DELTA",
        "table_name": "orders",
        "table_dir": "orders",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "latest_commit_id": str(table.version()),
        "checkpoint_version": 2,
        "log_files": log_files,
        "removed_log_files": removed,
        "total_rows": sum(f["record_count"] for f in data_files),
        "data_file_count": len(data_files),
        "partition_columns": ["region"],
        "partition_values": sorted({f["partition_values"]["region"] for f in data_files}),
        "data_files": data_files,
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "cleanup_metadata() deleted the version 0 and 1 JSON commits itself; the",
            "checkpoint at version 2 is the only source of the metaData and protocol state.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def _delta_commits(table_dir: Path) -> list[dict]:
    """Read every _delta_log/*.json commit and record its add and remove paths, in commit order.

    This is what T46 needs that the other two Delta generators do not: a per-commit record of which
    paths were added and which were removed, read straight out of the log the writer produced rather
    than inferred from the final `get_add_actions()` snapshot. A snapshot cannot tell a `remove`
    action happened at all once the same path is gone from both the old and new file list.
    """
    commits = []
    for log_file in sorted((table_dir / "_delta_log").glob("*.json")):
        added: list[str] = []
        removed: list[str] = []
        operation = ""
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
        commits.append(
            {
                "version": int(log_file.stem),
                "operation": operation,
                "added": sorted(added),
                "removed": sorted(removed),
            }
        )
    return commits


def generate_delta_deletes(out_dir: Path) -> dict:
    """Write a Delta table with a real `remove` action, from `DeltaTable.delete()`.

    Every fixture generate_delta() produces is append-only (T46's evidence table: zero `remove`
    actions in any committed fixture). This one carries two delete commits shaped differently on
    purpose:

      - version 2 deletes the whole `east` partition, which drops the file outright: a pure remove,
        zero adds, in one commit.
      - version 3 deletes a single row out of a multi-row `west` file, which forces delta-rs to
        rewrite the file: a remove of the old file and an add of the replacement, in the same commit.
        This is the shape a partial delete takes in production, and it is the one most likely to
        expose a reader that reports commit-level diffs as adds-only.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "returns"

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )

    def batch(ids, regions, amounts):
        return pa.table({"id": ids, "region": regions, "amount": amounts}, schema=schema)

    write_deltalake(
        table_dir,
        batch([1, 2, 3], ["east", "east", "east"], [10.0, 20.0, 30.0]),
        mode="error",
        partition_by=["region"],
        name="returns",
    )
    write_deltalake(table_dir, batch([4, 5, 6], ["west", "west", "west"], [40.0, 50.0, 60.0]), mode="append")

    table = DeltaTable(table_dir)
    partition_delete = table.delete("region = 'east'")
    if partition_delete["num_added_files"] != 0 or partition_delete["num_removed_files"] != 1:
        raise RuntimeError(f"expected a pure partition delete, got {partition_delete}")

    table = DeltaTable(table_dir)
    row_delete = table.delete("id = 5")
    if row_delete["num_added_files"] != 1 or row_delete["num_removed_files"] != 1:
        raise RuntimeError(f"expected a rewrite delete (add + remove), got {row_delete}")

    table = DeltaTable(table_dir)
    commits = _delta_commits(table_dir)
    if not any(c["removed"] and not c["added"] for c in commits):
        raise RuntimeError("fixture must carry a commit that removes files without adding any")
    if not any(c["removed"] and c["added"] for c in commits):
        raise RuntimeError("fixture must carry a commit that both adds and removes files")

    adds = pa.table(table.get_add_actions(flatten=True)).to_pylist()
    data_files = [
        {
            "path": add["path"],
            "record_count": add["num_records"],
            "size_bytes": add["size_bytes"],
            "partition_values": {"region": add["partition.region"]},
        }
        for add in sorted(adds, key=lambda a: a["path"])
    ]

    schema_out = [
        {
            "name": field.name,
            "type": DELTA_TYPE_NAMES[field.type.type],
            "nullable": field.nullable,
        }
        for field in table.schema().fields
    ]

    manifest = {
        "format": "DELTA",
        "table_name": "returns",
        "table_dir": "returns",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "latest_commit_id": str(table.version()),
        "total_rows": sum(f["record_count"] for f in data_files),
        "data_file_count": len(data_files),
        "schema": schema_out,
        "partition_columns": ["region"],
        "partition_values": sorted({f["partition_values"]["region"] for f in data_files}),
        "data_files": data_files,
        "commits": commits,
        "delete_commits": {
            "partition_delete": str(2),
            "rewrite_delete": str(3),
        },
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "Commit 2 deletes the whole 'east' partition: a remove with no compensating add.",
            "Commit 3 deletes one row ('id = 5') out of a multi-row 'west' file, which delta-rs",
            "rewrites as a remove of the old file and an add of the replacement in the same commit.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_delta_compaction(out_dir: Path) -> dict:
    """Write an unpartitioned Delta table, then run `optimize.compact()` over four tiny files.

    Unpartitioned is deliberate and is new coverage on its own: every other Delta fixture in this
    tree is partitioned by region, so an unpartitioned table has never gone through
    `convertAddAction`'s empty-`partitionValues` path or an unpartitioned conversion target.

    Compaction is the case T46 asks for that a row-level delete does not cover: the file set changes
    completely — every one of the four small files is replaced by one big one — while not a single
    row is added, changed or removed. `dataChange` on both the four removes and the one add is
    `false`, which is what marks this as a metadata-only rewrite rather than a data change.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "clicks"

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("amount", pa.float64(), nullable=True),
        ]
    )

    def batch(ids, amounts):
        return pa.table({"id": ids, "amount": amounts}, schema=schema)

    write_deltalake(table_dir, batch([1, 2], [1.5, 2.5]), mode="error", name="clicks")
    write_deltalake(table_dir, batch([3, 4], [3.5, 4.5]), mode="append")
    write_deltalake(table_dir, batch([5, 6], [5.5, 6.5]), mode="append")
    write_deltalake(table_dir, batch([7, 8], [7.5, 8.5]), mode="append")

    table = DeltaTable(table_dir)
    rows_before = sum(pa.table(table.get_add_actions(flatten=True)).to_pylist()[i]["num_records"] for i in range(4))

    compaction = table.optimize.compact()
    if compaction["numFilesAdded"] != 1 or compaction["numFilesRemoved"] != 4:
        raise RuntimeError(f"expected 4 files compacted into 1, got {compaction}")

    table = DeltaTable(table_dir)
    commits = _delta_commits(table_dir)
    compact_commit = commits[-1]
    if not compact_commit["removed"] or len(compact_commit["added"]) != 1:
        raise RuntimeError(f"the compaction commit does not read back as add+removes: {compact_commit}")

    adds = pa.table(table.get_add_actions(flatten=True)).to_pylist()
    if len(adds) != 1:
        raise RuntimeError(f"expected exactly one file after compaction, got {len(adds)}")
    data_files = [
        {
            "path": add["path"],
            "record_count": add["num_records"],
            "size_bytes": add["size_bytes"],
            "partition_values": {},
        }
        for add in adds
    ]
    rows_after = sum(f["record_count"] for f in data_files)
    if rows_after != rows_before:
        raise RuntimeError(f"compaction changed row count: {rows_before} -> {rows_after}")

    schema_out = [
        {
            "name": field.name,
            "type": DELTA_TYPE_NAMES[field.type.type],
            "nullable": field.nullable,
        }
        for field in table.schema().fields
    ]

    manifest = {
        "format": "DELTA",
        "table_name": "clicks",
        "table_dir": "clicks",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "commit_count": table.version() + 1,
        "latest_commit_id": str(table.version()),
        "total_rows": rows_after,
        "data_file_count": len(data_files),
        "schema": schema_out,
        "partition_columns": [],
        "partition_values": [],
        "data_files": data_files,
        "commits": commits,
        "compaction_commit": str(compact_commit["version"]),
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "Unpartitioned, unlike every other Delta fixture in this tree.",
            f"Commit {compact_commit['version']} is optimize.compact(): it removes the four files",
            "the four prior commits each added and replaces them with one, with no row changed.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_delta_torture(out_dir: Path) -> dict:
    """Write an unpartitioned Delta table carrying the value classes T-torture asks for: decimals at
    the precision boundaries where the backing width changes, both Delta timestamp kinds, floats that
    only survive with full precision, strings and binary with the usual escaping hazards, and struct /
    list / map nesting with null at every level.

    Every other Delta fixture in this tree is a handful of long/string/double columns; this one is
    deliberately as wide as delta-rs will accept in one write, so that a reader agreeing with itself
    on `id`, `region` and `amount` says nothing about whether it also agrees on `decimal(38,37)` or a
    map holding a null value.

    Four rows, not more: each carries a distinct null/edge shape rather than one row per column, since
    the class this fixture exists to cover is "does this value survive", not "does this row count add
    up" (`generate_delta` and its siblings already own that).

      - row 0: the high extreme of every bounded column, and every nested value present and non-null.
      - row 1: the low extreme, and a struct whose leaf fields are null without the struct itself
        being null (a struct present with null members, not a null struct).
      - row 2: a run of column-level nulls (decimal, struct, list, map all null at that column), a
        NaN float, and the DST-boundary timestamp both as a genuine instant and as a local naive
        wall-clock value that is itself ambiguous in America/New_York.
      - row 3: a map holding a null *value* — deliberately distinct from row 1's empty map and row 2's
        null map, per the task's #828 note that these are three different states worth having.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "torture"

    struct_type = pa.struct(
        [
            pa.field("inner_a", pa.string(), nullable=True),
            pa.field("inner_struct", pa.struct([pa.field("deep", pa.int64(), nullable=True)]), nullable=True),
        ]
    )
    list_type = pa.list_(pa.field("element", pa.struct([pa.field("x", pa.int64(), nullable=True)]), nullable=True))
    map_type = pa.map_(pa.string(), pa.field("value", pa.int64(), nullable=True))

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("dec38_0", pa.decimal128(38, 0), nullable=True),
            pa.field("dec38_37", pa.decimal128(38, 37), nullable=True),
            pa.field("dec9_2", pa.decimal128(9, 2), nullable=True),
            pa.field("dec18_4", pa.decimal128(18, 4), nullable=True),
            pa.field("ts_tz", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("ts_ntz", pa.timestamp("us"), nullable=True),
            pa.field("f64", pa.float64(), nullable=True),
            pa.field("str_col", pa.string(), nullable=True),
            pa.field("str_nullable", pa.string(), nullable=True),
            pa.field("bin_col", pa.binary(), nullable=True),
            pa.field("struct1", struct_type, nullable=True),
            pa.field("list1", list_type, nullable=True),
            pa.field("map1", map_type, nullable=True),
        ]
    )

    utc = datetime.timezone.utc
    nyc = ZoneInfo("America/New_York")
    # 2024-11-03 01:30 America/New_York falls in the repeated hour: EDT (UTC-4) rolls back to EST
    # (UTC-5) at 02:00 EDT, so this local wall-clock time occurs twice that day. Attaching the zone
    # picks the first (pre-fallback, EDT) occurrence; the point of including it at all is that this is
    # a real value production data carries, not that the ambiguity itself needs resolving here — a
    # Delta `timestamp` column stores an absolute UTC instant regardless.
    dst_instant = datetime.datetime(2024, 11, 3, 1, 30, 0, tzinfo=nyc)

    data = {
        "id": [1, 2, 3, 4],
        "dec38_0": [Decimal("9" * 38), Decimal("-" + "9" * 38), None, Decimal("0")],
        "dec38_37": [
            Decimal("9." + "9" * 37),
            Decimal("-9." + "9" * 37),
            None,
            Decimal("0." + "0" * 36 + "1"),
        ],
        "dec9_2": [Decimal("1234567.89"), Decimal("-1234567.89"), Decimal("0.00"), Decimal("0.01")],
        "dec18_4": [
            Decimal("99999999999999.9999"),
            Decimal("-99999999999999.9999"),
            Decimal("0.0000"),
            Decimal("0.0001"),
        ],
        "ts_tz": [
            datetime.datetime(2026, 1, 1, 0, 0, 0, tzinfo=utc),
            datetime.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=utc),
            dst_instant,
            datetime.datetime(2025, 6, 15, 12, 0, 0, tzinfo=utc),
        ],
        "ts_ntz": [
            datetime.datetime(2026, 1, 1, 0, 0, 0, 123456),
            datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
            datetime.datetime(2024, 11, 3, 1, 30, 0),
            datetime.datetime(2025, 6, 15, 12, 0, 0),
        ],
        # A float64 needing every significant digit to round-trip, and negative zero, which compares
        # equal to positive zero under IEEE 754 but is a distinct bit pattern. NaN is row 2's, not
        # this row's or a partition column's, per the task's instruction.
        "f64": [1.0 / 3.0, -0.0, math.nan, 1.5],
        "str_col": ["héllo wörld 日本語", "line1\nline2\ttab", "", "emoji é̈ combining"],
        "str_nullable": ["value", "", None, "x"],
        "bin_col": [b"\x00\x01\xff", b"", None, b"\xff"],
        "struct1": [
            {"inner_a": "hello", "inner_struct": {"deep": 42}},
            {"inner_a": None, "inner_struct": None},
            None,
            {"inner_a": "world", "inner_struct": {"deep": 7}},
        ],
        "list1": [
            [{"x": 1}, {"x": 2}],
            [],
            None,
            [{"x": 3}],
        ],
        "map1": [
            {"a": 1, "b": 2},
            {},
            None,
            {"a": None},
        ],
    }
    table = pa.table(data, schema=schema)

    # Both delta-rs and the Parquet stats it derives from accept a NaN float and a Decimal(38, x)
    # value outside int64 range without complaint; write_deltalake would raise if either were
    # actually rejected, so reaching get_add_actions below is itself the record of that.
    write_deltalake(table_dir, table, mode="error", name="torture")
    table_handle = DeltaTable(table_dir)

    log_path = table_dir / "_delta_log" / "00000000000000000000.json"
    raw_stats = None
    for line in log_path.read_text(encoding="utf-8").splitlines():
        action = json.loads(line)
        if "add" in action:
            raw_stats = json.loads(action["add"]["stats"])
            break
    if raw_stats is None:
        raise RuntimeError("torture fixture wrote no add action with stats")

    # Only the leaf scalar columns: delta-rs computes no min/max at all for bin_col, list1 or map1,
    # and reports struct1's bound as a nested object rather than a per-leaf scalar. Both are real
    # behavior, not a gap in this script, and are recorded in notes instead of column_bounds.
    scalar_columns = [
        "dec38_0", "dec38_37", "dec9_2", "dec18_4", "ts_tz", "ts_ntz", "f64", "str_col", "str_nullable",
    ]
    column_bounds = {
        col: {"min": raw_stats["minValues"].get(col), "max": raw_stats["maxValues"].get(col)}
        for col in scalar_columns
        if col in raw_stats["minValues"]
    }

    type_schema = [
        {"name": "id", "type": "LONG", "nullable": False},
        {"name": "dec38_0", "type": "DECIMAL", "nullable": True, "precision": 38, "scale": 0},
        {"name": "dec38_37", "type": "DECIMAL", "nullable": True, "precision": 38, "scale": 37},
        {"name": "dec9_2", "type": "DECIMAL", "nullable": True, "precision": 9, "scale": 2},
        {"name": "dec18_4", "type": "DECIMAL", "nullable": True, "precision": 18, "scale": 4},
        {"name": "ts_tz", "type": "TIMESTAMP", "nullable": True},
        {"name": "ts_ntz", "type": "TIMESTAMP_NTZ", "nullable": True},
        {"name": "f64", "type": "DOUBLE", "nullable": True},
        {"name": "str_col", "type": "STRING", "nullable": True},
        {"name": "str_nullable", "type": "STRING", "nullable": True},
        {"name": "bin_col", "type": "BYTES", "nullable": True},
        {
            "name": "struct1",
            "type": "RECORD",
            "nullable": True,
            "fields": [
                {"name": "inner_a", "type": "STRING", "nullable": True},
                {
                    "name": "inner_struct",
                    "type": "RECORD",
                    "nullable": True,
                    "fields": [{"name": "deep", "type": "LONG", "nullable": True}],
                },
            ],
        },
        {
            "name": "list1",
            "type": "LIST",
            "nullable": True,
            "element": {
                "type": "RECORD",
                "nullable": True,
                "fields": [{"name": "x", "type": "LONG", "nullable": True}],
            },
        },
        {
            "name": "map1",
            "type": "MAP",
            "nullable": True,
            "key": {"type": "STRING", "nullable": False},
            "value": {"type": "LONG", "nullable": True},
        },
    ]

    manifest = {
        "format": "DELTA",
        "table_name": "torture",
        "table_dir": "torture",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "total_rows": 4,
        "data_file_count": 1,
        "type_schema": type_schema,
        "column_bounds": column_bounds,
        "null_counts": raw_stats["nullCount"],
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "dec38_0's min/max in minValues/maxValues are clamped to int64 range "
            f"({raw_stats['minValues']['dec38_0']}..{raw_stats['maxValues']['dec38_0']}) even though "
            "the actual column values are the 38-nines int128 extremes: delta-rs's own JSON stats "
            "serialization loses precision here, before polytable ever reads the file. This is a "
            "writer-side limitation of the format's JSON stats representation, not something a "
            "reader can recover.",
            "dec38_37's min/max are JSON floating-point numbers, so the scale that makes this a "
            "decimal(38,37) column rather than a decimal(38,0) one is not preserved in the stats "
            "either — 9.999...9 (37 nines) round-trips through delta-rs's stats as 9.999999999999998.",
            "bin_col, list1 and map1 carry no minValues/maxValues entry at all: delta-rs computes no "
            "column statistics for binary, list or map columns.",
            "struct1's reported bound is the nested {inner_a, inner_struct} object itself, not a "
            "per-leaf scalar — Delta's stats format nests structs rather than flattening them.",
            "NaN is accepted in f64 (row index 2) and delta-rs excludes it from both minValues and "
            "maxValues, which is why max.f64 is 1.5 rather than NaN.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


def generate_delta_partition_torture(out_dir: Path) -> dict:
    """Write a Delta table partitioned by a string column carrying the partition-value hazards the
    task calls out by name: a null partition value (Delta and Hive's own #828 defect family), an
    empty string (which is not the same state as null but that a naive reader can conflate with it),
    and a value that needs percent-encoding to become a path segment.
    """
    import deltalake
    from deltalake import DeltaTable, write_deltalake

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)
    table_dir = out_dir / "part_torture"

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("region", pa.string(), nullable=True),
        ]
    )
    table = pa.table(
        {"id": [1, 2, 3, 4], "region": ["east", None, "", "north america/100%"]},
        schema=schema,
    )
    write_deltalake(table_dir, table, mode="error", partition_by=["region"], name="part_torture")
    table_handle = DeltaTable(table_dir)

    # partitionValues is read from the log directly rather than get_add_actions(flatten=True): the
    # flattened `partition.region` column collapses a null partition value and an empty-string one to
    # the same Python None, which would silently erase the distinction this fixture exists to record.
    log_path = table_dir / "_delta_log" / "00000000000000000000.json"
    entries = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        action = json.loads(line)
        if "add" not in action:
            continue
        add = action["add"]
        entries.append(
            {
                "path": add["path"],
                "record_count": json.loads(add["stats"])["numRecords"],
                # None here means the log's own partitionValues JSON literally said null, not that
                # this script chose to omit the key.
                "partition_value": add["partitionValues"]["region"],
            }
        )
    entries.sort(key=lambda e: e["path"])

    manifest = {
        "format": "DELTA",
        "table_name": "part_torture",
        "table_dir": "part_torture",
        "writer": {"library": "deltalake", "version": deltalake.__version__},
        "total_rows": sum(e["record_count"] for e in entries),
        "data_file_count": len(entries),
        "partition_columns": ["region"],
        "data_files": entries,
        "notes": [
            "Written by delta-rs; polytable has never touched this directory.",
            "The null-partition row's data/ directory is literally named "
            "__HIVE_DEFAULT_PARTITION__, Delta and Hive's sentinel for a null partition value; the "
            "empty-string row's directory is 'region=' (empty). Both directories decode to '' if a "
            "reader treats an absent add.partitionValues entry and a JSON null the same way it treats "
            "the empty string, which is the #828 defect family this fixture targets.",
            "The percent-encoded row's physical directory name is double percent-encoded by delta-rs "
            "(the literal value 'north america/100%' becomes "
            "'north%2520america%252F100%2525' on disk, i.e. the once-encoded name encoded a second "
            "time) — this fixture records that as an observation, not an assertion: decoding a "
            "partition value out of the physical directory name is path-handling code this suite does "
            "not own. add.partitionValues.region itself carries the raw, unencoded string, which is "
            "what polytable's Delta reader actually parses.",
        ],
    }
    _write_manifest(out_dir, manifest)
    return manifest


# ---------------------------------------------------------------------------- Iceberg (pyiceberg)


def generate_iceberg(out_dir: Path) -> dict:
    """Write a three-snapshot partitioned Iceberg table with a mid-history column addition."""
    import pyiceberg
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)

    # The catalog database is catalog state, not table state, so it is built in a throwaway
    # directory and never copied into the fixture.
    staging = Path(tempfile.mkdtemp(prefix="polytable-pyiceberg-"))
    try:
        warehouse = staging / "warehouse"
        warehouse.mkdir()
        catalog = SqlCatalog(
            "fixture",
            **{
                "uri": f"sqlite:///{staging / 'catalog.db'}",
                "warehouse": f"file://{warehouse}",
            },
        )
        catalog.create_namespace("lake")

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="category", field_type=StringType(), required=True),
            NestedField(field_id=3, name="value", field_type=DoubleType(), required=False),
        )
        spec = PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=IdentityTransform(), name="category"
            )
        )
        table = catalog.create_table("lake.events", schema=schema, partition_spec=spec)

        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("category", pa.string(), nullable=False),
                pa.field("value", pa.float64(), nullable=True),
            ]
        )
        table.append(
            pa.table(
                {
                    "id": [1, 2, 3, 4],
                    "category": ["alpha", "alpha", "beta", "beta"],
                    "value": [1.5, 2.5, 3.5, 4.5],
                },
                schema=arrow_schema,
            )
        )
        table.append(
            pa.table(
                {
                    "id": [5, 6, 7, 8],
                    "category": ["alpha", "alpha", "beta", "beta"],
                    "value": [5.5, 6.5, 7.5, 8.5],
                },
                schema=arrow_schema,
            )
        )

        with table.update_schema() as update:
            update.add_column("label", StringType())

        evolved_schema = pa.schema(list(arrow_schema) + [pa.field("label", pa.string(), nullable=True)])
        table.append(
            pa.table(
                {
                    "id": [9, 10, 11, 12],
                    "category": ["alpha", "alpha", "beta", "beta"],
                    "value": [9.5, 10.5, 11.5, 12.5],
                    "label": ["nine", "ten", "eleven", "twelve"],
                },
                schema=evolved_schema,
            )
        )

        table.refresh()
        files = sorted(table.inspect.files().to_pylist(), key=lambda f: f["file_path"])
        snapshots = table.snapshots()
        current = table.current_snapshot()

        table_location = table.location()
        source_dir = Path(table_location.removeprefix("file://"))
        target_dir = out_dir / "events"
        shutil.copytree(source_dir, target_dir)

        # Iceberg stores absolute locations. Rewrite them in the JSON metadata to a placeholder the
        # Go test substitutes; the Avro manifests keep the generation-time paths.
        for metadata_file in sorted(target_dir.glob("metadata/*.metadata.json")):
            text = metadata_file.read_text(encoding="utf-8")
            metadata_file.write_text(text.replace(table_location, PATH_PLACEHOLDER), encoding="utf-8")

        metadata_versions = sorted(
            int(p.name.split("-", 1)[0]) for p in target_dir.glob("metadata/*.metadata.json")
        )

        manifest = {
            "format": "ICEBERG",
            "table_name": "events",
            "table_dir": "events",
            "writer": {"library": "pyiceberg", "version": pyiceberg.__version__},
            "path_placeholder": PATH_PLACEHOLDER,
            "format_version": table.format_version,
            "snapshot_count": len(snapshots),
            "metadata_versions": metadata_versions,
            "latest_metadata_version": metadata_versions[-1],
            "current_snapshot_id": str(current.snapshot_id),
            "total_rows": sum(f["record_count"] for f in files),
            "data_file_count": len(files),
            "schema": [
                {
                    "name": field.name,
                    "type": ICEBERG_TYPE_NAMES[str(field.field_type)],
                    "nullable": not field.required,
                    "field_id": field.field_id,
                }
                for field in table.schema().fields
            ],
            "partition_columns": ["category"],
            "partition_values": sorted({f["partition"]["category"] for f in files}),
            "data_files": [
                {
                    "path": f["file_path"].removeprefix(table_location).lstrip("/"),
                    "record_count": f["record_count"],
                    "size_bytes": f["file_size_in_bytes"],
                    "partition_values": {"category": f["partition"]["category"]},
                }
                for f in files
            ],
            "schema_evolution": {
                "added_column": "label",
                "added_before_snapshot": str(current.snapshot_id),
            },
            "manifest_encoding": "avro",
            "notes": [
                "Written by pyiceberg; polytable has never touched this directory.",
                "manifest-list and manifest files are Avro OCF, which is what the Iceberg spec"
                " mandates and what every real writer emits.",
                "File paths inside the Avro manifests are the generation-time absolute paths and"
                " are not relocatable.",
            ],
        }
        _write_manifest(out_dir, manifest)
        return manifest
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def generate_iceberg_deletes(out_dir: Path) -> dict:
    """Write an Iceberg table with real removals: T40's evidence that GetTableChangeForCommit and
    GetChangesSince invented a target with no removal path at all (`model.NewFilesDiff(snap.DataFiles,
    nil)` — every live file reported as an add, `FilesRemoved` always nil).

    `table.overwrite(df, overwrite_filter=...)` turns out, in pyiceberg 0.11.1, not to be one
    snapshot: it commits a pure delete of the overwritten partition's old file first and the
    replacement as a separate append second. `table.delete()` on a predicate that does not align
    with a whole file's rows takes the other shape, a single copy-on-write snapshot that removes the
    old file and adds its rewritten replacement in the same commit. Both shapes matter to T40 and
    this fixture is not written to prefer one over the other; it takes whichever pyiceberg produces
    and records it.

    Every operation here is asserted copy-on-write (`total-delete-files` stays "0" throughout): a
    positional- or equality-delete file would not appear as a removed data file at all under
    `model.DiffFiles`'s current-generation scope, and a fixture that silently used merge-on-read
    instead would stop exercising the removal path T40 exists to fix.
    """
    import pyiceberg
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import DoubleType, LongType, NestedField, StringType

    _rmtree(out_dir)
    out_dir.mkdir(parents=True)

    staging = Path(tempfile.mkdtemp(prefix="polytable-pyiceberg-deletes-"))
    try:
        warehouse = staging / "warehouse"
        warehouse.mkdir()
        catalog = SqlCatalog(
            "fixture",
            **{
                "uri": f"sqlite:///{staging / 'catalog.db'}",
                "warehouse": f"file://{warehouse}",
            },
        )
        catalog.create_namespace("lake")

        schema = Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="category", field_type=StringType(), required=True),
            NestedField(field_id=3, name="value", field_type=DoubleType(), required=False),
        )
        spec = PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=IdentityTransform(), name="category"
            )
        )
        table = catalog.create_table("lake.returns", schema=schema, partition_spec=spec)

        arrow_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("category", pa.string(), nullable=False),
                pa.field("value", pa.float64(), nullable=True),
            ]
        )

        # Snapshot 1: append the alpha partition (3 rows, one file).
        table.append(
            pa.table(
                {"id": [1, 2, 3], "category": ["alpha", "alpha", "alpha"], "value": [1.0, 2.0, 3.0]},
                schema=arrow_schema,
            )
        )
        # Snapshot 2: append the beta partition (3 rows, one file).
        table.append(
            pa.table(
                {"id": [4, 5, 6], "category": ["beta", "beta", "beta"], "value": [4.0, 5.0, 6.0]},
                schema=arrow_schema,
            )
        )
        # Snapshots 3-4: overwrite the whole beta partition with different rows. pyiceberg commits
        # this as a pure delete of the old beta file followed by a pure append of the new one.
        table.overwrite(
            pa.table(
                {"id": [7, 8], "category": ["beta", "beta"], "value": [7.0, 8.0]}, schema=arrow_schema
            ),
            overwrite_filter="category == 'beta'",
        )
        # Snapshot 5: delete one row out of the three-row alpha file. Because the predicate does not
        # align with a whole file, pyiceberg rewrites: the old alpha file is removed and a
        # two-row replacement is added, both in this one commit.
        table.delete("id = 2")

        table.refresh()
        files = sorted(table.inspect.files().to_pylist(), key=lambda f: f["file_path"])
        snapshots = table.snapshots()
        current = table.current_snapshot()

        table_location = table.location()

        def _relative(path: str) -> str:
            return path.removeprefix(table_location).lstrip("/")

        # Ground truth for the per-commit add/remove assertion: each snapshot's complete live file
        # set, diffed against the previous snapshot's. This is read straight out of pyiceberg's own
        # accounting (table.inspect.files(snapshot_id=...)) rather than inferred from the summary
        # counters, so it does not depend on the Go reader agreeing with itself.
        snapshot_records = []
        previous_paths: set[str] = set()
        for snap in snapshots:
            snap_files = table.inspect.files(snapshot_id=snap.snapshot_id).to_pylist()
            current_paths = {f["file_path"] for f in snap_files}
            added = sorted(_relative(p) for p in current_paths - previous_paths)
            removed = sorted(_relative(p) for p in previous_paths - current_paths)
            operation = str(snap.summary.operation).removeprefix("Operation.")
            if snap.summary.additional_properties.get("total-delete-files", "0") != "0":
                raise RuntimeError(
                    f"snapshot {snap.snapshot_id} ({operation}) wrote delete files; this fixture "
                    "requires copy-on-write throughout so every removal is a missing data file"
                )
            snapshot_records.append(
                {
                    "snapshot_id": str(snap.snapshot_id),
                    "operation": operation,
                    "added": added,
                    "removed": removed,
                }
            )
            previous_paths = current_paths

        if not any(r["removed"] and not r["added"] for r in snapshot_records):
            raise RuntimeError("fixture must carry a snapshot that removes files without adding any")
        if not any(r["removed"] and r["added"] for r in snapshot_records):
            raise RuntimeError("fixture must carry a snapshot that both adds and removes files")
        if not any(not r["removed"] and r["added"] for r in snapshot_records):
            raise RuntimeError("fixture must carry a snapshot that only adds files")

        source_dir = Path(table_location.removeprefix("file://"))
        target_dir = out_dir / "returns"
        shutil.copytree(source_dir, target_dir)

        for metadata_file in sorted(target_dir.glob("metadata/*.metadata.json")):
            text = metadata_file.read_text(encoding="utf-8")
            metadata_file.write_text(text.replace(table_location, PATH_PLACEHOLDER), encoding="utf-8")

        metadata_versions = sorted(
            int(p.name.split("-", 1)[0]) for p in target_dir.glob("metadata/*.metadata.json")
        )

        manifest = {
            "format": "ICEBERG",
            "table_name": "returns",
            "table_dir": "returns",
            "writer": {"library": "pyiceberg", "version": pyiceberg.__version__},
            "path_placeholder": PATH_PLACEHOLDER,
            "format_version": table.format_version,
            "snapshot_count": len(snapshots),
            "metadata_versions": metadata_versions,
            "latest_metadata_version": metadata_versions[-1],
            "current_snapshot_id": str(current.snapshot_id),
            "total_rows": sum(f["record_count"] for f in files),
            "data_file_count": len(files),
            "schema": [
                {
                    "name": field.name,
                    "type": ICEBERG_TYPE_NAMES[str(field.field_type)],
                    "nullable": not field.required,
                    "field_id": field.field_id,
                }
                for field in table.schema().fields
            ],
            "partition_columns": ["category"],
            "partition_values": sorted({f["partition"]["category"] for f in files}),
            "data_files": [
                {
                    "path": _relative(f["file_path"]),
                    "record_count": f["record_count"],
                    "size_bytes": f["file_size_in_bytes"],
                    "partition_values": {"category": f["partition"]["category"]},
                }
                for f in files
            ],
            "iceberg_snapshots": snapshot_records,
            "manifest_encoding": "avro",
            "notes": [
                "Written by pyiceberg; polytable has never touched this directory.",
                "manifest-list and manifest files are Avro OCF, which is what the Iceberg spec"
                " mandates and what every real writer emits.",
                "File paths inside the Avro manifests are the generation-time absolute paths and"
                " are not relocatable.",
                "table.overwrite() committed as two snapshots here (a pure delete then a pure"
                " append) rather than one; table.delete() on a non-aligned predicate committed as"
                " a single remove+add rewrite. iceberg_snapshots records what pyiceberg actually"
                " did, not what generate_iceberg_deletes assumed it would do.",
            ],
        }
        _write_manifest(out_dir, manifest)
        return manifest
    finally:
        shutil.rmtree(staging, ignore_errors=True)


# Every fixture this script can write, keyed by the name `main`'s optional filter argument
# selects. Each generator mints its own table UUIDs, file names and timestamps per run, so running
# an entry that is not being worked on churns a fixture already committed to the tree for no reason
# — the filter lets a change to one generator regenerate only that fixture.
FIXTURES = {
    "delta-rs": lambda out_root: generate_delta(out_root / "delta-rs"),
    "delta-rs-checkpoint": lambda out_root: generate_delta_checkpoint(out_root / "delta-rs-checkpoint"),
    "delta-rs-deletes": lambda out_root: generate_delta_deletes(out_root / "delta-rs-deletes"),
    "delta-rs-compaction": lambda out_root: generate_delta_compaction(out_root / "delta-rs-compaction"),
    "pyiceberg": lambda out_root: generate_iceberg(out_root / "pyiceberg"),
    "pyiceberg-deletes": lambda out_root: generate_iceberg_deletes(out_root / "pyiceberg-deletes"),
}


def _describe(name: str, manifest: dict) -> str:
    if "checkpoint_version" in manifest:
        return (
            f"{name}: checkpoint at v{manifest['checkpoint_version']}, "
            f"{manifest['data_file_count']} files, {manifest['total_rows']} rows"
        )
    if "snapshot_count" in manifest:
        return (
            f"{name}: {manifest['snapshot_count']} snapshots, {manifest['data_file_count']} files, "
            f"{manifest['total_rows']} rows"
        )
    return (
        f"{name}: {manifest['commit_count']} commits, {manifest['data_file_count']} files, "
        f"{manifest['total_rows']} rows"
    )


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

    total = sum(
        os.path.getsize(os.path.join(root, name))
        for root, _, names in os.walk(out_root)
        for name in names
    )
    print(f"total fixture size: {total / 1024:.1f} KiB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
