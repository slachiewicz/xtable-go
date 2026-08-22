<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Interoperability coverage

This page maps what polytable's interoperability is actually verified to do,
against what it merely does not fail at. Those are different claims, and
collapsing them is how untested paths pass as tested ones.

A conversion that succeeds is not a conversion that is correct. Neither
polytable's own log nor a reader that shares code lineage with the writer can
tell the difference between the two. Only a foreign implementation — a reader
with no shared code path to the writer — can.

## Interoperability is five axes, not one

A coverage claim means nothing until it names which cell of this grid it
refers to:

1. **Format pair** — for example Delta to Iceberg, Iceberg to Delta, or any
   pair involving Hudi or Paimon.
2. **Writer of the source table** — delta-spark, delta-rs, Databricks,
   Fabric, and Snowflake all emit valid Delta metadata, and no two of them
   emit the same metadata.
3. **Reader used to verify the result** — and, critically, whether that
   reader shares code lineage with the writer that produced the input.
4. **Storage backend** — local disk, S3, MinIO, Azure ADLS Gen2, GCS, or an
   S3-compatible store such as Cloudflare R2.
5. **Catalog**, or the absence of one.

The matrix below covers the format-pair and reader axes together, since a
"verified" claim is only meaningful when it also says what verified it. The
sections that follow cover the writer, storage, and catalog axes.

## Coverage matrix

The following table lists each format pair or source that polytable
converts, its coverage status, and the reader or service that established
that status. Placeholders stand in for account and resource identifiers from
live runs.

| Format pair or source | Status | Verified by |
| :--- | :--- | :--- |
| Delta → Iceberg | Verified | DuckDB, cross-checked on six fixtures (`test/equivalence_duckdb_test.go`), using `iceberg_scan` |
| Iceberg → Delta | Verified | DuckDB, same harness, using `delta_scan` (delta-kernel-rs) |
| Delta ↔ Hudi | Untested | No independent reader exists for Hudi anywhere in this project; DuckDB cannot read it |
| Any format → Paimon | Untested | No engine has ever read polytable's Paimon output |
| Snowflake (source) → Delta, on S3 | Verified | DuckDB row count, sum, and distinct count matched Snowflake's own query results |
| Snowflake (source) → Delta, on GCS | Verified | Same DuckDB checks, against a GCS external volume |
| Snowflake (source) → Delta, on Azure ADLS Gen2 | Verified | DuckDB, plus an independent confirmation from delta-rs |
| Snowflake-managed Iceberg (source), catalog-vended credentials | Verified | Read succeeded with every AWS credential unset; the same table addressed by path, without vended credentials, fails in the same shell |
| Fabric (source) | Untested | No live Fabric workspace has been reached; the tenant used for this work has Fabric disabled |

"Verified" in this table means a foreign reader or a live service confirmed
the result, not that polytable's own test suite passed. polytable's suite
was green throughout every defect this page documents.

## Format pairs

Delta to Iceberg and Iceberg to Delta are the only pairs cross-checked
against a foreign reader, over six fixtures in
`test/equivalence_duckdb_test.go`. Every other pair polytable supports —
anything touching Hudi or Paimon — has never been read by anything other
than polytable itself.

Hudi has no independent reader anywhere in this project, and DuckDB cannot
read it. A Trino-based suite is being attempted; treat that as the current
state, not as a solved problem. Every foreign-reader check that exists today
is Delta or Iceberg, which means a third of polytable's target formats has
never been checked by anything but the tool that wrote it.

Paimon is in the same position, and worse on one count: its manifests are
written as JSON, where the specification says Avro (tracked as T34 in
[the improvement plan](improvement-plan.md)). No engine has ever read
polytable's Paimon output.

## Writers

The writer axis is nearly empty. Every Delta and Iceberg fixture used for
verification in this project was written by delta-rs or PyIceberg. Nothing
here has consumed a table written by delta-spark, Databricks, or Fabric.

This gap is not one-sided. The upstream Apache XTable session that produced
several of the facts on this page used fixtures that were all delta-spark
3.2.0, and those fixtures were never swapped with this project's. Between
the two projects, neither has tested the other's writer — both call their
own input "valid Delta," and both are right, and that is exactly the
failure mode this page exists to name.

Fabric inbound is untested by anyone. The belief that Fabric emits an
`XTABLE_METADATA` marker comes from Microsoft's own documentation sample,
not from a live call: no Fabric workspace has been reached, because the
tenant available for this work has the feature disabled.

## Readers used as oracles

A reader only counts as an oracle if it does not share code lineage with the
writer under test. The readers actually used here are DuckDB (`delta_scan`,
backed by delta-kernel-rs, and `iceberg_scan`) and delta-rs, used as an
independent second check on the Azure path-comparison defect described
below.

The following independent implementations exist per format. This list is
sourced from the upstream Apache XTable session rather than exercised in
this repository — treat it as a reference for choosing an oracle, not as a
coverage claim:

- **Delta**: delta-kernel-rs, Delta Kernel Java, Trino.
- **Iceberg**: PyIceberg, iceberg-rust, DuckDB, Trino.
- **Hudi**: hudi-rs, Trino.

## Storage backends

Storage is deliberately not a priority for further interoperability
testing. The upstream project measured a 26x difference in small-table sync
time — roughly two seconds — that traced almost entirely to JVM and Spark
startup cost, not to storage semantics. That ratio has no reason to carry
over to a Go binary, which is a reason to measure it directly in this
project rather than assume the upstream numbers transfer, not a reason to
spend further effort testing storage interoperability itself.

The exception is an S3-compatible store that is not AWS, such as R2, MinIO,
or Wasabi: that exercises client assumptions about the S3 API rather than
storage semantics, and is worth testing on those grounds. Local disk, S3,
MinIO, Azure ADLS Gen2 (both a real account and the Azurite emulator), GCS,
and Cloudflare R2 have all been exercised as storage backends in this
project; see [Cloud storage](cloud-storage.md), [Amazon S3 and AWS
Glue](aws.md), [Azure Data Lake Storage and OneLake](azure.md), and
[Cloudflare R2 and R2 Data Catalog](cloudflare.md) for the details of each.

## Catalogs

Live catalogs exercised in this project: AWS Glue (SigV4-signed requests,
and separately against its API via `motoserver/moto`), Amazon S3 Tables,
Cloudflare R2 Data Catalog, Snowflake Horizon, and Apache Polaris (OAuth2,
run as a container). See [Sync to the AWS Glue Data
Catalog](glue-catalog.md) and [Sync to an Iceberg REST
catalog](iceberg-rest-catalog.md).

The Hive Metastore catalog type is declared but not implemented; see
[Features and limitations](features-and-limitations.md).

## Seven defects found in one day

On 2026-08-22, a single day of testing against foreign readers and live
services found seven real defects in polytable, every one of them missed by
polytable's own test suite, which stayed green throughout:

1. Delta's `partitionColumns: null` for an unpartitioned table. delta-kernel-rs
   refuses the whole transaction log over this, which means every
   unpartitioned Delta table polytable had ever written was unreadable by
   DuckDB.
2. Iceberg's `PartitionSpec.fields: null` — the same nil-slice defect in the
   other conversion target, found by an equivalence harness within minutes
   of the harness existing.
3. `ParquetSchemaToModel` panicked on any Parquet file containing a map or
   list column.
4. Catalog-vended credentials dropped the AWS region, producing the exact
   `PermanentRedirect` error the credential-vending feature exists to
   prevent.
5. An absolute `add.path` written on Azure, because `.dfs.` and `.blob.`
   compare as different strings. Confirmed as polytable's defect rather
   than the reader's by two independent checks: the same code path wrote a
   *relative* path on S3, and delta-rs 1.6.3 fails to read the Azure output
   in the same way DuckDB does.
6. An unrecognized URI scheme mistaken for a relative path and silently
   mangled: `gcs://BUCKET/TABLE/FILE.parquet` became the relative path
   `gcs:/BUCKET/TABLE/FILE.parquet`. Reachable because Snowflake writes
   `gcs://` paths; the GCS run in this project's own verification escaped
   the bug only because Snowflake had already normalized the scheme to
   `gs://` in its metadata.
7. OAuth2 authentication required a `clientId` value that Snowflake
   rejects outright, and reports back as the misleading error
   `invalid_scope`.

The common thread: a foreign reader or a live service caught every one of
these, and polytable's own suite caught none of them. That is the argument
for the rest of this page — a green test suite is evidence the tool didn't
crash, not evidence the output is correct.

## What is not verified

State these limits directly rather than letting a passing build imply
otherwise:

- **Hudi has no independent reader anywhere.** See [Format
  pairs](#format-pairs).
- **Paimon likewise**, and its manifest format itself deviates from the
  specification (T34).
- **The writer axis is nearly empty.** See [Writers](#writers).
- **Fabric inbound is untested by anyone.** See [Writers](#writers).
- **Concurrency is untested.** polytable writes Iceberg's
  `version-hint.text` (`pkg/formats/iceberg/target.go:337`), which is not
  an atomic operation on object storage. Two concurrent syncs, or a sync
  racing another writer, could lose an update, and nothing in this project
  has tested that scenario. Upstream Apache XTable carries the same
  exposure.
- **Iceberg v3 is unsupported.** Until 2026-08-22 it was not even refused
  outright (T65).
- **Deletion vectors are dropped silently** (T24).

## Report an interoperability gap

If you find a format pair, writer, or reader combination that fails
silently or produces output only polytable itself can read back, report it
in this repository's issue tracker with the writer and reader you used —
that pairing is exactly the information this page is short on.
