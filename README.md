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

# polytable

[![CI](https://github.com/slachiewicz/polytable/actions/workflows/ci.yml/badge.svg)](https://github.com/slachiewicz/polytable/actions/workflows/ci.yml)
[![Integration Tests](https://github.com/slachiewicz/polytable/actions/workflows/integration.yml/badge.svg)](https://github.com/slachiewicz/polytable/actions/workflows/integration.yml)
[![Security & SAST](https://github.com/slachiewicz/polytable/actions/workflows/security.yml/badge.svg)](https://github.com/slachiewicz/polytable/actions/workflows/security.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/slachiewicz/polytable)](https://goreportcard.com/report/github.com/slachiewicz/polytable)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> **Not an Apache Software Foundation project.** `polytable` is an independent, unofficial Go port of
> [Apache XTable (incubating)](https://github.com/apache/incubator-xtable). It is not affiliated with,
> endorsed by, or supported by the ASF, and it is not an Apache release. "Apache", "Apache XTable",
> "Apache Iceberg", "Apache Hudi" and "Apache Paimon" are trademarks of The Apache Software
> Foundation, used here only to identify the upstream projects this software derives from or
> interoperates with. For the official project, see [xtable.apache.org](https://xtable.apache.org).

**polytable** is a lightweight, ultra-high performance, **zero-JVM** lakehouse metadata translation engine written in pure Go. It facilitates **omni-directional, zero-copy interoperability** across open lakehouse table formats (**Delta Lake**, **Apache Iceberg**, **Apache Hudi**, **Apache Paimon**, and **Raw Parquet**) without rewriting underlying data files.

---

## 🌟 Why an independent Go port?

[Apache XTable (incubating)](https://github.com/apache/incubator-xtable) is a JVM project: it ships
as a bundled jar and brings Spark and Hadoop machinery with it. That is a reasonable shape inside a
data platform, but metadata translation itself is a small job — read one format's metadata, write
another's, never touch the data files — and running it in a Lambda function, a Kubernetes sidecar,
a CI step or a laptop means paying JVM cold starts and container-image weight for milliseconds of
actual work. AWS's own reference architecture for running XTable in Lambda needs a Maven build
inside the Dockerfile and a Python-to-Java bridge to make it fit. A native reimplementation removes
that tax entirely, and makes embeddings the JVM cannot offer (C ABI, WASM) possible at all:

- ⚡ **Instant Execution**: Native static binary (13.9 MiB stripped) with **zero JVM boot latency** — 6.6 ms start to exit, measured; see `SPEC.md` section 9.2.
- 🛡️ **Pure Go & Zero-JVM**: No Spark, Hadoop XML, Java, or Scala runtime dependencies required.
- 🔄 **Omni-Directional Sync**: Any format $\longleftrightarrow$ Any format (e.g., Delta $\to$ Iceberg & Hudi; Parquet $\to$ Delta $\to$ Iceberg; Hudi $\to$ Delta).
- 📦 **Deletion Vectors**: Delta deletion-vector descriptors are translated as descriptors, leaving the roaring bitmap payload untouched. Iceberg and Hudi are not covered yet.
- 📈 **Column Statistics**: min/max bounds and null counts survive conversion between Delta, Iceberg and Parquet, so engines can keep pruning files on the converted table.
- 🌐 **Ubiquitous Embeddability**:
  - **CLI Tool** (`polytable`)
  - **Continuous REST Daemon & Sidecar** (`polytable-service` with OpenAPI 3.0.3)
  - **Python SDK** (`polytable` via ctypes C ABI)
  - **C-Shared Dynamic Library** (`libpolytable.so` / `libpolytable.dylib`)
  - **WebAssembly Engine** (`polytable.wasm`) — ⚠️ **experimental**, see [WebAssembly status](#webassembly-status)
- ☁️ **Cloud Native Storage & Catalogs**: Native AWS S3 (`aws-sdk-go-v2`), AWS Glue Data Catalog, and Iceberg REST Catalog (Polaris, Unity, Nessie, Tabular).

---

## 🤝 Relationship to Apache XTable

This project is **not affiliated with the Apache Software Foundation in any way**. It is an
independent reimplementation of the translation model published by
[apache/incubator-xtable](https://github.com/apache/incubator-xtable): the canonical pivot types
mirror upstream's `Internal*` model, and parity with upstream is tracked task-by-task in
[`docs/improvement-plan.md`](docs/improvement-plan.md). Tables synced by either tool interoperate —
both embed the same sync-continuity properties (`xtable_last_instant_synced`,
`xtable_source_format`), so a table can move between them without a resync. If you want the
official JVM implementation, use the upstream project.

---

## 🧭 Design principles

- **Metadata only, never data.** polytable translates and generates table metadata; it never
  rewrites, moves, or decodes physical data files. Deletion vectors travel as descriptors with the
  bitmap payload untouched. This is invariant INV-1 in `SPEC.md` and it bounds what the tool can
  ever cost you: a bad sync is a bad metadata directory, not a damaged table.
- **Depend on codecs, own the table semantics.** Serialization libraries are welcome
  (`parquet-go` for footers, Avro for Iceberg manifests); table-format *implementations* are not.
  Manifest structures, log actions, and timeline semantics are implemented natively against each
  format's spec — so behavior tracks the spec, not another implementation's release cadence, and
  every byte written can be audited in this repository.
- **Verified against real engines, not just itself.** A converter that only round-trips its own
  output will hide any bug that is symmetrical in its reader and writer. The test suite reads
  tables written by real engines (delta-rs, pyiceberg — checked-in fixtures under
  `test/testdata/fixtures/`) and has real engines read polytable's output (DuckDB in CI). Both
  directions have already caught bugs no self-referential test could see.
- **Honest capability claims.** Where a feature is partial or absent, the docs say so plainly —
  the format matrix above marks gaps with `—`, and `docs/improvement-plan.md` records what was
  found, fixed, declined, and why.

---

## 📊 Format Support Matrix

| Format | Source (Reader) | Target (Writer) | Schema Evolution | Partitioning | Column Statistics | Deletion Vectors |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Delta Lake** | ✅ | ✅ | ✅ (Field IDs) | ✅ | ✅ (read + write) | ✅ (Roaring Bitmap Descriptor) |
| **Apache Iceberg** (v2/v3) | ✅ | ✅ | ✅ (Field IDs) | ✅ (Transforms) | ✅ (read + write) | — |
| **Apache Hudi** | ✅ | ✅ | ✅ (Avro Schema) | ✅ | — | — |
| **Raw Parquet** | ✅ | ✅ | ✅ (Schema Crawler) | ✅ (Hive Style) | ✅ (source) | — |
| **Apache Paimon** | ✅ | ✅ | ✅ (Type Mapping) | ✅ | — | — |

---

## 🚀 Quickstart

### 1. Installation

```bash
# Clone repository
git clone https://github.com/slachiewicz/polytable.git
cd polytable

# Build all binaries
make build
```

### 2. Run Interactive Demo

```bash
make demo
```

### 3. CLI Synchronization (`polytable sync`)

Create a dataset configuration file `dataset.yaml`:

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
  - HUDI
datasets:
  - tableName: financial_events
    tableBasePath: s3://my-lakehouse-bucket/tables/financial_events
    syncMode: INCREMENTAL
```

Run synchronization:

```bash
./bin/polytable sync --config dataset.yaml
```

Or let the AWS Glue Data Catalog name the tables instead of a config file. A table opts in by
carrying a `polytable_target_formats` table property listing its targets (comma-separated, for
example `ICEBERG,HUDI`); tables without it are skipped, and each table's location comes from the
catalog:

```bash
./bin/polytable sync --catalog glue --database analytics
```

### 4. CLI Table Inspection (`polytable inspect`)

```bash
# Inspect Delta table
./bin/polytable inspect --path ./data/my_table --format DELTA

# Inspect Iceberg table
./bin/polytable inspect --path ./data/my_table --format ICEBERG

# Inspect raw Parquet directory
./bin/polytable inspect --path ./data/raw_parquet --format PARQUET
```

---

## 🛰️ Continuous REST Daemon (`polytable-service`)

Run `polytable-service` as a standalone REST server or Kubernetes sidecar:

```bash
./bin/polytable-service --port 8080 --daemon --interval 10s --config dataset.yaml
```

### OpenAPI REST API Endpoints:

- `POST /v1/conversion/table`: Trigger synchronous table translation.
- `POST /v1/conversion/table/async`: Trigger background asynchronous translation.
- `GET /v1/conversion/table/{id}`: Poll status of async translation task.
- `POST /v1/conversion/inspect`: Inspect table metadata and schema over HTTP.
- `GET /v1/health`: Liveness probe (`{"status":"UP","version":"0.1.0-SNAPSHOT"}`).

See full specification in [`spec/rest-service-open-api.yaml`](./spec/rest-service-open-api.yaml).

---

## 🐍 Python SDK (`polytable`)

Use polytable natively in Python without JVM or PySpark:

```python
from polytable import sync, inspect, version

print(f"Using polytable engine: {version()}")

# Perform zero-copy sync in Python
result = sync(
    source_format="DELTA",
    target_formats=["ICEBERG", "HUDI"],
    table_name="customers",
    table_base_path="/data/lakehouse/customers",
    sync_mode="INCREMENTAL"
)
print("Sync Result:", result)
```

---

## 🧪 Testing & Verification

```bash
# Run unit tests
make test

# Run tests with race detection
make test-race

# Run Docker container integration tests (MinIO S3 & Tabular Iceberg REST)
make test-containers

# Run linter
make lint
```

---

## 📚 Guides

Step-by-step guides:
- 🚀 [Create your first interoperable table](docs/how-to.md) — Delta → Iceberg + Hudi locally and on S3/MinIO, verified with DuckDB
- 🗄️ [Sync to the AWS Glue Data Catalog](docs/glue-catalog.md) — registration, table discovery, and source resolution
- 🧊 [Sync to an Iceberg REST catalog](docs/iceberg-rest-catalog.md) — Nessie, Polaris, Unity Catalog
- ☁️ [Cloud storage](docs/cloud-storage.md) — S3 credentials and IAM, MinIO and S3-compatible stores, GCS status
- 🪣 [Amazon S3 and AWS Glue](docs/aws.md) — the extended AWS reference: the credential chain, region/endpoint resolution, worked configs, Glue registration and discovery, and AWS's Iceberg REST endpoints
- 🔷 [Azure Data Lake Storage and OneLake](docs/azure.md) — the extended Azure reference: URI shapes, the Entra ID credential chain, endpoints, worked configs, catalogs, and Azurite
- 🧪 [Set up an Azure test environment](docs/azure-test-environment.md) — a disposable subscription sandbox for exercising the Azure paths, with teardown
- 🟧 [Cloudflare R2 and R2 Data Catalog](docs/cloudflare.md) — R2 as S3-compatible storage, the R2 Data Catalog beta, and its account-scoped token
- ❄️ [Snowflake Horizon Catalog](docs/snowflake.md) — Apache Polaris under the hood, a token-exchange auth flow with no client id, and why table data can't be read yet
- 🔍 [Query a synced table](docs/query-engines.md) — DuckDB (verified), Spark, Trino, Athena, Redshift, BigQuery, Snowflake, StarRocks, Fabric
- ⚖️ [Features and limitations](docs/features-and-limitations.md) — the honest capability reference and known issues
- 🧩 [Interoperability coverage](docs/interoperability-coverage.md) — the five axes of interoperability, a verified-by matrix, and defects only a foreign reader ever caught
- 🧪 [How polytable is tested](docs/testing.md) — foreign fixtures, engine verification, and the coverage bar
- 🗺️ [Roadmap](docs/roadmap.md) — direction, positioned against the upstream release train
- 🔭 [Upstream watch](docs/upstream-watch.md) — dated knowledge base of Java XTable's plans, bugs, and roadmap

---

## 🏛️ Architecture & Specifications

For detailed architectural diagrams, domain models, and conversion invariants, refer to:
- 📖 [**Technical Specification (`SPEC.md`)**](./SPEC.md)
- 🤖 [**Agent & Contributor Guide (`AGENTS.md`)**](./AGENTS.md)
- 📋 [**REST Service OpenAPI Contract (`spec/rest-service-open-api.yaml`)**](./spec/rest-service-open-api.yaml)

---

## ⚖️ License & Disclaimer

`polytable` is licensed under the [Apache License, Version 2.0](LICENSE).

It is **not** an Apache Software Foundation project and carries no ASF endorsement. Portions derive
from [Apache XTable (incubating)](https://github.com/apache/incubator-xtable); see [`NOTICE`](NOTICE)
for the upstream attribution required by Apache-2.0 §4(d). The official Apache project is at
[xtable.apache.org](https://xtable.apache.org) — bug reports for it do not belong here.

## WebAssembly status

**The WebAssembly target is experimental. It has never been executed in a browser or under Node.js,
and it is not covered by any test.** The build is compile-checked only: `make check` runs
`GOOS=js GOARCH=wasm go vet ./cmd/polytable-wasm`, which type-checks the package but never runs it.

Two known limitations, both consequences of how the target is currently built:

- **Only local and in-memory paths can work.** `NewStorageForPath` selects the S3 backend for
  `s3://` and `s3a://` URIs, which needs AWS credentials and network access that a browser sandbox
  does not provide. Catalog synchronization (AWS Glue, Iceberg REST) is likewise unreachable.
- **The artifact is 18.4 MiB.** The AWS SDK is no longer linked: `pkg/io/s3.go` and the Glue files
  in `pkg/catalog` are built `!js`, with `js` stubs returning `ErrS3Unsupported` and
  `ErrGlueUnsupported`. That removed all 103 `aws-sdk-go-v2` and `smithy` packages and cut 7 MiB.
  The remainder is the Go runtime and the format adapters.

Treat `polytableInspect` and `polytableSync` as unvalidated. Report anything that works as much as
anything that does not.
