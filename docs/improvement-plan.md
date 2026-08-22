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

# Improvement plan

**T1–T9** were the original structural plan, written against commit `ec2fd7e`. **T10–T12** were
added from the review of commits `d34ed36..ddd157a` — T10 was a release blocker. **T13–T15** were
parity gaps against Java XTable; T14 and T15 have since landed under other numbers, and T13 stays
unscheduled by decision. **T16–T19** came out of later reviews and are all resolved. **T20–T37** come
from the 2026-08-21 upstream survey and the reviews after it. **T38–T50** turn `docs/roadmap.md`
into work, and **T51–T52** are Azure, added the same day by maintainer decision. Every task from T20 on is written to be picked up cold by an agent, with the evidence, the
scope and the acceptance criteria in the task itself.

Every file path, line number, signature and command output below was read out of the tree or run
against it, not recalled. Where a task is marked ✅ but its acceptance criteria were not met, that is
stated in the task rather than the status.

## Ground rules for every task

1. **Gate.** `make check` must pass before you commit. It runs `gofmt -l .`, `go vet ./...`,
   `GOOS=js GOARCH=wasm go vet ./cmd/xtable-wasm`, `go test -short ./...`, `golangci-lint run ./...`.
   Never substitute `go test ./...` — without `-short` it starts MinIO and Nessie containers.
2. **One commit per task.** Conventional Commits, matching existing subjects (`feat:`, `fix:`, `ci:`,
   `docs:`, `test:`). No trailers, no sign-off — the repo uses none.
3. **New `.go` files** carry the identical 16-line ASF header. Copy it verbatim from any file in
   `pkg/model/`. New `.md`/`.yml` files use the comment-syntax equivalent.
4. **Tests** go in the external `<pkg>_test` package, are table-driven, and call `t.Parallel()` in both
   parent and subtests. `testify` (`require`/`assert`) only. Do not add `t.Parallel()` to anything in
   `test/dockertest_*`.
5. **Do not edit `go.mod`'s `go` directive** (currently `go 1.26.0` — the 1.26 minor floor, forced to
   that exact spelling by `golang.org/x/sys`; do not let it drift to a later patch such as `1.26.7`).
   Note `go get -u ./...` rewrites this directive, so re-check it after any dependency sweep. If a task genuinely requires it, update the `ci.yml` matrix in the
   same commit.
6. **Never assert on `model.DiffFiles` slice ordering** — it ranges over maps and is nondeterministic.
7. After editing `.golangci.yml`, run `golangci-lint config verify`. `golangci-lint run` silently
   ignores misplaced keys; `config verify` is the only command that rejects them.
8. **✅ means every acceptance criterion in the task was checked and passed** — not "the code landed".
   If some criteria are unmet, mark the task ⚠️ and say which. This rule exists because it has been
   broken three times so far (T3, T8, T10 were each marked complete with criteria outstanding), and a
   status nobody can trust makes this document worse than no document.
9. **Run `go test -short -race ./pkg/...` for any change touching `pkg/daemon` or goroutines.**
   `make check` does not enable the race detector; a data race was shipped and later fixed in `3162cf3`.
10. **A format adapter is not done until a foreign implementation agrees with it.** Self-referential
    round-trips hide every bug that is symmetrical in this repo's reader and writer — that is how
    JSON Iceberg manifests, the Delta `omitempty` keys, and the Paimon layout mismatch all shipped
    (T28/T29/T31/T32 hold the case studies). For any change to a source: it must read a fixture a
    real engine wrote (`test/testdata/fixtures/`). For any change to a target: a real engine must
    read its output (the DuckDB suite, pyiceberg, or the T30 job). Where no real-engine oracle
    exists yet for a format, say so in the task outcome instead of letting the matrix imply one.

---

## T1 — Extract a format registry ✅ COMPLETED

**Why first.** Format construction is duplicated across **six** switch statements. Two of them
(`bindings/c`, `cmd/xtable-wasm`) were already found out of sync — they returned
`unsupported format: PAIMON` for months. Adding targets (T4) before this multiplies the problem.

### The six call sites

| File | Line | Kind |
|---|---|---|
| `pkg/conversion/controller.go` | 133 | `createSource` |
| `pkg/conversion/controller.go` | 156 | `createTarget` |
| `cmd/xtable/main.go` | 194 | CLI `inspect` |
| `pkg/daemon/server.go` | 247 | REST `/inspect` |
| `bindings/c/xtable.go` | 126 | `xtable_inspect_json` |
| `cmd/xtable-wasm/main.go` | 65 | `xtableInspect` |

`pkg/catalog/glue.go:182` also switches on `model.TableFormat` but maps formats to Glue table
properties — **not** construction. Leave it alone.

### Steps

1. Create `pkg/formats/registry.go` (package `formats`) exposing:

   ```go
   func NewSource(format model.TableFormat, storage io.Storage, basePath string) (spi.ConversionSource, error)
   func NewTarget(format model.TableFormat, storage io.Storage, basePath, tableName string) (spi.ConversionTarget, error)
   func SupportedSources() []model.TableFormat
   func SupportedTargets() []model.TableFormat
   ```

   Existing constructors this wraps — all verified:
   - `delta.NewSource(storage, basePath)`, `iceberg.NewSource(...)`, `hudi.NewSource(...)`,
     `parquet.NewSource(...)`, `paimon.NewSource(...)` — all `(io.Storage, string)`.
   - `delta.NewTarget(storage)`, `iceberg.NewTarget(storage)`, `hudi.NewTarget(storage)` — all
     `(io.Storage)` only; the target is configured later via `Init`.

   `NewTarget` must reproduce `createTarget`'s current behavior: build
   `&model.Table{Name: tableName, TableFormat: format, BasePath: basePath}`, call `target.Init(ctx, …)`,
   and return the error if `Init` fails. **Take a `ctx context.Context` parameter** rather than calling
   `context.Background()` as `controller.go:158/165/172` currently does.

   Error strings must stay compatible: `unsupported format: %s` for sources,
   `unsupported target table format: %s` for targets. Tests and the C/WASM JSON payloads surface them.

2. **Watch for an import cycle.** `pkg/formats/<fmt>` packages import `pkg/io`, `pkg/model`, `pkg/spi`.
   A new `pkg/formats` package importing its own subpackages is fine — Go has no parent/child cycle —
   but confirm no format package imports `pkg/formats`. If one ever does, move the registry to
   `pkg/formats/registry/`.

3. Replace all six switches with registry calls. In `controller.go`, `createSource`/`createTarget`
   become thin wrappers or disappear entirely — check their callers before deleting.

4. `bindings/c/xtable.go` and `cmd/xtable-wasm/main.go` **must** call the registry. If they keep their
   own switches, this task has failed its purpose.

### Acceptance

- `grep -rn "case model.TableFormatDelta:" --include='*.go' .` returns exactly **two** hits:
  `pkg/formats/registry.go` and `pkg/catalog/glue.go:182`.
- New `pkg/formats/registry_test.go` (package `formats_test`), table-driven, asserting every format in
  `SupportedSources()` constructs, and that an unknown format returns an error matching
  `unsupported format:`.
- `make check` passes, including the js/wasm vet stage.

### Commit

`refactor: replace six format switches with a single registry`

Body should say the two entrypoints that were out of sync and that the registry is what prevents recurrence.

---

## T2 — Wire catalog sync into the conversion path ⚠️ LANDED BUT INCORRECT — see T16

**Current state:** `pkg/catalog` is dead code outside its own tests. `DatasetConfig`
(`pkg/conversion/config.go`) has no catalog field and `Controller` never builds a `SyncClient`, so Glue
and Iceberg REST are unreachable from the CLI, the config file, and the REST service. This is why
`pkg/catalog` sits at 26.5% coverage.

### Steps

1. Add to `DatasetConfig`, following the existing json+yaml dual-tag style:

   ```go
   // Catalogs lists external catalogs to register the synced table in. Optional.
   Catalogs []*catalog.Config `json:"catalogs,omitempty" yaml:"catalogs,omitempty"`
   ```

   `catalog.Config` already exists with `Type`, `CatalogID`, `DatabaseName`, `URI`, `Properties`.

   **Check for an import cycle first**: `pkg/catalog` imports `pkg/model`. If it ever imports
   `pkg/conversion`, invert the dependency instead of adding this field.

2. Add a constructor dispatch in `pkg/catalog` (there is currently none):

   ```go
   func NewSyncClient(ctx context.Context, cfg *Config) (SyncClient, error)
   ```

   Wrapping the real constructors — verified names, note they are not uniform:
   - `NewGlueCatalogSyncClient(ctx, cfg)` → `CatalogTypeGlue`
   - `NewIcebergRESTCatalogClient(cfg)` → `CatalogTypeIcebergREST` (**no ctx**)
   - `CatalogTypeHMS` → return an explicit "not implemented" error until T5 decides its fate.

3. In `Controller.Sync` (`pkg/conversion/controller.go:48`), after each target sync succeeds, call
   `CreateOrUpdateTable(ctx, table, snapshot)` on every configured client. Thread the caller's `ctx`.

4. **Failure policy — decide and document in the code:** a catalog registration failure must not silently
   discard a successful metadata sync. Record it on the per-format `spi.SyncResult` rather than aborting
   the whole run, and say so in a comment.

5. Close every client. `SyncClient.Close()` releases network connections; use `defer` per client.

6. Surface it: the CLI (`cmd/xtable/main.go`) and REST (`pkg/daemon/server.go`) read `DatasetConfig`
   already, so YAML/JSON config works for free — but verify the daemon's config decode path
   (`cmd/xtable-service/main.go:87`, which picks JSON for `.json` and YAML for everything else).

### Acceptance

- A YAML config with a `catalogs:` block round-trips through `conversion.LoadConfig` and reaches a
  `SyncClient`.
- Unit test with a fake `SyncClient` asserting `CreateOrUpdateTable` is called once per target per
  catalog, and that a returned error lands on `SyncResult` without failing the sync.
- `test/dockertest_iceberg_rest_test.go` extended to drive the catalog **through** `Controller.Sync`
  rather than constructing the client directly.
- `make check` passes.

### Commit

`feat: wire catalog sync into the conversion controller`

---

## T3 — Make storage options reachable from configuration ⚠️ PARTIAL — see T12

Landed for the CLI (`cmd/xtable/main.go:123`) and the daemon config path
(`pkg/daemon/daemon.go:90`), via `StorageConfig` + `ToS3OptionFuncs()` and the new
`io.NewStorageForPathWithOptions`. Credentials correctly stayed out of the struct.

**Two acceptance criteria were not met** — tracked as T12:
1. The REST API still cannot set storage options. `Server.getStorage` calls `io.NewStorageForPath`
   with no options and `ConvertTableRequest` has no storage field.
2. `test/dockertest_minio_matrix_test.go:119` still builds storage with
   `io.NewS3StorageWithClient(s3Client)` directly. The plan named reworking this as the acceptance
   test precisely because bypassing config in the test is how the original gap survived. The new
   config path currently has **zero** integration coverage.

**Current state:** `pkg/io/storage.go:85` — `NewStorageForPath` calls `NewS3Storage(ctx)` with no
options. `NewS3Storage` accepts `optFns ...func(*S3Options)` and `S3Options` carries
`Region`, `Endpoint`, `UsePathStyle`, `CustomHTTPClient`. So MinIO and any custom endpoint work only
from Go code — not the CLI, config file, or REST service. `test/dockertest_minio_matrix_test.go`
constructs storage directly, which is why this gap never surfaced in tests.

### Steps

1. Add a serializable storage block to `DatasetConfig`:

   ```go
   // Storage carries optional object-store overrides (custom endpoint, path-style addressing).
   Storage *StorageConfig `json:"storage,omitempty" yaml:"storage,omitempty"`
   ```

   Define `StorageConfig` in `pkg/conversion` with `Region`, `Endpoint`, `UsePathStyle` — **do not**
   expose `CustomHTTPClient`; it is not serializable and must stay Go-only.

2. Add an options-aware entrypoint in `pkg/io` rather than changing the existing signature:

   ```go
   func NewStorageForPathWithOptions(ctx context.Context, path string, optFns ...func(*S3Options)) (Storage, error)
   ```

   Keep `NewStorageForPath` as a wrapper that passes no options, so existing callers are untouched.

3. Have the controller/CLI/daemon build the option funcs from `StorageConfig`.

4. Credentials stay with the AWS default chain (`awsconfig.LoadDefaultConfig`). **Do not** add
   access-key or secret fields to the config struct — that would put secrets in a YAML file.

### Acceptance

- A config with `storage.endpoint` pointed at MinIO drives an end-to-end sync through the public API.
- `test/dockertest_minio_matrix_test.go` reworked to go through config rather than constructing
  `S3Storage` directly — this is the regression test for the gap.
- `make check` passes.

### Commit

`feat: expose S3 endpoint and path-style options through dataset config`

---

## T4 — Parquet and Paimon targets ✅ COMPLETED

**Depends on T1.** With the registry in place, each new target is registered once.

Note the framing correction: these are missing because `pkg/formats/parquet/target.go` and
`pkg/formats/paimon/target.go` were **never written** — not because a switch was missed. Both packages
currently ship a `Source` only.

### Steps (per format)

1. Create `pkg/formats/<fmt>/target.go` implementing all six `spi.ConversionTarget` methods
   (`pkg/spi/target.go:27-45`): `Format`, `Init`, `GetTableMetadata`, `CommitSnapshot`, `CommitChanges`,
   `Close`.
2. Add the compile-time assertion — every one of the eight existing adapters has one:
   `var _ spi.ConversionTarget = (*Target)(nil)`.
3. Constructor must match the established shape: `func NewTarget(storage io.Storage) *Target`.
   Configuration arrives via `Init`, not the constructor.
4. `GetTableMetadata` **must** return the embedded `TableSyncMetadata` — this is invariant INV-4 and
   what makes incremental sync safe. Keys live in `pkg/model/metadata.go:36-40`
   (`xtable_last_instant_synced`, `xtable_source_format`).
5. All file access goes through the injected `io.Storage`. Never use `os` directly, or the format breaks
   on S3 and in WASM.
6. Register in `pkg/formats/registry.go` — the only wiring step needed after T1.
7. Consult `SPEC.md` §6 for the on-disk layout before writing either. It is the authoritative reference
   for what each format's metadata files must contain.

**Sequence Parquet before Paimon.** Parquet's target is a directory of data files plus Hive-style
partition layout — much smaller than Paimon's snapshot/manifest tree, and it validates the registry
extension path first.

### Acceptance

- Round-trip test per format: sync a source table to the new target, read it back with that format's
  `Source`, assert schema and file list survive.
- `SupportedTargets()` includes the new format; C and WASM entrypoints accept it with no edits — that is
  the proof T1 worked.
- `make check` passes.

### Commits

`feat: add Parquet conversion target`, then `feat: add Paimon conversion target`. Keep them separate.

---

## T5 — Resolve Hive Metastore: implement or withdraw ✅ COMPLETED (option B)

**Executed.** Capability claims withdrawn from `SPEC.md:30`, `SPEC.md:239`, `AGENTS.md:28`,
`AGENTS.md:71` and `CLAUDE.md:13`. `CLAUDE.md:32` deliberately keeps HMS — it is the forward-looking
"Parity order" list, not a statement of current capability.

In code, `pkg/catalog/catalog.go` gained `ErrCatalogNotImplemented` and
`CatalogType.Implemented()`, covered by `TestCatalogTypeImplemented`. T2 must route
`CatalogTypeHMS` to that error rather than skipping it silently. Full scope for a real
implementation is now T13.

`CatalogTypeHMS` is declared at `pkg/catalog/catalog.go:32` with **no implementation**, while
`AGENTS.md:28`, `SPEC.md:30` and `SPEC.md:239` all advertise Hive Metastore support. The README does
**not** claim it (its only Hive mention is "Hive Style" partitioning for Parquet — unrelated).

This is a judgment call, not a mechanical fix. **Ask the maintainer before executing.** Options:

- **(A) Implement** an HMS client. Large: HMS speaks Thrift, which means a new dependency and a
  container for integration testing. Weigh against the "no JVM/Hadoop dependencies" invariant — a Go
  Thrift client is acceptable under it, a Hadoop config shim is not.
- **(B) Withdraw the claim.** Keep the constant (it is part of the Java-parity surface), have
  `NewSyncClient` return a clear "not implemented" error, and correct `AGENTS.md:28`, `SPEC.md:30`
  and `SPEC.md:239` to list only Glue and Iceberg REST as implemented.

### Library evaluation (verified 2026-08-12) — take (B)

A proposal to implement via `github.com/akolb1/gometastore/hmsclient` was checked against primary
sources. Three of its load-bearing claims do not hold:

| Claim | Verified |
|---|---|
| gometastore is "production-ready, mature" | GitHub API `pushed_at` = **2022-12-18**. Zero commits in 3y8m, 14 stars. pkg.go.dev flags the latest pseudo-version "not in the latest version of its module". |
| gohive's MIT license "needs ASF legal review" | **False.** MIT/X11 is [Category A](https://www.apache.org/legal/resolved.html), same tier as Apache-2.0. No review needed. |
| Effort "~1–2 days" | Java's `xtable-hive-metastore` has `HMSCatalogSyncClient`, `HMSCatalogConversionSource`, `HMSSchemaExtractor`, `HMSCatalogPartitionSyncOperations` **and three per-format table builders** (Delta/Hudi/Iceberg), plus `HMSCatalogConfig` (`maxPartitionsPerRequest=1000`, `schemaLengthThreshold=4000`, partition-extractor class). |

Two further constraints nobody raised: `hmsclient` returns Thrift-generated `*hive_metastore.Table`,
so Apache Thrift's Go runtime enters `go.mod` (currently 12 direct deps, no Thrift); and Java sets
table ownership via Hadoop `UserGroupInformation` (`HMSCatalogTableBuilderFactory.java:67`), which Go
cannot replicate — ownership semantics would silently diverge.

**Decision: (B).** Withdrawing takes about an hour and unblocks T2 now. Since the licensing argument
that favoured `gometastore` is void and that library is abandoned, any future implementation should
start from `beltran/gohive` (MIT/Category A, pushed 2026-05-30, 258 stars) — not gometastore.

Do **not** ship a partial HMS that only creates tables. It would read as supported while missing
partition sync and the per-format builders, which is worse than the current honest gap. Track the full
scope as its own issue (see T13).

### Commit

`docs: scope catalog support to Glue and Iceberg REST`

---

## T6 — Python packaging ✅ COMPLETED

`bindings/python/pyproject.toml` is bare setuptools with no build hook, so nothing compiles or vendors
`libxtable` into a wheel. `pyxtable/__init__.py:_find_library()` searches the package dir, then
`../../lib/`, `../lib/`, `./lib/`, then the bare name — so it only works when run from the repo root
after `make bindings-c`. Import failure is swallowed (`except Exception: _lib = None`) and surfaces
later as `RuntimeError: libxtable shared library is not loaded`.

### Steps

1. Add `[tool.setuptools.package-data]` so a built shared library inside `pyxtable/` is included.
2. Add a build step (a `Makefile` target is enough; a full PEP 517 in-tree backend is optional) that runs
   `make bindings-c` and copies the artifact into `bindings/python/pyxtable/` before `python -m build`.
3. Wheels are platform-specific — `libxtable.so` and `libxtable.dylib` cannot ship in one `py3-none-any`
   wheel. Either tag wheels per platform or document source-install only.
4. Fix the swallowed error: keep import working, but retain the underlying exception and include it in
   the `RuntimeError` message so failures are diagnosable.
5. Update `bindings/python/README.md` — it currently documents the limitation this task removes.

### Acceptance

- `python -m build` produces a wheel; `pip install` into a clean venv, then `python -c "import pyxtable; pyxtable.version()"` succeeds outside the repo root.
- `make check` unaffected (no Go changes).

### Commit

`build: package libxtable into the pyxtable wheel`

---

## T7 — Release process ✅ COMPLETED (proven via T17)

`git tag` returns zero tags; there are no releases.

### Completed

The release workflow fixes were completed in T10:

1. **Agree the scheme** — `v0.x.y` until ASF donation settles naming ✅
2. **Add `.github/workflows/release.yml`** — workflow exists and triggers on `v*` tags ✅
3. **Fix workflow** — T10 resolved the C-shared library build issue with OS-specific matrix.os guards ✅
4. **ASF requirements** — workflow documented as pre-donation convenience only ✅

The release workflow now correctly builds artifacts on `ubuntu-latest` and `macos-latest` with proper cross-compilation guards.

### Steps

1. Agree the scheme. The module is pre-1.0 and headed for ASF donation — `v0.x.y` until the donation
   settles the naming. Note the module path is already `github.com/slachiewicz/xtable-go` while the
   remote is a personal fork; **resolve that before tagging**, since a Go module tag is immutable and
   the path is baked into consumers.
2. Add `.github/workflows/release.yml` triggered on `v*` tags, building the matrix already proven in
   `build-artifacts.yml` (linux/darwin CLI + service, `xtable.wasm`, `libxtable.{so,dylib}`).
3. Attach artifacts to the GitHub release. Consider GoReleaser, but a plain workflow reusing
   `build-artifacts.yml`'s steps is less new machinery.
4. ASF releases have their own requirements (source tarball, signatures, `DISCLAIMER-WIP`). Treat
   GitHub releases as pre-donation convenience only, and say so in the workflow comments.

### Commit

`ci: add tag-triggered release workflow`

---

## T8 — Targeted test coverage ⚠️ PARTIAL (daemon only) — see T18

Measured (`go test -short -cover ./pkg/...`):

```
model 68.4   hudi 65.6   parquet 64.1   paimon 58.9   delta 56.2
iceberg 55.4  conversion 53.9   io 45.2
daemon 30.9  catalog 26.5  spi 0.0        ← the actual gaps
```

Do **not** apply a blanket percentage target; seven of eleven packages already clear 40%.

- **`pkg/spi` (0%)** — interface-only package, so coverage is partly meaningless. Add contract tests:
  a fake `ConversionSource`/`ConversionTarget` plus assertions that the compile-time
  `var _ spi.X = (*Y)(nil)` assertions exist for every adapter. Low value if it is only interfaces —
  check what statements exist before investing.
- **`pkg/catalog` (26.5%)** — will rise naturally from T2. Re-measure after T2 and only then add tests.
- **`pkg/daemon` (30.9%)** — REST handler tests via `httptest`. Cover `/v1/health`,
  `/v1/conversion/inspect`, and both sync and `Prefer: respond-async` paths on
  `POST /v1/conversion/table`, including the async polling state machine.

### Completed

Focused on `pkg/daemon` REST surface coverage, improving from 30.9% to 53.8%:

- Added `TestServer_AsyncConversionAndStatusPolling` - tests async conversion submission and polling state machine
- Added `TestServer_StatusNotFound` - tests conversion job not found error case
- Added `TestServer_StatusMissingConversionID` - tests missing conversion ID error case
- Added `TestServer_ConvertInvalidJSON` - tests invalid JSON payload handling
- Added `TestServer_ConvertInvalidMethod` - tests method not allowed error for conversion endpoint
- Added `TestServer_InspectInvalidMethod` - tests method not allowed error for inspect endpoint
- Added `eventually` utility for async polling with timeout

Coverage impact: daemon 30.9% → 53.8% (**+22.9 percentage points**)

**`pkg/spi` and `pkg/catalog`**: Deferred. SPI has minimal concrete code (helper functions only), catalog coverage will increase naturally with integration usage when catalog sync is exercised.

### Commit

`test: cover the daemon REST surface and spi contracts` (partial)

---

## T9 — Correct the Roaring Bitmap claim in the README ✅ COMPLETED

`model.DeletionVector` (`pkg/model/datafile.go:21`) is a **descriptor**: `StoragePath`, `Offset`,
`SizeInBytes`, `Cardinality`, `InlineBytes`. No roaring bitmap is decoded or encoded anywhere —
`grep -ri roaring --include='*.go' .` returns nothing.

That is **correct by design**: decoding the bitmap would mean reading data files, violating INV-1
(zero data rewrites). Do not "implement roaring bitmaps."

Scope is narrow — two lines, README only:

- `README.md:37` — "**Roaring Bitmap Deletion Vectors**: Full translation…" → say the descriptor is
  translated and the bitmap payload is passed through untouched.
- `README.md:52` — the Delta row's "✅ (Roaring Bitmap)" cell → same correction.

**Leave `SPEC.md` alone.** `SPEC.md:174` already says "Roaring Bitmap **descriptors**" and `:125` says
"Path to external Roaring Bitmap" — both accurate.

### Commit

`docs: describe deletion vectors as descriptor pass-through`

---

# Review findings (2026-08-12, commits `d34ed36..ddd157a`)

Raised by reviewing the 15 unpushed commits. T10 is a release blocker.

## T10 — Fix the release workflow ✅ COMPLETED (finished and proven in T17)

`.github/workflows/release.yml`, step **"Build C-Shared Dynamic Libraries"**, has **no `if:` guard**.
It runs on both `ubuntu-latest` and `macos-latest` and attempts all four platform pairs on each:

```sh
GOOS=linux  GOARCH=amd64 CGO_ENABLED=1 go build -buildmode=c-shared ...
GOOS=linux  GOARCH=arm64 ...
GOOS=darwin GOARCH=amd64 ...
GOOS=darwin GOARCH=arm64 ...
```

Cross-compiling cgo needs a matching `CC` cross-toolchain, which GitHub runners do not provide.
Verified locally on darwin:

```
$ GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -buildmode=c-shared ./bindings/c
exit 1
# runtime/cgo
gcc_amd64.S:27:8: error: unknown token in expression
```

It is one `run:` block with no `|| true`, so the step fails, the job fails, and **no release is ever
produced on the first `v*` tag**. The WASM and `sha256sum` steps in the same file *are* guarded with
`if: matrix.os == 'ubuntu-latest'` — only this step was missed.

### Steps

1. Split the step in two, each guarded by `matrix.os`, building only the runner's native `GOOS`
   (ubuntu → the two `.so`; macos → the two `.dylib`). Note `GOARCH` cross-compilation *within* one
   OS also needs a cross-linker — if the arm64 leg fails on an amd64 runner, either add
   `macos-14`/`ubuntu-24.04-arm` runners or drop the non-native arch.
2. `SHA256SUMS.txt` is generated only on ubuntu, so macOS artifacts currently ship unchecksummed.
   Either checksum per-runner or generate it after artifacts are merged in the release job.
3. Prove it before tagging: push a throwaway `v0.0.0-test` tag to a fork, confirm green, delete it.

**Commit:** `ci: build c-shared libraries only on their native runner`

## T11 — Restore CI coverage and re-sync docs ✅ COMPLETED

Three regressions from the same batch:

1. **The Go matrix lost `stable`.** `ci.yml` went `["1.22","1.23","stable"]` → `["1.25.5"]`. Removing
   the 1.22/1.23 legs was right (dead against the `go.mod` floor), but nothing now tests against
   current Go, so a Go 1.26 incompatibility ships silently. Restore `["1.25", "stable"]`.
2. **`go.mod` moved to a patch-level directive** (`go 1.25.5`). This is the pattern `CLAUDE.md` warns
   about — it was deliberately lowered from `1.26.5` to `1.25.0` for exactly this reason. A patch
   floor forces every downstream consumer onto ≥1.25.5 for no stated benefit; `go 1.25` is the
   conventional library floor. Reverting is preferred but is a maintainer call.
3. **`CLAUDE.md:80` is stale again** — still says `go 1.25.0`. Re-sync whichever value wins.

Also minor: the registry returns `unsupported source table format: %s`, so the C and WASM APIs now
emit `failed to create format source: unsupported source table format: PAIMON` where they previously
emitted `unsupported format: PAIMON`. T1 asked for string compatibility. Nothing is released, so the
cost is zero today — but decide deliberately, because FFI consumers parse that text.

**Commit:** `ci: test against current Go alongside the declared minimum`

## T12 — Finish T3: REST storage options and the MinIO regression test ✅ COMPLETED

Carries the two unmet acceptance criteria from T3 (see above).

1. Add a storage block to `ConvertTableRequest`/`InspectTableRequest` (`pkg/daemon/types.go`) and
   thread it through `Server.getStorage`, mirroring `ToS3OptionFuncs()`. Keep credentials out.
2. Rework `test/dockertest_minio_matrix_test.go` to reach MinIO through `DatasetConfig.Storage`
   instead of `io.NewS3StorageWithClient`. **This is the point of the task** — without it the config
   path is untested and will rot exactly as the original gap did.

**Commit:** `feat: accept storage options over the REST API`

---

# Round-2 review findings (commits `d34ed36..3162cf3`)

From reviewing all 28 unpushed commits. **Do T16 and T17 before pushing further work.** Neither blocks
`git push`; T17 blocks tagging.

Health at time of review: `make check` green, `go test -short -race ./pkg/...` clean across all 11
packages, `pkg/formats` at 95.2% coverage. The problems below are design and status, not stability.

## T16 — Rewrite catalog sync as per-target ✅ COMPLETED

`Controller.syncToCatalogs` (`pkg/conversion/controller.go:102`) calls `source.GetCurrentSnapshot(ctx)`
**once** and passes `snapshot.Table` to every catalog client. That is the **source** table. Converting
Delta→Iceberg with a Glue catalog therefore registers the *Delta* table in Glue — not the Iceberg
output the catalog exists to advertise.

Java, `../incubator-xtable/xtable-core/src/main/java/org/apache/xtable/conversion/ConversionController.java:140-157`:

```java
for (TargetTable targetTable : config.getTargetTables()) {
    Map<CatalogTableIdentifier, CatalogSyncClient> catalogSyncClients =
        config.getTargetCatalogs().get(targetTable).stream()...
            catalogConversionFactory.createCatalogSyncClient(
                targetCatalog.getCatalogConfig(), targetTable.getFormatName(), conf);
    catalogSyncResults.put(
        targetTable.getFormatName(),
        syncCatalogsForTargetTable(targetTable, catalogSyncClients,
            conversionSourceProvider.get(targetTable.getFormatName())));   // reads back the TARGET
}
mergeSyncResults(tableFormatSyncResults, catalogSyncResults);
```

Three defects, all one divergence — fix as a single restructure, not three patches:

1. **Wrong table registered.** Loop per target format; read the produced table back with
   `formats.NewSource(targetFormat, storage, basePath)` and register *that* snapshot.
2. **First-error abort.** `syncToCatalogs` returns on the first catalog failure, so a second configured
   catalog is never attempted. Attempt every catalog; collect errors with `errors.Join`.
3. **Success is destroyed.** On catalog failure the controller rewrites every successful format's
   `SyncResult` from `SyncStatusSuccess` to `SyncStatusError`. The metadata *was* written to storage —
   reporting ERROR invites a caller to retry believing nothing landed. Record the catalog outcome
   without overwriting the conversion status; Java merges rather than replaces.

**Open design question for the maintainer:** Java attaches catalogs **per target**
(`config.getTargetCatalogs().get(targetTable)`), whereas Go's `DatasetConfig.Catalogs` is a flat list
applied to all targets. Decide whether to adopt Java's per-target mapping — it is a config schema
change — or keep the flat list and register every target into every catalog. The flat list is simpler
and probably right for now; just make the choice deliberately and write it down here.

### Acceptance

- Test: Delta source → Iceberg target with a fake `SyncClient`; assert the registered
  `model.Table.TableFormat` is `ICEBERG`, not `DELTA`. **This is the regression test for the defect.**
- Test: two catalogs where the first fails; assert the second is still attempted and both errors surface.
- Test: catalog failure leaves `SyncResult.StatusCode == SyncStatusSuccess` for formats whose metadata
  conversion succeeded, with the catalog error reported separately.
- `make check` and `go test -short -race ./pkg/...` both pass.

**Commit:** `fix: register the target table in catalogs, not the source`

### Outcome (verified)

Landed in `6f52b08`, corrected by `4fd7e19`. All three acceptance criteria pass:
`TestController_CatalogSyncRegistersTargetTable` asserts the registered format is `ICEBERG` not
`DELTA`; `...MultipleCatalogsPartialFailure` proves the second catalog is still attempted after the
first fails; `...CatalogFailureDoesNotOverwriteSuccessStatus` proves `SyncStatusSuccess` survives a
catalog error.

**Design decision recorded:** the flat `DatasetConfig.Catalogs` list was kept rather than adopting
Java's per-target catalog mapping. Every target is registered into every configured catalog. This
avoids a config schema change and is sufficient while catalogs are homogeneous; revisit only if
per-target routing is actually needed.

`6f52b08` originally exposed the test seam as four exported setters plus two mutable package globals,
putting a `GetFakeClient` accessor into the public API and blocking `t.Parallel()`. `4fd7e19` replaced
it with `conversion.WithCatalogClientFactory`, a functional option matching the `optFns` pattern in
`pkg/io`. Coverage moved `pkg/catalog` 25.9% -> **37.4%** and `pkg/conversion` 54.4% -> **71.0%**.

## T17 — Finish the release workflow: the arm64 leg ✅ COMPLETED (proven by a tag run)

T10 added the `matrix.os` guards, but the ubuntu leg still runs:

```sh
GOOS=linux GOARCH=arm64 CGO_ENABLED=1 go build -buildmode=c-shared -o dist/libxtable-linux-arm64.so ./bindings/c
```

There is **no `apt-get`, no `CC=`, no cross-toolchain setup anywhere in `release.yml`**, and
`ubuntu-latest` ships x86_64 gcc only. cgo cannot cross-compile architectures without a matching C
compiler. T10's own step 1 predicted this leg and the fix skipped it. The macOS legs are fine — Apple
clang cross-builds `x86_64`/`arm64` natively.

Not reproduced on a Linux runner (none available locally); the mechanism is the same one verified
earlier as exit 1, and nothing in the workflow supplies the missing compiler.

Pick one:
- **(a)** Add a native arm64 runner leg (`ubuntu-24.04-arm`) — cleanest, no cross-compilation at all.
- **(b)** `apt-get install -y gcc-aarch64-linux-gnu` and `CC=aarch64-linux-gnu-gcc` for that one build.
- **(c)** Drop linux/arm64 and document the omission in the release notes.

### Acceptance

- Push a throwaway `v0.0.0-test` tag **to a fork**, confirm the workflow goes green end to end, delete
  the tag. Do not tag the real repository until this passes — a Go module tag is immutable.
- Also confirm `SHA256SUMS.txt` covers the macOS artifacts; it is currently generated only on ubuntu.

**Commit:** `ci: build linux/arm64 on a native runner`

### Outcome (code fixed, NOT verified by a tag run)

`076edd9` took option (a): `ubuntu-24.04-arm` joined the matrix, so linux/arm64 c-shared builds
natively and no cgo cross-compilation remains. Checksums moved to the merge job, covering macOS too.

`083da1b` fixed two further defects found while checking that work, both of which would have let the
job go green while shipping the wrong thing:

- `files: artifacts/*/*.*` requires a dot in the filename. The `.so`, `.dylib` and `.wasm` artifacts
  matched, but the CLI binaries (`xtable-linux-amd64`, `xtable-service-darwin-arm64`, ...) have no
  extension and were never attached - while `SHA256SUMS.txt` still listed them. The glob is now `*`.
- The CLI build step had no `matrix.os` guard, so all three runners produced the same eight
  cross-compiled binaries. They are pure Go (both build under `CGO_ENABLED=0`), so it now runs on
  `ubuntu-latest` only.

**Proven.** `v0.0.0-test` was pushed against `0c3333f`; run 31642578814 succeeded on all four jobs,
including the `ubuntu-24.04-arm` leg that motivated the task. The release carried **18 assets**: four
`xtable-*` CLI binaries, four `xtable-service-*`, four `libxtable-*` shared libraries with their four
`.h` headers, `xtable.wasm`, and `SHA256SUMS.txt` covering all 13 binaries. The four CLI binaries are
the check that mattered — the previous `artifacts/*/*.*` glob would have dropped them while the job
still went green. Sizes confirmed stripping (13–14 MiB rather than 19–20). Release and tag were then
deleted; the repository is back to zero tags.

## T18 — Finish T8: the two coverage targets that did not move ✅ COMPLETED

T8 named three packages. Only one moved:

| Package | T8 target | Before | Now |
|---|---|---|---|
| `daemon` | REST handler tests | 30.9% | **54.3%** ✅ |
| `catalog` | rise via T2 | 26.5% | **25.9%** ↓ |
| `spi` | contract tests | 0% | **0%** untouched |

`catalog` fell because T2 added paths without tests. T16's tests will cover some of it; measure again
after T16 and only then decide whether more is needed — do not chase a number.

For `spi`: it is interface-only, so statement coverage is close to meaningless. Check what executable
statements exist before investing. If there are none, say so here and close the item rather than
writing tests that assert nothing.

**Commit:** `test: cover catalog sync client selection`

### Outcome (verified)

Measured after T16, as instructed. `pkg/catalog` reached **37.4%** from T16's tests alone, so no extra
catalog tests were written - the number moved for the right reason rather than by chasing it.

`pkg/spi` was **not** interface-only, contrary to T8's assumption: `NewSuccessSyncResult` and
`NewErrorSyncResult` contain real statements, including the nil-error branch every controller failure
path relies on. `a86e258` covers both; `pkg/spi` now reports **100.0% of statements**.

---

## T19 — Per-file licence header ✅ DECIDED: keep the ASF header

All 80 `.go` files keep the ASF grant header. **Decided by the maintainer, who is an ASF member and
intends to donate this code.** The header is forward-looking rather than inaccurate: at donation the
software grant makes it true of the whole tree, and rewriting 80 files now only to rewrite them back
would be churn.

Do not "fix" these headers. New `.go` files take the same 16-line header, copied verbatim from
`pkg/model/`.

### What reverses at donation time

The de-affiliation work in `ad19dee` is correct only while the code sits outside the ASF. Once the
podling is accepted, reverse it:

| Item | Now | At donation |
|---|---|---|
| `DISCLAIMER-WIP` | removed — the repo is not incubating | restore the standard incubator disclaimer |
| `NOTICE` | names the xtable-go authors, disclaims affiliation, reproduces the upstream notice | revert to the ASF form: `Apache XTable (incubating)`, copyright The Apache Software Foundation |
| README / SPEC / AGENTS banners | "Not an Apache Software Foundation project" | remove |
| Project name | `xtable-go` | whatever the podling is named; ASF branding becomes correct |
| Python package | author "the xtable-go authors", homepage the GitHub repo | ASF contact and homepage become correct again |
| Module path | `github.com/slachiewicz/xtable-go` | the donated path — settle **before** the first tag, since Go module tags are immutable |

The last row is the one with a deadline attached: tagging under the personal path publishes an
immutable version that consumers can pin, so either tag after the path is final or accept that early
tags are throwaway.

---

# Parity gaps against Java XTable

Surveyed from `../incubator-xtable` on 2026-08-12. These are **missing features**, not defects — none
is scheduled, and each needs a maintainer decision before it becomes work.

Size context: the whole of Go's `pkg/catalog` is **652 lines**. Java's Glue support alone is
`GlueCatalogSyncClient` (10.2K) + `GlueSchemaExtractor` (10.5K) +
`GlueCatalogPartitionSyncOperations` (13.3K) + `GlueCatalogConversionSource` (3.7K) plus table
builders, config and a client factory. The Go catalog layer is a thin slice of the Java one.

## T13 — Hive Metastore catalog (deferred from T5)

Full scope, for whoever picks it up: sync client, schema extractor, partition sync operations, three
per-format table builders, a Thrift dependency, and an HMS container for integration tests. Start
from `beltran/gohive`. See the T5 evaluation for why not gometastore.

## T14 — Catalog conversion sources (read side) ✅ LANDED UNDER T23

Go's `catalog.SyncClient` was **write-only**: `CatalogType`, `CreateOrUpdateTable`, `DropTable`,
`Close`. Java additionally has `GlueCatalogConversionSource` and `HMSCatalogConversionSource` behind
`CatalogConversionFactory`, which resolve a table **from a catalog identifier** rather than a base
path — i.e. `--catalog glue --table db.customers` instead of `--basePath s3://…`.

**Delivered by T23**, which was scoped as its extension and overtook it: `catalog.ConversionSource`
with `GetSourceTable` and `ListTables` (`pkg/catalog/conversion.go:115`, `:120`),
`GlueConversionSource` (`pkg/catalog/glue_conversion.go`), `IcebergRESTConversionSource`
(`pkg/catalog/rest_conversion.go`), `conversion.DiscoverDatasets` (`pkg/conversion/discovery.go`),
and `polytable sync --catalog glue --database <db>`. Read T23's outcome for what was checked. The
identifier form this task asked for — resolving one named table rather than scanning a database —
is `GetSourceTable`; the CLI exposes the scan, not the single-table lookup, which is the one piece
of the original scope still open and is not worth its own task until someone asks for it.

Two limits of the delivered read side are recorded as later tasks rather than here: the Iceberg
REST source discards the catalog's `metadata-location` pointer (T39), and `ListTables` on the
Iceberg REST catalog yields `ErrCatalogNotImplemented` by design.

## T15 — Catalog partition synchronization ⚠️ LANDED, NOT VERIFIED AGAINST A REAL GLUE CATALOG

Java has `CatalogPartitionSyncTool`, `CatalogPartitionSyncOperations`, `CatalogPartition`,
`CatalogPartitionEvent` and a 13.3K `GlueCatalogPartitionSyncOperations`. Go had **none of it** — the
Go `SyncClient` registered a table but never its partitions.

Consequence, and the reason it was worth doing: for Hive-style partitioned tables registered in
Glue, engines that resolve partitions through the catalog do not see them.

**The code landed** without this task being updated. `pkg/catalog/partition.go` holds
`PartitionSyncOperations` (`:68`) and `SyncPartitions` (`:167`), which diffs desired against
existing and batches every action — Java's `CatalogPartitionEvent` shape;
`pkg/catalog/glue_partition.go` implements it over Glue and is embedded into
`GlueCatalogSyncClient` (`pkg/catalog/glue.go:43`) so the four methods promote onto it;
`Config.MaxPartitionsPerRequest` (`pkg/catalog/catalog.go:60`) is the batch cap, defaulting to
`DefaultMaxPartitionsPerRequest`. It is wired into the sync path at
`pkg/conversion/controller.go:158`, whose `syncPartitions` (`:173`) type-asserts the client and
no-ops for a catalog that tracks partitions itself.

**What is not done is the verification this task asked for**, which is why the status is ⚠️ and not
✅: "verify the actual impact against a real Glue table". Every test drives a fake. Until a real
Glue catalog — or LocalStack, noted as a follow-up under T30 — has registered a partitioned table
this way and an engine has resolved partitions through it, the claim is untested end to end. That
check is the remaining work, and it belongs with T30's integration lane rather than as new code
here.

---

## Upstream parity backlog — 2026-08-21 cut

Surveyed `../incubator-xtable` at `origin/main` (#882, 2026-08-20) against this tree. The port's
first commit is `09b26ef`, 2026-08-12, so the "merged upstream since we started" window is nine days
and holds exactly one code change — #861, now T21. Everything else in that window (#882, #881, #884,
#857, #859, #850, #877, #856, #873, #867, #851) is JVM packaging, ASF licensing or the Docusaurus
site.

**Deliberately not tracked**, because a Go port does not have the problem: Delta Kernel source and
target (#801, #713, #886), the Spark runtime bundle (#836, PRs #838/#839), bundled-jar size and
licensing (#896, #701), and the `jol-core` classpath bug (#736).

**Watch list** — upstream work with a counterpart gap here, not yet worth a task:

| Upstream | What | Where it would land |
| :--- | :--- | :--- |
| #804, #803 | Variant and geospatial Parquet logical types | `pkg/model/schema.go` — neither type exists |
| #758 | All Paimon `PartitionTransformType` values | `pkg/formats/paimon/` |
| #711, PR #712 | Column rename mishandled in Iceberg schema sync | Check our field-ID path for the same bug |
| #642 | Delta generated columns in schema extraction | `pkg/formats/delta/schema.go` |
| #590, #810, PR #802 | Multi-catalog sync; OneLake; Unity | `pkg/catalog` — we have Glue and Iceberg REST |
| #726 | DuckLake as source/target | New format; parity first |
| #657 | Warn and continue when a target cannot express deletes | Needed once T24 decides the DV story |

Two ideas from PR #829 (a Claude Code skill, not a feature, so nothing to port) are worth keeping:
a per-table SUCCESS/FAILED/NO_OP verdict after sync, and no-op sync detection. Both belong in
T22.

## T20 — Column statistics for the Iceberg and Parquet sources ✅ COMPLETED

**The largest correctness gap in the port.** Only Delta populates `model.DataFile.ColumnStats`.

- `pkg/formats/iceberg/source.go:304` builds its `DataFile` with `PhysicalPath`, `FileFormat`,
  `FileSizeBytes` and `RecordCount` only — while `pkg/formats/iceberg/metadata.go:107` already
  parses `column_sizes`, `lower_bounds`, `upper_bounds`, `value_counts` and
  `null_value_counts` off the manifest entry and then drops them.
- `pkg/formats/parquet/source.go:130` reads footers for the row count and extracts no statistics.
- `pkg/formats/hudi/source.go` reads `PartitionToWriteStats` for file listing, not column stats.

Consequence: Iceberg→Delta and Parquet→Delta lose file-pruning metadata, and the Iceberg target
writes manifests with no bounds, so engines cannot skip files on a converted table.

Upstream did this in three passes worth reading: #805 (expand Parquet column stats, fix schema
conversion bugs), #818 (fall back to Parquet footers when the metadata table has no column stats),
and open PRs #760 and #811. #798 is a trap to avoid — Iceberg requires float and double bounds
normalized before encoding (`-0.0`, `NaN`).

Scope, in order of value:
1. Iceberg source: map the already-parsed manifest bounds into `model.ColumnStat`, keyed by field ID
   through the schema so the names survive a rename.
2. Iceberg target: write bounds back out, with the #798 float normalization.
3. Parquet source: read row-group statistics from the footer (`parquet-go` exposes them) and
   aggregate per column.

**Acceptance:** an Iceberg→Delta sync of a table with bounds produces Delta `add` actions whose
`stats` carry `minValues`/`maxValues`/`nullCount`; a Parquet→Delta sync of a file with row-group
stats does the same; a round trip Iceberg→Delta→Iceberg preserves bounds within the type's
precision. Table-driven tests per format, plus one case each for `NaN` and `-0.0`.

### Outcome ✅

All four criteria checked. `pkg/formats/iceberg/stats.go` carries the bound codec — Iceberg's
single-value binary serialization, base64-encoded because this port's manifests are JSON rather
than Avro — and maps the manifest's five statistic maps both ways, keyed by field ID so a rename
keeps its bounds. `pkg/formats/parquet/stats.go` aggregates row-group chunk bounds via
`FileColumnChunk.Bounds()`, merging with the column's own `parquet.Type.Compare`.

On #798: upstream's `IcebergColumnStatsConverter.toIceberg` still normalizes nothing. The rule
implemented here widens a zero float bound away from the range (lower → `-0.0`, upper → `0.0`),
because Iceberg orders bounds the way `Float.compare` does, under which `-0.0 < 0.0`; the opposite
direction would prune a file that holds the value being searched for. NaN never becomes a bound.

Related defect found and fixed while here: `delta/target.go` marshalled its stats object with the
error discarded, so one NaN bound emptied a file's *entire* stats string rather than its own range.
`finiteBound` drops non-finite values before the marshal; `TestE2E_ParquetToDeltaCarriesColumnStats`
pins it.

Not covered, deliberately: decimal bounds (they need the minimal big-endian unscaled encoding —
omitted rather than written wrong), nested columns (only top-level field IDs are resolvable from
an Iceberg schema), `column_sizes` (`model.ColumnStat` has no size field, unlike Java's
`totalSize`), and the Hudi and Paimon sources, which still emit no column stats.

## T21 — Stop re-reading Delta commits during incremental sync

Upstream #861, merged 2026-08-12 — the only upstream code change since the port began, and it
applies verbatim.

`pkg/formats/delta/source.go:289` (`GetChangesSince`) lists commit files, reads each one to test its
timestamp, then calls `GetTableChangeForCommit`, which at `:255` reads **the same file again** and at
`:260` calls `GetTable(commitID)` to rebuild the table. An N-commit backlog costs 2N object reads and
N schema rebuilds.

Fix: read each commit once, pass the parsed commit into the change builder, and reuse one table
snapshot across the backlog instead of rebuilding per commit. Watch for the case the rebuild exists
to serve — a schema change mid-backlog — and carry the schema forward rather than dropping it.

**Acceptance:** a benchmark over a 100-commit Delta backlog shows the object-read count halved (count
reads through a counting `io.Storage` wrapper in the test, do not assert on wall-clock); existing
Delta incremental tests stay green; add a case with a schema change in the middle of the backlog.

### Outcome — done, and the saving is larger than 2× ✅

Measured through a counting `io.Storage` wrapper in
`pkg/formats/delta/incremental_test.go`, a 100-commit backlog cost **5350 object reads** before and
costs **100** after — one read per commit file, nothing else.

The 2N in the task description understated it. `GetTableChangeForCommit` called `GetTable`, which
walks the log prefix from version 0, so the backlog was quadratic: N reads to test instants, N to
convert, and N(N+1)/2 to rebuild the table once per commit, plus a final `GetCurrentTable` prefix
walk (100 + 100 + 5050 + 100).

`GetChangesSince` now walks the log once. `tableFromMetadata` — the schema-parsing half of
`GetTable` — runs only where a commit carries a metaData action; commits in between reuse that table
through `tableAsOf`, a shallow copy that moves the instant and shares the schema. The current table
comes out of the same walk rather than a second one. `changeFromCommit` takes the parsed commit, so
`GetTableChangeForCommit` (still an `spi` method, still a single-commit read plus `GetTable`) and the
backlog walk share one converter.

Two cases guard the schema handling: a metaData action in the middle of the backlog (versions before
it keep one column, versions after it see two), and one in a commit *older* than `fromInstant`, which
is walked but never emitted — the case the per-commit rebuild existed to serve.

`GetCurrentSnapshot` still has the older 2N shape it does not share with this path (a `GetTable`
prefix walk followed by a second walk over every commit). Left alone deliberately; it is a separate
change.

## T22 — Agent-legible CLI: sync mode, dry run, JSON output, timeout ✅ COMPLETED

Upstream issue #889 is open and unresolved, so this is a place the port can lead rather than follow.
Related upstream: #821 and PR #823 (per-table timeout), PR #794 (fail on sync error), #594
(continuous sync).

`cmd/polytable/main.go` exposes three flags — `--datasetConfig`, `--basePath`, `--format` — and prints
emoji-decorated prose to stdout (`:119`, `:124`, `:153`). Exit codes are already correct: `:164`
aggregates `hasErrors` and returns an error, so a failed sync exits non-zero. What is missing:

- `--output json` on `sync` and `inspect`, emitting one machine-readable document — per-table target
  results, durations, `lastInstantSynced`, and errors as strings. Keep the human output as the
  default; send JSON to stdout and progress chatter to stderr.
- `--mode full|incremental` to override `DatasetConfig.SyncMode` from the command line. The field
  exists (`pkg/conversion/config.go:54`) and is only reachable from the config file.
- `--dry-run`, which resolves the source, computes the changes and reports what would be written
  without committing. Needs a no-commit path through `Controller.Sync`, not a flag checked in `main`.
- `--timeout` per table, applied as a `context.WithTimeout` around each dataset.
- A per-table verdict in the JSON: `SUCCESS`, `FAILED`, or `NO_OP` when the incremental path found no
  new commits (`pkg/conversion/controller.go:205` already returns success with an unchanged instant —
  that is the no-op case, and it is currently indistinguishable from real work).

**Acceptance:** `polytable sync --output json` output parses with `encoding/json` and round-trips into a
struct in `cmd/polytable`; `--mode full` forces a snapshot sync on a table that would otherwise go
incremental; `--dry-run` leaves the target's metadata directory byte-identical; a table whose source
has no new commits reports `NO_OP`. Golden-file tests for the JSON shape.

### Outcome ✅ COMPLETED

All four flags landed, plus the `SyncResult.Verdict()` this task named. `pkg/spi/sync.go` gained
`SyncResult.NoOp` and a `Verdict()` method (`SUCCESS`/`FAILED`/`NO_OP`); `pkg/conversion/controller.go`
sets `NoOp` at exactly the site the task named (the empty-changes branch of `syncToTarget`, was
`:205`) and gained `Sync(ctx, cfg, opts ...SyncOption)` with `WithDryRun()` — a controller-level
no-commit path, not a `main`-only flag check, satisfying the task's explicit requirement. Dry run
skips `CommitSnapshot`/`CommitChanges` and skips catalog registration (registering an unwritten
target would read stale or absent state); `Init`/`GetTableMetadata` still run under dry run because
every target adapter's implementation of both is in-memory only (verified by reading all five
`pkg/formats/*/target.go` before relying on it).

`cmd/xtable/output.go` (new file) holds the JSON types (`SyncOutput`, `TableSyncOutput`,
`TargetSyncOutput`, `InspectOutput`) and pure builder functions (`buildTableSyncOutput`,
`buildInspectOutput`) that sort targets by format name, since `Controller.Sync` returns a map.
`cmd/xtable/main.go` gained `--output text|json`, `--mode full|incremental`, `--dry-run`, `--timeout`
on `sync`, and `--output` on `inspect`; JSON goes to stdout via `cmd.OutOrStdout()`, progress chatter
moves to stderr only in JSON mode (`--output text`, the default, keeps both on stdout, matching prior
behavior). `--timeout` wraps each dataset via `withDatasetTimeout`, a helper factored out so the
wiring is unit-testable without depending on storage slow enough to observe a real deadline.

Acceptance, checked directly:
- **JSON round-trip**: `TestSyncOutput_JSONRoundTrip`/`TestInspectOutput_JSONRoundTrip`
  (`cmd/xtable/main_test.go`) marshal a fixed fixture and unmarshal it back into the same struct.
  Also proven against the built binary — `xtable sync --output json` piped through `python3 -c
  "import json; json.load(...)"` parsed cleanly, with progress chatter confirmed absent from stdout.
- **`--mode full` forces a snapshot sync**: `TestController_ModeFullForcesSnapshotSyncEvenWithNoNewCommits`
  and the CLI-level `TestSyncOneDataset_ModeFullForcesSnapshotSync` both show incremental mode going
  `NO_OP` on a table with no new commits, and the same table under forced `SyncMode: FULL` reporting
  `SUCCESS` instead — proving the snapshot path actually ran. Reproduced against the built binary
  with a real Delta→Iceberg table.
- **`--dry-run` leaves target metadata byte-identical**: `TestController_DryRunLeavesTargetByteIdentical`
  (in-memory storage, SHA-256 per file) and `TestSyncOneDataset_DryRunLeavesTargetByteIdentical`
  (local filesystem) both hash every file under the target's metadata directory before and after a
  dry run and assert equality. Reproduced against the built binary: `shasum` of every file under
  `metadata/` was identical before and after `xtable sync --output json --mode full --dry-run`.
- **`NO_OP` verdict**: `TestController_IncrementalSyncWithNoNewCommitsReportsNoOp` and
  `TestSyncOneDataset_NoNewCommitsReportsNoOp` build a real one-commit Delta source, sync it twice,
  and assert the second sync's verdict is `NO_OP` while `StatusCode` stays `SUCCESS`. Reproduced
  against the built binary.
- **Golden-file tests for the JSON shape**: `TestSyncOutput_GoldenFile`/`TestInspectOutput_GoldenFile`
  compare indented JSON against `cmd/xtable/testdata/{sync,inspect}_output.golden.json`, built from a
  fixed-clock fixture so timestamps/durations cannot make the test flaky (`UPDATE_GOLDEN=1` regenerates
  them).

Deviation from the ground rules, forced by the language rather than chosen: `cmd/xtable/main_test.go`
is `package main`, not an external `<pkg>_test` package. Go cannot import a `main` package, so the
black-box convention has no available form here; the tests stay table-driven with `t.Parallel()`
throughout. `make check` passes, including `go test -short -race ./pkg/...` (this task touches
`pkg/conversion` and `pkg/spi`, not goroutine code, but the race run was still executed per the
ground rules and is clean).

## T23 — Catalog table discovery (list and scan) ✅ COMPLETED

Extends T14, and blocked on nothing.

`pkg/catalog` resolves exactly one table at a time: `ConversionSource.GetSourceTable`
(`pkg/catalog/conversion.go:92`). There is no list, scan or pagination anywhere — `grep -rn
"GetTables\|ListTables" pkg/` returns nothing.

The AWS Lambda reference architecture builds its whole design on the operation we lack: page through
the Glue Data Catalog, select tables carrying conversion markers, and convert each. It uses its own
property convention — `xtable_table_type` and `xtable_target_formats` — which appears nowhere in the
Java tree, so we are free to choose ours. Note that
`catalog.TableFormatFromProperties` (`pkg/catalog/conversion.go:100`) already resolves the *source*
format from `table_type` and `spark.sql.sources.provider`, matching Java's `TableFormatUtils`;
nothing resolves a *target* format list.

Scope:
1. Add `ListTables(ctx, database string, filter TableFilter) iter.Seq2[TableIdentifier, error]` to
   `ConversionSource`, implemented with the Glue paginator.
2. Add target-format resolution from table properties, alongside the existing source resolution.
3. Expose it as `polytable sync --catalog glue --database <db>`, converting every marked table.

**Acceptance:** a fake Glue client returning three pages yields every table exactly once and
surfaces a mid-pagination error rather than truncating silently; a table with no target-format
property is skipped, not failed; the CLI path is covered by a test against the fake.

### Outcome ✅ COMPLETED

`ConversionSource` gained `ListTables(ctx, database, TableFilter) iter.Seq2[TableIdentifier, error]`,
implemented in `pkg/catalog/glue_conversion.go` over the SDK's `glue.NewGetTablesPaginator`; the
unexported `glueTableReader` seam widened with the `glue.GetTablesAPIClient` signature so a fake can
be handed straight to the paginator. `TableFilter` carries one field, `RequireConversionMarkers`; its
zero value selects everything. A failing page yields the error and ends the sequence, and abandoning
the sequence stops the paging. `IcebergRESTConversionSource.ListTables` yields a single
`ErrCatalogNotImplemented` — listing is Glue-only for now, and refusing beats reporting an empty
namespace.

**Marker key decision: `polytable_target_formats`**, comma-separated `model.TableFormat` values,
resolved by `catalog.TargetFormatsFromProperties`. It is this project's own convention: Java XTable
has no target-format property at all, and the AWS Lambda reference architecture's
`xtable_target_formats` is that solution's private convention, deliberately not adopted (the doc
comment on `PropTargetFormats` says so). The `polytable_` prefix matches `polytable_synced_time`,
which `GlueCatalogSyncClient` already writes. Absent or blank marker returns `(nil, nil)` so an
unmarked table is skipped; a marker naming an unknown format is an error, since that is a typo the
operator wants to hear about. Source-format resolution is untouched — `TableFormatFromProperties`
still reads `table_type` then `spark.sql.sources.provider`, keeping Java `TableFormatUtils` parity.

`conversion.DiscoverDatasets(ctx, *catalog.Config, CatalogSourceFactory)` (new
`pkg/conversion/discovery.go`) turns a database into `[]*DatasetConfig` — format, paths and name from
the catalog entry, targets from the marker, namespace from the database. `CatalogSourceFactory` is
the read-side counterpart of `CatalogClientFactory` and the fake seam; `ResolveSourceCatalog`'s
inline factory parameter now uses the same named type. Materialization lives in `pkg/conversion`
rather than `cmd/polytable` so the CLI, the daemon and the bindings can all reach it.

`polytable sync` gained `--catalog glue --database <db>` (plus `--catalogId`). `RunE` is now a thin
wrapper over `runSync(cmd, *syncOptions)`; both source paths produce `[]*conversion.DatasetConfig`
and converge on the pre-existing loop, so `--output json`, `--dry-run`, `--timeout` and `--mode`
apply identically to a discovered table and a configured one, and per-table results land in the same
`SyncOutput` document. `--datasetConfig` and `--catalog`/`--database` are mutually exclusive, and
naming neither is an error that names both options. `syncOptions.newCatalogSource` is per-call
injection rather than a package-level variable, so the CLI tests stay parallel-safe.

Acceptance, checked directly:
- **Three pages, every table exactly once**: `TestGlueConversionSourceListTables/three pages yield
  every table exactly once` (`pkg/catalog/conversion_test.go`) drives the real SDK paginator against
  a fake serving 2+2+1 tables with distinct continuation tokens, and asserts a count of 1 per table
  plus exactly three page calls.
- **A mid-pagination error surfaces**: the same test's error subtest fails page 2 of 3 and asserts
  the yielded error names the database and the cause, with only the first page's table yielded —
  never a short listing that looks complete. `TestDiscoverDatasets/a listing error surfaces` shows
  the same at the `pkg/conversion` layer, returning no partial slice.
- **An unmarked table is skipped, not failed**: asserted at all three layers —
  `TestGlueConversionSourceListTables` (filter drops it), `TestTargetFormatsFromProperties` (absent
  and blank markers return `nil, nil`), `TestDiscoverDatasets`, and the CLI test below.
- **The CLI path is covered against the fake**: `TestRunSync_CatalogDiscovery`
  (`cmd/polytable/catalog_sync_test.go`) builds three real one-commit Delta tables, two marked and
  one not, hands `runSync` a fake `catalog.ConversionSource`, and parses the emitted JSON: two tables
  each `SUCCESS` on `ICEBERG`, the unmarked one absent, progress chatter on stderr only. Sibling
  subtests cover `--dry-run` (no `metadata/` directory written), text output, a discovery failure
  surfacing instead of an empty run, and `TestRunSync_SourceSelectionFlags` covers every rejected
  flag combination.

Same `package main` deviation as T22 for the CLI tests, for the same reason: Go cannot import a
`main` package. `make check` passes, and `go test -short -race ./pkg/...` is clean.

## T24 — Deletion vectors beyond Delta: implement or narrow the claim

### What happens today, checked rather than assumed

`pkg/formats/delta/source.go:540` parses a Delta `deletionVector` into `model.DeletionVector`, so
the descriptor reaches the internal model intact. **Nothing consumes it.** `grep -rn DeletionVector
pkg/formats/iceberg pkg/formats/hudi` returns nothing; only `pkg/model/datafile.go` and the Delta
adapter mention it at all.

**And there is no diagnostic.** A search across `pkg/` and `cmd/` for any deletion-vector mention
co-occurring with warn, log, error, unsupported or skip returns zero lines. A sync of a Delta table
whose rows were deleted by a deletion vector reports success and produces a target table that
answers queries with the deleted rows still present.

The README once claimed Iceberg "✅ (Equality/Positional)" and Hudi "✅"; both were corrected to `—`
on 2026-08-21. That made the documentation honest. **It did not change the behavior**, and honest
documentation is not a substitute for a tool that declines to do the thing it cannot do.

### The same defect exists in Java XTable, demonstrated

The session working on upstream PR #897 reproduced it end to end on 2026-08-22, and the shape is
worth recording because it is the oracle this repository should copy:

- 1000 rows, `delta.enableDeletionVectors=true`, sync DELTA→ICEBERG. Iceberg snapshot reports
  `total-records=1000`, 4 data files, 0 delete files. Correct.
- `DELETE FROM ... WHERE id < 100`. Delta now reads 900 rows; one `deletion_vector_*.bin` on disk.
- Incremental sync reports **"Sync is successful"**, advances `sourceIdentifier`, writes a new
  Iceberg snapshot — still `total-records=1000`, `total-delete-files=0`.
- A **pyiceberg** scan of the result returns 1000 rows with all 100 deleted ids present.

The conclusion held because the oracle was an independent reader, not the tool's own log. No
assertion about XTable made using XTable could have found this.

### polytable's mechanism differs, and the difference is not an improvement

Java cancels a `RemoveFile(old)` + `AddFile(same path, with DV)` pair out of both maps, since at
file level nothing changed. polytable has **no cancel-out**: `GetTableChangeForCommit`
(`pkg/formats/delta/source.go:381-396`) appends every `Remove` to `removed` and every `Add` to
`added` with no pairing step, so the same commit surfaces as a removal and an addition of the same
path. Re-adding the file re-adds every row in it, so the target ends up equally wrong by a different
route. **Do not cite polytable as the better-behaved implementation.**

### The design question that has to be settled first

**An earlier version of this task argued that translation was inexpressible without violating
INV-1. That argument was wrong, and it is corrected here rather than quietly removed**, because it
was repeated to another session and nearly published.

A Delta deletion vector is a roaring bitmap of **row positions**, stored in its own small side file
(`deletion_vector_*.bin`) beside the data — not inside the data file. Decoding it means reading that
side file, which is metadata-sized. Iceberg positional deletes are a list of positions. So the
transform is side-file to side-file, and **nothing about it requires reading data files**. Upstream's
own merged design says so: RFC-2 (`rfc/rfc-2`, PR #634, merged 2025-03-03) specifies the conversion
and costs it as "proportional to the number of records affected by the deletion operations, and not
the size of metadata or number of files".

INV-1 therefore does **not** forbid this. Not decoding remains a defensible *choice* — it keeps the
translator cheap — but it must be stated as a choice. `SPEC.md` §10.4 has been corrected the same
way.

Two facts bound the decision:

- **Upstream already designed it.** RFC-2 is merged and specifies the conversion, so this is not a
  question of feasibility. The claim previously made here — that Microsoft extended XTable for
  Fabric and therefore must be reading data files — was doubly unsound: the reading-data-files half
  is false as above, and the Fabric half rests on weaker evidence than stated. See T30's note.
- **Upstream has not.** #339 is the epic, with #343–#348 splitting read and write per format, #640
  for the snapshot case, and PR #661 open since March 2025. Open since 2024 and unimplemented.
  Whether that is neglect or a deliberate refusal to cross the same line is not recorded.

### Three outcomes, and only one of them is cheap

1. **Refuse and say so.** Detect a deletion vector the target cannot represent, and fail the sync
   with an error naming the file and the reason. Safe, honest, and small. Breaks anyone currently
   getting silently-wrong output, which is the point.
2. **Warn and continue.** Upstream #657's flag. Preserves current behavior for people who know what
   they are getting. Weaker: a warning in a log is not seen by whoever queries the table a week
   later, and the table itself carries no marker.
3. **Translate.** Requires reading data files, so requires reopening INV-1 — a decision about what
   polytable *is*, not about deletion vectors.

**Option 1 is the recommended default**, with the target-side capability check written so that
option 3 can be added later without changing the call sites. Note that Delta→Delta and Delta→Hudi
have different answers from Delta→Iceberg, so the check belongs on the target, not in the source.

### Acceptance

- A Delta table with a deletion vector, synced to Iceberg, either produces a target whose row count
  matches the source **or** fails with an error naming the deletion vector. It must not report
  success and produce a table with the deleted rows present.
- A fixture with a real deletion vector is committed. delta-rs `DeltaTable.delete()` rewrites files
  rather than writing deletion vectors by default, so generating one needs
  `delta.enableDeletionVectors=true` and a writer that honours it — establish which, and record the
  recipe next to the fixture, because the next person will hit the same wall.
- Verification uses an **independent reader** — pyiceberg for Iceberg output, delta-rs for Delta —
  and asserts the row count and the absence of the deleted ids. A test that asks polytable whether
  polytable is correct does not close this task.
- `SPEC.md` records the decision and its reason, whichever outcome is chosen.

**Commit:** `fix: refuse to sync a deletion vector the target cannot represent`

## T25 — Audit upstream bug fixes merged before the port started

Out of scope for the 2026-08-21 survey, which only covered the nine days since `09b26ef`. The port
was written against a checkout that already contained these fixes, but whether each survived the
rewrite into Go is **unverified** — a Go reimplementation can reintroduce a bug the Java tree fixed
years earlier, and nothing in this repo has checked.

Highest-value candidates, each with a Java test to port:

| Upstream | Fix | Go file to check |
| :--- | :--- | :--- |
| #826 | Map key path handling in Delta schema extractors | `pkg/formats/delta/schema.go` |
| #795 | NPE for binary type inside map/array schemas | `pkg/formats/delta/schema.go` |
| #828 | Null partition value for composite generated-column partitions | `pkg/formats/delta/source.go` |
| #816 | Batch `INSERT_OVERWRITE` replacecommits dropping adds | `pkg/formats/hudi/timeline.go` |
| #732 | Empty `EarliestCommitToRetain` | `pkg/formats/hudi/source.go` |
| #797 | Iceberg nested comments with a qualified name | `pkg/formats/iceberg/schema.go` |
| #806 | Parquet snapshot sync over multiple partitioned commits | `pkg/formats/parquet/source.go` |

**Acceptance:** one table-driven test per row, written from the Java test case, each either passing
against current Go or accompanied by the fix. Report the tally — a row that already passes is a
result worth recording, not a wasted test.

### Outcome — all seven resolved: 3 already correct, 2 fixed, 2 with no Go counterpart ✅

Tests live in `pkg/formats/<format>/upstream_audit_test.go`, one per row, each naming the Java
commit and test it was written from.

| Upstream | Verdict | What the audit found |
| :--- | :--- | :--- |
| #826 | Passes (path half no-counterpart) | No Go adapter populates `model.Field.ParentPath`, so there is no path to mis-qualify. The portable half — a struct-keyed map keeping its key and value subtrees apart — was already correct. |
| #795 | Passes | Go's parser takes the type node and its nullability directly and never reads field metadata, so the null dereference the binary branch had in Java is structurally impossible. |
| #828 | No counterpart | Generated-column partitions do not exist in Go: every Delta partition column becomes its own `VALUE` field, so nothing joins components with `-` and no `"2013-null-20"` can be produced. |
| #816 | **Fixed** | Mirror image of the Java bug. Go never lost adds, but `HoodieCommitMetadata` had no `partitionToReplaceFileIds` at all, so an `INSERT_OVERWRITE` left the overwritten files in the snapshot beside their replacements. |
| #732 | No counterpart | `IsIncrementalSyncSafeFrom` compares the earliest instant still in the active timeline and reads no clean metadata, so the empty `earliestCommitToRetain` branch cannot arise. |
| #797 | **Fixed** | The Go target writes whole metadata rather than name-addressed column updates, so nothing under-qualifies a name — but nested docs were dropped on the way out and never read back into `Comment` in either direction, so a nested comment could not survive a sync at all. |
| #806 | Passes | The Java change was test-only; the Go source re-crawls on every call and handles successive waves of files, into existing and into new partitions. |

Both no-counterpart rows still got a test, covering the invariant the Java fix protects rather than
the branch that does not exist: for #828, that a missing or null component yields no fabricated
composite; for #732, that unreadable retention metadata resolves to "unsafe" and never to "safe by
default".

Parity gaps the audit surfaced but did not close — each is a feature, not a regression:

- `model.Field.ParentPath` is dead in production code; no format adapter sets it.
- Delta `AddAction.PartitionValues` is `map[string]string`, so a JSON `null` decodes to `""` and a
  null partition value is indistinguishable from an empty one. Fixing the representation ripples
  into every target that formats `Range.MinValue` into a path.
- Generated-column (composite) Delta partitions are unsupported.
- Hudi reads no clean or archival metadata.
- Hudi `GetTableChangeForCommit` ignores its `commitID` and returns the whole snapshot as adds.
- The Delta reader ignores field metadata, so the `__xtable_logical_type` UUID mapping is absent.

## T26 — Investigate the timestamp basis of Delta incremental sync

**Investigate before writing code.** Upstream #779 ("handle log truncation in Delta to Iceberg
incremental sync") is still open, so there is no Java fix to mirror, and the obvious reading of the
title does not match our code.

What the code actually does: `pkg/formats/delta/source.go:322` (`IsIncrementalSyncSafeFrom`) returns
safe when `versions[0].CommitTime <= earliestInstant`. Log cleanup removes a contiguous *prefix* of
commits, so after truncation `versions[0]` is a **later** commit with a **later** timestamp, the
comparison fails, and `pkg/conversion/controller.go:184` already falls back to a snapshot sync. A
fixture that deletes commits below a checkpoint will therefore pass against current code — do not
write one, see it green, and mark this task done.

The exposure worth chasing is that both halves of incremental sync key on the **commit timestamp**
rather than the version number: the safety check compares `CommitTime`, and `GetChangesSince`
(`:289`) selects commits with `CommitTime > fromInstant`. Two consequences to test:

- Two commits written inside the same millisecond share a `CommitTime`. Sync after the first, and
  the second is `> fromInstant` false — silently never synced, with the sync still reporting success.
- A commit whose `commitInfo` timestamp is not greater than its predecessor's (clock skew, or a
  writer that does not enforce monotonicity) is dropped the same way.

Establish whether either reproduces before choosing a fix. If they do, the fix is likely to key
incremental selection on the Delta **version number**, which is monotonic by construction, and keep
the timestamp only for the retention comparison.

**Acceptance:** a written finding either way. If reproduced, a table-driven test per case plus the
fix; if not, a note in this task saying what was tested and why the current basis is sound, so the
next reader does not repeat the investigation.

### Outcome — reproduced, three ways, and fixed ✅

Both suspected cases reproduce, and a third one does. Fixtures are hand-written log files in
`pkg/formats/delta/incremental_test.go` (the Delta target cannot produce these timestamps); each
case is three commits with the last one anomalous, `GetChangesSince(2000)` against them, and the
expectation that version 2 comes back:

| Case | Before | After |
| :--- | :--- | :--- |
| Two commits sharing a millisecond | version 2 dropped, sync reports success | synced |
| Timestamp goes backwards (clock skew) | version 2 dropped | synced |
| Commit with no `commitInfo` action | dropped for **every** `fromInstant`, since its `CommitTime` is 0 | synced |

The third is the widest: `commitInfo` is optional in the Delta protocol, and a commit without it
was invisible to incremental sync regardless of clock behaviour.

The fix keeps the instant an `int64` — target state is a bare timestamp
(`model.KeyLastInstantSynced`), so a version number has nowhere to live — but derives it from
version order rather than trusting the log: `advanceCommitTime(previous, raw)` returns `raw` when it
exceeds the preceding commit's instant and `previous + 1` otherwise. The result increases strictly
with the version, which makes it an injective proxy for the version number, so
`instant > fromInstant` selects exactly the versions after the one `fromInstant` names. A
well-behaved log is unaffected: instants pass through as the raw timestamps. Java resolves the same
ambiguity by mapping the sync instant to a version once and keying the backlog on versions
(`DeltaConversionSource#getCommitsBacklog`, `:204`).

The derived instant is what `GetTable`, `GetTableChangeForCommit` and `GetChangesSince` report,
because the controller persists the last one and feeds it back as the next `fromInstant`; reporting
a raw timestamp while selecting on a derived one would replay the boundary commit forever.
Retention (`IsIncrementalSyncSafeFrom`) still compares raw timestamps — it asks when the earliest
retained commit happened, not which version an instant names — and now returns false when that
commit has no timestamp at all, rather than reading 0 as "old enough".

One migration edge, no code: a target synced before this change whose stored instant sits at an
anomaly can see one commit re-emitted on the next sync. That is at-least-once, not loss.

## T27 — Rename the project to `polytable` ✅ COMPLETED

**Decided by the maintainer, 2026-08-21.** "XTable" is an ASF podling mark, so publishing
independently under `xtable-go` is a trademark problem. The new name is **polytable** — cleared
against the lakehouse space on 2026-08-21 (no project uses it; the only near names are Apache XTable
itself and its predecessor OneTable, which is Onehouse's mark and equally off-limits).

**This task blocks the first tag.** A Go module tag bakes the module path in immutably (see T19's
donation table — this row now resolves the other way: independent publication, not donation naming).

Rename surface:

1. Module path `github.com/slachiewicz/xtable-go` → `github.com/slachiewicz/polytable` — every
   import in the tree, `go.mod`, and the checked-in docs that quote it.
2. Binaries and dirs: `cmd/xtable` → `cmd/polytable`, `cmd/xtable-service` → `cmd/polytable-service`,
   `cmd/xtable-wasm` → `cmd/polytable-wasm` (keep the `//go:build js && wasm` tag and the js/wasm vet
   line in `Makefile`/CI pointed at the new path). Artifact names in `release.yml`,
   `build-artifacts.yml` and `Makefile` follow.
3. C binding: `libxtable` → `libpolytable`; exported symbols `xtable_sync_json`/`xtable_inspect_json`
   → `polytable_*`. Nothing is released, so the FFI break costs nothing today.
4. Python: `pyxtable` → `polytable` (claim the PyPI name before publishing; T6's packaging work
   carries over).
5. REST: `spec/rest-service-open-api.yaml` title/description, then regenerate via `spec/Makefile`.
6. Docs: `README.md`, `SPEC.md`, `AGENTS.md`, `CLAUDE.md`, `NOTICE`. Keep one nominative-fair-use
   line — "a Go implementation of the translation model of Apache XTable (incubating)" — as factual
   attribution; the Apache-2.0 §4(d) attribution in `NOTICE` stays regardless.
7. GitHub repo rename to `polytable` — maintainer action, outside the tree; do it in the same window
   as the module-path commit so the path is never published dangling.

**Deliberately NOT renamed:**

- The on-disk metadata keys `xtable_last_instant_synced` / `xtable_source_format`
  (`pkg/model/metadata.go:36-40`). They are interop with Java-XTable-synced tables, not branding;
  renaming them silently breaks round-tripping against upstream.
- The 16-line ASF grant headers (T19). Donation remains possible after a rename — ASF renames
  incoming podlings anyway — so the T19 decision stands unless the maintainer revisits it.
- References to `../incubator-xtable` and upstream issue numbers in docs — those *name* the upstream
  project, which is exactly what nominative use is for.

**Acceptance:** case-insensitive `grep -ri xtable` over the tree returns only the metadata keys and
their tests, nominative upstream references in docs, and this file's history. `make check` passes.
The C header, wheel and REST spec all carry the new name. No tag exists yet when this lands.

**Commit:** `refactor: rename the project to polytable`

### Outcome ✅

Done in one commit. The module path, the three `cmd/` directories, the C library and its four
exported symbols, the Python package and the REST spec title all carry the new name; `make check`
passes, `oapi-codegen` regenerates from the retitled spec, and the generated `libpolytable.h`
declares `polytable_version`, `polytable_free_string`, `polytable_sync_json` and
`polytable_inspect_json`.

Three on-disk strings the task text did not list were renamed too, because the carve-out's stated
reason is Java interop and these have no Java counterpart: the Delta/Hudi commit operation labels
(`XTABLE_SYNC_SNAPSHOT` → `POLYTABLE_SYNC_SNAPSHOT` and siblings — Java writes
`DeltaOperations.Update("xtable-delta-sync")` and `WriteOperationType.UNKNOWN`), the Parquet
sidecar directory `_xtable_metadata`, and the Glue parameter `xtable_synced_time`. Nothing is
released, so this was the last free moment to change them. `MetadataPropertyPrefix = "xtable_"`
stays with the two keys it prefixes, and its doc comment now says why.

All four C symbols were renamed rather than the two the task names: a `polytable_sync_json` beside
an `xtable_version` would be an incoherent ABI.

What is left on a case-insensitive grep, by category: the two metadata keys and the prefix constant
(plus their `pkg/catalog/rest.go` literals and the `SPEC.md`/`AGENTS.md` invariants that quote
them); nominative references to Apache XTable, `../incubator-xtable`, `xtable.apache.org` and the
upstream Maven module names in docs and `.claude/skills/`; and this file's history of earlier
tasks.

---

# Integration-test plan — 2026-08-21

## T28 — Real-writer fixtures ✅ (Iceberg half unblocked by T31)

Every suite in the tree reads metadata polytable itself wrote, so a reader that agrees with
polytable's writer passes even where the pair disagrees with the format. Check in small tables
written by engines that have never seen this code, and assert the readers against them.

### Steps

1. `test/fixtures/generate.py` writes the fixtures from delta-rs and pyiceberg — JVM-free writers —
   each with three commits, a mid-history column addition, a partition column and numeric columns so
   statistics exist.
2. Beside each fixture, a `manifest.json` records what the writer reported: schema, commit count,
   row totals, per-column bounds, partition values, per-file row counts. The Go tests assert against
   that, so regenerating a fixture regenerates its expectations.
3. `test/foreign_fixtures_test.go` reads each fixture through `pkg/formats.NewSource` and converts
   it to the other formats through `pkg/conversion`.

**Acceptance:** the fixtures are committed and under 1 MB; the tests run under `go test -short` with
no container; every assertion traces to `manifest.json`; a reader gap is fixed or recorded, never
assumed away.

**Commit:** `test: read fixtures written by delta-rs and pyiceberg`

### Outcome ✅ — the fixtures landed and found four reader gaps, one of them fixed here

Fixtures: `test/testdata/fixtures/delta-rs/sales` (delta-rs 1.6.2, 3 commits, 6 data files, 14 rows)
and `test/testdata/fixtures/pyiceberg/events` (pyiceberg 0.11.1, 3 snapshots, 5 metadata versions,
6 data files, 12 rows). 55 KiB in total.

**The Delta source reads delta-rs output correctly** — schema including the merged column, partition
values from `partitionValues` rather than the directory name, per-file row counts, and the bounds
delta-rs recorded. Walked as an incremental backlog it returns all three commits, each carrying the
schema as of that commit rather than the latest. Conversion to Iceberg and Hudi keeps the file list
and the row counts. That is the first evidence in this repo that any polytable reader agrees with a
foreign writer.

**The Iceberg source could not read the pyiceberg table at all.** Two causes, one fixed here:

- *Fixed:* `listMetadataFiles` matched only `v<N>.metadata.json`, the Hadoop-layout name polytable's
  own target writes. Every catalog-backed writer — pyiceberg, the Java library, Spark — writes
  `<%05d version>-<uuid>.metadata.json`, so a real table looked like it had no metadata whatsoever.
  `iceberg.MetadataFileVersion` now accepts both and `listMetadataFiles` carries the path rather than
  rebuilding it from the version, since the UUID cannot be reconstructed. With that, the schema,
  field IDs, partition spec and commit instant of the pyiceberg table all read correctly.
- *Recorded, then fixed by T31:* the file list could not be read at all. See F1.

Since T31 the pyiceberg fixture reads and converts like the delta-rs one:
`TestForeignFixtures_ReadIcebergSnapshot` checks the file list, row counts and partition tuples
against `manifest.json`, and `TestForeignFixtures_ConvertIceberg` runs it through every other
target. F2, F3 and F4 remain recorded rather than fixed, each with a pinned assertion.

### Watch list

- ~~**F1 — Iceberg manifests are Avro, and polytable parses them as JSON.**~~ **Fixed by T31.** The
  pyiceberg fixture now reads and converts; `TestForeignFixtures_ReadIcebergSnapshot` and
  `TestForeignFixtures_ConvertIceberg` assert the file list instead of the failure.
- **F4 — the Hudi target mangles a data file path that carries a URI scheme.** It trims the table's
  base path off `PhysicalPath` with a plain string prefix match. The Iceberg source reports the
  location the manifest recorded, scheme and all (`file:///…`), so the prefix never matches, the
  whole absolute path is stored as if it were relative, and the Hudi source joins it onto the base
  path a second time — `…/events/file:/…/events/data/…`. Found by T31 when a foreign Iceberg table
  first became a conversion source. Pinned by `pathsDoubled` in `TestForeignFixtures_ConvertIceberg`.
- **F2 — the Paimon target and the Paimon source disagree on the table layout.** The target writes
  `metadata/schema-<epoch>.json` and `metadata/manifest.json`; the source reads `schema/schema-*`
  and `snapshot/`, which is the real Paimon layout. Nothing polytable writes as Paimon can be read
  back as Paimon, in either direction of the mismatch. No existing test caught it because the e2e
  matrix round-trips only Delta, Iceberg and Hudi. Pinned in `TestForeignFixtures_ConvertDelta`.
  **Fixed by T32**: the target now writes the source's layout and the pin asserts the round trip.
- **F3 — the raw-Parquet source rebuilds the schema from one data file.** It reads the footer of
  whichever file sorts first, so on a schema-evolved table the result depends on file names, and the
  Hive partition column — which lives in the directory name, not in the file — is missing from the
  schema even though the same source reports it as a partitioning field. The table it returns is
  partitioned by a column its own schema does not contain. **Fixed by T33**: the schema is the merge
  of every footer, the partition column is synthesized into it, and the pin asserts both.

### Out of scope

Spark-written and Hudi fixtures need a JVM; they are T30's, and nothing here presumes what they will
show. Neither fixture proves polytable can *write* a table another engine will read — that is the
other half of the question, and it needs those engines as readers.

Everything above verifies polytable's output with polytable's own reader. A deviation both sides
share is invisible to that: the writer emits it, the reader accepts it, the test goes green, and
the table is unreadable to the engines the project exists to serve. These tasks put independent
readers on the output.

## T29 — Engine verification of polytable output with DuckDB ✅ (Iceberg pairs unblocked by T31)

DuckDB reads Delta through `delta_scan` (delta-kernel-rs) and Iceberg through `iceberg_scan`, is a
single static binary and needs no JVM, which makes it the cheapest independent judge available.

**Scope.** A `test/` suite, gated on `testing.Short()` and on `exec.LookPath("duckdb")`, that syncs
small tables to Delta and Iceberg from three source formats each, then shells out to `duckdb -json`
and asserts on the row count, on a predicate outside the written value range returning nothing, and
on a partition-value predicate returning the right subset. Out of `make check` on purpose; CI runs
it through `integration.yml`, which already invokes `./test/...` without `-short`.

**Acceptance:** the suite passes against a real duckdb; `make check` stays green; CI installs a
pinned duckdb before the test step.

**Commit:** `test: verify Delta and Iceberg outputs with DuckDB`

### Outcome ✅

Verified with **duckdb v1.5.5 (Variegata)** on macOS arm64, core `delta` extension `45c4087` and
core `iceberg` extension `45163a28`. The Iceberg rows below were re-run after T31 landed; before it
they were skipped.

| Pair | Result |
|---|---|
| Parquet → Delta | ✅ read by `delta_scan` |
| Iceberg → Delta | ✅ read by `delta_scan` |
| Hudi → Delta | ✅ read by `delta_scan` |
| Parquet → Iceberg | ✅ read by `iceberg_scan` — after T31 |
| Delta → Iceberg | ✅ read by `iceberg_scan` — after T31 |
| Hudi → Iceberg | ✅ read by `iceberg_scan` — after T31 |

**The Delta writer was broken for every non-polytable reader, and this is the headline finding.**
Two keys were emitted with `omitempty`, so a table with no format options and no field metadata —
that is, every table polytable has ever written — omitted them entirely:

- `metaData.format.options`. The Delta protocol declares it non-nullable. delta-kernel-rs fails the
  whole log with `Encountered unmasked nulls in non-nullable StructArray child`.
- `metadata` on each `schemaString` field. delta-kernel-rs fails with ``missing field `metadata` ``.

Both are now written as empty objects, via `delta.NewParquetFormat()` and a non-omitempty tag, and
`TestDelta_MetadataCarriesKernelRequiredKeys` guards them on the raw JSON. It has to assert on the
JSON: a round trip through polytable's own Delta source passes either way, because the reader
ignores both keys — which is exactly how this survived until an outside reader looked at it. The
same defect would have hit delta-rs, Spark's Delta reader and anything else built on the kernel.

**Iceberg output was not readable by any Iceberg engine.** `pkg/formats/iceberg/target.go` wrote
manifest files and manifest lists as JSON (`*-m0.json`, `snap-*.json`); the Iceberg spec mandates
Avro, and DuckDB rejected them with `Incorrect Avro container file magic number`. That was a
feature gap rather than a bug to fix in a test commit — it needed an Avro writer, and `go.mod` had
no Avro dependency — so the three Iceberg subtests synced, asserted the sync succeeded, then
`t.Skip`ped with the DuckDB error attached. **T31 closed it**: the skip is gone, all six pairs
assert, and the run above is the result. T31's outcome records the two further defects the Iceberg
half exposed once DuckDB could open the metadata at all.

**Not verified:** pruning. `EXPLAIN`/`EXPLAIN ANALYZE` output is not a stable contract across DuckDB
releases, so the suite proves correctness under a predicate rather than that files were skipped.
Reading every column (`SELECT *`) is in the suite so the checks decode data pages rather than
answering from the Parquet footer — this is what confirms the `timestamp(millisecond)` column
survives the Delta round trip.



## T30 — Java XTable interop nightly (unscheduled until T28/T29 land)

The sharpest claim was that tables move between polytable and Apache XTable without resync via
shared `xtable_*` keys. **That claim is false** — see T60, which disproves it against a real
Java-synced table. This job is still worth building, but its purpose changes from confirming
interop to measuring it. A `workflow_dispatch` + nightly job pulls the upstream bundled jar, syncs a
fixture with Java XTable, continues **incrementally** with polytable, and the reverse — asserting
the second tool reports incremental, not a snapshot fallback. Never gates PRs (bench.yml
philosophy: failures prompt investigation, not red builds). This job is also where Spark-written
and Hudi fixtures for T28 get generated. Needs a maintainer decision on jar version pinning.

Follow-ups noted, not scheduled: a bindings smoke lane (the C ABI and wheel are built but never
executed in CI; one `polytable.sync()` via ctypes and a node run of `polytable.wasm`), and
LocalStack-Glue for catalog sync integration.

**Coverage bar (ground rule, 2026-08-22):** upstream's `ITConversionController` (in
`xtable-core`) is the benchmark for cross-engine testing — write through a real engine, sync,
read source and every target back through a real engine, compare full datasets. Its scenario
list (upserts and deletes, compaction, concurrent writes, time travel, partition specs, sync
modes, out-of-sync incremental, corrupted-snapshot recovery, Delta column mapping, metadata
retention) is what this suite must match or exceed, not just the happy-path insert. T28/T29
cover the read/write halves with delta-rs, pyiceberg and DuckDB; the scenario dimension is
still mostly open.

## T31 — Iceberg manifests must be Avro, not JSON ✅

**The largest interop defect in the port**, found 2026-08-21 while planning engine verification.
The Iceberg spec requires Avro manifest lists and manifest files. This adapter uses JSON on both
sides: the source parses manifests with `json.Unmarshal`
(`pkg/formats/iceberg/source.go:217,228`), and the target writes `snap-<id>-<uuid>.json` and
`<uuid>-m0.json` (`pkg/formats/iceberg/target.go:170,191`). Consequences, both directions:

- No real engine (Spark, Trino, DuckDB `iceberg_scan`, pyiceberg) can read a polytable-written
  Iceberg table. T29 is expected to confirm this; do not let a JSON-reading shortcut in that suite
  mask it.
- The polytable source cannot read a real Iceberg table — T28's pyiceberg fixture will hit this on
  the manifest read. Only tables polytable itself wrote round-trip.

Library decision first: `hamba/avro/v2` (maintained, pure Go, schema-first) for a manifest codec
of our own, versus adopting `apache/iceberg-go`'s manifest read/write. Prefer **hamba/avro** unless
inspection shows iceberg-go's manifest layer can be used without dragging in its catalog/table
stack — this repo's value is the thin native implementation, and a second table library blurs it.

Scope:
1. Manifest-list and manifest-entry Avro schemas per the Iceberg v2 spec (field-ids in the Avro
   schema metadata matter — engines read them).
2. Target: write Avro; drop the JSON writer entirely. Nothing is released, so there is no
   migration path to preserve — delete, don't fallback. T20's base64-of-binary bounds encoding was
   explicitly "the JSON accommodation"; in Avro the bounds become native `bytes`, so revisit
   `pkg/formats/iceberg/stats.go` in the same change.
3. Source: read Avro manifests. Keep the JSON read path only as long as existing test fixtures
   need it, then delete those too — foreign fixtures (T28) become the read-side truth.
4. `metadata.json` itself stays JSON — that part is spec-correct today.

**Acceptance:** the T28 pyiceberg fixture is read successfully (schema, files, stats); a
polytable-written Iceberg table is read back by pyiceberg and by DuckDB `iceberg_scan` (extend the
T29 suite — that is the regression test); Iceberg→Delta→Iceberg round trip still preserves bounds;
`grep -rn '\.json' pkg/formats/iceberg/target.go` shows only `metadata.json` writes.

**Commit:** `fix: write and read Iceberg manifests as Avro per the spec`

### Outcome ✅

**Library: `github.com/hamba/avro/v2`, with a manifest codec of our own.** `apache/iceberg-go` was
rejected on its dependency graph rather than on its code: the manifest layer lives in the root
package, and that module's `go.mod` requires arrow-go, gocloud, the Azure and GCP SDKs, substrait,
docker/compose and testcontainers. Go's minimal version selection resolves those for the whole
module whether or not the catalog packages are imported, and several of them — aws-sdk-go-v2 above
all — would move pins this repo sets deliberately. hamba brings six pure-Go dependencies, none
already in the graph beyond `klauspost/compress`, and compiles for `js/wasm`. The codec is
`pkg/formats/iceberg/manifest.go`: the v2 `manifest_entry` and `manifest_file` schemas as literal
JSON with the specification's field ids, and records carried as `map[string]any` in both directions.
That shape is not a shortcut. Writing needs it because the `partition` column's Avro type is derived
per table from the partition spec; reading needs it because the schema is whichever one the writing
engine chose, and hamba decodes against the schema embedded in the file.

The schema is written through `ocf.WithSchemaMarshaler` returning the literal JSON, because a round
trip through the parser is not guaranteed to preserve `field-id` properties — and a manifest without
them is one no engine can resolve columns against. `TestIceberg_ManifestCarriesFieldIDsAndHeader`
asserts on the header bytes rather than on a read-back.

**Two further defects surfaced once DuckDB could open the metadata at all.** Both are exactly the
kind a round trip through polytable's own reader cannot see, which is the T29 lesson repeating:

- *`file_format` was written as the canonical name.* `APACHE_PARQUET` reached the manifest verbatim;
  the specification admits `PARQUET`, `ORC` and `AVRO` only, and DuckDB answers `File format
  'APACHE_PARQUET' not supported`. Now mapped both ways.
- *No name mapping, so every column read as null.* An Iceberg reader binds a Parquet column by the
  field id in the file's own schema. Polytable never writes data files, so the files it describes
  carry no ids, and DuckDB returned the right number of rows with every data column NULL — only the
  partition column, which comes from the manifest's partition tuple, had a value. The fallback
  `schema.name-mapping.default` table property fixes it, and it is what the specification prescribes
  for exactly this case. `TestIceberg_MetadataCarriesNameMapping` guards it on the raw JSON.

**Partitions.** Only the identity transform on `boolean`, `int`, `long`, `float`, `double` and
`string` is written; anything else fails the commit with the transform or type named. Date and the
timestamps are excluded deliberately — their Avro form carries a logical type whose unit this port
does not normalize on either side, and a guessed unit files data under the wrong partition as surely
as a wrong type would. A partition column the source reports but its schema does not contain (the
Hive layouts, F3) is added to the written schema: Iceberg resolves every partition field against a
schema column, and an identity-partitioned column need not be stored in the data files because a
reader materializes it from the partition tuple.

**Bounds** moved from T20's base64-of-binary to native Avro `bytes` in the `k126_v127`/`k129_v130`
entry maps. The single-value binary serialization and the -0.0 widening are unchanged;
`EncodeBound`/`DecodeBound` now take and return `[]byte`.

**Verified.** DuckDB v1.5.5 `iceberg_scan` reads all three →Iceberg pairs of the T29 suite — row
count, a predicate above every written id, a partition predicate, and `SELECT *` decoding every
data page. pyiceberg 0.11.1 scans a polytable-written table through `StaticTable.from_metadata`
(`test/fixtures/verify_pyiceberg.py`, a manual check): every value of every column, from a Delta
source and from a raw-Parquet one, and on a Hive-partitioned directory whose files omit the
partition column it materializes the synthesized column from the partition tuple. The T28 pyiceberg
fixture reads and converts into Delta, Hudi and raw Parquet. Iceberg→Delta→Iceberg still preserves
bounds.

**Fixture relocation.** A fixture's Avro manifests hold absolute paths from the machine that
generated them, and generate.py's placeholder substitution cannot reach inside a compressed
container file. `relocateAvroManifests` in `test/foreign_fixtures_test.go` decodes the records,
replaces the location and encodes them again under the file's own schema and header, so what the
Iceberg source reads is still pyiceberg's shape.

## T32 — Paimon round-trip is broken: target and source disagree on layout ✅

T28's F2. The Paimon target writes `metadata/schema-<epoch>.json`; the Paimon source reads
`schema/schema-*` and `snapshot/`. Nothing polytable writes as Paimon can be read back as Paimon —
the e2e matrix round-trips only Delta, Iceberg and Hudi, so nothing caught it. Decide which side
matches the real Paimon spec (almost certainly the source's `schema/` + `snapshot/` layout — check
`../incubator-xtable`'s Paimon module and the Paimon docs), fix the other side, add Paimon to the
round-trip matrix, and pin it with the T28-style assertion already in place.

**Acceptance:** a Paimon table written by the target is read back by the source; the e2e matrix
includes Paimon↔Delta both ways; the T28 pinning assertion flips from "expected broken" to green.

**Commit:** `fix: align the Paimon target with the source's on-disk layout`

### Outcome ✅ — the source's layout was the spec-correct one

Established from the Paimon sources themselves (`paimon-bundle` 1.3.1, the version
`../incubator-xtable`'s `pom.xml` pins, sources jar in the local Maven repository), because the Java
module reads through Paimon's own `FileStoreTable`/`SnapshotManager` and so records no layout of its
own:

- `SchemaManager.toSchemaPath` → `<table>/schema/schema-<id>`, `SCHEMA_PREFIX = "schema-"`.
- `SnapshotManager.snapshotPath` → `<table>/snapshot/snapshot-<id>`, `SNAPSHOT_PREFIX = "snapshot-"`.
- `HintFileUtils` → `snapshot/LATEST` and `snapshot/EARLIEST`, each holding a bare snapshot id.
- `FileStorePathFactory` → `manifest/manifest-<uuid>-<n>` and `manifest/manifest-list-<uuid>-<n>`.

The ids are counters, not epochs, and none of the files carries a `.json` extension — so the
target's `metadata/schema-<epoch>.json` was wrong on the directory, the name and the suffix.

The target now writes that layout, with `Snapshot`'s own `FIELD_*` names in the snapshot JSON, and
carries the sync metadata in the snapshot's `properties` map (Paimon ≥ 1.2), which keeps INV-4 while
leaving schema files immutable the way Paimon treats them. `CommitChanges` writes one snapshot per
change whose base manifest is the previous state, so the newest snapshot always describes the whole
table. The source resolves the newest snapshot through the `LATEST` hint, falling back to a numeric
scan — the old lexicographic sort put `snapshot-10` before `snapshot-9`.

**One deviation remains, and it is the same defect as T31's:** manifests and manifest lists are
JSON, where Paimon writes Avro (`CoreOptions.MANIFEST_FORMAT` defaults to `avro`). The records mirror
`ManifestEntry`, `ManifestFileMeta` and `DataFileMeta` field for field, but a Paimon engine still
cannot open the result. Closing that needs the Avro writer T31 is adding for Iceberg; nothing else
in the layout blocks it.

Verified: `make check` green; `go test -short -race ./pkg/...` clean;
`TestForeignFixtures_ConvertDelta/paimon` flipped from the "expected broken" pin to the same file
list, row count and schema assertions the other targets get; `TestE2E_PaimonDeltaRoundTrip` covers
Delta → Paimon → Delta.

## T34 — Paimon against the real spec: Avro manifests and engine verification

T32 aligned the on-disk layout with Paimon 1.3.1's own writers (evidence in its outcome), but two
gaps separate "matches the layout" from "follows the spec":

1. **Manifests are JSON; Paimon's `CoreOptions.MANIFEST_FORMAT` defaults to Avro.** The records
   mirror `ManifestEntry`/`ManifestFileMeta`/`DataFileMeta` field for field, but a real Paimon
   engine cannot open them. Same shape of work as T31 — once T31 settles the Avro codec choice,
   reuse it here.
2. **No engine-level verification.** No fixture written by real Paimon exists (T28 covers Delta
   and Iceberg only) and no real reader checks polytable's Paimon output. Check whether `pypaimon`
   (the Python SDK) is mature enough to generate a fixture and/or read our output JVM-free; if
   not, both belong in T30's JVM job alongside the Spark/Hudi fixtures.

**Acceptance:** a real-Paimon-written fixture reads and converts; polytable's Paimon output opens
in a real Paimon reader (pypaimon or the T30 job); manifests are Avro where the spec defaults to
Avro. Until then, the format matrix must not claim Paimon interop beyond polytable↔polytable.

## T33 — Parquet source schema: merge footers, include the partition column

T28's F3, two related defects in `pkg/formats/parquet/source.go`:
1. The schema comes from a single file's footer — whichever sorts first — so a schema-evolved
   directory reports whichever generation that happens to be. Merge footers across files (newest
   superset wins; conflicting types are an error, not a guess).
2. The Hive partition column lives only in directory names, so it is reported as a partition field
   but missing from the schema. Engines converting the output see a partition key that no column
   defines. Synthesize the column (type-inferred from the values, STRING on ambiguity) the way
   Java's parquet source does — check `../incubator-xtable` for its behavior first.

**Acceptance:** a fixture directory with two file generations reports the merged schema
deterministically regardless of file naming; the partition column appears in the schema with a
type; the T28 pinning assertions flip to green.

**Commit:** `fix: merge Parquet footers and surface the Hive partition column`

### Outcome ✅ — both fixes are deliberate divergences from Java

**What Java does**, read from `xtable-core`'s `org.apache.xtable.parquet`:

- *Schema:* `ParquetConversionSource.createInternalTableFromFile` takes one footer — the file with
  the greatest modification time (`getMostRecentParquetFile`). No merge, so a column an older
  generation carries and the newest does not is simply gone. Picking the newest file is better than
  picking the first, and it is where "newest wins" below comes from, but it is still one file.
- *Partition column:* Java never infers partitioning from directory names. The spec comes from
  configuration (`xtable.parquet.source.partition_field_spec_config`, `path:type[:format]`) and
  `ParquetPartitionSpecExtractor.spec` resolves each path against the footer schema with
  `SchemaFieldFinder`, so Java assumes the partition column is physically in the files; a
  Hive-partitioned table where it is not yields a partition field with a null source field. There
  is nothing to port, so the synthesis here is new.

**What this port does instead.** `MergeFooterSchemas` folds every footer into one schema: newest
file first, columns only older files carry appended after, and a column absent from any file
nullable, since its rows have no value. Types must be *identical* — an INT and a LONG column of the
same name conflict rather than widening, because a widened schema is a guess about the writer's
intent — and a conflict is an error naming the column, both types and both files. Files sharing a
modification time fall back to path order; only column order can move that way, never the set or the
types. Each file is still read once: `footerAggregates` collects the row-group statistics before the
merge and `columnStatsForSchema` resolves them against the merged schema afterwards, so a column a
file does not carry contributes no statistics for it rather than zeros.

`partitionSpec` then puts the Hive partition columns in that schema, appended after the physical
ones, typed from the observed directory values: LONG when every value is an integer, DOUBLE when
every value is numeric, DATE when every value is an ISO date, STRING otherwise, and STRING for
anything ambiguous, including Hive's `__HIVE_DEFAULT_PARTITION__` null marker. Synthesized columns
are nullable. A physical column of the same name wins — resolved through `FieldByPath`, so
`Region=eu` over a `region` column is one column and not two — and the directory values then have to
be readable as its type, or the crawl fails rather than describing the table wrongly. Partition
values stay the raw directory strings, which is what every target formats today. Partitioning fields
now come out in directory-nesting order rather than a map's iteration order.

`ExtractHivePartitions` is gone, replaced by `HivePartitionsForFile` and `PartitionColumnSchema`;
nothing outside the package used it.

Verified: `make check` green; `go test -short -race ./pkg/...` clean;
`TestForeignFixtures_ConvertDelta/parquet` flipped from pinning the missing partition column to
asserting every column of the delta-rs manifest, `region` included, with its type — the same fixture
also exercises the merge, since `discount` exists only in the third commit's files.
`TestParquet_SourceMergesFooterSchemas` pins the merge under both file namings.

## T35 — The Hudi target drops a data file path that carries a URI scheme

T31's F4. `pkg/formats/hudi/target.go` computes each file's path relative to the table with
`strings.TrimPrefix(df.PhysicalPath, t.targetTable.BasePath)`. An Iceberg source reports the
location its manifest recorded, scheme included, so the prefix never matches, the absolute path is
stored as if it were relative, and `pkg/formats/hudi/source.go` joins it onto the base path again:
`…/events/file:/…/events/data/…`. Nothing caught it before a foreign Iceberg table could be a
conversion source. **Paimon is confirmed to share the exposure**: after T31 and T32 composed, an
Iceberg→Paimon conversion reads back doubled the same way, pinned by `pathsDoubled` on the Paimon
row of `TestForeignFixtures_ConvertIceberg` (Delta→Paimon is clean — the exposure needs a
scheme-qualified source path). Decide whether the fix belongs in each adapter or in `pkg/io` —
every target that trims a base path by string prefix has the same exposure, so check Delta too
before choosing.

**Acceptance:** Iceberg→Hudi and Iceberg→Paimon conversions report each data file once, under the
table's base path; both `pathsDoubled` pins in `TestForeignFixtures_ConvertIceberg` are deleted
rather than adjusted.

**Commit:** `fix: relativize data file paths against the base path scheme-aware`

### Outcome ✅ — one helper in `pkg/io`, three targets

`io.RelativizePath(physicalPath, basePath) (string, error)` in `pkg/io/storage.go` replaces the
trim-by-prefix in all three targets that carried it. It strips a recognized scheme from *either*
side before comparing — the list is now the package-level `uriSchemes` that `JoinPath` and
`NewStorageForPathWithOptions` already keyed off, so `s3://` and `s3a://` compare equal, which is
right: they name the same store. Both sides go through `path.Clean`, so a base path written with or
without a trailing slash behaves the same, and the match has to end on a separator, so `/data/events`
does not claim `/data/events2/f.parquet`. A path already relative (no scheme, no leading separator)
comes back unchanged — `model.DataFile` documents `PhysicalPath` as "fully qualified URI or relative
path" — unless it climbs out with `..`.

**Which targets carried the pattern.** Hudi (`target.go`, the reported defect), Paimon
(`manifest.go`'s `relativeFilePath`, now gone) and Delta (`target.go`'s `makeRelativePath`). The
Parquet target does not: it only joins, under `_metadata/`. `parquet/partition.go`'s
`HivePartitionsForFile` trims by prefix but is source-side, where the file path and the base path
come from the same crawl and cannot disagree on scheme, so it was left alone.

**A file outside the base path is handled per format, which is the asymmetry worth recording.** Hudi
and Paimon cannot represent one — a write stat's path and a manifest entry's `_FILE_NAME` are both
joined back onto the base path on read — so they fail the commit. Delta can: the protocol allows an
absolute URI in an add action, which is why `delta/source.go`'s `resolveDataPath` accepts one, so the
Delta target keeps the absolute path deliberately. Paimon's `applyDiff` keys files by the same
relative name but falls back to the physical path, since that key is identity only and a file that
cannot be relativized still fails in `entryForDataFile`.

`JoinPath` was fixed in passing: it dropped the leading separator after a scheme, turning
`file:///data/events` into the relative `file://data/events`, so the round trip the targets depend on
did not close for a `file://` base path.

Verified: `make check` green; `go test -short -race ./pkg/...` clean; `go test -short -count=1
./test/` green with both `pathsDoubled` pins in `TestForeignFixtures_ConvertIceberg` deleted — the
field and its handling in `assertFileListMatchesManifest` had no users left and went with them, so
Iceberg→Hudi and Iceberg→Paimon now assert the plain file list every other target asserts.

---

## Ordering

**Current status**

| | Tasks |
|---|---|
| ✅ Done | T1, T3 (via T12), T4, T5, T6, T9, T11, T12, T15 (via moto), T16, T18, T20, T21, T22, T23, T25, T26, T27, T28, T29, T31, T32, T33, T35, T36, T40, T45, T53, T54, T55, T56, T58, T59, T60, T62, T66 |
| ⚠️ Superseded | T2 → T16 · T8 → T18 |
| ✅ Proven | T7, T10 → T17 — release workflow verified end to end by a throwaway tag |
| 🧩 Landed under another number | T14 → T23 (`ListTables`, `DiscoverDatasets`) · T15 → `catalog.SyncPartitions` with `pkg/catalog/glue_partition.go`, wired at `pkg/conversion/controller.go:158`. Both are covered by tests against fakes, and **neither has been checked against a real Glue catalog** — which is what T15 asked for — so they are recorded here rather than as ✅ |
| 📋 Unscheduled | T13 (HMS) — the roadmap's answer is to keep the explicit not-implemented refusal until a consumer with a concrete deployment appears |
| 🎯 Open queue | T24, T30, T34, T37, T38, T39, T41–T44, T46–T50 from the roadmap, and T57, T61, T65 (v3 support), T67 |
| ⚠️ Landed, unverified against the real service | T51 (Azure storage — green on the Azurite emulator, never run against Azure) · T52 (Entra ID auth — no Fabric workspace reached). Both name their unmet criteria in the task |

**Picking up the queue.** T51 and T52 need an Azure subscription, not more work: the emulator lane
is green, and `docs/azure-test-environment.md` is the recipe for the rest. Otherwise by value: **T40** first — the Iceberg
source has no incremental sync at all, which is a correctness defect rather than a gap — then T45
(the Parquet source crawls other formats' metadata as data), then T37's 1.x timeline, then T34.

Three tasks are decisions before they are code, and each says so in its own text: T24 (read
`SPEC.md:390` first, and do not start an Iceberg deletion-vector translator until the INV-1 question
is answered), T42 (whether a rollback is expressible across formats at all) and T49 (where a
per-target catalog identifier lives).

**Standing chore, not a task.** Re-sweep upstream at each of their cutoffs — 0.5.0 on 2026-09-30,
0.6.0 on 2026-11-15 — and refresh `docs/upstream-watch.md`.

Gate at review time: `make check` green, `go test -short -race ./pkg/...` clean, working tree clean.
T17 proved the release workflow, so tagging is no longer blocked.

```
T40 ─────> T46-deletes  (a real incremental reader needs a fixture that removes files)
T45 ─────> T48          (auto-detection must not point a Parquet source at a Delta table)
T44 ─────> T40          (pin path canonicalization before a second reader can diverge from it)
T31/T34 ─ share the Avro codec; T34 reuses it for Paimon manifests
T51 ─────> T52          (a OneLake catalog cannot read a table polytable cannot open)
T13 ────── unscheduled by decision, not by omission
```

## T36 — Delta source: read Parquet checkpoints ✅

Found auditing against upstream #902's engine baseline. The Delta source replayed JSON commits
only: a table whose pre-checkpoint commits were expired by log retention (the default within 30
days of the first checkpoint) had no readable `metaData` and failed outright — and a truncated log
without a checkpoint would silently build a partial snapshot from the surviving tail.

Landed: `pkg/formats/delta/checkpoint.go` reads `_last_checkpoint` and the classic single- or
multi-part checkpoint Parquet (parquet-go), and `loadLogState` seeds every read path (table,
snapshot, incremental) from checkpoint state plus the JSON tail strictly after it. A truncated log
with no checkpoint is now a hard error. v2 checkpoints (sidecars, `v2Checkpoint` reader feature)
are rejected with a clear message — implementing them is a follow-up, and matters more as Delta
4.0 makes them the default.

**Verified:** `test/testdata/fixtures/delta-rs-checkpoint/` is written by delta-rs, whose
`cleanup_metadata()` really deleted the version 0–1 commits (generator asserts it);
`test/delta_checkpoint_fixture_test.go` covers snapshot-from-checkpoint, incremental-safety
refusal across the cleaned history, the truncated-log error, and a full conversion to Iceberg.
`pkg/formats/delta/checkpoint_test.go` pins the v2 rejection and the multi-part merge.

## T37 — Hudi 1.x: confirm and close the timeline-layout gap

Upstream builds against Hudi 1.2 (#902); polytable's target stamps `hoodie.table.version=6`
(0.14-era) and `timeline.go` parses 0.x instants. **Confirmed with a real fixture** —
`test/testdata/fixtures/hudi-1.x/trips`, written by Hudi 1.2.0 through Spark 3.5
(`hoodie.table.version=9`, timeline under `.hoodie/timeline/`): the failure mode was worse than
unreadable. The reader returned an **empty table silently** — zero files, empty schema, exit 0 —
so a sync would have succeeded with empty target metadata. Landed as the first step: a version
guard (`TableProperties.AssertReadableVersion`, floor `hoodie.table.version=6`) makes every read
path refuse loudly, and the Hudi target no longer swallows ReadProperties errors — an unreadable
properties file stops the sync instead of being overwritten as if the target were fresh.
`test/hudi_1x_fixture_test.go` pins the refusal and flips red when someone implements 1.x reading
— replace it with manifest assertions then. Remaining: implement the 1.x timeline
(completion-time instant names, `.hoodie/timeline/` plus `history/`, the metadata-table
directory needs excluding from file listing) or keep the documented 0.x floor. Writing version 6
remains correct — 1.x readers consume it — so the write side is not part of the gap.

Related upstream context noted while here: #810 asks for a OneLake/Fabric catalog client. Its
read API is Iceberg-REST-compatible, so `pkg/catalog/rest.go` is the eventual entry point, but
any OneLake work is blocked on an Azure/`abfss://` storage backend either way (see Non-goals).

---

## Roadmap queue — T38–T52

`docs/roadmap.md` sets direction; these are its items turned into work, with the evidence read out
of the tree on 2026-08-22. Three roadmap bullets became no task: the doc-version guard is already
CLAUDE.md policy, the upstream re-sweep at each cutoff is a standing chore recorded under Ordering,
and the "watching, not building" list is explicitly not work. T13 keeps its unscheduled status
because the roadmap's answer for HMS is to keep the explicit refusal until a consumer with a
concrete deployment appears.

## T38 — Delta v2 checkpoints: sidecars and the `v2Checkpoint` reader feature

T36 landed classic checkpoints and drew the line explicitly: `pkg/formats/delta/checkpoint.go:157`
rejects a checkpoint whose protocol lists the `v2Checkpoint` reader feature, with
`"delta checkpoint %s declares the v2Checkpoint reader feature, which is not supported yet"`
(`:158`). Delta 4.0 makes v2 the default, and upstream's 0.6.0 engine baseline (#902) moves to
Spark 4 with Delta 4.0 — so this rejection turns from a documented limit into the common case
inside one upstream release.

What v2 adds over what `checkpoint.go` already does: the checkpoint file is a *manifest* holding a
`checkpointMetadata` action and `sidecar` actions, and the `add`/`remove` actions live in separate
sidecar Parquet files under `_delta_log/_sidecars/`. `readLastCheckpoint` (`:107`) and
`checkpointPartFiles` (`:127`) already handle the `_last_checkpoint` pointer and the classic
single- and multi-part naming; the v2 work is a third shape, not a rewrite.

**Fixture first, per ground rule 10.** No fixture in the tree has v2 checkpoints —
`test/testdata/fixtures/delta-rs-checkpoint/` is a classic single-file v1 checkpoint
(`_last_checkpoint` is `{"version":2,"size":7,…}` with no `v2Checkpoint`, `sidecar` or
`checkpointMetadata` key anywhere; its manifest's `checkpoint_version: 2` means table version 2,
not checkpoint format v2). Generate one with a delta-rs or Spark writer that enables the feature,
alongside the existing generator at `test/fixtures/generate.py`.

**Acceptance:** a real-engine-written table with v2 checkpoints and expired pre-checkpoint commits
loads its schema and full file list; `pkg/formats/delta/checkpoint_test.go`'s v2-rejection pin is
replaced by a positive assertion rather than deleted; a sidecar referenced but missing is a hard
error, matching T36's treatment of a truncated log.

**Commit:** `feat: read Delta v2 checkpoints and their sidecar files`

---

## T39 — Iceberg metadata resolution: unparseable names vanish, the catalog pointer is discarded

Upstream's most user-visible bug family (#431, #287, #504, #354): path loading assumes the Hadoop
`version-hint.text` convention, catalog-writing engines never create it, and Snowflake names
metadata `v<nanoseconds>.metadata.json`, which overflows an int32 version parse.

**Half of this the port already gets right, and that is worth recording before anyone "fixes" it.**
`listMetadataFiles` (`pkg/formats/iceberg/source.go:86`) *is* the resolution mechanism — it lists
`metadata/` and takes the highest parsed version (`:130`, `:216`). `version-hint.text` is write-only
here (`pkg/formats/iceberg/target.go:325`); no read path consults it. `MetadataFileVersion`
(`:63`) returns a plain `int`, 64-bit on every platform this builds for, so a Snowflake epoch-nanos
token parses. Do not introduce a version-hint fast path: that is the upstream bug.

**What is actually wrong:**

1. **A filename that does not parse is silently skipped** — `source.go:96`, `if !ok { continue }`.
   A metadata directory whose newest file uses an unrecognized convention resolves to an *older*
   table state with no warning: silent staleness, the worst failure mode available. It must warn,
   or fail when nothing parses for a reason other than "not a metadata file".
2. **Same-version collisions are broken by lexical name order** (`:100`), which is a guess.
3. **The catalog's metadata pointer is thrown away.** `rest_conversion.go:47` decodes
   `metadata-location`, but `:129` uses it only when `metadata.location` is blank, and only to
   derive a base path by trimming `/metadata/<file>` (`baseFromMetadataLocation`, `:170`). It is
   not stored on `SourceTable` (`:149`) nor copied into `Properties` (`:143`), and
   `Controller.createSource` (`pkg/conversion/controller.go:257`) constructs the Iceberg source
   from `(storage, basePath)` alone (`pkg/formats/iceberg/source.go:43`). So a catalog-managed
   table is re-resolved by listing even when the catalog said exactly which file is current —
   upstream #504's failure, reachable here.
4. **Column stats are not a panic risk, but one field is dropped.** `avroKVInt64`
   (`pkg/formats/iceberg/manifest.go:710`) returns nil for absent maps and every consumer in
   `stats.go:222` reads through a possibly-nil map, which is legal Go — the #641/#667 NPE class
   does not exist here, and the task should say so rather than re-checking it forever.
   `column_sizes` is parsed (`manifest.go:552`) and written (`:443`) but never reaches the model:
   `model.ColumnStat` (`pkg/model/stats.go:45`) has no size field.

**Acceptance:** the source takes a metadata pointer when the catalog supplies one and lists only
when it does not; an unparseable newest file produces a diagnostic, never silent staleness; a
fixture with a Snowflake-style `v<epoch-nanos>.metadata.json` name resolves to that file. Whether
`column_sizes` earns a model field is a decision inside this task, not an assumption.

**Commit:** `fix: resolve Iceberg metadata from the catalog pointer and diagnose unparseable names`

---

## T40 — The Iceberg source has no incremental sync: every change is a full re-add ✅

The sharpest correctness defect found while scheduling the roadmap, and larger than the
expired-snapshot fallback (#147) the roadmap asked for.

- `GetTableChangeForCommit` (`pkg/formats/iceberg/source.go:298`) ignores `commitID` for content.
  It re-reads the current snapshot and returns `model.NewFilesDiff(snap.DataFiles, nil)` (`:304`):
  every live file as an add, `FilesRemoved` always nil. The Iceberg source cannot report a deletion.
- `GetChangesSince` (`:312`) never walks the snapshot history. It calls `GetCurrentSnapshot` and, if
  `snap.Table.LatestCommitTime > fromInstant` (`:323`), emits exactly one change covering the whole
  table. Ten source commits become one.
- The history is not even modeled: `TableMetadata` (`pkg/formats/iceberg/metadata.go:75`) has no
  `snapshot-log`, and `TableSnapshot.ParentSnapshotID` (`:66`) is never traversed on the read path.
- Consequently an expired starting snapshot is *invisible* rather than detected — there is no lookup
  to fail, so nothing triggers a fallback. The only safety check,
  `IsIncrementalSyncSafeFrom` (`:338`), compares the oldest surviving **metadata file**'s
  `LastUpdatedMs`, which is metadata-file retention, not snapshot retention: expiring snapshots
  while keeping metadata files reports safe and syncs wrong.
- `Controller` (`pkg/conversion/controller.go:212`) swallows the error from that call —
  `if err == nil && isSafe` — so "unsafe" and "errored" both fall silently through to a full
  snapshot sync. That silence is correct in outcome and wrong in reporting; T22's per-table verdict
  should say a fallback happened.

**Scope:** model `snapshots` and `snapshot-log`; walk parent links from the requested snapshot to
the current one; per commit, diff the manifests of that snapshot against its parent so removals are
real; make `IsIncrementalSyncSafeFrom` test snapshot retention; surface the fallback in the sync
result instead of swallowing it.

**Acceptance:** a pyiceberg fixture with an append, an overwrite and a delete syncs incrementally
to Delta with one target commit per source snapshot, and the overwrite's removals appear as
removals; expiring the starting snapshot makes `IsIncrementalSyncSafeFrom` report unsafe and the
sync report a snapshot fallback rather than staying quiet.

**Commit:** `fix: walk the Iceberg snapshot history during incremental sync`

### Outcome ✅ — parent-link walk, full-set diffing, and a surfaced fallback

`TableMetadata` (`pkg/formats/iceberg/metadata.go`) gained `SnapshotLog` (`snapshot-log`, matched
against `test/testdata/fixtures/pyiceberg`'s real shape). `GetChangesSince` now walks
`ParentSnapshotID` backward from the current snapshot to the oldest one `Snapshots` still retains,
replays oldest-first, and per snapshot diffs its full live file set (read via the same manifest-list
walk `GetCurrentSnapshot` already did, now factored into `dataFilesForSnapshot`) against its
parent's with `model.DiffFiles` — the task's directed differ, and its first non-test caller.
`GetTableChangeForCommit(commitID)` now looks `commitID` up as a snapshot id and diffs it against
its own parent, instead of ignoring the argument and re-diffing the current snapshot against
nothing. `IsIncrementalSyncSafeFrom` now tests the latest metadata file's `Snapshots` retention
(oldest retained `TimestampMs <= earliestInstant`) rather than the oldest metadata *file*'s
`LastUpdatedMs`. A parent id absent from `Snapshots` before the walk has gone far enough back to
cover `fromInstant` is a hard error (`GetChangesSince`) rather than a silently partial backlog; a
resume point whose own snapshot is gone is what `IsIncrementalSyncSafeFrom` now catches before that
walk is ever attempted. `Controller.syncToTarget` (`pkg/conversion/controller.go`) records *why* it
fell back and attaches it to whichever result the full-sync path returns; `spi.SyncResult`
(`pkg/spi/sync.go`) gained `FellBackToFullSync`/`FallbackReason` to carry it — surfaced in the
daemon's JSON (it serializes `spi.SyncResult` directly) but not the CLI's (`cmd/polytable/output.go`
was out of this task's scope; a follow-up should add the two fields to `TargetSyncOutput`).

**Fixture:** `test/testdata/fixtures/pyiceberg-deletes`, generated by
`generate_iceberg_deletes` in `test/fixtures/generate.py`. pyiceberg 0.11.1's `table.overwrite()`
turned out to commit as two snapshots (a pure delete of the superseded file, then a pure append of
the replacement) rather than one; `table.delete()` on a predicate not aligned to a whole file
committed as a single remove+add rewrite. The generator asserts `total-delete-files == "0"`
throughout so the fixture stays copy-on-write — a positional- or equality-delete file would not
show up as a removed data file under `model.DiffFiles`'s current scope, silently defeating the
fixture's purpose. Per-snapshot ground truth (`iceberg_snapshots` in `manifest.json`) comes from
diffing consecutive `table.inspect.files(snapshot_id=...)` results in Python, independent of the Go
reader. `test/foreign_fixtures_test.go`'s `TestForeignFixtures_IcebergDeletes` checks the Iceberg
source against it directly and then replays the same backlog into a `delta.Target` via
`CommitChanges`, checking one Delta commit lands per Iceberg snapshot and the rewrite's removal
survives the conversion. `pkg/formats/iceberg/incremental_test.go` and `pkg/conversion/t40_test.go`
cover the synthetic cases a real fixture can't pin down on demand: an expired parent mid-chain
(error, not a partial backlog), snapshot-retention safety flipping on expiry, and the controller's
`FellBackToFullSync` firing with a non-empty reason.

**Cost, not quietly absorbed.** Diffing full sets, as directed, means an incremental sync now reads
each pending snapshot's manifest list and manifests *and* its parent's — O(pending snapshots ×
manifests-per-snapshot) reads, where the previous (wrong) code did one read of the current
snapshot only. Manifests two adjacent snapshots share unchanged are read twice, once as the child's
set and once as the parent's. A full snapshot sync is unaffected. The cheaper alternative — filter
each snapshot's own manifest list to the manifests `ManifestListEntry.AddedSnapshotID` attributes to
it, then classify entries by `Status` — reads only the handful of manifests each commit actually
wrote, but the task named `model.DiffFiles` as the differ to use, so that path was not taken.

---

## T41 — Iceberg partition specs are resolved table-current, and every transform is flattened

Upstream #126: partition specs must be resolved per-manifest by `spec-id`, not from the table's
current spec. Confirmed here, with a second defect on top.

- The manifest's own spec id is decoded (`pkg/formats/iceberg/manifest.go:511` into
  `ManifestListEntry.PartitionSpecID`, `metadata.go:130`) and read only on the write path
  (`target.go:233`, `manifest.go:464`). The manifest Avro header carries `partition-spec` and
  `partition-spec-id` on write (`manifest.go:404`), but `readAvroContainer` (`:594`) returns records
  and discards header metadata, so the spec cannot be recovered on read even if someone wanted it.
- The source always builds `PartitioningFields` from `meta.DefaultSpecID`
  (`pkg/formats/iceberg/source.go:185`), then matches partition values **by name** against those
  current fields (`:367`, `if val, ok := mdf.Partition[pf.SourceField.Name]; ok`) with no else
  branch. A manifest written under an older spec whose field names differ has its partition values
  silently dropped — the same name-not-id trap as #711.
- **Every transform is flattened to identity on read**: `source.go:198` hardcodes
  `TransformType: model.PartitionTransformValue` regardless of the spec's `Transform` string
  (`metadata.go:44`). A table partitioned by `days(ts)` or `bucket(16, id)` is reported as
  partitioned by the raw column. The write path hardcodes `SpecID: 0` and `DefaultSpecID: 0`
  (`target.go:172`, `:306`).

**Acceptance:** a pyiceberg fixture that evolves its partition spec mid-history converts with each
manifest's values resolved under the spec that wrote it; a table partitioned by a non-identity
transform reports that transform, or is refused with a message naming it — not silently reported as
identity.

**Commit:** `fix: resolve Iceberg partition values under the manifest's own spec`

---

## T42 — Rollbacks and restores degrade into bare file removals

Upstream #40: a source rollback or restore reaches the target as plain removes and adds, so target
history diverges from source history. Confirmed absent here — a repo-wide search for
`rollback|restore|savepoint` across non-test files under `pkg/` returns nothing.

The information is parsed and then dropped:

- `pkg/formats/delta/actions.go:89` has `CommitInfoAction.Operation`, but
  `pkg/formats/delta/source.go:165` reads only `Timestamp` from it, and `changeFromCommit` (`:379`)
  builds the change purely from `Add` and `Remove` actions. A Delta `RESTORE` is indistinguishable
  from an overwrite.
- `pkg/formats/iceberg/metadata.go:53` has `SnapshotSummary.Operation` — commented
  "append, replace, overwrite, delete" — and no code path reads it.
- `pkg/model` has nowhere to put it. `TableChange` (`pkg/model/changes.go:21`) is
  `FilesDiff`/`TableAsOfChange`/`SourceIdentifier`/`CommitTime`; `FilesDiff` (`pkg/model/diff.go:23`)
  is adds and removes. Every target hardcodes an operation string instead:
  `paimon/target.go:113` and `:158` (OVERWRITE for snapshots, APPEND for every change whatever it
  contains), `delta/target.go:158`/`:228`, `hudi/target.go:142`, and `iceberg/target.go:279`, which
  writes `"operation": "replace"` even on the incremental path.

**Decide before writing code, as with T24.** Either `model.TableChange` gains an operation kind and
targets stamp it, or the honest answer is that a rollback is not expressible across formats and the
outcome is a documented divergence plus the #657-style warning. Upstream has not resolved this
either, so there is no reference implementation to copy.

**Acceptance:** a Delta table rolled back with `RESTORE` reaches an Iceberg target with a snapshot
summary that says what happened, or the sync warns that it cannot express the rollback and `SPEC.md`
records the limit. The fixture is written by a real engine (delta-rs `restore`), per ground rule 10.

**Commit:** `feat: carry the source operation kind through TableChange`

---

## T43 — Target commits are last-writer-wins: no optimistic concurrency anywhere

Upstream #124 reports the Delta target committing without optimistic-concurrency handling. All five
targets here have the same hole, and `pkg/io` has no primitive to close it with.

Each target picks a version by listing, then writes unconditionally:

| Target | Version choice | Write |
| :--- | :--- | :--- |
| Delta | `listCommitFiles()` last `+ 1` (`target.go:102`); **the list error is discarded**, so a failed listing silently commits version 0 | `%020d.json` (`:256`) |
| Iceberg | `listMetadataFiles()` last `+ 1` (`:104`), list error discarded | `v%d.metadata.json` (`:315`) plus `metadata/version-hint.text` (`:326`) |
| Hudi | no listing — the instant is `time.Now()` at millisecond resolution (`target.go:94`–`:95`, `timeline.go:32`) | `.hoodie/<instant>.commit` (`:150`) |
| Paimon | `nextSnapshotID` lists `snapshot-*` and adds 1 (`target.go:389`) | `snapshot/snapshot-<id>` (`:264`), `LATEST` rewritten each commit (`:276`) |
| Parquet | no versioning at all | fixed `_polytable_metadata/manifest.json` (`:112`) |

`pkg/io`'s `Storage` interface (`pkg/io/storage.go:54`) is `Read/Write/List/Exists/Delete/Close` —
no put-if-absent, no CAS, no conditional headers. `pkg/io/local.go:85` is `os.Rename`, which
clobbers; `pkg/io/s3.go:114` is a plain `PutObject` with no `IfNoneMatch`. `ErrAlreadyExists`
(`pkg/io/storage.go:33`) is declared and never returned or checked anywhere in the module. No target
calls `Exists` before writing, and `iceberg/target.go:242` says so outright: "polytable never
retries a commit, so it is always zero."

Two writers — polytable racing itself, or racing a foreign writer between the snapshot read and the
commit — therefore overwrite one another's version file. Hudi is worst: two commits inside the same
millisecond collide on the instant name.

**Scope, in order.** (1) Give `Storage` a conditional write — `WriteIfAbsent` returning
`ErrAlreadyExists`, `O_EXCL` locally and `If-None-Match: *` on S3, so the declared error finally has
a producer. (2) Make each target's commit retry: re-read the log, recompute the version, re-apply,
bounded attempts. (3) Stop discarding the listing error in the Delta and Iceberg version choice — a
failed listing must abort the commit, never restart numbering at zero.

**Acceptance:** a test drives two concurrent commits at one table per format and asserts both land
at distinct versions with no lost update; a `Storage` stub whose `List` fails makes the commit fail
instead of writing version 0; the S3 conditional write is exercised against MinIO in the existing
dockertest matrix.

**Commit:** `fix: commit target metadata conditionally and retry on conflict`

---

## T44 — One property test: snapshot and incremental must agree on every path

Upstream #586: on a 7M-file table, alternate snapshots emitted spurious add/remove pairs because
state diffs key on data-file paths and two code paths canonicalized them differently.

**This is preventive here, and the survey says why in a way that matters.** For Iceberg, Hudi,
Paimon and Parquet the two paths cannot disagree because the incremental entry point *is* the
snapshot one: `iceberg/source.go:299` and `:317`, `hudi/source.go:297` and `:311`,
`paimon/source.go:330` and `:344`, `parquet/source.go:248` and `:262` all call
`GetCurrentSnapshot`. The identity holds vacuously, not by construction — nothing structural would
hold a real incremental reader (T40's, for one) to the snapshot's string form. Delta, the only
format with an independent incremental reader, does agree: snapshot (`delta/source.go:326`, `:337`)
and incremental (`:385`, `:389`) both go through `resolveDataPath` (`:554`).

So the deliverable is the test, plus two real inconsistencies it should be written against:

1. `resolveDataPath`'s pass-through scheme list (`delta/source.go:555`) is `s3://`, `gs://`,
   `mem://`, `file://` and a leading `/` — it omits `s3a://`, which `io.uriSchemes`
   (`pkg/io/storage.go:43`) does recognize and which `io.RelativizePath` treats as equal to `s3://`.
   An `s3a://` add path is therefore joined onto the base path instead of passed through. Use
   `io.TrimScheme`/`uriSchemes` rather than a second private list.
2. Delta-protocol paths are percent-encoded, and nothing under `pkg/formats` ever unescapes them —
   no `url.PathUnescape` call exists. A file with a space or `#` in its name has one spelling in the
   log and another on disk.

Worth recording while here: `model.DiffFiles` (`pkg/model/diff.go:46`) has **no non-test caller**.
Every source builds `model.NewFilesDiff` from already-separated sets, so the path-keyed diff that
upstream's bug lives in is currently exercised only by `pkg/model/model_test.go`. That makes the
property test the only thing standing between this repo and #586.

**Acceptance:** for each format, one table read both ways yields byte-identical `PhysicalPath`
strings for the same file, asserted as a property over a generated table rather than a fixed list;
the `s3a://` and percent-encoded cases are in it and pass.

**Commit:** `test: pin snapshot and incremental path canonicalization`

---

## T45 — The Parquet source crawls other formats' metadata as data ✅

Upstream #813 and #814: Hudi partition discovery treated `_delta_log/` — which holds checkpoint
Parquet — as a Hudi partition, so synced tables self-corrupted on round trip. polytable is more
exposed than upstream, not less: a polytable-synced directory always holds every target's metadata
side by side, by design.

`pkg/formats/parquet/source.go:185` is the entire filter:
`!f.IsDir && strings.HasSuffix(f.Path, ".parquet") && !strings.HasPrefix(filepath.Base(f.Path), ".")`.
Only the file's own base name is consulted; directory names are never inspected, and a leading `_`
is not excluded at all. The listing above it (`:178`) is `s.storage.List(ctx, s.basePath)` with no
pruning, and `pkg/io`'s three backends return everything — `local.go:112` walks recursively with no
filter, `memory.go:76` and `s3.go:132` are raw prefix matches.

So `<base>/_delta_log/00000000000000000010.checkpoint.parquet` satisfies the predicate — `.parquet`
suffix, base name without a leading dot — and is admitted as a data file. `metadata/` (Iceberg
statistics files), `_metadata/`, Paimon's directories and `_temporary/` are the same. The partition
parser does not save it either: `pkg/formats/parquet/partition.go:41` silently skips segments
without `=`, so `_delta_log` contributes no partition and the file is still counted as data.

**Hudi is clean, for a structural reason worth recording** so nobody "fixes" it: Hudi data files
come from `meta.PartitionToWriteStats` in the commit JSON (`pkg/formats/hudi/source.go:256`), not
from a listing. Its only listing is the timeline (`:73`), already filtered to `.commit`,
`.deltacommit` and `.replacecommit`. Delta's log listing (`delta/source.go:65`) filters to
`<digits>.json`.

**Scope:** one exclusion helper in `pkg/io` — the format metadata directory names plus the
`_`/`.`-prefixed convention — since the next directory-crawling source will need it, and a single
list is the only way the set stays in sync with the format registry.

**Acceptance:** a Parquet source pointed at a directory that also holds `_delta_log/`, `metadata/`,
`.hoodie/`, `_polytable_metadata/` and `_temporary/` reports only the real data files; the test
builds that directory by running actual syncs into it, not by hand-placing files.

**Commit:** `fix: exclude other formats' metadata directories from Parquet file discovery`

### Outcome ✅

One helper, `io.IsMetadataPath`/`io.IsMetadataPathComponent` in `pkg/io/metadata.go`, checked
component-wise against the path relativized to the source's base path (`io.RelativizePath`), not
against the full absolute path or the file's own base name. Two rules, combined:

1. Any component starting with `_` or `.` is excluded outright — the Hive/Spark hidden-file
   convention, which covers Delta's `_delta_log`, the Parquet target's own
   `_polytable_metadata`, Hadoop's `_temporary` and `_SUCCESS`, and Hudi's `.hoodie` in one rule.
   Decision, recorded in the doc comment: this also excludes a hypothetical raw (non
   `key=value`) partition segment starting with `_` or `.`, which is accepted because Hive-style
   partitioning never uses such a segment as a value in the first place, and because a curated
   name list alone would silently admit every future format's underscore- or dot-prefixed
   metadata as data until someone remembered to add it.
2. An exact-name list for the metadata directories that do *not* follow that convention:
   Iceberg's `metadata`, and Paimon's `schema`, `snapshot` and `manifest` (read from
   `pkg/formats/paimon/manifest.go`, not guessed). Checked against a single component only, so
   `region=metadata` (a valid Hive partition segment) is untouched.

`pkg/formats/parquet/source.go`'s `listDataFiles` now relativizes every listed path and skips it
when `io.IsMetadataPath` matches, instead of a base-name suffix/prefix check. `io.Storage.List`
itself is untouched, per the task's explicit constraint — the exclusion lives in the source, not
the listing contract.

The acceptance test (`test/parquet_metadata_exclusion_test.go`) is built the way the task asked:
`delta-rs-checkpoint` — a genuine deltalake-written fixture whose `_delta_log` already carries a
real `.checkpoint.parquet`, the exact upstream #813/#814 shape — is run through
`conversion.Controller.Sync` into Iceberg, Hudi, Paimon and Parquet targets, all sharing the same
directory the Delta fixture lives in. That produced `_delta_log`, `metadata`, `.hoodie`, `schema`,
`snapshot`, `manifest` and `_polytable_metadata` as a real, sync-populated layout, not a hand-built
one. Only Hadoop's `_temporary`/`_SUCCESS` had to be added by hand, because no writer in this
repository produces the Hadoop staging convention — documented in the test as the one deliberate
exception. A fresh `parquet.Source` pointed at that directory reports exactly the fixture's 6 real
data files (`assertFileListMatchesManifest`) and still recovers the genuine `region=` Hive
partition. `pkg/io/metadata_test.go` covers the helper directly, including the component-wise case
(`data/_delta_log/x.parquet` excluded despite an unremarkable base name).

Reverting the fix locally and re-running the suite confirmed it is load-bearing: without it,
`GetCurrentSnapshot` picked up the checkpoint Parquet file as a 7th "data file" and then panicked
trying to parse its schema as table data (`parquet-go`'s `groupType.Kind` on a nested group it
does not expect) — the exact self-corruption class #813/#814 describes, reproduced with a real
foreign fixture rather than a synthetic one.

---

## T46 — Widen the fixture matrix beyond insert-only

The coverage bar recorded under T30 is upstream's `ITConversionController` scenario list. The
fixture inventory says how far short the tree is: four fixtures, and between them they cover
appends, Hive and identity partitioning, one added column each, and one classic Delta checkpoint.

| Fixture | Writer | What it covers |
| :--- | :--- | :--- |
| `test/testdata/fixtures/delta-rs/sales` | deltalake 1.6.2 | 3 appends, partitioned by `region`, adds `discount` mid-history. Zero `remove` actions. |
| `test/testdata/fixtures/delta-rs-checkpoint/orders` | deltalake 1.6.3 | classic v1 single-file checkpoint with v0–v1 commits deleted by `cleanup_metadata()` |
| `test/testdata/fixtures/pyiceberg/events` | pyiceberg 0.11.1 | format-version 2, identity partition, 3 appends, adds `label` mid-history, real Avro manifests |
| `test/testdata/fixtures/hudi-1.x/trips` | hudi-spark3.5-bundle 1.2.0, PySpark 3.5, JDK 17 | contains a genuine upsert — but polytable refuses to read it (T37), so the only assertion is the refusal |

Missing outright: **deletes** (no `remove` action anywhere; no Iceberg positional or equality
deletes), **compaction or replace** (no `REPLACE`/`OPTIMIZE` commit, no Iceberg `replace` snapshot,
no Hudi compaction instant), **column rename under Delta column mapping**
(`grep -rl columnMapping test/testdata/fixtures/` is empty — only column *addition* is covered, and
#711's field-id trap needs a rename), **time travel** (every fixture test reads
`GetCurrentSnapshot`/`GetCurrentTable`), and **a readable upsert** (the one that exists is behind
T37's refusal).

**Version diversity is part of this task, not a separate one.** `test/fixtures/generate.py` pins
nothing — its docstring says `pip install deltalake pyarrow 'pyiceberg[sql-sqlite]'` and the
versions are recorded post-hoc in each `manifest.json`'s `writer` field, which is how the two Delta
fixtures ended up on 1.6.2 and 1.6.3. `generate_hudi_1x.py` does pin
(`pyspark==3.5.*`, `hudi-spark3.5-bundle_2.12:1.2.0`). Pin the generator, and add a deliberately old
delta-rs protocol version alongside the current one. Adding fixtures freely is authorized; the cost
is repository size, so keep each table small.

**Acceptance:** every scenario above has a fixture written by a real engine and a test that reads
it; the generator pins its writer versions and the pins match what the manifests record; no scenario
is claimed in `docs/testing.md` that has no fixture behind it.

**Delta half landed, Iceberg half still open.** `test/testdata/fixtures/delta-rs-deletes/returns`
(`DeltaTable.delete()`: a whole-partition delete and a single-row delete that forces a rewrite) and
`delta-rs-compaction/clicks` (`optimize.compact()`, and the tree's first unpartitioned Delta fixture)
close the deletes and compaction rows of the table above on the Delta side. `generate.py`'s
docstring now pins the install line (`deltalake==1.6.3 pyarrow==25.0.1
'pyiceberg[sql-sqlite]==0.11.1'`) rather than leaving it bare, matching what the manifests already
recorded; `delta-rs/sales` keeps its 1.6.2 writer on purpose, as a deliberately old version.
`test/foreign_fixtures_test.go`'s `TestForeignFixtures_DeltaDeletes` and
`TestForeignFixtures_DeltaCompaction` assert the per-commit `FilesRemoved`/`FilesAdded` against a
`commits` array the generator reads straight out of `_delta_log`, not merely that it is non-empty —
both pass, so `GetChangesSince` and `changeFromCommit` (`pkg/formats/delta/source.go`) already
handle `remove` actions correctly; this had never been exercised by a fixture before. **Column
rename under column mapping is unreachable, not merely undone**: `deltalake` 1.6.3, the writer these
fixtures are pinned to, refuses `delta.columnMapping.mode` at `CREATE TABLE` and at
`SET TBLPROPERTIES` alike (`_internal.DeltaError: Column mapping is not supported for write
operation … yet`), and exposes no rename API anywhere in the package — so no fixture at this writer
version can exercise #711's trap. The trap is real regardless: `DeltaJSONToSchema`
(`pkg/formats/delta/schema.go:176`) discards `DeltaStructField.Metadata` outright, so
`delta.columnMapping.id`/`physicalName` can never reach `model.Field.FieldID` for a Delta source —
found by reading the code, not by a fixture, and still open. Time travel, concurrent writes, a
readable upsert, and every Iceberg-side row of the table above (deletes, compaction/replace) remain
unaddressed.

**Commit:** `test: extend the fixture matrix to deletes, compaction, rename and time travel`

---

## T47 — Round-trip pair testing: A→B→A equivalence

Open upstream since the beginning (#24, #113, dead #252) with no implementation on either side —
ground to lead on rather than follow.

Exactly one true round trip exists today: `TestE2E_PaimonDeltaRoundTrip`
(`test/e2e_paimon_roundtrip_test.go:65`), which seeds Parquet → Delta, then runs `DeltaToPaimon`
(`:104`) and `PaimonToDelta` (`:135`), comparing the final Delta snapshot against the original on
file record counts and schema field types. It exists because T32 found the Paimon target writing a
layout its own source could not read, and no suite crossed the two. There is no Delta↔Iceberg,
Delta↔Hudi or Iceberg↔Hudi round trip.

Do not count these as coverage, though their names suggest it: the per-format `Test*_SchemaRoundTrip`
cases are schema *serialization* round trips inside one format;
`pkg/formats/paimon/roundtrip_test.go:32` is Paimon-write then Paimon-read;
`test/e2e_column_stats_test.go:242` names a variable `roundTripped` but only checks that a second
sync wrote a new metadata version.

**Scope:** a table-driven matrix over ordered format pairs, each converting A→B→A and comparing the
recovered table to the original on a stated equivalence — schema (names, types, nullability,
field ids where the format carries them), file set by relativized path, record counts, partition
fields, and column statistics where both formats express them. The equivalence must be explicit
about what is *not* preserved, per pair, rather than comparing only what happens to survive. The
`readBackError` field already on `convertExpectation` (`test/foreign_fixtures_test.go:606`) is the
existing idiom for pinning a leg that stops short — currently unarmed, since no target sets it.

**Acceptance:** every ordered pair of implemented formats has a round-trip case that either passes
the stated equivalence or carries a named pin for exactly what it loses; a pin that starts passing
fails the suite rather than being ignored.

**Commit:** `test: assert A→B→A equivalence for every format pair`

---

## T48 — Detect the source format from the table directory

Upstream #830, and the cheapest usability win on the list. Every entry point requires the format to
be stated:

- `polytable sync` has no format flag at all (`cmd/polytable/main.go:139`–`160`). It comes from the
  config file's `sourceFormat` (`pkg/conversion/config.go:43`, propagated at
  `cmd/polytable/main.go:280`) or from catalog properties (`pkg/conversion/config.go:114` via
  `catalog.TableFormatFromProperties`), and `DatasetConfig.Validate` (`pkg/conversion/config.go:159`)
  refuses an empty one.
- `polytable inspect` takes `-f/--format` (`cmd/polytable/main.go:446`), parsed at `:383`.
- The daemon requires `sourceFormat` (`pkg/daemon/types.go:30`, used at `pkg/daemon/server.go:123`),
  and `spec/rest-service-open-api.yaml` lists it first in `ConvertTableRequest`'s `required` array.

Nothing anywhere probes the directory: a search for detection across `cmd/` and `pkg/` returns no
non-test hits.

**Scope:** one probe — `_delta_log/`, `metadata/`, `.hoodie/`, Paimon's `schema/` + `snapshot/`,
else Parquet — behind an explicit opt-in, with the stated format always winning when present. Two
constraints: an ambiguous directory (a synced table holds several) must report every format it found
and refuse rather than pick, and the probe must not be the thing that makes T45's exposure worse by
teaching the CLI to point a Parquet source at a Delta table.

**Acceptance:** each of the five formats is detected from a table this repo's own targets wrote; a
directory synced to three formats is refused with all three named; an explicit format is never
overridden; the flag is documented in `docs/how-to.md`.

**Commit:** `feat: detect the source format from the table directory`

---

## T49 — Catalog fan-out: one table into many catalogs, one identifier per target

Upstream RFC-1 (XCatalogSync) syncs a single table into several catalogs, each with its own table
identifier. polytable needs the same shape for a reason already recorded as a defect rather than a
future requirement: `docs/features-and-limitations.md:74` says "a catalog entry registers each
target format under the same table name, so registering multiple target formats overwrites the
previous registration."

The mechanism: `syncTargetToCatalogs` (`pkg/conversion/controller.go:132`) runs once per target
format and calls `client.CreateOrUpdateTable(ctx, snapshot.Table, snapshot)` (`:151`);
`GlueCatalogSyncClient` takes the name straight off that table — `tableName := table.Name`
(`pkg/catalog/glue.go:97`). So the Delta and Iceberg targets of one dataset land on the same Glue
entry and the second overwrites the first. `catalog.Config` (`pkg/catalog/catalog.go:52`) carries
`Type`, `CatalogID`, `DatabaseName`, `URI`, `Properties` and `MaxPartitionsPerRequest` — there is
nowhere to put a per-target identifier.

**Decide before code, as with T24 and T42.** Does the identifier belong to the catalog entry (one
`catalog.Config` per target, each naming its table) or to the dataset (one config holding a
format-to-identifier map)? RFC-1 puts it on the sync target. Whichever is chosen must leave the
existing single-target configuration working, which names no table at all and infers it from the
source.

**Acceptance:** a dataset with two target formats and one Glue catalog produces two distinct
registrations; the existing one-target configuration keeps working with no new required field;
`docs/features-and-limitations.md` loses the overwrite limitation instead of restating it.

**Commit:** `feat: register each target format under its own catalog identifier`

---

## T50 — Diff the vendored REST spec against upstream PR #715

A standing watch item with a fixed, small shape. Upstream #715 carries approved-but-unmerged changes
to the XTable REST service spec; `spec/rest-service-open-api.yaml` is OpenAPI 3.0.3 with four
operations — `getHealth` (`:50`), `convertTable` (`:64`), `getConversionStatus` (`:106`) and
`inspectTable` (`:136`) — and the server types are generated from it by `oapi-codegen` through
`spec/Makefile`.

The point is not to adopt #715: it is unmerged, and unmerged upstream mechanisms are intent rather
than spec. The point is to know where the two have diverged before the divergence is expensive, and
to record each difference as deliberate or accidental.

One local drift is already known and belongs in the same pass: the daemon accepts a `storage` object
on both requests (`pkg/daemon/types.go:38`, `:55`) and `spec/rest-service-open-api.yaml` documents no
such property on either `ConvertTableRequest` or `InspectTableRequest`. That is this repository's own
spec falling behind its own server, not upstream divergence.

**Acceptance:** a written diff of the two specs, each difference marked as adopted, deliberately
divergent, or an accidental drift to fix; the undocumented `storage` property is either specified or
removed; anything adopted regenerates through `spec/Makefile` in
the same commit; `docs/upstream-watch.md`'s #715 line updates to name the outcome.

**Commit:** `docs: record the REST spec divergence from upstream #715`


---

## T51 — Azure storage: ADLS Gen2, `abfss://`, and the OneLake path shape ⚠️ GREEN ON AZURITE, UNTESTED ON AZURE

**A stated requirement, not an extension** (maintainer decision, 2026-08-22), and the reason the
GCS/Azure non-goal was withdrawn: its stated precondition — T3's unreachable storage configuration —
was closed by T12.

**Where the scheme dies today.** `NewStorageForPathWithOptions` (`pkg/io/storage.go:164`) matches
literal prefixes: `s3://`/`s3a://` (`:165`), `mem://` (`:168`), then any other `<scheme>://` is
refused at `:174`, with `abfss://` reaching the same branch as `gs://` and `hdfs://`. The error
(`:175`) names the scheme and lists the supported ones — deliberate, and correct until a backend
exists. `pkg/io/storage_scheme_test.go:46` already pins the `abfss://` refusal, so that assertion
flips when this task lands; treat the flip as the acceptance signal rather than deleting it quietly.

**Three structural problems the survey turned up, none of them "add a client":**

1. **`uriSchemes` (`pkg/io/storage.go:43`) is `s3://, s3a://, gs://, mem://, file://` — no Azure
   scheme.** That list is not decoration: `JoinPath` (`:75`), `TrimScheme` (`:100`) and
   `RelativizePath` (`:120`) all iterate it, and `RelativizePath:139` treats a path with no
   recognized scheme as *already relative*. So an `abfss://` data-file path is silently mangled by
   the same helper T35 introduced to stop paths being mangled. Add the Azure schemes to `uriSchemes`
   **before** adding a backend — the fix is independently correct, and it is what makes foreign
   Azure-written metadata round-trip even where polytable cannot read the bytes.
2. **The public option type is S3-specific.** The signature is
   `NewStorageForPathWithOptions(ctx, path string, optFns ...func(*S3Options))`. Every caller —
   `cmd/polytable/main.go:330`, `pkg/daemon/daemon.go:95`, `bindings/c/polytable.go:80` — passes
   `ds.Storage.ToS3OptionFuncs()` (`pkg/conversion/config.go:130`). Azure needs its own options, so
   this is a public-API change; decide its shape here rather than bolting Azure fields onto
   `S3Options`.
3. **Credentials are not modeled at all.** `S3Options` (`pkg/io/s3.go:46`) is
   `Region`/`Endpoint`/`UsePathStyle`/`CustomHTTPClient` — no credential fields; S3 relies entirely
   on `awsconfig.LoadDefaultConfig` (`:68`) and the MinIO suite sets `AWS_ACCESS_KEY_ID` env vars
   (`test/dockertest_minio_matrix_test.go:102`). Azure has no single equivalent chain that covers
   every deployment, so credential coverage **is** the scope: Entra ID workload identity, managed
   identity, service principal, SAS, and account key. A backend accepting one of them is not
   deployable.

**Two places already duplicate the option plumbing** and will both need the Azure equivalent:
`pkg/daemon/server.go:72` re-implements `ToS3OptionFuncs` inline instead of calling it, and
`cmd/polytable/main.go:392` plus `cmd/polytable-wasm/main.go:60`/`:122` use the no-options
`NewStorageForPath`, so storage configuration is unreachable from `inspect` today. Fold that
consolidation in rather than tripling it.

**WASM constraint, non-negotiable.** §9.4 of `SPEC.md` records that keeping `aws-sdk-go-v2` out of
the `js/wasm` build took 103 packages and 7.2 MiB off the artifact. An Azure SDK gets the identical
treatment: `//go:build !js` on the real backend, a `js` stub returning `ErrAzureUnsupported`
alongside `ErrS3Unsupported` (`pkg/io/s3_js.go:30`) and `ErrGlueUnsupported`
(`pkg/catalog/glue_js.go:29`). `go.mod` has no Azure dependency today — the only `Azure/` line is
`go-ansiterm`, indirect via dockertest.

**Acceptance:** an `abfss://` table syncs end to end against Azurite in the dockertest matrix, the
way MinIO covers S3; each credential mode above is either exercised or documented as untested with
the reason; `GOOS=js GOARCH=wasm go list -deps ./cmd/polytable-wasm` reports zero Azure SDK
packages; `pkg/io/storage_scheme_test.go` still refuses `gs://`, `hdfs://` and `wasbs://` if the
last is out of scope — and `wasbs://` is currently untested either way, so state which it is.

**Commit:** `feat: add an Azure Data Lake Storage backend`

### Outcome ⚠️ — green against the Azurite emulator; nothing has touched a real Azure account

**The scheme-list fix came first and stands on its own.** `uriSchemes` (`pkg/io/storage.go`) now
carries `abfss://`, `abfs://`, `wasbs://` and `wasb://`, with `azureSchemes` and `IsAzurePath`
beside it. That is not preparation for the backend, it is a bug fix: `RelativizePath` treats a path
with no recognized scheme as *already relative*, so before this change every Azure path handed to
the helper T35 added to stop path mangling was silently mangled by it. Hadoop's ABFS driver spells
one store four ways and foreign metadata carries whichever the writing engine used, so all four
belong there whether or not a backend exists.

**The option type was generalized, as the task required.**
`NewStorageForPathWithOptions(ctx, path, ...func(*Options))` replaces the S3-specific signature;
`Options` is `{S3 S3Options; Azure AzureOptions}`, because the router picks a backend from the path
scheme, which the caller generally has not inspected. `StorageConfig.ToS3OptionFuncs` became
`ToOptionFuncs`. All six call sites were swept, including the two the task named: `pkg/daemon/server.go`
no longer inlines its own drifted copy of the translation, and `test/dockertest_minio_matrix_test.go`
stopped re-implementing it too. `polytable inspect` gained `--storage-region`, `--storage-endpoint`,
`--storage-path-style`, `--azure-endpoint`, `--azure-account` and `--azure-anonymous`: storage
configuration was previously unreachable from that command at all.

**azblob, not azdatalake — both are official Microsoft SDKs, and the choice is about API surface,
not vendor support.** `sdk/storage/azdatalake` speaks the ADLS Gen2 (dfs) API and adds hierarchical
operations: real directories, atomic directory rename, POSIX ACLs, recursive delete.
`sdk/storage/azblob` speaks the Blob API against the same account and the same bytes — ADLS Gen2 is
Blob Storage with a hierarchical-namespace flag, and both endpoints serve one store. polytable's
`Storage` contract is flat read/write/list/exists/delete on single paths, so **not one of
azdatalake's advantages is reachable through it**. Against that, Azurite — the only Azure emulator
that can run in CI — implements the Blob API and not the dfs one (Azure/Azurite#409, #553, #909 are
still open), so an azdatalake implementation would have had no executable test path at all. If the
`Storage` interface ever grows directory rename or ACLs, that is when azdatalake earns its place.

**Credentials are the scope, and they are not config fields.** `NewAzureStorage` selects, first match
wins: SAS (`SASToken` or `AZURE_STORAGE_SAS_TOKEN`), shared key (`AccountKey` or `AZURE_STORAGE_KEY`),
anonymous when configured, then `azidentity.NewDefaultAzureCredential` — the one call that covers
workload identity, managed identity, an environment service principal and the Azure CLI.
`AzureStorageConfig` exposes only `endpoint`, `accountName` and `anonymous`; secrets reach the
process through the environment, never through a file that gets committed, logged or POSTed to the
REST service. This mirrors `S3Options`, which has no credential fields either.

**WASM stays clean.** `pkg/io/azure.go` is `//go:build !js` with an `azure_js.go` stub returning
`ErrAzureUnsupported`, matching `ErrS3Unsupported` and `ErrGlueUnsupported`.
`GOOS=js GOARCH=wasm go list -deps ./cmd/polytable-wasm | grep -ci azure` reports **0**.

**Binary cost, measured on this machine with `-ldflags="-s -w"`:** 33.1 MiB before, 36.5 MiB after —
**+3.4 MiB**, almost all of it `azidentity` and its MSAL dependency. Recorded as a delta because that
is the comparable number; note `SPEC.md` §9.2's 13.9 MiB was measured in a different environment and
is not rewritten from this one.

**What was checked:** `make check` green, `go test -short -race ./pkg/...` clean, `abfss://`,
`abfs://`, `wasbs://` and `wasb://` each route to `*io.AzureStorage`, and `ParseAzureURI` is
table-tested including the OneLake workspace-as-container shape and the four malformed cases. The
`storage_scheme_test.go` pin that asserted `abfss://` was refused is inverted rather than deleted,
so it now proves the opposite.

**The Azurite suite runs, and it passes.** `test/dockertest_azurite_test.go` was executed against
`mcr.microsoft.com/azure-storage/azurite`: `DeltaToIcebergAndHudi_OnAzurite` and
`HudiToDeltaAndIceberg_OnAzurite` both sync through the Azure backend, and `RoundTripsAbfssPaths`
asserts every `FileInfo.Path` that `List` returns starts with `abfss://` and parses back through
`ParseAzureURI` to the same container, host and blob. `go test -count=1 ./test/...` is green with
the MinIO and Iceberg REST suites alongside it, so the `ToOptionFuncs` change did not regress them.

**The CLI path was driven by hand as well, which the suite does not cover.** A delta-rs-written
Delta table — the `delta-rs-checkpoint` fixture, whose early commits were expired by log retention —
was uploaded into Azurite and synced with `polytable sync --datasetConfig` over `abfss://`: both
Iceberg and Hudi targets reported `SUCCESS`. Re-running the same command reported `NO_OP` for both,
which is the check that matters, since it proves `TableSyncMetadata` was written into the target and
read back out of Azure rather than the table being resynced blind. `--dry-run` behaved. All three
formats then read back through `polytable inspect` with the new `--azure-endpoint` and
`--azure-account` flags, reporting the same schema, the same `region` partition field and the same
six data files. That exercises the flag plumbing, the config path and the sync-metadata round trip,
none of which the library-level suite touches.

**Two flags are load-bearing for the emulator, and both cost a debugging cycle to find.** Azurite
needs `--blobHost 0.0.0.0`, because the listener otherwise binds the container's loopback and a
published port resets rather than answering; and `--skipApiVersionCheck`, because `azblob` sends a
newer `x-ms-version` than Azurite recognizes and Azurite rejects the first request with
`InvalidHeaderValue`. The emulator trails the service, so the second will keep being true after the
next SDK bump. Both are in the `RunOptions.Cmd` with that reasoning in a comment.

**Unrelated to this code, worth recording because it wasted a cycle:** the failure that looked like
a broken backend was Docker Desktop's port forwarding, which accepted the TCP connection and then
reset every HTTP request while the container answered correctly on its own network. The
pre-existing MinIO suite failed identically. Restarting Docker Desktop fixed it. Suspect the daemon
before the code when every published port resets at once.

**The credential and scheme coverage was widened afterwards**, closing two of the four gaps below
as originally written. `test/dockertest_azurite_test.go` now also covers: a container-scoped SAS as
the only credential, the same SAS carrying the leading `?` that a portal copy-paste includes,
anonymous access to a private container failing rather than looking like an empty table, SAS
outranking a deliberately invalid account key — which is how the first-match-wins order is pinned,
since a regression would fail loudly on the bad key — and all four ABFS spellings reading back one
blob byte-identically. Pagination is deliberately skipped rather than faked: `List` sets no
`MaxResults`, and the server-side default page is 5000 on both Azure and Azurite, so forcing a
second page would need 5000 uploads.

### Verified against a real Azure account, 2026-08-22 ✅

Run against a pay-as-you-go subscription with a `StorageV2` account created `--hns true`, so a
genuine ADLS Gen2 hierarchical namespace, seeded with the `delta-rs-checkpoint` fixture:

- **Sync works.** `polytable sync` over `abfss://` reported `SUCCESS` for both the Iceberg and Hudi
  targets, reading a Delta table whose early commits were expired by log retention.
- **`DefaultAzureCredential` works**, which is what could not be tested without a tenant. With no
  `AZURE_STORAGE_KEY` in the environment, `polytable inspect` read the Iceberg target back through
  the Entra chain — the `az` CLI login plus a `Storage Blob Data Contributor` assignment — and
  reported the same schema, partition field and six data files.
- **T54's fix is confirmed on the account type that produces the bug.** Uploading 10 files created
  17 blobs: the hierarchical namespace materializes directories as real objects. `List` reports 6
  directories and 10 files, each `IsDir` correct. Before T54 all six would have been zero-byte
  *files*, and `region=east` and its siblings would have been crawled as data. This is the failure
  the emulator structurally cannot produce, now observed and closed.

**It now runs in CI on every night.** `.github/workflows/azure-live.yml` authenticates with a
federated OIDC credential — no client secret exists — scoped to `refs/heads/main` and to one
storage account with `Storage Blob Data Contributor` and nothing else. First run green:
`SyncOverAbfss`, `ReSyncIsNoOp`, `HierarchicalNamespaceDirectories` and `ExistsSemantics` all pass
against the real account. So the real-Azure coverage is continuous rather than a one-off.

One gotcha cost a run and is written up in `docs/azure-test-environment.md`: GitHub presented an
**immutable** subject claim embedding numeric owner and repository IDs
(`repo:owner@6705942/polytable@1332162500:ref:refs/heads/main`), not the classic
`repo:owner/repo:ref:...`. A credential registered for the classic form fails with `AADSTS700213`,
and the error names the subject GitHub actually sent.

**Why this is still ⚠️ and not ✅ — two acceptance criteria remain unmet:**

1. **No OneLake request has succeeded.** One has now been *made* — see T52's first-contact note —
   but only the failure path ran. The shapes are sourced rather than guessed.
   Microsoft documents the abfss form as
   `abfs[s]://<workspace>@onelake.dfs.fabric.microsoft.com/<item>.<itemtype>/<path>/<fileName>`,
   with "the account name is always `onelake`, the container name is your workspace name" — which
   is exactly what `ParseAzureURI` produces — and documents `onelake.blob.fabric.microsoft.com`
   as carrying the same compatibility as the ADLS endpoint, so the `.dfs.` → `.blob.` swap is the
   documented pair. Three host shapes it cannot derive are what `Endpoint` is for, and all three
   are now named in the code comment: the regional endpoint
   `<region>-onelake.dfs.fabric.microsoft.com` (which OneLake recommends over the global one,
   since resolving the global endpoint for an out-of-region workspace can move data across a
   region boundary), a workspace private-link FQDN, and `api.onelake.fabric.microsoft.com`, which
   contains neither `.dfs.` nor `.blob.` and passes through the swap untouched.

Closing this to ✅ needs an Azure subscription. Until then `docs/features-and-limitations.md` says
so, and the format matrix must not claim Azure interop beyond the emulator.

---

## T52 — OneLake and Fabric as a catalog ⚠️ AUTH LANDED, NO FABRIC WORKSPACE REACHED

Blocked on T51 for data access, and separate work: OneLake's read API is Iceberg-REST compatible, so
`pkg/catalog/rest.go` is the entry point rather than a new client — but the authentication and
identifier surfaces are both new. Upstream tracks the same idea as #810 and has not built it, so
there is no reference implementation.

**The auth gap is concrete.** The Iceberg REST client supports a static bearer token and nothing
else: `IcebergRESTCatalogClient` reads `cfg.Properties["token"]` (`pkg/catalog/rest.go:53`) and sets
`Authorization: Bearer` on each of its three calls (`:104`, `:137`, `:172`), with a hardcoded 30-second
`http.Client` (`:59`) and no injection seam. There is no OAuth2 client-credentials flow, no refresh,
no 401 retry, no custom headers. Entra ID tokens expire, so a long-running daemon sync fails partway
through with a static token.

**The asymmetry that decides the design.** The read side already has the seam —
`NewIcebergRESTConversionSourceWithClient(client *http.Client, …)`
(`pkg/catalog/rest_conversion.go:77`) — which is exactly where a token-refreshing `RoundTripper`
belongs. The sync client has no equivalent constructor. Give it one, and Entra ID becomes a
`RoundTripper` rather than auth logic spread through both files.

Also note `ListTables` yields `ErrCatalogNotImplemented` for REST catalogs
(`pkg/catalog/rest_conversion.go:161`), so T23's `--catalog … --database` discovery does not reach a
OneLake workspace. Whether Fabric's API supports listing, and whether a workspace maps to a
`DatabaseName`, is part of this task: `catalog.Config` (`pkg/catalog/catalog.go:52`) has `URI`,
`DatabaseName` and `Properties`, and Fabric addresses a table by workspace and lakehouse, which may
not fit `DatabaseName` alone.

**Acceptance:** a Fabric lakehouse table resolves as a conversion source by identifier and converts;
a token that expires mid-sync is refreshed rather than failing the run; `CatalogTypeOneLake` either
exists as a distinct type or the task records why `ICEBERG_REST` with properties is the right
spelling; `docs/iceberg-rest-catalog.md` and `docs/cloud-storage.md` state which is supported.

**Commit:** `feat: authenticate the Iceberg REST catalog with Entra ID for OneLake`

### Outcome ⚠️ — the auth gap is closed; the Fabric half is unverified

**No new catalog client, and that was the point.** OneLake's read API is Iceberg-REST compatible, so
the work was authentication, not a client. `pkg/catalog/rest_auth.go` holds the one decision point,
`restHTTPClient(cfg, timeout)`, and both REST types call it — `NewIcebergRESTCatalogClient` and
`NewIcebergRESTConversionSource` no longer each read `Properties["token"]` and build their own
`http.Client`.

**`ICEBERG_REST` with properties, not a new `CatalogTypeOneLake`** — the task asked for this decision
to be recorded either way. A distinct type would have duplicated every method to change one header,
and `CatalogType.Implemented()` would have gained a third case that behaves identically to the
second. OneLake differs from Polaris or Unity in *how it authenticates*, not in what it serves, and
authentication is already a per-config concern. Properties: `auth` (empty, `entra`, `entra-id` or
`azure`), `scope`, and the pre-existing `token`. An unrecognized `auth` value is an error naming what
was accepted, deliberately: a typo must not silently downgrade a deployment to unauthenticated.

**The transport.** `entraTransport` (`pkg/catalog/entra.go`, `//go:build !js`) refreshes when the
cached token is empty or expires within five minutes, holds its mutex across the check and refresh
only — never across `base.RoundTrip` — and **clones the request**, since mutating the caller's
`*http.Request` violates the `http.RoundTripper` contract. `newEntraHTTPClient` wraps
`azidentity.NewDefaultAzureCredential`, the single call covering workload identity, managed
identity, an environment service principal and the Azure CLI.
`NewEntraHTTPClientWithCredential` is the seam the tests drive without a tenant.

**The asymmetry that shaped the design is gone.** The read side already had
`NewIcebergRESTConversionSourceWithClient`; the write side had no equivalent, so auth logic would
have had to be duplicated across both files. `NewIcebergRESTCatalogClientWithHTTPClient` closes it.

**WASM:** `entra_js.go` returns `ErrEntraUnsupported`; `pkg/catalog` still builds for `js/wasm` and
`GOOS=js GOARCH=wasm go list -deps ./cmd/polytable-wasm | grep -ci azure` is 0.

**What was checked**, all against a fake `azcore.TokenCredential` and `httptest`: the bearer header is
attached; a still-valid token is reused (two requests, one `GetToken`); a token inside the five-minute
window is refreshed and the second request carries the new value; a `GetToken` failure surfaces and
names the scope; the caller's request is not mutated; and `restHTTPClient` covers the static default,
all three Entra spellings, an unknown value, and a nil `Properties` map. ~20 concurrent requests
through one client are clean under `-race`, which matters because the cached token is shared mutable
state. `make check` green.

### First contact with the real endpoint, 2026-08-22

A tenant-level Entra login (`az login --allow-no-subscriptions`, tenant
`31rstw.onmicrosoft.com`, no Azure subscription) was enough to exercise part of this against the
live service, and it settled three things:

- **`DefaultOneLakeScope` is right, verified rather than read.** `az account get-access-token
  --resource https://storage.azure.com/` returns a token whose decoded `aud` is exactly
  `https://storage.azure.com/`, the audience OneLake documents as the only one it accepts.
- **The Entra transport works against a real Microsoft endpoint.** Driving
  `IcebergRESTConversionSource` with `auth: entra` at
  `https://onelake.table.fabric.microsoft.com/iceberg`, `DefaultAzureCredential` picked up the CLI
  login, acquired the token and attached it. The response was a `500`, not a `401` — authentication
  passed. That closes this task's second criterion: a token has now been presented to a Fabric
  endpoint and accepted.
- **T53's negotiation behaves correctly against the real service.** It surfaced
  `iceberg REST catalog config endpoint returned 500: {"Error":{"Code":"CommunicationError"...}}`
  rather than silently falling back to an empty prefix, which is exactly the non-latched path the
  design calls for.

**What it does not prove**, and the reason this task stays ⚠️: only the failure path ran. The `500`
comes from a warehouse that does not exist — this tenant has Fabric disabled outright
(`AADSTS500014: The service principal for resource 'https://api.fabric.microsoft.com' is
disabled`), so no workspace could be created to point at. The success path — a real warehouse
resolving a prefix and a table — is still unrun.

**Worth recording for the troubleshooting guide:** OneLake answers a non-existent warehouse with a
`500 CommunicationError`, not a `404`. A typo in the `warehouse` property therefore surfaces as an
opaque internal error. polytable passes the status and body through verbatim, which is the right
call, but a reader needs to know that a 500 here usually means "wrong warehouse" rather than
"OneLake is broken".

**Why ⚠️ — three criteria unmet, and one of them was only discovered after the code landed:**

1. **No Fabric lakehouse table has been resolved or converted.** Everything above is unit-level. The
   acceptance criterion needs a Fabric workspace, and none was available.
2. ~~**`DefaultOneLakeScope` is unexercised.**~~ **Closed above**: the audience is confirmed from a
   decoded token and one has been accepted by the live endpoint. `scope` stays configurable because
   a non-OneLake REST catalog behind Entra ID may want a different audience.

3. ~~**The endpoint needs prefix negotiation polytable does not do.**~~ **Closed by T53.** Microsoft publishes OneLake's Iceberg REST endpoint at
   `https://onelake.table.fabric.microsoft.com/iceberg`. A client first calls
   `GET /v1/config?warehouse=<Workspace>/<DataItem>`, and the response's `overrides.prefix` goes
   into every later path as `/v1/{prefix}/namespaces/...`. polytable builds `/v1/namespaces/...`
   with no prefix segment (`pkg/catalog/rest.go:110`, `:128`, `:179`;
   `pkg/catalog/rest_conversion.go:98`), so **every request would miss regardless of
   authentication**. Scheduled as **T53**. The same page settles two other things: the endpoint is
   read-only — its advertised operations are `GET` and `HEAD` only — so the sync client can never
   register a table into a Fabric workspace through it, and the sample configuration requests
   `https://storage.azure.com/.default`, confirming `DefaultOneLakeScope` exactly.
4. ~~**Listing is still unavailable.**~~ **Closed by T53**, which implemented `ListTables` for REST
   catalogs. `docs/iceberg-rest-catalog.md` documents the `auth`, `token`, `scope` and `warehouse`
   properties, which closes the documentation half of the acceptance.

Also still true and not part of this task: `ListTables` yields `ErrCatalogNotImplemented` for REST
catalogs (`pkg/catalog/rest_conversion.go`), so T23's `--catalog … --database` discovery does not
reach a OneLake workspace. Whether Fabric supports listing, and whether a workspace maps onto
`DatabaseName`, stays open.

---

## T53 — Iceberg REST: negotiate the catalog prefix before addressing anything ✅ COMPLETED

Found while documenting T52, by reading Microsoft's OneLake table-API guide rather than the code.
It is a general Iceberg REST conformance gap that happens to block OneLake completely.

The REST catalog specification has clients call `GET /v1/config?warehouse=<warehouse>` first. The
response carries `overrides.prefix`, and every later path is `/v1/{prefix}/namespaces/...`.
polytable never makes that call and hardcodes the prefix-less form in four places:
`pkg/catalog/rest.go:110` (create), `:128` (commit), `:179` (drop) and
`pkg/catalog/rest_conversion.go:98` (load). A catalog that returns an empty prefix — Nessie and the
`tabulario/iceberg-rest` image the test suite uses — works by accident, which is why this has gone
unnoticed.

OneLake does not return an empty prefix. Its warehouse is `<WorkspaceID>/<DataItemID>` or
`<WorkspaceName>/<DataItemName>.<DataItemType>`, and the config response's prefix is "usually the
same as the warehouse". So every polytable request to a Fabric workspace misses, whatever the
credentials are.

Two related facts from the same source, which shape the scope:

- **The OneLake endpoint is read-only.** It advertises `GET` and `HEAD` over namespaces and tables
  and nothing else. `IcebergRESTCatalogClient.CreateOrUpdateTable` can therefore never succeed
  against it. The reachable half is `IcebergRESTConversionSource` — reading a Fabric table as a
  conversion source. A target that cannot register must fail with a message saying the catalog is
  read-only, not with a bare 404 or 405.
- **`ListTables` becomes implementable.** The endpoint advertises
  `GET /v1/{prefix}/namespaces` and `GET /v1/{prefix}/namespaces/{namespace}/tables`, so the
  `ErrCatalogNotImplemented` that `pkg/catalog/rest_conversion.go` returns today is a gap rather
  than a protocol limit. It is what would let `polytable sync --catalog ... --database ...` scan a
  Fabric workspace.

**A `warehouse` property is needed**, since `catalog.Config` has `URI`, `DatabaseName` and
`Properties` but nothing that carries a warehouse. `DatabaseName` maps to the Iceberg namespace —
`dbo` in Fabric's examples — so it cannot double as the warehouse.

**Worth recording for its own sake:** the sample `GET /v1/{prefix}/namespaces/{ns}/tables/{table}`
response in Microsoft's guide carries an `XTABLE_METADATA` table property and the same key in the
snapshot summary, holding `lastInstantSynced` and `sourceTableFormat: DELTA`. Fabric is exposing
Delta tables as Iceberg through Apache XTable itself. That makes the shared-key claim behind T30
directly load-bearing for Fabric interop rather than a theoretical nicety: a table Fabric published
carries the metadata polytable reads, and vice versa.

**Acceptance:** a config call resolves the prefix once per client and every subsequent path uses
it; a catalog returning no prefix behaves exactly as today, pinned by the existing
`tabulario/iceberg-rest` dockertest suite so this is provably not a regression; a `warehouse`
property reaches the config call; `ListTables` is implemented for REST catalogs; and a write
attempt against a read-only catalog fails with a message naming that as the reason. The OneLake leg
needs a Fabric workspace — see `docs/azure-test-environment.md`.

**Commit:** `fix: negotiate the Iceberg REST catalog prefix before addressing tables`

### Outcome ✅

`restCatalogEndpoint` (`pkg/catalog/rest_config.go`) is the one place the HTTP client, bearer token,
base URI and negotiated prefix live, and both REST types are built around it instead of each
carrying their own copy.

**What "at most once" actually means here.** `negotiatePrefix` runs under a mutex and latches only
*terminal* outcomes: a decoded 200, or a 404/405 meaning the catalog predates `/v1/config`, which
latches an empty prefix — today's behavior exactly. A transport error or an unexpected status is
deliberately **not** latched, because negotiation did not conclude; latching it would let one
network blip brick the client for its whole lifetime. That distinction is the design decision worth
remembering.

**The path builder handles the shape OneLake actually uses.** `path()` escapes every segment, and
splits a prefix that itself contains a slash — OneLake's is `<workspace>/<item>` — so the separator
survives escaping instead of becoming `%2F`. An empty prefix emits no segment at all, which is what
keeps the existing catalogs working.

**`ListTables` is implemented**, replacing `ErrCatalogNotImplemented`. An empty `database` walks
every namespace via `GET /v1/{prefix}/namespaces`; a named one lists just that. Both listings follow
`next-page-token`, so a large namespace is not silently truncated. It mirrors
`GlueConversionSource.ListTables` for mid-iteration errors and early abandonment. Note
`TableFilter.RequireConversionMarkers` costs one `GetSourceTable` per candidate, because the REST
listing endpoints return identifiers without properties; the default unset filter costs nothing
extra.

**Read-only catalogs fail with a reason.** `writeEndpointAdvertised` reads the config response's
`endpoints` array and counts only POST/PUT/DELETE routes mentioning `/tables` or `/namespaces`, so
an unrelated `POST /v1/oauth/tokens` cannot mask a genuinely read-only table surface. A write is
refused pre-emptively when the catalog advertises no write route, and on a runtime 405, with a
message naming the operation rather than a bare status.

**Verified:** `pkg/catalog/rest_prefix_test.go` drives an `httptest` server shaped like OneLake and
covers all eight required scenarios — the prefix reaching the request path, the warehouse reaching
the config query, an empty prefix producing today's paths, one config call across two operations, a
404 falling back, `ListTables` walking and surfacing a mid-iteration error, a 405 naming the
operation, and concurrent operations making exactly one config call under `-race`.
`TestDockertest_IcebergREST` against the `tabulario/iceberg-rest` container passes unchanged, which
is the proof the empty-prefix path did not regress. `make check` green.

**The OneLake leg is still unrun** — the endpoint now addresses correctly by construction, but no
request has reached a Fabric workspace. That stays T52's open criterion, not this task's.

---

## T54 — ADLS Gen2 directories are reported as files ✅ COMPLETED

Found while planning the Azure test work, by reasoning about what the emulator cannot cover rather
than by a failing test — which is the point: **no test in this repository can catch it**, because
Azurite has no hierarchical namespace.

`AzureStorage.List` (`pkg/io/azure.go`) sets `IsDir: strings.HasSuffix(name, "/")`. That is the
Blob Storage convention, where a directory is not an object and appears only as a name prefix. On an
**ADLS Gen2 account — one with the hierarchical namespace enabled, which is what an `abfss://` path
means and what `docs/azure-test-environment.md` provisions** — directories are real objects, and
`NewListBlobsFlatPager` returns them as blob items whose names do not end in `/`. Every directory on
a real ADLS Gen2 account is therefore reported as a zero-byte file.

The SDK carries the signal twice, and both are in `azblob@v1.8.0`'s
`internal/generated/zz_models.go`: `BlobItem.Properties.ResourceType` is `"directory"`, and
`BlobItem.Metadata["hdi_isfolder"]` is `"true"`. The second is only populated when the listing asks
for metadata, which the current call does not.

**Why it has not bitten yet, and why that is not reassuring.** `pkg/formats/parquet/source.go:185`
filters on a `.parquet` suffix, and the Delta and Hudi listings filter on their own suffixes, so a
directory usually falls out on its name rather than on `IsDir`. The exposure is a directory whose
name matches one of those filters, and T45 — which will replace suffix filtering with proper
metadata-directory exclusion — makes `IsDir` load-bearing rather than incidental.

**Acceptance:** an item carrying `ResourceType: "directory"`, one carrying only `hdi_isfolder`, and
one with a trailing slash are all reported as directories; an ordinary blob is not; the check is
nil-safe on `Properties`, `Metadata` and the `*string` values; the Azurite suite still passes,
proving the Blob-convention path is unchanged.

**Commit:** `fix: report ADLS Gen2 directories as directories`

### Outcome ✅

`isDirectory(name, item)` in `pkg/io/azure.go` replaces the trailing-slash test, checking
`Properties.ResourceType == "directory"` and `Metadata["hdi_isfolder"] == "true"` (both
case-insensitively and nil-safe) before falling back to the Blob-Storage convention, and `List` now
sets `Include: azblob.ListBlobsInclude{Metadata: true}` — without which the second signal is never
populated. The helper takes `*container.BlobItem`: `azblob` does not re-export `BlobItem` at the top
level, only `container.BlobItem`.

**The test is proven non-vacuous**, which matters more than usual here because no emulator can
exercise the path. `pkg/io/azure_list_test.go` drives `List` against an `httptest` server returning
a hand-written Azure `ListBlobs` XML body, with five cases: a directory by `ResourceType`, one by
`hdi_isfolder` alone, one by trailing slash, an ordinary blob, and one with no `<Properties>`
element at all. Stashing just the `azure.go` change makes exactly the two ADLS Gen2 subtests fail
and the other three pass; restoring it makes all five pass.

One branch is deliberately untested: a nil `*string` inside `Metadata`. The SDK's own
`additionalProperties.UnmarshalXML` always wraps values with `to.Ptr`, so it cannot produce one —
the guard is defensive against a hand-constructed item, and the test says so rather than contorting
to reach it.

Verified: `go test -race ./pkg/io/` clean, `golangci-lint` 0 issues, `js/wasm` build clean, and the
Azurite suite still passes — the Blob-convention path is unchanged.

---

## T55 — One process, one Azure account: credentials are process-wide ✅ COMPLETED

The daemon loops datasets (`pkg/daemon/daemon.go:75`) and builds a `Storage` per dataset from that
dataset's own `StorageConfig`. Azure credentials do not follow that per-dataset path: `SASToken`
falls back to `AZURE_STORAGE_SAS_TOKEN` and `AccountKey` to `AZURE_STORAGE_KEY`
(`pkg/io/azure.go:119`, `:123`), both process-wide. So a daemon or REST service syncing tables that
live in two storage accounts with different keys cannot serve both, and there is no configuration
that expresses it.

`AzureStorageConfig` deliberately holds no secret, and that rule stays: a config file gets
committed, logged and POSTed to the REST service. **The fix is to name the variable, not the
value** — the config says which environment variable holds the secret for this dataset:

```yaml
storage:
  azure:
    accountName: acct1
    accountKeyEnv: ACCT1_STORAGE_KEY
```

The secret still never appears in the file, and one process can hold as many accounts as it has
variables. An unset named variable is an error naming the variable, not a silent fall-through to the
Entra chain — that fallback would turn a typo into a confusing 403 much later.

Ordering is the part to get right: an explicitly named variable outranks the well-known one, so
`accountKeyEnv` beats `AZURE_STORAGE_KEY`, and the existing first-match-wins order between SAS,
shared key, anonymous and Entra is otherwise unchanged.

**S3 has the same shape of limitation** — `NewS3Storage` relies entirely on
`awsconfig.LoadDefaultConfig` — so decide in this task whether the mechanism is Azure-only or
`StorageConfig`-wide. Azure-only is defensible if nobody has asked for the S3 case; say which was
chosen and why.

**Acceptance:** two datasets naming different account-key variables sync to different accounts in
one process, covered by the Azurite suite with two containers and two variables; an unset named
variable fails with a message naming it; the well-known variables keep working unchanged, pinned by
the existing tests.

**Commit:** `feat: let a dataset name the environment variable holding its Azure credential`

### Outcome ✅

`AzureStorageConfig` gained `accountKeyEnv` and `sasTokenEnv`, mirrored onto `AzureOptions` and
carried through `ToOptionFuncs`. `resolveAzureCredential(literal, envVarName, configField,
wellKnownEnv)` in `pkg/io/azure.go` applies one order at both the SAS and shared-key sites: a
literal option wins outright, then the named variable, then the well-known one. With no new field
set the behavior is byte-identical to before.

**The no-fall-through rule is the load-bearing part.** A named variable that is unset or empty is a
hard error naming both the config field and the variable — it does **not** try the next credential
mode. Falling through would turn a typo in `accountKeyEnv` into an Entra 403 much later, from a
completely different subsystem, which is the kind of error nobody traces back to a config typo.

**Scope decision: Azure only.** `NewS3Storage` relies entirely on `awsconfig.LoadDefaultConfig` and
has the same shape of limitation, but nobody has asked for the S3 case. `resolveAzureCredential`'s
signature is directly reusable there if they do.

**Acceptance, checked:**
- **Two datasets, two variables, two accounts, one process** — `TwoAccountsOneProcess` in
  `test/dockertest_azurite_test.go` starts a second Azurite container with its own account via
  `AZURITE_ACCOUNTS`, so the two datasets really do address different stores rather than one store
  on two ports. Both configs go through `conversion.StorageConfig.ToOptionFuncs()` and
  `io.NewStorageForPathWithOptions`, which is what proves the per-dataset plumbing rather than the
  option struct. A blob written to account A is asserted invisible from account B's storage.
- **`AZURE_STORAGE_KEY` is set to a deliberately wrong value for the whole subtest.** That is the
  point of the test: if resolution ever falls back to the well-known variable, both datasets fail
  loudly instead of passing quietly. Do not "simplify" it away.
- **An unset named variable fails construction** with an error naming the variable, asserted in the
  same subtest and in a 15-case unit table in `pkg/io/azure_credentials_test.go`.

**Negative control run:** pointing both datasets at the same variable fails, at `Write` with
`403 AuthorizationFailure` rather than at the cross-account assertion — account B rejecting account
A's key, which is exactly the production failure this guards against.

`docs/azure.md` and `docs/cloud-storage.md` document both keys and the precedence rule.

---

## T56 — `Exists` cannot tell "absent" from "not allowed" ✅ INVESTIGATED, NO CHANGE NEEDED

Found by the Azurite credential work, as a reasoned risk rather than a reproduction — record it that
way and confirm it before writing the fix.

`AzureStorage.Exists` (`pkg/io/azure.go`) maps `bloberror.BlobNotFound` to `(false, nil)`, mirroring
what the S3 backend does with `NotFound`. Azure's documented behavior for an **anonymous request
against a private container is `404`, not `403`** — deliberately, so that the API does not leak
whether a resource exists. Under that response, a permissions failure is indistinguishable from a
missing file, and every caller treats it as absent.

That is the same failure class as T37's Hudi 1.x finding: a table that reads as empty and exits 0 is
worse than one that fails, because a sync succeeds and writes empty target metadata over it.

**Not reproduced.** On Azurite, anonymous access to a private container returns `403
AuthorizationFailure`, and `AnonymousAccessFailsClosed` in `test/dockertest_azurite_test.go` pins
that behavior. The emulator and the service differ here, which is precisely why this needs a real
account before anyone changes the mapping.

**Scope, in order.** (1) Confirm against a real storage account what an anonymous and an
under-privileged request return for a blob that exists and one that does not — four cases, and the
answer decides everything after. (2) If `404` is returned for both, `Exists` cannot use the status
alone: the distinguishing signal is whether the client is anonymous, or the `x-ms-error-code` header
Azure sets alongside. (3) Check whether the S3 backend has the same exposure before deciding where
the fix lives.

**Acceptance:** against a real account, an unauthorized `Exists` returns an error rather than
`(false, nil)`, a genuinely missing blob still returns `(false, nil)`, and the Azurite suite keeps
passing unchanged. If the investigation shows Azure returns `403` here after all, the outcome is a
line in this task saying so and no code change.

**Commit:** `fix: distinguish an inaccessible blob from a missing one`

### Outcome ✅ — the concern does not reproduce; `Exists` already fails closed

Checked against a real ADLS Gen2 account on 2026-08-22, all four cases the task asked for:

| Caller | Blob | Azure response | `Exists` returns |
| :--- | :--- | :--- | :--- |
| Anonymous | exists | `401 NoAuthenticationInformation` | `(false, error)` |
| Anonymous | missing | `401 NoAuthenticationInformation` | `(false, error)` |
| Authenticated, no RBAC role | exists | `403 AuthorizationPermissionMismatch` | `(false, error)` |
| Authenticated, no RBAC role | missing | `403 AuthorizationPermissionMismatch` | `(false, error)` |

The under-privileged case used a service principal created with no role assignment at all, driven
through `DefaultAzureCredential`'s environment path, and deleted afterwards.

**Azure does not return `404` here.** The existence-hiding `404` behavior applies to probing a
*public* blob endpoint, not to a private container reached without adequate credentials. Since
`bloberror.BlobNotFound` never fires on these paths, the `(false, nil)` mapping is never reached
and `Exists` fails closed in all four cases — which is the behavior the task wanted.

**No code change.** This task exists as the record that the risk was real enough to check and the
check came back clean, so the next person reading `Exists` does not have to re-derive it. Azurite's
`403` and real Azure's `401`/`403` differ in code but agree in kind, so
`AnonymousAccessFailsClosed` in the emulator suite pins the right behavior after all.

---

## T57 — Delta field identity is the name, not the column-mapping id

Found by reading `pkg/formats/delta/schema.go` while building T46's Delta fixtures, not by a failing
test — and it cannot currently be caught by one, which is the reason to write it down.

`DeltaStructField` (`pkg/formats/delta/schema.go:34`) parses `metadata`, and `DeltaJSONToSchema`
reads it for `MetadataKeyDecimalPrecision` and `MetadataKeyDecimalScale` (`:127`, `:130`) — but
nothing reads `delta.columnMapping.id` or `delta.columnMapping.physicalName`. `grep -rn FieldID
pkg/formats/delta/*.go` on the non-test files returns **nothing**: `model.Field.FieldID` is never
populated for a Delta source at all.

Under Delta column mapping, the column-mapping id is the *only* correct source of field identity: a
rename changes the name and keeps the id. With identity taken from the name, a renamed column
reaches an Iceberg or Hudi target as a drop plus an add, losing the column's history and its
statistics. That is exactly upstream #711, which is open there.

**It is not reproducible at the pinned writer version**, and the T46 work established that with
evidence rather than assumption: `deltalake==1.6.3` refuses column mapping on both paths —
`write_deltalake(..., configuration={"delta.columnMapping.mode": "name"})` and
`DeltaTable.alter.set_table_properties(...)` both fail with
`Column mapping is not supported for write operation ... yet`, and the package exposes no rename API
at all. So a fixture needs either a newer delta-rs or a Spark-written table, which puts it in T30's
JVM lane.

**Scope.** Read the column-mapping keys into `model.Field.FieldID` when the table properties enable
column mapping, and decide what `FieldID` means when they do not — Delta without column mapping has
no stable id, so inventing one would be worse than leaving it unset. Check what the Iceberg and Hudi
targets do with a source field that has no id before changing either.

**Acceptance:** a Delta table with `delta.columnMapping.mode=name` and a renamed column converts to
Iceberg with the field id preserved and the column reported as renamed, not dropped and re-added;
a Delta table without column mapping behaves exactly as it does today. The fixture is the blocker,
so this task starts by getting one — newer delta-rs, or Spark under T30.

**Commit:** `fix: take Delta field identity from the column-mapping id`

---

## T58 — Iceberg REST conformance against real catalogs ✅ COMPLETED

T53 made the client negotiate a prefix and tested it against an `httptest` fake and
`tabulario/iceberg-rest`. Both return the shape T53 was written for. Running five real
implementations found four defects that neither could show, and two of them break current,
maintained catalogs outright.

**1. The prefix can arrive under `defaults`, and we read only `overrides`.** Nessie 0.108 and
Lakekeeper 0.13 both put it in `defaults.prefix` and send no `overrides.prefix` at all, so Go left
it at the zero value and every path the client built afterwards was prefix-less. Curl-verified
against both: prefix-less `POST /v1/namespaces` → `404`, prefixed → `200`. The specification's merge
order is defaults, then client-supplied, then overrides; polytable supplies none of its own, so
`mergeConfigPrefix` takes `overrides` when present and `defaults` otherwise. **This is the finding
that justifies the exercise**: T53 closed the OneLake shape and left two other real catalogs broken.

**2. The prefix must not be escaped.** Nessie serves it already percent-encoded
(`main%7Cwarehouse`); OneLake serves `<workspace>/<item>` with a literal separator. Splitting on `/`
and escaping each part is right for the second and turns the first's `%` into `%25`. `path()` now
passes the prefix through verbatim — a catalog dictates it as a ready-to-use path component — while
still escaping the namespace and table segments.

**3. A `404` from `/v1/config` was latched as "this catalog predates the endpoint".** Polaris
answers a *typo'd warehouse* with `404` even though the endpoint exists, so a misconfigured
warehouse degraded silently to prefix-less requests forever. `fetchRESTConfig` now asks again
without the warehouse: if the endpoint answers, the warehouse was the problem and the error says so.
Lakekeeper's `400` when no warehouse is given counts as "the route exists", which is why the check
tests for absence rather than success.

**4. The read-only heuristic missed the one catalog it exists for.** Unity Catalog OSS is `GET`/`HEAD`
only but advertises `POST /v1/{prefix}/namespaces/{ns}/tables/{table}/metrics`, which matched the
write-route filter. polytable would have called it writable and failed a registration with a bare
status instead of the read-only message. Routes ending in `/metrics` no longer count as writes.

**Confirmed against Amazon S3 Tables, which would have been broken without both fixes.** Its
`GET /v1/config` for a table bucket returns:

```json
{"defaults":{...,"prefix":"arn%3Aaws%3As3tables%3A<region>%3A<account>%3Abucket%2F<name>"},"overrides":{}}
```

That is both bugs at once, from a service nobody had suggested was affected. The prefix is under
`defaults` with `overrides` present but **empty**, so the pre-fix client would have computed no
prefix at all; and it arrives **already percent-encoded** — the ARN's `:` and `/` are `%3A` and
`%2F` — so the pre-fix per-segment escaping would have turned each `%` into `%25`. Either alone
breaks it. polytable's client listed namespaces and discovered a table against the live endpoint on
the first attempt, with the region and signing name derived from the host and no properties set.

**Confirmed against a live Apache Polaris container**, after the fixes landed: `overrides.prefix`
is `pt_catalog` with `defaults` carrying no prefix, so the merge order is right; an unknown
warehouse really does answer `404 NotFoundException` while no warehouse answers `400`, which is
exactly the pair the disambiguation distinguishes; and the advertised `endpoints` array carries both
real write routes and the `/metrics` route, so the exclusion narrows the heuristic without disabling
it. The same run surfaced one thing none of the fixes covers, filed as T61: Polaris also returns
`"namespace-separator": "%1F"`, which polytable ignores.

**Verified.** Four scenario groups in `pkg/catalog/rest_prefix_test.go`, each driving an `httptest`
server shaped like the real catalog, and **each confirmed to go red when its fix is reverted** —
checked by actually reverting each change and restoring it, which is the only way to know a
regression test regresses. Wire-level assertions read `r.RequestURI` rather than `r.URL.Path`,
because a parsed-and-re-encoded path cannot show a double-encoding bug. `make check` green,
`go test -race ./pkg/catalog/` clean.

**Not verified:** the `tabulario/iceberg-rest` container suite, which would prove the empty-prefix
path did not regress. Docker Desktop was wedged on this machine when the work landed. The unit
coverage includes a neither-present case that pins the same behavior, but the container run is still
owed.

**Correction to T53's record.** Its outcome says Nessie returns an empty prefix. That is true only of
the stale `docker.io/projectnessie/nessie` image (0.76.6), which has no Iceberg REST module at all
and 404s every Iceberg path. Nessie's releases moved to `ghcr.io/projectnessie/nessie`, and the
current server returns a non-empty prefix under `defaults`.

**Commit:** `fix: read the Iceberg REST prefix from defaults and stop escaping it`

---

## T59 — REST catalog auth: OAuth2 client-credentials and SigV4

`restHTTPClient` (`pkg/catalog/rest_auth.go`) recognizes two `auth` modes: a static bearer token,
and Entra ID. That leaves polytable unable to authenticate to several catalogs it can otherwise
address correctly — the protocol work T53 and T58 did is wasted on them.

**OAuth2 client-credentials** is the Iceberg REST specification's own mechanism, exchanged at
`POST /v1/oauth/tokens` with a client id and secret. Apache Polaris uses it natively, and so does
Snowflake Open Catalog, which is Polaris — its endpoint path is literally
`https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog`. A user pointing polytable at
either today has to fetch a token out of band and paste it in as a static `token`, which then
expires mid-sync exactly the way T52's Entra work fixed for OneLake.

**SigV4** is what both of AWS's native Iceberg REST endpoints require: Glue's at
`https://glue.<region>.amazonaws.com/iceberg` (warehouse is the account id) and S3 Tables' at
`https://s3tables.<region>.amazonaws.com/iceberg` (warehouse is the table bucket ARN). Neither is
reachable with a bearer token. The endpoints and their warehouse shapes come from AWS's
documentation, cited in `docs/aws.md`; **the predicted failure has not been reproduced against a
live account**, so confirm it before building the fix.

Both fit the shape T52 established: an `http.RoundTripper` selected by `auth`, so neither leaks into
the call sites. SigV4 can reuse the AWS SDK's signer, which `go.mod` already carries — but it must
stay behind `//go:build !js` like the rest of the AWS code, and the `js` stub needs the same
treatment `entra_js.go` got.

**Decide the naming here**: `auth: oauth2` and `auth: sigv4` alongside `entra`, with credentials
from the environment rather than config fields, matching the rule the Azure work settled.

**Acceptance:** polytable authenticates to a local Polaris container using client-credentials
without a hand-fetched token, pinned by a dockertest suite; the token is refreshed rather than
presented until it expires; a SigV4 request to Glue's endpoint is signed correctly, verified against
a live account or explicitly recorded as unverified; `GOOS=js GOARCH=wasm go list -deps
./cmd/polytable-wasm | grep -ci azure` stays 0 and no AWS package enters the wasm build either.

**Outcome: both halves implemented, and both run against the real thing. One criterion is unmet.**

*SigV4* landed in `a30d5b2` and has since been run against **two** live AWS endpoints. Glue's
(`https://glue.<region>.amazonaws.com/iceberg`) listed without an auth error. Amazon **S3 Tables**
worked on the first attempt with the region and signing name derived from the host and no properties
set — and its config response turned out to carry both of T58's bugs, so it is recorded there too.

*OAuth2 client-credentials* is implemented as SigV4's sibling: `auth: oauth2` (also `oauth`,
`client-credentials`), with the token endpoint defaulting to `<uri>/v1/oauth/tokens` and refreshed
30 seconds before `expires_in`, a shorter margin than Entra's five minutes because these tokens are
often short-lived.

**The secret never appears in config.** `clientSecretEnv` *names* an environment variable, mirroring
`AccountKeyEnv` from T51 and T55. There is no well-known fallback variable — OAuth2 has no
equivalent of the AWS credential chain — and an unset variable is a named error rather than a silent
fallthrough, so one process can authenticate to several catalogs from different variables.

A generic `header.<Name>` property carries vendor headers. Polaris needs `Polaris-Realm`, and a
review caught that those headers reached the catalog request but **not the token exchange**, which
lives on the catalog's own host, unlike Entra's. Both now carry them, with a test for each.

**Verified against a live Apache Polaris container**, which is what makes this more than unit
coverage: given only a client id, a scope, and the name of an environment variable, polytable
fetched its own token and listed namespaces through the negotiated prefix. Polaris's `/v1/config`
returns 401 unauthenticated and no static token was supplied, so the exchange demonstrably happened;
its log shows the token request. Since **Snowflake Open Catalog is Polaris** — its endpoint path is
literally `/polaris/api/catalog` — this covers the protocol Snowflake serves.

**Now pinned** by `test/dockertest_polaris_test.go`, which boots `apache/polaris:latest` and drives
the real thing. Two of its subtests matter more than the happy path: a wrong secret must fail with a
*named* authentication error and yield no identifiers, since a test that only proves success passes
equally against an auth path that silently degrades; and the prefixed path must appear in a recorded
request while the unprefixed form never does.

Two facts the suite establishes that the manual run did not. Creating a namespace returns **200**,
not the 201 that catalog creation returns. And a full round trip through the catalog **cannot** be
tested this way: Polaris's INTERNAL catalog tries to resolve an AWS region to write the initial
metadata file under the dummy `s3://` location and fails with a 500 naming the region-provider
chain. Supplying `AWS_REGION` only moves the failure to `AssumeRole` against the dummy role, so the
subtest skips with that reason stated rather than being weakened. **Its read-back assertions have
therefore never executed** — treat them as unwritten until real storage backs a Polaris container.

**Snowflake has since been reached**, via its Horizon Catalog rather than Open Catalog, and the run
found a defect no fake would have. Snowflake Open Catalog is **closed to new customers** — its own
documentation says a customer who has never had one cannot sign up — and directs them to Horizon
Catalog, which serves the same Polaris-derived endpoint at
`https://<account>.snowflakecomputing.com/polaris/api/catalog`.

Its `/v1/config` for a database returns `overrides.prefix` set to the **database name**, `defaults`
carrying only an empty `default-base-location`, and real write routes. Unlike self-hosted Polaris it
advertises **no `namespace-separator`**, so the identity `docs/snowflake.md` assumed does not extend
to every field.

**The defect:** Snowflake authenticates the client-credentials exchange on the secret alone — a
programmatic access token — and **rejects a request carrying a `client_id`**, reporting it as
`invalid_scope`, an error naming a field that is not the problem. polytable required `clientId` and
always sent it, so Snowflake was unreachable. It is now optional and omitted from the form when
empty, pinned by `TestOAuth2OmitsEmptyClientID`. With that, polytable authenticates to Snowflake,
negotiates the prefix, and lists.

Two things worth knowing before repeating this. A **network policy must be attached** to the user or
account before Snowflake accepts a programmatic access token at all — without one every call fails
`390432 Fail : Network policy is required`, including the SQL API, which is the quickest way to tell
a bad token from a bad policy. And the `warehouse` property is the **database** name, not a
Snowflake warehouse.

**Unmet:** listing returned zero tables, because the databases available carry no Iceberg tables —
**no table has been read through Snowflake**, only the catalog protocol exercised. Key-pair/JWT and
External OAuth are unimplemented; only the token-exchange path works. — no account — so its hostname
handling, its `/v1/config`, and any auth it layers on top are assumed by software identity rather
than verified. `docs/snowflake.md` says so.

**Commit:** `feat: authenticate REST catalogs with OAuth2 client credentials and SigV4`

---

## T60 — polytable and Java XTable do not recognize each other's sync state ✅

**T30's headline claim is false, and this is the evidence.** That task says the sharpest claim is
"tables move between polytable and Apache XTable without resync (shared `xtable_*` keys)". They do
not, and the keys are not shared.

Read from a real Iceberg table that the Java jar synced from Delta, sitting in Azure at
`abfss://xtable-pr897@polytable410464.dfs.core.windows.net/src/delta_small`, courtesy of the session
testing upstream PR #897:

```
XTABLE_METADATA = {"lastInstantSynced":"2026-08-22T16:10:52Z","instantsToConsiderForNextSync":[],
                   "version":0,"sourceTableFormat":"DELTA","sourceIdentifier":"1"}
```

polytable writes and reads something with no overlap at all (`pkg/model/metadata.go`):
`xtable_last_instant_synced`, holding **epoch milliseconds**, and `xtable_source_format`. Java
writes **one** property holding **JSON**, with an **ISO-8601** instant inside it.

Different property names, different cardinality, different value types. So:

- polytable does not recognize a Java-synced table and resyncs it in full.
- Java does not recognize a polytable-synced table and will do the same.

**Confirmed empirically, not inferred.** Copying that table and running `polytable sync` on it
reports `"verdict": "SUCCESS"` with a fresh `lastInstantSynced`, where a recognized sync state would
report `NO_OP`. The comment in `pkg/model/metadata.go` asserting that the `xtable_` spelling "is the
on-disk contract with Java-XTable-synced tables" was simply wrong; it is corrected in the same commit
that files this.

**This reaches further than the interop nightly.** Microsoft Fabric exposes Delta as Iceberg through
Apache XTable, and its table responses carry the same `XTABLE_METADATA` property. So polytable
cannot recognize a Fabric-synced table either, which makes this a OneLake correctness issue and not
only an upstream-compatibility nicety.

**What is genuinely shared**, and worth keeping in view: polytable reads the Java jar's Iceberg
output correctly — Avro manifests, schema, all eight data files — and reads the delta-spark 3.2.0
source table too, which is a writer no fixture in this repository had covered. The formats
interoperate. Only the sync bookkeeping does not.

**Decide first, because the compatible spelling is a choice with consequences.** Reading
`XTABLE_METADATA` is straightforward and strictly additive. *Writing* it is the real decision: emit
Java's shape so upstream recognizes polytable's output, emit both, or keep ours and accept one-way
compatibility. Emitting both is the only option that lets either tool continue the other's work, and
it costs one extra property. Note `instantsToConsiderForNextSync` and `sourceIdentifier` have no
polytable equivalent and their semantics need reading out of the Java source before anything is
written into them.

**Acceptance:** polytable syncing a Java-synced table reports `NO_OP` or an incremental sync rather
than a full snapshot, pinned by a committed fixture rather than by the live Azure copy; the
ISO-8601-versus-epoch conversion round-trips without drift; and whatever is decided about writing is
recorded here with its reason. The reverse direction — Java recognizing polytable's output — needs
the JVM lane in T30 to verify.

**Commit:** `fix: read Java XTable's XTABLE_METADATA sync state`

### Outcome ✅ — both directions read, both shapes written, verified against the live fixture

**Decision: write both shapes**, as this task anticipated. `model.WriteSyncMetadataProperties`
(`pkg/model/metadata.go`) sets polytable's own flat `xtable_last_instant_synced` /
`xtable_source_format` keys and Java's single `XTABLE_METADATA` JSON blob on every sync, in every
target (Delta, Iceberg, Hudi, Paimon). `model.ReadSyncMetadataFromProperties` reads either shape,
preferring `XTABLE_METADATA` when both are present because it is the richer, canonical one (it
alone carries `sourceIdentifier` and pending instants) — tested in
`TestReadSyncMetadataFromProperties/both_present_prefers_XTABLE_METADATA_because_it_is_the_richer_canonical_shape`.

**The instant conversion round-trips without drift.** Java's Jackson (`JavaTimeModule`,
`WRITE_DATES_AS_TIMESTAMPS=false`) renders a millisecond-precision `Instant` as whole seconds when
the millisecond component is zero, or exactly 3 fractional digits otherwise —
`DateTimeFormatterBuilder#appendInstant(-1)`'s fixed-group rule. Go's `time.RFC3339Nano`, formatting
a `time.UnixMilli(ms).UTC()` value, produces the identical decimal value under its own
trim-trailing-zeros rule whenever the input is a whole number of milliseconds, so no bespoke
formatter was needed — `instantToISO8601`/`iso8601ToInstant` in `pkg/model/metadata.go`, round-trip
covered by `TestSyncMetadataInstantRoundTrip_NoDrift`.

**The remaining fields mapped cleanly, nothing was left unset by guesswork.** `sourceIdentifier`
turned out to already exist in `model.Snapshot`/`model.TableChange` under that exact name and
meaning (the source-side commit version/instant) — Java's `TableSyncMetadata.sourceIdentifier` is
the same value, just also carried into the sync watermark; `model.TableSyncMetadata` grew a field
for it. `instantsToConsiderForNextSync` has no polytable equivalent (no pending-commit tracking
exists yet) and is honestly written as an always-present empty JSON array, matching the fixture and
avoiding a null Java's own `TableFormatSync.isChangeApplicableForLastSyncMetadata` would NPE on.
`version` is written as the constant `0`, matching Java's `CURRENT_VERSION`, and is read but not
acted on.

**Malformed `XTABLE_METADATA` falls back to the flat keys** (or to "no metadata" if those are absent
too) rather than failing the sync — a corrupted copy of the optional, richer property should not be
worse than never having written it, and `pkg/conversion/controller.go` already refuses to trust a
non-positive `LastInstantSynced`, so this can never manufacture a zero-instant incremental resume.

**Read-path differences across targets, discovered rather than assumed:** Delta, Iceberg and Paimon
all keep sync metadata in ordinary table properties, but Hudi does not — Java's own
`HudiConversionTarget#getExtraMetadata` writes `XTABLE_METADATA` into the *commit file's*
`extraMetadata`, not into `hoodie.properties`, and reads it back from there. polytable's Hudi target
previously read only `hoodie.properties`. `GetTableMetadata` now checks the latest commit's
`extraMetadata` first (where a real Java-Hudi-synced table's state actually lives) and falls back to
`hoodie.properties` (polytable's own convenience copy, written alongside on every commit). Parquet
has no equivalent at all: Java XTable does not target Parquet, and polytable's Parquet target
already serializes the whole `TableSyncMetadata` struct as its own JSON file rather than flat table
properties, so `XTABLE_METADATA` does not apply there and was left untouched.

**Verified against the live fixture**, not only inferred from the Java source: fetched
`abfss://xtable-pr897@polytable410464.dfs.core.windows.net/src/delta_small/metadata/v3.metadata.json`
directly (`az storage fs file download`) and confirmed its `properties` block matches this task's
evidence exactly, byte for byte. That block, trimmed to the properties relevant here, is committed
as `pkg/model/testdata/java_xtable_iceberg_properties.json` and exercised by
`TestReadSyncMetadataFromProperties_JavaFixture` and `TestParseXTableMetadataJSON_RealJavaSample`, so
the acceptance criterion is pinned by a committed fixture rather than the live copy. A synthetic
end-to-end case — one Delta commit synced to Iceberg, its target metadata then rewritten to carry
only `XTABLE_METADATA` (no flat keys at all) — reports `NO_OP` on the next sync
(`TestController_RecognizesJavaOnlyShapedSyncState`, `pkg/conversion/t60_test.go`).

**Not verified: the reverse direction.** Java recognizing a polytable-synced table still needs the
JVM lane in T30; this task only confirmed polytable reads Java's shape, and that Java's own round
trip on its own tables works (a third sync of an unchanged source produced no new metadata version).

---

## T61 — The catalog's `namespace-separator` is ignored

Found by running Apache Polaris rather than reading about it. Its `GET /v1/config` returns:

```json
"overrides": {"namespace-separator": "%1F", "prefix": "pt_catalog"}
```

`%1F` is the ASCII unit separator, URL-encoded. Polaris is telling clients that a multi-level
namespace must be joined with that character when it appears in a path, because a dot is a legal
character inside a namespace level and cannot be the separator.

polytable reads `prefix` out of that object and nothing else (`pkg/catalog/rest_config.go`), and
passes the namespace to `path()` as one already-joined string. For a single-level namespace — every
case the tests and the container suites cover — that is indistinguishable from correct. For
`a.b.c` it is wrong in a way that produces a confusing 404 rather than an error naming the cause.

**Scope.** Read `overrides["namespace-separator"]`, default to the specification's `%1F` when a
catalog does not say, and join multi-level namespaces with it. The awkward part is upstream of that:
`catalog.Config.DatabaseName` and `TableIdentifier.Database` are single strings, so polytable has no
representation of a multi-level namespace to join in the first place. Decide whether the identifier
type grows levels or whether a configured `a.b.c` is split on the dot at the edge — the second is
cheaper and wrong for a namespace level containing a dot, which is exactly what the separator exists
to allow.

**Acceptance:** a two-level namespace addresses correctly against a real Polaris container; a
catalog that advertises no separator behaves as today; and a namespace level containing a dot either
works or is refused with a message naming the limitation, not silently mis-addressed.

**Commit:** `fix: join multi-level namespaces with the catalog's namespace-separator`

---

## T62 — Google Cloud Storage backend, and the BigLake catalog behind it

Unscheduled until 2026-08-22 on the stated grounds that nobody had asked. Someone has: an account
is available, which is what the earlier reasoning was waiting for.

**Nothing exists today.** `gs://` is in `uriSchemes` (`pkg/io/storage.go:48`) so the path helpers
treat it as a scheme and foreign metadata carrying such paths round-trips, but
`NewStorageForPathWithOptions` refuses it and there is no backend file and no dependency. That
refusal is deliberate and must stay until a backend exists: falling through to local storage would
read `gs://bucket/table` as a relative directory and create a literal `gs:` directory on first write.

**Two distinct pieces, and they are worth separating.**

1. **The storage backend.** ✅ **Done**, on `google.golang.org/api/storage/v1` rather than the
   client named below -- see the outcome at the end of this task. The original plan read:
   `cloud.google.com/go/storage`, shaped like `pkg/io/azure.go`: an
   `Options` field alongside `S3` and `Azure`, credentials from the environment and the Application
   Default Credentials chain rather than config fields, and the same `//go:build !js` split with a
   stub returning an `ErrGCSUnsupported`. Measure the binary cost the way T51 did — Azure added
   3.4 MiB — and record it, because a small static binary is a stated differentiator.
2. **BigLake Metastore / BigQuery's Iceberg REST catalog.** A fourth managed catalog after Glue,
   Iceberg REST and OneLake. Its auth is a Google OAuth2 token, which is neither of the two modes
   `restHTTPClient` supports, so it lands on T59 rather than being free. Verify the endpoint and its
   `/v1/config` prefix shape against the live service rather than the documentation — that habit has
   already found four bugs the docs would not have.

**Why it is worth doing beyond parity.** Upstream's documented end-to-end flows include GCS with
BigLake; the PR #897 review enumerated six, and GCS is one of the two polytable cannot run at all.
Closing it makes the three-cloud story complete.

**Acceptance:** a table on `gs://` syncs and reads back, exercised against a real bucket the way
T51's Azure lane is; `GOOS=js GOARCH=wasm go list -deps ./cmd/polytable-wasm` still reports zero
cloud SDK packages; the binary-size delta is recorded; and the BigLake half either works or is
explicitly deferred to T59 with the reason.

**Outcome (storage half).** Implemented, but not on the client this task first named. The obvious
choice, `cloud.google.com/go/storage`, cost **+20.16 MiB** on `cmd/polytable` (36.37 → 56.52 MiB,
`-ldflags="-s -w"`) — against Azure's +3.4 MiB, and a 55% increase in a binary whose size is a
stated differentiator. It links gRPC, OpenTelemetry, `genproto` and `s2a-go` unconditionally even
though only the JSON/HTTP transport is ever used.

Rebuilt on `google.golang.org/api/storage/v1`, the generated JSON client: **+4.80 MiB** (41.17 MiB),
a **15.35 MiB saving, 76% less**. Dependency count fell from 839 to 613 packages.

One thing worth knowing before someone tries to shave it further: gRPC and friends are **still in
`go.mod`**, because `google.golang.org/api/internal` imports them, and `go list -deps` still counts
114 such packages. The size went down anyway because nothing on the used code path calls into
gRPC's transport, so the linker strips it. **Pruning those indirect requirements will not buy
anything** — the residual 4.8 MiB is the REST API surface and the ADC/oauth2 stack.

`NewGCSStorageWithClient` changed shape with the client; its only caller was a test.

**Verified against real GCS**, not only a fake: with ADC from `gcloud auth application-default
login`, a write to a scratch prefix, `Exists`, a byte-identical read back, a `List` returning the
right size and path, a delete, and `Exists` false afterwards. No committed test depends on live GCS.

**Still open:** the BigLake catalog half, which needs OAuth2 (T59) and is not started.

**Commit:** `feat: add a Google Cloud Storage backend`

---

## T63 — `ParquetSchemaToModel` panics on an unexpected nested group ✅ COMPLETED

Found by T45's negative control, and it is worse than the bug T45 fixed.

Reverting T45's exclusion and re-running its acceptance test did not merely miscount a Delta
checkpoint as a seventh data file — `GetCurrentSnapshot` **panicked**, inside
`ParquetSchemaToModel`, on `parquet-go`'s `groupType.Kind` for the checkpoint's nested schema shape.

A hand-placed file of garbage bytes would have produced a clean `OpenFile` error and hidden this
entirely. It took a real delta-rs-written checkpoint — a shape this repository's own Delta target
never produces — to reach the panicking path, which is a good argument for T45's insistence that the
fixture be built by real writers rather than by hand.

**A library must not panic on input it merely failed to understand.** T45 stops the Parquet source
from *reaching* this file, so the specific route is closed, but the schema converter is still
reachable with any Parquet file whose nested group shape it does not expect — a user pointing the
Parquet source at a directory of genuinely odd Parquet takes the process down.

**Scope.** Audit `ParquetSchemaToModel` for every type assertion and interface conversion that can
fail on a well-formed-but-unexpected schema, and return an error naming the column and the shape
instead. Check whether `parquet-go` exposes a safe way to interrogate a group before calling `Kind`.
Decide whether a `recover` at the package boundary is warranted as a backstop, and record the
reasoning — a converted panic is better than a crash but worse than a real error, so it is a
fallback, not the fix.

**Acceptance:** feeding a Delta checkpoint Parquet file straight to the schema converter returns an
error naming the file and what it could not map, and does not panic; the existing Parquet fixtures
still convert unchanged; and a fuzz or table test covers at least the nested-group shape that
triggered this.

**Commit:** `fix: return an error instead of panicking on an unmappable Parquet schema`

### Outcome ✅

`parquet-go`'s own `Node` interface names the safe guard: `Type()` is documented as "Calling this
method on non-leaf nodes will panic", and `Leaf()` is what tells a caller whether that is true. The
package backs the doc with real panics — `type_group.go`, `type_list.go`, `type_map.go` and
`type_variant.go` all panic unconditionally out of `Kind()` — so the fix in
`pkg/formats/parquet/schema.go` is to never call `Field.Type()` (directly, or via `convertType`)
until `Field.Leaf()` has confirmed it is safe. `convertField` now has three cases instead of two:
a leaf column (repeated or not — a repeated leaf is the old two-level list encoding, and `Type()`
is safe for it), a non-repeated non-leaf group (an ordinary struct, recursed into as before), and a
repeated non-leaf group — the modern three-level LIST's `list` node, or a MAP's `key_value` node —
which is reported as an error naming the full dotted column path instead of guessed at, since
folding it into a plain struct would silently drop the list/map cardinality it represents.

A narrow `recover` wraps only `ParquetSchemaToModel` itself, not the rest of the package, as a
backstop for a parquet-go node type this audit did not find; every path the audit did find returns
its own named error rather than relying on it. `ParquetSchemaToModel` and its two internal helpers
now return `(*model.Schema, error)`, wrapping a new `ErrUnmappableSchema` sentinel; the one non-test
caller (`pkg/formats/parquet/source.go`) now names the file in the wrapping error, since the
converter itself only sees a `*parquet.Schema` and cannot.

`pkg/formats/parquet/schema_test.go`'s table drives four shapes: the real delta-rs checkpoint that
found this (`add.partitionValues.key_value`, a MAP), a hand-built `parquet.Map` and a hand-built
`parquet.List` (confirmed to reproduce the identical repeated/leaf shape a real
`GenericWriter`-written, `OpenFile`-reopened MAP/LIST column has, both erroring with the full
path), and a positive case (an untagged repeated leaf column, still converting to `model.TypeList`
as before). `TestParquetSchemaToModel_ExistingFixturesUnchanged` pins that a flat schema is
unaffected. Reverting the fix locally and rerunning the table reproduced the original panic
verbatim — same message, same `groupType.Kind` frame — confirming the test is load-bearing rather
than passing either way.

---

## T64 — Catalogs that vend storage credentials ⚠️ IMPLEMENTED, UNVERIFIED AGAINST SNOWFLAKE; NO CREDENTIAL REFRESH

Found by running Cloudflare R2's Data Catalog. Its `GET /v1/config` response carries
`overrides["s3.signer.uri"]`, and its advertised endpoints include
`GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials`.

R2 **vends per-table storage credentials through the catalog**. So does AWS S3 Tables, and the
Iceberg REST specification defines the mechanism generally: a client asks the catalog for the
table, and the catalog returns short-lived, scoped credentials for the storage under it, rather than
the client holding long-lived keys for the whole bucket.

polytable resolves storage entirely separately from the catalog. `Controller.createSource`
(`pkg/conversion/controller.go`) builds a `Storage` from the dataset's own `StorageConfig` and the
path scheme; nothing in `pkg/catalog` ever influences it. So a user pointing polytable at a
credential-vending catalog must independently configure credentials broad enough to cover every
table it might resolve — which is exactly what vending exists to avoid.

**This is a design gap, not a bug.** Nothing is broken today: supply the storage credentials
yourself and it works. What is missing is the safer path, and it becomes more pressing as managed
catalogs become the normal deployment.

**The shape of the work.** `catalog.SourceTable` would carry optional credentials, and the
conversion path would prefer them over the dataset's `StorageConfig` when present. That crosses a
boundary the code currently keeps clean — `pkg/catalog` has never influenced `pkg/io` — so the
decision is whether that separation is worth keeping. Note the credentials are short-lived, so
whatever carries them needs a refresh story, not a one-shot fetch, and `Storage` has no notion of
re-authenticating today.

**Related but distinct:** T59's SigV4 and OAuth2 work is about authenticating *to the catalog*. This
is about the catalog authenticating you *to the storage*. Do not conflate them.

**A note on the Fabric evidence, recorded because it was overstated.** This register and several
messages have said that Fabric's Iceberg responses carry `XTABLE_METADATA` with
`sourceTableFormat: DELTA`, described as observed. It was **read from Microsoft's documentation
sample** (`learn.microsoft.com/fabric/onelake/table-apis/iceberg-table-apis-get-started`), not from
a live Fabric call — no Fabric workspace has ever been reached from here, because the tenant has
Fabric disabled (T52). It is good evidence, but it is a documentation sample, and any argument
resting on it should say so.

### Observed, not inferred: Snowflake

Filed from reading R2's advertised endpoints. Snowflake then supplied the failure and the fix in one
response, on 2026-08-22 against a live account.

A Snowflake-managed Iceberg table resolves correctly through the catalog — `ListTables` finds it,
`GetSourceTable` returns `format=ICEBERG` with a base path in **Snowflake's own managed bucket**,
`s3://sfc-prod3-...-customer-interop-fs-.../iceberg/<db>/<schema>/<table>.<suffix>`. Nobody has
credentials for that bucket, and nobody can be given them: it is Snowflake's, not the customer's.

Pointing `polytable inspect` at that path fails:

```
failed to list s3 objects ...: api error PermanentRedirect: The bucket you are attempting to access
must be addressed using the specified endpoint.
```

Meanwhile `GET /v1/{prefix}/namespaces/{ns}/tables/{table}` with
`X-Iceberg-Access-Delegation: vended-credentials` returns exactly what is needed:

```
keys:        ["metadata-location", "metadata", "config", "storage-credentials"]
config keys: ["s3.access-key-id", "s3.secret-access-key", "s3.session-token",
              "expiration-time", "client.region"]
```

So this is not a convenience. **For a Snowflake-managed Iceberg table, catalog-vended credentials
are the only way to read the data at all** — there is no bucket-wide credential a user could
configure instead. `client.region` in that same response is also the answer to the 301 above, which
is a second reason the config block cannot be ignored.

**Acceptance:** a table resolved from a credential-vending catalog is readable using only the
credentials that catalog returned, with no bucket-wide credentials configured; the credentials
refresh before expiry on a long sync; and a catalog that vends nothing behaves exactly as today.
Verify against a Snowflake-managed Iceberg table, since that case cannot be faked with static
credentials — it is the one that proves the feature rather than exercising it.

### Outcome ⚠️ Implemented, and one bug only the live run could find

`GetSourceTable` now sends `X-Iceberg-Access-Delegation: vended-credentials`, parses both credential
shapes into a redacted `StorageCredentials` on `SourceTable`, and `S3Options` grew an optional static
provider that leaves the default chain untouched when unset.

**The bug: the two blocks are complementary, not alternatives.** The first implementation preferred
`storage-credentials` over `config` when both were present, which reads correctly and is wrong.
Snowflake returns:

```
top-level config:           client.region, expiration-time, s3.access-key-id,
                            s3.secret-access-key, s3.session-token
storage-credentials[0]:     prefix + s3.access-key-id, s3.secret-access-key,
                            s3.session-token, expiration-time      <- no client.region
```

The array scopes a credential to a prefix; the top-level config carries table-wide client settings.
Choosing the array therefore produced a credential with **no region**, and the `PermanentRedirect`
that the region exists to prevent — the exact failure T64 was filed to fix. The fields are now
merged, with a regression test built from the real response and confirmed to fail when reverted.

**Verified live.** With every AWS credential unset, `polytable sync` against a Snowflake-managed
table resolved through the catalog now reports `SUCCESS`, where the same table addressed by path in
the same shell fails with `PermanentRedirect`. That contrast is the control: the catalog path
supplies what the environment does not.

**Still unmet:** credentials are detected as expired, not refreshed within a single long sync — the
catalog source is closed before storage exists, so no refresh handle survives. The daemon
re-resolves per cycle, so this bites only a sync outliving one credential's lifetime. The
`storage-credentials` array shape remains partly spec-derived: Snowflake's is the only one observed.

**Commit:** `feat: use storage credentials vended by the catalog`

### Outcome ⚠️ — the read/parse/wire path landed; two acceptance criteria are open

**`pkg/catalog`.** `GetSourceTable` now sends `X-Iceberg-Access-Delegation: vended-credentials` on
every load-table request (`pkg/catalog/rest_conversion.go`) — unconditionally, since a catalog that
does not support it simply ignores an unrecognized header and returns exactly what it always has.
The response is parsed by `pkg/catalog/vended_credentials.go` into a new `StorageCredentials` struct
(access key, secret, session token, region, expiration — never a bare map), attached to
`SourceTable.StorageCredentials`. Both wire shapes are handled: the flat `config` map is the shape
**observed** against a live Snowflake catalog on 2026-08-22 (exactly the five keys the task names);
the `storage-credentials` array is the current specification's documented shape, **not** exercised
against a real server, and is preferred over `config` when both are present, picking the
longest-matching `prefix` per the specification. `StorageCredentials.String`, `GoString` and
`MarshalJSON` all redact unconditionally, covering `%v`/`%s`/`%+v`/`%#v` and `encoding/json`.

**`pkg/io`.** `S3Options` gained `Credentials *S3StaticCredentials` (access key, secret, session
token, expiration), wired to `credentials.NewStaticCredentialsProvider` in `NewS3Storage`. Nil
`Credentials` — the default, and every caller not using a vending catalog — changes nothing about
the existing `awsconfig.LoadDefaultConfig` chain.

**Wiring found a mismatch with this task's own framing, resolved in favor of what the code actually
does.** `Controller.createSource` does not build a `Storage` at all — it receives one already
constructed by the caller (`cmd/polytable/main.go`, `pkg/daemon/daemon.go`), the same way
`StorageConfig.ToOptionFuncs()` already worked. `catalog.SourceTable`'s `StorageCredentials` is
picked up by `ResolveSourceCatalog` into a new unexported `DatasetConfig.vendedCredentials` field —
unexported specifically so json/yaml marshaling of `DatasetConfig` skips it for free, matching
`StorageConfig`'s own documented policy against ever accepting credentials from a config file. A new
`DatasetConfig.StorageOptionFuncs()` combines `Storage.ToOptionFuncs()` with the vended credential
when present, and is what both `main.go` and `daemon.go` now call instead of
`ds.Storage.ToOptionFuncs()` directly. `DiscoverDatasets` carries the same field through for
catalog-discovered datasets. The vended region wins over a configured `StorageConfig.Region` when
both are present — the vended credentials are scoped to a specific bucket's region, and yielding to
a stale configured region would reproduce the exact `PermanentRedirect` this task exists to fix.
`pkg/daemon/server.go`'s REST path was left untouched: it never calls `ResolveSourceCatalog`, so
there was nothing to wire, and adding catalog support there is out of scope.

**Expiry: detection, not a refresh loop, and here is why.** `ResolveSourceCatalog` closes the
`ConversionSource` before returning (`defer src.Close()`), so no live catalog handle survives to the
point storage is actually used — a real refresh would mean keeping that source open for the sync's
lifetime and threading a re-fetching provider through `io.Options`, a structural change beyond this
task's scope. What landed instead: `NewS3Storage` rejects an already-expired-or-near-expired (one
minute clock-skew buffer) credential at construction, naming `io.ErrCredentialsExpired` rather than
proceeding; and every `S3Storage` method translates the AWS `ExpiredToken` / `ExpiredTokenException`
/ `RequestExpired` error codes into the same sentinel, so a sync that outlives its vended credential
mid-flight fails naming the cause instead of a generic S3 access error. This is the "detect and name
it" half of the acceptance criterion, not the "refresh before expiry" half — that remains open.

**What was checked**, all via `httptest` and testify table-driven tests, no live catalog or AWS
account involved: both wire shapes parse correctly, including the array shape's prefix-matching and
its precedence over the flat shape; the delegation header is sent unconditionally; a catalog vending
nothing yields a nil `*StorageCredentials` (not a zero-valued one) and `StorageOptionFuncs` then
produces output identical to `Storage.ToOptionFuncs()` alone; the region is carried through end to
end into `io.Options`; an expired or near-expired credential is rejected at `NewS3Storage`
construction, and a mid-sync `ExpiredToken`/`RequestExpired` response is translated to
`ErrCredentialsExpired` for `Read`, `List` and (by construction) every other `S3Storage` method; and
a `StorageCredentials` value resists leaking its secret through `%v`, `%s`, `%+v`, `%#v` and
`json.Marshal`, including when embedded in a `SourceTable`. `gofmt -l .`, `go vet ./...`,
`GOOS=js GOARCH=wasm go vet ./cmd/polytable-wasm`, `go test -short -race ./pkg/...`,
`go test -count=1 ./test/` and `golangci-lint run ./...` are all clean;
`GOOS=js GOARCH=wasm go list -deps ./cmd/polytable-wasm | grep -ci aws-sdk-go` is 0.

**Why this stays ⚠️ rather than ✅:**

1. **Not verified against a Snowflake-managed Iceberg table**, which the acceptance criterion calls
   out by name as "the one that proves the feature rather than exercising it." No Snowflake
   credentials were available here; everything above is a fake-server unit test.
2. **No refresh-before-expiry on a long sync.** The credentials are read once, at
   `ResolveSourceCatalog` time, and used for the rest of that `Sync` call. A sync that runs longer
   than the vended credential's lifetime now fails with a named `ErrCredentialsExpired` instead of an
   opaque AWS error, which is strictly better than before, but it is detection, not the refresh the
   acceptance criterion asks for.
3. **The expiration value's wire format is unconfirmed.** The task supplied the key name
   (`expiration-time`) observed against Snowflake, not the value's format. Both RFC3339 and integer
   epoch-milliseconds are accepted, in that order, and this is recorded in
   `pkg/catalog/vended_credentials.go` as an assumption rather than a verified fact.

---

## T65 — Iceberg format-version 3: unsupported, and not refused ✅ (cheap half only)

**No, polytable does not support Iceberg v3 — and the worse half is that it does not say so.**

Checked rather than assumed:

- The target writes v2, hardcoded: `icebergFormatVersion = 2` (`pkg/formats/iceberg/target.go:36`),
  stamped into the metadata and into every manifest and manifest list.
- The **source never reads `FormatVersion` at all.** `TableMetadata` parses the field
  (`pkg/formats/iceberg/metadata.go:86`) and nothing consults it. A grep for any rejection,
  unsupported-version error or comparison against 3 across the Iceberg adapter returns nothing.

So a v3 table is read as though it were v2. That is the same silent-wrongness this register keeps
finding: not a missing feature, a missing refusal. Compare T24, where a deletion vector is dropped
and the sync reports success.

**What v3 adds that v2 readers cannot fake.** Row lineage (`next-row-id`, per-row `_row_id` and
`_last_updated_sequence_number`), deletion vectors carried as Puffin blobs replacing positional
delete files, and new primitive types including variant, geometry and geography. A v2-shaped reader
meeting any of them either misreads or silently ignores it — and row lineage in particular is
metadata the whole point of which is that a consumer honours it.

**This is arriving, not hypothetical.** Snowflake's 2026 material describes v3 alongside Horizon
Catalog and managed storage as the new default direction. The Snowflake-managed table exercised on
2026-08-22 wrote `format-version: 2`, so there is time — but not much, and the default will move
without asking.

**The cheap half is worth doing immediately and separately from support.** Read `format-version`
and refuse anything above what the adapter implements, with an error naming the version and the
table. That is a small, self-contained change which converts a silent misread into a clear failure,
and it does not depend on deciding anything about v3 itself.

**The expensive half is a real decision.** Supporting v3 reading means handling Puffin deletion
vectors, which runs into the same INV-1 wall as T24: a Puffin blob is a bitmap over row positions,
and honouring it means knowing which rows those are. Do not start v3 support before T24's decision
is made, because they are the same question wearing different clothes.

**Acceptance:** a v3 table presented to the Iceberg source fails with an error naming the format
version, rather than being read as v2; the version the target writes stays configurable in one place
rather than spreading; and `SPEC.md` states which versions are supported for read and for write.

**Resolved (cheap half only).** `readMetadata` (`pkg/formats/iceberg/source.go`) — the single choke
point every read path (`GetCurrentTable`/`GetTable`, `GetCurrentSnapshot`, `GetTableChangeForCommit`,
`GetChangesSince`, `IsIncrementalSyncSafeFrom`) already passed through — now refuses a
`format-version` above `maxReadableFormatVersion` (declared next to `icebergFormatVersion` in
`target.go`, both `2` today) and refuses one that is missing, zero or negative, naming the version
found, the ceiling and the metadata file path in the error. Before this change a missing or
non-positive `format-version` decoded to Go's zero value and was silently read as if it were v2, the
same as a genuine v3 table. `SPEC.md` §6.2 now states the supported read/write versions. The
catalog-backed sources (`pkg/catalog`'s Glue and Iceberg REST `ConversionSource`s) resolve only a
base path and properties from `loadTable`/`GetTable`; the actual read goes through
`pkg/formats/registry.go`'s `iceberg.NewSource`, so it is the same gated `Source`, not a second
metadata parse — confirmed by grep, not inferred. Left deliberately untouched: `Target.CommitSnapshot`
already discards `readMetadata`'s error (`prevMeta, _ = ...`) when layering a new snapshot onto
existing metadata, which was already true for any unparseable metadata file before this change; the
practical effect on a v3-labelled directory is now that a commit proceeds as though starting fresh
(`prevMeta` nil, schema id resets to 0) rather than as though continuing a misread v2 table — neither
is correct, and fixing the target's error handling is outside a read-side refusal. `Target.GetTableMetadata`,
by contrast, already propagates the same error, so a sync's read-modify-write of `TableSyncMetadata`
against a v3 target is refused, not silently wrong. The expensive half — actually reading v3, which
needs a decision on Puffin deletion vectors shared with T24's INV-1 — is deliberately not attempted.

**Commit:** `fix: refuse an Iceberg table whose format version exceeds what the reader implements`

---

## T66 — Delta output was unreadable by delta-kernel-rs when unpartitioned ✅ COMPLETED

Found by converting a real Snowflake Iceberg table to Delta and reading the result with **DuckDB**,
not by any test in this repository.

polytable wrote `"partitionColumns": null` in the Delta `metaData` action for an unpartitioned
table. Go marshals a nil slice as `null`, and the field was declared `var partitionColumns []string`
and only ever appended to. The Delta protocol requires an array — empty when there are no partition
columns — and **delta-kernel-rs enforces it**, refusing the entire log:

```
DeltaKernel ArrowError (2): Json error: whilst decoding field 'metaData':
Encountered unmasked nulls in non-nullable StructArray child:
Field { "partitionColumns": List(non-null Utf8, field: 'element') }
```

So every unpartitioned Delta table polytable produced was unreadable by DuckDB, and by anything else
built on the kernel. polytable's own reader was untroubled, which is exactly why this survived.

**Why no test caught it.** Every Delta fixture in this repository is partitioned. A partitioned table
appends at least one column, so the slice is never nil, so the bug is invisible. The existing
`TestDelta_MetadataCarriesKernelRequiredKeys` already used an *unpartitioned* table and already
guarded two other kernel requirements — `"options":{}` and `"metadata":{}` — and simply did not
assert this one. The lesson is not "add a fixture" but that a foreign reader found in one command
what the suite could not.

**Fixed** by initialising both slices with `make([]string, 0, ...)` — the snapshot path and the
incremental path, which had the same declaration. Pinned by extending that existing test rather than
adding a new one, and **verified by reverting**: the test fails without the fix and passes with it.

**Verified end to end afterwards**: Snowflake wrote an Iceberg table to an external volume in the
user's own S3 bucket; polytable converted it to Delta; DuckDB's `delta_scan` read the result and
reported 6 rows, sum 212.0 and 4 distinct regions — identical to what Snowflake itself reports for
the source table.

**Commit:** `fix: write partitionColumns as an array, never null`

---

## T67 — Relativization does not know `.dfs.` and `.blob.` are the same store ✅ COMPLETED

Found by converting a Snowflake Iceberg table on Azure to Delta and reading the result with DuckDB.

Snowflake writes its Iceberg locations against the **blob** endpoint:
`azure://<account>.blob.core.windows.net/<container>/...`. The sync was addressed against the
**dfs** endpoint, `abfss://<container>@<account>.dfs.core.windows.net/...`, which is the same store —
ADLS Gen2 serves both, and `pkg/io/azure.go` itself swaps `.dfs.` for `.blob.` when building its
service URL, with a comment explaining they are the same account.

`RelativizePath` compares strings. The data file path starts `.blob.`, the table root starts
`.dfs.`, the prefix does not match, so nothing is stripped and the **absolute** URI is written into
the Delta log's `add.path`:

```
add.path = abfss://<container>@<account>.blob.core.windows.net/<prefix>/data/<file>.parquet
```

delta-kernel-rs then resolves it relative to the table root and asks for a path with the root
prepended to the absolute URI:

```
IO Error: ... open file 'abfss://<c>@<a>.dfs.core.windows.net/<prefix>/abfss://<c>@<a>.blob.core.windows.net/<prefix>/data/<file>.parquet'
BlobNotFound
```

So the table is unreadable by DuckDB, while polytable reads it back happily — the same shape as T66,
and found the same way.

**Note what is not claimed.** Whether the Delta protocol permits an absolute `add.path` at all is a
separate question this task does not need to answer: the file **is** under the table root, so a
relative path is both correct and what every writer emits. The defect is failing to recognise that.

**Scope.** Teach path relativization that a set of host spellings denote one store, at minimum
`.dfs.` and `.blob.` for `*.core.windows.net`, and the OneLake pair `onelake.dfs.fabric.microsoft.com`
and `onelake.blob.fabric.microsoft.com` that `pkg/io/azure.go` already documents. Check whether the
same class exists elsewhere: S3 virtual-hosted versus path-style, and `gs://` versus
`storage.googleapis.com`, are the obvious candidates and neither has been tested.

**Do not fix it by rewriting every path to one spelling on read.** That hides the mismatch rather
than resolving it and would rewrite paths a source deliberately wrote. Normalise for **comparison**
only.

**Acceptance:** an Iceberg table whose file locations use the blob endpoint, synced from a dfs base
path, produces a Delta log with relative `add.path` entries; DuckDB's `delta_scan` reads the result;
a table genuinely outside its root still gets an absolute path; and the equivalent S3 and GCS
spellings are either covered or explicitly recorded as untested.

### Outcome

**Confirmed ours, not the reader's**, by two checks rather than one. The same conversion on S3 wrote a
**relative** path (`data/<file>.parquet`) while Azure wrote an absolute one, from the same code — so
the absolute path was an inconsistency, not a choice. And **delta-rs 1.6.3**, the reference Rust
implementation, fails identically to DuckDB, joining the absolute URI onto the table root. Two
independent readers agreeing settled it without needing Spark.

Fixed by normalising the **authority only**, for the comparison only, so the stored path is never
rewritten and a directory spelled like an endpoint is untouched. Covers the Azure blob/dfs pair and
the OneLake pair that `pkg/io/azure.go` already documents.

**Verified end to end afterwards**: DuckDB reads 8 rows / sum 40.0 / 4 regions and delta-rs reads the
same, matching what Snowflake reports for the source table.

**The other backends were then tested too, and one was worse.** Each alias pair was run through
`RelativizePath` directly:

| Pair | Before |
| :--- | :--- |
| `s3a://` file under an `s3://` base, and the reverse | Already correct — both are in `uriSchemes` |
| `wasbs://` file under an `abfss://` base | Already correct |
| `gcs://` file under a `gs://` base | **Silently mangled** |
| `https://storage.googleapis.com/...`, `https://<bucket>.s3.<region>.amazonaws.com/...` | **Silently mangled** |

The `s3`/`s3a` result matters most in practice: Hadoop and Spark write `s3a://` while AWS tooling
writes `s3://`, so a Java-written table read against an `s3://` base lands there, and it works.

The mangling is worse than the Azure defect this task started from. `TrimScheme` reports no scheme
for a spelling it does not know, and the "no scheme means already relative" branch then returned the
whole string — after `path.Clean` had collapsed the `//` — so `gcs://b/t/f.parquet` came back as the
*relative path* `gcs:/b/t/f.parquet`. Azure at least produced a valid absolute URI that a reader
could fail on loudly; this is neither relative nor a URI, and nothing downstream can detect it.

**Reachable, not theoretical**: Snowflake writes external volume locations as `gcs://`. The GCS chain
verified above escaped only because Snowflake normalised it to `gs://` in the Iceberg metadata it
then wrote.

An unrecognised scheme is now **refused** rather than mistaken for a relative path, and every pair
above is pinned in `TestRelativizePath_SchemeAliasesAcrossBackends` — including the two that were
already correct, so the table reads as a record rather than only covering the fix.

**Commit:** `fix: treat dfs and blob endpoints as one store when relativizing`

---

## T68 — Iceberg incremental sync drops every previously live file ✅ COMPLETED

**The most serious defect found on 2026-08-22, and it is silent data loss on the main incremental
path.**

`Target.CommitChanges` (`pkg/formats/iceberg/target.go:342`) builds a snapshot from
`change.FilesDiff.FilesAdded` alone and hands it to `CommitSnapshot`, which writes a manifest
containing exactly those files. **The previous snapshot's still-live files are never carried
forward.** `FilesRemoved` is not consulted at all.

So after an incremental sync, the Iceberg table contains only the files added by the last commit.
Every row written by any earlier commit is gone from the table's view of itself. The sync reports
success.

**Demonstrated**: `TestSchemaEvolution_AddColumn` loses 2 of 4 rows across a two-commit incremental
boundary — a metadata-only schema change followed by a partial append.

**Why nothing caught it.** Every fixture in this repository until now was a single whole-table
rewrite, where the added files happen to be *all* the live files, so the bug is invisible by
coincidence rather than absent. The drop, widen and rename fixtures pass for exactly that reason and
prove nothing about this path.

**The fix is not to append to the previous manifest.** The new live set is the previous live set,
minus `FilesRemoved`, plus `FilesAdded` — which means the target must read its own previous snapshot
to know what was live, since `model.TableChange` carries a diff and not a full file list. That is
what an Iceberg writer does anyway, and `CommitSnapshot` already reads `prevMeta`.

**Watch for the interaction with T40.** The Iceberg *source* now walks snapshot history and emits one
change per snapshot; this is the *target* failing to accumulate them. A fix here should be checked
against a multi-commit source, not a single change.

**Also fix the swallowed error while here**: `CommitSnapshot` discards `readMetadata`'s error
(`prevMeta, _ = ...`), so a table it cannot read is treated as a table that does not exist and the
commit silently starts from version 0. T65 noted the same swallow from the other side.

**Acceptance:** a multi-commit incremental sync to Iceberg produces a table whose row count and
values match the source, verified by DuckDB rather than by polytable; a removal in `FilesRemoved`
disappears from the target; and the existing single-commit fixtures still pass.

**Fixed** in `pkg/formats/iceberg/target.go`. `CommitChanges` now reads the target's own current
live file set back via `Source.GetCurrentSnapshot` before each commit (`previousLiveFiles`), applies
that change's `FilesDiff` (`applyFilesDiff`: removals first, by `PhysicalPath`, then appends
`FilesAdded`, so a rewrite in place — the same path in both lists — ends up live exactly once with
the new metadata) and hands `CommitSnapshot` the resulting full set. `CommitSnapshot`'s own contract
— replace the live set outright with whatever `DataFiles` it is given — is unchanged, since
`incremental_test.go`'s `commitThreeSnapshots` and the single-commit fixtures both depend on it.

**A second defect surfaced while wiring this up and is fixed alongside it**: `CommitSnapshot`
derives `SnapshotID` from `time.Now().UnixMilli()`, and `CommitChanges` now commits several snapshots
back-to-back for one incoming batch (the T40 interaction this task called out) — two commits inside
the same millisecond collided on the same snapshot id, and `GetCurrentSnapshot`'s id lookup silently
resolved to the wrong (typically older) one, reintroducing the row loss this task exists to fix.
`CommitSnapshot` now bumps `snapshotID` past the highest id already recorded in `prevMeta.Snapshots`
when the clock does not separate them, so an incremental sync that lands several commits inside one
call is no longer clock-resolution-dependent for correctness. `TimestampMs` still reports the wall
clock; only the id is adjusted.

**The swallowed error is fixed, and both instances of it, not just the one named above.**
`CommitSnapshot` discarded the error from *both* `listMetadataFiles` and `readMetadata`; either one
failing was indistinguishable from "no table exists yet" and would have silently restarted version
numbering at 1 next to a metadata file the commit could not read — the exact T65 interaction this
task named (a v3-labelled table now fails a v2 writer's `readMetadata` outright). Both are now
propagated and abort the commit; a genuinely absent table is unaffected, since `listMetadataFiles`
reports that case as `(nil, nil, nil)`, not an error.

**Tests**: `pkg/formats/iceberg/carry_forward_test.go` adds five cases — multi-commit accumulation
with a metadata-only middle commit, a removal actually removing, a same-path rewrite ending up live
once with the new size, a single-commit sync staying unchanged, and an unreadable previous metadata
file aborting the commit rather than restarting at version 1 (confirmed by asserting no new
`v1.metadata.json` is written). `TestSchemaEvolution_AddColumn` in `test/schema_evolution_test.go`
now passes **without modification** — confirmed both ways, by reverting `target.go` alone and
re-running it red again. `go test -count=1 ./test/ -run Equivalence` (DuckDB reading polytable's
Iceberg output) passes. The only failure left in `go test -count=1 ./test/` is
`TestSchemaEvolution_ReorderColumns`, reproduced identically before this change (`schema.go`'s
positional field-id assignment, T69, explicitly out of this task's file scope).

**A second defect surfaced while fixing this, and it is worse than it sounds.** `CommitSnapshot`
derived `SnapshotID` from `time.Now().UnixMilli()`. Once `CommitChanges` began round-tripping through
the target after every commit, several commits landed inside one millisecond and **collided on the
same snapshot id**, resolving to the wrong snapshot on read-back and reintroducing the row loss this
task exists to fix. It is timing-dependent, so it would have appeared as a flaky test rather than a
bug. `CommitSnapshot` now allocates an id above the highest already in `prevMeta.Snapshots`.

**Commit:** `fix: carry forward live files across an Iceberg incremental commit`

---

## T69 — Iceberg field ids are assigned by position

`SchemaToIceberg` (`pkg/formats/iceberg/schema.go`) assigns `fieldID := nextID`, incrementing per
loop iteration, unless the source field already carries a positive `FieldID` — which a Delta source
never does, because Delta field identity here comes from the name rather than the column-mapping id
(T57).

So a **pure column reorder changes every field's Iceberg id**. Demonstrated by
`TestSchemaEvolution_ReorderColumns`: `id`, `region` and `amount` all take different ids across a
reorder that changed no data.

Field ids are the one thing an Iceberg consumer is entitled to rely on. A reader that maps by id —
which is the whole point of ids — silently reads the wrong column under the right name. **That is
worse than a hard failure**, because nothing surfaces: the query succeeds and the values are wrong.

**Related but distinct from T57.** T57 is about Delta field *identity* on the source side. This is
about *assignment* on the Iceberg target side, and it would still be wrong even if T57 were fixed,
for any source that does not supply ids.

**The fix needs a stable mapping**, most likely reading the previous Iceberg schema and reusing the
id already assigned to each field name, allocating new ids only for genuinely new fields — which is
what `last-column-id` in the metadata exists to support.

**Acceptance:** a reorder leaves every existing field's id unchanged; a new column gets a fresh id
above `last-column-id`; a dropped and re-added column does not silently inherit the old id.

**Commit:** `fix: keep Iceberg field ids stable across schema changes`

---

## T70 — Seven type-translation defects, six of them silent

Found by `test/torture_types_test.go` on 2026-08-22, whose fixtures put values on the boundaries
where translation breaks rather than in the middle where it does not. Five of these are pinned by
skipped tests rather than asserted as correct behaviour.

**1. A struct column silently discards the entire statistics blob.** The headline defect.
`StatsJSON.NullCount` is `map[string]int64` (`pkg/formats/delta`), but delta-rs legitimately nests a
struct column's null count as a JSON *object*. `json.Unmarshal` fails, and `convertAddAction`'s
`if err == nil` then drops **all** stats — `RecordCount` and every column's bounds, not just the
struct's — with nothing surfaced. Any Delta table containing a struct loses its record counts.

**2. A null partition value is indistinguishable from an empty one.**
`AddAction.PartitionValues` is `map[string]string`, so JSON `null` and `""` both decode to `""`.
This is upstream's #828 family. Iceberg's Avro-decoded partition record does not have the problem.

**3. `fixed[N]` narrows to string silently.** `parseIcebergType` has no case for it, and the
mistake cascades: `DecodeBound` then decodes the bound as a Go string rather than `[]byte`.

**4. Decimal bounds are dropped on read and write.** `EncodeBound`/`DecodeBound` have no `DECIMAL`
case — the code comment already said so. Nothing is surfaced in the sync result.

**5. An empty bound is indistinguishable from a missing one.** `DecodeBound` returns early on
`len(raw) == 0`, so a genuinely empty string or bytes minimum is lost.

**6. Hudi's writer and its own reader disagree.** The writer emits the Avro logical type
`local-timestamp-micros` for `TIMESTAMP_NTZ`; its reader has no case for it and narrows to `STRING`.
A self-inconsistent round trip, which is the one class a self-test *should* have caught.

**7. Paimon narrows structures and zone-awareness.** `modelTypeToPaimonType` has no `TypeRecord`
case; `parsePaimonType` cannot parse `ARRAY<` or `MAP<`, so it narrows on read even where the write
was right; and `TIMESTAMP` and `TIMESTAMP_NTZ` both collapse to `TIMESTAMP(6)`.

**Not defects, recorded so they are not re-investigated.** Iceberg `UUID` → Delta `STRING` is an
explicit, defensible mapping rather than the default fallback. The Parquet schema reader *correctly*
errors by name on LIST/MAP-shaped nesting rather than narrowing.

**Writer limitations polytable cannot recover**, observed and recorded in the fixture manifests:
delta-rs clamps `decimal(38,0)` stats to int64 range and collapses `decimal(38,37)` to a lossy
float; it computes no stats at all for binary, list or map columns; pyiceberg 0.11.1 cannot write
format-version 3, so nanosecond timestamps are untestable with it; delta-rs accepts `timestamp[ns]`
and silently truncates to microseconds.

**Order to fix.** (1) first — it is silent loss of record counts on an ordinary schema. Then (6),
because a format disagreeing with itself needs no foreign reader to be wrong. Then (2), which has an
upstream issue to match against. (3), (4) and (5) are narrower and share the `DecodeBound` seam, so
they are one change.

**Acceptance:** each skipped test in `test/torture_types_test.go` is unskipped and passes, or is
recorded here as a genuine format limitation with the reason.

---

## Non-goals

- **Renaming stuttering identifiers** (`delta.DeltaCommit` → `Commit`, `catalog.CatalogType` → `Type`).
  `revive`'s stuttering check is deliberately disabled in `.golangci.yml`. This is a breaking public API
  change and needs a maintainer decision, not a lint-driven sweep.
- **Fixing the three known model defects** (`ParseTableFormat` mixed case, `DiffFiles` map ordering and
  `PhysicalPath`-only keying, `FieldByPath` case-insensitivity). They are documented in `CLAUDE.md`;
  fix them when touching the surrounding code, not as a campaign.
- ~~**GCS/Azure storage backends.**~~ — **withdrawn 2026-08-22 by maintainer decision.** The
  condition this non-goal set has been met: T3's storage-options gap was closed by T12, so a new
  backend now has a configuration path to hang off. Azure is scheduled as **T51** (ADLS Gen2 and
  OneLake storage) and **T52** (the OneLake catalog), and Azure support is a stated requirement
  rather than an extension. GCS was left unscheduled on the grounds that nobody had asked for it;
  that stopped being true on 2026-08-22 when an account was offered, and it is now **T62**.
- **Changing `go.mod`'s Go directive.**
