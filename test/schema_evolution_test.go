// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Schema-evolution and deletion torture tests for incremental sync.
//
// Every fixture here is a multi-commit Delta table written by delta-rs
// (test/fixtures/generate_evolution.py); polytable has never touched any of these directories. The
// point is not "can the source read the current snapshot" -- test/foreign_fixtures_test.go already
// covers that -- it is "does a real incremental sync, resumed at a real prior commit through
// conversion.Controller, carry a schema change or a deletion across the boundary correctly". Every
// assertion is against the fixture's own manifest.json, generated independently in Python, never
// against a re-read of polytable's own output.
package test_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// evolutionFixtureRoot is separate from foreign_fixtures_test.go's fixtureRoot only in name; both
// constants point at the same directory. Kept distinct so this file has no compile-time dependency
// on a name defined elsewhere, even though the value is identical.
const evolutionFixtureRoot = "testdata/fixtures"

// evolutionCommit is one _delta_log commit, read straight out of the log by
// generate_evolution.py's own _delta_commits(), independently of generate.py's fixtureCommit.
type evolutionCommit struct {
	Version       int      `json:"version"`
	Operation     string   `json:"operation"`
	Timestamp     int64    `json:"timestamp"`
	Added         []string `json:"added"`
	Removed       []string `json:"removed"`
	SchemaColumns []string `json:"schema_columns"`
}

// evolutionField mirrors one entry of manifest.json's "schema" array.
type evolutionField struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// evolutionDataFile mirrors one entry of manifest.json's "data_files" array.
type evolutionDataFile struct {
	Path            string            `json:"path"`
	RecordCount     int64             `json:"record_count"`
	SizeBytes       int64             `json:"size_bytes"`
	PartitionValues map[string]string `json:"partition_values"`
}

// evolutionNullFile mirrors generate_add_column_null's per-file null-count record.
type evolutionNullFile struct {
	Path        string `json:"path"`
	RecordCount int64  `json:"record_count"`
	NullCount   int64  `json:"null_count"`
}

// evolutionManifest is the manifest.json shape every fixture generate_evolution.py writes shares.
// Each generator populates only the fields relevant to its own scenario; the rest are the zero
// value. Deliberately distinct from foreign_fixtures_test.go's fixtureManifest (which this file
// cannot add fields to, since that struct is defined in a file this task does not own) even where
// the two overlap.
type evolutionManifest struct {
	Format        string              `json:"format"`
	TableName     string              `json:"table_name"`
	TableDir      string              `json:"table_dir"`
	CommitCount   int                 `json:"commit_count"`
	TotalRows     int64               `json:"total_rows"`
	DataFileCount int                 `json:"data_file_count"`
	Schema        []evolutionField    `json:"schema"`
	DataFiles     []evolutionDataFile `json:"data_files"`
	Commits       []evolutionCommit   `json:"commits"`

	AddedColumn        string `json:"added_column"`
	SchemaChangeCommit string `json:"schema_change_commit"`
	PopulatedCommit    string `json:"populated_commit"`

	NullPopulatedCommit string              `json:"null_populated_commit"`
	NullFiles           []evolutionNullFile `json:"null_files"`
	TableNullCount      int64               `json:"table_null_count"`

	DroppedColumn string   `json:"dropped_column"`
	DropCommit    string   `json:"drop_commit"`
	ColumnsBefore []string `json:"columns_before"`
	ColumnsAfter  []string `json:"columns_after"`

	OldName      string `json:"old_name"`
	NewName      string `json:"new_name"`
	RenameCommit string `json:"rename_commit"`

	WidenCommit string `json:"widen_commit"`

	ReorderCommit string `json:"reorder_commit"`

	DeleteCommit     string `json:"delete_commit"`
	CompactionCommit string `json:"compaction_commit"`

	DrainCommits       []string `json:"drain_commits"`
	FinalRemovalCommit string   `json:"final_removal_commit"`
	DrainedPartition   string   `json:"drained_partition"`

	ErasedCommitTimestampMs        int64 `json:"erased_commit_timestamp_ms"`
	RetainedFirstCommitTimestampMs int64 `json:"retained_first_commit_timestamp_ms"`
	FirstSurvivingVersion          int   `json:"first_surviving_version"`
}

// loadEvolutionFixture copies a fixture's table directory into a temporary directory and returns
// the copy's path together with its manifest. Every fixture here is Delta, written by delta-rs
// with only relative paths recorded, so -- unlike foreign_fixtures_test.go's loadFixture -- no
// placeholder rewriting is needed: nothing inside the copied directory records its own location.
func loadEvolutionFixture(t *testing.T, name string) (string, *evolutionManifest) {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(evolutionFixtureRoot, name, "manifest.json"))
	require.NoError(t, err)

	var manifest evolutionManifest
	require.NoError(t, json.Unmarshal(raw, &manifest))
	require.NotEmpty(t, manifest.TableDir, "manifest is missing table_dir")

	source := filepath.Join(evolutionFixtureRoot, name, manifest.TableDir)
	dest := filepath.Join(t.TempDir(), manifest.TableDir)
	require.NoError(t, os.CopyFS(dest, os.DirFS(source)))
	return dest, &manifest
}

// deltaLogVersionPattern matches a Delta commit's JSON log file name.
var deltaLogVersionPattern = regexp.MustCompile(`^(\d{20})\.json$`)

// truncateLogAfter moves every _delta_log commit whose version is greater than keepThrough out of
// tableDir, so a source reading tableDir sees only the log as it stood at keepThrough. It returns a
// restore function that moves every held-out commit back, simulating the commits "arriving" as a
// later incremental sync's backlog.
//
// This is the mechanism every boundary test in this file uses to exercise a real incremental sync
// through conversion.Controller, rather than asserting only against Source.GetChangesSince(ctx, 0)
// -- which test/foreign_fixtures_test.go already does for the sibling delta-rs fixtures. A resumed
// sync is what the task under test actually is.
func truncateLogAfter(t *testing.T, tableDir string, keepThrough int) func() {
	t.Helper()

	logDir := filepath.Join(tableDir, "_delta_log")
	entries, err := os.ReadDir(logDir)
	require.NoError(t, err)

	holding := t.TempDir()
	var moved []string
	for _, entry := range entries {
		match := deltaLogVersionPattern.FindStringSubmatch(entry.Name())
		if match == nil {
			continue
		}
		version, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		if version <= keepThrough {
			continue
		}
		require.NoError(t, os.Rename(filepath.Join(logDir, entry.Name()), filepath.Join(holding, entry.Name())))
		moved = append(moved, entry.Name())
	}
	require.NotEmpty(t, moved, "truncateLogAfter(%d) moved nothing out of %s; the split point does not precede the fixture's last commit", keepThrough, tableDir)

	return func() {
		for _, name := range moved {
			require.NoError(t, os.Rename(filepath.Join(holding, name), filepath.Join(logDir, name)))
		}
	}
}

// syncOnce runs a single-target Controller.Sync and returns that target's result.
func syncOnce(t *testing.T, ctx context.Context, storage io.Storage, tableDir, tableName string, target model.TableFormat) *spi.SyncResult {
	t.Helper()

	results, err := conversion.NewController(storage).Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{target},
		TableBasePath: tableDir,
		TableName:     tableName,
	})
	require.NoError(t, err)
	result, ok := results[target]
	require.True(t, ok, "controller reported no result for %s", target)
	return result
}

// syncAcrossBoundary drives exactly the shape every schema-evolution and deletion fixture in this
// file needs: a real first sync that only sees the log through keepThrough, then a real second,
// incremental sync once the rest of the log is restored. Both results are returned so a caller can
// assert on either leg -- in particular, that the second leg is a genuine incremental sync
// (FellBackToFullSync == false) and not a silent fallback wearing a SUCCESS.
func syncAcrossBoundary(t *testing.T, ctx context.Context, storage io.Storage, tableDir, tableName string, keepThrough int, target model.TableFormat) (first, second *spi.SyncResult) {
	t.Helper()

	restore := truncateLogAfter(t, tableDir, keepThrough)
	first = syncOnce(t, ctx, storage, tableDir, tableName, target)
	require.Equal(t, spi.SyncStatusSuccess, first.StatusCode, "first (pre-boundary) sync: %s", first.Error)

	restore()
	second = syncOnce(t, ctx, storage, tableDir, tableName, target)
	require.Equal(t, spi.SyncStatusSuccess, second.StatusCode, "second (incremental) sync: %s", second.Error)
	return first, second
}

// rawIcebergSchemaField and rawIcebergMetadata decode just enough of an Iceberg metadata.json to
// recover the name-to-field-id mapping of its current schema, by parsing the file directly with
// encoding/json rather than through pkg/formats/iceberg. This keeps the field-id-stability
// assertions below semi-foreign: they check what polytable's Iceberg *target* actually wrote on
// disk, not what polytable's own Iceberg *source* reads back from it.
type rawIcebergSchemaField struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type rawIcebergSchema struct {
	SchemaID int                     `json:"schema-id"`
	Fields   []rawIcebergSchemaField `json:"fields"`
}

type rawIcebergMetadata struct {
	CurrentSchemaID int                `json:"current-schema-id"`
	Schemas         []rawIcebergSchema `json:"schemas"`
}

// latestIcebergMetadataVersion reads metadata/version-hint.text and returns the version number it
// names.
func latestIcebergMetadataVersion(t *testing.T, tableDir string) int {
	t.Helper()

	hint, err := os.ReadFile(filepath.Join(tableDir, "metadata", "version-hint.text"))
	require.NoError(t, err)
	version, err := strconv.Atoi(string(hint))
	require.NoError(t, err)
	return version
}

// icebergMetadataVersionCount counts how many v*.metadata.json files an Iceberg target has
// written -- one per CommitSnapshot call, and so, for an incremental replay, one per TableChange
// CommitChanges was handed.
func icebergMetadataVersionCount(t *testing.T, tableDir string) int {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(tableDir, "metadata", "v*.metadata.json"))
	require.NoError(t, err)
	return len(matches)
}

// icebergFieldIDsAtVersion reads a specific Iceberg metadata version directly and returns its
// current schema's name-to-field-id map.
func icebergFieldIDsAtVersion(t *testing.T, tableDir string, version int) map[string]int {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(tableDir, "metadata", "v"+strconv.Itoa(version)+".metadata.json"))
	require.NoError(t, err)

	var meta rawIcebergMetadata
	require.NoError(t, json.Unmarshal(raw, &meta))

	ids := make(map[string]int)
	for _, schema := range meta.Schemas {
		if schema.SchemaID != meta.CurrentSchemaID {
			continue
		}
		for _, field := range schema.Fields {
			ids[field.Name] = field.ID
		}
	}
	require.NotEmpty(t, ids, "no current-schema-id %d found among %d schemas in v%d.metadata.json", meta.CurrentSchemaID, len(meta.Schemas), version)
	return ids
}

// icebergCurrentFieldIDs reads the Iceberg target's latest metadata.json directly and returns its
// current schema's name-to-field-id map.
func icebergCurrentFieldIDs(t *testing.T, tableDir string) map[string]int {
	t.Helper()
	return icebergFieldIDsAtVersion(t, tableDir, latestIcebergMetadataVersion(t, tableDir))
}

// sortedKeys is a small assertion-message helper.
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// commitByVersion finds the fixture's own record of a given Delta commit version.
func commitByVersion(t *testing.T, commits []evolutionCommit, version string) evolutionCommit {
	t.Helper()

	v, err := strconv.Atoi(version)
	require.NoError(t, err)
	for _, c := range commits {
		if c.Version == v {
			return c
		}
	}
	t.Fatalf("no commit recorded for version %s", version)
	return evolutionCommit{}
}

// --------------------------------------------------------------------- schema evolution scenarios

// TestSchemaEvolution_AddColumn: a nullable column added mid-history, in its own metadata-only
// commit, then populated with real values in the next. Both commits land after the boundary in one
// incremental sync, so this doubles as the "several commits in one pass" check for Delta -> Iceberg:
// one target metadata version must be written per source commit, not one for the pair.
func TestSchemaEvolution_AddColumn(t *testing.T) {
	ctx := context.Background()
	tableDir, manifest := loadEvolutionFixture(t, "evolution-add-column")
	storage := io.NewLocalStorage()

	splitVersion := 0 // the fixture's commit 0, i.e. everything before the schema change
	_, second := syncAcrossBoundary(t, ctx, storage, tableDir, manifest.TableName, splitVersion, model.TableFormatIceberg)
	assert.False(t, second.FellBackToFullSync, "incremental sync fell back to a full sync: %s", second.FallbackReason)
	assert.False(t, second.NoOp, "incremental sync reported no-op despite two pending commits")

	idsBeforeBoundary := icebergFieldIDsAtVersion(t, tableDir, 1) // written by the pre-boundary full sync
	idsAfterBoundary := icebergCurrentFieldIDs(t, tableDir)

	// Two source commits landed in the one incremental sync (the metadata-only ADD COLUMN and the
	// populating append): CommitChanges must write one Iceberg metadata version per source commit.
	assert.Equal(t, 3, icebergMetadataVersionCount(t, tableDir),
		"expected 3 Iceberg metadata versions total (1 full sync + 2 incremental commits)")

	require.Contains(t, idsAfterBoundary, manifest.AddedColumn, "the added column never reached the Iceberg target")
	for name, id := range idsBeforeBoundary {
		assert.Equal(t, id, idsAfterBoundary[name], "field %q's id drifted across the boundary (%v -> %v)",
			name, sortedKeys(idsBeforeBoundary), sortedKeys(idsAfterBoundary))
	}

	source, err := formats.NewSource(model.TableFormatIceberg, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	assert.NotNil(t, table.ReadSchema.FieldByPath(manifest.AddedColumn), "the Iceberg target's current schema is missing %q", manifest.AddedColumn)

	// This is the point the fixture exists for: the Iceberg target's CommitChanges builds each
	// commit's manifest purely from that change's own FilesDiff.FilesAdded and writes a manifest
	// list containing only that one new manifest (pkg/formats/iceberg/target.go, CommitChanges and
	// CommitSnapshot), never folding in the previous snapshot's still-live files. The ADD COLUMN
	// commit here adds zero files, so if this holds the current snapshot ends up with the *populated*
	// commit's files only -- losing the two rows the first, full sync wrote -- rather than all four.
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	var total int64
	for _, f := range snapshot.DataFiles {
		total += f.RecordCount
	}
	assert.Equal(t, manifest.TotalRows, total,
		"iceberg target lost rows across the incremental boundary: got %d of %d "+
			"(pkg/formats/iceberg/target.go CommitChanges does not accumulate prior live files)",
		total, manifest.TotalRows)
}

// TestSchemaEvolution_AddColumnNull mirrors TestSchemaEvolution_AddColumn but for the case where
// the rows written after the column exists never set it, leaving every one of them null -- the
// shape a producer that has not yet been updated to populate a new field takes in production.
func TestSchemaEvolution_AddColumnNull(t *testing.T) {
	ctx := context.Background()
	tableDir, manifest := loadEvolutionFixture(t, "evolution-add-column-null")
	storage := io.NewLocalStorage()
	require.NotEmpty(t, manifest.NullFiles, "manifest is missing null_files")

	splitVersion := 0
	_, second := syncAcrossBoundary(t, ctx, storage, tableDir, manifest.TableName, splitVersion, model.TableFormatIceberg)
	assert.False(t, second.FellBackToFullSync, "incremental sync fell back to a full sync: %s", second.FallbackReason)

	source, err := formats.NewSource(model.TableFormatDelta, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)

	byPath := relativeFilePaths(t, tableDir, snapshot.DataFiles)
	for _, expected := range manifest.NullFiles {
		actual, ok := byPath[expected.Path]
		require.True(t, ok, "delta source did not report %s", expected.Path)
		assert.Equal(t, expected.RecordCount, actual, "row count of %s", expected.Path)
	}

	byExpected := make(map[string]evolutionNullFile, len(manifest.NullFiles))
	for _, f := range manifest.NullFiles {
		byExpected[f.Path] = f
	}
	for path := range byPath {
		expected, ok := byExpected[path]
		if !ok {
			continue
		}
		var file *model.DataFile
		for _, f := range snapshot.DataFiles {
			if _, ok := relativeFilePaths(t, tableDir, []*model.DataFile{f})[path]; ok {
				file = f
				break
			}
		}
		require.NotNil(t, file)
		field := findFieldStat(file, manifest.AddedColumn)
		require.NotNil(t, field, "%s carries no column statistics for %q", path, manifest.AddedColumn)
		assert.Equal(t, expected.NullCount, field.NumNulls, "null count of %q in %s", manifest.AddedColumn, path)
	}
}

// findFieldStat locates a DataFile's ColumnStats entry for the named field, or nil.
func findFieldStat(file *model.DataFile, name string) *model.ColumnStat {
	for _, stat := range file.ColumnStats {
		if stat.Field != nil && stat.Field.Name == name {
			return stat
		}
	}
	return nil
}

// TestSchemaEvolution_DropColumn: the trailing column of the schema is dropped mid-history (see
// generate_drop_column's docstring for why it is the trailing column and not a middle one). The
// dropped column must not survive into the target's post-boundary schema.
func TestSchemaEvolution_DropColumn(t *testing.T) {
	ctx := context.Background()
	tableDir, manifest := loadEvolutionFixture(t, "evolution-drop-column")
	storage := io.NewLocalStorage()
	require.NotEmpty(t, manifest.DropCommit)

	splitVersion := 0
	_, second := syncAcrossBoundary(t, ctx, storage, tableDir, manifest.TableName, splitVersion, model.TableFormatIceberg)
	assert.False(t, second.FellBackToFullSync, "incremental sync fell back to a full sync: %s", second.FallbackReason)

	ids := icebergCurrentFieldIDs(t, tableDir)
	assert.NotContains(t, ids, manifest.DroppedColumn, "the dropped column %q still appears in the Iceberg target's schema", manifest.DroppedColumn)
	for _, survivor := range manifest.ColumnsAfter {
		assert.Contains(t, ids, survivor, "surviving column %q is missing from the Iceberg target's schema", survivor)
	}

	source, err := formats.NewSource(model.TableFormatIceberg, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	var total int64
	for _, f := range snapshot.DataFiles {
		total += f.RecordCount
	}
	assert.Equal(t, manifest.TotalRows, total, "row total across the drop-column boundary")
}

// TestSchemaEvolution_RenameColumn documents T57 (docs/improvement-plan.md) rather than asserting
// around it: deltalake==1.6.3 has no column-mapping support, so a Delta reader has no field
// identity to carry a rename across at all. What the log actually records, and what this asserts,
// is a plain remove-and-add: the renamed column reaches the target as a new field with a fresh
// identity, not silently misassociated with the old one's history. That is the honest, if lossy,
// outcome T57 describes -- this test is not "left red" because nothing here is happening silently:
// the old name disappears and a distinct new name appears, which is visible and correct as far as
// it goes.
func TestSchemaEvolution_RenameColumn(t *testing.T) {
	ctx := context.Background()
	tableDir, manifest := loadEvolutionFixture(t, "evolution-rename-column")
	storage := io.NewLocalStorage()
	require.NotEmpty(t, manifest.RenameCommit)

	// Ground truth from the fixture's own log: the rename commit is a plain file rewrite, not a
	// column-mapping-aware in-place update.
	renameCommit := commitByVersion(t, manifest.Commits, manifest.RenameCommit)
	require.NotEmpty(t, renameCommit.Added)
	require.NotEmpty(t, renameCommit.Removed)

	splitVersion := 0
	_, second := syncAcrossBoundary(t, ctx, storage, tableDir, manifest.TableName, splitVersion, model.TableFormatIceberg)
	assert.False(t, second.FellBackToFullSync, "incremental sync fell back to a full sync: %s", second.FallbackReason)

	ids := icebergCurrentFieldIDs(t, tableDir)
	assert.NotContains(t, ids, manifest.OldName,
		"T57: the old name %q is gone from the target's schema, as expected without Delta column mapping", manifest.OldName)
	assert.Contains(t, ids, manifest.NewName,
		"T57: the new name %q must appear, even though it carries a fresh field id with no link to %q's history",
		manifest.NewName, manifest.OldName)

	source, err := formats.NewSource(model.TableFormatDelta, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	assert.Nil(t, table.ReadSchema.FieldByPath(manifest.OldName))
	assert.NotNil(t, table.ReadSchema.FieldByPath(manifest.NewName))
}

// TestSchemaEvolution_WidenType covers int32 -> int64 and decimal(10,2) -> decimal(10,4), both
// widened in the same rewrite commit without moving either column's schema position (see
// generate_widen_type's docstring for why this writer version cannot do an in-place widen).
func TestSchemaEvolution_WidenType(t *testing.T) {
	ctx := context.Background()
	tableDir, manifest := loadEvolutionFixture(t, "evolution-widen-type")
	storage := io.NewLocalStorage()
	require.NotEmpty(t, manifest.WidenCommit)

	splitVersion := 0
	_, second := syncAcrossBoundary(t, ctx, storage, tableDir, manifest.TableName, splitVersion, model.TableFormatIceberg)
	assert.False(t, second.FellBackToFullSync, "incremental sync fell back to a full sync: %s", second.FallbackReason)

	source, err := formats.NewSource(model.TableFormatIceberg, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)

	quantity := table.ReadSchema.FieldByPath("quantity")
	require.NotNil(t, quantity)
	assert.Equal(t, model.Type("LONG"), quantity.Schema.DataType, "quantity did not widen to LONG")

	price := table.ReadSchema.FieldByPath("price")
	require.NotNil(t, price)
	assert.Equal(t, model.Type("DECIMAL"), price.Schema.DataType, "price is no longer reported as DECIMAL")
	scale, ok := price.Schema.Metadata[model.MetadataKeyDecimalScale].(int)
	require.True(t, ok, "price carries no DECIMAL_SCALE metadata")
	assert.Equal(t, 4, scale, "price's scale did not widen to 4")

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	var total int64
	for _, f := range snapshot.DataFiles {
		total += f.RecordCount
	}
	assert.Equal(t, manifest.TotalRows, total, "row total across the widen-type boundary")
}

// TestSchemaEvolution_ReorderColumns is the one the task calls sharp: (id, region, amount)
// reordered to (region, amount, id) with a new trailing column ('tax') added in the same commit
// (see generate_reorder_columns's docstring for why a pure reorder is invisible to a Delta reader
// at all at this writer version). pkg/formats/iceberg/schema.go's SchemaToIceberg assigns a field
// id purely by the current position in schema.Fields -- `fieldID := nextID` incrementing per loop
// iteration -- unless the source field already carries a non-nil, positive FieldID of its own.
// model.Field.FieldID is never populated for a Delta source (T57), so every field id the Iceberg
// target assigns is positional, every single commit, independent of any id a previous commit
// assigned the same-named column. A consumer that maps a data file's columns by id -- which is
// exactly what Iceberg is for -- gets 'region' under whatever id 'id' held a moment ago.
func TestSchemaEvolution_ReorderColumns(t *testing.T) {
	ctx := context.Background()
	tableDir, manifest := loadEvolutionFixture(t, "evolution-reorder-columns")
	storage := io.NewLocalStorage()
	require.NotEmpty(t, manifest.ReorderCommit)

	splitVersion := 0
	_, second := syncAcrossBoundary(t, ctx, storage, tableDir, manifest.TableName, splitVersion, model.TableFormatIceberg)
	assert.False(t, second.FellBackToFullSync, "incremental sync fell back to a full sync: %s", second.FallbackReason)

	idsPreBoundary := icebergFieldIDsAtVersion(t, tableDir, 1) // the pre-boundary full sync's own metadata version
	idsPostBoundary := icebergCurrentFieldIDs(t, tableDir)

	for _, name := range manifest.ColumnsBefore {
		require.Contains(t, idsPostBoundary, name, "column %q is missing after the boundary", name)
	}
	require.Contains(t, idsPostBoundary, manifest.AddedColumn, "the new trailing column never reached the target")

	// This is the defect: a consumer that trusts Iceberg field ids to identify a column regardless
	// of its position gets the wrong data under the right name once fields are reordered, because
	// SchemaToIceberg (pkg/formats/iceberg/schema.go) never consults the previous commit's schema.
	// Left failing on purpose per the task's own instruction: a bug found here must not be encoded
	// as the expected result.
	for _, name := range manifest.ColumnsBefore {
		assert.Equal(t, idsPreBoundary[name], idsPostBoundary[name],
			"pkg/formats/iceberg/schema.go SchemaToIceberg assigns field ids by position, not by "+
				"name: %q had id %d before the reorder and %d after, with no persisted "+
				"name-to-id mapping consulted across the incremental boundary",
			name, idsPreBoundary[name], idsPostBoundary[name])
	}
}

// --------------------------------------------------------------------------------- deletions

// TestDeletions_ThenCompact chains a row-level rewrite delete into a compaction on top of it: row
// total must survive both (8 written, 1 deleted, 7 live), and the compaction commit's removal must
// be reported even though it changes no data, only file layout.
func TestDeletions_ThenCompact(t *testing.T) {
	ctx := context.Background()
	tableDir, manifest := loadEvolutionFixture(t, "deletes-then-compact")
	storage := io.NewLocalStorage()
	require.NotEmpty(t, manifest.DeleteCommit)
	require.NotEmpty(t, manifest.CompactionCommit)

	deleteVersion, err := strconv.Atoi(manifest.DeleteCommit)
	require.NoError(t, err)

	splitVersion := deleteVersion - 1
	_, second := syncAcrossBoundary(t, ctx, storage, tableDir, manifest.TableName, splitVersion, model.TableFormatIceberg)
	assert.False(t, second.FellBackToFullSync, "incremental sync fell back to a full sync: %s", second.FallbackReason)

	source, err := formats.NewSource(model.TableFormatDelta, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, manifest.DataFileCount)
	var total int64
	for _, f := range snapshot.DataFiles {
		total += f.RecordCount
	}
	assert.Equal(t, manifest.TotalRows, total)

	changes, err := source.GetChangesSince(ctx, 0)
	require.NoError(t, err)
	compactionChange := changeForVersion(changes.TableChanges, manifest.CompactionCommit)
	require.NotNil(t, compactionChange, "no TableChange for the compaction commit")
	assert.NotEmpty(t, compactionChange.FilesDiff.FilesRemoved, "compaction commit reported no removal")
	assert.Len(t, compactionChange.FilesDiff.FilesAdded, 1, "compaction commit did not report the single merged file")
}

// TestDeletions_DrainPartition deletes a partition's rows one at a time rather than by a single
// aligned predicate: the first two deletes rewrite the file, and the third removes it outright with
// no replacement once nothing is left. Every one of the three delete commits must be reported, and
// the final one as a pure removal.
func TestDeletions_DrainPartition(t *testing.T) {
	ctx := context.Background()
	tableDir, manifest := loadEvolutionFixture(t, "deletes-drain-partition")
	storage := io.NewLocalStorage()
	require.Len(t, manifest.DrainCommits, 3, "manifest is missing drain_commits")
	require.NotEmpty(t, manifest.FinalRemovalCommit)

	firstDrainVersion, err := strconv.Atoi(manifest.DrainCommits[0])
	require.NoError(t, err)

	splitVersion := firstDrainVersion - 1
	_, second := syncAcrossBoundary(t, ctx, storage, tableDir, manifest.TableName, splitVersion, model.TableFormatIceberg)
	assert.False(t, second.FellBackToFullSync, "incremental sync fell back to a full sync: %s", second.FallbackReason)

	source, err := formats.NewSource(model.TableFormatDelta, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	changes, err := source.GetChangesSince(ctx, 0)
	require.NoError(t, err)
	for i, version := range manifest.DrainCommits {
		change := changeForVersion(changes.TableChanges, version)
		require.NotNil(t, change, "no TableChange for drain commit %s", version)
		assert.NotEmpty(t, change.FilesDiff.FilesRemoved, "drain commit %s (%d/%d) reported no removal", version, i+1, len(manifest.DrainCommits))
	}

	final := changeForVersion(changes.TableChanges, manifest.FinalRemovalCommit)
	require.NotNil(t, final)
	assert.NotEmpty(t, final.FilesDiff.FilesRemoved, "final drain commit reported no removal")
	assert.Empty(t, final.FilesDiff.FilesAdded, "final drain commit reported a replacement add it did not make")

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	for _, f := range snapshot.DataFiles {
		for _, pv := range f.PartitionValues {
			if pv.Range == nil {
				continue
			}
			assert.NotEqual(t, manifest.DrainedPartition, pv.Range.MinValue, "the drained partition still has a live file")
		}
	}
}

// --------------------------------------------------------------------------------- the boundary

// TestIncrementalBoundary_LogRetentionFallback: log retention erased the commit an incremental sync
// would need to resume from. IsIncrementalSyncSafeFrom must say so (false) rather than silently
// missing history, and it must say the opposite (true) from a genuinely retained instant --
// generate_log_retention_fallback captures both timestamps itself, from the log before cleanup
// erased the early commit, so this checks both sides of the same comparison rather than only the
// unsafe one.
func TestIncrementalBoundary_LogRetentionFallback(t *testing.T) {
	ctx := context.Background()
	tableDir, manifest := loadEvolutionFixture(t, "evolution-log-retention-fallback")
	require.Positive(t, manifest.ErasedCommitTimestampMs)
	require.Positive(t, manifest.RetainedFirstCommitTimestampMs)
	require.Greater(t, manifest.RetainedFirstCommitTimestampMs, manifest.ErasedCommitTimestampMs)

	source, err := formats.NewSource(model.TableFormatDelta, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	safeFromRetained, err := source.IsIncrementalSyncSafeFrom(ctx, manifest.RetainedFirstCommitTimestampMs)
	require.NoError(t, err)
	assert.True(t, safeFromRetained, "a genuinely retained instant must be reported safe")

	safeFromErased, err := source.IsIncrementalSyncSafeFrom(ctx, manifest.ErasedCommitTimestampMs)
	require.NoError(t, err)
	assert.False(t, safeFromErased, "an instant whose commit log retention erased must be reported unsafe")

	// The controller must not just detect this -- it must fall back to a full sync and report the
	// fallback (T40's FellBackToFullSync), rather than staying quiet about a partial history. A
	// target that already claims a prior sync at the erased instant is bootstrapped directly here,
	// via one real CommitSnapshot call, rather than performed by an earlier Controller.Sync: the
	// whole point of this fixture is that the pre-erasure state is gone, so there is nothing left to
	// resync from to produce that prior sync organically.
	storage := io.NewLocalStorage()
	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	bootstrapTable := *table
	bootstrapTable.LatestCommitTime = manifest.ErasedCommitTimestampMs

	target, err := formats.NewTarget(ctx, model.TableFormatIceberg, storage, tableDir, manifest.TableName)
	require.NoError(t, err)
	require.NoError(t, target.Init(ctx, &bootstrapTable))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{Table: &bootstrapTable, SourceIdentifier: "bootstrap"}))
	require.NoError(t, target.Close())

	result := syncOnce(t, ctx, storage, tableDir, manifest.TableName, model.TableFormatIceberg)
	require.Equal(t, spi.SyncStatusSuccess, result.StatusCode, result.Error)
	assert.True(t, result.FellBackToFullSync, "the controller must report falling back to a full sync")
	assert.NotEmpty(t, result.FallbackReason)

	finalSource, err := formats.NewSource(model.TableFormatIceberg, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = finalSource.Close() })
	snapshot, err := finalSource.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	var total int64
	for _, f := range snapshot.DataFiles {
		total += f.RecordCount
	}
	assert.Equal(t, manifest.TotalRows, total, "the fallback full sync must carry every row, not a partial history")
}
