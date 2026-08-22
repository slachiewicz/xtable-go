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

// Type-torture conversion tests: fixtures whose values sit on the boundaries where format
// translation actually breaks — decimal precision/scale, timestamp zone-awareness, floats that need
// every significant digit, strings and binary with escaping hazards, struct/list/map nesting with
// null at every level, and a null partition value.
//
// polytable never rewrites a data file: a conversion only ever changes metadata. So "the value
// survives" is checked on the three surfaces a conversion can actually touch — schema (type,
// precision/scale, nullability), partition values and column statistics — against the manifest
// test/fixtures/generate.py wrote from the values it fed the writer, never against a re-read of
// polytable's own output.
//
// This file's fixtures (delta-rs-torture, delta-rs-partition-torture, pyiceberg-torture,
// pyiceberg-partition-torture) are independent of foreign_fixtures_test.go's, though it shares that
// file's rewriteMetadataPaths and relocateAvroManifests helpers to relocate a copied Iceberg fixture.
package test_test

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

const tortureFixtureRoot = "testdata/fixtures"

// tortureTypeNode is one field of a torture fixture's recorded schema, recursively: RECORD carries
// Fields, LIST carries Element, MAP carries Key and Value.
type tortureTypeNode struct {
	Name      string            `json:"name,omitempty"`
	Type      string            `json:"type"`
	Nullable  bool              `json:"nullable"`
	Precision *int              `json:"precision,omitempty"`
	Scale     *int              `json:"scale,omitempty"`
	Size      *int              `json:"size,omitempty"`
	Fields    []tortureTypeNode `json:"fields,omitempty"`
	Element   *tortureTypeNode  `json:"element,omitempty"`
	Key       *tortureTypeNode  `json:"key,omitempty"`
	Value     *tortureTypeNode  `json:"value,omitempty"`
}

// tortureBound is one column's recorded bound in generate.py's column_bounds. Min/Max carry a
// JSON-native value (number, string or null) read the same way polytable's own reader decodes a
// bound; MinHex/MaxHex carry raw bytes for a column whose logical value is not JSON-representable
// (binary, fixed).
type tortureBound struct {
	Min    any    `json:"min,omitempty"`
	Max    any    `json:"max,omitempty"`
	MinHex string `json:"min_hex,omitempty"`
	MaxHex string `json:"max_hex,omitempty"`
}

// tortureRawBound is the exact lower/upper_bounds byte string an Iceberg manifest entry recorded for
// one field, hex-encoded, independent of any polytable decoding.
type tortureRawBound struct {
	LowerHex string `json:"lower_hex,omitempty"`
	UpperHex string `json:"upper_hex,omitempty"`
}

// tortureDataFile is one data file of a partition-torture fixture. PartitionValue is a pointer so
// that the JSON literal null this fixture exists to cover decodes to a nil pointer rather than to
// the zero value of string, which would erase exactly the distinction under test.
type tortureDataFile struct {
	PartitionValue *string `json:"partition_value"`
	Path           string  `json:"path,omitempty"`
	RecordCount    int64   `json:"record_count"`
}

// tortureManifest is the record generate.py's torture generators leave beside each fixture. The
// schema- and partition-torture fixtures each populate a different subset of these fields.
type tortureManifest struct {
	Format           string                     `json:"format"`
	TableName        string                     `json:"table_name"`
	TableDir         string                     `json:"table_dir"`
	PathPlaceholder  string                     `json:"path_placeholder"`
	ManifestEncoding string                     `json:"manifest_encoding"`
	TotalRows        int64                      `json:"total_rows"`
	DataFileCount    int                        `json:"data_file_count"`
	TypeSchema       []tortureTypeNode          `json:"type_schema"`
	ColumnBounds     map[string]tortureBound    `json:"column_bounds"`
	RawBounds        map[string]tortureRawBound `json:"raw_bounds"`
	PartitionColumns []string                   `json:"partition_columns"`
	DataFiles        []tortureDataFile          `json:"data_files"`
	Notes            []string                   `json:"notes"`
	Writer           struct {
		Library string `json:"library"`
		Version string `json:"version"`
	} `json:"writer"`
}

// loadTortureFixture copies a torture fixture's table directory into a temporary directory and
// returns the copy's path together with the manifest, exactly as foreign_fixtures_test.go's
// loadFixture does for the delete/compaction fixtures — including Iceberg's absolute-path relocation,
// via that file's own rewriteMetadataPaths and relocateAvroManifests.
func loadTortureFixture(t *testing.T, name string) (string, *tortureManifest) {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(tortureFixtureRoot, name, "manifest.json"))
	require.NoError(t, err)

	var manifest tortureManifest
	require.NoError(t, json.Unmarshal(raw, &manifest))
	require.NotEmpty(t, manifest.TableDir, "manifest is missing table_dir")

	source := filepath.Join(tortureFixtureRoot, name, manifest.TableDir)
	dest := filepath.Join(t.TempDir(), manifest.TableDir)
	require.NoError(t, os.CopyFS(dest, os.DirFS(source)))

	if manifest.PathPlaceholder != "" {
		rewriteMetadataPaths(t, dest, manifest.PathPlaceholder, "file://"+dest)
	}
	if manifest.ManifestEncoding == "avro" {
		relocateAvroManifests(t, dest)
	}
	return dest, &manifest
}

// assertTortureSchema recursively checks a read schema's fields against a torture fixture's recorded
// type_schema: the canonical data type, nullability at every nesting level, and — for DECIMAL — the
// precision and scale a reader that only checks the type name would miss entirely.
func assertTortureSchema(t *testing.T, expected []tortureTypeNode, fields []*model.Field, path string) {
	t.Helper()

	require.Len(t, fields, len(expected), "field count mismatch at %s", path)
	for i, node := range expected {
		field := fields[i]
		label := path + "." + field.Name
		if node.Name != "" {
			assert.Equal(t, node.Name, field.Name, "name at %s", label)
		}
		require.NotNil(t, field.Schema, "%s carries no schema", label)
		assert.Equal(t, model.Type(node.Type), field.Schema.DataType, "type at %s", label)
		assert.Equal(t, node.Nullable, field.Schema.IsNullable, "nullability at %s", label)
		assertTortureNode(t, node, field.Schema, label)
	}
}

// assertTortureNode checks the metadata and children a single node in a torture schema carries,
// dispatching on the shape (record/list/map/decimal/fixed) present in the expected node.
func assertTortureNode(t *testing.T, node tortureTypeNode, schema *model.Schema, label string) {
	t.Helper()

	if node.Precision != nil {
		precision, ok := schema.Metadata[model.MetadataKeyDecimalPrecision].(int)
		require.True(t, ok, "%s carries no decimal precision", label)
		assert.Equal(t, *node.Precision, precision, "precision at %s", label)
	}
	if node.Scale != nil {
		scale, ok := schema.Metadata[model.MetadataKeyDecimalScale].(int)
		require.True(t, ok, "%s carries no decimal scale", label)
		assert.Equal(t, *node.Scale, scale, "scale at %s", label)
	}
	if node.Size != nil {
		size, ok := schema.Metadata[model.MetadataKeyFixedBytesSize].(int)
		require.True(t, ok, "%s carries no fixed size", label)
		assert.Equal(t, *node.Size, size, "fixed size at %s", label)
	}
	if node.Fields != nil {
		assertTortureSchema(t, node.Fields, schema.Fields, label)
	}
	if node.Element != nil {
		require.NotNil(t, schema.ElementSchema, "%s carries no element schema", label)
		assert.Equal(t, model.Type(node.Element.Type), schema.ElementSchema.Schema.DataType, "element type at %s", label)
		assert.Equal(t, node.Element.Nullable, schema.ElementSchema.Schema.IsNullable, "element nullability at %s", label)
		assertTortureNode(t, *node.Element, schema.ElementSchema.Schema, label+".element")
	}
	if node.Key != nil {
		require.NotNil(t, schema.KeySchema, "%s carries no key schema", label)
		assert.Equal(t, model.Type(node.Key.Type), schema.KeySchema.Schema.DataType, "key type at %s", label)
	}
	if node.Value != nil {
		require.NotNil(t, schema.ValueSchema, "%s carries no value schema", label)
		assert.Equal(t, model.Type(node.Value.Type), schema.ValueSchema.Schema.DataType, "value type at %s", label)
		assert.Equal(t, node.Value.Nullable, schema.ValueSchema.Schema.IsNullable, "value nullability at %s", label)
	}
}

// tortureColumnStat finds the column statistic for name among a data file's stats.
func tortureColumnStat(files []*model.DataFile, name string) *model.ColumnStat {
	if len(files) == 0 {
		return nil
	}
	for _, stat := range files[0].ColumnStats {
		if stat.Field != nil && stat.Field.Name == name {
			return stat
		}
	}
	return nil
}

// TestTortureTypes_DeltaSchema checks that every value class delta-rs-torture carries — decimal
// precision/scale at the boundaries where the backing width changes, both Delta timestamp kinds, and
// struct/list/map nesting — survives a plain Delta read, not just the column names.
func TestTortureTypes_DeltaSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadTortureFixture(t, "delta-rs-torture")
	source, err := formats.NewSource(model.TableFormatDelta, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	require.NotNil(t, table.ReadSchema)
	assertTortureSchema(t, manifest.TypeSchema, table.ReadSchema.Fields, "root")
}

// TestTortureTypes_DeltaColumnBounds checks the per-column bounds delta-rs itself recorded in the
// commit's stats JSON survive a polytable read unchanged. It found something more severe than the
// per-column precision loss it went looking for: pkg/formats/delta's StatsJSON.NullCount is declared
// `map[string]int64`, but delta-rs legitimately writes a *nested* null count for a struct column
// (this fixture's own commit carries `"nullCount":{"struct1":{"inner_a":2,"inner_struct":{"deep":2}},
// ...}`). json.Unmarshal(add.Stats, &stats) therefore fails with a type-mismatch error for the whole
// stats blob, and convertAddAction's `if err == nil` silently discards not just struct1's null count
// but the file's RecordCount and every other column's min/max/null-count too — one nested field
// poisons every column's statistics, with no error surfaced anywhere in GetCurrentSnapshot's result.
//
// This is demonstrated directly below, not hypothesized, and is a materially worse finding than the
// decimal-precision-loss question this test set out to check: right now none of dec38_0's clamped
// int64 bound, dec38_37's float-collapsed scale, or any other column's bound is reachable at all.
func TestTortureTypes_DeltaColumnBounds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadTortureFixture(t, "delta-rs-torture")
	source, err := formats.NewSource(model.TableFormatDelta, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, manifest.DataFileCount)
	file := snapshot.DataFiles[0]

	t.Run("stats_blob_dropped_by_nested_struct_null_count", func(t *testing.T) {
		// Remove this pin once pkg/formats/delta/source.go's convertAddAction tolerates (or reports)
		// a nested null count rather than silently discarding the whole stats blob on a type
		// mismatch, then replace it with real per-column assertions against manifest.ColumnBounds.
		assert.Zero(t, file.RecordCount,
			"RecordCount is no longer silently dropped; the nested-null-count defect in "+
				"pkg/formats/delta appears fixed — replace this pin with the real assertion below")
		assert.Empty(t, file.ColumnStats,
			"ColumnStats is no longer silently dropped; the nested-null-count defect in "+
				"pkg/formats/delta appears fixed — replace this pin with the real assertion below")
	})

	t.Run("per_column_bounds_once_stats_parsing_is_fixed", func(t *testing.T) {
		t.Skip("blocked on the stats-blob-dropped defect demonstrated above: convertAddAction " +
			"never populates ColumnStats for this fixture at all, so no per-column bound (including " +
			"dec38_0's int64-clamped one and dec38_37's float-collapsed one, both real, separately " +
			"confirmed writer-side precision losses — see the fixture's own manifest notes) can be " +
			"checked yet.")

		for name, expected := range manifest.ColumnBounds {
			stat := tortureColumnStat(snapshot.DataFiles, name)
			require.NotNil(t, stat, "no column statistic for %s", name)
			require.NotNil(t, stat.Range, "%s carries no bound", name)
			assert.Equal(t, expected.Min, stat.Range.MinValue, "minimum of %s", name)
			assert.Equal(t, expected.Max, stat.Range.MaxValue, "maximum of %s", name)
		}

		// assert.Equal on float64 treats -0.0 and 0.0 as equal (IEEE 754 equality), which would pass
		// whether or not the sign bit survived — exactly the property this column exists to check.
		stat := tortureColumnStat(snapshot.DataFiles, "f64")
		require.NotNil(t, stat)
		require.NotNil(t, stat.Range)
		minVal, ok := stat.Range.MinValue.(float64)
		require.True(t, ok, "f64 minimum is not a float64: %T", stat.Range.MinValue)
		assert.True(t, math.Signbit(minVal), "negative zero lost its sign bit reading it back: got %v", minVal)
	})
}

// TestTortureTypes_DeltaPartitionNullVsEmpty is delta-rs-partition-torture's whole point: a null
// partition value, an empty-string one and an ordinary one, read back through polytable's Delta
// source.
//
// Fixed under T70 defect 2: pkg/formats/delta/actions.go's AddAction.PartitionValues is now
// map[string]*string rather than map[string]string, so a JSON null decodes as a nil *string
// instead of colliding with a genuine empty string at the zero value "". This is #828's defect
// family, named in the task this fixture was written for.
func TestTortureTypes_DeltaPartitionNullVsEmpty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadTortureFixture(t, "delta-rs-partition-torture")
	require.NotEmpty(t, manifest.DataFiles)

	source, err := formats.NewSource(model.TableFormatDelta, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, manifest.DataFileCount)

	byRecordCount := make(map[int64][]*model.DataFile)
	for _, file := range snapshot.DataFiles {
		byRecordCount[file.RecordCount] = append(byRecordCount[file.RecordCount], file)
	}

	// The ordinary and escaped values are unaffected by the null/empty collision and have to survive
	// exactly: this is what already works today.
	for _, expected := range manifest.DataFiles {
		if expected.PartitionValue == nil || *expected.PartitionValue == "" {
			continue
		}
		found := false
		for _, file := range snapshot.DataFiles {
			require.Len(t, file.PartitionValues, 1)
			if value, ok := file.PartitionValues[0].Range.MinValue.(string); ok && value == *expected.PartitionValue {
				found = true
			}
		}
		assert.True(t, found, "partition value %q did not survive the read", *expected.PartitionValue)
	}

	t.Run("null_distinct_from_empty_string", func(t *testing.T) {
		var sawNull, sawEmpty bool
		for _, file := range snapshot.DataFiles {
			value, ok := file.PartitionValues[0].Range.MinValue.(string)
			if ok && value == "" {
				sawEmpty = true
			}
		}
		// A null partition value decodes as a nil Range.MinValue rather than "".
		for _, file := range snapshot.DataFiles {
			if file.PartitionValues[0].Range.MinValue == nil {
				sawNull = true
			}
		}
		assert.True(t, sawNull, "no data file reported a nil (as opposed to empty-string) partition value")
		assert.True(t, sawEmpty, "no data file reported an empty-string partition value")
	})
}

// TestTortureTypes_IcebergSchema checks pyiceberg-torture's schema, including the two types Iceberg
// has that Delta does not: FIXED and UUID.
//
// fixed4 is a known, load-bearing gap: pkg/formats/iceberg/schema.go's parseIcebergType has no case
// for Iceberg's `"fixed[N]"` type string, so it falls through to the function's default branch and
// arrives as TypeString with no size metadata and, critically, no error or warning — the task's
// "fail or warn by name, not silently narrow" clause names exactly this shape of defect. The correct
// assertion is left in the test, skipped, rather than weakened to match the narrowing.
func TestTortureTypes_IcebergSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadTortureFixture(t, "pyiceberg-torture")
	source, err := formats.NewSource(model.TableFormatIceberg, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	require.NotNil(t, table.ReadSchema)

	for _, node := range manifest.TypeSchema {
		if node.Name == "fixed4" {
			continue
		}
		field := table.ReadSchema.FieldByPath(node.Name)
		require.NotNil(t, field, "field %s is missing", node.Name)
		require.NotNil(t, field.Schema)
		assert.Equal(t, model.Type(node.Type), field.Schema.DataType, "type of %s", node.Name)
		assert.Equal(t, node.Nullable, field.Schema.IsNullable, "nullability of %s", node.Name)
		assertTortureNode(t, node, field.Schema, node.Name)
	}

	t.Run("fixed_type_preserved", func(t *testing.T) {
		t.Skip("known defect: pkg/formats/iceberg/schema.go parseIcebergType has no case for " +
			"Iceberg's fixed[N] type string; it falls through to the default branch and silently " +
			"arrives as TypeString with no size metadata and no error or warning.")

		field := table.ReadSchema.FieldByPath("fixed4")
		require.NotNil(t, field)
		require.NotNil(t, field.Schema)
		assert.Equal(t, model.TypeFixed, field.Schema.DataType)
		size, ok := field.Schema.Metadata[model.MetadataKeyFixedBytesSize].(int)
		require.True(t, ok, "fixed4 carries no size metadata")
		assert.Equal(t, 4, size)
	})
}

// TestTortureTypes_IcebergColumnBounds checks pyiceberg-torture's per-column bounds, both against the
// logical value generate.py computed directly from the rows it wrote (column_bounds) and against the
// exact bytes pyiceberg's own manifest entry recorded (raw_bounds, cross-checked here for the
// non-decimal columns to confirm the two sources of truth agree).
//
// DECIMAL is a confirmed gap, not a hypothesis: pkg/formats/iceberg/stats.go's EncodeBound/DecodeBound
// explicitly lists DECIMAL in their shared default branch, alongside the nested types, as a bound
// this port does not serialize or parse — the comment there says so. A decimal column's ColumnStat
// therefore carries no Range at all when read through polytable, even though pyiceberg wrote a
// perfectly good 16-byte two's-complement bound (raw_bounds proves this: dec38_0 and dec9_2 both have
// non-empty lower_hex/upper_hex). This is asserted directly below rather than skipped, because it is
// exactly the documented, current behavior — not a hypothesis about a future fix.
func TestTortureTypes_IcebergColumnBounds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadTortureFixture(t, "pyiceberg-torture")
	source, err := formats.NewSource(model.TableFormatIceberg, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, manifest.DataFileCount)

	// fixed4's bound is a downstream consequence of the schema defect TestTortureTypes_IcebergSchema
	// already names: because parseIcebergType mis-parses fixed[4] as TypeString, DecodeBound switches
	// on that (wrong) type and decodes the bound as a Go string instead of raw bytes. It is not a
	// second, independent defect, so it is skipped here rather than re-reported.
	//
	// str_col, str_nullable and bin_col each have a *minimum* that is the empty string or empty
	// bytes — exactly the value class the task asks for ("an empty string versus null"). That
	// minimum is a second, independent, confirmed defect: DecodeBound's own
	// `if schema == nil || len(raw) == 0 { return nil, false }` treats a zero-length bound the same
	// as "no bound was recorded", so a genuinely empty lower bound decodes as no bound at all
	// (Range.MinValue == nil) rather than as "". Their maxima are unaffected and checked normally.
	emptyMinimumDefect := map[string]bool{"str_col": true, "str_nullable": true, "bin_col": true}

	for name, expected := range manifest.ColumnBounds {
		t.Run(name, func(t *testing.T) {
			if name == "fixed4" {
				t.Skip("blocked on the FIXED schema defect (see TestTortureTypes_IcebergSchema): " +
					"fixed4's bound decodes as a Go string via DecodeBound's TypeString case, not " +
					"as []byte, because the schema itself was mis-parsed as TypeString")
			}

			stat := tortureColumnStat(snapshot.DataFiles, name)
			require.NotNil(t, stat, "no column statistic for %s", name)
			require.NotNil(t, stat.Range, "%s carries no bound", name)

			if emptyMinimumDefect[name] {
				assert.Nil(t, stat.Range.MinValue,
					"%s's minimum is no longer nil; DecodeBound's zero-length-bound handling in "+
						"pkg/formats/iceberg/stats.go appears to have been fixed — drop this pin and "+
						"assert the real (empty) value", name)
				assertTortureBound(t, expected.Max, expected.MaxHex, stat.Range.MaxValue, name+" maximum")
				assertRawBoundEncodesTo(t, stat, "", manifest.RawBounds[name].UpperHex, name)
				return
			}
			assertTortureBound(t, expected.Min, expected.MinHex, stat.Range.MinValue, name+" minimum")
			assertTortureBound(t, expected.Max, expected.MaxHex, stat.Range.MaxValue, name+" maximum")
			// Cross-check against the exact bytes pyiceberg itself wrote (raw_bounds), independent of
			// the logical value generate.py computed (column_bounds): re-encoding what polytable
			// decoded has to reproduce the writer's own bytes, confirming the two sources of truth in
			// the fixture manifest agree rather than each merely being self-consistent.
			if raw, ok := manifest.RawBounds[name]; ok {
				assertRawBoundEncodesTo(t, stat, raw.LowerHex, raw.UpperHex, name)
			}
		})
	}

	t.Run("f64_negative_zero_bit_pattern", func(t *testing.T) {
		stat := tortureColumnStat(snapshot.DataFiles, "f64")
		require.NotNil(t, stat)
		require.NotNil(t, stat.Range)
		minVal, ok := stat.Range.MinValue.(float64)
		require.True(t, ok, "f64 minimum is not a float64: %T", stat.Range.MinValue)
		assert.True(t, math.Signbit(minVal), "negative zero lost its sign bit reading it back: got %v", minVal)
	})

	t.Run("decimal_bounds_not_decoded", func(t *testing.T) {
		for _, name := range []string{"dec38_0", "dec38_37", "dec9_2"} {
			raw, ok := manifest.RawBounds[name]
			require.True(t, ok, "fixture manifest carries no raw_bounds for %s", name)
			require.NotEmpty(t, raw.LowerHex, "%s: fixture's own raw manifest bytes are empty; the "+
				"writer wrote no bound to test against", name)

			stat := tortureColumnStat(snapshot.DataFiles, name)
			require.NotNil(t, stat, "no column statistic for %s", name)
			assert.Nil(t, stat.Range,
				"%s now carries a Range; EncodeBound/DecodeBound in pkg/formats/iceberg/stats.go "+
					"appear to have gained a DECIMAL case — drop this pin and assert the real bound", name)
		}
	})
}

// assertTortureBound compares one bound value against the fixture's recorded expectation.
//
// The manifest's numeric bounds decode through encoding/json into float64 (Go's generic behavior for
// `any`); polytable's own readers decode a LONG or TIMESTAMP bound natively as int64
// (pkg/formats/iceberg/stats.go's DecodeBound is explicit about this). Comparing those two directly
// with assert.Equal fails on the type mismatch alone despite the values being numerically identical,
// which is a property of this test's two sources of ground truth, not of polytable — numericBound
// (defined in foreign_fixtures_test.go) is reused here to compare both sides as float64.
func assertTortureBound(t *testing.T, expected any, expectedHex string, actual any, label string) {
	t.Helper()

	if expectedHex != "" {
		actualBytes, ok := actual.([]byte)
		require.True(t, ok, "%s is not []byte: %T", label, actual)
		assert.Equal(t, expectedHex, hexString(actualBytes), label)
		return
	}
	if expectedNum, ok := numericBound(expected); ok {
		actualNum, ok := numericBound(actual)
		require.True(t, ok, "%s: actual value %v (%T) is not numeric", label, actual, actual)
		assert.InDelta(t, expectedNum, actualNum, 1e-9, label)
		return
	}
	assert.Equal(t, expected, actual, label)
}

// assertRawBoundEncodesTo re-encodes a decoded ColumnStat's bound through polytable's own
// EncodeBound and checks it reproduces pyiceberg's exact recorded bytes (generate.py's raw_bounds),
// the fixture's second, independent source of truth alongside the logical column_bounds value. An
// empty expected hex string skips that side (used for the empty-minimum defect, whose MinValue is
// nil and has nothing to re-encode).
func assertRawBoundEncodesTo(t *testing.T, stat *model.ColumnStat, lowerHex, upperHex, label string) {
	t.Helper()

	if lowerHex != "" {
		encoded, ok := iceberg.EncodeBound(stat.Field.Schema, stat.Range.MinValue, iceberg.LowerBound)
		require.True(t, ok, "%s: could not re-encode the decoded minimum", label)
		assert.Equal(t, lowerHex, hexString(encoded), "%s: re-encoded minimum does not match pyiceberg's own bytes", label)
	}
	if upperHex != "" {
		encoded, ok := iceberg.EncodeBound(stat.Field.Schema, stat.Range.MaxValue, iceberg.UpperBound)
		require.True(t, ok, "%s: could not re-encode the decoded maximum", label)
		assert.Equal(t, upperHex, hexString(encoded), "%s: re-encoded maximum does not match pyiceberg's own bytes", label)
	}
}

// hexString renders bytes the same way generate.py's Python .hex() does, for a byte-for-byte
// comparison against the fixture's own raw_bounds/column_bounds hex fields.
func hexString(b []byte) string {
	const hextable = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = hextable[c>>4]
		out[i*2+1] = hextable[c&0x0f]
	}
	return string(out)
}

// TestTortureTypes_IcebergPartitionNullVsEmpty is the Iceberg counterpart of
// TestTortureTypes_DeltaPartitionNullVsEmpty, over the same three partition values. Unlike Delta's
// map[string]string, an Iceberg manifest's partition record decodes through Avro into a Go `any`, so
// a null partition value and an empty string are not forced through the same zero value — this test
// checks whether that structural difference actually pays off in the read polytable produces.
func TestTortureTypes_IcebergPartitionNullVsEmpty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadTortureFixture(t, "pyiceberg-partition-torture")
	require.NotEmpty(t, manifest.DataFiles)

	source, err := formats.NewSource(model.TableFormatIceberg, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, manifest.DataFileCount)

	var sawNull, sawEmpty, sawOrdinary, sawEscaped bool
	for _, file := range snapshot.DataFiles {
		require.Len(t, file.PartitionValues, 1)
		value := file.PartitionValues[0].Range.MinValue
		switch v := value.(type) {
		case nil:
			sawNull = true
		case string:
			switch v {
			case "":
				sawEmpty = true
			case "east":
				sawOrdinary = true
			case "north america/100%":
				sawEscaped = true
			}
		}
	}

	assert.True(t, sawNull, "no data file reported a nil partition value for the null-partition row")
	assert.True(t, sawEmpty, "no data file reported an empty-string partition value")
	assert.True(t, sawOrdinary, "the ordinary partition value did not survive the read")
	assert.True(t, sawEscaped, "the partition value needing path-escaping did not survive the read")
}

// TestTortureTypes_ConvertDeltaTortureIntoIceberg is the conversion half of the suite: every value
// class in delta-rs-torture, synced into Iceberg and read back through Iceberg's own source, checking
// the schema — decimal precision/scale, the TIMESTAMP/TIMESTAMP_NTZ distinction and nested
// nullability — survives translation rather than merely a same-format read.
func TestTortureTypes_ConvertDeltaTortureIntoIceberg(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadTortureFixture(t, "delta-rs-torture")
	storage := io.NewLocalStorage()

	results, err := conversion.NewController(storage).Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: tableDir,
		TableName:     manifest.TableName,
		SyncMode:      spi.SyncModeFull,
	})
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode, results[model.TableFormatIceberg].Error)

	source, err := formats.NewSource(model.TableFormatIceberg, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	require.NotNil(t, table.ReadSchema)

	// Iceberg has no field the schema didn't ask for and Delta carries no field ids, so the shape
	// asserted here is the same recursion used for a plain Delta read: name, type, nullability and
	// decimal precision/scale at every level.
	assertTortureSchema(t, manifest.TypeSchema, table.ReadSchema.Fields, "root")

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, manifest.DataFileCount)

	t.Run("decimal_bound_dropped_on_write", func(t *testing.T) {
		// Intended as the write-side counterpart of TestTortureTypes_IcebergColumnBounds's
		// decimal_bounds_not_decoded: columnStatsToManifest calls EncodeBound for every column stat
		// the source reported, and EncodeBound's own default branch silently omits DECIMAL rather
		// than erroring, so a decimal bound should never reach the Iceberg manifest at all.
		//
		// It cannot be demonstrated from this fixture today: TestTortureTypes_DeltaColumnBounds's
		// stats_blob_dropped_by_nested_struct_null_count defect means the Delta *source* itself
		// reports zero ColumnStats for every column, dec38_0 included — there is no bound for
		// EncodeBound to drop in the first place. TestTortureTypes_IcebergColumnBounds's
		// decimal_bounds_not_decoded already demonstrates the same EncodeBound/DecodeBound gap
		// directly, from a fixture unaffected by the Delta-side defect, so that is this port's
		// evidence for the underlying gap until the Delta stats parsing is fixed.
		t.Skip("blocked on the Delta source's stats_blob_dropped_by_nested_struct_null_count " +
			"defect (see TestTortureTypes_DeltaColumnBounds): it reports zero ColumnStats for this " +
			"fixture, so there is no dec38_0 bound for this conversion to drop yet")

		stat := tortureColumnStat(snapshot.DataFiles, "dec38_0")
		require.NotNil(t, stat, "no column statistic for dec38_0")
		assert.Nil(t, stat.Range,
			"dec38_0 now carries a Range after conversion; EncodeBound in "+
				"pkg/formats/iceberg/stats.go appears to have gained a DECIMAL case")
	})
}

// tortureNodeByName finds a top-level field's recorded schema node by name.
func tortureNodeByName(nodes []tortureTypeNode, name string) *tortureTypeNode {
	for i := range nodes {
		if nodes[i].Name == name {
			return &nodes[i]
		}
	}
	return nil
}

// tortureScalarColumns is delta-rs-torture's columns unaffected by the target-specific defects this
// test asserts separately (struct1/list1/map1 and ts_ntz), used to confirm the ordinary scalar
// surface — including decimal precision/scale — survives a conversion before drilling into what does
// not.
var tortureScalarColumns = []string{
	"id", "dec38_0", "dec38_37", "dec9_2", "dec18_4", "ts_tz", "f64", "str_col", "str_nullable", "bin_col",
}

// TestTortureTypes_ConvertDeltaTortureAcrossTargets is TestTortureTypes_ConvertDeltaTortureIntoIceberg
// for every other target polytable can sync a Delta source into, following
// TestForeignFixtures_ConvertDelta's per-target pattern. Each target turned up its own, independent
// defect — none of them a repeat of Iceberg's:
//
//   - Parquet fails outright reading the schema back, by name: parquet-go's reader does not support
//     the 3-level repeated-group shape a LIST or MAP column takes in a real Parquet file
//     ("LIST/MAP-shaped nested repetition is not supported"). This is the "fail... by name" outcome
//     the task asks for, not silent narrowing, so it is pinned as an expected error rather than
//     treated as a defect.
//   - Hudi silently narrows TIMESTAMP_NTZ to STRING. Its own writer
//     (pkg/formats/hudi/schema.go convertTypeToAvro) emits the Avro logical type
//     "local-timestamp-micros" for TIMESTAMP_NTZ, but its own reader (parseAvroType) has no case for
//     that logical type — only "timestamp-millis"/"timestamp-micros" (TIMESTAMP) are recognized — so
//     the value falls through to parseAvroType's final `return model.NewPrimitiveSchema(TypeString,
//     false)`. Hudi's own round trip disagrees with itself. Nested types are unaffected: Hudi
//     preserves struct1/list1/map1 correctly, asserted normally below.
//   - Paimon silently narrows every nested type to STRING, and separately collapses the
//     TIMESTAMP/TIMESTAMP_NTZ distinction. modelTypeToPaimonType
//     (pkg/formats/paimon/schema.go) has no case for TypeRecord at all — it falls to that function's
//     own default branch and writes the literal string "STRING" into the table's schema for a struct
//     column, before any read is involved. TypeList and TypeMap do serialize structurally
//     ("ARRAY<...>", "MAP<...>"), but parsePaimonType's read side has no prefix case for either, so
//     they narrow to STRING on the way back in regardless. Separately, both TypeTimestamp and
//     TypeTimestampNTZ map to the same "TIMESTAMP(6)" column type with no distinguishing marker, so
//     the zone-awareness distinction this suite exists to check is lost even though real Paimon has a
//     `TIMESTAMP WITH LOCAL TIME ZONE` type it could have used.
func TestTortureTypes_ConvertDeltaTortureAcrossTargets(t *testing.T) {
	t.Parallel()

	for _, target := range []model.TableFormat{model.TableFormatHudi, model.TableFormatParquet, model.TableFormatPaimon} {
		t.Run(strings.ToLower(string(target)), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			tableDir, manifest := loadTortureFixture(t, "delta-rs-torture")
			storage := io.NewLocalStorage()

			results, err := conversion.NewController(storage).Sync(ctx, &conversion.DatasetConfig{
				SourceFormat:  model.TableFormatDelta,
				TargetFormats: []model.TableFormat{target},
				TableBasePath: tableDir,
				TableName:     manifest.TableName,
				SyncMode:      spi.SyncModeFull,
			})
			require.NoError(t, err)
			require.Equal(t, spi.SyncStatusSuccess, results[target].StatusCode, results[target].Error)

			source, err := formats.NewSource(target, storage, tableDir)
			require.NoError(t, err)
			t.Cleanup(func() { _ = source.Close() })

			table, err := source.GetCurrentTable(ctx)
			if target == model.TableFormatParquet {
				require.Error(t, err, "parquet can now read this fixture's schema back; drop this "+
					"pin and assert the real schema")
				assert.ErrorContains(t, err, "LIST/MAP-shaped nested repetition is not supported")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, table.ReadSchema)

			for _, name := range tortureScalarColumns {
				expected := tortureNodeByName(manifest.TypeSchema, name)
				require.NotNil(t, expected, "torture fixture manifest carries no node for %s", name)
				field := table.ReadSchema.FieldByPath(name)
				require.NotNil(t, field, "%s: field is missing after conversion to %s", name, target)
				require.NotNil(t, field.Schema)
				assert.Equal(t, model.Type(expected.Type), field.Schema.DataType, "%s: type after conversion to %s", name, target)
				assert.Equal(t, expected.Nullable, field.Schema.IsNullable, "%s: nullability after conversion to %s", name, target)
				assertTortureNode(t, *expected, field.Schema, name)
			}

			switch target {
			case model.TableFormatHudi:
				t.Run("nested_types_preserved", func(t *testing.T) {
					for _, name := range []string{"struct1", "list1", "map1"} {
						expected := tortureNodeByName(manifest.TypeSchema, name)
						require.NotNil(t, expected)
						field := table.ReadSchema.FieldByPath(name)
						require.NotNil(t, field, "%s is missing", name)
						require.NotNil(t, field.Schema)
						assert.Equal(t, model.Type(expected.Type), field.Schema.DataType, "type of %s", name)
						assertTortureNode(t, *expected, field.Schema, name)
					}
				})
				t.Run("timestamp_ntz_narrowed_to_string", func(t *testing.T) {
					field := table.ReadSchema.FieldByPath("ts_ntz")
					require.NotNil(t, field)
					require.NotNil(t, field.Schema)
					assert.Equal(t, model.TypeString, field.Schema.DataType,
						"ts_ntz no longer round-trips as STRING; Hudi's parseAvroType appears to have "+
							"gained a case for the local-timestamp-micros logical type its own writer "+
							"emits — drop this pin and assert model.TypeTimestampNTZ instead")
				})

			case model.TableFormatPaimon:
				t.Run("nested_types_narrowed_to_string", func(t *testing.T) {
					for _, name := range []string{"struct1", "list1", "map1"} {
						field := table.ReadSchema.FieldByPath(name)
						require.NotNil(t, field, "%s is missing", name)
						require.NotNil(t, field.Schema)
						assert.Equal(t, model.TypeString, field.Schema.DataType,
							"%s no longer narrows to STRING; modelTypeToPaimonType/parsePaimonType in "+
								"pkg/formats/paimon/schema.go appear to have gained support for it — "+
								"drop this pin and assert the real nested type", name)
					}
				})
				t.Run("timestamp_ntz_collapsed_to_timestamp", func(t *testing.T) {
					field := table.ReadSchema.FieldByPath("ts_ntz")
					require.NotNil(t, field)
					require.NotNil(t, field.Schema)
					assert.Equal(t, model.TypeTimestamp, field.Schema.DataType,
						"ts_ntz no longer collapses to TIMESTAMP; modelTypeToPaimonType appears to "+
							"have gained a distinct mapping for TIMESTAMP_NTZ — drop this pin and "+
							"assert model.TypeTimestampNTZ instead")
				})
			}
		})
	}
}

// TestTortureTypes_ConvertIcebergTortureIntoDelta is the Iceberg-as-source counterpart: UUID and
// FIXED, the two types Iceberg has and Delta does not, synced across the format boundary.
//
// UUID arriving as a bare Delta STRING is not narrowing in the silent, accidental sense the rest of
// this suite reports: pkg/formats/delta/schema.go's typeToDeltaJSON has an explicit
// `case model.TypeString, model.TypeEnum, model.TypeUUID` — a deliberate choice, consistent with how
// Spark and every other Delta-writing engine represents a UUID column, and the canonical string form
// is a faithful, re-parseable transcription rather than a lossy one. It is asserted here as the
// correct, intended behavior.
//
// FIXED is folded into the schema defect TestTortureTypes_IcebergSchema already names
// (parseIcebergType has no fixed[N] case) rather than re-reported: since the *source* schema recovery
// already mis-reports fixed4 as STRING, this conversion has nothing FIXED-shaped left to lose by the
// time it reaches Delta.
func TestTortureTypes_ConvertIcebergTortureIntoDelta(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir, manifest := loadTortureFixture(t, "pyiceberg-torture")
	storage := io.NewLocalStorage()

	results, err := conversion.NewController(storage).Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatIceberg,
		TargetFormats: []model.TableFormat{model.TableFormatDelta},
		TableBasePath: tableDir,
		TableName:     manifest.TableName,
		SyncMode:      spi.SyncModeFull,
	})
	require.NoError(t, err)
	require.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode, results[model.TableFormatDelta].Error)

	source, err := formats.NewSource(model.TableFormatDelta, storage, tableDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	require.NotNil(t, table.ReadSchema)

	for _, name := range []string{"id", "dec38_0", "dec38_37", "dec9_2", "ts_tz", "ts_ntz", "f64", "str_col", "str_nullable", "bin_col"} {
		expected := tortureNodeByName(manifest.TypeSchema, name)
		require.NotNil(t, expected, "torture fixture manifest carries no node for %s", name)
		field := table.ReadSchema.FieldByPath(name)
		require.NotNil(t, field, "%s: field is missing after conversion to Delta", name)
		require.NotNil(t, field.Schema)
		assert.Equal(t, model.Type(expected.Type), field.Schema.DataType, "type of %s", name)
		assert.Equal(t, expected.Nullable, field.Schema.IsNullable, "nullability of %s", name)
		assertTortureNode(t, *expected, field.Schema, name)
	}

	t.Run("uuid_becomes_delta_string_by_design", func(t *testing.T) {
		field := table.ReadSchema.FieldByPath("uid")
		require.NotNil(t, field)
		require.NotNil(t, field.Schema)
		assert.Equal(t, model.TypeString, field.Schema.DataType,
			"Delta has no UUID type; typeToDeltaJSON's explicit TypeUUID case maps it to STRING")
	})

	t.Run("fixed_type_already_lost_upstream", func(t *testing.T) {
		field := table.ReadSchema.FieldByPath("fixed4")
		require.NotNil(t, field)
		require.NotNil(t, field.Schema)
		// See TestTortureTypes_IcebergSchema's fixed_type_preserved: the Iceberg *source* already
		// mis-reports this column as TypeString before the Delta target ever sees it.
		assert.Equal(t, model.TypeString, field.Schema.DataType)
	})
}
