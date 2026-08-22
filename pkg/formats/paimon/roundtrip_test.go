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

package paimon_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/paimon"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// paimonRoundTripTable builds a small table descriptor for the round-trip cases below.
func paimonRoundTripTable(basePath string, partitioned bool) *model.Table {
	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	amountField := &model.Field{Name: "amount", Schema: model.NewPrimitiveSchema(model.TypeDouble, true)}
	regionField := &model.Field{Name: "region", Schema: model.NewPrimitiveSchema(model.TypeString, false)}

	table := &model.Table{
		Name:             "sales",
		TableFormat:      model.TableFormatDelta,
		BasePath:         basePath,
		LatestCommitTime: 1_700_000_000_000,
		ReadSchema:       model.NewRecordSchema("sales", []*model.Field{idField, amountField, regionField}, false),
	}
	if partitioned {
		table.PartitioningFields = []*model.PartitionField{{
			SourceField:   regionField,
			TransformType: model.PartitionTransformValue,
		}}
	}
	return table
}

func paimonDataFile(basePath, relPath string, records int64, region string, partitioned bool) *model.DataFile {
	file := &model.DataFile{
		PhysicalPath:  io.JoinPath(basePath, relPath),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 1024,
		RecordCount:   records,
	}
	if partitioned {
		file.PartitionValues = []*model.PartitionValue{{
			PartitionField: &model.PartitionField{
				SourceField:   &model.Field{Name: "region", Schema: model.NewPrimitiveSchema(model.TypeString, false)},
				TransformType: model.PartitionTransformValue,
			},
			Range: model.NewScalarRange(region),
		}}
	}
	return file
}

// TestPaimon_TargetOutputIsReadableBySource is T32's core assertion: whatever the target writes as
// Paimon, the Paimon source reads back — schema, file list and row counts included.
func TestPaimon_TargetOutputIsReadableBySource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		partitioned bool
		files       []struct {
			path    string
			records int64
			region  string
		}
	}{
		{
			name: "flat table",
			files: []struct {
				path    string
				records int64
				region  string
			}{
				{path: "part-0.parquet", records: 7},
				{path: "part-1.parquet", records: 5},
			},
		},
		{
			name:        "hive partitioned table",
			partitioned: true,
			files: []struct {
				path    string
				records int64
				region  string
			}{
				{path: "region=EU/part-0.parquet", records: 3, region: "EU"},
				{path: "region=US/part-1.parquet", records: 11, region: "US"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/sales"
			table := paimonRoundTripTable(basePath, tt.partitioned)

			var files []*model.DataFile
			var expectedRows int64
			for _, f := range tt.files {
				files = append(files, paimonDataFile(basePath, f.path, f.records, f.region, tt.partitioned))
				expectedRows += f.records
			}

			target := paimon.NewTarget(storage)
			require.NoError(t, target.Init(ctx, table))
			require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
				Table:            table,
				DataFiles:        files,
				SourceIdentifier: "1",
			}))
			t.Cleanup(func() { _ = target.Close() })

			// The layout is Paimon's own: schema/schema-<id>, snapshot/snapshot-<id> and the
			// LATEST hint beside it.
			for _, expected := range []string{"schema/schema-0", "snapshot/snapshot-1", "snapshot/LATEST", "snapshot/EARLIEST"} {
				exists, err := storage.Exists(ctx, io.JoinPath(basePath, expected))
				require.NoError(t, err)
				assert.True(t, exists, "the target did not write %s", expected)
			}

			source := paimon.NewSource(storage, basePath)
			t.Cleanup(func() { _ = source.Close() })

			readBack, err := source.GetCurrentSnapshot(ctx)
			require.NoError(t, err)
			assert.Equal(t, "1", readBack.SourceIdentifier)

			require.NotNil(t, readBack.Table.ReadSchema)
			require.Len(t, readBack.Table.ReadSchema.Fields, len(table.ReadSchema.Fields))
			for i, expected := range table.ReadSchema.Fields {
				actual := readBack.Table.ReadSchema.Fields[i]
				assert.Equal(t, expected.Name, actual.Name)
				assert.Equal(t, expected.Schema.DataType, actual.Schema.DataType, "type of %s", expected.Name)
				assert.Equal(t, expected.Schema.IsNullable, actual.Schema.IsNullable, "nullability of %s", expected.Name)
			}

			require.Len(t, readBack.Table.PartitioningFields, len(table.PartitioningFields))

			byPath := make(map[string]int64, len(readBack.DataFiles))
			for _, file := range readBack.DataFiles {
				byPath[file.PhysicalPath] = file.RecordCount
			}
			var totalRows int64
			for _, file := range files {
				records, ok := byPath[file.PhysicalPath]
				require.True(t, ok, "the round trip dropped %s", file.PhysicalPath)
				assert.Equal(t, file.RecordCount, records, "row count of %s", file.PhysicalPath)
				totalRows += records
			}
			assert.Equal(t, expectedRows, totalRows)

			if tt.partitioned {
				require.NotEmpty(t, readBack.DataFiles)
				for _, file := range readBack.DataFiles {
					require.Len(t, file.PartitionValues, 1, "%s lost its partition value", file.PhysicalPath)
				}
			}
		})
	}
}

// TestPaimon_NullPartitionValueDoesNotBecomeLiteralNil is the write-side guard adjacent to T70
// defect 2: a nil Range.MinValue must never be formatted as the literal string "<nil>" in the
// manifest. Paimon's own manifest is map[string]string on read (the same JSON-collapse limitation
// Delta had before the fix) and this reader does not know the writer's configured
// partition-default-name, so a null partition value here deliberately folds into the same "" a
// genuinely empty partition value produces, rather than fabricating an unrecognisable marker.
func TestPaimon_NullPartitionValueDoesNotBecomeLiteralNil(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/null_partition"
	table := paimonRoundTripTable(basePath, true)

	regionField := &model.PartitionField{
		SourceField:   &model.Field{Name: "region", Schema: model.NewPrimitiveSchema(model.TypeString, false)},
		TransformType: model.PartitionTransformValue,
	}
	file := &model.DataFile{
		PhysicalPath:  io.JoinPath(basePath, "region=__DEFAULT_PARTITION__/part-0.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 1024,
		RecordCount:   3,
		PartitionValues: []*model.PartitionValue{{
			PartitionField: regionField,
			Range:          model.NewScalarRange(nil),
		}},
	}

	target := paimon.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	t.Cleanup(func() { _ = target.Close() })
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{file},
		SourceIdentifier: "1",
	}))

	source := paimon.NewSource(storage, basePath)
	t.Cleanup(func() { _ = source.Close() })
	readBack, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, readBack.DataFiles, 1)
	require.Len(t, readBack.DataFiles[0].PartitionValues, 1)

	value := readBack.DataFiles[0].PartitionValues[0].Range.MinValue
	assert.NotEqual(t, "<nil>", value, "a null partition value must never be formatted as the literal \"<nil>\"")
	assert.Equal(t, "", value)
}

// TestPaimon_IncrementalCommitsAccumulate checks that each incremental commit lands as its own
// snapshot and that the newest snapshot still describes the whole table.
func TestPaimon_IncrementalCommitsAccumulate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/incremental"
	table := paimonRoundTripTable(basePath, false)

	first := paimonDataFile(basePath, "part-0.parquet", 4, "", false)
	second := paimonDataFile(basePath, "part-1.parquet", 6, "", false)

	target := paimon.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	t.Cleanup(func() { _ = target.Close() })

	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{first},
		SourceIdentifier: "1",
	}))

	require.NoError(t, target.CommitChanges(ctx, &model.IncrementalTableChanges{
		CurrentTable: table,
		TableChanges: []*model.TableChange{{
			SourceIdentifier: "2",
			CommitTime:       1_700_000_001_000,
			TableAsOfChange:  table,
			FilesDiff:        model.NewFilesDiff([]*model.DataFile{second}, nil),
		}},
	}))

	source := paimon.NewSource(storage, basePath)
	t.Cleanup(func() { _ = source.Close() })

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, "2", snapshot.SourceIdentifier)
	require.Len(t, snapshot.DataFiles, 2)

	var rows int64
	for _, file := range snapshot.DataFiles {
		rows += file.RecordCount
	}
	assert.Equal(t, int64(10), rows)

	// The schema did not change, so the second commit reuses schema-0 rather than writing a new
	// schema file, which is how Paimon versions schemas.
	exists, err := storage.Exists(ctx, io.JoinPath(basePath, "schema/schema-1"))
	require.NoError(t, err)
	assert.False(t, exists, "an unchanged schema was rewritten as a new version")

	syncMetadata, err := target.GetTableMetadata(ctx)
	require.NoError(t, err)
	require.NotNil(t, syncMetadata)
	assert.Equal(t, int64(1_700_000_001_000), syncMetadata.LastInstantSynced)
	assert.Equal(t, model.TableFormatDelta, syncMetadata.SourceFormat)
	assert.Equal(t, model.TableFormatPaimon, syncMetadata.TargetFormat)
}
