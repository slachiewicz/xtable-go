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

package delta_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/delta"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestDelta_SchemaRoundTrip(t *testing.T) {
	t.Parallel()

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	nameField := &model.Field{Name: "name", Schema: model.NewPrimitiveSchema(model.TypeString, true)}
	priceField := &model.Field{Name: "price", Schema: model.NewDecimalSchema(10, 2, true)}
	createdField := &model.Field{Name: "created_at", Schema: model.NewPrimitiveSchema(model.TypeTimestamp, false)}

	origSchema := model.NewRecordSchema("item", []*model.Field{idField, nameField, priceField, createdField}, false)

	jsonStr, err := delta.SchemaToDeltaJSON(origSchema)
	require.NoError(t, err)
	assert.Contains(t, jsonStr, `"type":"integer"`)
	assert.Contains(t, jsonStr, `"type":"string"`)
	assert.Contains(t, jsonStr, `"type":"decimal(10,2)"`)
	assert.Contains(t, jsonStr, `"type":"timestamp"`)

	parsedSchema, err := delta.DeltaJSONToSchema(jsonStr)
	require.NoError(t, err)
	require.Len(t, parsedSchema.Fields, 4)

	assert.Equal(t, "id", parsedSchema.Fields[0].Name)
	assert.Equal(t, model.TypeInt, parsedSchema.Fields[0].Schema.DataType)
	assert.Equal(t, "price", parsedSchema.Fields[2].Name)
	assert.Equal(t, model.TypeDecimal, parsedSchema.Fields[2].Schema.DataType)
}

func TestDelta_SnapshotCommitAndRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/delta_table"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	cityField := &model.Field{Name: "city", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	schema := model.NewRecordSchema("people", []*model.Field{idField, cityField}, false)

	partField := &model.PartitionField{
		SourceField:   cityField,
		TransformType: model.PartitionTransformValue,
	}

	table := &model.Table{
		Name:               "people",
		TableFormat:        model.TableFormatDelta,
		ReadSchema:         schema,
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	dataFile1 := &model.DataFile{
		PhysicalPath:  "mem://lake/delta_table/city=NYC/part-0.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 1024,
		RecordCount:   50,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange("NYC")},
		},
		ColumnStats: []*model.ColumnStat{
			{Field: idField, Range: model.NewRange(1, 50), NumNulls: 0},
		},
		LastModified: time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile1},
		SourceIdentifier: "snap-1",
	}

	// 1. Commit snapshot using Target
	target := delta.NewTarget(memStorage)
	err := target.Init(ctx, table)
	require.NoError(t, err)

	err = target.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 2. Read snapshot using Source
	source := delta.NewSource(memStorage, basePath)
	currentTable, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	assert.Equal(t, "people", currentTable.Name)
	assert.Equal(t, model.TableFormatDelta, currentTable.TableFormat)
	require.Len(t, currentTable.PartitioningFields, 1)
	assert.Equal(t, "city", currentTable.PartitioningFields[0].SourceField.Name)

	readSnapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, readSnapshot.DataFiles, 1)

	readDF := readSnapshot.DataFiles[0]
	assert.Equal(t, int64(50), readDF.RecordCount)
	assert.Equal(t, int64(1024), readDF.FileSizeBytes)
	require.Len(t, readDF.PartitionValues, 1)
	assert.Equal(t, "NYC", readDF.PartitionValues[0].Range.MinValue)

	// 3. Verify TableSyncMetadata
	meta, err := target.GetTableMetadata(ctx)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, table.LatestCommitTime, meta.LastInstantSynced)
	assert.Equal(t, "snap-1", meta.SourceIdentifier)

	// T60: every sync must leave both polytable's own flat keys and Java XTable's single
	// XTABLE_METADATA property behind, or one direction of interop silently regresses.
	assert.Contains(t, meta.CustomProperties, model.KeyLastInstantSynced)
	assert.Contains(t, meta.CustomProperties, model.KeySourceFormat)
	assert.Contains(t, meta.CustomProperties, model.KeyXTableMetadata)
}

// TestDelta_MetadataCarriesKernelRequiredKeys guards the two keys delta-kernel-rs refuses to read a
// log without: metaData.format.options and a metadata object on every schemaString field. Both were
// emitted with omitempty until T29 put DuckDB's delta_scan on the output, which failed the whole
// table on each in turn. The assertions are on the raw JSON on purpose — a round trip through the
// Go source would pass either way, because the reader ignores both keys.
func TestDelta_MetadataCarriesKernelRequiredKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/delta_kernel_keys"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	nested := model.NewRecordSchema("addr", []*model.Field{
		{Name: "city", Schema: model.NewPrimitiveSchema(model.TypeString, true)},
	}, true)
	table := &model.Table{
		Name:             "people",
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       model.NewRecordSchema("people", []*model.Field{idField, {Name: "addr", Schema: nested}}, false),
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	target := delta.NewTarget(memStorage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table: table,
		DataFiles: []*model.DataFile{{
			PhysicalPath:  basePath + "/part-0.parquet",
			FileFormat:    model.FileFormatParquet,
			FileSizeBytes: 512,
			RecordCount:   4,
			LastModified:  time.Now().UnixMilli(),
		}},
		SourceIdentifier: "snap-1",
	}))

	raw, err := memStorage.Read(ctx, io.JoinPath(basePath, "_delta_log", "00000000000000000000.json"))
	require.NoError(t, err)

	var metaAction *delta.MetadataAction
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var action delta.SingleAction
		require.NoError(t, json.Unmarshal([]byte(line), &action))
		if action.MetaData != nil {
			metaAction = action.MetaData
		}
	}
	require.NotNil(t, metaAction)

	encoded, err := json.Marshal(metaAction)
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"options":{}`, "delta-kernel rejects a format without options")
	assert.NotNil(t, metaAction.Format.Options)

	// Every field at every depth, not just the top level.
	assert.Equal(t, 3, strings.Count(metaAction.SchemaString, `"metadata":{}`))
	assert.NotContains(t, metaAction.SchemaString, `"metadata":null`)

	// partitionColumns must be an array, empty for this unpartitioned table, never null. Go
	// marshals a nil slice as null, and delta-kernel-rs then refuses the entire log with
	// "unmasked nulls in non-nullable StructArray child" -- so a null makes the table unreadable
	// by DuckDB and everything else on the kernel, while polytable's own reader is untroubled.
	// Found by converting an unpartitioned Snowflake table and reading the result with DuckDB;
	// every Delta fixture here is partitioned, so no test could have found it.
	assert.Contains(t, string(encoded), `"partitionColumns":[]`,
		"an unpartitioned table must write an empty array, not null")
	assert.NotContains(t, string(encoded), `"partitionColumns":null`)
	assert.NotNil(t, metaAction.PartitionColumns)
}

func TestDelta_DeletionVectors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/delta_dv_table"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	schema := model.NewRecordSchema("dv_test", []*model.Field{idField}, false)

	table := &model.Table{
		Name:             "dv_test",
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dv := &model.DeletionVector{
		StoragePath: "ab89-deletion-vector.bin",
		Offset:      4,
		SizeInBytes: 32,
		Cardinality: 5,
	}

	dataFile := &model.DataFile{
		PhysicalPath:   "mem://lake/delta_dv_table/data.parquet",
		FileFormat:     model.FileFormatParquet,
		FileSizeBytes:  2048,
		RecordCount:    100,
		LastModified:   time.Now().UnixMilli(),
		DeletionVector: dv,
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "0",
	}

	target := delta.NewTarget(memStorage)
	err := target.Init(ctx, table)
	require.NoError(t, err)

	err = target.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	source := delta.NewSource(memStorage, basePath)
	readSnap, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, readSnap.DataFiles, 1)

	readDF := readSnap.DataFiles[0]
	require.NotNil(t, readDF.DeletionVector)
	assert.Equal(t, "ab89-deletion-vector.bin", readDF.DeletionVector.StoragePath)
	assert.Equal(t, int64(4), readDF.DeletionVector.Offset)
	assert.Equal(t, int64(32), readDF.DeletionVector.SizeInBytes)
	assert.Equal(t, int64(5), readDF.DeletionVector.Cardinality)
}

// TestDelta_AbsoluteAddPathSchemesAreNotJoined pins a defect found by checking polytable against a
// defect class the upstream Apache XTable session identified in DeltaActionsConverter.
//
// The Delta protocol permits an absolute add.path for externally-referenced files and shallow
// clones. resolveDataPath tested a hand-written subset of schemes -- s3, gs, mem, file -- and so
// mistook an absolute s3a://, abfss://, abfs://, wasbs:// or wasb:// path for a relative one and
// joined it onto the table root. Hadoop and Spark write s3a://, so this was reachable through the
// most widely used Delta writer there is.
func TestDelta_AbsoluteAddPathSchemesAreNotJoined(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const base = "mem://lake/abs_paths"
	// Every scheme polytable recognises must be treated as already absolute.
	absolute := []string{
		"s3://other/data/f.parquet",
		"s3a://other/data/f.parquet",
		"gs://other/data/f.parquet",
		"abfss://c@a.dfs.core.windows.net/data/f.parquet",
		"abfs://c@a.dfs.core.windows.net/data/f.parquet",
		"wasbs://c@a.blob.core.windows.net/data/f.parquet",
		"wasb://c@a.blob.core.windows.net/data/f.parquet",
		"file:///tmp/data/f.parquet",
	}

	memStorage := io.NewMemoryStorage()
	var log strings.Builder
	log.WriteString(`{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}` + "\n")
	log.WriteString(`{"metaData":{"id":"t","format":{"provider":"parquet","options":{}},` +
		`"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",` +
		`"partitionColumns":[],"configuration":{},"createdTime":1}}` + "\n")
	for _, p := range absolute {
		log.WriteString(`{"add":{"path":"` + p + `","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}` + "\n")
	}
	require.NoError(t, memStorage.Write(ctx, io.JoinPath(base, "_delta_log", "00000000000000000000.json"), []byte(log.String())))

	src := delta.NewSource(memStorage, base)
	snap, err := src.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snap.DataFiles, len(absolute))

	got := make([]string, 0, len(snap.DataFiles))
	for _, df := range snap.DataFiles {
		got = append(got, df.PhysicalPath)
		assert.NotContains(t, df.PhysicalPath, base+"/",
			"an absolute path must not be joined onto the table root")
	}
	assert.ElementsMatch(t, absolute, got)
}
