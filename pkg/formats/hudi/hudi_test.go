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

package hudi_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats/delta"
	"github.com/slachiewicz/polytable/pkg/formats/hudi"
	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

func TestHudi_PropertiesParsing(t *testing.T) {
	t.Parallel()

	rawProps := []byte(`
# Hudi Properties Sample
hoodie.table.name=test_table
hoodie.table.type=COPY_ON_WRITE
hoodie.table.version=6
hoodie.table.partition.fields=region,year
`)

	props, err := hudi.ParseProperties(rawProps)
	require.NoError(t, err)
	assert.Equal(t, "test_table", props.Get(hudi.PropTableName))
	assert.Equal(t, "COPY_ON_WRITE", props.Get(hudi.PropTableType))
	assert.Equal(t, "region,year", props.Get(hudi.PropPartitionFields))

	serialized := props.Serialize()
	reparsed, err := hudi.ParseProperties(serialized)
	require.NoError(t, err)
	assert.Equal(t, "test_table", reparsed.Get(hudi.PropTableName))
}

func TestHudi_SchemaRoundTrip(t *testing.T) {
	t.Parallel()

	idField := &model.Field{Name: "account_id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	emailField := &model.Field{Name: "email", Schema: model.NewPrimitiveSchema(model.TypeString, true)}
	balanceField := &model.Field{Name: "balance", Schema: model.NewDecimalSchema(14, 2, false)}
	createdField := &model.Field{Name: "created_at", Schema: model.NewPrimitiveSchema(model.TypeTimestamp, false)}

	origSchema := model.NewRecordSchema("account", []*model.Field{idField, emailField, balanceField, createdField}, false)

	avroJSON, err := hudi.SchemaToAvroJSON(origSchema, "account", "com.example")
	require.NoError(t, err)
	assert.Contains(t, avroJSON, `"name":"account_id"`)
	assert.Contains(t, avroJSON, `"type":"long"`)
	assert.Contains(t, avroJSON, `"logicalType":"decimal"`)

	parsedSchema, err := hudi.AvroJSONToSchema(avroJSON)
	require.NoError(t, err)
	require.Len(t, parsedSchema.Fields, 4)

	assert.Equal(t, "account_id", parsedSchema.Fields[0].Name)
	assert.Equal(t, model.TypeLong, parsedSchema.Fields[0].Schema.DataType)
	assert.False(t, parsedSchema.Fields[0].Schema.IsNullable)

	assert.Equal(t, "email", parsedSchema.Fields[1].Name)
	assert.Equal(t, model.TypeString, parsedSchema.Fields[1].Schema.DataType)
	assert.True(t, parsedSchema.Fields[1].Schema.IsNullable)

	assert.Equal(t, "balance", parsedSchema.Fields[2].Name)
	assert.Equal(t, model.TypeDecimal, parsedSchema.Fields[2].Schema.DataType)
}

func TestHudi_SnapshotCommitAndRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/hudi_trips"

	idField := &model.Field{Name: "trip_id", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	cityField := &model.Field{Name: "city", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	fareField := &model.Field{Name: "fare", Schema: model.NewPrimitiveSchema(model.TypeDouble, false)}
	schema := model.NewRecordSchema("trips", []*model.Field{idField, cityField, fareField}, false)

	partField := &model.PartitionField{
		SourceField:   cityField,
		TransformType: model.PartitionTransformValue,
	}

	table := &model.Table{
		Name:               "trips",
		TableFormat:        model.TableFormatHudi,
		ReadSchema:         schema,
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	dataFile1 := &model.DataFile{
		PhysicalPath:  "mem://lake/hudi_trips/city=SF/trip_data_0.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 5120,
		RecordCount:   250,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange("city=SF")},
		},
		LastModified: time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile1},
		SourceIdentifier: "20260812165200000",
	}

	// 1. Commit snapshot using Hudi Target
	target := hudi.NewTarget(memStorage)
	err := target.Init(ctx, table)
	require.NoError(t, err)

	err = target.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 2. Read snapshot using Hudi Source
	source := hudi.NewSource(memStorage, basePath)
	currentTable, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	assert.Equal(t, "trips", currentTable.Name)
	assert.Equal(t, model.TableFormatHudi, currentTable.TableFormat)
	require.Len(t, currentTable.PartitioningFields, 1)
	assert.Equal(t, "city", currentTable.PartitioningFields[0].SourceField.Name)

	readSnapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, readSnapshot.DataFiles, 1)

	readDF := readSnapshot.DataFiles[0]
	assert.Equal(t, int64(250), readDF.RecordCount)
	assert.Equal(t, int64(5120), readDF.FileSizeBytes)

	// 3. Verify TableSyncMetadata
	meta, err := target.GetTableMetadata(ctx)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, table.LatestCommitTime, meta.LastInstantSynced)
	assert.Equal(t, "20260812165200000", meta.SourceIdentifier)

	// T60: every sync must leave both polytable's own flat keys and Java XTable's single
	// XTABLE_METADATA property behind (here, in the latest commit's extraMetadata, which is where
	// Java's own Hudi target reads and writes it), or one direction of interop silently regresses.
	assert.Contains(t, meta.CustomProperties, model.KeyLastInstantSynced)
	assert.Contains(t, meta.CustomProperties, model.KeySourceFormat)
	assert.Contains(t, meta.CustomProperties, model.KeyXTableMetadata)
}

// TestHudi_NullPartitionValueDoesNotBecomeLiteralNil is the write-side guard adjacent to T70
// defect 2: a nil Range.MinValue (a genuine null partition value) must become the
// __HIVE_DEFAULT_PARTITION__ marker Java XTable's own hudi.PathBasedPartitionValuesExtractor reads
// back to null, never the literal string "<nil>" that fmt.Sprintf("%v", nil) would fabricate.
func TestHudi_NullPartitionValueDoesNotBecomeLiteralNil(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/hudi_null_partition"

	regionField := &model.Field{Name: "region", Schema: model.NewPrimitiveSchema(model.TypeString, true)}
	schema := model.NewRecordSchema("events", []*model.Field{regionField}, false)
	partField := &model.PartitionField{SourceField: regionField, TransformType: model.PartitionTransformValue}

	table := &model.Table{
		Name:               "events",
		TableFormat:        model.TableFormatHudi,
		ReadSchema:         schema,
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  "mem://lake/hudi_null_partition/region=__HIVE_DEFAULT_PARTITION__/part-0.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 100,
		RecordCount:   1,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange(nil)},
		},
		LastModified: time.Now().UnixMilli(),
	}

	target := hudi.NewTarget(memStorage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "1",
	}))

	source := hudi.NewSource(memStorage, basePath)
	instants, err := source.ListCompletedCommits(ctx)
	require.NoError(t, err)
	require.Len(t, instants, 1)

	commitPath := io.JoinPath(basePath, ".hoodie", instants[0].FileName)
	raw, err := memStorage.Read(ctx, commitPath)
	require.NoError(t, err)
	assert.Contains(t, string(raw), "__HIVE_DEFAULT_PARTITION__")
	assert.NotContains(t, string(raw), "<nil>")
}

func TestHudi_CrossFormatSync(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/sync_delta_to_hudi_and_iceberg"

	// 1. Create source Delta table
	idField := &model.Field{Name: "rider_id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	nameField := &model.Field{Name: "name", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	schema := model.NewRecordSchema("riders", []*model.Field{idField, nameField}, false)

	table := &model.Table{
		Name:             "riders",
		TableFormat:      model.TableFormatDelta,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  "mem://lake/sync_delta_to_hudi_and_iceberg/data/riders.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 1024,
		RecordCount:   50,
		LastModified:  time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "0",
	}

	deltaTarget := delta.NewTarget(memStorage)
	err := deltaTarget.Init(ctx, table)
	require.NoError(t, err)
	err = deltaTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 2. Sync Delta -> [HUDI, ICEBERG]
	controller := conversion.NewController(memStorage)
	datasetConfig := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatHudi, model.TableFormatIceberg},
		TableBasePath: basePath,
		TableName:     "riders",
		SyncMode:      spi.SyncModeFull,
	}

	results, err := controller.Sync(ctx, datasetConfig)
	require.NoError(t, err)
	require.Len(t, results, 2)

	assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatHudi].StatusCode)
	assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)

	// 3. Verify Hudi Source can read synced table
	hudiSource := hudi.NewSource(memStorage, basePath)
	hudiTable, err := hudiSource.GetCurrentTable(ctx)
	require.NoError(t, err)
	assert.Equal(t, model.TableFormatHudi, hudiTable.TableFormat)
	require.Len(t, hudiTable.ReadSchema.Fields, 2)

	hudiSnap, err := hudiSource.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, hudiSnap.DataFiles, 1)
	assert.Equal(t, int64(50), hudiSnap.DataFiles[0].RecordCount)

	// 4. Verify Iceberg Source can read synced table
	icebergSource := iceberg.NewSource(memStorage, basePath)
	icebergSnap, err := icebergSource.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, icebergSnap.DataFiles, 1)
	assert.Equal(t, int64(50), icebergSnap.DataFiles[0].RecordCount)
}
