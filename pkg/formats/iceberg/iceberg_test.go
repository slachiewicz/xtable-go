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

package iceberg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestIceberg_SchemaRoundTrip(t *testing.T) {
	t.Parallel()

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	nameField := &model.Field{Name: "name", Schema: model.NewPrimitiveSchema(model.TypeString, true)}
	priceField := &model.Field{Name: "price", Schema: model.NewDecimalSchema(12, 4, true)}
	createdField := &model.Field{Name: "created_at", Schema: model.NewPrimitiveSchema(model.TypeTimestamp, false)}

	origSchema := model.NewRecordSchema("products", []*model.Field{idField, nameField, priceField, createdField}, false)

	icebergSchema, lastColID, err := iceberg.SchemaToIceberg(origSchema, 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, lastColID, 4)
	require.Len(t, icebergSchema.Fields, 4)

	assert.Equal(t, "id", icebergSchema.Fields[0].Name)
	assert.Equal(t, "int", icebergSchema.Fields[0].Type)
	assert.True(t, icebergSchema.Fields[0].Required)

	assert.Equal(t, "price", icebergSchema.Fields[2].Name)
	assert.Equal(t, "decimal(12,4)", icebergSchema.Fields[2].Type)

	// Roundtrip back to canonical schema
	canonicalSchema, err := iceberg.IcebergToSchema(icebergSchema)
	require.NoError(t, err)
	require.Len(t, canonicalSchema.Fields, 4)

	assert.Equal(t, "id", canonicalSchema.Fields[0].Name)
	assert.Equal(t, model.TypeInt, canonicalSchema.Fields[0].Schema.DataType)
	assert.Equal(t, "price", canonicalSchema.Fields[2].Name)
	assert.Equal(t, model.TypeDecimal, canonicalSchema.Fields[2].Schema.DataType)
}

// TestIceberg_MetadataFileVersion covers both metadata file naming conventions. The
// `<version>-<uuid>.metadata.json` form is what every catalog-backed writer emits — pyiceberg, the
// Java library, Spark — and matching only polytable's own `v<N>.metadata.json` made a table written
// by any of them look empty.
func TestIceberg_MetadataFileVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		file   string
		want   int
		wantOK bool
	}{
		{name: "polytable form", file: "v3.metadata.json", want: 3, wantOK: true},
		{name: "polytable form at zero", file: "v0.metadata.json", want: 0, wantOK: true},
		{name: "catalog form", file: "00002-4163fc97-f5ea-4684-b936-1836edf04b60.metadata.json", want: 2, wantOK: true},
		{name: "catalog form at zero", file: "00000-12b8b0f8-4b89-4a8a-9fde-41825354af52.metadata.json", want: 0, wantOK: true},
		{name: "catalog form beyond five digits", file: "123456-abc.metadata.json", want: 123456, wantOK: true},
		{name: "manifest list", file: "snap-7248439550557988192-0-950c80ca.avro"},
		{name: "no version", file: "metadata.json"},
		{name: "unparsable version", file: "vlatest.metadata.json"},
		{name: "unparsable catalog version", file: "current-abc.metadata.json"},
		{name: "empty stem", file: ".metadata.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := iceberg.MetadataFileVersion(tt.file)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestIceberg_SnapshotCommitAndRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/iceberg_table"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	regionField := &model.Field{Name: "region", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	schema := model.NewRecordSchema("orders", []*model.Field{idField, regionField}, false)

	partField := &model.PartitionField{
		SourceField:   regionField,
		TransformType: model.PartitionTransformValue,
	}

	table := &model.Table{
		Name:               "orders",
		TableFormat:        model.TableFormatIceberg,
		ReadSchema:         schema,
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	dataFile1 := &model.DataFile{
		PhysicalPath:  "mem://lake/iceberg_table/data/region=eu/part-1.parquet",
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 4096,
		RecordCount:   100,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange("eu")},
		},
		LastModified: time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile1},
		SourceIdentifier: "snap-1",
	}

	// 1. Commit snapshot using Target
	target := iceberg.NewTarget(memStorage)
	err := target.Init(ctx, table)
	require.NoError(t, err)

	err = target.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 2. Read snapshot using Source
	source := iceberg.NewSource(memStorage, basePath)
	currentTable, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	assert.Equal(t, model.TableFormatIceberg, currentTable.TableFormat)
	require.Len(t, currentTable.PartitioningFields, 1)
	assert.Equal(t, "region", currentTable.PartitioningFields[0].SourceField.Name)

	readSnapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, readSnapshot.DataFiles, 1)

	readDF := readSnapshot.DataFiles[0]
	assert.Equal(t, int64(100), readDF.RecordCount)
	assert.Equal(t, int64(4096), readDF.FileSizeBytes)
	require.Len(t, readDF.PartitionValues, 1)
	assert.Equal(t, "eu", readDF.PartitionValues[0].Range.MinValue)

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

// TestIceberg_PartitionSpecFieldsIsArrayNotNull pins the second instance of a defect first found in
// the Delta target. PartitionSpec.Fields carries no omitempty, so a nil slice marshals to null,
// and the Iceberg specification requires an array -- empty for an unpartitioned table. DuckDB's
// reader refuses such metadata outright:
//
//	Invalid Input Error: PartitionSpec property 'fields' is not of type 'array', found 'null' instead
//
// Every partitioned fixture appends at least one field and so cannot see this, which is how the
// identical bug survived in the Delta target's partitionColumns until a foreign reader found it.
// Both were fixed on the same day; this test exists so the third one is caught here instead.
func TestIceberg_PartitionSpecFieldsIsArrayNotNull(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/iceberg_unpartitioned"

	table := &model.Table{
		Name:        "events",
		TableFormat: model.TableFormatIceberg,
		ReadSchema: model.NewRecordSchema("events", []*model.Field{
			{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)},
		}, false),
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
		// Deliberately no PartitioningFields: that is the case that produces the nil slice.
	}

	target := iceberg.NewTarget(memStorage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table: table,
		DataFiles: []*model.DataFile{{
			PhysicalPath:  basePath + "/data/part-0.parquet",
			FileFormat:    model.FileFormatParquet,
			FileSizeBytes: 256,
			RecordCount:   2,
			LastModified:  time.Now().UnixMilli(),
		}},
		SourceIdentifier: "snap-1",
	}))

	var found bool
	entries, err := memStorage.List(ctx, io.JoinPath(basePath, "metadata"))
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir || !strings.HasSuffix(e.Path, ".metadata.json") {
			continue
		}
		raw, readErr := memStorage.Read(ctx, e.Path)
		require.NoError(t, readErr)
		assert.Contains(t, string(raw), `"fields":[]`,
			"an unpartitioned table must write an empty array, not null")
		assert.NotContains(t, string(raw), `"fields":null`)
		found = true
	}
	require.True(t, found, "expected at least one metadata.json to inspect")
}
