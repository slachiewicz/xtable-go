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

// Tests for T69 (docs/improvement-plan.md) beyond what test/schema_evolution_test.go's
// TestSchemaEvolution_ReorderColumns already covers: a synthetic (Hive-style) partition column,
// which never appears in ReadSchema and so is never seen by SchemaToIceberg's own stability logic;
// nested field ids surviving a reorder inside a struct; and the dropped-then-re-added case, which
// must NOT recover the old id. All three exercise a schema decoded from JSON as prevSchema, which
// is the shape a real metadata.json produces (nested "fields" arrive as []interface{} of
// map[string]interface{}, not []*NestedField) and is the fragile half of fieldPathIDs that
// TestSchemaEvolution_ReorderColumns, being flat, does not reach.
package iceberg_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// jsonRoundTrip forces a TableSchema through the same encode/decode path a real commit takes when
// it reads back a previous metadata.json: NestedField.Type is `any`, so a nested struct's "fields"
// decode as []interface{} of map[string]interface{} rather than the []*NestedField a schema built
// in-process holds. Passing a schema straight from SchemaToIceberg as prevSchema, without this,
// would only ever exercise the easy half of fieldPathIDs.
func jsonRoundTrip(t *testing.T, schema *iceberg.TableSchema) *iceberg.TableSchema {
	t.Helper()
	encoded, err := json.Marshal(schema)
	require.NoError(t, err)
	var decoded iceberg.TableSchema
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	return &decoded
}

// TestIcebergTarget_CommitSnapshot_PartitionColumnIDStable covers the synthetic partition column
// target.go's CommitSnapshot adds when a partition field has no corresponding entry in ReadSchema
// -- the Hive-style layout the surrounding comment describes, where the column lives only in the
// directory name. That branch runs on *every* commit for such a column, because ReadSchema never
// grows the column for SchemaToIceberg to have already stabilized. Before accounting for this, the
// column's id (and so the partition spec's source-id, under an unchanging spec-id) incremented by
// one on every single commit.
func TestIcebergTarget_CommitSnapshot_PartitionColumnIDStable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/partition_id_stable"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	amountField := &model.Field{Name: "amount", Schema: model.NewPrimitiveSchema(model.TypeDouble, true)}
	regionPartition := &model.PartitionField{
		SourceField:   &model.Field{Name: "region", Schema: model.NewPrimitiveSchema(model.TypeString, true)},
		TransformType: model.PartitionTransformValue,
	}
	table := &model.Table{
		Name:               "events",
		TableFormat:        model.TableFormatIceberg,
		ReadSchema:         model.NewRecordSchema("events", []*model.Field{idField, amountField}, false),
		BasePath:           basePath,
		PartitioningFields: []*model.PartitionField{regionPartition},
	}

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))

	commit := func(path string) {
		file := &model.DataFile{PhysicalPath: path, FileFormat: model.FileFormatParquet, FileSizeBytes: 10, RecordCount: 1}
		snap := &model.Snapshot{Table: table, DataFiles: []*model.DataFile{file}, SourceIdentifier: path}
		require.NoError(t, target.CommitSnapshot(ctx, snap))
	}
	commit(io.JoinPath(basePath, "data", "a.parquet"))
	commit(io.JoinPath(basePath, "data", "b.parquet"))
	commit(io.JoinPath(basePath, "data", "c.parquet"))

	meta1 := readMetadata(t, storage, io.JoinPath(basePath, "metadata", "v1.metadata.json"))
	meta2 := readMetadata(t, storage, io.JoinPath(basePath, "metadata", "v2.metadata.json"))
	meta3 := readMetadata(t, storage, io.JoinPath(basePath, "metadata", "v3.metadata.json"))

	regionID := func(meta *iceberg.TableMetadata) int {
		for _, f := range meta.Schemas[0].Fields {
			if f.Name == "region" {
				return f.ID
			}
		}
		t.Fatalf("region field missing from schema in metadata %+v", meta)
		return 0
	}
	sourceID := func(meta *iceberg.TableMetadata) int {
		require.Len(t, meta.PartitionSpecs, 1)
		require.Len(t, meta.PartitionSpecs[0].Fields, 1)
		return meta.PartitionSpecs[0].Fields[0].SourceID
	}

	id1, id2, id3 := regionID(meta1), regionID(meta2), regionID(meta3)
	assert.Equal(t, id1, id2, "region's field id changed between commit 1 and commit 2 with no schema change")
	assert.Equal(t, id2, id3, "region's field id changed between commit 2 and commit 3 with no schema change")

	assert.Equal(t, id1, sourceID(meta1))
	assert.Equal(t, id2, sourceID(meta2))
	assert.Equal(t, id3, sourceID(meta3))

	assert.Equal(t, meta1.LastColumnID, meta2.LastColumnID, "last-column-id must not keep climbing once every column is already known")
	assert.Equal(t, meta2.LastColumnID, meta3.LastColumnID)
}

// TestSchemaToIceberg_NestedFieldsStableAcrossReorder reorders the fields of a nested struct and
// adds a new top-level column in the same step, using a prevSchema that has been through
// jsonRoundTrip -- the shape fieldPathIDs must handle when SchemaToIceberg is fed a real
// metadata.json's schema rather than one this package just built.
func TestSchemaToIceberg_NestedFieldsStableAcrossReorder(t *testing.T) {
	t.Parallel()

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	cityField := &model.Field{Name: "city", Schema: model.NewPrimitiveSchema(model.TypeString, true)}
	zipField := &model.Field{Name: "zip", Schema: model.NewPrimitiveSchema(model.TypeString, true)}
	addrField := &model.Field{Name: "addr", Schema: model.NewRecordSchema("addr", []*model.Field{cityField, zipField}, true)}

	original := model.NewRecordSchema("t", []*model.Field{idField, addrField}, false)
	firstSchema, lastColID, err := iceberg.SchemaToIceberg(original, 0, nil, 0)
	require.NoError(t, err)
	require.Len(t, firstSchema.Fields, 2)

	prevSchema := jsonRoundTrip(t, firstSchema)

	// Reorder addr's fields (zip, city instead of city, zip) and add a new top-level column.
	reorderedAddr := &model.Field{Name: "addr", Schema: model.NewRecordSchema("addr", []*model.Field{zipField, cityField}, true)}
	taxField := &model.Field{Name: "tax", Schema: model.NewPrimitiveSchema(model.TypeDouble, true)}
	reordered := model.NewRecordSchema("t", []*model.Field{idField, reorderedAddr, taxField}, false)

	secondSchema, secondLastColID, err := iceberg.SchemaToIceberg(reordered, 1, prevSchema, lastColID)
	require.NoError(t, err)

	fieldID := func(schema *iceberg.TableSchema, name string) int {
		for _, f := range schema.Fields {
			if f.Name == name {
				return f.ID
			}
		}
		t.Fatalf("field %q not found", name)
		return 0
	}
	nestedFieldID := func(schema *iceberg.TableSchema, parent, name string) int {
		for _, f := range schema.Fields {
			if f.Name != parent {
				continue
			}
			typed, ok := f.Type.(map[string]any)
			require.True(t, ok, "%s is not a struct type: %#v", parent, f.Type)
			fields, ok := typed["fields"].([]*iceberg.NestedField)
			require.True(t, ok, "%s.fields is not []*NestedField: %#v", parent, typed["fields"])
			for _, nf := range fields {
				if nf.Name == name {
					return nf.ID
				}
			}
		}
		t.Fatalf("field %s.%s not found", parent, name)
		return 0
	}

	assert.Equal(t, fieldID(firstSchema, "id"), fieldID(secondSchema, "id"), "id's id must survive the reorder")
	assert.Equal(t, fieldID(firstSchema, "addr"), fieldID(secondSchema, "addr"), "addr's own id must survive the reorder")
	assert.Equal(t, nestedFieldID(firstSchema, "addr", "city"), nestedFieldID(secondSchema, "addr", "city"),
		"addr.city's id must survive its struct being reordered")
	assert.Equal(t, nestedFieldID(firstSchema, "addr", "zip"), nestedFieldID(secondSchema, "addr", "zip"),
		"addr.zip's id must survive its struct being reordered")

	assert.Greater(t, fieldID(secondSchema, "tax"), lastColID, "the new tax column must get a fresh id above the previous last-column-id")
	assert.Greater(t, secondLastColID, lastColID, "last-column-id must advance once a genuinely new column is added")
}

// TestSchemaToIceberg_DroppedThenReAddedColumnGetsFreshID pins the deliberate choice T69's
// acceptance criteria calls out: a column dropped in one commit and re-added by the same name in a
// later one does not recover its old id. prevSchema for the re-add step is the schema from
// immediately after the drop, which -- by construction -- no longer mentions the dropped column at
// all, so it cannot be told apart from a genuinely new column. The data files written while the
// column was absent carry no value for it, so resurrecting the id would let a reader associate the
// wrong file's bytes with the column; a fresh id makes that discontinuity visible instead.
func TestSchemaToIceberg_DroppedThenReAddedColumnGetsFreshID(t *testing.T) {
	t.Parallel()

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	amountField := &model.Field{Name: "amount", Schema: model.NewPrimitiveSchema(model.TypeDouble, true)}

	withAmount := model.NewRecordSchema("t", []*model.Field{idField, amountField}, false)
	schemaV1, lastColV1, err := iceberg.SchemaToIceberg(withAmount, 0, nil, 0)
	require.NoError(t, err)
	amountIDBeforeDrop := 0
	for _, f := range schemaV1.Fields {
		if f.Name == "amount" {
			amountIDBeforeDrop = f.ID
		}
	}
	require.NotZero(t, amountIDBeforeDrop)

	// Drop amount.
	withoutAmount := model.NewRecordSchema("t", []*model.Field{idField}, false)
	schemaV2, lastColV2, err := iceberg.SchemaToIceberg(withoutAmount, 1, jsonRoundTrip(t, schemaV1), lastColV1)
	require.NoError(t, err)
	require.Len(t, schemaV2.Fields, 1, "amount must actually be gone from the schema after the drop")
	assert.Equal(t, lastColV1, lastColV2, "last-column-id must not regress just because a column was dropped")

	// Re-add amount under the same name, using the post-drop schema as prevSchema.
	reAdded := model.NewRecordSchema("t", []*model.Field{idField, amountField}, false)
	schemaV3, lastColV3, err := iceberg.SchemaToIceberg(reAdded, 2, jsonRoundTrip(t, schemaV2), lastColV2)
	require.NoError(t, err)

	amountIDAfterReAdd := 0
	for _, f := range schemaV3.Fields {
		if f.Name == "amount" {
			amountIDAfterReAdd = f.ID
		}
	}
	require.NotZero(t, amountIDAfterReAdd)

	assert.NotEqual(t, amountIDBeforeDrop, amountIDAfterReAdd, "a re-added column must not silently inherit its old id")
	assert.Greater(t, amountIDAfterReAdd, lastColV2, "the re-added column's id must come from above the last-column-id, not reuse a retired one")
	assert.Greater(t, lastColV3, lastColV2)
}
