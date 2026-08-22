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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/delta"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// TestUpstream826_MapKeyAndValueSides ports upstream #826 ("Fix map key path handling in Delta
// schema extractors", Java a4e729d, TestDeltaSchemaExtractor#testMapWithStructKey).
//
// The Java defect was in the *path* the extractor gave a map key's nested fields: it qualified them
// with MAP_VALUE_FIELD_NAME instead of MAP_KEY_FIELD_NAME, so a struct-keyed map produced two
// subtrees claiming to live under the value side. This port has no counterpart for that half —
// model.Field.ParentPath is never populated by any Go format adapter, so there is no path to
// mis-qualify (recorded as a parity gap under T25).
//
// What does port is the semantic payload the Java test asserts: a struct-keyed map must land its
// key struct on KeySchema and its value struct on ValueSchema, with the nullability each side
// carries in Delta. A rewrite can transpose those two as easily as it can mis-name a path.
func TestUpstream826_MapKeyAndValueSides(t *testing.T) {
	t.Parallel()

	const structKeyMapJSON = `{"type":"struct","fields":[
		{"name":"structKeyMap","nullable":true,"metadata":{},"type":{
			"type":"map",
			"keyType":{"type":"struct","fields":[
				{"name":"id","type":"long","nullable":false,"metadata":{}},
				{"name":"region","type":"string","nullable":true,"metadata":{}}]},
			"valueType":{"type":"struct","fields":[
				{"name":"payload","type":"string","nullable":false,"metadata":{}}]},
			"valueContainsNull":true}}]}`

	tests := []struct {
		name  string
		check func(t *testing.T, mapSchema *model.Schema)
	}{
		{
			name: "key side carries the key struct",
			check: func(t *testing.T, mapSchema *model.Schema) {
				key := mapSchema.KeySchema
				require.NotNil(t, key)
				assert.Equal(t, "key", key.Name)
				require.Equal(t, model.TypeRecord, key.Schema.DataType)
				// Delta map keys are never null.
				assert.False(t, key.Schema.IsNullable)

				require.Len(t, key.Schema.Fields, 2)
				assert.Equal(t, "id", key.Schema.Fields[0].Name)
				assert.Equal(t, model.TypeLong, key.Schema.Fields[0].Schema.DataType)
				assert.False(t, key.Schema.Fields[0].Schema.IsNullable)
				assert.Equal(t, "region", key.Schema.Fields[1].Name)
				assert.Equal(t, model.TypeString, key.Schema.Fields[1].Schema.DataType)
				assert.True(t, key.Schema.Fields[1].Schema.IsNullable)
			},
		},
		{
			name: "value side carries the value struct",
			check: func(t *testing.T, mapSchema *model.Schema) {
				value := mapSchema.ValueSchema
				require.NotNil(t, value)
				assert.Equal(t, "value", value.Name)
				require.Equal(t, model.TypeRecord, value.Schema.DataType)
				// valueContainsNull: true.
				assert.True(t, value.Schema.IsNullable)

				require.Len(t, value.Schema.Fields, 1)
				assert.Equal(t, "payload", value.Schema.Fields[0].Name)
				assert.Equal(t, model.TypeString, value.Schema.Fields[0].Schema.DataType)
				assert.False(t, value.Schema.Fields[0].Schema.IsNullable)
			},
		},
		{
			name: "the two sides are distinct subtrees",
			check: func(t *testing.T, mapSchema *model.Schema) {
				require.NotNil(t, mapSchema.KeySchema)
				require.NotNil(t, mapSchema.ValueSchema)
				assert.NotEqual(t, mapSchema.KeySchema.Schema, mapSchema.ValueSchema.Schema)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			schema, err := delta.DeltaJSONToSchema(structKeyMapJSON)
			require.NoError(t, err)
			require.Len(t, schema.Fields, 1)
			require.Equal(t, "structKeyMap", schema.Fields[0].Name)
			require.Equal(t, model.TypeMap, schema.Fields[0].Schema.DataType)
			require.True(t, schema.Fields[0].Schema.IsNullable)

			tt.check(t, schema.Fields[0].Schema)
		})
	}

	t.Run("survives a write-read round trip", func(t *testing.T) {
		t.Parallel()

		schema, err := delta.DeltaJSONToSchema(structKeyMapJSON)
		require.NoError(t, err)

		written, err := delta.SchemaToDeltaJSON(schema)
		require.NoError(t, err)
		reparsed, err := delta.DeltaJSONToSchema(written)
		require.NoError(t, err)

		mapSchema := reparsed.Fields[0].Schema
		require.Equal(t, model.TypeMap, mapSchema.DataType)
		require.Len(t, mapSchema.KeySchema.Schema.Fields, 2)
		assert.Equal(t, "id", mapSchema.KeySchema.Schema.Fields[0].Name)
		require.Len(t, mapSchema.ValueSchema.Schema.Fields, 1)
		assert.Equal(t, "payload", mapSchema.ValueSchema.Schema.Fields[0].Name)
	})
}

// TestUpstream795_BinaryInsideMapAndArray ports upstream #795 ("avoid NPE for binary in map/array
// schemas", Java 8cab6a2, TestDeltaSchemaExtractor#testBinaryInMapAndArrayWithoutMetadata).
//
// The Java defect was a null dereference: the BINARY branch read the field's metadata to decide
// between BYTES and the xtable UUID logical type, and a binary nested inside a map or array has no
// StructField and therefore no metadata. Go's parser takes a json.RawMessage type node with the
// nullability passed alongside and never consults metadata, so the dereference is structurally
// impossible — but "impossible" is worth a fixture, since the nesting is what made Java's
// assumption wrong in the first place.
func TestUpstream795_BinaryInsideMapAndArray(t *testing.T) {
	t.Parallel()

	const binaryNestingJSON = `{"type":"struct","fields":[
		{"name":"binaryList","nullable":false,"metadata":{},"type":{
			"type":"array","elementType":"binary","containsNull":false}},
		{"name":"binaryMap","nullable":false,"metadata":{},"type":{
			"type":"map","keyType":"string","valueType":"binary","valueContainsNull":false}},
		{"name":"binaryInStructInMap","nullable":true,"metadata":{},"type":{
			"type":"map","keyType":"string",
			"valueType":{"type":"struct","fields":[
				{"name":"blob","type":"binary","nullable":true,"metadata":{}}]},
			"valueContainsNull":true}},
		{"name":"topLevelBinary","nullable":true,"metadata":{},"type":"binary"}]}`

	tests := []struct {
		name  string
		field string
		want  func(t *testing.T, s *model.Schema)
	}{
		{
			name:  "binary array element",
			field: "binaryList",
			want: func(t *testing.T, s *model.Schema) {
				require.Equal(t, model.TypeList, s.DataType)
				require.NotNil(t, s.ElementSchema)
				assert.Equal(t, model.TypeBytes, s.ElementSchema.Schema.DataType)
				assert.False(t, s.ElementSchema.Schema.IsNullable)
			},
		},
		{
			name:  "binary map value",
			field: "binaryMap",
			want: func(t *testing.T, s *model.Schema) {
				require.Equal(t, model.TypeMap, s.DataType)
				require.NotNil(t, s.ValueSchema)
				assert.Equal(t, model.TypeString, s.KeySchema.Schema.DataType)
				assert.Equal(t, model.TypeBytes, s.ValueSchema.Schema.DataType)
				assert.False(t, s.ValueSchema.Schema.IsNullable)
			},
		},
		{
			name:  "binary inside a struct inside a map",
			field: "binaryInStructInMap",
			want: func(t *testing.T, s *model.Schema) {
				require.Equal(t, model.TypeMap, s.DataType)
				require.Equal(t, model.TypeRecord, s.ValueSchema.Schema.DataType)
				require.Len(t, s.ValueSchema.Schema.Fields, 1)
				assert.Equal(t, model.TypeBytes, s.ValueSchema.Schema.Fields[0].Schema.DataType)
			},
		},
		{
			name:  "top level binary",
			field: "topLevelBinary",
			want: func(t *testing.T, s *model.Schema) {
				assert.Equal(t, model.TypeBytes, s.DataType)
				assert.True(t, s.IsNullable)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			schema, err := delta.DeltaJSONToSchema(binaryNestingJSON)
			require.NoError(t, err)

			field := schema.FieldByPath(tt.field)
			require.NotNil(t, field, "field %s missing from parsed schema", tt.field)
			tt.want(t, field.Schema)
		})
	}
}

// TestUpstream828_PartitionValueComponents ports upstream #828 ("null partition value for composite
// generated-column partitions", Java 244003d,
// TestDeltaPartitionExtractor#testGeneratedPartitionValueExtractionWithNullSource).
//
// The Java defect is specific to generated columns: one internal partition field backed by several
// Delta partition columns (year/month/day derived from a timestamp), whose serialized value was
// built by joining the components with "-". A null source value made every component null, and the
// join rendered them as the literal string "null", handing "null-null-null" to a date parser.
//
// Go has no generated-column partition support at all — every Delta partition column becomes its
// own VALUE-transform field in tableFromMetadata — so there is no join and no corrupt composite to
// produce. These cases pin what the Go reader does with the same shaped input instead: an absent
// component, and a component explicitly written as JSON null.
func TestUpstream828_PartitionValueComponents(t *testing.T) {
	t.Parallel()

	partitionValueFor := func(t *testing.T, file *model.DataFile, column string) (any, bool) {
		t.Helper()
		for _, pv := range file.PartitionValues {
			if pv.PartitionField.SourceField.Name == column {
				return pv.Range.MinValue, true
			}
		}
		return nil, false
	}

	tests := []struct {
		name            string
		partitionValues string
		check           func(t *testing.T, file *model.DataFile)
	}{
		{
			name:            "all components present",
			partitionValues: `{"year":"2013","month":"05","day":"20"}`,
			check: func(t *testing.T, file *model.DataFile) {
				require.Len(t, file.PartitionValues, 3)
				for column, want := range map[string]string{"year": "2013", "month": "05", "day": "20"} {
					got, ok := partitionValueFor(t, file, column)
					require.True(t, ok, "no partition value for %s", column)
					assert.Equal(t, want, got)
				}
			},
		},
		{
			name:            "a component missing from the map",
			partitionValues: `{"year":"2013","day":"20"}`,
			check: func(t *testing.T, file *model.DataFile) {
				// The absent component yields no partition value rather than a placeholder, and
				// crucially the present ones are never fused into a joined literal such as
				// "2013-null-20" — the corruption #828 fixed in Java.
				require.Len(t, file.PartitionValues, 2)
				_, ok := partitionValueFor(t, file, "month")
				assert.False(t, ok, "an absent component must not produce a partition value")
				year, ok := partitionValueFor(t, file, "year")
				require.True(t, ok)
				assert.Equal(t, "2013", year)
			},
		},
		{
			name:            "every component explicitly null",
			partitionValues: `{"year":null,"month":null,"day":null}`,
			check: func(t *testing.T, file *model.DataFile) {
				// Fixed under T70 defect 2: AddAction.PartitionValues is map[string]*string, so a
				// JSON null decodes as a nil *string and the reader reports a nil Range.MinValue
				// rather than "" — distinguishable from a genuinely empty partition value. What
				// matters for #828 is the negative: no component is rendered as the literal "null"
				// and nothing is joined.
				require.Len(t, file.PartitionValues, 3)
				for _, column := range []string{"year", "month", "day"} {
					got, ok := partitionValueFor(t, file, column)
					require.True(t, ok)
					assert.Nil(t, got, "null partition value must not become a literal")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/upstream828"
			writePartitionedDeltaLog(t, storage, basePath, tt.partitionValues)

			snapshot, err := delta.NewSource(storage, basePath).GetCurrentSnapshot(ctx)
			require.NoError(t, err)
			require.Len(t, snapshot.DataFiles, 1)

			file := snapshot.DataFiles[0]
			for _, pv := range file.PartitionValues {
				assert.NotContains(t, fmt.Sprintf("%v", pv.Range.MinValue), "-",
					"a partition value must never be a joined composite")
			}
			tt.check(t, file)
		})
	}
}

// writePartitionedDeltaLog writes a single-commit Delta log for a table partitioned by
// year/month/day, with one add action carrying the supplied raw partitionValues JSON object.
func writePartitionedDeltaLog(t *testing.T, storage io.Storage, basePath, partitionValues string) {
	t.Helper()

	const schemaJSON = `{\"type\":\"struct\",\"fields\":[` +
		`{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},` +
		`{\"name\":\"year\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},` +
		`{\"name\":\"month\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},` +
		`{\"name\":\"day\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}`

	commit := fmt.Sprintf(`{"metaData":{"id":"fixture","name":"fixture","format":{"provider":"parquet"},`+
		`"schemaString":"%s","partitionColumns":["year","month","day"]}}
{"add":{"path":"part-0.parquet","partitionValues":%s,"size":128,"modificationTime":1700000000000,"dataChange":true}}
{"commitInfo":{"timestamp":1700000000000,"operation":"WRITE"}}
`, schemaJSON, partitionValues)

	path := io.JoinPath(basePath, "_delta_log", "00000000000000000000.json")
	require.NoError(t, storage.Write(context.Background(), path, []byte(commit)))
}
