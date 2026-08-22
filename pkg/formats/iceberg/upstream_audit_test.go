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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/model"
)

// TestUpstream797_NestedFieldComments ports upstream #797 ("iceberg: nested comments with qualified
// name", Java abbf4b7, TestIcebergSchemaSync).
//
// Java addressed columns by name when applying a schema update, and for a nested field the bare
// name is not addressable: updateColumnDoc("int_field", ...) has to be
// updateColumnDoc("record.int_field", ...). The fix qualified the name with its parent path, and
// the test asserts the qualified forms for a struct, a list element and a map value.
//
// The Go target writes whole metadata rather than issuing name-addressed column updates, so there
// is no name to under-qualify. The equivalent exposure is one level down, in the conversion itself:
// the audit found nested docs dropped on the way out (the record branch of the type converter never
// set Doc) and never read back into Comment in either direction, so a nested comment could not
// survive a sync at all. Both sites are fixed; this pins them.
func TestUpstream797_NestedFieldComments(t *testing.T) {
	t.Parallel()

	// A schema shaped like the Java test's: a comment on a top-level field, on a field inside a
	// struct, on a list element and on a map value.
	elementSchema := model.NewPrimitiveSchema(model.TypeString, true)
	elementSchema.Comment = "element doc"
	valueSchema := model.NewPrimitiveSchema(model.TypeString, true)
	valueSchema.Comment = "value doc"

	intField := &model.Field{Name: "int_field", Schema: model.NewPrimitiveSchema(model.TypeInt, true)}
	intField.Schema.Comment = "nested int doc"

	recordSchema := model.NewRecordSchema("record", []*model.Field{intField}, true)
	recordSchema.Comment = "record doc"

	topLevel := model.NewPrimitiveSchema(model.TypeString, true)
	topLevel.Comment = "top level doc"

	source := model.NewRecordSchema("root", []*model.Field{
		{Name: "top_field", Schema: topLevel},
		{Name: "record", Schema: recordSchema},
		{Name: "array_field", Schema: &model.Schema{
			DataType:      model.TypeList,
			IsNullable:    true,
			ElementSchema: &model.Field{Name: "element", Schema: elementSchema},
		}},
		{Name: "map_field", Schema: &model.Schema{
			DataType:    model.TypeMap,
			IsNullable:  true,
			KeySchema:   &model.Field{Name: "key", Schema: model.NewPrimitiveSchema(model.TypeString, false)},
			ValueSchema: &model.Field{Name: "value", Schema: valueSchema},
		}},
	}, false)

	icebergSchema, _, err := iceberg.SchemaToIceberg(source, 0, nil, 0)
	require.NoError(t, err)

	// Round trip through JSON, which is how a target's metadata reaches a source.
	encoded, err := json.Marshal(icebergSchema)
	require.NoError(t, err)
	var decoded iceberg.TableSchema
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	roundTripped, err := iceberg.IcebergToSchema(&decoded)
	require.NoError(t, err)

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "top level field", path: "top_field", want: "top level doc"},
		{name: "struct itself", path: "record", want: "record doc"},
		{name: "field inside a struct", path: "record.int_field", want: "nested int doc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			field := roundTripped.FieldByPath(tt.path)
			require.NotNil(t, field, "field %s missing after the round trip", tt.path)
			assert.Equal(t, tt.want, field.Schema.Comment)
		})
	}

	t.Run("emitted metadata carries the nested doc", func(t *testing.T) {
		t.Parallel()

		// The doc has to be on the wire, not merely reconstructed: an Iceberg reader other than
		// this one gets nothing but the JSON.
		assert.Contains(t, string(encoded), `"doc":"nested int doc"`)
		assert.Contains(t, string(encoded), `"doc":"top level doc"`)
	})

	t.Run("list element and map value docs", func(t *testing.T) {
		t.Parallel()

		// Element and value docs have no NestedField of their own in the Iceberg JSON — the
		// element/value slots are bare types — so they are not expected to survive. Asserted so
		// that the boundary of the fix is explicit rather than assumed.
		arrayField := roundTripped.FieldByPath("array_field")
		require.NotNil(t, arrayField)
		require.NotNil(t, arrayField.Schema.ElementSchema)
		assert.Empty(t, arrayField.Schema.ElementSchema.Schema.Comment)

		mapField := roundTripped.FieldByPath("map_field")
		require.NotNil(t, mapField)
		require.NotNil(t, mapField.Schema.ValueSchema)
		assert.Empty(t, mapField.Schema.ValueSchema.Schema.Comment)
	})
}
