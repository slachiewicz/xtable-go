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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/paimon"
	"github.com/slachiewicz/polytable/pkg/model"
)

// wrapSingleField builds a one-field record schema, the smallest unit SchemaToPaimon/PaimonToSchema
// accept, so a single type can be pushed through the public API in isolation.
func wrapSingleField(s *model.Schema) *model.Schema {
	return model.NewRecordSchema("t", []*model.Field{{Name: "f", Schema: s}}, false)
}

// roundTrip pushes a single-field schema through SchemaToPaimon and back through PaimonToSchema,
// returning the field's schema after the round trip and the raw JSON the Paimon side produced (for
// grammar assertions).
func roundTrip(t *testing.T, s *model.Schema) (*model.Schema, string, error) {
	t.Helper()

	ts, err := paimon.SchemaToPaimon(wrapSingleField(s), nil)
	if err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(ts)
	require.NoError(t, err)

	back, err := paimon.PaimonToSchema(ts)
	if err != nil {
		return nil, string(raw), err
	}
	require.Len(t, back.Fields, 1)
	return back.Fields[0].Schema, string(raw), nil
}

func TestPaimon_ScalarTypesRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		in       *model.Schema
		wantJSON string     // substring expected in the marshaled "type"
		wantType model.Type // expected type after the round trip, when it differs from in.DataType
	}{
		{"boolean nullable", model.NewPrimitiveSchema(model.TypeBoolean, true), `"BOOLEAN"`, ""},
		{"boolean not null", model.NewPrimitiveSchema(model.TypeBoolean, false), `"BOOLEAN NOT NULL"`, ""},
		{"int", model.NewPrimitiveSchema(model.TypeInt, true), `"INT"`, ""},
		{"long", model.NewPrimitiveSchema(model.TypeLong, true), `"BIGINT"`, ""},
		{"float", model.NewPrimitiveSchema(model.TypeFloat, true), `"FLOAT"`, ""},
		{"double", model.NewPrimitiveSchema(model.TypeDouble, true), `"DOUBLE"`, ""},
		{"string", model.NewPrimitiveSchema(model.TypeString, true), `"STRING"`, ""},
		// UUID has no dedicated Paimon type, so it deliberately widens to STRING (see schema.go) —
		// this is not the round trip failing, it is the documented, intentional exception to it.
		{"uuid widens to string", model.NewPrimitiveSchema(model.TypeUUID, true), `"STRING"`, model.TypeString},
		{"bytes", model.NewPrimitiveSchema(model.TypeBytes, true), `"BYTES"`, ""},
		{"date", model.NewPrimitiveSchema(model.TypeDate, true), `"DATE"`, ""},
		{"decimal", model.NewDecimalSchema(20, 4, true), `"DECIMAL(20, 4)"`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			back, raw, err := roundTrip(t, tt.in)
			require.NoError(t, err)
			assert.Contains(t, raw, tt.wantJSON)
			want := tt.wantType
			if want == "" {
				want = tt.in.DataType
			}
			assert.Equal(t, want, back.DataType)
			assert.Equal(t, tt.in.IsNullable, back.IsNullable)
		})
	}
}

// TestPaimon_TimestampZoneAwarenessSurvivesRoundTrip is the regression test for the third defect:
// TIMESTAMP (zone-aware) and TIMESTAMP_NTZ (naive) must not collapse into the same Paimon spelling.
func TestPaimon_TimestampZoneAwarenessSurvivesRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("zone-aware", func(t *testing.T) {
		t.Parallel()
		back, raw, err := roundTrip(t, model.NewPrimitiveSchema(model.TypeTimestamp, true))
		require.NoError(t, err)
		assert.Contains(t, raw, `"TIMESTAMP(6) WITH LOCAL TIME ZONE"`)
		assert.Equal(t, model.TypeTimestamp, back.DataType)
	})

	t.Run("naive", func(t *testing.T) {
		t.Parallel()
		back, raw, err := roundTrip(t, model.NewPrimitiveSchema(model.TypeTimestampNTZ, true))
		require.NoError(t, err)
		assert.Contains(t, raw, `"TIMESTAMP(6)"`)
		assert.NotContains(t, raw, "WITH LOCAL TIME ZONE")
		assert.Equal(t, model.TypeTimestampNTZ, back.DataType)
	})

	// The two must not be spelled the same way, which is the literal shape of the original defect.
	t.Run("distinct spellings", func(t *testing.T) {
		t.Parallel()
		_, zoneAwareRaw, err := roundTrip(t, model.NewPrimitiveSchema(model.TypeTimestamp, true))
		require.NoError(t, err)
		_, naiveRaw, err := roundTrip(t, model.NewPrimitiveSchema(model.TypeTimestampNTZ, true))
		require.NoError(t, err)
		assert.NotEqual(t, zoneAwareRaw, naiveRaw)
	})
}

func TestPaimon_StructRoundTrip(t *testing.T) {
	t.Parallel()

	inner := model.NewRecordSchema("inner", []*model.Field{
		{Name: "x", Schema: model.NewPrimitiveSchema(model.TypeInt, false)},
		{Name: "y", Schema: model.NewPrimitiveSchema(model.TypeInt, false)},
	}, true)
	outer := model.NewRecordSchema("outer", []*model.Field{
		{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)},
		{Name: "point", Schema: inner},
	}, false)

	back, raw, err := roundTrip(t, outer)
	require.NoError(t, err)
	assert.Contains(t, raw, `"ROW`)
	assert.NotContains(t, raw, `STRING`, "a struct must not silently narrow to STRING")

	require.Equal(t, model.TypeRecord, back.DataType)
	require.Len(t, back.Fields, 2)
	assert.Equal(t, "id", back.Fields[0].Name)
	assert.Equal(t, model.TypeLong, back.Fields[0].Schema.DataType)
	assert.Equal(t, "point", back.Fields[1].Name)
	require.Equal(t, model.TypeRecord, back.Fields[1].Schema.DataType)
	require.Len(t, back.Fields[1].Schema.Fields, 2)
	assert.Equal(t, "x", back.Fields[1].Schema.Fields[0].Name)
	assert.Equal(t, model.TypeInt, back.Fields[1].Schema.Fields[0].Schema.DataType)
}

func TestPaimon_ListOfStructsRoundTrip(t *testing.T) {
	t.Parallel()

	element := model.NewRecordSchema("elem", []*model.Field{
		{Name: "k", Schema: model.NewPrimitiveSchema(model.TypeString, false)},
		{Name: "v", Schema: model.NewPrimitiveSchema(model.TypeDouble, true)},
	}, false)
	list := &model.Schema{
		DataType:      model.TypeList,
		IsNullable:    true,
		ElementSchema: &model.Field{Name: "element", Schema: element},
		Metadata:      map[model.MetadataKey]any{},
	}

	back, raw, err := roundTrip(t, list)
	require.NoError(t, err)
	assert.Contains(t, raw, `"ARRAY`)
	assert.Contains(t, raw, `"ROW`)

	require.Equal(t, model.TypeList, back.DataType)
	require.NotNil(t, back.ElementSchema)
	require.Equal(t, model.TypeRecord, back.ElementSchema.Schema.DataType)
	require.Len(t, back.ElementSchema.Schema.Fields, 2)
	assert.Equal(t, "k", back.ElementSchema.Schema.Fields[0].Name)
	assert.Equal(t, model.TypeString, back.ElementSchema.Schema.Fields[0].Schema.DataType)
}

// TestPaimon_MapWithNonTrivialKeyRoundTrip covers a map whose key is not a bare scalar name — here a
// DECIMAL key with a STRUCT value — so both MAP arms exercise real nesting rather than atomic leaves
// on both sides.
func TestPaimon_MapWithNonTrivialKeyRoundTrip(t *testing.T) {
	t.Parallel()

	key := model.NewDecimalSchema(12, 2, false)
	value := model.NewRecordSchema("v", []*model.Field{
		{Name: "count", Schema: model.NewPrimitiveSchema(model.TypeLong, false)},
	}, false)
	m := &model.Schema{
		DataType:    model.TypeMap,
		IsNullable:  false,
		KeySchema:   &model.Field{Name: "key", Schema: key},
		ValueSchema: &model.Field{Name: "value", Schema: value},
		Metadata:    map[model.MetadataKey]any{},
	}

	back, raw, err := roundTrip(t, m)
	require.NoError(t, err)
	assert.Contains(t, raw, `"MAP NOT NULL"`)
	assert.Contains(t, raw, `"DECIMAL(12, 2) NOT NULL"`)
	assert.Contains(t, raw, `"ROW`)

	require.Equal(t, model.TypeMap, back.DataType)
	assert.False(t, back.IsNullable)
	require.NotNil(t, back.KeySchema)
	assert.Equal(t, model.TypeDecimal, back.KeySchema.Schema.DataType)
	require.NotNil(t, back.ValueSchema)
	require.Equal(t, model.TypeRecord, back.ValueSchema.Schema.DataType)
	require.Len(t, back.ValueSchema.Schema.Fields, 1)
	assert.Equal(t, "count", back.ValueSchema.Schema.Fields[0].Name)
}

// TestPaimon_UnsupportedWriteTypeErrorsNamed is the regression test for the "no silent STRING"
// requirement on the write side: ENUM has no Paimon representation and must fail loudly, naming the
// type, rather than being written as a STRING column nobody would notice was wrong.
func TestPaimon_UnsupportedWriteTypeErrorsNamed(t *testing.T) {
	t.Parallel()

	enumSchema := &model.Schema{DataType: model.TypeEnum, IsNullable: true, Metadata: map[model.MetadataKey]any{}}
	_, _, err := roundTrip(t, enumSchema)
	require.Error(t, err)
	assert.ErrorIs(t, err, paimon.ErrUnsupportedPaimonType)
	assert.Contains(t, err.Error(), "ENUM")
}

// TestPaimon_UnsupportedReadTypeErrorsNamed is the read-side counterpart: a Paimon type this package
// cannot represent (TIME has no model.Type) must fail by name instead of being narrowed to STRING.
func TestPaimon_UnsupportedReadTypeErrorsNamed(t *testing.T) {
	t.Parallel()

	schemaJSON := `{
		"id": 0,
		"fields": [{"id": 1, "name": "t", "type": "TIME(3)"}],
		"highestFieldId": 1
	}`
	ts, err := paimon.ParseTableSchemaJSON([]byte(schemaJSON))
	require.NoError(t, err)

	_, err = paimon.PaimonToSchema(ts)
	require.Error(t, err)
	assert.ErrorIs(t, err, paimon.ErrUnsupportedPaimonType)
	assert.Contains(t, err.Error(), "TIME")
}

// TestPaimon_ParseHandWrittenCompositeFixture parses a JSON fixture shaped exactly the way
// org.apache.paimon.types.DataTypeJsonParser expects (nested objects, not a flat string), standing in
// for a schema file written by real Paimon rather than by this package. This is the closest available
// substitute for a foreign-engine conformance check: no engine has ever read this package's own
// Paimon output (see the commit message), so this fixture is hand-derived from Paimon 1.3.1 sources.
func TestPaimon_ParseHandWrittenCompositeFixture(t *testing.T) {
	t.Parallel()

	schemaJSON := `{
		"id": 0,
		"fields": [
			{
				"id": 1,
				"name": "readings",
				"type": {
					"type": "MAP",
					"key": "INT NOT NULL",
					"value": {
						"type": "ARRAY",
						"element": "TIMESTAMP(6) WITH LOCAL TIME ZONE"
					}
				}
			}
		],
		"highestFieldId": 1
	}`

	ts, err := paimon.ParseTableSchemaJSON([]byte(schemaJSON))
	require.NoError(t, err)

	schema, err := paimon.PaimonToSchema(ts)
	require.NoError(t, err)
	require.Len(t, schema.Fields, 1)

	mapSchema := schema.Fields[0].Schema
	require.Equal(t, model.TypeMap, mapSchema.DataType)
	assert.True(t, mapSchema.IsNullable)

	require.NotNil(t, mapSchema.KeySchema)
	assert.Equal(t, model.TypeInt, mapSchema.KeySchema.Schema.DataType)
	assert.False(t, mapSchema.KeySchema.Schema.IsNullable)

	require.NotNil(t, mapSchema.ValueSchema)
	listSchema := mapSchema.ValueSchema.Schema
	require.Equal(t, model.TypeList, listSchema.DataType)
	assert.True(t, listSchema.IsNullable)

	require.NotNil(t, listSchema.ElementSchema)
	elementSchema := listSchema.ElementSchema.Schema
	assert.Equal(t, model.TypeTimestamp, elementSchema.DataType)
	assert.True(t, elementSchema.IsNullable)
}

// TestPaimon_ParseIsInverseOfEncode exercises the acceptance criterion directly: for a representative
// spread of types, encode-then-parse must return exactly the type that went in.
func TestPaimon_ParseIsInverseOfEncode(t *testing.T) {
	t.Parallel()

	cases := []*model.Schema{
		model.NewPrimitiveSchema(model.TypeBoolean, true),
		model.NewPrimitiveSchema(model.TypeInt, false),
		model.NewPrimitiveSchema(model.TypeLong, true),
		model.NewPrimitiveSchema(model.TypeFloat, true),
		model.NewPrimitiveSchema(model.TypeDouble, true),
		model.NewPrimitiveSchema(model.TypeString, true),
		model.NewPrimitiveSchema(model.TypeBytes, true),
		model.NewPrimitiveSchema(model.TypeDate, true),
		model.NewPrimitiveSchema(model.TypeTimestamp, true),
		model.NewPrimitiveSchema(model.TypeTimestampNTZ, true),
		model.NewDecimalSchema(38, 10, false),
	}

	for _, c := range cases {
		c := c
		t.Run(string(c.DataType)+"_nullable="+boolString(c.IsNullable), func(t *testing.T) {
			t.Parallel()
			back, _, err := roundTrip(t, c)
			require.NoError(t, err)
			assert.Equal(t, c.DataType, back.DataType)
			assert.Equal(t, c.IsNullable, back.IsNullable)
		})
	}
}

func boolString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func TestPaimon_MalformedTypeJSONIsNotSilentlyAccepted(t *testing.T) {
	t.Parallel()

	// A "type" that is neither a JSON string nor a JSON object (a bare number) must fail, not be
	// coerced into something plausible-looking.
	schemaJSON := `{"id": 0, "fields": [{"id": 1, "name": "t", "type": 5}], "highestFieldId": 1}`
	ts, err := paimon.ParseTableSchemaJSON([]byte(schemaJSON))
	require.NoError(t, err)

	_, err = paimon.PaimonToSchema(ts)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "neither a JSON string nor a JSON object") || errors.Is(err, paimon.ErrUnsupportedPaimonType))
}
