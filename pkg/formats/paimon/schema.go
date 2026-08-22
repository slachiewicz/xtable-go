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

package paimon

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/slachiewicz/polytable/pkg/model"
)

// ErrUnsupportedPaimonType is returned, wrapped with the offending type, whenever a model type has
// no Paimon representation or a Paimon type string cannot be parsed. It is never papered over with a
// silent narrowing to STRING: an unsupported type is a caller-visible failure.
var ErrUnsupportedPaimonType = errors.New("unsupported paimon type")

// PaimonType is the JSON encoding of a single Paimon logical type (org.apache.paimon.types.DataType).
//
// Paimon 1.3.1 does not encode composite types as a flat SQL-string: DataType#serializeJson writes an
// atomic (leaf) type as a bare JSON string using the type's asSQLString() spelling ("INT",
// "TIMESTAMP(6) WITH LOCAL TIME ZONE", " NOT NULL" appended when non-nullable), but RowType, ArrayType
// and MapType each override serializeJson to emit a JSON *object* carrying a "type" discriminator
// ("ROW" / "ROW NOT NULL", ...) plus name-keyed children ("fields" for ROW, "element" for ARRAY,
// "key"/"value" for MAP). DataTypeJsonParser#parseDataType mirrors that exactly on the read side:
// textual nodes go through the atomic-string tokenizer, object nodes are dispatched on the "type"
// field's prefix. PaimonType reproduces that shape byte-for-byte so a schema-N file written by this
// package has the same "type" structure Paimon's own parser expects, rather than a private
// ARRAY<...>/MAP<...> string dialect Paimon cannot read back.
//
// Verified against org.apache.paimon:paimon-bundle:1.3.1 sources (DataType, RowType, ArrayType,
// MapType, DataField, DataTypeJsonParser, TimestampType, LocalZonedTimestampType, DecimalType,
// VarCharType, VarBinaryType) — see the commit message for how those sources were obtained.
type PaimonType struct {
	raw json.RawMessage
}

// MarshalJSON implements json.Marshaler.
func (t PaimonType) MarshalJSON() ([]byte, error) {
	if len(t.raw) == 0 {
		return []byte("null"), nil
	}
	return t.raw, nil
}

// UnmarshalJSON implements json.Unmarshaler. It only captures the raw bytes; interpretation happens
// in parsePaimonType, once we know whether the node is a textual atomic leaf or a composite object.
func (t *PaimonType) UnmarshalJSON(data []byte) error {
	t.raw = append(json.RawMessage(nil), data...)
	return nil
}

// atomicPaimonType wraps an atomic SQL-string spelling ("INT", "STRING", "DECIMAL(10, 2)", ...) as
// the bare JSON string Paimon's DataType#serializeJson default implementation emits for scalar types.
func atomicPaimonType(sqlString string) (PaimonType, error) {
	raw, err := json.Marshal(sqlString)
	if err != nil {
		return PaimonType{}, err
	}
	return PaimonType{raw: raw}, nil
}

func withNullabilitySuffix(base string, nullable bool) string {
	if !nullable {
		return base + " NOT NULL"
	}
	return base
}

// DataField represents a field in Apache Paimon schema.
type DataField struct {
	ID          int        `json:"id"`
	Name        string     `json:"name"`
	Type        PaimonType `json:"type"`
	Description string     `json:"description,omitempty"`
}

// TableSchema represents an Apache Paimon schema JSON file (schema/schema-N).
type TableSchema struct {
	ID             int64             `json:"id"`
	Fields         []DataField       `json:"fields"`
	HighestFieldID int               `json:"highestFieldId"`
	PartitionKeys  []string          `json:"partitionKeys,omitempty"`
	PrimaryKeys    []string          `json:"primaryKeys,omitempty"`
	Options        map[string]string `json:"options,omitempty"`
}

// SchemaToPaimon converts canonical model.Schema into Paimon TableSchema.
func SchemaToPaimon(schema *model.Schema, partitionKeys []string) (*TableSchema, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	nextID := 0
	var fields []DataField
	for _, f := range schema.Fields {
		df, err := modelFieldToPaimonField(f, &nextID)
		if err != nil {
			return nil, err
		}
		fields = append(fields, df)
	}

	return &TableSchema{
		ID:             0,
		Fields:         fields,
		HighestFieldID: nextID,
		PartitionKeys:  partitionKeys,
	}, nil
}

// PaimonToSchema converts Paimon TableSchema into canonical model.Schema.
func PaimonToSchema(ts *TableSchema) (*model.Schema, error) {
	if ts == nil {
		return nil, fmt.Errorf("paimon table schema cannot be nil")
	}

	fields, err := paimonFieldsToModelFields(ts.Fields)
	if err != nil {
		return nil, err
	}

	return model.NewRecordSchema("paimon_table", fields, false), nil
}

// modelFieldToPaimonField converts one struct member (top-level or nested) into a Paimon DataField.
//
// nextID is a single counter shared across the *entire* schema tree, not reset at each nesting level.
// Paimon requires field ids to be unique across the whole tree: RowType#collectFieldIds throws
// "Broken schema, field id %s is duplicated" the moment the same id appears twice anywhere, and
// RowType.Builder/DataTypeJsonParser both allocate new ids from one shared counter for exactly this
// reason. A naive per-ROW "i+1" scheme would hand out id 1 to both a top-level field and an unrelated
// field inside a nested struct — a schema Paimon's own invariant rejects, which would just be this
// task's silent-narrowing defect wearing a different shape.
func modelFieldToPaimonField(f *model.Field, nextID *int) (DataField, error) {
	var fID int
	if f.FieldID != nil && *f.FieldID > 0 {
		fID = *f.FieldID
		if fID > *nextID {
			*nextID = fID
		}
	} else {
		*nextID++
		fID = *nextID
	}

	t, err := modelTypeToPaimonType(f.Schema, nextID)
	if err != nil {
		return DataField{}, fmt.Errorf("field %q: %w", f.Name, err)
	}

	return DataField{
		ID:          fID,
		Name:        f.Name,
		Type:        t,
		Description: f.Schema.Comment,
	}, nil
}

// paimonFieldsToModelFields is the inverse of the loop in modelFieldToPaimonField's caller: it
// converts a Paimon field list (top-level TableSchema.Fields, or a nested ROW's "fields") back into
// model fields, preserving each field's id.
func paimonFieldsToModelFields(dfs []DataField) ([]*model.Field, error) {
	fields := make([]*model.Field, 0, len(dfs))
	for _, df := range dfs {
		s, err := parsePaimonType(df.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", df.Name, err)
		}
		s.Comment = df.Description

		fieldID := df.ID
		fields = append(fields, &model.Field{
			Name:    df.Name,
			FieldID: &fieldID,
			Schema:  s,
		})
	}
	return fields, nil
}

// modelTypeToPaimonType converts one canonical model.Schema into its Paimon type encoding.
//
// Any DataType with no Paimon representation (ENUM, UNION, NULL, ...) returns
// ErrUnsupportedPaimonType rather than degrading to STRING: a struct silently written as its field
// count's worth of "STRING" columns, or a schema-less blob, is a worse failure than refusing to write
// it at all, because nothing downstream would ever notice.
func modelTypeToPaimonType(s *model.Schema, nextID *int) (PaimonType, error) {
	if s == nil {
		return PaimonType{}, fmt.Errorf("%w: nil schema", ErrUnsupportedPaimonType)
	}

	switch s.DataType {
	case model.TypeBoolean:
		return atomicPaimonType(withNullabilitySuffix("BOOLEAN", s.IsNullable))
	case model.TypeInt:
		return atomicPaimonType(withNullabilitySuffix("INT", s.IsNullable))
	case model.TypeLong:
		return atomicPaimonType(withNullabilitySuffix("BIGINT", s.IsNullable))
	case model.TypeFloat:
		return atomicPaimonType(withNullabilitySuffix("FLOAT", s.IsNullable))
	case model.TypeDouble:
		return atomicPaimonType(withNullabilitySuffix("DOUBLE", s.IsNullable))
	case model.TypeString, model.TypeUUID:
		// UUID has no dedicated Paimon type; STRING is the widening every other format adapter in
		// this codebase already uses for it (e.g. Iceberg's schema.go), so it is a defensible,
		// deliberate choice rather than the same silent-narrowing default this fix removes elsewhere.
		return atomicPaimonType(withNullabilitySuffix("STRING", s.IsNullable))
	case model.TypeBytes, model.TypeFixed:
		// TypeFixed's length metadata is dropped here (Paimon's fixed-length BINARY(n) is available
		// but not used), matching this package's pre-existing behavior for that type; only the three
		// defects named in T70 defect 7 are in scope for this change.
		return atomicPaimonType(withNullabilitySuffix("BYTES", s.IsNullable))
	case model.TypeDate:
		return atomicPaimonType(withNullabilitySuffix("DATE", s.IsNullable))
	case model.TypeTimestampNTZ:
		// Naive timestamp: org.apache.paimon.types.TimestampType, "TIMESTAMP(p)".
		precision := timestampWritePrecision(s)
		return atomicPaimonType(withNullabilitySuffix(fmt.Sprintf("TIMESTAMP(%d)", precision), s.IsNullable))
	case model.TypeTimestamp:
		// Zone-aware timestamp: org.apache.paimon.types.LocalZonedTimestampType,
		// "TIMESTAMP(p) WITH LOCAL TIME ZONE" — the only zone-aware timestamp spelling Paimon's own
		// DataTypeJsonParser produces (confirmed from LocalZonedTimestampType.FORMAT).
		precision := timestampWritePrecision(s)
		return atomicPaimonType(withNullabilitySuffix(fmt.Sprintf("TIMESTAMP(%d) WITH LOCAL TIME ZONE", precision), s.IsNullable))
	case model.TypeDecimal:
		precision := 10
		scale := 0
		if p, ok := s.Metadata[model.MetadataKeyDecimalPrecision].(int); ok {
			precision = p
		}
		if sc, ok := s.Metadata[model.MetadataKeyDecimalScale].(int); ok {
			scale = sc
		}
		return atomicPaimonType(withNullabilitySuffix(fmt.Sprintf("DECIMAL(%d, %d)", precision, scale), s.IsNullable))
	case model.TypeList:
		if s.ElementSchema == nil || s.ElementSchema.Schema == nil {
			return PaimonType{}, fmt.Errorf("%w: LIST with no element schema", ErrUnsupportedPaimonType)
		}
		element, err := modelTypeToPaimonType(s.ElementSchema.Schema, nextID)
		if err != nil {
			return PaimonType{}, fmt.Errorf("array element: %w", err)
		}
		return arrayPaimonType(s.IsNullable, element)
	case model.TypeMap:
		if s.KeySchema == nil || s.KeySchema.Schema == nil || s.ValueSchema == nil || s.ValueSchema.Schema == nil {
			return PaimonType{}, fmt.Errorf("%w: MAP with no key/value schema", ErrUnsupportedPaimonType)
		}
		key, err := modelTypeToPaimonType(s.KeySchema.Schema, nextID)
		if err != nil {
			return PaimonType{}, fmt.Errorf("map key: %w", err)
		}
		value, err := modelTypeToPaimonType(s.ValueSchema.Schema, nextID)
		if err != nil {
			return PaimonType{}, fmt.Errorf("map value: %w", err)
		}
		return mapPaimonType(s.IsNullable, key, value)
	case model.TypeRecord:
		nested := make([]DataField, 0, len(s.Fields))
		for _, f := range s.Fields {
			df, err := modelFieldToPaimonField(f, nextID)
			if err != nil {
				return PaimonType{}, err
			}
			nested = append(nested, df)
		}
		return rowPaimonType(s.IsNullable, nested)
	default:
		return PaimonType{}, fmt.Errorf("%w: %s", ErrUnsupportedPaimonType, s.DataType)
	}
}

// rowPaimonType builds the JSON object org.apache.paimon.types.RowType#serializeJson emits:
// {"type": "ROW" | "ROW NOT NULL", "fields": [...]}.
func rowPaimonType(nullable bool, fields []DataField) (PaimonType, error) {
	discriminator := withNullabilitySuffix("ROW", nullable)
	raw, err := json.Marshal(struct {
		Type   string      `json:"type"`
		Fields []DataField `json:"fields"`
	}{Type: discriminator, Fields: fields})
	if err != nil {
		return PaimonType{}, err
	}
	return PaimonType{raw: raw}, nil
}

// arrayPaimonType builds the JSON object org.apache.paimon.types.ArrayType#serializeJson emits:
// {"type": "ARRAY" | "ARRAY NOT NULL", "element": <DataType>}.
func arrayPaimonType(nullable bool, element PaimonType) (PaimonType, error) {
	discriminator := withNullabilitySuffix("ARRAY", nullable)
	raw, err := json.Marshal(struct {
		Type    string     `json:"type"`
		Element PaimonType `json:"element"`
	}{Type: discriminator, Element: element})
	if err != nil {
		return PaimonType{}, err
	}
	return PaimonType{raw: raw}, nil
}

// mapPaimonType builds the JSON object org.apache.paimon.types.MapType#serializeJson emits:
// {"type": "MAP" | "MAP NOT NULL", "key": <DataType>, "value": <DataType>}.
func mapPaimonType(nullable bool, key, value PaimonType) (PaimonType, error) {
	discriminator := withNullabilitySuffix("MAP", nullable)
	raw, err := json.Marshal(struct {
		Type  string     `json:"type"`
		Key   PaimonType `json:"key"`
		Value PaimonType `json:"value"`
	}{Type: discriminator, Key: key, Value: value})
	if err != nil {
		return PaimonType{}, err
	}
	return PaimonType{raw: raw}, nil
}

// timestampWritePrecision recovers a Paimon TIMESTAMP precision from the TIMESTAMP_PRECISION metadata
// left by parsePaimonType, defaulting to Paimon's own DEFAULT_PRECISION (6) when absent — which is
// also what this package always wrote before precision was tracked at all. Because the bucket is one
// of exactly {3, 6, 9}, a value we wrote ourselves round-trips exactly; a foreign TIMESTAMP(2) would
// bucket to MILLIS on read and re-emit as TIMESTAMP(3) on write, which is not byte-identical but is a
// safe over-approximation (3 >= 2), not a narrowing.
func timestampWritePrecision(s *model.Schema) int {
	if s.Metadata != nil {
		switch s.Metadata[model.MetadataKeyTimestampPrecision] {
		case model.MetadataValueMillis:
			return 3
		case model.MetadataValueMicros:
			return 6
		case model.MetadataValueNanos:
			return 9
		}
	}
	return 6
}

// timestampPrecisionBucket buckets a parsed numeric precision into the metadata value polytable's
// other format adapters use, mirroring org.apache.xtable.paimon.PaimonSchemaExtractor's own
// precision<=3/<=6/else bucketing exactly (xtable-core's Java Paimon reader, read from
// ../incubator-xtable during this fix).
func timestampPrecisionBucket(precision int) model.MetadataValue {
	switch {
	case precision <= 3:
		return model.MetadataValueMillis
	case precision <= 6:
		return model.MetadataValueMicros
	default:
		return model.MetadataValueNanos
	}
}

var decimalParamsPattern = regexp.MustCompile(`\(\s*(\d+)\s*,\s*(\d+)\s*\)`)

// parsePaimonType converts one Paimon type encoding back into a canonical model.Schema, and is the
// exact inverse of modelTypeToPaimonType: an atomic JSON string goes through the SQL-string grammar
// (org.apache.paimon.types.DataTypeJsonParser#parseAtomicTypeSQLString), a composite JSON object is
// dispatched on its "type" discriminator's prefix, exactly mirroring
// DataTypeJsonParser#parseDataType's isTextual()/isObject() branch.
func parsePaimonType(t PaimonType) (*model.Schema, error) {
	trimmed := bytes.TrimSpace(t.raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("%w: empty type", ErrUnsupportedPaimonType)
	}

	if trimmed[0] == '{' {
		return parseCompositePaimonType(t.raw)
	}

	var sqlString string
	if err := json.Unmarshal(t.raw, &sqlString); err != nil {
		return nil, fmt.Errorf("paimon type %q is neither a JSON string nor a JSON object: %w", string(t.raw), err)
	}
	return parseAtomicTypeSQLString(sqlString)
}

type paimonCompositeHeader struct {
	Type string `json:"type"`
}

// parseCompositePaimonType handles the JSON-object shapes RowType, ArrayType and MapType serialize as
// (see PaimonType's doc comment). It intentionally does not accept MULTISET: this package never
// writes it, and Paimon's own MultisetType has no canonical model.Type to parse it into.
func parseCompositePaimonType(raw json.RawMessage) (*model.Schema, error) {
	var header paimonCompositeHeader
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, fmt.Errorf("invalid paimon composite type: %w", err)
	}

	upper := strings.ToUpper(strings.TrimSpace(header.Type))
	nullable := !strings.Contains(upper, "NOT NULL")

	switch {
	case strings.HasPrefix(upper, "ROW"):
		var row struct {
			Fields []DataField `json:"fields"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, fmt.Errorf("invalid ROW type: %w", err)
		}
		fields, err := paimonFieldsToModelFields(row.Fields)
		if err != nil {
			return nil, err
		}
		schema := model.NewRecordSchema("", fields, nullable)
		return schema, nil

	case strings.HasPrefix(upper, "ARRAY"):
		var arr struct {
			Element PaimonType `json:"element"`
		}
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, fmt.Errorf("invalid ARRAY type: %w", err)
		}
		elementSchema, err := parsePaimonType(arr.Element)
		if err != nil {
			return nil, fmt.Errorf("array element: %w", err)
		}
		return &model.Schema{
			DataType:      model.TypeList,
			IsNullable:    nullable,
			ElementSchema: &model.Field{Name: "element", Schema: elementSchema},
			Metadata:      make(map[model.MetadataKey]any),
		}, nil

	case strings.HasPrefix(upper, "MAP"):
		var m struct {
			Key   PaimonType `json:"key"`
			Value PaimonType `json:"value"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("invalid MAP type: %w", err)
		}
		keySchema, err := parsePaimonType(m.Key)
		if err != nil {
			return nil, fmt.Errorf("map key: %w", err)
		}
		valueSchema, err := parsePaimonType(m.Value)
		if err != nil {
			return nil, fmt.Errorf("map value: %w", err)
		}
		return &model.Schema{
			DataType:    model.TypeMap,
			IsNullable:  nullable,
			KeySchema:   &model.Field{Name: "key", Schema: keySchema},
			ValueSchema: &model.Field{Name: "value", Schema: valueSchema},
			Metadata:    make(map[model.MetadataKey]any),
		}, nil

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedPaimonType, header.Type)
	}
}

// parseAtomicTypeSQLString parses one Paimon atomic (leaf) type spelling — the subset of
// org.apache.paimon.types.DataTypeJsonParser's SQL-string grammar this package's writer emits, plus
// the handful of alternate spellings (TINYINT/SMALLINT, VARCHAR/CHAR, BINARY/VARBINARY,
// TIMESTAMP_LTZ, WITHOUT TIME ZONE) that a real Paimon table produced by something other than this
// package may use. Anything DataTypeJsonParser accepts that this package cannot represent in
// model.Type (TIME, INTERVAL, RAW, VARIANT, ...) is refused by name rather than narrowed to STRING.
func parseAtomicTypeSQLString(raw string) (*model.Schema, error) {
	clean := strings.TrimSpace(raw)
	upperFull := strings.ToUpper(clean)

	isNullable := true
	if strings.HasSuffix(upperFull, "NOT NULL") {
		isNullable = false
		clean = strings.TrimSpace(clean[:len(clean)-len("NOT NULL")])
		upperFull = strings.ToUpper(clean)
	}
	upper := upperFull

	switch {
	case upper == "BOOLEAN":
		return model.NewPrimitiveSchema(model.TypeBoolean, isNullable), nil
	case upper == "TINYINT" || upper == "SMALLINT" || upper == "INT" || upper == "INTEGER":
		return model.NewPrimitiveSchema(model.TypeInt, isNullable), nil
	case upper == "BIGINT":
		return model.NewPrimitiveSchema(model.TypeLong, isNullable), nil
	case upper == "FLOAT":
		return model.NewPrimitiveSchema(model.TypeFloat, isNullable), nil
	case upper == "DOUBLE" || upper == "DOUBLE PRECISION":
		return model.NewPrimitiveSchema(model.TypeDouble, isNullable), nil
	case upper == "STRING" || strings.HasPrefix(upper, "VARCHAR") || strings.HasPrefix(upper, "CHAR"):
		return model.NewPrimitiveSchema(model.TypeString, isNullable), nil
	case upper == "BYTES" || strings.HasPrefix(upper, "BINARY") || strings.HasPrefix(upper, "VARBINARY"):
		return model.NewPrimitiveSchema(model.TypeBytes, isNullable), nil
	case upper == "DATE":
		return model.NewPrimitiveSchema(model.TypeDate, isNullable), nil
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "DEC"):
		precision, scale := 10, 0
		if m := decimalParamsPattern.FindStringSubmatch(upper); m != nil {
			precision, _ = strconv.Atoi(m[1])
			scale, _ = strconv.Atoi(m[2])
		}
		return model.NewDecimalSchema(precision, scale, isNullable), nil
	case strings.HasPrefix(upper, "TIMESTAMP"):
		return parseTimestampSQLString(upper, isNullable)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedPaimonType, clean)
	}
}

// parseTimestampSQLString distinguishes Paimon's two timestamp types by their exact spelling:
// TimestampType.asSQLString() is "TIMESTAMP(p)" (optionally "TIMESTAMP(p) WITHOUT TIME ZONE"), while
// LocalZonedTimestampType.asSQLString() is "TIMESTAMP(p) WITH LOCAL TIME ZONE" — the only spelling
// DataTypeJsonParser's TokenParser#parseTimestampType or #parseTimestampLtzType ever accept for the
// zone-aware variant (both confirmed against paimon-bundle:1.3.1 sources). Session-zone
// "WITH TIME ZONE" (without LOCAL) is not a shape Paimon materializes, so it is refused rather than
// guessed at, per the "fail loudly" instruction for ambiguous grammar points.
func parseTimestampSQLString(upper string, isNullable bool) (*model.Schema, error) {
	zoneAware := false
	body := upper
	switch {
	case strings.HasPrefix(body, "TIMESTAMP_LTZ"):
		zoneAware = true
		body = strings.TrimPrefix(body, "TIMESTAMP_LTZ")
	case strings.HasPrefix(body, "TIMESTAMP"):
		body = strings.TrimPrefix(body, "TIMESTAMP")
	}
	body = strings.TrimSpace(body)

	precision := 6
	if strings.HasPrefix(body, "(") {
		end := strings.Index(body, ")")
		if end < 0 {
			return nil, fmt.Errorf("%w: malformed TIMESTAMP precision in %q", ErrUnsupportedPaimonType, upper)
		}
		p, err := strconv.Atoi(strings.TrimSpace(body[1:end]))
		if err != nil {
			return nil, fmt.Errorf("%w: malformed TIMESTAMP precision in %q", ErrUnsupportedPaimonType, upper)
		}
		precision = p
		body = strings.TrimSpace(body[end+1:])
	}

	switch body {
	case "", "WITHOUT TIME ZONE":
		// naive, unless the TIMESTAMP_LTZ spelling above already marked it zone-aware.
	case "WITH LOCAL TIME ZONE":
		zoneAware = true
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedPaimonType, upper)
	}

	dataType := model.TypeTimestampNTZ
	if zoneAware {
		dataType = model.TypeTimestamp
	}
	schema := model.NewPrimitiveSchema(dataType, isNullable)
	schema.Metadata[model.MetadataKeyTimestampPrecision] = timestampPrecisionBucket(precision)
	return schema, nil
}

// ParseTableSchemaJSON parses Paimon schema JSON bytes.
func ParseTableSchemaJSON(data []byte) (*TableSchema, error) {
	var ts TableSchema
	if err := json.Unmarshal(data, &ts); err != nil {
		return nil, err
	}
	return &ts, nil
}
