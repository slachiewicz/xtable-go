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

package iceberg

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/slachiewicz/polytable/pkg/model"
)

// ErrUnsupportedIcebergType is returned by parseIcebergType (and so by IcebergToSchema) for an
// Iceberg type this port has no canonical model representation for. It replaces a former silent
// fallback to TypeString: a type that cannot be translated must fail loudly, by name, rather than
// arrive as an unlabeled string column with no indication anything was lost.
var ErrUnsupportedIcebergType = errors.New("iceberg: unsupported type")

// SchemaToIceberg converts a canonical model.Schema to an Iceberg TableSchema, assigning field IDs.
//
// prevSchema and prevLastColumnID give this a stable identity to assign against (T69). Without
// them, a field id was assigned purely by the field's position in schema.Fields, so a pure column
// reorder -- which changes no data -- changed every field's id. A reader that maps a data file's
// columns by id, which is the entire point of Iceberg field ids, then read the wrong column under
// the right name: not a failure, a silently wrong answer.
//
// The rule: a field whose model.Field.FieldID is already a positive int keeps that id outright --
// an explicit id from the source is authoritative. Otherwise, prevSchema is searched by the
// field's dotted path (a struct field's ancestry, or "element"/"key"/"value" for the anonymous
// nodes of a list or map); a name found there keeps its previous id. Only a field with no explicit
// id and no match in prevSchema -- a genuinely new column -- is allocated a fresh id, starting
// above prevLastColumnID.
//
// A column that is dropped and later re-added under the same name does *not* recover its old id:
// prevSchema is the immediately preceding commit's schema, and a dropped column is absent from it,
// so the re-added column is indistinguishable from a new one and gets a fresh id. This is
// deliberate rather than an accident of the lookup: the data files written while the column was
// absent carry no value for it, so resurrecting the old id would let a reader associate old
// on-disk data with a column that in fact has none, or vice versa if a later file reuses the slot.
// A fresh id makes the discontinuity visible instead of silently papering over it. Reusing the
// dropped id for some unrelated new column is avoided the same way Iceberg's own last-column-id
// is meant to: nextID starts at prevLastColumnID+1 regardless of what ids still appear in
// prevSchema, so an id once assigned is never handed out again even after its field is gone.
func SchemaToIceberg(schema *model.Schema, schemaID int, prevSchema *TableSchema, prevLastColumnID int) (*TableSchema, int, error) {
	if schema == nil {
		return nil, 0, fmt.Errorf("schema cannot be nil")
	}

	prevIDs := fieldPathIDs(prevSchema)
	nextID := prevLastColumnID + 1
	if nextID < 1 {
		nextID = 1
	}
	var nestedFields []*NestedField

	for _, f := range schema.Fields {
		fieldID, updatedNextID := assignFieldID(f.FieldID, f.Name, prevIDs, nextID)
		nextID = updatedNextID

		icebergType, updatedNextID, err := convertTypeToIceberg(f.Schema, f.Name, prevIDs, nextID)
		if err != nil {
			return nil, 0, err
		}
		nextID = updatedNextID

		nestedFields = append(nestedFields, &NestedField{
			ID:       fieldID,
			Name:     f.Name,
			Type:     icebergType,
			Required: !f.Schema.IsNullable,
			Doc:      f.Schema.Comment,
		})
	}

	lastColumnID := nextID - 1
	return &TableSchema{
		Type:     "struct",
		SchemaID: schemaID,
		Fields:   nestedFields,
	}, lastColumnID, nil
}

// assignFieldID picks the id for one field: explicit (if the source supplied a positive id),
// else whatever prevIDs already has on record for path, else the next unused id. It also returns
// the nextID to use for whatever is assigned after it, bumped past the chosen id so ids stay
// unique even when a reused id from explicit or prevIDs is higher than the running counter.
func assignFieldID(explicit *int, path string, prevIDs map[string]int, nextID int) (int, int) {
	if explicit != nil && *explicit > 0 {
		id := *explicit
		if id >= nextID {
			nextID = id + 1
		}
		return id, nextID
	}
	if id, ok := prevIDs[path]; ok {
		if id >= nextID {
			nextID = id + 1
		}
		return id, nextID
	}
	id := nextID
	nextID++
	return id, nextID
}

// fieldPathIDs walks an Iceberg TableSchema and returns a map from each field's dotted path to the
// id it was assigned. A list's element and a map's key/value use the specification's fixed names
// for those anonymous nodes ("element", "key", "value"), mirroring nameMappingForType.
//
// It has to tolerate two different in-memory shapes for the same schema, because NestedField.Type
// is `any`: a TableSchema this package just built (as SchemaToIceberg's own recursion does) holds a
// struct's nested fields as []*NestedField and element/key/value ids as int, while one just decoded
// from a metadata.json file (prevSchema, read back by the caller) holds the same data as
// []interface{} of map[string]interface{} and ids as float64, because encoding/json has no static
// type to decode an `any` field against. asFieldViews and asInt normalize both.
func fieldPathIDs(schema *TableSchema) map[string]int {
	out := make(map[string]int)
	if schema == nil {
		return out
	}
	collectFieldIDs(schema.Fields, "", out)
	return out
}

func collectFieldIDs(rawFields any, prefix string, out map[string]int) {
	for _, fv := range asFieldViews(rawFields) {
		path := fv.name
		if prefix != "" {
			path = prefix + "." + fv.name
		}
		out[path] = fv.id
		collectTypeIDs(fv.typ, path, out)
	}
}

func collectTypeIDs(raw any, prefix string, out map[string]int) {
	typed, ok := raw.(map[string]any)
	if !ok {
		return
	}
	switch typed["type"] {
	case "struct":
		collectFieldIDs(typed["fields"], prefix, out)
	case "list":
		elemPath := prefix + ".element"
		if id, ok := asInt(typed["element-id"]); ok {
			out[elemPath] = id
		}
		collectTypeIDs(typed["element"], elemPath, out)
	case "map":
		keyPath := prefix + ".key"
		valPath := prefix + ".value"
		if id, ok := asInt(typed["key-id"]); ok {
			out[keyPath] = id
		}
		if id, ok := asInt(typed["value-id"]); ok {
			out[valPath] = id
		}
		collectTypeIDs(typed["key"], keyPath, out)
		collectTypeIDs(typed["value"], valPath, out)
	}
}

// fieldView is the normalized shape asFieldViews reduces both in-process and JSON-decoded nested
// fields to.
type fieldView struct {
	id   int
	name string
	typ  any
}

func asFieldViews(raw any) []fieldView {
	switch v := raw.(type) {
	case []*NestedField:
		views := make([]fieldView, 0, len(v))
		for _, f := range v {
			views = append(views, fieldView{id: f.ID, name: f.Name, typ: f.Type})
		}
		return views
	case []any:
		views := make([]fieldView, 0, len(v))
		for _, rf := range v {
			m, ok := rf.(map[string]any)
			if !ok {
				continue
			}
			id, _ := asInt(m["id"])
			name, _ := m["name"].(string)
			views = append(views, fieldView{id: id, name: name, typ: m["type"]})
		}
		return views
	default:
		return nil
	}
}

func asInt(raw any) (int, bool) {
	switch v := raw.(type) {
	case int:
		return v, true
	case float64:
		return int(v), true
	default:
		return 0, false
	}
}

// NameMappingProperty is the table property under which Iceberg records the fallback mapping from
// column name to field id.
//
// Every reader resolves a Parquet column by the field id stored in the file's own schema, and falls
// back to this mapping when the file carries none. Polytable never writes data files — it describes
// files another engine wrote — so the files it points at rarely carry ids, and without the mapping
// an engine reads the table as the right number of rows of nothing but nulls. That is what DuckDB
// did before this was written.
const NameMappingProperty = "schema.name-mapping.default"

// nameMappingEntry is one node of the name mapping, which mirrors the shape of the schema.
type nameMappingEntry struct {
	FieldID *int               `json:"field-id,omitempty"`
	Names   []string           `json:"names"`
	Fields  []nameMappingEntry `json:"fields,omitempty"`
}

// NameMappingJSON renders the fallback name mapping of an Iceberg schema.
func NameMappingJSON(schema *TableSchema) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("iceberg schema cannot be nil")
	}
	encoded, err := json.Marshal(nameMappingForFields(schema.Fields))
	if err != nil {
		return "", fmt.Errorf("failed to encode the iceberg name mapping: %w", err)
	}
	return string(encoded), nil
}

func nameMappingForFields(fields []*NestedField) []nameMappingEntry {
	entries := make([]nameMappingEntry, 0, len(fields))
	for _, f := range fields {
		id := f.ID
		entries = append(entries, nameMappingEntry{
			FieldID: &id,
			Names:   []string{f.Name},
			Fields:  nameMappingForType(f.Type),
		})
	}
	return entries
}

// nameMappingForType descends into the nested types. The names of the anonymous nodes — a list's
// element, a map's key and value — are fixed by the specification.
func nameMappingForType(raw any) []nameMappingEntry {
	typed, ok := raw.(map[string]any)
	if !ok {
		return nil
	}

	switch typed["type"] {
	case "struct":
		fields, ok := typed["fields"].([]*NestedField)
		if !ok {
			return nil
		}
		return nameMappingForFields(fields)
	case "list":
		return []nameMappingEntry{nameMappingNode(typed["element-id"], "element", typed["element"])}
	case "map":
		return []nameMappingEntry{
			nameMappingNode(typed["key-id"], "key", typed["key"]),
			nameMappingNode(typed["value-id"], "value", typed["value"]),
		}
	default:
		return nil
	}
}

func nameMappingNode(rawID any, name string, elementType any) nameMappingEntry {
	entry := nameMappingEntry{Names: []string{name}, Fields: nameMappingForType(elementType)}
	if id, ok := rawID.(int); ok {
		entry.FieldID = &id
	}
	return entry
}

// convertTypeToIceberg converts one model.Schema node, allocating field ids for any nested
// fields it introduces (a struct's members, a list's element, a map's key and value). path is the
// dotted ancestry of s within the overall schema, used to look up a stable id for each nested node
// in prevIDs -- see SchemaToIceberg's doc comment for the id-stability rule this implements.
func convertTypeToIceberg(s *model.Schema, path string, prevIDs map[string]int, nextID int) (any, int, error) {
	if s == nil {
		return "string", nextID, nil
	}

	switch s.DataType {
	case model.TypeBoolean:
		return "boolean", nextID, nil
	case model.TypeInt:
		return "int", nextID, nil
	case model.TypeLong:
		return "long", nextID, nil
	case model.TypeFloat:
		return "float", nextID, nil
	case model.TypeDouble:
		return "double", nextID, nil
	case model.TypeString, model.TypeEnum:
		return "string", nextID, nil
	case model.TypeUUID:
		return "uuid", nextID, nil
	case model.TypeBytes:
		return "binary", nextID, nil
	case model.TypeFixed:
		size := 16
		if sz, ok := s.Metadata[model.MetadataKeyFixedBytesSize].(int); ok {
			size = sz
		}
		return fmt.Sprintf("fixed[%d]", size), nextID, nil
	case model.TypeDate:
		return "date", nextID, nil
	case model.TypeTimestamp:
		return "timestamptz", nextID, nil
	case model.TypeTimestampNTZ:
		return "timestamp", nextID, nil
	case model.TypeDecimal:
		precision := 10
		scale := 0
		if p, ok := s.Metadata[model.MetadataKeyDecimalPrecision].(int); ok {
			precision = p
		}
		if sc, ok := s.Metadata[model.MetadataKeyDecimalScale].(int); ok {
			scale = sc
		}
		return fmt.Sprintf("decimal(%d,%d)", precision, scale), nextID, nil
	case model.TypeRecord:
		var structFields []*NestedField
		for _, cf := range s.Fields {
			childPath := cf.Name
			if path != "" {
				childPath = path + "." + cf.Name
			}
			cfID, updated := assignFieldID(cf.FieldID, childPath, prevIDs, nextID)
			nextID = updated
			cType, uID, err := convertTypeToIceberg(cf.Schema, childPath, prevIDs, nextID)
			if err != nil {
				return nil, nextID, err
			}
			nextID = uID
			structFields = append(structFields, &NestedField{
				ID:       cfID,
				Name:     cf.Name,
				Type:     cType,
				Required: !cf.Schema.IsNullable,
				Doc:      cf.Schema.Comment,
			})
		}
		return map[string]any{
			"type":   "struct",
			"fields": structFields,
		}, nextID, nil
	case model.TypeList:
		elemPath := path + ".element"
		elemID, updated := assignFieldID(nil, elemPath, prevIDs, nextID)
		nextID = updated
		elemType, uID, err := convertTypeToIceberg(s.ElementSchema.Schema, elemPath, prevIDs, nextID)
		if err != nil {
			return nil, nextID, err
		}
		nextID = uID
		return map[string]any{
			"type":             "list",
			"element-id":       elemID,
			"element":          elemType,
			"element-required": !s.ElementSchema.Schema.IsNullable,
		}, nextID, nil
	case model.TypeMap:
		keyPath := path + ".key"
		keyID, updated := assignFieldID(nil, keyPath, prevIDs, nextID)
		nextID = updated
		keyType, uID1, err := convertTypeToIceberg(s.KeySchema.Schema, keyPath, prevIDs, nextID)
		if err != nil {
			return nil, nextID, err
		}
		nextID = uID1

		valPath := path + ".value"
		valID, updated2 := assignFieldID(nil, valPath, prevIDs, nextID)
		nextID = updated2
		valType, uID2, err := convertTypeToIceberg(s.ValueSchema.Schema, valPath, prevIDs, nextID)
		if err != nil {
			return nil, nextID, err
		}
		nextID = uID2

		return map[string]any{
			"type":           "map",
			"key-id":         keyID,
			"key":            keyType,
			"value-id":       valID,
			"value":          valType,
			"value-required": !s.ValueSchema.Schema.IsNullable,
		}, nextID, nil
	default:
		return "string", nextID, nil
	}
}

// IcebergToSchema converts an Iceberg TableSchema into a canonical model.Schema.
func IcebergToSchema(icebergSchema *TableSchema) (*model.Schema, error) {
	if icebergSchema == nil {
		return nil, fmt.Errorf("iceberg schema cannot be nil")
	}

	fields := make([]*model.Field, 0, len(icebergSchema.Fields))
	for _, nf := range icebergSchema.Fields {
		fSchema, err := parseIcebergType(nf.Type, !nf.Required)
		if err != nil {
			return nil, fmt.Errorf("failed to parse iceberg field %s: %w", nf.Name, err)
		}
		if nf.Doc != "" {
			fSchema.Comment = nf.Doc
		}
		fieldID := nf.ID
		fields = append(fields, &model.Field{
			Name:    nf.Name,
			FieldID: &fieldID,
			Schema:  fSchema,
		})
	}

	return model.NewRecordSchema("root", fields, false), nil
}

func parseIcebergType(raw any, nullable bool) (*model.Schema, error) {
	switch v := raw.(type) {
	case string:
		v = strings.ToLower(v)
		switch {
		case v == "boolean":
			return model.NewPrimitiveSchema(model.TypeBoolean, nullable), nil
		case v == "int":
			return model.NewPrimitiveSchema(model.TypeInt, nullable), nil
		case v == "long":
			return model.NewPrimitiveSchema(model.TypeLong, nullable), nil
		case v == "float":
			return model.NewPrimitiveSchema(model.TypeFloat, nullable), nil
		case v == "double":
			return model.NewPrimitiveSchema(model.TypeDouble, nullable), nil
		case v == "string":
			return model.NewPrimitiveSchema(model.TypeString, nullable), nil
		case v == "uuid":
			return model.NewPrimitiveSchema(model.TypeUUID, nullable), nil
		case v == "binary":
			return model.NewPrimitiveSchema(model.TypeBytes, nullable), nil
		case v == "date":
			return model.NewPrimitiveSchema(model.TypeDate, nullable), nil
		case v == "timestamp":
			return model.NewPrimitiveSchema(model.TypeTimestampNTZ, nullable), nil
		case v == "timestamptz":
			return model.NewPrimitiveSchema(model.TypeTimestamp, nullable), nil
		case strings.HasPrefix(v, "decimal"):
			p, s := 10, 0
			trimmed := strings.TrimPrefix(v, "decimal(")
			trimmed = strings.TrimSuffix(trimmed, ")")
			parts := strings.Split(trimmed, ",")
			if len(parts) == 2 {
				if parsedP, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
					p = parsedP
				}
				if parsedS, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
					s = parsedS
				}
			}
			return model.NewDecimalSchema(p, s, nullable), nil
		case strings.HasPrefix(v, "fixed[") && strings.HasSuffix(v, "]"):
			inner := strings.TrimSuffix(strings.TrimPrefix(v, "fixed["), "]")
			size, err := strconv.Atoi(strings.TrimSpace(inner))
			if err != nil {
				return nil, fmt.Errorf("%w: malformed fixed length %q: %v", ErrUnsupportedIcebergType, v, err)
			}
			return &model.Schema{
				DataType:   model.TypeFixed,
				IsNullable: nullable,
				Metadata: map[model.MetadataKey]any{
					model.MetadataKeyFixedBytesSize: size,
				},
			}, nil
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedIcebergType, v)
		}

	case map[string]any:
		typeStr, _ := v["type"].(string)
		switch typeStr {
		case "struct":
			rawFields, _ := v["fields"].([]any)
			var fields []*model.Field
			for _, rf := range rawFields {
				fMap, ok := rf.(map[string]any)
				if !ok {
					continue
				}
				fName, _ := fMap["name"].(string)
				fReq, _ := fMap["required"].(bool)
				fType := fMap["type"]
				cSchema, err := parseIcebergType(fType, !fReq)
				if err != nil {
					return nil, err
				}
				// A nested field's doc is as much part of the schema as a top-level one's.
				if fDoc, _ := fMap["doc"].(string); fDoc != "" {
					cSchema.Comment = fDoc
				}
				fields = append(fields, &model.Field{
					Name:   fName,
					Schema: cSchema,
				})
			}
			return model.NewRecordSchema("", fields, nullable), nil
		case "list":
			elemType := v["element"]
			elemReq, _ := v["element-required"].(bool)
			elemSchema, err := parseIcebergType(elemType, !elemReq)
			if err != nil {
				return nil, err
			}
			return &model.Schema{
				DataType:   model.TypeList,
				IsNullable: nullable,
				ElementSchema: &model.Field{
					Name:   "element",
					Schema: elemSchema,
				},
			}, nil
		case "map":
			kType := v["key"]
			vType := v["value"]
			vReq, _ := v["value-required"].(bool)
			kSchema, err := parseIcebergType(kType, false)
			if err != nil {
				return nil, err
			}
			vSchema, err := parseIcebergType(vType, !vReq)
			if err != nil {
				return nil, err
			}
			return &model.Schema{
				DataType:   model.TypeMap,
				IsNullable: nullable,
				KeySchema: &model.Field{
					Name:   "key",
					Schema: kSchema,
				},
				ValueSchema: &model.Field{
					Name:   "value",
					Schema: vSchema,
				},
			}, nil
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnsupportedIcebergType, typeStr)
		}
	}

	return nil, fmt.Errorf("%w: %#v", ErrUnsupportedIcebergType, raw)
}
