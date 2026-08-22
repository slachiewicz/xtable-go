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
func SchemaToIceberg(schema *model.Schema, schemaID int) (*TableSchema, int, error) {
	if schema == nil {
		return nil, 0, fmt.Errorf("schema cannot be nil")
	}

	nextID := 1
	var nestedFields []*NestedField

	for _, f := range schema.Fields {
		fieldID := nextID
		if f.FieldID != nil && *f.FieldID > 0 {
			fieldID = *f.FieldID
			if fieldID >= nextID {
				nextID = fieldID + 1
			}
		} else {
			nextID++
		}

		icebergType, updatedNextID, err := convertTypeToIceberg(f.Schema, nextID)
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

func convertTypeToIceberg(s *model.Schema, nextID int) (any, int, error) {
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
			cfID := nextID
			if cf.FieldID != nil && *cf.FieldID > 0 {
				cfID = *cf.FieldID
				if cfID >= nextID {
					nextID = cfID + 1
				}
			} else {
				nextID++
			}
			cType, uID, err := convertTypeToIceberg(cf.Schema, nextID)
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
		elemID := nextID
		nextID++
		elemType, uID, err := convertTypeToIceberg(s.ElementSchema.Schema, nextID)
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
		keyID := nextID
		nextID++
		keyType, uID1, err := convertTypeToIceberg(s.KeySchema.Schema, nextID)
		if err != nil {
			return nil, nextID, err
		}
		nextID = uID1

		valID := nextID
		nextID++
		valType, uID2, err := convertTypeToIceberg(s.ValueSchema.Schema, nextID)
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
