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

package hudi

import (
	"encoding/json"
	"fmt"

	"github.com/slachiewicz/polytable/pkg/model"
)

// AvroField represents a field in an Avro record schema.
type AvroField struct {
	Name    string `json:"name"`
	Type    any    `json:"type"`
	Doc     string `json:"doc,omitempty"`
	Default any    `json:"default,omitempty"`
}

// AvroRecordSchema represents an Avro record schema structure.
type AvroRecordSchema struct {
	Type      string       `json:"type"`
	Name      string       `json:"name"`
	Namespace string       `json:"namespace,omitempty"`
	Doc       string       `json:"doc,omitempty"`
	Fields    []*AvroField `json:"fields"`
}

// SchemaToAvroJSON converts a canonical model.Schema into Avro schema JSON string.
func SchemaToAvroJSON(schema *model.Schema, recordName, namespace string) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("schema cannot be nil")
	}

	if recordName == "" {
		recordName = "record"
	}

	var avroFields []*AvroField
	for _, f := range schema.Fields {
		avroType, err := convertTypeToAvro(f.Schema)
		if err != nil {
			return "", err
		}

		avroField := &AvroField{
			Name: f.Name,
			Type: avroType,
			Doc:  f.Schema.Comment,
		}
		if f.Schema.IsNullable {
			avroField.Default = nil
		}
		avroFields = append(avroFields, avroField)
	}

	avroSchema := &AvroRecordSchema{
		Type:      "record",
		Name:      recordName,
		Namespace: namespace,
		Fields:    avroFields,
	}

	bytes, err := json.Marshal(avroSchema)
	if err != nil {
		return "", fmt.Errorf("failed to serialize avro schema: %w", err)
	}
	return string(bytes), nil
}

func convertTypeToAvro(s *model.Schema) (any, error) {
	if s == nil {
		return "string", nil
	}

	var baseType any

	switch s.DataType {
	case model.TypeBoolean:
		baseType = "boolean"
	case model.TypeInt:
		baseType = "int"
	case model.TypeLong:
		baseType = "long"
	case model.TypeFloat:
		baseType = "float"
	case model.TypeDouble:
		baseType = "double"
	case model.TypeString, model.TypeEnum:
		baseType = "string"
	case model.TypeUUID:
		baseType = map[string]any{"type": "string", "logicalType": "uuid"}
	case model.TypeBytes, model.TypeFixed:
		baseType = "bytes"
	case model.TypeDate:
		baseType = map[string]any{"type": "int", "logicalType": "date"}
	case model.TypeTimestamp:
		baseType = map[string]any{"type": "long", "logicalType": "timestamp-micros"}
	case model.TypeTimestampNTZ:
		baseType = map[string]any{"type": "long", "logicalType": "local-timestamp-micros"}
	case model.TypeDecimal:
		precision := 10
		scale := 0
		if p, ok := s.Metadata[model.MetadataKeyDecimalPrecision].(int); ok {
			precision = p
		}
		if sc, ok := s.Metadata[model.MetadataKeyDecimalScale].(int); ok {
			scale = sc
		}
		baseType = map[string]any{
			"type":        "bytes",
			"logicalType": "decimal",
			"precision":   precision,
			"scale":       scale,
		}
	case model.TypeRecord:
		var fields []*AvroField
		for _, cf := range s.Fields {
			cType, err := convertTypeToAvro(cf.Schema)
			if err != nil {
				return nil, err
			}
			fields = append(fields, &AvroField{
				Name: cf.Name,
				Type: cType,
			})
		}
		baseType = map[string]any{
			"type":   "record",
			"name":   s.Name,
			"fields": fields,
		}
	case model.TypeList:
		elemType, err := convertTypeToAvro(s.ElementSchema.Schema)
		if err != nil {
			return nil, err
		}
		baseType = map[string]any{
			"type":  "array",
			"items": elemType,
		}
	case model.TypeMap:
		valType, err := convertTypeToAvro(s.ValueSchema.Schema)
		if err != nil {
			return nil, err
		}
		baseType = map[string]any{
			"type":   "map",
			"values": valType,
		}
	default:
		baseType = "string"
	}

	if s.IsNullable {
		return []any{"null", baseType}, nil
	}
	return baseType, nil
}

// AvroJSONToSchema converts an Avro schema JSON into canonical model.Schema.
func AvroJSONToSchema(avroJSON string) (*model.Schema, error) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(avroJSON), &raw); err != nil {
		return nil, fmt.Errorf("failed to parse avro JSON: %w", err)
	}

	fieldsRaw, ok := raw["fields"].([]any)
	if !ok {
		return nil, fmt.Errorf("invalid avro record schema: missing fields")
	}

	var fields []*model.Field
	for _, fr := range fieldsRaw {
		fMap, ok := fr.(map[string]any)
		if !ok {
			continue
		}
		fName, _ := fMap["name"].(string)
		fType := fMap["type"]
		fSchema, err := parseAvroType(fType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse avro field %s: %w", fName, err)
		}
		fields = append(fields, &model.Field{
			Name:   fName,
			Schema: fSchema,
		})
	}

	recordName, _ := raw["name"].(string)
	return model.NewRecordSchema(recordName, fields, false), nil
}

func parseAvroType(raw any) (*model.Schema, error) {
	switch v := raw.(type) {
	case string:
		switch v {
		case "boolean":
			return model.NewPrimitiveSchema(model.TypeBoolean, false), nil
		case "int":
			return model.NewPrimitiveSchema(model.TypeInt, false), nil
		case "long":
			return model.NewPrimitiveSchema(model.TypeLong, false), nil
		case "float":
			return model.NewPrimitiveSchema(model.TypeFloat, false), nil
		case "double":
			return model.NewPrimitiveSchema(model.TypeDouble, false), nil
		case "string":
			return model.NewPrimitiveSchema(model.TypeString, false), nil
		case "bytes":
			return model.NewPrimitiveSchema(model.TypeBytes, false), nil
		default:
			return model.NewPrimitiveSchema(model.TypeString, false), nil
		}

	case []any:
		// Union type (e.g. ["null", "string"])
		nullable := false
		var actualType any
		for _, item := range v {
			if itemStr, ok := item.(string); ok && itemStr == "null" {
				nullable = true
			} else {
				actualType = item
			}
		}
		if actualType != nil {
			s, err := parseAvroType(actualType)
			if err != nil {
				return nil, err
			}
			s.IsNullable = nullable
			return s, nil
		}
		return model.NewPrimitiveSchema(model.TypeString, true), nil

	case map[string]any:
		if logType, ok := v["logicalType"].(string); ok {
			switch logType {
			case "date":
				return model.NewPrimitiveSchema(model.TypeDate, false), nil
			case "timestamp-millis", "timestamp-micros":
				return model.NewPrimitiveSchema(model.TypeTimestamp, false), nil
			case "local-timestamp-millis", "local-timestamp-micros":
				// convertTypeToAvro above emits local-timestamp-micros for TIMESTAMP_NTZ; without
				// this case, that logical type fell through to the map[string]any switch's final
				// `return model.NewPrimitiveSchema(model.TypeString, false)` below, and a Hudi
				// table's own writer and reader disagreed about a TIMESTAMP_NTZ column's type.
				// local-timestamp-millis is accepted too even though this package's writer never
				// emits it: Avro defines both millis and micros variants of the local (zone-naive)
				// logical type, so a foreign writer's local-timestamp-millis column deserves the
				// same treatment rather than narrowing just because this writer picked one width.
				return model.NewPrimitiveSchema(model.TypeTimestampNTZ, false), nil
			case "uuid":
				return model.NewPrimitiveSchema(model.TypeUUID, false), nil
			case "decimal":
				p, _ := v["precision"].(float64)
				s, _ := v["scale"].(float64)
				return model.NewDecimalSchema(int(p), int(s), false), nil
			}
		}

		typeStr, _ := v["type"].(string)
		switch typeStr {
		case "array":
			elemSchema, err := parseAvroType(v["items"])
			if err != nil {
				return nil, err
			}
			return &model.Schema{
				DataType:   model.TypeList,
				IsNullable: false,
				ElementSchema: &model.Field{
					Name:   "element",
					Schema: elemSchema,
				},
			}, nil
		case "map":
			valSchema, err := parseAvroType(v["values"])
			if err != nil {
				return nil, err
			}
			return &model.Schema{
				DataType:   model.TypeMap,
				IsNullable: false,
				KeySchema: &model.Field{
					Name:   "key",
					Schema: model.NewPrimitiveSchema(model.TypeString, false),
				},
				ValueSchema: &model.Field{
					Name:   "value",
					Schema: valSchema,
				},
			}, nil
		case "record":
			fieldsRaw, _ := v["fields"].([]any)
			var fields []*model.Field
			for _, fr := range fieldsRaw {
				fMap, ok := fr.(map[string]any)
				if !ok {
					continue
				}
				fName, _ := fMap["name"].(string)
				fSchema, err := parseAvroType(fMap["type"])
				if err != nil {
					return nil, err
				}
				fields = append(fields, &model.Field{
					Name:   fName,
					Schema: fSchema,
				})
			}
			return model.NewRecordSchema("", fields, false), nil
		}
	}

	return model.NewPrimitiveSchema(model.TypeString, false), nil
}
