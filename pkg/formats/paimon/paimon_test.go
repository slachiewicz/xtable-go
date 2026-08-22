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
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/paimon"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestPaimon_SchemaRoundTrip(t *testing.T) {
	t.Parallel()

	idField := &model.Field{Name: "sensor_id", Schema: model.NewPrimitiveSchema(model.TypeInt, false)}
	tempField := &model.Field{Name: "temperature", Schema: model.NewPrimitiveSchema(model.TypeDouble, true)}
	locField := &model.Field{Name: "location", Schema: model.NewPrimitiveSchema(model.TypeString, false)}

	origSchema := model.NewRecordSchema("sensors", []*model.Field{idField, tempField, locField}, false)

	paimonSchema, err := paimon.SchemaToPaimon(origSchema, []string{"location"})
	require.NoError(t, err)
	assert.Equal(t, 3, len(paimonSchema.Fields))
	assert.Equal(t, []string{"location"}, paimonSchema.PartitionKeys)

	convertedSchema, err := paimon.PaimonToSchema(paimonSchema)
	require.NoError(t, err)
	require.Len(t, convertedSchema.Fields, 3)

	assert.Equal(t, "sensor_id", convertedSchema.Fields[0].Name)
	assert.Equal(t, model.TypeInt, convertedSchema.Fields[0].Schema.DataType)
	assert.False(t, convertedSchema.Fields[0].Schema.IsNullable)

	assert.Equal(t, "temperature", convertedSchema.Fields[1].Name)
	assert.Equal(t, model.TypeDouble, convertedSchema.Fields[1].Schema.DataType)
	assert.True(t, convertedSchema.Fields[1].Schema.IsNullable)
}

func TestPaimon_SourceReadSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/paimon_table"

	// 1. Write schema/schema-0. The file is written by hand as raw JSON (rather than through a
	// paimon.DataField literal) because paimon.PaimonType's atomic leaves are constructed via its
	// package-private constructor; a hand-written fixture is also a closer stand-in for a schema
	// file that came from real Paimon.
	schemaJSON := `{
		"id": 0,
		"fields": [
			{"id": 1, "name": "device_id", "type": "BIGINT NOT NULL"},
			{"id": 2, "name": "reading", "type": "FLOAT"},
			{"id": 3, "name": "city", "type": "STRING NOT NULL"}
		],
		"highestFieldId": 3,
		"partitionKeys": ["city"]
	}`
	schemaBytes := []byte(schemaJSON)
	err := memStorage.Write(ctx, "mem://lake/paimon_table/schema/schema-0", schemaBytes)
	require.NoError(t, err)

	// 2. Write snapshot/snapshot-1
	now := time.Now().UnixMilli()
	totalRecords := int64(1500)
	snapObj := paimon.Snapshot{
		Version:          3,
		ID:               1,
		SchemaID:         0,
		TimeMillis:       now,
		TotalRecordCount: &totalRecords,
	}
	snapBytes, err := json.Marshal(snapObj)
	require.NoError(t, err)
	err = memStorage.Write(ctx, "mem://lake/paimon_table/snapshot/snapshot-1", snapBytes)
	require.NoError(t, err)

	// 3. Test Source reads table and snapshot
	source := paimon.NewSource(memStorage, basePath)
	table, err := source.GetCurrentTable(ctx)
	require.NoError(t, err)
	assert.Equal(t, "paimon_table", table.Name)
	assert.Equal(t, model.TableFormatPaimon, table.TableFormat)
	require.Len(t, table.PartitioningFields, 1)
	assert.Equal(t, "city", table.PartitioningFields[0].SourceField.Name)

	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	assert.Equal(t, "1", snapshot.SourceIdentifier)
	assert.Equal(t, now, snapshot.Table.LatestCommitTime)
}
