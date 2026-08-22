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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/delta"
	"github.com/slachiewicz/polytable/pkg/io"
)

// The row structs below mirror the checkpoint Parquet schema from the outside, the way a foreign
// writer would lay it out. They deliberately do not reuse the reader's own types (which are
// unexported anyway): a test that wrote with the reader's structs would inherit its mistakes.

type testCheckpointMeta struct {
	ID               string            `parquet:"id"`
	Format           testFormat        `parquet:"format"`
	SchemaString     string            `parquet:"schemaString"`
	PartitionColumns []string          `parquet:"partitionColumns,list"`
	Configuration    map[string]string `parquet:"configuration"`
}

type testFormat struct {
	Provider string            `parquet:"provider"`
	Options  map[string]string `parquet:"options"`
}

type testCheckpointAdd struct {
	Path             string             `parquet:"path"`
	PartitionValues  map[string]*string `parquet:"partitionValues"`
	Size             int64              `parquet:"size"`
	ModificationTime int64              `parquet:"modificationTime"`
	DataChange       bool               `parquet:"dataChange"`
}

type testCheckpointSidecar struct {
	Path string `parquet:"path"`
}

type testCheckpointRow struct {
	Add      *testCheckpointAdd     `parquet:"add,optional"`
	MetaData *testCheckpointMeta    `parquet:"metaData,optional"`
	Sidecar  *testCheckpointSidecar `parquet:"sidecar,optional"`
}

const testSchemaString = `{"type":"struct","fields":[` +
	`{"name":"id","type":"long","nullable":false,"metadata":{}}]}`

func writeParquetRows(t *testing.T, storage io.Storage, path string, rows []testCheckpointRow) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, parquet.Write(&buf, rows))
	require.NoError(t, storage.Write(context.Background(), path, buf.Bytes()))
}

func metaRow() testCheckpointRow {
	return testCheckpointRow{MetaData: &testCheckpointMeta{
		ID:           "0000",
		Format:       testFormat{Provider: "parquet", Options: map[string]string{}},
		SchemaString: testSchemaString,
	}}
}

func addRow(path string) testCheckpointRow {
	return testCheckpointRow{Add: &testCheckpointAdd{
		Path: path, PartitionValues: map[string]*string{}, Size: 1, ModificationTime: 1, DataChange: true,
	}}
}

// TestCheckpoint_V2SidecarRejected pins the guard: a v2 checkpoint's state lives in sidecar files
// this reader does not follow, so it must refuse rather than return a partial snapshot.
func TestCheckpoint_V2SidecarRejected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storage := io.NewMemoryStorage()
	base := "mem://v2table"

	writeParquetRows(t, storage, base+"/_delta_log/00000000000000000001.checkpoint.parquet",
		[]testCheckpointRow{metaRow(), testCheckpointRow{Sidecar: &testCheckpointSidecar{Path: "sc-1.parquet"}}})
	require.NoError(t, storage.Write(ctx, base+"/_delta_log/_last_checkpoint",
		[]byte(`{"version":1,"size":2}`)))

	_, err := delta.NewSource(storage, base).GetCurrentSnapshot(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "v2 checkpoint")
}

// TestCheckpoint_MultiPart covers the parts field of _last_checkpoint: state split across the
// multi-part file naming scheme must be merged.
func TestCheckpoint_MultiPart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storage := io.NewMemoryStorage()
	base := "mem://multipart"

	logDir := base + "/_delta_log"
	writeParquetRows(t, storage, fmt.Sprintf("%s/%020d.checkpoint.%010d.%010d.parquet", logDir, 1, 1, 2),
		[]testCheckpointRow{metaRow(), addRow("part-a.parquet")})
	writeParquetRows(t, storage, fmt.Sprintf("%s/%020d.checkpoint.%010d.%010d.parquet", logDir, 1, 2, 2),
		[]testCheckpointRow{addRow("part-b.parquet")})
	require.NoError(t, storage.Write(ctx, logDir+"/_last_checkpoint",
		[]byte(`{"version":1,"size":3,"parts":2}`)))

	snapshot, err := delta.NewSource(storage, base).GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 2)
	assert.Equal(t, "1", snapshot.SourceIdentifier)
}

// TestCheckpoint_PartitionValueNullVsEmpty is the classic Parquet checkpoint counterpart of T70
// defect 2: a partitionValues map entry can be a genuine Parquet null (this fixture's "north"
// file) or the empty string (its "south" file), and parquet-go must round-trip the distinction
// through cpAdd's map[string]*string the same way encoding/json does for the JSON commit log.
func TestCheckpoint_PartitionValueNullVsEmpty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storage := io.NewMemoryStorage()
	base := "mem://checkpoint-partition-null"

	const partitionedSchema = `{"type":"struct","fields":[` +
		`{"name":"id","type":"long","nullable":false,"metadata":{}},` +
		`{"name":"region","type":"string","nullable":true,"metadata":{}}]}`

	empty := ""
	meta := testCheckpointRow{MetaData: &testCheckpointMeta{
		ID:               "0000",
		Format:           testFormat{Provider: "parquet", Options: map[string]string{}},
		SchemaString:     partitionedSchema,
		PartitionColumns: []string{"region"},
	}}
	nullRow := testCheckpointRow{Add: &testCheckpointAdd{
		Path:            "region=__HIVE_DEFAULT_PARTITION__/part-north.parquet",
		PartitionValues: map[string]*string{"region": nil},
		Size:            1, ModificationTime: 1, DataChange: true,
	}}
	emptyRow := testCheckpointRow{Add: &testCheckpointAdd{
		Path:            "region=/part-south.parquet",
		PartitionValues: map[string]*string{"region": &empty},
		Size:            1, ModificationTime: 1, DataChange: true,
	}}

	logDir := base + "/_delta_log"
	writeParquetRows(t, storage, fmt.Sprintf("%s/%020d.checkpoint.parquet", logDir, 0),
		[]testCheckpointRow{meta, nullRow, emptyRow})
	require.NoError(t, storage.Write(ctx, logDir+"/_last_checkpoint",
		[]byte(`{"version":0,"size":3}`)))

	snapshot, err := delta.NewSource(storage, base).GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 2)

	var sawNull, sawEmpty bool
	for _, file := range snapshot.DataFiles {
		require.Len(t, file.PartitionValues, 1)
		switch file.PartitionValues[0].Range.MinValue {
		case nil:
			sawNull = true
		case "":
			sawEmpty = true
		}
	}
	assert.True(t, sawNull, "no data file reported a nil (null) partition value from the checkpoint")
	assert.True(t, sawEmpty, "no data file reported an empty-string partition value from the checkpoint")
}
