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

// Tests for T70 defect 1: a struct column's nested nullCount (and minValues/maxValues) used to fail
// json.Unmarshal against StatsJSON's flat map[string]int64, and convertAddAction's `if err == nil`
// silently discarded the whole stats blob -- RecordCount and every other column's bounds included --
// with nothing surfaced. These tests hand-write the commit JSON rather than going through
// delta.Target, because the case under test (a nested struct column's stats) is a shape this
// package's own target never produces; test/torture_types_test.go covers the same defect against a
// real delta-rs fixture, which this table-driven suite complements with the specific malformed shapes
// a fixture cannot easily carry.
package delta_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/delta"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// writeDeltaAddWithRawStats writes a single-commit Delta log whose schema is a "value" long column
// plus a "meta" struct column with a "count" long child, and whose one add action carries rawStats
// verbatim as AddAction.Stats -- letting the test control the exact JSON shape, nested or malformed,
// that a foreign writer put there.
func writeDeltaAddWithRawStats(t *testing.T, storage io.Storage, basePath, rawStats string) {
	t.Helper()
	ctx := context.Background()

	metaSchema := model.NewRecordSchema("meta", []*model.Field{
		{Name: "count", Schema: model.NewPrimitiveSchema(model.TypeLong, true)},
	}, true)
	schemaJSON, err := delta.SchemaToDeltaJSON(model.NewRecordSchema("fixture", []*model.Field{
		{Name: "value", Schema: model.NewPrimitiveSchema(model.TypeLong, true)},
		{Name: "meta", Schema: metaSchema},
	}, false))
	require.NoError(t, err)

	actions := []delta.SingleAction{
		{MetaData: &delta.MetadataAction{
			ID:           "fixture",
			Name:         "fixture",
			Format:       delta.NewParquetFormat(),
			SchemaString: schemaJSON,
		}},
		{Add: &delta.AddAction{
			Path:       "part-0.parquet",
			Size:       128,
			DataChange: true,
			Stats:      rawStats,
		}},
		{CommitInfo: &delta.CommitInfoAction{Timestamp: 1, Operation: "WRITE"}},
	}

	var buf bytes.Buffer
	for _, action := range actions {
		line, err := json.Marshal(action)
		require.NoError(t, err)
		buf.Write(line)
		buf.WriteByte('\n')
	}
	require.NoError(t, storage.Write(ctx, io.JoinPath(basePath, "_delta_log", "00000000000000000000.json"), buf.Bytes()))
}

func TestDelta_NestedStructStatsFlatten(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/nested_stats"

	// A real delta-rs commit nests a struct column's bounds and null count as a JSON object rather
	// than a flat scalar. "value" stays flat; "meta.count" is nested exactly the way delta-rs writes
	// it: "nullCount":{"value":0,"meta":{"count":2}}.
	rawStats := `{"numRecords":5,` +
		`"minValues":{"value":1,"meta":{"count":10}},` +
		`"maxValues":{"value":9,"meta":{"count":90}},` +
		`"nullCount":{"value":0,"meta":{"count":2}}}`
	writeDeltaAddWithRawStats(t, storage, basePath, rawStats)

	source := delta.NewSource(storage, basePath)
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err, "a legitimately nested stats blob must not fail the read")
	require.Len(t, snapshot.DataFiles, 1)
	file := snapshot.DataFiles[0]

	assert.Equal(t, int64(5), file.RecordCount, "RecordCount must survive a nested null count elsewhere in the blob")

	var valueStat, countStat *model.ColumnStat
	for _, stat := range file.ColumnStats {
		switch stat.Field.Name {
		case "value":
			valueStat = stat
		case "count":
			countStat = stat
		}
	}

	require.NotNil(t, valueStat, "flat column value lost its stats to an unrelated nested column")
	require.NotNil(t, valueStat.Range)
	assert.InDelta(t, 1, valueStat.Range.MinValue, 0)
	assert.InDelta(t, 9, valueStat.Range.MaxValue, 0)
	assert.Equal(t, int64(0), valueStat.NumNulls)

	require.NotNil(t, countStat, "nested column meta.count was not flattened into its own statistic")
	require.NotNil(t, countStat.Range)
	assert.InDelta(t, 10, countStat.Range.MinValue, 0)
	assert.InDelta(t, 90, countStat.Range.MaxValue, 0)
	assert.Equal(t, int64(2), countStat.NumNulls)
}

func TestDelta_MalformedStatsSurfacesError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rawStats  string
		wantMatch string
	}{
		{
			name:      "not JSON at all",
			rawStats:  `not json`,
			wantMatch: "part-0.parquet",
		},
		{
			name:      "nullCount leaf is not a number",
			rawStats:  `{"numRecords":5,"nullCount":{"value":"oops"}}`,
			wantMatch: "part-0.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			storage := io.NewMemoryStorage()
			basePath := "mem://lake/malformed_stats/" + tt.name
			writeDeltaAddWithRawStats(t, storage, basePath, tt.rawStats)

			source := delta.NewSource(storage, basePath)
			_, err := source.GetCurrentSnapshot(ctx)
			require.Error(t, err, "a genuinely malformed stats blob must not be silently dropped")
			assert.Contains(t, err.Error(), tt.wantMatch, "the error should name the offending file")
		})
	}
}
