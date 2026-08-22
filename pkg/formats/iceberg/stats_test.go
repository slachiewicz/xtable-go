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
	"context"
	"encoding/json"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestIceberg_BoundEncoding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		schema  *model.Schema
		value   any
		pos     iceberg.BoundPosition
		wantOK  bool
		want    any
		checkFn func(t *testing.T, decoded any)
	}{
		{
			name:   "int",
			schema: model.NewPrimitiveSchema(model.TypeInt, true),
			value:  int32(42),
			want:   int32(42),
			wantOK: true,
		},
		{
			// A bound that has round-tripped through Delta's stats JSON arrives as float64.
			name:   "int from json float",
			schema: model.NewPrimitiveSchema(model.TypeInt, true),
			value:  float64(42),
			want:   int32(42),
			wantOK: true,
		},
		{
			name:   "int out of range",
			schema: model.NewPrimitiveSchema(model.TypeInt, true),
			value:  int64(math.MaxInt32) + 1,
			wantOK: false,
		},
		{
			name:   "long",
			schema: model.NewPrimitiveSchema(model.TypeLong, true),
			value:  int64(-1 << 40),
			want:   int64(-1 << 40),
			wantOK: true,
		},
		{
			name:   "string",
			schema: model.NewPrimitiveSchema(model.TypeString, true),
			value:  "hello",
			want:   "hello",
			wantOK: true,
		},
		{
			name:   "boolean",
			schema: model.NewPrimitiveSchema(model.TypeBoolean, true),
			value:  true,
			want:   true,
			wantOK: true,
		},
		{
			name:   "double",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			value:  1.5,
			want:   1.5,
			wantOK: true,
		},
		{
			name:   "float",
			schema: model.NewPrimitiveSchema(model.TypeFloat, true),
			value:  float32(2.5),
			want:   float32(2.5),
			wantOK: true,
		},
		{
			name:   "date",
			schema: model.NewPrimitiveSchema(model.TypeDate, true),
			value:  int32(19000),
			want:   int32(19000),
			wantOK: true,
		},
		{
			name:   "timestamp",
			schema: model.NewPrimitiveSchema(model.TypeTimestamp, true),
			value:  int64(1690000000000000),
			want:   int64(1690000000000000),
			wantOK: true,
		},
		{
			name:   "uuid",
			schema: model.NewPrimitiveSchema(model.TypeUUID, true),
			value:  "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			want:   "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			wantOK: true,
		},
		{
			name:   "bytes",
			schema: model.NewPrimitiveSchema(model.TypeBytes, true),
			value:  []byte{0x01, 0xff},
			want:   []byte{0x01, 0xff},
			wantOK: true,
		},
		{
			// Iceberg forbids a NaN bound: it would prune nothing and cannot be JSON-encoded.
			name:   "double NaN",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			value:  math.NaN(),
			wantOK: false,
		},
		{
			name:   "float NaN",
			schema: model.NewPrimitiveSchema(model.TypeFloat, true),
			value:  float32(math.NaN()),
			wantOK: false,
		},
		{
			name:   "double negative zero stays negative as a lower bound",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			value:  math.Copysign(0, -1),
			pos:    iceberg.LowerBound,
			wantOK: true,
			checkFn: func(t *testing.T, decoded any) {
				f, ok := decoded.(float64)
				require.True(t, ok)
				assert.Zero(t, f)
				assert.True(t, math.Signbit(f), "lower bound must keep the negative zero")
			},
		},
		{
			name:   "double positive zero widens to negative as a lower bound",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			value:  0.0,
			pos:    iceberg.LowerBound,
			wantOK: true,
			checkFn: func(t *testing.T, decoded any) {
				f, ok := decoded.(float64)
				require.True(t, ok)
				assert.Zero(t, f)
				assert.True(t, math.Signbit(f), "lower bound must widen 0.0 to -0.0")
			},
		},
		{
			name:   "double negative zero widens to positive as an upper bound",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			value:  math.Copysign(0, -1),
			pos:    iceberg.UpperBound,
			wantOK: true,
			checkFn: func(t *testing.T, decoded any) {
				f, ok := decoded.(float64)
				require.True(t, ok)
				assert.Zero(t, f)
				assert.False(t, math.Signbit(f), "upper bound must widen -0.0 to 0.0")
			},
		},
		{
			name:   "float negative zero widens to positive as an upper bound",
			schema: model.NewPrimitiveSchema(model.TypeFloat, true),
			value:  float32(math.Copysign(0, -1)),
			pos:    iceberg.UpperBound,
			wantOK: true,
			checkFn: func(t *testing.T, decoded any) {
				f, ok := decoded.(float32)
				require.True(t, ok)
				assert.Zero(t, f)
				assert.False(t, math.Signbit(float64(f)))
			},
		},
		{
			// A float bound is the *logical* decimal value, not the unscaled integer the wire
			// format carries, and this port has no schema-driven rescaling for it (see EncodeBound's
			// doc comment) — so it is refused rather than silently reinterpreted as unscaled.
			name:   "decimal from a logical float is refused, not reinterpreted as unscaled",
			schema: model.NewDecimalSchema(10, 2, true),
			value:  1.25,
			wantOK: false,
		},
		{
			name:   "decimal small unscaled value",
			schema: model.NewDecimalSchema(10, 2, true),
			value:  big.NewInt(12345),
			want:   big.NewInt(12345),
			wantOK: true,
		},
		{
			name:   "decimal negative unscaled value",
			schema: model.NewDecimalSchema(10, 2, true),
			value:  big.NewInt(-12345),
			want:   big.NewInt(-12345),
			wantOK: true,
		},
		{
			name:   "decimal accepts a plain int64 unscaled value",
			schema: model.NewDecimalSchema(18, 4, true),
			value:  int64(-1),
			want:   big.NewInt(-1),
			wantOK: true,
		},
		{
			// dec38_0's own recorded bound in test/testdata/fixtures/pyiceberg-torture: the unscaled
			// value of a decimal(38,0) at its widest, an order of magnitude past int64's range.
			name:   "decimal unscaled value above the int64 range",
			schema: model.NewDecimalSchema(38, 0, true),
			value:  mustBigInt(t, "99999999999999999999999999999999999999"),
			want:   mustBigInt(t, "99999999999999999999999999999999999999"),
			wantOK: true,
		},
		{
			name:   "decimal unscaled value below the int64 range",
			schema: model.NewDecimalSchema(38, 0, true),
			value:  mustBigInt(t, "-99999999999999999999999999999999999999"),
			want:   mustBigInt(t, "-99999999999999999999999999999999999999"),
			wantOK: true,
		},
		{
			// A large-scale decimal, decimal(38,37): the scale itself does not change the wire
			// encoding at all — it only changes how the unscaled integer is interpreted, which is
			// the field schema's concern, not EncodeBound/DecodeBound's.
			name:   "decimal at a large scale",
			schema: model.NewDecimalSchema(38, 37, true),
			value:  mustBigInt(t, "-99999999999999999999999999999999999999"),
			want:   mustBigInt(t, "-99999999999999999999999999999999999999"),
			wantOK: true,
		},
		{
			name:   "decimal unscaled zero",
			schema: model.NewDecimalSchema(10, 2, true),
			value:  big.NewInt(0),
			want:   big.NewInt(0),
			wantOK: true,
		},
		{
			name:   "decimal accepts a base-10 unscaled string",
			schema: model.NewDecimalSchema(10, 2, true),
			value:  "-129",
			want:   big.NewInt(-129),
			wantOK: true,
		},
		{
			name:   "nil value",
			schema: model.NewPrimitiveSchema(model.TypeLong, true),
			value:  nil,
			wantOK: false,
		},
		{
			name:   "nil schema",
			schema: nil,
			value:  int64(1),
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded, ok := iceberg.EncodeBound(tt.schema, tt.value, tt.pos)
			require.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				assert.Empty(t, encoded)
				return
			}

			decoded, ok := iceberg.DecodeBound(tt.schema, encoded)
			require.True(t, ok)
			if tt.checkFn != nil {
				tt.checkFn(t, decoded)
				return
			}
			// big.Int has two internal representations of zero (nil abs versus an empty, non-nil
			// nat), which reflect.DeepEqual — and so assert.Equal — treats as unequal even though
			// Cmp reports them as the same value. Compare numerically for *big.Int, structurally
			// otherwise.
			if wantBig, ok := tt.want.(*big.Int); ok {
				decodedBig, isBig := decoded.(*big.Int)
				require.True(t, isBig, "decoded value is not *big.Int: %T", decoded)
				assert.Zero(t, wantBig.Cmp(decodedBig), "want %s, got %s", wantBig, decodedBig)
				return
			}
			assert.Equal(t, tt.want, decoded)
		})
	}
}

func TestIceberg_DecodeBoundRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		schema *model.Schema
		raw    []byte
	}{
		{name: "empty", schema: model.NewPrimitiveSchema(model.TypeLong, true), raw: nil},
		{name: "wrong length for long", schema: model.NewPrimitiveSchema(model.TypeLong, true), raw: []byte{1, 2}},
		{name: "wrong length for int", schema: model.NewPrimitiveSchema(model.TypeInt, true), raw: []byte{1, 2}},
		{name: "wrong length for uuid", schema: model.NewPrimitiveSchema(model.TypeUUID, true), raw: []byte{1, 2}},
		// Eight bytes of a quiet NaN, little-endian: what a writer that ignores the Iceberg rule
		// would emit. Decoding must refuse it rather than let a NaN into the model.
		{
			name:   "nan payload for double",
			schema: model.NewPrimitiveSchema(model.TypeDouble, true),
			raw:    []byte{0, 0, 0, 0, 0, 0, 0xf8, 0x7f},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			decoded, ok := iceberg.DecodeBound(tt.schema, tt.raw)
			assert.False(t, ok)
			assert.Nil(t, decoded)
		})
	}
}

// TestIceberg_EncodeBoundProducesNonNilEmptyForAnEmptyValue closes the other direction of T70 defect
// 5: EncodeBound("") must itself produce a non-nil, zero-length []byte, not a nil one — otherwise the
// exact-inverse claim (encode then decode reproduces the original value) would not hold for the
// empty value in the first place, regardless of what DecodeBound does with it.
func TestIceberg_EncodeBoundProducesNonNilEmptyForAnEmptyValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		schema *model.Schema
		value  any
	}{
		{name: "empty string", schema: model.NewPrimitiveSchema(model.TypeString, true), value: ""},
		{name: "empty bytes", schema: model.NewPrimitiveSchema(model.TypeBytes, true), value: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded, ok := iceberg.EncodeBound(tt.schema, tt.value, iceberg.LowerBound)
			require.True(t, ok)
			require.NotNil(t, encoded, "EncodeBound produced a nil []byte for an empty value, "+
				"indistinguishable from omitting the bound entirely")
			assert.Empty(t, encoded)

			decoded, ok := iceberg.DecodeBound(tt.schema, encoded)
			require.True(t, ok)
			assert.Equal(t, tt.value, decoded)
		})
	}
}

// TestIceberg_DecodeBoundDistinguishesMissingFromEmpty pins T70 defect 5: a nil []byte ("this field
// id is not in the manifest's bounds map at all") and a non-nil, zero-length []byte ("the writer
// recorded an empty string or empty binary minimum") used to collapse to the same "no bound"
// result. They must not.
func TestIceberg_DecodeBoundDistinguishesMissingFromEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		schema  *model.Schema
		raw     []byte
		wantOK  bool
		wantVal any
	}{
		{
			name:   "missing string bound is nil raw and reports false",
			schema: model.NewPrimitiveSchema(model.TypeString, true),
			raw:    nil,
			wantOK: false,
		},
		{
			name:    "present empty string bound decodes to the empty string",
			schema:  model.NewPrimitiveSchema(model.TypeString, true),
			raw:     []byte{},
			wantOK:  true,
			wantVal: "",
		},
		{
			name:   "missing binary bound is nil raw and reports false",
			schema: model.NewPrimitiveSchema(model.TypeBytes, true),
			raw:    nil,
			wantOK: false,
		},
		{
			name:    "present empty binary bound decodes to a non-nil empty slice",
			schema:  model.NewPrimitiveSchema(model.TypeBytes, true),
			raw:     []byte{},
			wantOK:  true,
			wantVal: []byte{},
		},
		{
			// A fixed-width type has no zero-length encoding at all, so an empty-but-present bound
			// here is a malformed manifest entry: still "no usable bound", but for a length reason
			// rather than the missing-vs-empty distinction this test otherwise exercises.
			name:   "present empty bound for a fixed-width type is still rejected, by length",
			schema: model.NewPrimitiveSchema(model.TypeLong, true),
			raw:    []byte{},
			wantOK: false,
		},
		{
			name:   "present empty decimal bound is rejected: zero has no zero-length encoding",
			schema: model.NewDecimalSchema(10, 2, true),
			raw:    []byte{},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			decoded, ok := iceberg.DecodeBound(tt.schema, tt.raw)
			require.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				assert.Nil(t, decoded)
				return
			}
			assert.Equal(t, tt.wantVal, decoded)
			// assert.Equal on []byte treats nil and an empty slice as equal (bytes.Equal), which
			// would hide exactly the defect this test exists to catch, so nilness is checked
			// separately for the binary case.
			if _, isBytes := tt.wantVal.([]byte); isBytes {
				assert.NotNil(t, decoded, "empty binary bound decoded to a nil slice, indistinguishable from missing")
			}
		})
	}
}

func TestIceberg_ColumnStatsSurviveACommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/iceberg_stats"

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	nameField := &model.Field{Name: "name", Schema: model.NewPrimitiveSchema(model.TypeString, true)}
	scoreField := &model.Field{Name: "score", Schema: model.NewPrimitiveSchema(model.TypeDouble, true)}
	schema := model.NewRecordSchema("events", []*model.Field{idField, nameField, scoreField}, false)

	table := &model.Table{
		Name:             "events",
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  io.JoinPath(basePath, "data", "part-0.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 2048,
		RecordCount:   10,
		ColumnStats: []*model.ColumnStat{
			{Field: idField, Range: model.NewRange(int64(1), int64(10)), TotalValues: 10},
			{Field: nameField, Range: model.NewRange("alice", "zoe"), NumNulls: 2, TotalValues: 10},
			{Field: scoreField, Range: model.NewRange(-1.5, 99.25), NumNulls: 1, NumNaNs: 3, TotalValues: 10},
		},
	}

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "snap-1",
	}))

	source := iceberg.NewSource(storage, basePath)
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 1)

	byName := statsByFieldName(t, snapshot.DataFiles[0].ColumnStats)
	require.Len(t, byName, 3)

	require.NotNil(t, byName["id"].Range)
	assert.Equal(t, int64(1), byName["id"].Range.MinValue)
	assert.Equal(t, int64(10), byName["id"].Range.MaxValue)
	assert.Equal(t, int64(10), byName["id"].TotalValues)

	require.NotNil(t, byName["name"].Range)
	assert.Equal(t, "alice", byName["name"].Range.MinValue)
	assert.Equal(t, "zoe", byName["name"].Range.MaxValue)
	assert.Equal(t, int64(2), byName["name"].NumNulls)

	require.NotNil(t, byName["score"].Range)
	assert.InDelta(t, -1.5, byName["score"].Range.MinValue, 0)
	assert.InDelta(t, 99.25, byName["score"].Range.MaxValue, 0)
	assert.Equal(t, int64(1), byName["score"].NumNulls)
	assert.Equal(t, int64(3), byName["score"].NumNaNs)
}

// TestIceberg_ColumnStatsKeyedByFieldID pins the rename behaviour: the manifest records bounds
// against the Iceberg field ID, so reading a snapshot whose column has since been renamed still
// attaches the bounds — under the new name.
func TestIceberg_ColumnStatsKeyedByFieldID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/iceberg_rename"

	amountField := &model.Field{Name: "amount", Schema: model.NewPrimitiveSchema(model.TypeLong, true)}
	table := &model.Table{
		Name:             "orders",
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       model.NewRecordSchema("orders", []*model.Field{amountField}, false),
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table: table,
		DataFiles: []*model.DataFile{{
			PhysicalPath:  io.JoinPath(basePath, "data", "part-0.parquet"),
			FileFormat:    model.FileFormatParquet,
			FileSizeBytes: 512,
			RecordCount:   7,
			ColumnStats: []*model.ColumnStat{
				{Field: amountField, Range: model.NewRange(int64(5), int64(500)), TotalValues: 7, NumNulls: 1},
			},
		}},
		SourceIdentifier: "snap-1",
	}))

	// The column was called "amount" when the manifest was written. Renaming it in a later metadata
	// version leaves the manifest untouched, which is the situation the field ID exists for: the
	// bounds have to follow the ID, not the name.
	committed := readMetadata(t, storage, io.JoinPath(basePath, "metadata", "v1.metadata.json"))
	require.Len(t, committed.Schemas, 1)
	require.Len(t, committed.Schemas[0].Fields, 1)
	require.Equal(t, "amount", committed.Schemas[0].Fields[0].Name)
	committed.Schemas[0].Fields[0].Name = "amount_eur"
	writeJSON(t, storage, io.JoinPath(basePath, "metadata", "v2.metadata.json"), committed)

	source := iceberg.NewSource(storage, basePath)
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	require.Len(t, snapshot.DataFiles, 1)
	require.Len(t, snapshot.DataFiles[0].ColumnStats, 1)

	stat := snapshot.DataFiles[0].ColumnStats[0]
	assert.Equal(t, "amount_eur", stat.Field.Name)
	require.NotNil(t, stat.Range)
	assert.Equal(t, int64(5), stat.Range.MinValue)
	assert.Equal(t, int64(500), stat.Range.MaxValue)
	assert.Equal(t, int64(1), stat.NumNulls)
	assert.Equal(t, int64(7), stat.TotalValues)
}

// mustBigInt parses a base-10 unscaled decimal value used across the DECIMAL test cases; the
// literal is kept as a string in the test table because it exceeds what an untyped Go integer
// constant can hold portably.
func mustBigInt(t *testing.T, s string) *big.Int {
	t.Helper()
	n, ok := new(big.Int).SetString(s, 10)
	require.True(t, ok, "malformed big.Int literal in test table: %q", s)
	return n
}

func statsByFieldName(t *testing.T, stats []*model.ColumnStat) map[string]*model.ColumnStat {
	t.Helper()
	byName := make(map[string]*model.ColumnStat, len(stats))
	for _, cs := range stats {
		require.NotNil(t, cs.Field)
		byName[cs.Field.Name] = cs
	}
	return byName
}

func writeJSON(t *testing.T, storage io.Storage, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, storage.Write(context.Background(), path, data))
}

// readMetadata parses a metadata.json the target wrote. Unlike a manifest, it really is JSON.
func readMetadata(t *testing.T, storage io.Storage, path string) *iceberg.TableMetadata {
	t.Helper()
	data, err := storage.Read(context.Background(), path)
	require.NoError(t, err)
	var meta iceberg.TableMetadata
	require.NoError(t, json.Unmarshal(data, &meta))
	return &meta
}
