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
	"encoding/binary"
	"math"
	"math/big"
	"strconv"

	"github.com/google/uuid"

	"github.com/slachiewicz/polytable/pkg/model"
)

// BoundPosition identifies which end of a model.Range a bound value occupies.
//
// It changes only the encoding of float and double columns. Iceberg orders those bounds the way
// Float.compare does, and under that ordering -0.0 sorts below 0.0 even though the two compare
// equal under IEEE 754. A zero bound is therefore widened away from the range — a lower bound
// becomes -0.0 and an upper bound becomes 0.0 — because otherwise a predicate on -0.0 prunes a
// file that really does hold 0.0.
type BoundPosition int

const (
	// LowerBound marks the minimum end of a range.
	LowerBound BoundPosition = iota
	// UpperBound marks the maximum end of a range.
	UpperBound
)

// EncodeBound serializes value for the given field schema using Iceberg's single-value binary
// serialization, which is what the `lower_bounds` and `upper_bounds` maps of a manifest entry hold.
//
// It reports false when the value carries no usable bound: a nil value, a NaN float — Iceberg
// forbids NaN bounds and tracks NaN counts separately — or a type this port does not serialize (the
// nested types have no single-value form at all). Callers omit the bound in that case rather than
// failing.
//
// DECIMAL encodes the unscaled value (schema and scale agree on where the point goes; the wire
// format only ever carries the integer) as minimal two's-complement big-endian bytes, per the
// specification. The accepted Go shapes are *big.Int, the built-in integer types, and a base-10
// string of the unscaled integer — deliberately not float32/float64: a float bound arriving here is
// the *logical* decimal value (e.g. from Delta's JSON-decoded stats), and reinterpreting it as
// already-unscaled would silently write the wrong bound rather than merely omitting it. That is
// worse than the previous behavior, so it is refused here the same as any other unusable value.
func EncodeBound(schema *model.Schema, value any, pos BoundPosition) ([]byte, bool) {
	if schema == nil || value == nil {
		return nil, false
	}

	switch schema.DataType {
	case model.TypeBoolean:
		b, ok := coerceBool(value)
		if !ok {
			return nil, false
		}
		out := []byte{0}
		if b {
			out[0] = 1
		}
		return out, true

	case model.TypeInt, model.TypeDate:
		n, ok := coerceInt64(value)
		if !ok || n < math.MinInt32 || n > math.MaxInt32 {
			return nil, false
		}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(int32(n))) //nolint:gosec // n is range-checked against int32 above
		return buf, true

	case model.TypeLong, model.TypeTimestamp, model.TypeTimestampNTZ:
		n, ok := coerceInt64(value)
		if !ok {
			return nil, false
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(n)) //nolint:gosec // two's-complement bit pattern, decoded back to int64
		return buf, true

	case model.TypeFloat:
		f, ok := coerceFloat64(value)
		if !ok || math.IsNaN(f) {
			return nil, false
		}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(float32(normalizeZeroBound(f, pos))))
		return buf, true

	case model.TypeDouble:
		f, ok := coerceFloat64(value)
		if !ok || math.IsNaN(f) {
			return nil, false
		}
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(normalizeZeroBound(f, pos)))
		return buf, true

	case model.TypeString, model.TypeEnum:
		s, ok := coerceString(value)
		if !ok {
			return nil, false
		}
		return []byte(s), true

	case model.TypeUUID:
		s, ok := coerceString(value)
		if !ok {
			return nil, false
		}
		parsed, err := uuid.Parse(s)
		if err != nil {
			return nil, false
		}
		return parsed[:], true

	case model.TypeBytes, model.TypeFixed:
		b, ok := coerceBytes(value)
		if !ok {
			return nil, false
		}
		return b, true

	case model.TypeDecimal:
		n, ok := coerceBigInt(value)
		if !ok {
			return nil, false
		}
		return encodeUnscaledDecimal(n), true

	default:
		// The nested types have no single-value form at all. Omitting the bound loses pruning
		// power; writing it wrong loses rows.
		return nil, false
	}
}

// DecodeBound reverses EncodeBound, returning a Go value typed after the field schema: int32 for
// int and date, int64 for long and the timestamps, float32/float64 for the floating types, string
// for string, enum and uuid, []byte for binary and fixed, *big.Int (the unscaled value) for decimal.
//
// It reports false for a *missing* bound, for a length that does not match the type, and for a NaN
// float — a NaN that reaches this point came from a writer that ignored the Iceberg rule, and
// letting it into the model would break every JSON encoder downstream.
//
// "Missing" is raw == nil specifically, not len(raw) == 0: a manifest entry can legitimately record
// an empty string or an empty binary minimum, and that has to decode to "" / a non-nil empty []byte
// rather than collapse to the same "no bound at all" result a genuinely absent entry produces.
// Callers reading a bound out of a map — a Go map returns the nil zero value for a missing key —
// get this distinction for free; a caller that cannot tell the two apart at the source must
// normalize an unrecorded entry to nil and a recorded-but-empty one to a non-nil empty slice before
// calling this.
func DecodeBound(schema *model.Schema, raw []byte) (any, bool) {
	if schema == nil || raw == nil {
		return nil, false
	}

	switch schema.DataType {
	case model.TypeBoolean:
		if len(raw) != 1 {
			return nil, false
		}
		return raw[0] != 0, true

	case model.TypeInt, model.TypeDate:
		if len(raw) != 4 {
			return nil, false
		}
		return int32(binary.LittleEndian.Uint32(raw)), true //nolint:gosec // two's-complement bit pattern written by EncodeBound

	case model.TypeLong, model.TypeTimestamp, model.TypeTimestampNTZ:
		if len(raw) != 8 {
			return nil, false
		}
		return int64(binary.LittleEndian.Uint64(raw)), true //nolint:gosec // two's-complement bit pattern written by EncodeBound

	case model.TypeFloat:
		if len(raw) != 4 {
			return nil, false
		}
		f := math.Float32frombits(binary.LittleEndian.Uint32(raw))
		if math.IsNaN(float64(f)) {
			return nil, false
		}
		return f, true

	case model.TypeDouble:
		if len(raw) != 8 {
			return nil, false
		}
		f := math.Float64frombits(binary.LittleEndian.Uint64(raw))
		if math.IsNaN(f) {
			return nil, false
		}
		return f, true

	case model.TypeString, model.TypeEnum:
		return string(raw), true

	case model.TypeUUID:
		parsed, err := uuid.FromBytes(raw)
		if err != nil {
			return nil, false
		}
		return parsed.String(), true

	case model.TypeBytes, model.TypeFixed:
		return raw, true

	case model.TypeDecimal:
		// Unlike the fixed-width numeric types above, DECIMAL has no representation for a
		// zero-length payload — the minimal encoding of 0 is one byte (0x00) — so an empty-but-
		// present bound here is a malformed manifest entry, not a legitimate value.
		if len(raw) == 0 {
			return nil, false
		}
		return decodeUnscaledDecimal(raw), true

	default:
		return nil, false
	}
}

// normalizeZeroBound widens a zero float bound away from the range. See BoundPosition.
func normalizeZeroBound(f float64, pos BoundPosition) float64 {
	if f != 0 {
		return f
	}
	if pos == LowerBound {
		return math.Copysign(0, -1)
	}
	return math.Copysign(0, 1)
}

// columnStatsFromManifest maps the per-field statistic maps of a manifest entry onto the canonical
// model. Entries are keyed by Iceberg field ID rather than by name, so a column renamed between
// the snapshot and the current schema keeps its bounds. Fields the schema carries no ID for —
// IcebergToSchema assigns IDs to top-level fields only — are skipped.
func columnStatsFromManifest(mdf *ManifestDataFile, schema *model.Schema) []*model.ColumnStat {
	if mdf == nil || schema == nil {
		return nil
	}

	var stats []*model.ColumnStat
	for _, field := range schema.AllFields() {
		if field.FieldID == nil {
			continue
		}
		id := *field.FieldID
		if !manifestHasStatsFor(mdf, id) {
			continue
		}

		stat := &model.ColumnStat{
			Field:       field,
			TotalValues: mdf.ValueCounts[id],
			NumNulls:    mdf.NullValueCounts[id],
			NumNaNs:     mdf.NanValueCounts[id],
		}
		minVal, hasMin := DecodeBound(field.Schema, boundBytes(mdf.LowerBounds, id))
		maxVal, hasMax := DecodeBound(field.Schema, boundBytes(mdf.UpperBounds, id))
		if hasMin || hasMax {
			stat.Range = model.NewRange(minVal, maxVal)
		}
		stats = append(stats, stat)
	}
	return stats
}

// boundBytes reads a bound out of a manifest's bytes map, normalizing what DecodeBound sees so that
// "no entry for this field id" and "entry present but the Avro decoder handed back a nil slice for
// a zero-length value" are not conflated. A plain map index cannot make this distinction on its
// own: both a missing key and a present key whose value happens to be a nil []byte return the same
// nil. The two-value form recovers presence; a present-but-nil value is then normalized to a
// non-nil empty slice, which DecodeBound treats as a genuinely empty (not missing) bound.
func boundBytes(m map[int][]byte, id int) []byte {
	raw, ok := m[id]
	if !ok {
		return nil
	}
	if raw == nil {
		return []byte{}
	}
	return raw
}

// manifestHasStatsFor reports whether any statistic map mentions the field. Presence in the bound
// maps alone is enough: a table converted from a format that records bounds without value counts
// (Delta does) would otherwise lose its bounds on the way back in.
func manifestHasStatsFor(mdf *ManifestDataFile, id int) bool {
	if _, ok := mdf.ValueCounts[id]; ok {
		return true
	}
	if _, ok := mdf.NullValueCounts[id]; ok {
		return true
	}
	if _, ok := mdf.NanValueCounts[id]; ok {
		return true
	}
	if _, ok := mdf.LowerBounds[id]; ok {
		return true
	}
	_, ok := mdf.UpperBounds[id]
	return ok
}

// columnStatsToManifest fills the statistic maps of a manifest data file from the canonical column
// stats. Field IDs come from the Iceberg schema being written, which is authoritative — the IDs a
// model field carries may have come from an unrelated source table, or be absent entirely.
func columnStatsToManifest(mdf *ManifestDataFile, stats []*model.ColumnStat, tableSchema *TableSchema) {
	if mdf == nil || len(stats) == 0 || tableSchema == nil {
		return
	}

	idsByName := make(map[string]int, len(tableSchema.Fields))
	for _, f := range tableSchema.Fields {
		idsByName[f.Name] = f.ID
	}

	for _, cs := range stats {
		if cs == nil || cs.Field == nil {
			continue
		}
		// Nested columns are not addressable here: the manifest keys on a leaf field ID and only
		// top-level IDs are resolvable from the written schema.
		id, ok := idsByName[cs.Field.Path()]
		if !ok {
			continue
		}

		putStat(&mdf.ValueCounts, id, cs.TotalValues)
		putStat(&mdf.NullValueCounts, id, cs.NumNulls)
		if cs.NumNaNs > 0 {
			putStat(&mdf.NanValueCounts, id, cs.NumNaNs)
		}
		if cs.Range == nil {
			continue
		}
		if encoded, ok := EncodeBound(cs.Field.Schema, cs.Range.MinValue, LowerBound); ok {
			putBound(&mdf.LowerBounds, id, encoded)
		}
		if encoded, ok := EncodeBound(cs.Field.Schema, cs.Range.MaxValue, UpperBound); ok {
			putBound(&mdf.UpperBounds, id, encoded)
		}
	}
}

func putStat(m *map[int]int64, id int, value int64) {
	if *m == nil {
		*m = make(map[int]int64)
	}
	(*m)[id] = value
}

func putBound(m *map[int][]byte, id int, value []byte) {
	if *m == nil {
		*m = make(map[int][]byte)
	}
	(*m)[id] = value
}

func coerceBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case string:
		b, err := strconv.ParseBool(x)
		return b, err == nil
	default:
		return false, false
	}
}

// coerceInt64 accepts the shapes an integral bound arrives in. A bound that has round-tripped
// through Delta's stats JSON comes back as float64, so that arm is load-bearing rather than
// defensive; a non-integral float is rejected because it signals a type mismatch, not a bound.
func coerceInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		if x > math.MaxInt64 {
			return 0, false
		}
		return int64(x), true
	case float32:
		return floatToInt64(float64(x))
	case float64:
		return floatToInt64(x)
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func floatToInt64(f float64) (int64, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) {
		return 0, false
	}
	if f < math.MinInt64 || f >= math.MaxInt64 {
		return 0, false
	}
	return int64(f), true
}

func coerceFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func coerceString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	default:
		return "", false
	}
}

func coerceBytes(v any) ([]byte, bool) {
	switch x := v.(type) {
	case []byte:
		return x, true
	case string:
		return []byte(x), true
	default:
		return nil, false
	}
}

// coerceBigInt accepts the shapes a decimal bound's unscaled value arrives in: *big.Int and big.Int
// itself (what DecodeBound hands back, round-tripped), the built-in integer types, and a base-10
// string of the unscaled integer. Floating-point is deliberately not accepted here — see
// EncodeBound's doc comment for why treating a logical float as an already-unscaled integer would
// be worse than refusing it.
func coerceBigInt(v any) (*big.Int, bool) {
	switch x := v.(type) {
	case *big.Int:
		if x == nil {
			return nil, false
		}
		return x, true
	case big.Int:
		return new(big.Int).Set(&x), true
	case int:
		return big.NewInt(int64(x)), true
	case int8:
		return big.NewInt(int64(x)), true
	case int16:
		return big.NewInt(int64(x)), true
	case int32:
		return big.NewInt(int64(x)), true
	case int64:
		return big.NewInt(x), true
	case uint:
		return new(big.Int).SetUint64(uint64(x)), true
	case uint8:
		return big.NewInt(int64(x)), true
	case uint16:
		return big.NewInt(int64(x)), true
	case uint32:
		return big.NewInt(int64(x)), true
	case uint64:
		return new(big.Int).SetUint64(x), true
	case string:
		n, ok := new(big.Int).SetString(x, 10)
		if !ok {
			return nil, false
		}
		return n, true
	default:
		return nil, false
	}
}

// encodeUnscaledDecimal renders n as the minimal-length two's-complement big-endian byte sequence
// the Iceberg specification requires for a DECIMAL single value: zero is one zero byte, a positive
// value gains a leading 0x00 byte whenever its high bit would otherwise read as a sign bit, and a
// negative value uses the smallest byte count whose two's-complement range contains it.
func encodeUnscaledDecimal(n *big.Int) []byte {
	switch n.Sign() {
	case 0:
		return []byte{0}
	case 1:
		b := n.Bytes()
		if b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	default:
		abs := new(big.Int).Neg(n)
		numBytes := (abs.BitLen() + 7) / 8
		if numBytes == 0 {
			numBytes = 1
		}
		// A k-byte two's-complement value can represent down to -2^(8k-1); grow by one byte if the
		// magnitude does not fit that range (e.g. -129 needs two bytes, not the one its bit length
		// alone would suggest).
		limit := new(big.Int).Lsh(big.NewInt(1), uint(8*numBytes-1)) //nolint:gosec // numBytes is small and non-negative
		if abs.Cmp(limit) > 0 {
			numBytes++
		}
		modulus := new(big.Int).Lsh(big.NewInt(1), uint(8*numBytes)) //nolint:gosec // numBytes is small and non-negative
		complement := new(big.Int).Add(modulus, n)
		out := complement.Bytes()
		if len(out) < numBytes {
			padded := make([]byte, numBytes)
			copy(padded[numBytes-len(out):], out)
			out = padded
		}
		return out
	}
}

// decodeUnscaledDecimal reverses encodeUnscaledDecimal. raw is assumed non-empty; the DECIMAL case
// in DecodeBound checks that before calling this.
func decodeUnscaledDecimal(raw []byte) *big.Int {
	n := new(big.Int).SetBytes(raw)
	if raw[0]&0x80 != 0 {
		modulus := new(big.Int).Lsh(big.NewInt(1), uint(8*len(raw))) //nolint:gosec // len(raw) is small and non-negative
		n.Sub(n, modulus)
	}
	return n
}
