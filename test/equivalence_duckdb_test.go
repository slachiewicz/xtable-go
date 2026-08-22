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

// This is the cross-engine equivalence harness: after polytable converts a fixture written by a
// real engine into another format, DuckDB -- and only DuckDB -- reads both the original table and
// the converted one and proves they hold the same data. Neither reader is polytable's own: DuckDB
// reads Delta through delta-kernel-rs and Iceberg through its own extension, so the two sides of
// every comparison are independent of the tool under test and of each other. Every other suite in
// this tree checks polytable's output with polytable's own reader, which cannot catch a deviation
// both sides share; engineverify_duckdb_test.go established that DuckDB catches real bugs a
// self-check cannot, and this harness applies the same idea across a conversion rather than to one
// side of it.
//
// A sibling prototype (Python, ad hoc) found a real divergence this way -- a deletion-vector table
// read as 900 rows through Delta and 1000 through Iceberg -- but had four weaknesses that this file
// deliberately does not reproduce. Each is called out at the function that fixes it:
//
//  1. checksumming every column as VARCHAR (see buildComparableProjection)
//  2. naive whole-schema tuple equality (see compareSchemas)
//  3. a row-level join keyed on the first column (see compareRows)
//  4. iceberg_scan refusing a table with no version-hint.text (see icebergScanExpr)
//
// Hudi is out of scope: DuckDB has no Hudi reader, so there is no independent second reader to
// compare against, and this is a permanent gap rather than an oversight.
package test_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// floatComparisonScale is the number of decimal places a DOUBLE/FLOAT/REAL column is rounded to
// before two sides are compared. The conversions exercised here never rewrite the underlying
// Parquet data pages -- they translate metadata around the same files -- so a float column read
// through two different engines is decoding the same IEEE-754 bytes and should agree exactly. The
// rounding is defensive rather than load-bearing: it absorbs a last-bit difference should one
// engine's decoder promote the value through a different arithmetic path, while nine decimal
// places is far tighter than any value a fixture actually writes, so it cannot paper over a real
// divergence.
const floatComparisonScale = 9

// equivalenceCase names one fixture, the format it already is, and the format polytable converts
// it to for this harness. Both formats must have a DuckDB reader.
type equivalenceCase struct {
	name    string
	fixture string
	source  model.TableFormat
	target  model.TableFormat
}

// TestEquivalence_DuckDBCrossEngine is the acceptance harness described at the top of this file.
// It runs every fixture in testdata/fixtures that DuckDB can read on both sides of a conversion:
// the delta-rs fixtures converted to Iceberg and read back through delta_scan/iceberg_scan, and the
// pyiceberg fixtures converted to Delta and read back the other way around. That set already
// covers a plain table, one with log-expired early commits (delta-rs-checkpoint), one with deletes
// (delta-rs-deletes and pyiceberg-deletes) and one compacted (delta-rs-compaction).
func TestEquivalence_DuckDBCrossEngine(t *testing.T) {
	if testing.Short() {
		t.Skip("cross-engine equivalence shells out to the duckdb CLI; skipped in short mode")
	}
	bin := duckdbBin(t)
	// Both extensions are needed by every case below (one per side of the comparison), so load
	// them once rather than once per subtest.
	requireExtension(t, bin, "delta")
	requireExtension(t, bin, "iceberg")

	cases := []equivalenceCase{
		{name: "delta_rs_to_iceberg", fixture: "delta-rs", source: model.TableFormatDelta, target: model.TableFormatIceberg},
		{name: "delta_rs_checkpoint_to_iceberg", fixture: "delta-rs-checkpoint", source: model.TableFormatDelta, target: model.TableFormatIceberg},
		// This case is a known, real failure, not a harness bug: delta-rs-compaction is the one
		// unpartitioned fixture in the tree, and polytable's Iceberg target leaves the Go slice
		// backing PartitionSpec.Fields nil rather than empty when there are no partition columns
		// (pkg/formats/iceberg/target.go, the `partitionFieldDefs` accumulator). encoding/json
		// marshals a nil slice as `"fields":null`, and DuckDB's iceberg extension refuses that
		// metadata.json outright ("PartitionSpec property 'fields' is not of type 'array', found
		// 'null' instead") rather than treating it as an empty spec. It is the same class of bug
		// as the historical `partitionColumns: null` defect this harness was built to catch, just
		// on the write side. Left failing deliberately: fixing pkg/ is another agent's task.
		{name: "delta_rs_compaction_to_iceberg", fixture: "delta-rs-compaction", source: model.TableFormatDelta, target: model.TableFormatIceberg},
		{name: "delta_rs_deletes_to_iceberg", fixture: "delta-rs-deletes", source: model.TableFormatDelta, target: model.TableFormatIceberg},
		{name: "pyiceberg_to_delta", fixture: "pyiceberg", source: model.TableFormatIceberg, target: model.TableFormatDelta},
		{name: "pyiceberg_deletes_to_delta", fixture: "pyiceberg-deletes", source: model.TableFormatIceberg, target: model.TableFormatDelta},
	}

	// No t.Parallel() here, matching engineverify_duckdb_test.go: each case shells out to a
	// duckdb process that installs and loads extensions into a shared per-user extension
	// directory.
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			tableDir, manifest := loadFixture(t, tc.fixture)

			results, err := conversion.NewController(io.NewLocalStorage()).Sync(ctx, &conversion.DatasetConfig{
				SourceFormat:  tc.source,
				TargetFormats: []model.TableFormat{tc.target},
				TableBasePath: tableDir,
				TableName:     manifest.TableName,
				SyncMode:      spi.SyncModeFull,
			})
			require.NoError(t, err)
			require.Equal(t, spi.SyncStatusSuccess, results[tc.target].StatusCode, results[tc.target].Error)

			sourceScan, err := duckdbScanExpr(tc.source, tableDir)
			require.NoError(t, err, "no duckdb reader for the %s source", tc.source)
			targetScan, err := duckdbScanExpr(tc.target, tableDir)
			require.NoError(t, err, "no duckdb reader for the %s target", tc.target)

			// A pair of scans that both, wrongly, come back empty would otherwise pass the
			// multiset comparison below trivially: two empty sets are equal. manifest.TotalRows is
			// ground truth from the fixture's own writer (delta-rs or pyiceberg), independent of
			// both duckdb and polytable, so anchoring the source side against it closes that hole --
			// if the source scan does not see every row the writer actually wrote, that surfaces
			// here instead of silently degrading the row comparison into nothing-vs-nothing.
			sourceCount, err := duckdbCount(t, bin, fmt.Sprintf("SELECT count(*) AS n FROM %s", sourceScan))
			require.NoError(t, err, "%s reader could not count the source table", tc.source)
			require.Equal(t, manifest.TotalRows, sourceCount, "%s row count vs the fixture manifest's ground truth", tc.source)

			assertDuckDBEquivalence(t, bin, string(tc.source), sourceScan, string(tc.target), targetScan)
		})
	}
}

// duckdbScanExpr returns the FROM-clause expression duckdb should use to read a table of the
// given format at dir.
func duckdbScanExpr(format model.TableFormat, dir string) (string, error) {
	switch format {
	case model.TableFormatDelta:
		return fmt.Sprintf("delta_scan('%s')", dir), nil
	case model.TableFormatIceberg:
		return icebergScanExpr(dir)
	default:
		return "", fmt.Errorf("no duckdb reader is wired for %s in this harness", format)
	}
}

// icebergScanExpr is weakness (4) of the prototype: a table with no metadata/version-hint.text --
// which is exactly what pyiceberg writes, since only polytable's own target and the Hadoop table
// layout convention produce that file -- makes iceberg_scan('<dir>') refuse the table outright.
// `SET unsafe_enable_version_guessing = true` clears the refusal but is a real hazard: it lists the
// metadata directory and guesses the latest file by name, which can pick up a metadata.json a
// concurrent or crashed writer left behind before it was ever committed to a catalog. This harness
// instead resolves the latest version itself and hands iceberg_scan that file directly, which is
// deterministic and is what a real catalog would have handed a reader anyway. It reuses
// iceberg.MetadataFileVersion, the same version-number parser polytable's own Iceberg source uses,
// so "latest" means the same thing here as it does to the code under test.
func icebergScanExpr(dir string) (string, error) {
	if _, err := os.Stat(filepath.Join(dir, "metadata", "version-hint.text")); err == nil {
		return fmt.Sprintf("iceberg_scan('%s')", dir), nil
	}

	metadataPath, err := latestIcebergMetadataPath(dir)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("iceberg_scan('%s')", metadataPath), nil
}

// latestIcebergMetadataPath finds the highest-versioned metadata.json under dir/metadata, in
// either polytable's own v<N>.metadata.json naming or the <NNNNN>-<uuid>.metadata.json convention
// every catalog-backed writer (pyiceberg, the Java library, Spark) uses.
func latestIcebergMetadataPath(dir string) (string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "metadata", "*.metadata.json"))
	if err != nil {
		return "", fmt.Errorf("listing %s/metadata: %w", dir, err)
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("no metadata.json files under %s/metadata", dir)
	}

	best, bestVersion := "", -1
	for _, m := range matches {
		v, ok := iceberg.MetadataFileVersion(filepath.Base(m))
		if !ok {
			continue
		}
		// A rewritten metadata file can keep its version with a new uuid; prefer the lexically
		// last name for a tie, matching polytable's own listMetadataFiles.
		if v > bestVersion || (v == bestVersion && m > best) {
			best, bestVersion = m, v
		}
	}
	if best == "" {
		return "", fmt.Errorf("no parseable metadata.json version under %s/metadata", dir)
	}
	return best, nil
}

// duckdbColumn is one column as DuckDB's DESCRIBE reports it.
type duckdbColumn struct {
	Type     string
	Nullable bool
}

// describeColumns runs DESCRIBE against a scan expression and returns its columns keyed by name.
func describeColumns(t *testing.T, bin, scan string) (map[string]duckdbColumn, error) {
	t.Helper()
	rows, err := duckdbQuery(t, bin, fmt.Sprintf("DESCRIBE SELECT * FROM %s", scan))
	if err != nil {
		return nil, err
	}
	cols := make(map[string]duckdbColumn, len(rows))
	for _, row := range rows {
		name, _ := row["column_name"].(string)
		typ, _ := row["column_type"].(string)
		nullable, _ := row["null"].(string)
		cols[name] = duckdbColumn{Type: typ, Nullable: strings.EqualFold(nullable, "YES")}
	}
	return cols, nil
}

// assertDuckDBEquivalence is the comparison itself. A read failure on either side is reported as
// the finding, not as a test-infrastructure error (see the "missing" case in
// TestEquivalence_DuckDBCrossEngine's package comment and the partitionColumns: null history this
// harness exists to catch): the delta and iceberg readers are two different codebases, and one of
// them refusing a table it should be able to read is exactly the kind of divergence this harness
// looks for, so it is surfaced with t.Errorf and the reader's own message rather than aborting the
// run with require.NoError.
func assertDuckDBEquivalence(t *testing.T, bin, sourceName, sourceScan, targetName, targetScan string) {
	t.Helper()

	sourceCols, err := describeColumns(t, bin, sourceScan)
	if err != nil {
		t.Errorf("%s reader could not describe the source table %s: %v", sourceName, sourceScan, err)
		return
	}
	targetCols, err := describeColumns(t, bin, targetScan)
	if err != nil {
		t.Errorf("%s reader could not describe the converted table %s: %v", targetName, targetScan, err)
		return
	}

	common := compareSchemas(t, sourceName, sourceCols, targetName, targetCols)
	if len(common) == 0 {
		t.Errorf("%s and %s share no columns; nothing to compare", sourceName, targetName)
		return
	}

	compareRows(t, bin, sourceName, sourceScan, targetName, targetScan, common, sourceCols)
}

// compareSchemas is weakness (2): the prototype compared the whole schema as one tuple, so any
// difference reported "DIFFERS" with no indication of which column or what about it differed. This
// walks the union of both sides' column names and reports, per column, whether it is missing from
// one side, or present on both with a different type or a different nullability -- the two things
// DESCRIBE can tell apart. It returns the column names present on both sides, which is what
// compareRows compares; a column absent from one side is already reported here, and comparing it
// as a row-level value again would either fail outright or report a redundant divergence.
func compareSchemas(t *testing.T, sourceName string, source map[string]duckdbColumn, targetName string, target map[string]duckdbColumn) []string {
	t.Helper()

	names := make(map[string]struct{}, len(source)+len(target))
	for name := range source {
		names[name] = struct{}{}
	}
	for name := range target {
		names[name] = struct{}{}
	}
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)

	common := make([]string, 0, len(sorted))
	for _, name := range sorted {
		s, sok := source[name]
		tg, tok := target[name]
		switch {
		case sok && !tok:
			t.Errorf("column %q: present in %s, missing from %s", name, sourceName, targetName)
		case !sok && tok:
			t.Errorf("column %q: present in %s, missing from %s", name, targetName, sourceName)
		default:
			common = append(common, name)
			if s.Type != tg.Type {
				t.Errorf("column %q: type %s in %s vs type %s in %s", name, s.Type, sourceName, tg.Type, targetName)
			}
			if s.Nullable != tg.Nullable {
				t.Errorf("column %q: nullable=%v in %s vs nullable=%v in %s", name, s.Nullable, sourceName, tg.Nullable, targetName)
			}
		}
	}
	return common
}

// compareRows is weakness (3): the prototype joined the two tables on their first column to find
// differing rows, which is only correct when that column happens to be a unique key -- none of the
// fixtures compared here have one (delta-rs-checkpoint's leading column is "id", but a Hive
// partition column often sorts first and is never unique). Rather than hunt for a key, this does a
// full order-independent multiset comparison that needs no key at all: GROUP BY ALL over every
// common column plus COUNT(*) turns each distinct row into its own group, and EXCEPT between the
// two grouped queries is then an exact multiset symmetric difference -- it catches a row missing
// from one side and also a row present with a different multiplicity on each side, which a plain
// DISTINCT-based EXCEPT would not. This is more robust than deriving a key would be: no fixture
// schema needs to be inspected to find one, and adding a fixture whose key assumption breaks can
// never silently produce a wrong diff again.
func compareRows(t *testing.T, bin, sourceName, sourceScan, targetName, targetScan string, columns []string, sourceTypes map[string]duckdbColumn) {
	t.Helper()

	projection := buildComparableProjection(columns, sourceTypes)

	multisetDiff := func(leftScan, rightScan string) ([]map[string]any, error) {
		sql := fmt.Sprintf(`SELECT * FROM (
  SELECT %[1]s, count(*) AS __polytable_equiv_count FROM %[2]s GROUP BY ALL
  EXCEPT
  SELECT %[1]s, count(*) AS __polytable_equiv_count FROM %[3]s GROUP BY ALL
) LIMIT 20`, projection, leftScan, rightScan)
		return duckdbQuery(t, bin, sql)
	}

	onlyInSource, err := multisetDiff(sourceScan, targetScan)
	if err != nil {
		t.Errorf("%s reader failed while comparing rows against %s: %v", sourceName, targetName, err)
		return
	}
	onlyInTarget, err := multisetDiff(targetScan, sourceScan)
	if err != nil {
		t.Errorf("%s reader failed while comparing rows against %s: %v", targetName, sourceName, err)
		return
	}

	if len(onlyInSource) == 0 && len(onlyInTarget) == 0 {
		return
	}
	t.Errorf(
		"%s and %s disagree on row content (order-independent multiset comparison; up to 20 rows shown per side)\nonly in %s:\n%s\nonly in %s:\n%s",
		sourceName, targetName,
		sourceName, formatRows(onlyInSource),
		targetName, formatRows(onlyInTarget),
	)
}

// buildComparableProjection is weakness (1): the prototype cast every column to VARCHAR before
// hashing, so a DOUBLE such as 10.5 printed as "10.5" by one engine and "10.500000" (or in
// scientific notation, depending on magnitude) by another reported a false divergence that had
// nothing to do with the data. Every column here keeps its native DuckDB type -- comparing a
// BIGINT, VARCHAR, BOOLEAN or DATE/TIMESTAMP column exactly is correct, because none of those types
// has a legitimate source of representational imprecision, and silently tolerating a difference
// there would hide a real bug such as a truncated timestamp. Only DOUBLE/FLOAT/REAL columns are
// rounded (see floatComparisonScale) before comparison, which is the one type family where two
// independent decoders of the same bytes could legitimately disagree in the last bit. DECIMAL is
// deliberately excluded from that rounding: it is fixed-point and exact, so comparing it exactly
// is correct, not merely convenient.
func buildComparableProjection(columns []string, types map[string]duckdbColumn) string {
	exprs := make([]string, len(columns))
	for i, name := range columns {
		quoted := quoteIdent(name)
		if isFloatType(types[name].Type) {
			exprs[i] = fmt.Sprintf("round(CAST(%s AS DOUBLE), %d) AS %s", quoted, floatComparisonScale, quoted)
		} else {
			exprs[i] = quoted
		}
	}
	return strings.Join(exprs, ", ")
}

// isFloatType reports whether a DuckDB type name is one of the inexact binary floating-point
// types. DECIMAL is intentionally not included: it is exact fixed-point arithmetic.
func isFloatType(duckdbType string) bool {
	upper := strings.ToUpper(duckdbType)
	return strings.Contains(upper, "DOUBLE") || strings.Contains(upper, "FLOAT") || strings.Contains(upper, "REAL")
}

// quoteIdent double-quotes a DuckDB identifier, escaping an embedded quote by doubling it.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// formatRows renders diverging rows for a failure message.
func formatRows(rows []map[string]any) string {
	if len(rows) == 0 {
		return "  (none)"
	}
	encoded, err := json.MarshalIndent(rows, "  ", "  ")
	if err != nil {
		return fmt.Sprintf("  %v", rows)
	}
	return "  " + string(encoded)
}
