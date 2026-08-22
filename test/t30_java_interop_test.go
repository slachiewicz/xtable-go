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

// T30 (docs/improvement-plan.md): the "Java -> polytable" half of the interop check.
//
// testdata/fixtures/java-xtable-delta-to-iceberg/sales is not synthetic: it is the exact table the
// Apache XTable 0.5.0-SNAPSHOT (commit 16778bb) bundled jar produced by running its RunSync utility
// against test/testdata/fixtures/delta-rs/sales, unmodified except for dropping the .crc sidecar
// files and relocating the absolute paths the jar baked into metadata.json and the Avro manifests.
// No JVM is needed to read it back -- only to have produced it once -- so this is the "committed
// fixture, not a nightly JVM lane" half of T30's deliverable. The reverse direction (Java reading a
// polytable-written table) needs a live jar and is recorded in docs/improvement-plan.md instead.
package test_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// javaXTableFixtureOldPath is the absolute path recorded throughout
// testdata/fixtures/java-xtable-delta-to-iceberg/sales's metadata.json and Avro manifests -- the
// scratch directory the real bundled jar wrote the table into when this fixture was generated.
const javaXTableFixtureOldPath = "/tmp/t30/java-dir/sales"

// loadJavaXTableFixture copies the Java-XTable-synced fixture into a temporary directory and
// relocates every absolute path baked into its metadata to point there instead.
//
// The Java jar's HadoopTableOperations writes "location" and "write.data.path" as
// "file:///abs/path" but a snapshot's "manifest-list" as "file:/abs/path" -- one slash short of the
// URI form polytable's own io.CleanPath understands. Both are handled by replacing the longer,
// three-slash form first so the shorter replacement cannot double up inside it, and rewriting both
// to the canonical "file://" form polytable writes itself.
func loadJavaXTableFixture(t *testing.T) string {
	t.Helper()

	source := filepath.Join(fixtureRoot, "java-xtable-delta-to-iceberg", "sales")
	dest := filepath.Join(t.TempDir(), "sales")
	require.NoError(t, os.CopyFS(dest, os.DirFS(source)))

	metadataJSONs, err := filepath.Glob(filepath.Join(dest, "metadata", "*.metadata.json"))
	require.NoError(t, err)
	require.NotEmpty(t, metadataJSONs, "fixture has no Iceberg metadata.json to relocate")

	oldTripleSlash, newTripleSlash := "file://"+javaXTableFixtureOldPath, "file://"+dest
	oldSingleSlash, newSingleSlash := "file:"+javaXTableFixtureOldPath, "file://"+dest
	for _, path := range metadataJSONs {
		data, err := os.ReadFile(path) //nolint:gosec // G304: glob of this test's own temp directory
		require.NoError(t, err)
		rewritten := strings.ReplaceAll(string(data), oldTripleSlash, newTripleSlash)
		rewritten = strings.ReplaceAll(rewritten, oldSingleSlash, newSingleSlash)
		//nolint:gosec // G306: table metadata must stay group/world readable, matching io.LocalStorage.Write
		require.NoError(t, os.WriteFile(path, []byte(rewritten), 0o644))
	}

	relocateAvroManifests(t, dest)
	return dest
}

// metadataDirDigest fingerprints every file under a table's metadata directory by name and content,
// so a no-op sync can be verified against observable on-disk state rather than the verdict alone.
func metadataDirDigest(t *testing.T, tableDir string) map[string][32]byte {
	t.Helper()

	entries, err := os.ReadDir(filepath.Join(tableDir, "metadata"))
	require.NoError(t, err)

	digest := make(map[string][32]byte, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(tableDir, "metadata", entry.Name())) //nolint:gosec // G304
		require.NoError(t, err)
		digest[entry.Name()] = sha256.Sum256(data)
	}
	return digest
}

// TestT30_PolytableRecognizesJavaSyncState_NoOp is T30's acceptance criterion for the direction that
// can be tested without a JVM: syncing a Delta source that a real Java XTable jar already synced to
// Iceberg must report NO_OP, not a full snapshot fallback. Before T60, polytable read only its own
// flat xtable_* properties and never recognized Java's XTABLE_METADATA, so this would have produced
// a fresh SUCCESS and a new metadata version every time.
func TestT30_PolytableRecognizesJavaSyncState_NoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tableDir := loadJavaXTableFixture(t)
	before := metadataDirDigest(t, tableDir)
	versionHintBefore, err := os.ReadFile(filepath.Join(tableDir, "metadata", "version-hint.text"))
	require.NoError(t, err)

	// Sanity check on the fixture itself: it must actually carry Java's XTABLE_METADATA property,
	// not polytable's own flat keys, or this test would pass for the wrong reason.
	latest, err := os.ReadFile(filepath.Join(tableDir, "metadata", "v3.metadata.json"))
	require.NoError(t, err)
	var meta struct {
		Properties map[string]string `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(latest, &meta))
	require.Contains(t, meta.Properties, model.KeyXTableMetadata, "fixture must carry Java's XTABLE_METADATA")

	// The NO_OP path below only reads the target's properties, so it alone would not catch a
	// relocation that silently broke the Avro manifests (a wrong path just fails a later real
	// read, not this sync). Reading the table back through polytable's own Iceberg source first
	// guards the relocation and doubles as a committed confirmation of T60's claim that polytable
	// reads the Java jar's Iceberg output -- Avro manifests included -- without a JVM.
	source, err := formats.NewSource(model.TableFormatIceberg, io.NewLocalStorage(), tableDir)
	require.NoError(t, err)
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)
	assert.Len(t, snapshot.DataFiles, 6, "the jar's sync added 6 Parquet files across region=east/west")
	require.NoError(t, source.Close())

	results, err := conversion.NewController(io.NewLocalStorage()).Sync(ctx, &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: tableDir,
		TableName:     "sales",
		SyncMode:      spi.SyncModeIncremental,
	})
	require.NoError(t, err)

	result := results[model.TableFormatIceberg]
	require.Equal(t, spi.SyncStatusSuccess, result.StatusCode, result.Error)
	assert.True(t, result.NoOp, "a Delta source with no commits since Java's own sync must be a no-op")
	assert.Equal(t, spi.SyncVerdictNoOp, result.Verdict())

	// The claim under test is observable state, not the verdict string: nothing under metadata/
	// may have changed -- no new v4.metadata.json, no touched version-hint.text, no rewritten
	// manifest -- because a NO_OP that still writes is indistinguishable from a very small SUCCESS.
	after := metadataDirDigest(t, tableDir)
	assert.Equal(t, before, after, "a NO_OP sync must not write anything under metadata/")

	versionHintAfter, err := os.ReadFile(filepath.Join(tableDir, "metadata", "version-hint.text"))
	require.NoError(t, err)
	assert.Equal(t, versionHintBefore, versionHintAfter)
}
