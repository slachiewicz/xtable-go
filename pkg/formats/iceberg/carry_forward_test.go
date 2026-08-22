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

// Tests for T68 (docs/improvement-plan.md): Target.CommitChanges must carry the previous
// snapshot's still-live files forward across an incremental commit rather than replacing the
// table's manifest with just that commit's FilesDiff.FilesAdded.
package iceberg_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// carryForwardTable builds a minimal single-field table descriptor rooted at basePath, reused
// across the tests below.
func carryForwardTable(basePath string) *model.Table {
	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	return &model.Table{
		Name:        "events",
		TableFormat: model.TableFormatIceberg,
		ReadSchema:  model.NewRecordSchema("events", []*model.Field{idField}, false),
		BasePath:    basePath,
	}
}

func liveFilesByPath(t *testing.T, ctx context.Context, storage io.Storage, basePath string) map[string]*model.DataFile {
	t.Helper()

	source := iceberg.NewSource(storage, basePath)
	snapshot, err := source.GetCurrentSnapshot(ctx)
	require.NoError(t, err)

	byPath := make(map[string]*model.DataFile, len(snapshot.DataFiles))
	for _, f := range snapshot.DataFiles {
		byPath[f.PhysicalPath] = f
	}
	return byPath
}

// TestIcebergTarget_CommitChanges_CarriesLiveFilesForward is T68's core regression test: three
// commits land through one CommitChanges call, the middle one adding no files at all (the shape a
// metadata-only schema change takes), and every file added by any of them must still be live
// afterwards -- not just the last commit's.
func TestIcebergTarget_CommitChanges_CarriesLiveFilesForward(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/carry_forward"
	table := carryForwardTable(basePath)

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))

	fileA := &model.DataFile{PhysicalPath: io.JoinPath(basePath, "data", "a.parquet"), FileFormat: model.FileFormatParquet, FileSizeBytes: 100, RecordCount: 1}
	fileC := &model.DataFile{PhysicalPath: io.JoinPath(basePath, "data", "c.parquet"), FileFormat: model.FileFormatParquet, FileSizeBytes: 300, RecordCount: 3}

	changes := &model.IncrementalTableChanges{
		TableChanges: []*model.TableChange{
			{
				FilesDiff:        model.NewFilesDiff([]*model.DataFile{fileA}, nil),
				TableAsOfChange:  table,
				SourceIdentifier: "c1",
			},
			{
				// A metadata-only commit: no files added or removed at all. This is exactly the
				// shape that made the bug invisible to append-only fixtures -- if CommitChanges
				// replaces the live set with this commit's FilesAdded alone, the table ends up
				// with zero files after this step.
				FilesDiff:        model.NewFilesDiff(nil, nil),
				TableAsOfChange:  table,
				SourceIdentifier: "c2",
			},
			{
				FilesDiff:        model.NewFilesDiff([]*model.DataFile{fileC}, nil),
				TableAsOfChange:  table,
				SourceIdentifier: "c3",
			},
		},
		CurrentTable: table,
	}
	require.NoError(t, target.CommitChanges(ctx, changes))

	byPath := liveFilesByPath(t, ctx, storage, basePath)
	require.Len(t, byPath, 2, "both commit 1's and commit 3's files must be live; commit 2 added none of its own")
	assert.Contains(t, byPath, fileA.PhysicalPath)
	assert.Contains(t, byPath, fileC.PhysicalPath)

	var total int64
	for _, f := range byPath {
		total += f.RecordCount
	}
	assert.Equal(t, int64(4), total, "commit 2's metadata-only change must not have dropped commit 1's rows")
}

// TestIcebergTarget_CommitChanges_RemovalRemoves is bar item 2: a file named in a later commit's
// FilesRemoved must not be live afterwards, even though an earlier commit in the same
// CommitChanges call added it.
func TestIcebergTarget_CommitChanges_RemovalRemoves(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/carry_forward_removal"
	table := carryForwardTable(basePath)

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))

	fileA := &model.DataFile{PhysicalPath: io.JoinPath(basePath, "data", "a.parquet"), FileFormat: model.FileFormatParquet, FileSizeBytes: 100, RecordCount: 1}
	fileB := &model.DataFile{PhysicalPath: io.JoinPath(basePath, "data", "b.parquet"), FileFormat: model.FileFormatParquet, FileSizeBytes: 200, RecordCount: 2}

	changes := &model.IncrementalTableChanges{
		TableChanges: []*model.TableChange{
			{
				FilesDiff:        model.NewFilesDiff([]*model.DataFile{fileA, fileB}, nil),
				TableAsOfChange:  table,
				SourceIdentifier: "c1",
			},
			{
				FilesDiff:        model.NewFilesDiff(nil, []*model.DataFile{fileA}),
				TableAsOfChange:  table,
				SourceIdentifier: "c2",
			},
		},
	}
	require.NoError(t, target.CommitChanges(ctx, changes))

	byPath := liveFilesByPath(t, ctx, storage, basePath)
	require.Len(t, byPath, 1)
	assert.NotContains(t, byPath, fileA.PhysicalPath, "a.parquet was removed in commit 2 and must not be live")
	assert.Contains(t, byPath, fileB.PhysicalPath)
}

// TestIcebergTarget_CommitChanges_RewriteInPlace is bar item 3: the same PhysicalPath appearing in
// both FilesAdded and FilesRemoved of the same change -- what model.DiffFiles reports for a file
// whose size or record count changed without moving -- must end up live exactly once, carrying the
// new metadata rather than being dropped (removal processed after addition) or duplicated
// (addition processed without removing the old entry).
func TestIcebergTarget_CommitChanges_RewriteInPlace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/carry_forward_rewrite"
	table := carryForwardTable(basePath)

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))

	path := io.JoinPath(basePath, "data", "a.parquet")
	original := &model.DataFile{PhysicalPath: path, FileFormat: model.FileFormatParquet, FileSizeBytes: 100, RecordCount: 1}
	rewritten := &model.DataFile{PhysicalPath: path, FileFormat: model.FileFormatParquet, FileSizeBytes: 999, RecordCount: 9}

	changes := &model.IncrementalTableChanges{
		TableChanges: []*model.TableChange{
			{
				FilesDiff:        model.NewFilesDiff([]*model.DataFile{original}, nil),
				TableAsOfChange:  table,
				SourceIdentifier: "c1",
			},
			{
				FilesDiff:        model.NewFilesDiff([]*model.DataFile{rewritten}, []*model.DataFile{original}),
				TableAsOfChange:  table,
				SourceIdentifier: "c2",
			},
		},
	}
	require.NoError(t, target.CommitChanges(ctx, changes))

	byPath := liveFilesByPath(t, ctx, storage, basePath)
	require.Len(t, byPath, 1, "the rewrite must leave exactly one live entry at the path")
	got := byPath[path]
	require.NotNil(t, got)
	assert.Equal(t, int64(999), got.FileSizeBytes, "the rewrite's new size must win")
	assert.Equal(t, int64(9), got.RecordCount, "the rewrite's new record count must win")
}

// TestIcebergTarget_CommitChanges_SingleCommitUnchanged is bar item 4: a fresh target's first
// incremental commit has no previous live set to carry forward, so this must reduce to the
// pre-fix behavior and not regress the existing single-commit fixtures.
func TestIcebergTarget_CommitChanges_SingleCommitUnchanged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/carry_forward_single"
	table := carryForwardTable(basePath)

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))

	fileA := &model.DataFile{PhysicalPath: io.JoinPath(basePath, "data", "a.parquet"), FileFormat: model.FileFormatParquet, FileSizeBytes: 100, RecordCount: 1}

	changes := &model.IncrementalTableChanges{
		TableChanges: []*model.TableChange{
			{
				FilesDiff:        model.NewFilesDiff([]*model.DataFile{fileA}, nil),
				TableAsOfChange:  table,
				SourceIdentifier: "c1",
			},
		},
	}
	require.NoError(t, target.CommitChanges(ctx, changes))

	byPath := liveFilesByPath(t, ctx, storage, basePath)
	require.Len(t, byPath, 1)
	assert.Contains(t, byPath, fileA.PhysicalPath)
}

// TestIcebergTarget_CommitSnapshot_UnreadablePreviousMetadataAborts is bar item 5 and covers T68's
// "also fix the swallowed error" requirement: a previous metadata file this reader cannot parse
// (simulated here the same way incremental_test.go's expired-parent tests do, by hand-editing the
// written JSON) must fail the next commit outright rather than being silently treated as "no table
// exists yet", which would restart version numbering at 1 and orphan the unreadable file.
func TestIcebergTarget_CommitSnapshot_UnreadablePreviousMetadataAborts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	storage := io.NewMemoryStorage()
	basePath := "mem://lake/carry_forward_unreadable"
	table := carryForwardTable(basePath)

	target := iceberg.NewTarget(storage)
	require.NoError(t, target.Init(ctx, table))

	fileA := &model.DataFile{PhysicalPath: io.JoinPath(basePath, "data", "a.parquet"), FileFormat: model.FileFormatParquet, FileSizeBytes: 100, RecordCount: 1}
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table: table, DataFiles: []*model.DataFile{fileA}, SourceIdentifier: "s1",
	}))

	path := latestMetadataPath(t, ctx, storage, basePath)
	meta := readTableMetadata(t, ctx, storage, path)
	meta.FormatVersion = 3
	writeTableMetadata(t, ctx, storage, path, meta)

	fileB := &model.DataFile{PhysicalPath: io.JoinPath(basePath, "data", "b.parquet"), FileFormat: model.FileFormatParquet, FileSizeBytes: 200, RecordCount: 2}
	err := target.CommitSnapshot(ctx, &model.Snapshot{
		Table: table, DataFiles: []*model.DataFile{fileA, fileB}, SourceIdentifier: "s2",
	})
	require.Error(t, err, "an unreadable previous metadata file must abort the commit, not restart at version 0")
	assert.Contains(t, err.Error(), "format-version")

	entries, err := storage.List(ctx, io.JoinPath(basePath, "metadata"))
	require.NoError(t, err)
	var versions []int
	for _, e := range entries {
		if v, ok := iceberg.MetadataFileVersion(filepath.Base(e.Path)); ok {
			versions = append(versions, v)
		}
	}
	assert.Equal(t, []int{1}, versions, "the aborted commit must not have written a new v1 metadata file alongside the tampered one")
}
