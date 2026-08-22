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

package delta

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// Source implements spi.ConversionSource for Delta Lake tables.
type Source struct {
	storage  io.Storage
	basePath string
}

var _ spi.ConversionSource = (*Source)(nil)

// NewSource creates a new Delta ConversionSource instance.
func NewSource(storage io.Storage, basePath string) *Source {
	return &Source{
		storage:  storage,
		basePath: basePath,
	}
}

// Format returns the format identifier.
func (s *Source) Format() model.TableFormat {
	return model.TableFormatDelta
}

// DeltaCommit represents parsed actions from a single Delta commit file.
type DeltaCommit struct {
	Version    int64
	Actions    []SingleAction
	CommitTime int64
}

// listCommitFiles lists and sorts all commit .json files in _delta_log.
func (s *Source) listCommitFiles(ctx context.Context) ([]int64, error) {
	logPath := io.JoinPath(s.basePath, "_delta_log")
	files, err := s.storage.List(ctx, logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to list delta log directory %s: %w", logPath, err)
	}

	var versions []int64
	for _, f := range files {
		base := filepath.Base(f.Path)
		if strings.HasSuffix(base, ".json") && !strings.HasPrefix(base, ".") {
			verStr := strings.TrimSuffix(base, ".json")
			if v, err := strconv.ParseInt(verStr, 10, 64); err == nil {
				versions = append(versions, v)
			}
		}
	}
	sort.Slice(versions, func(i, j int) bool {
		return versions[i] < versions[j]
	})
	return versions, nil
}

// logState pairs the surviving JSON commit versions with the checkpoint state that precedes them.
// versions holds only versions strictly after the checkpoint: everything at or before it is already
// reconciled into the checkpoint state, and replaying the checkpoint version's own JSON file (which
// log cleanup often leaves behind) would double-apply its actions.
type logState struct {
	checkpoint *checkpointState
	versions   []int64
}

// latestVersion returns the newest version the log can reconstruct, from the JSON tail or, for a
// fully cleaned log, the checkpoint itself. ok is false for an empty log.
func (st *logState) latestVersion() (version int64, ok bool) {
	if n := len(st.versions); n > 0 {
		return st.versions[n-1], true
	}
	if st.checkpoint != nil {
		return st.checkpoint.Version, true
	}
	return 0, false
}

// loadLogState lists the log and loads the checkpoint the reader must start from. A log whose
// earliest JSON commit is not version 0 and that has no checkpoint is truncated — its head state is
// unrecoverable — and fails here rather than letting a replay silently build a partial snapshot.
func (s *Source) loadLogState(ctx context.Context) (*logState, error) {
	versions, err := s.listCommitFiles(ctx)
	if err != nil {
		return nil, err
	}
	last, err := s.readLastCheckpoint(ctx)
	if err != nil {
		return nil, err
	}

	state := &logState{}
	if last == nil {
		if len(versions) > 0 && versions[0] > 0 {
			return nil, fmt.Errorf("delta log at %s is truncated: earliest commit is version %d and no checkpoint exists",
				s.basePath, versions[0])
		}
		state.versions = versions
		return state, nil
	}

	if state.checkpoint, err = s.readCheckpoint(ctx, last); err != nil {
		return nil, err
	}
	for _, v := range versions {
		if v > state.checkpoint.Version {
			state.versions = append(state.versions, v)
		}
	}
	return state, nil
}

// readCommit reads and parses actions from a specific commit version file.
func (s *Source) readCommit(ctx context.Context, version int64) (*DeltaCommit, error) {
	fileName := fmt.Sprintf("%020d.json", version)
	filePath := io.JoinPath(s.basePath, "_delta_log", fileName)

	data, err := s.storage.Read(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read delta commit %s: %w", filePath, err)
	}

	var actions []SingleAction
	var commitTime int64
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var action SingleAction
		if err := json.Unmarshal(line, &action); err != nil {
			return nil, fmt.Errorf("failed to parse action line in commit %d: %w", version, err)
		}
		if action.CommitInfo != nil {
			commitTime = action.CommitInfo.Timestamp
		}
		actions = append(actions, action)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return &DeltaCommit{
		Version:    version,
		Actions:    actions,
		CommitTime: commitTime,
	}, nil
}

// advanceCommitTime folds a raw commitInfo timestamp into a strictly increasing instant, given the
// instant already derived for the preceding version (0 before the first commit).
//
// The Delta protocol makes commitInfo optional and puts no monotonicity guarantee on its timestamp,
// so the raw value is unusable as a sync cursor: two commits inside the same millisecond share it, a
// skewed writer clock can move it backwards, and a commit without commitInfo reports 0. Every one of
// those made the "changes strictly after fromInstant" selection drop commits while reporting success.
// Deriving the instant from version order instead keeps it a faithful, injective proxy for the
// version, which is monotonic by construction. Java resolves the same ambiguity by mapping the sync
// instant to a version once and then keying the backlog on versions
// (DeltaConversionSource#getCommitsBacklog).
//
// Instants only differ from the raw timestamp where the log is anomalous; a well-behaved log passes
// through unchanged.
func advanceCommitTime(previous, raw int64) int64 {
	if raw > previous {
		return raw
	}
	return previous + 1
}

// GetCurrentTable returns the latest table state.
func (s *Source) GetCurrentTable(ctx context.Context) (*model.Table, error) {
	state, err := s.loadLogState(ctx)
	if err != nil {
		return nil, err
	}
	latestVer, ok := state.latestVersion()
	if !ok {
		return nil, fmt.Errorf("no delta log commits found in %s", s.basePath)
	}
	return s.tableAt(ctx, state, latestVer)
}

// GetTable returns table metadata reconstructed up to the specified commit version.
func (s *Source) GetTable(ctx context.Context, commitID string) (*model.Table, error) {
	targetVer, err := strconv.ParseInt(commitID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid delta commit version %s: %w", commitID, err)
	}
	state, err := s.loadLogState(ctx)
	if err != nil {
		return nil, err
	}
	return s.tableAt(ctx, state, targetVer)
}

// tableAt reconstructs the table as of targetVer: the checkpoint's metaData first, then any newer
// metaData action in the JSON tail up to targetVer. The commit-time chain starts at the first
// surviving JSON commit — a checkpoint carries no timestamps.
func (s *Source) tableAt(ctx context.Context, state *logState, targetVer int64) (*model.Table, error) {
	var latestMeta *MetadataAction
	var latestCommitTime int64

	if cp := state.checkpoint; cp != nil {
		if targetVer < cp.Version {
			return nil, fmt.Errorf("delta version %d predates the checkpoint at version %d and its history has been cleaned up",
				targetVer, cp.Version)
		}
		latestMeta = cp.Meta
	}

	for _, v := range state.versions {
		if v > targetVer {
			break
		}
		commit, err := s.readCommit(ctx, v)
		if err != nil {
			return nil, err
		}
		latestCommitTime = advanceCommitTime(latestCommitTime, commit.CommitTime)
		for _, a := range commit.Actions {
			if a.MetaData != nil {
				latestMeta = a.MetaData
			}
		}
	}

	if latestMeta == nil {
		return nil, fmt.Errorf("no metadata action found in delta log up to version %d", targetVer)
	}

	return s.tableFromMetadata(latestMeta, latestCommitTime)
}

// tableFromMetadata builds the table a metaData action describes. It is the expensive half of
// GetTable — it parses the schema — so a backlog walk calls it only when a commit carries a new
// metaData action rather than once per commit.
func (s *Source) tableFromMetadata(meta *MetadataAction, commitTime int64) (*model.Table, error) {
	readSchema, err := DeltaJSONToSchema(meta.SchemaString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse delta schema: %w", err)
	}

	var partitionFields []*model.PartitionField
	for _, col := range meta.PartitionColumns {
		field := readSchema.FieldByPath(col)
		if field == nil {
			field = &model.Field{Name: col, Schema: model.NewPrimitiveSchema(model.TypeString, true)}
		}
		partitionFields = append(partitionFields, &model.PartitionField{
			SourceField:   field,
			TransformType: model.PartitionTransformValue,
		})
	}

	return &model.Table{
		Name:               meta.Name,
		TableFormat:        model.TableFormatDelta,
		ReadSchema:         readSchema,
		BasePath:           s.basePath,
		PartitioningFields: partitionFields,
		LatestCommitTime:   commitTime,
	}, nil
}

// tableAsOf returns table with its instant moved to commitTime. The copy is shallow on purpose:
// consecutive commits with no metaData action between them share one schema, which is read-only
// here, and rebuilding it per commit is exactly the cost this avoids.
func tableAsOf(table *model.Table, commitTime int64) *model.Table {
	asOf := *table
	asOf.LatestCommitTime = commitTime
	return &asOf
}

// GetCurrentSnapshot constructs the full active data file snapshot at the latest commit.
func (s *Source) GetCurrentSnapshot(ctx context.Context) (*model.Snapshot, error) {
	state, err := s.loadLogState(ctx)
	if err != nil {
		return nil, err
	}
	latestVer, ok := state.latestVersion()
	if !ok {
		return nil, fmt.Errorf("no delta commits found in %s", s.basePath)
	}

	table, err := s.tableAt(ctx, state, latestVer)
	if err != nil {
		return nil, err
	}

	activeFiles := make(map[string]*model.DataFile)

	if cp := state.checkpoint; cp != nil {
		for _, add := range cp.Adds {
			dataFile, err := s.convertAddAction(add, table)
			if err != nil {
				return nil, err
			}
			activeFiles[add.Path] = dataFile
		}
	}

	for _, v := range state.versions {
		commit, err := s.readCommit(ctx, v)
		if err != nil {
			return nil, err
		}
		for _, a := range commit.Actions {
			if a.Add != nil {
				dataFile, err := s.convertAddAction(a.Add, table)
				if err != nil {
					return nil, err
				}
				activeFiles[a.Add.Path] = dataFile
			} else if a.Remove != nil {
				delete(activeFiles, a.Remove.Path)
			}
		}
	}

	dataFilesList := make([]*model.DataFile, 0, len(activeFiles))
	for _, df := range activeFiles {
		dataFilesList = append(dataFilesList, df)
	}

	return &model.Snapshot{
		Table:            table,
		DataFiles:        dataFilesList,
		SourceIdentifier: strconv.FormatInt(latestVer, 10),
	}, nil
}

// GetTableChangeForCommit returns the diff of files added and removed in a single version commit.
func (s *Source) GetTableChangeForCommit(ctx context.Context, commitID string) (*model.TableChange, error) {
	v, err := strconv.ParseInt(commitID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid commit ID %s: %w", commitID, err)
	}

	commit, err := s.readCommit(ctx, v)
	if err != nil {
		return nil, err
	}

	table, err := s.GetTable(ctx, commitID)
	if err != nil {
		return nil, err
	}

	return s.changeFromCommit(commit, table)
}

// changeFromCommit converts an already-parsed commit into a table change against the table as of
// that commit, whose LatestCommitTime carries the commit's instant.
func (s *Source) changeFromCommit(commit *DeltaCommit, table *model.Table) (*model.TableChange, error) {
	var added []*model.DataFile
	var removed []*model.DataFile

	for _, a := range commit.Actions {
		if a.Add != nil {
			dataFile, err := s.convertAddAction(a.Add, table)
			if err != nil {
				return nil, err
			}
			added = append(added, dataFile)
		}
		if a.Remove != nil {
			removed = append(removed, &model.DataFile{
				PhysicalPath: s.resolveDataPath(a.Remove.Path),
				FileFormat:   model.FileFormatParquet,
			})
		}
	}

	return &model.TableChange{
		FilesDiff:        model.NewFilesDiff(added, removed),
		TableAsOfChange:  table,
		SourceIdentifier: strconv.FormatInt(commit.Version, 10),
		// The instant reported here is the derived one, not commitInfo's raw timestamp: the
		// controller persists it and hands it back as the next sync's fromInstant.
		CommitTime: table.LatestCommitTime,
	}, nil
}

// GetChangesSince returns all sequential table changes since fromInstant.
//
// The whole backlog is served by one walk of the log: each commit file is read exactly once, and the
// table is rebuilt only where a commit carries a metaData action, with the schema carried forward
// across the commits in between. Reading a commit to test its instant and then reading it again to
// convert it — with a full log-prefix walk per commit to rebuild the table — made the cost quadratic
// in the backlog length. Upstream #861 made the same change in Java.
func (s *Source) GetChangesSince(ctx context.Context, fromInstant int64) (*model.IncrementalTableChanges, error) {
	state, err := s.loadLogState(ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := state.latestVersion(); !ok {
		return nil, fmt.Errorf("no delta log commits found in %s", s.basePath)
	}

	var (
		changes    []*model.TableChange
		table      *model.Table
		commitTime int64
	)
	// Commits reconciled into a checkpoint cannot be replayed as changes; the checkpoint's metaData
	// seeds the schema so the tail parses even when no surviving commit carries one.
	if cp := state.checkpoint; cp != nil {
		if table, err = s.tableFromMetadata(cp.Meta, 0); err != nil {
			return nil, err
		}
	}
	for _, v := range state.versions {
		commit, err := s.readCommit(ctx, v)
		if err != nil {
			return nil, err
		}
		// Derived instants increase strictly with the version, so "instant greater than
		// fromInstant" selects exactly the versions after the one that instant identifies.
		commitTime = advanceCommitTime(commitTime, commit.CommitTime)

		for _, a := range commit.Actions {
			if a.MetaData != nil {
				if table, err = s.tableFromMetadata(a.MetaData, commitTime); err != nil {
					return nil, err
				}
			}
		}

		if commitTime <= fromInstant {
			continue
		}
		if table == nil {
			return nil, fmt.Errorf("no metadata action found in delta log up to version %d", v)
		}
		change, err := s.changeFromCommit(commit, tableAsOf(table, commitTime))
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}

	if table == nil {
		latest, _ := state.latestVersion()
		return nil, fmt.Errorf("no metadata action found in delta log up to version %d", latest)
	}

	return &model.IncrementalTableChanges{
		TableChanges: changes,
		CurrentTable: tableAsOf(table, commitTime),
	}, nil
}

// IsIncrementalSyncSafeFrom checks if log retention covers the requested instant.
func (s *Source) IsIncrementalSyncSafeFrom(ctx context.Context, earliestInstant int64) (bool, error) {
	versions, err := s.listCommitFiles(ctx)
	if err != nil {
		return false, err
	}
	if len(versions) == 0 {
		return false, nil
	}
	firstCommit, err := s.readCommit(ctx, versions[0])
	if err != nil {
		return false, err
	}
	// Retention is the one comparison that stays on the raw timestamp, because it asks when the
	// earliest retained commit happened rather than which version an instant identifies. Without a
	// commitInfo timestamp there is nothing to compare, so fall back to a snapshot sync rather than
	// read "0" as "old enough".
	if firstCommit.CommitTime <= 0 {
		return false, nil
	}
	return firstCommit.CommitTime <= earliestInstant, nil
}

// Close is a no-op for delta source.
func (s *Source) Close() error {
	return nil
}

// convertAddAction turns a Delta add action into a model.DataFile, including its statistics.
//
// A stats blob that fails to parse is reported rather than swallowed: RecordCount and every column's
// bounds all come from the same JSON blob, and a caller acting on a silently-zeroed RecordCount for a
// perfectly good data file is worse than a sync that fails loudly and names the file. The nested-struct
// shape a real writer such as delta-rs emits (see statsBlob) is not an error case at all — it is decoded
// and flattened — so this path is reached only by a genuinely malformed stats string.
func (s *Source) convertAddAction(add *AddAction, table *model.Table) (*model.DataFile, error) {
	dataFile := &model.DataFile{
		PhysicalPath:  s.resolveDataPath(add.Path),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: add.Size,
		LastModified:  add.ModificationTime,
	}

	// Parse Partition Values. val is *string: a nil entry is a genuine JSON null partition value
	// (the __HIVE_DEFAULT_PARTITION__ case) and must become a nil Range.MinValue, not the string
	// "<nil>" or the empty string — those two are handled explicitly rather than passing the
	// pointer straight to NewScalarRange, which would wrap it in a non-nil `any` holding a nil
	// *string and defeat both the `== nil` check and the `.(string)` type assertion downstream.
	if len(add.PartitionValues) > 0 && table != nil {
		for _, pf := range table.PartitioningFields {
			if val, ok := add.PartitionValues[pf.SourceField.Name]; ok {
				var value any
				if val != nil {
					value = *val
				}
				dataFile.PartitionValues = append(dataFile.PartitionValues, &model.PartitionValue{
					PartitionField: pf,
					Range:          model.NewScalarRange(value),
				})
			}
		}
	}

	// Parse Stats JSON
	if add.Stats != "" {
		var stats statsBlob
		if err := json.Unmarshal([]byte(add.Stats), &stats); err != nil {
			return nil, fmt.Errorf("delta: parsing stats for %s: %w", add.Path, err)
		}
		dataFile.RecordCount = stats.NumRecords

		if table != nil && table.ReadSchema != nil {
			minValues := make(map[string]any)
			maxValues := make(map[string]any)
			nullCounts := make(map[string]any)
			flattenStatsMap(stats.MinValues, "", minValues)
			flattenStatsMap(stats.MaxValues, "", maxValues)
			flattenStatsMap(stats.NullCount, "", nullCounts)

			colStats, err := columnStatsFromFlatMaps(table.ReadSchema, "", minValues, maxValues, nullCounts)
			if err != nil {
				return nil, fmt.Errorf("delta: stats for %s: %w", add.Path, err)
			}
			dataFile.ColumnStats = colStats
		}
	}

	// Parse Deletion Vector
	if add.DeletionVector != nil {
		dataFile.DeletionVector = &model.DeletionVector{
			StoragePath: add.DeletionVector.PathOrInlineDv,
			SizeInBytes: add.DeletionVector.SizeInBytes,
			Cardinality: add.DeletionVector.Cardinality,
		}
		if add.DeletionVector.Offset != nil {
			dataFile.DeletionVector.Offset = *add.DeletionVector.Offset
		}
	}

	return dataFile, nil
}

// columnStatsFromFlatMaps walks schema's fields recursively, matching each one (by the same
// dot-delimited path convention flattenStatsMap builds, "parent.child") against the flattened
// minValues/maxValues/nullCounts maps, and returns one model.ColumnStat per field that has at
// least one of the three.
//
// The path is built here, from the schema itself, rather than read off model.Field.Path(): no
// schema builder in this package (or, per a repo-wide check, in any other format package) ever
// populates Field.ParentPath, so Path() returns just the leaf name for a nested field everywhere in
// this codebase today. Recursing structurally like this — the same technique
// model.Schema.FieldByPath uses to walk a dotted path down into a schema, run in the opposite
// direction — sidesteps that gap rather than depending on it being fixed first.
func columnStatsFromFlatMaps(schema *model.Schema, prefix string, minValues, maxValues, nullCounts map[string]any) ([]*model.ColumnStat, error) {
	if schema == nil {
		return nil, nil
	}

	var stats []*model.ColumnStat
	for _, f := range schema.Fields {
		path := f.Name
		if prefix != "" {
			path = prefix + "." + f.Name
		}

		minVal, hasMin := minValues[path]
		maxVal, hasMax := maxValues[path]

		var numNulls int64
		hasNulls := false
		if raw, ok := nullCounts[path]; ok {
			n, valid := int64FromStatsValue(raw)
			if !valid {
				return nil, fmt.Errorf("nullCount at %q is not a number (%T)", path, raw)
			}
			numNulls, hasNulls = n, true
		}

		if hasMin || hasMax || hasNulls {
			colStat := &model.ColumnStat{Field: f}
			if hasMin || hasMax {
				colStat.Range = model.NewRange(minVal, maxVal)
			}
			if hasNulls {
				colStat.NumNulls = numNulls
			}
			stats = append(stats, colStat)
		}

		if f.Schema != nil && f.Schema.DataType == model.TypeRecord {
			nested, err := columnStatsFromFlatMaps(f.Schema, path, minValues, maxValues, nullCounts)
			if err != nil {
				return nil, err
			}
			stats = append(stats, nested...)
		}
	}
	return stats, nil
}

func (s *Source) resolveDataPath(relPath string) string {
	// Ask the canonical parser rather than testing a hand-written subset of schemes. The list here
	// was missing s3a://, abfss://, abfs://, wasbs:// and wasb:// -- five of the nine polytable
	// recognises -- so an absolute add.path carrying any of them was mistaken for a relative one
	// and joined onto the table root, producing a path like
	// "s3://bucket/tbl/s3a://bucket/tbl/data/f.parquet". Silent, and no reader can resolve it.
	//
	// Reachable through the most common writer there is: Hadoop and Spark write s3a://, and the
	// Delta protocol permits an absolute add.path for externally-referenced files and shallow
	// clones. The same defect exists in Apache XTable's DeltaActionsConverter.getFullPathToFile,
	// which concatenates on a startsWith test; the class was pointed out by the session working on
	// it, and checking polytable found this.
	if scheme, _ := io.TrimScheme(relPath); scheme != "" || strings.HasPrefix(relPath, "/") {
		return relPath
	}
	return io.JoinPath(s.basePath, relPath)
}
