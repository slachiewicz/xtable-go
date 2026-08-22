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
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// icebergFormatVersion is the table format version this target writes. Version 2 is what the
// manifest and manifest-list schemas in manifest.go describe.
const icebergFormatVersion = 2

// maxReadableFormatVersion is the highest Iceberg format-version this adapter's source can read.
// It is a distinct constant from icebergFormatVersion — "the version we write" and "the highest
// version we can read" are different guarantees, even though today they happen to be the same
// number. Version 3 adds row lineage (next-row-id, per-row _row_id), deletion vectors carried as
// Puffin blobs, and new primitive types that this reader does not implement; see docs/improvement-plan.md
// T65 (and T24's still-open INV-1 question, which the same Puffin-blob handling depends on).
// Source.readMetadata refuses anything above this rather than silently misreading it as v2.
const maxReadableFormatVersion = 2

// Target implements spi.ConversionTarget for Apache Iceberg tables.
type Target struct {
	storage     io.Storage
	targetTable *model.Table
	source      *Source
}

var _ spi.ConversionTarget = (*Target)(nil)

// NewTarget creates a new Iceberg ConversionTarget instance.
func NewTarget(storage io.Storage) *Target {
	return &Target{
		storage: storage,
	}
}

// Format returns the format identifier.
func (t *Target) Format() model.TableFormat {
	return model.TableFormatIceberg
}

// Init initializes the target with table configuration.
func (t *Target) Init(_ context.Context, targetTable *model.Table) error {
	t.targetTable = targetTable
	t.source = NewSource(t.storage, targetTable.BasePath)
	return nil
}

// GetTableMetadata retrieves previously recorded TableSyncMetadata from Iceberg table properties.
func (t *Target) GetTableMetadata(ctx context.Context) (*model.TableSyncMetadata, error) {
	if t.source == nil {
		return nil, nil
	}
	versions, paths, err := t.source.listMetadataFiles(ctx)
	if err != nil || len(versions) == 0 {
		return nil, nil
	}

	latestVer := versions[len(versions)-1]
	meta, err := t.source.readMetadata(ctx, paths[latestVer])
	if err != nil {
		return nil, err
	}

	if meta.Properties == nil {
		return nil, nil
	}

	syncMeta := model.ReadSyncMetadataFromProperties(meta.Properties)
	if syncMeta == nil {
		return nil, nil
	}
	syncMeta.TargetFormat = model.TableFormatIceberg
	syncMeta.CustomProperties = meta.Properties
	return syncMeta, nil
}

// CommitSnapshot writes a full snapshot into Apache Iceberg metadata and manifests.
func (t *Target) CommitSnapshot(ctx context.Context, snapshot *model.Snapshot) error {
	versions, paths, err := t.source.listMetadataFiles(ctx)
	if err != nil {
		// A listing failure is not "no table exists" -- treating it that way silently restarts
		// version numbering at 1 and clobbers version-hint.text on the next write, discarding the
		// table's real history. Aborting is the only non-destructive option: a table that truly has
		// no metadata yet still commits fine, because listMetadataFiles returns (nil, nil, nil) for
		// that case rather than an error.
		return fmt.Errorf("failed to list existing iceberg metadata before committing a snapshot: %w", err)
	}
	nextVersion := 1
	var prevMeta *TableMetadata
	if len(versions) > 0 {
		latestVer := versions[len(versions)-1]
		nextVersion = latestVer + 1
		prevMeta, err = t.source.readMetadata(ctx, paths[latestVer])
		if err != nil {
			// Same reasoning as above: a metadata file this reader cannot parse (a future format
			// version rejected by readMetadata's maxReadableFormatVersion gate, or corruption) must
			// not be treated as an absent table. Silently starting over at version 1 would write a
			// new v1.metadata.json next to the v3 file the table's catalog and other readers still
			// consider current, and version-hint.text would point readers at the wrong history.
			return fmt.Errorf("failed to read existing iceberg metadata before committing a snapshot: %w", err)
		}
	}

	now := time.Now().UnixMilli()
	snapshotID := now
	if prevMeta != nil {
		// Snapshot IDs are derived from the wall clock, and CommitChanges can commit several
		// snapshots back-to-back for a single incoming batch of source commits (T40 walks source
		// history into one TableChange per commit). Two commits inside the same millisecond would
		// otherwise collide: CurrentSnapshotID would point at an id shared by two entries in
		// Snapshots, and a reader resolving it takes whichever appears first, silently reading the
		// wrong -- typically older -- one. Bumping past the highest id already on record keeps ids
		// strictly increasing regardless of clock resolution.
		for _, s := range prevMeta.Snapshots {
			if s.SnapshotID >= snapshotID {
				snapshotID = s.SnapshotID + 1
			}
		}
	}
	schemaID := 0
	if prevMeta != nil {
		schemaID = prevMeta.CurrentSchemaID + 1
	}

	// 1. Convert Schema
	tableSchema, lastColID, err := SchemaToIceberg(snapshot.Table.ReadSchema, schemaID)
	if err != nil {
		return fmt.Errorf("failed to convert schema to iceberg: %w", err)
	}

	// 2. Convert Partition Spec
	// Initialised rather than declared nil: PartitionSpec.Fields carries no omitempty, and
	// encoding/json renders a nil slice as null. The Iceberg specification requires an array --
	// empty for an unpartitioned table -- and DuckDB's reader refuses the metadata outright with
	// "PartitionSpec property 'fields' is not of type 'array', found 'null' instead". Every
	// partitioned fixture appends at least once and so hides this, which is exactly how the same
	// defect survived in the Delta target's partitionColumns until a foreign reader found it.
	partitionFieldDefs := make([]*PartitionFieldDef, 0, len(snapshot.Table.PartitioningFields))
	for idx, pf := range snapshot.Table.PartitioningFields {
		if pf.TransformType != model.PartitionTransformValue {
			return fmt.Errorf("iceberg target cannot write the %s partition transform on %s: only "+
				"the identity transform is implemented", pf.TransformType, pf.SourceField.Name)
		}
		sourceID := 0
		for _, f := range tableSchema.Fields {
			if f.Name == pf.SourceField.Name {
				sourceID = f.ID
				break
			}
		}
		if sourceID == 0 {
			// Iceberg resolves every partition field against a column of the schema, so a source
			// that reports a partition column its schema does not carry — the Hive-style layouts,
			// where the column lives in the directory name and not in the data files — needs the
			// column added here. Nothing is invented by doing so: an identity-partitioned column
			// need not be stored in the data files at all, because a reader materializes it from
			// the partition tuple in the manifest.
			sourceID = lastColID + 1
			icebergType, nextID, err := convertTypeToIceberg(pf.SourceField.Schema, sourceID+1)
			if err != nil {
				return fmt.Errorf("failed to type the partition column %s: %w", pf.SourceField.Name, err)
			}
			if _, ok := icebergType.(string); !ok {
				return fmt.Errorf("partition column %s has a nested type, which cannot be a "+
					"partition source", pf.SourceField.Name)
			}
			lastColID = nextID - 1
			tableSchema.Fields = append(tableSchema.Fields, &NestedField{
				ID:       sourceID,
				Name:     pf.SourceField.Name,
				Type:     icebergType,
				Required: false,
			})
		}
		partitionFieldDefs = append(partitionFieldDefs, &PartitionFieldDef{
			SourceID:  sourceID,
			FieldID:   1000 + idx,
			Name:      pf.SourceField.Name,
			Transform: "identity",
		})
	}
	partitionSpec := &PartitionSpec{
		SpecID: 0,
		Fields: partitionFieldDefs,
	}

	// 3. Build the manifest entries
	var manifestEntries []ManifestEntry
	var addedRows int64
	for _, df := range snapshot.AllDataFiles() {
		partitionVals := make(map[string]any)
		for _, pv := range df.PartitionValues {
			if pv.PartitionField != nil && pv.PartitionField.SourceField != nil && pv.Range != nil {
				partitionVals[pv.PartitionField.SourceField.Name] = pv.Range.MinValue
			}
		}
		fileFormat, err := icebergFileFormat(df.FileFormat)
		if err != nil {
			return fmt.Errorf("cannot write %s into an iceberg manifest: %w", df.PhysicalPath, err)
		}
		manifestDataFile := &ManifestDataFile{
			Content:         contentData,
			FilePath:        df.PhysicalPath,
			FileFormat:      fileFormat,
			Partition:       partitionVals,
			RecordCount:     df.RecordCount,
			FileSizeInBytes: df.FileSizeBytes,
		}
		columnStatsToManifest(manifestDataFile, df.ColumnStats, tableSchema)

		addedRows += df.RecordCount
		manifestEntries = append(manifestEntries, ManifestEntry{
			Status:     manifestStatusAdded,
			SnapshotID: snapshotID,
			DataFile:   manifestDataFile,
		})
	}

	seqNumber := int64(1)
	if prevMeta != nil {
		seqNumber = prevMeta.LastSequenceNumber + 1
	}
	var parentSnapID *int64
	if prevMeta != nil && prevMeta.CurrentSnapshotID != nil {
		parentSnapID = prevMeta.CurrentSnapshotID
	}

	// 4. Write the manifest, then the manifest list that points at it. Both are Avro object
	// container files: the specification mandates that, and it is the only form an Iceberg engine
	// can open.
	manifestFileName := fmt.Sprintf("%s-m0.avro", uuid.New().String())
	manifestPath := io.JoinPath(t.targetTable.BasePath, "metadata", manifestFileName)
	manifestBytes, err := writeManifest(manifestEntries, tableSchema, partitionSpec, icebergFormatVersion)
	if err != nil {
		return fmt.Errorf("failed to encode manifest file %s: %w", manifestPath, err)
	}
	if err := t.storage.Write(ctx, manifestPath, manifestBytes); err != nil {
		return fmt.Errorf("failed to write manifest file %s: %w", manifestPath, err)
	}

	manifestList := []ManifestListEntry{{
		ManifestPath:      manifestPath,
		ManifestLength:    int64(len(manifestBytes)),
		PartitionSpecID:   partitionSpec.SpecID,
		Content:           contentData,
		SequenceNumber:    seqNumber,
		MinSequenceNumber: seqNumber,
		AddedSnapshotID:   snapshotID,
		AddedFilesCount:   len(manifestEntries),
		AddedRowsCount:    addedRows,
	}}
	// The attempt number between the snapshot id and the uuid is part of the conventional name;
	// polytable never retries a commit, so it is always zero.
	manifestListFileName := fmt.Sprintf("snap-%d-0-%s.avro", snapshotID, uuid.New().String())
	manifestListPath := io.JoinPath(t.targetTable.BasePath, "metadata", manifestListFileName)
	manifestListBytes, err := writeManifestList(manifestList, snapshotID, parentSnapID, seqNumber, icebergFormatVersion)
	if err != nil {
		return fmt.Errorf("failed to encode manifest list %s: %w", manifestListPath, err)
	}
	if err := t.storage.Write(ctx, manifestListPath, manifestListBytes); err != nil {
		return fmt.Errorf("failed to write manifest list %s: %w", manifestListPath, err)
	}

	// 5. Build Properties
	props := make(map[string]string)
	if prevMeta != nil && prevMeta.Properties != nil {
		for k, v := range prevMeta.Properties {
			props[k] = v
		}
	}
	model.WriteSyncMetadataProperties(props, &model.TableSyncMetadata{
		LastInstantSynced: snapshot.Table.LatestCommitTime,
		SourceFormat:      snapshot.Table.TableFormat,
		SourceIdentifier:  snapshot.SourceIdentifier,
	})

	// The name mapping is written on every commit rather than once, because the schema it mirrors
	// is rewritten on every commit too.
	nameMapping, err := NameMappingJSON(tableSchema)
	if err != nil {
		return err
	}
	props[NameMappingProperty] = nameMapping

	// 6. Build Snapshot
	tableSnapshot := &TableSnapshot{
		SnapshotID:       snapshotID,
		ParentSnapshotID: parentSnapID,
		SequenceNumber:   seqNumber,
		TimestampMs:      now,
		ManifestList:     manifestListPath,
		Summary: map[string]string{
			"operation":        "replace",
			"added-data-files": strconv.Itoa(len(manifestEntries)),
			"total-data-files": strconv.Itoa(len(manifestEntries)),
		},
		SchemaID: &schemaID,
	}

	var snapshots []*TableSnapshot
	if prevMeta != nil {
		snapshots = append(snapshots, prevMeta.Snapshots...)
	}
	snapshots = append(snapshots, tableSnapshot)

	tableUUID := uuid.New().String()
	if prevMeta != nil && prevMeta.TableUUID != "" {
		tableUUID = prevMeta.TableUUID
	}

	metadata := &TableMetadata{
		FormatVersion:      icebergFormatVersion,
		TableUUID:          tableUUID,
		Location:           t.targetTable.BasePath,
		LastSequenceNumber: seqNumber,
		LastUpdatedMs:      now,
		LastColumnID:       lastColID,
		CurrentSchemaID:    schemaID,
		Schemas:            []*TableSchema{tableSchema},
		DefaultSpecID:      0,
		PartitionSpecs:     []*PartitionSpec{partitionSpec},
		LastPartitionID:    1000 + len(partitionFieldDefs),
		// This target never sorts a data file it writes, so the only sort order it ever emits is
		// the specification's reserved unsorted order (id 0). Fields is initialised rather than
		// left nil for the same reason PartitionSpec.Fields is above: the struct tag carries no
		// omitempty, and an empty-but-present array is what the specification requires, not null.
		SortOrders:         []*SortOrder{{OrderID: 0, Fields: []*SortField{}}},
		DefaultSortOrderID: 0,
		Properties:         props,
		CurrentSnapshotID:  &snapshotID,
		Snapshots:          snapshots,
	}

	// 7. Write v{N}.metadata.json
	metaFileName := fmt.Sprintf("v%d.metadata.json", nextVersion)
	metaFilePath := io.JoinPath(t.targetTable.BasePath, "metadata", metaFileName)
	metaBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata JSON: %w", err)
	}
	if err := t.storage.Write(ctx, metaFilePath, metaBytes); err != nil {
		return fmt.Errorf("failed to write metadata file %s: %w", metaFilePath, err)
	}

	// 8. Update version-hint.text
	hintPath := io.JoinPath(t.targetTable.BasePath, "metadata", "version-hint.text")
	return t.storage.Write(ctx, hintPath, []byte(strconv.Itoa(nextVersion)))
}

// CommitChanges writes incremental changes.
//
// model.TableChange carries a diff, not a full file list, but CommitSnapshot's contract is to
// replace the table's live file set outright with whatever DataFiles it is given -- that is what
// "commit a snapshot" means for this target, and incremental_test.go's commitThreeSnapshots relies
// on it. So each change here is turned into the full live set before being handed to CommitSnapshot:
// the previous live set (read back from what the last commit -- in this loop or an earlier sync --
// actually wrote), minus that change's FilesRemoved, plus its FilesAdded. Without this, a
// metadata-only change that adds zero files (e.g. an ADD COLUMN with no new data) would commit a
// manifest with nothing in it, and every row written before it would vanish from the table's own
// view of itself.
func (t *Target) CommitChanges(ctx context.Context, changes *model.IncrementalTableChanges) error {
	for _, change := range changes.TableChanges {
		liveFiles, err := t.previousLiveFiles(ctx)
		if err != nil {
			return fmt.Errorf("failed to read the previous live file set before committing an incremental change: %w", err)
		}
		snap := &model.Snapshot{
			Table:            change.TableAsOfChange,
			DataFiles:        applyFilesDiff(liveFiles, change.FilesDiff),
			SourceIdentifier: change.SourceIdentifier,
		}
		if err := t.CommitSnapshot(ctx, snap); err != nil {
			return err
		}
	}
	return nil
}

// previousLiveFiles returns the complete, live data file set of the target table's current
// snapshot, or (nil, nil) if the table has no metadata yet -- a fresh incremental sync's first
// commit is exactly that case, and it is not an error.
func (t *Target) previousLiveFiles(ctx context.Context) ([]*model.DataFile, error) {
	versions, _, err := t.source.listMetadataFiles(ctx)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, nil
	}
	snapshot, err := t.source.GetCurrentSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snapshot.DataFiles, nil
}

// applyFilesDiff computes the new live file set from the previous one: live files, minus any file
// whose PhysicalPath appears in diff.FilesRemoved, plus diff.FilesAdded. Removals are applied before
// additions are appended so that a rewrite in place -- model.DiffFiles reports a file whose size or
// record count changed at the same path in both FilesAdded and FilesRemoved -- ends up live exactly
// once, carrying the new metadata rather than being dropped or duplicated.
func applyFilesDiff(live []*model.DataFile, diff *model.FilesDiff) []*model.DataFile {
	if diff == nil {
		return live
	}
	removed := make(map[string]bool, len(diff.FilesRemoved))
	for _, f := range diff.FilesRemoved {
		removed[f.PhysicalPath] = true
	}
	result := make([]*model.DataFile, 0, len(live)+len(diff.FilesAdded))
	for _, f := range live {
		if !removed[f.PhysicalPath] {
			result = append(result, f)
		}
	}
	result = append(result, diff.FilesAdded...)
	return result
}

// Close is a no-op for Iceberg target.
func (t *Target) Close() error {
	return nil
}
