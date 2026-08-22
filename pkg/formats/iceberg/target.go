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
	versions, paths, _ := t.source.listMetadataFiles(ctx)
	nextVersion := 1
	var prevMeta *TableMetadata
	if len(versions) > 0 {
		latestVer := versions[len(versions)-1]
		nextVersion = latestVer + 1
		prevMeta, _ = t.source.readMetadata(ctx, paths[latestVer])
	}

	now := time.Now().UnixMilli()
	snapshotID := now
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
func (t *Target) CommitChanges(ctx context.Context, changes *model.IncrementalTableChanges) error {
	for _, change := range changes.TableChanges {
		snap := &model.Snapshot{
			Table:            change.TableAsOfChange,
			DataFiles:        change.FilesDiff.FilesAdded,
			SourceIdentifier: change.SourceIdentifier,
		}
		if err := t.CommitSnapshot(ctx, snap); err != nil {
			return err
		}
	}
	return nil
}

// Close is a no-op for Iceberg target.
func (t *Target) Close() error {
	return nil
}
