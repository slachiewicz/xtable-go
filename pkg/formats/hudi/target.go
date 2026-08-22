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

package hudi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// hiveDefaultPartition is the directory-name convention Hive, Spark and Java XTable's own
// hudi.PathBasedPartitionValuesExtractor use for a null partition value.
const hiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__"

// Target implements spi.ConversionTarget for Apache Hudi tables.
type Target struct {
	storage     io.Storage
	targetTable *model.Table
	source      *Source
}

var _ spi.ConversionTarget = (*Target)(nil)

// NewTarget creates a new Hudi ConversionTarget.
func NewTarget(storage io.Storage) *Target {
	return &Target{
		storage: storage,
	}
}

// Format returns the format identifier.
func (t *Target) Format() model.TableFormat {
	return model.TableFormatHudi
}

// Init initializes the target with table configuration.
func (t *Target) Init(_ context.Context, targetTable *model.Table) error {
	t.targetTable = targetTable
	t.source = NewSource(t.storage, targetTable.BasePath)
	return nil
}

// GetTableMetadata retrieves previously saved TableSyncMetadata. Java XTable's Hudi target
// (HudiConversionTarget#getExtraMetadata) writes XTABLE_METADATA into the extraMetadata of the
// latest commit file, not into hoodie.properties, and reads it back from there too -- so that is
// tried first. polytable's own tables carry the same information (both shapes, see
// WriteSyncMetadataProperties) in hoodie.properties as well, which is the fallback for a table only
// polytable has touched or whose latest commit file cannot be read.
//
// This only checks the *latest* completed commit's extraMetadata, matching what the Java source
// reads. Whether Java itself scans further back when the newest commit lacks XTABLE_METADATA (for
// example, a native Hudi writer committed after a Java XTable sync) was not established from the
// Java source and is left alone rather than guessed; the fallback to hoodie.properties, or to
// "no metadata" if that too is absent, makes an unrecognized case a safe full resync rather than a
// wrong incremental one.
func (t *Target) GetTableMetadata(ctx context.Context) (*model.TableSyncMetadata, error) {
	commits, err := t.source.ListCompletedCommits(ctx)
	if err != nil {
		return nil, err
	}
	if len(commits) > 0 {
		latest := commits[len(commits)-1]
		commitFilePath := io.JoinPath(t.targetTable.BasePath, ".hoodie", latest.FileName)
		if data, readErr := t.storage.Read(ctx, commitFilePath); readErr == nil {
			var commitMeta HoodieCommitMetadata
			if json.Unmarshal(data, &commitMeta) == nil && len(commitMeta.ExtraMetadata) > 0 {
				if syncMeta := model.ReadSyncMetadataFromProperties(commitMeta.ExtraMetadata); syncMeta != nil {
					syncMeta.TargetFormat = model.TableFormatHudi
					syncMeta.CustomProperties = commitMeta.ExtraMetadata
					return syncMeta, nil
				}
			}
		}
		// A missing, unreadable or metadata-free commit file falls through to hoodie.properties
		// rather than failing the sync: for a polytable-synced table that file always carries the
		// same sync state anyway (both are written on every commit).
	}

	props, err := t.source.ReadProperties(ctx)
	if err != nil {
		// A table with no properties yet has no metadata; an unreadable one (a Hudi 1.x table,
		// storage failure) must stop the sync rather than masquerade as a fresh target.
		if errors.Is(err, io.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	syncMeta := model.ReadSyncMetadataFromProperties(props.Properties)
	if syncMeta == nil {
		return nil, nil
	}
	syncMeta.TargetFormat = model.TableFormatHudi
	syncMeta.CustomProperties = props.Properties
	return syncMeta, nil
}

// CommitSnapshot writes a full table snapshot into Hudi table format.
func (t *Target) CommitSnapshot(ctx context.Context, snapshot *model.Snapshot) error {
	now := time.Now()
	instant := InstantFromTime(now)

	// 1. Convert Schema to Avro JSON
	avroJSON, err := SchemaToAvroJSON(snapshot.Table.ReadSchema, snapshot.Table.Name, "hoodie."+snapshot.Table.Name)
	if err != nil {
		return fmt.Errorf("failed to convert schema to avro JSON: %w", err)
	}

	// 2. Prepare partition field names
	var partFieldNames []string
	for _, pf := range snapshot.Table.PartitioningFields {
		partFieldNames = append(partFieldNames, pf.SourceField.Name)
	}

	// 3. Build Write Stats
	partitionStats := make(map[string][]HoodieWriteStat)
	for _, df := range snapshot.AllDataFiles() {
		partPath := ""
		if len(df.PartitionValues) > 0 && df.PartitionValues[0].Range != nil {
			if df.PartitionValues[0].Range.MinValue == nil {
				// A nil MinValue is a genuine null partition value (T70 defect 2), not the string
				// "<nil>" that fmt.Sprintf("%v", nil) would otherwise fabricate. hiveDefaultPartition
				// is the marker Java XTable's own PathBasedPartitionValuesExtractor reads back to
				// null, so a foreign Hive/Spark-compatible reader of this Hudi table sees the same
				// convention it already expects.
				partPath = hiveDefaultPartition
			} else {
				partPath = fmt.Sprintf("%v", df.PartitionValues[0].Range.MinValue)
			}
		}

		// A write stat's path is relative to the base path, and the Hudi source joins it back onto
		// one. A file outside the table is not representable here, so it fails the commit.
		relPath, err := io.RelativizePath(df.PhysicalPath, t.targetTable.BasePath)
		if err != nil {
			return fmt.Errorf("failed to place data file under %s: %w", t.targetTable.BasePath, err)
		}

		ws := HoodieWriteStat{
			FileID:          uuid.New().String(),
			Path:            relPath,
			NumWrites:       df.RecordCount,
			TotalWriteBytes: df.FileSizeBytes,
			FileSizeInBytes: df.FileSizeBytes,
		}
		partitionStats[partPath] = append(partitionStats[partPath], ws)
	}

	// 4. Build and write Commit Metadata. extraMetadata is where Java XTable's Hudi target stores
	// XTABLE_METADATA (see GetTableMetadata), so it is written here even though it is also
	// duplicated into hoodie.properties below for polytable's own read path.
	syncMeta := &model.TableSyncMetadata{
		LastInstantSynced: snapshot.Table.LatestCommitTime,
		SourceFormat:      snapshot.Table.TableFormat,
		SourceIdentifier:  snapshot.SourceIdentifier,
	}
	extraMeta := make(map[string]string)
	model.WriteSyncMetadataProperties(extraMeta, syncMeta)

	commitMeta := HoodieCommitMetadata{
		PartitionToWriteStats: partitionStats,
		ExtraMetadata:         extraMeta,
		OperationType:         "POLYTABLE_SYNC",
	}

	commitBytes, err := json.Marshal(commitMeta)
	if err != nil {
		return fmt.Errorf("failed to serialize commit metadata: %w", err)
	}

	commitFilePath := io.JoinPath(t.targetTable.BasePath, ".hoodie", fmt.Sprintf("%s.commit", instant))
	if err := t.storage.Write(ctx, commitFilePath, commitBytes); err != nil {
		return fmt.Errorf("failed to write commit file %s: %w", commitFilePath, err)
	}

	// 5. Update hoodie.properties
	props := NewTableProperties()
	existingProps, err := t.source.ReadProperties(ctx)
	switch {
	case err == nil:
		props = existingProps
	case !errors.Is(err, io.ErrNotFound):
		// Overwriting properties that exist but cannot be read (a Hudi 1.x table, a storage
		// failure) would destroy them; only a genuinely absent file starts fresh.
		return fmt.Errorf("refusing to overwrite unreadable hoodie.properties: %w", err)
	}

	props.Set(PropTableName, snapshot.Table.Name)
	props.Set(PropTableType, "COPY_ON_WRITE")
	props.Set(PropTableVersion, "6")
	// hoodie.table.version 6 is Hudi's HoodieTableVersion.SIX, which Hudi's own
	// HoodieTableMetaClient.TableBuilder#setTableVersion always pairs with timeline layout version 1
	// (see TimelineLayoutVersionV1's comment). Without this key, HoodieTableMetaClient's constructor
	// throws TableNotFoundException("Table does not exist") the moment neither the caller nor
	// hoodie.properties supplies a layout version (HoodieTableMetaClient.java:202-209 in hudi-common
	// 1.2.0) -- it is not a validation of this property's value, it is the meta client's only proxy
	// for "is this actually a Hudi table". T71.
	props.Set(PropTimelineLayoutVersion, TimelineLayoutVersionV1)
	// polytable's data files are foreign Parquet written by another format's writer, so they carry
	// none of Hudi's _hoodie_* meta columns. hoodie.populate.meta.fields defaults to true in Hudi
	// itself, which would tell a reader to expect those columns; Java XTable's HudiConversionTarget
	// sets this to false for the same reason (HudiTableManager#initializeHudiTable's
	// setPopulateMetaFields(false) call).
	props.Set(PropPopulateMetaFields, "false")
	props.Set(PropBaseFileFormat, "PARQUET")
	if len(partFieldNames) > 0 {
		props.Set(PropPartitionFields, strings.Join(partFieldNames, ","))
	}
	props.Set(PropTableSchema, avroJSON)
	// Same syncMeta as the commit file's extraMetadata above, duplicated here for polytable's own
	// GetTableMetadata fallback -- see the comment on that method for why both locations matter.
	for k, v := range extraMeta {
		props.Set(k, v)
	}

	propsFilePath := io.JoinPath(t.targetTable.BasePath, ".hoodie", "hoodie.properties")
	return t.storage.Write(ctx, propsFilePath, props.Serialize())
}

// CommitChanges commits incremental changes.
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

// Close is a no-op.
func (t *Target) Close() error {
	return nil
}
