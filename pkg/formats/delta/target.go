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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// Target implements spi.ConversionTarget for Delta Lake tables.
type Target struct {
	storage     io.Storage
	targetTable *model.Table
	source      *Source
}

var _ spi.ConversionTarget = (*Target)(nil)

// NewTarget creates a new Delta ConversionTarget instance.
func NewTarget(storage io.Storage) *Target {
	return &Target{
		storage: storage,
	}
}

// Format returns the format identifier.
func (t *Target) Format() model.TableFormat {
	return model.TableFormatDelta
}

// Init initializes the target with target table configuration.
func (t *Target) Init(_ context.Context, targetTable *model.Table) error {
	t.targetTable = targetTable
	t.source = NewSource(t.storage, targetTable.BasePath)
	return nil
}

// GetTableMetadata retrieves synchronization metadata previously stored in Delta table properties.
func (t *Target) GetTableMetadata(ctx context.Context) (*model.TableSyncMetadata, error) {
	if t.source == nil {
		return nil, nil
	}
	versions, err := t.source.listCommitFiles(ctx)
	if err != nil || len(versions) == 0 {
		return nil, nil
	}

	latestVer := versions[len(versions)-1]
	commit, err := t.source.readCommit(ctx, latestVer)
	if err != nil {
		return nil, err
	}

	for _, a := range commit.Actions {
		if a.MetaData != nil && a.MetaData.Configuration != nil {
			syncMeta := model.ReadSyncMetadataFromProperties(a.MetaData.Configuration)
			if syncMeta == nil {
				return nil, nil
			}
			syncMeta.TargetFormat = model.TableFormatDelta
			syncMeta.CustomProperties = a.MetaData.Configuration
			return syncMeta, nil
		}
	}
	return nil, nil
}

// CommitSnapshot writes a full table snapshot into Delta Lake format.
func (t *Target) CommitSnapshot(ctx context.Context, snapshot *model.Snapshot) error {
	versions, _ := t.source.listCommitFiles(ctx)
	nextVersion := int64(0)
	if len(versions) > 0 {
		nextVersion = versions[len(versions)-1] + 1
	}

	schemaJSON, err := SchemaToDeltaJSON(snapshot.Table.ReadSchema)
	if err != nil {
		return fmt.Errorf("failed to convert schema to delta JSON: %w", err)
	}

	// Initialised rather than declared nil, because encoding/json renders a nil slice as null and
	// the Delta protocol requires partitionColumns to be an array -- empty for an unpartitioned
	// table. delta-kernel-rs enforces that and refuses the whole log with "unmasked nulls in
	// non-nullable StructArray child", so a null here makes the table unreadable by DuckDB and
	// anything else built on the kernel. Every fixture in this repository is partitioned, which is
	// why nothing caught it until an unpartitioned Snowflake table was converted.
	partitionColumns := make([]string, 0, len(snapshot.Table.PartitioningFields))
	for _, pf := range snapshot.Table.PartitioningFields {
		partitionColumns = append(partitionColumns, pf.SourceField.Name)
	}

	config := make(map[string]string)
	model.WriteSyncMetadataProperties(config, &model.TableSyncMetadata{
		LastInstantSynced: snapshot.Table.LatestCommitTime,
		SourceFormat:      snapshot.Table.TableFormat,
		SourceIdentifier:  snapshot.SourceIdentifier,
	})

	now := time.Now().UnixMilli()
	tableID := uuid.New().String()

	var actions []SingleAction

	// 1. Protocol Action
	actions = append(actions, SingleAction{
		Protocol: &ProtocolAction{
			MinReaderVersion: 1,
			MinWriterVersion: 2,
		},
	})

	// 2. MetaData Action
	actions = append(actions, SingleAction{
		MetaData: &MetadataAction{
			ID:               tableID,
			Name:             snapshot.Table.Name,
			Format:           NewParquetFormat(),
			SchemaString:     schemaJSON,
			PartitionColumns: partitionColumns,
			Configuration:    config,
			CreatedTime:      now,
		},
	})

	// 3. Add Actions for all active files
	for _, df := range snapshot.AllDataFiles() {
		addAction := t.convertDataFileToAddAction(df, snapshot.Table)
		actions = append(actions, SingleAction{Add: addAction})
	}

	// 4. CommitInfo Action
	actions = append(actions, SingleAction{
		CommitInfo: &CommitInfoAction{
			Timestamp:  now,
			Operation:  "POLYTABLE_SYNC_SNAPSHOT",
			EngineInfo: "polytable",
		},
	})

	return t.writeCommitFile(ctx, nextVersion, actions)
}

// CommitChanges writes an incremental batch of changes to the Delta table log.
func (t *Target) CommitChanges(ctx context.Context, changes *model.IncrementalTableChanges) error {
	for _, change := range changes.TableChanges {
		versions, err := t.source.listCommitFiles(ctx)
		if err != nil {
			return err
		}
		nextVersion := int64(0)
		if len(versions) > 0 {
			nextVersion = versions[len(versions)-1] + 1
		}

		schemaJSON, err := SchemaToDeltaJSON(change.TableAsOfChange.ReadSchema)
		if err != nil {
			return err
		}

		config := make(map[string]string)
		// SourceFormat and SourceIdentifier are filled in here too -- CommitSnapshot always did, but
		// the incremental path previously wrote only the instant, leaving KeySourceFormat unset on a
		// table that had only ever been synced incrementally.
		model.WriteSyncMetadataProperties(config, &model.TableSyncMetadata{
			LastInstantSynced: change.CommitTime,
			SourceFormat:      change.TableAsOfChange.TableFormat,
			SourceIdentifier:  change.SourceIdentifier,
		})

		now := time.Now().UnixMilli()
		var actions []SingleAction

		// Metadata Action
		// Non-nil for the same reason as the snapshot path above: a nil slice marshals to null,
		// and delta-kernel-rs rejects a log whose partitionColumns is null.
		partitionColumns := make([]string, 0, len(change.TableAsOfChange.PartitioningFields))
		for _, pf := range change.TableAsOfChange.PartitioningFields {
			partitionColumns = append(partitionColumns, pf.SourceField.Name)
		}
		actions = append(actions, SingleAction{
			MetaData: &MetadataAction{
				ID:               uuid.New().String(),
				Name:             change.TableAsOfChange.Name,
				Format:           NewParquetFormat(),
				SchemaString:     schemaJSON,
				PartitionColumns: partitionColumns,
				Configuration:    config,
				CreatedTime:      now,
			},
		})

		// Remove Actions
		for _, rf := range change.FilesDiff.FilesRemoved {
			relPath := t.makeRelativePath(rf.PhysicalPath)
			actions = append(actions, SingleAction{
				Remove: &RemoveAction{
					Path:              relPath,
					DeletionTimestamp: now,
					DataChange:        true,
				},
			})
		}

		// Add Actions
		for _, af := range change.FilesDiff.FilesAdded {
			addAction := t.convertDataFileToAddAction(af, change.TableAsOfChange)
			actions = append(actions, SingleAction{Add: addAction})
		}

		// CommitInfo Action
		actions = append(actions, SingleAction{
			CommitInfo: &CommitInfoAction{
				Timestamp:  now,
				Operation:  "POLYTABLE_SYNC_INCREMENTAL",
				EngineInfo: "polytable",
			},
		})

		if err := t.writeCommitFile(ctx, nextVersion, actions); err != nil {
			return err
		}
	}
	return nil
}

// Close is a no-op for delta target.
func (t *Target) Close() error {
	return nil
}

func (t *Target) writeCommitFile(ctx context.Context, version int64, actions []SingleAction) error {
	var buf bytes.Buffer
	for _, action := range actions {
		line, err := json.Marshal(action)
		if err != nil {
			return fmt.Errorf("failed to marshal delta action: %w", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}

	commitFileName := fmt.Sprintf("%020d.json", version)
	commitFilePath := io.JoinPath(t.targetTable.BasePath, "_delta_log", commitFileName)
	return t.storage.Write(ctx, commitFilePath, buf.Bytes())
}

func (t *Target) convertDataFileToAddAction(df *model.DataFile, _ *model.Table) *AddAction {
	// A nil Range.MinValue is a genuine null partition value and must marshal to JSON null, not
	// the string "<nil>" that fmt.Sprintf("%v", nil) would produce — that would corrupt the
	// log with a fabricated value instead of writing the null this data file actually has.
	partitionValues := make(map[string]*string)
	for _, pv := range df.PartitionValues {
		if pv.PartitionField == nil || pv.PartitionField.SourceField == nil || pv.Range == nil {
			continue
		}
		name := pv.PartitionField.SourceField.Name
		if pv.Range.MinValue == nil {
			partitionValues[name] = nil
			continue
		}
		value := fmt.Sprintf("%v", pv.Range.MinValue)
		partitionValues[name] = &value
	}

	stats := StatsJSON{
		NumRecords: df.RecordCount,
		MinValues:  make(map[string]any),
		MaxValues:  make(map[string]any),
		NullCount:  make(map[string]int64),
	}

	for _, cs := range df.ColumnStats {
		if cs.Field != nil {
			if cs.Range != nil {
				if v, ok := finiteBound(cs.Range.MinValue); ok {
					stats.MinValues[cs.Field.Name] = v
				}
				if v, ok := finiteBound(cs.Range.MaxValue); ok {
					stats.MaxValues[cs.Field.Name] = v
				}
			}
			stats.NullCount[cs.Field.Name] = cs.NumNulls
		}
	}

	statsBytes, _ := json.Marshal(stats)

	var dvAction *DeletionVectorInfo
	if df.DeletionVector != nil {
		offset := df.DeletionVector.Offset
		dvAction = &DeletionVectorInfo{
			StorageType:    "u",
			PathOrInlineDv: df.DeletionVector.StoragePath,
			Offset:         &offset,
			SizeInBytes:    df.DeletionVector.SizeInBytes,
			Cardinality:    df.DeletionVector.Cardinality,
		}
	}

	return &AddAction{
		Path:             t.makeRelativePath(df.PhysicalPath),
		PartitionValues:  partitionValues,
		Size:             df.FileSizeBytes,
		ModificationTime: df.LastModified,
		DataChange:       true,
		Stats:            string(statsBytes),
		DeletionVector:   dvAction,
	}
}

// finiteBound filters out the float values that encoding/json refuses. A single NaN or infinity
// anywhere in a file's column stats fails the marshal of the whole stats object, which would drop
// every statistic for that file — and the error is not reported, so the loss is silent. Such a
// bound prunes nothing anyway, so leaving it out costs no information.
func finiteBound(v any) (any, bool) {
	switch x := v.(type) {
	case nil:
		return nil, false
	case float32:
		f := float64(x)
		return x, !math.IsNaN(f) && !math.IsInf(f, 0)
	case float64:
		return x, !math.IsNaN(x) && !math.IsInf(x, 0)
	default:
		return v, true
	}
}

// makeRelativePath names a data file the way an add or remove action should: relative to the table
// directory, so the log survives the table being copied. Unlike Hudi and Paimon, the Delta protocol
// also allows an absolute URI here — resolveDataPath on the read side accepts one — so a file
// outside the table is kept whole rather than failing the commit.
func (t *Target) makeRelativePath(path string) string {
	rel, err := io.RelativizePath(path, t.targetTable.BasePath)
	if err != nil {
		return path
	}
	return rel
}
