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
	"errors"
	"fmt"
	"slices"

	"github.com/parquet-go/parquet-go"

	"github.com/slachiewicz/polytable/pkg/io"
)

// lastCheckpointFile is the pointer file Delta writers maintain at _delta_log/_last_checkpoint.
type lastCheckpointFile struct {
	Version int64  `json:"version"`
	Size    int64  `json:"size"`
	Parts   *int64 `json:"parts,omitempty"`
}

// checkpointState is the table state a Parquet checkpoint carries: the reconciled file set plus the
// metaData and protocol actions in force at the checkpoint version. remove tombstones are dropped on
// purpose — they exist for vacuum bookkeeping, not for state reconstruction.
type checkpointState struct {
	Version int64
	Meta    *MetadataAction
	Adds    []*AddAction
}

// The cp* structs mirror the classic checkpoint Parquet schema. They are separate from the JSON
// action structs because the physical types differ (the deletion vector's offset and size are
// INT32 here) and because parquet-go matches columns by parquet tag, not JSON tag. Columns this
// reader has no use for (txn, domainMetadata, baseRowId, ...) are omitted; parquet-go skips file
// columns absent from the target struct.
type cpRow struct {
	Add      *cpAdd      `parquet:"add,optional"`
	MetaData *cpMetaData `parquet:"metaData,optional"`
	Protocol *cpProtocol `parquet:"protocol,optional"`
	Sidecar  *cpSidecar  `parquet:"sidecar,optional"`
}

type cpAdd struct {
	Path             string             `parquet:"path"`
	PartitionValues  map[string]*string `parquet:"partitionValues"`
	Size             int64              `parquet:"size"`
	ModificationTime int64              `parquet:"modificationTime"`
	DataChange       bool               `parquet:"dataChange"`
	Stats            string             `parquet:"stats,optional"`
	Tags             map[string]string  `parquet:"tags,optional"`
	DeletionVector   *cpDeletionVector  `parquet:"deletionVector,optional"`
}

type cpDeletionVector struct {
	StorageType    string `parquet:"storageType"`
	PathOrInlineDv string `parquet:"pathOrInlineDv"`
	Offset         *int32 `parquet:"offset,optional"`
	SizeInBytes    int32  `parquet:"sizeInBytes"`
	Cardinality    int64  `parquet:"cardinality"`
}

type cpMetaData struct {
	ID               string            `parquet:"id"`
	Name             string            `parquet:"name,optional"`
	Description      string            `parquet:"description,optional"`
	Format           cpFormat          `parquet:"format"`
	SchemaString     string            `parquet:"schemaString"`
	PartitionColumns []string          `parquet:"partitionColumns,list"`
	CreatedTime      *int64            `parquet:"createdTime,optional"`
	Configuration    map[string]string `parquet:"configuration"`
}

type cpFormat struct {
	Provider string            `parquet:"provider"`
	Options  map[string]string `parquet:"options"`
}

type cpProtocol struct {
	MinReaderVersion int32    `parquet:"minReaderVersion"`
	MinWriterVersion int32    `parquet:"minWriterVersion"`
	ReaderFeatures   []string `parquet:"readerFeatures,optional,list"`
	WriterFeatures   []string `parquet:"writerFeatures,optional,list"`
}

type cpSidecar struct {
	Path string `parquet:"path"`
}

// readLastCheckpoint reads _delta_log/_last_checkpoint. A missing file is not an error — young
// tables simply have no checkpoint yet — so the caller gets (nil, nil).
func (s *Source) readLastCheckpoint(ctx context.Context) (*lastCheckpointFile, error) {
	path := io.JoinPath(s.basePath, "_delta_log", "_last_checkpoint")
	data, err := s.storage.Read(ctx, path)
	if err != nil {
		if errors.Is(err, io.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read %s: %w", path, err)
	}
	var cp lastCheckpointFile
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", path, err)
	}
	return &cp, nil
}

// checkpointPartFiles names the Parquet file(s) of a checkpoint: one classic single-file
// checkpoint, or `parts` files under the multi-part naming scheme.
func checkpointPartFiles(cp *lastCheckpointFile) []string {
	if cp.Parts == nil || *cp.Parts <= 1 {
		return []string{fmt.Sprintf("%020d.checkpoint.parquet", cp.Version)}
	}
	files := make([]string, 0, *cp.Parts)
	for i := int64(1); i <= *cp.Parts; i++ {
		files = append(files, fmt.Sprintf("%020d.checkpoint.%010d.%010d.parquet", cp.Version, i, *cp.Parts))
	}
	return files
}

// readCheckpoint loads the table state from the checkpoint that _last_checkpoint points at.
func (s *Source) readCheckpoint(ctx context.Context, cp *lastCheckpointFile) (*checkpointState, error) {
	state := &checkpointState{Version: cp.Version}

	for _, name := range checkpointPartFiles(cp) {
		path := io.JoinPath(s.basePath, "_delta_log", name)
		data, err := s.storage.Read(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("failed to read delta checkpoint %s: %w", path, err)
		}
		rows, err := parquet.Read[cpRow](bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, fmt.Errorf("failed to decode delta checkpoint %s: %w", path, err)
		}
		for i := range rows {
			row := &rows[i]
			switch {
			case row.Sidecar != nil && row.Sidecar.Path != "":
				return nil, fmt.Errorf("delta checkpoint %s is a v2 checkpoint with sidecar files, which is not supported yet", path)
			case row.Protocol != nil && slices.Contains(row.Protocol.ReaderFeatures, "v2Checkpoint"):
				return nil, fmt.Errorf("delta checkpoint %s declares the v2Checkpoint reader feature, which is not supported yet", path)
			case row.MetaData != nil:
				state.Meta = row.MetaData.toAction()
			case row.Add != nil:
				state.Adds = append(state.Adds, row.Add.toAction())
			}
		}
	}

	if state.Meta == nil {
		return nil, fmt.Errorf("delta checkpoint at version %d carries no metaData action", cp.Version)
	}
	return state, nil
}

func (m *cpMetaData) toAction() *MetadataAction {
	action := &MetadataAction{
		ID:               m.ID,
		Name:             m.Name,
		Description:      m.Description,
		Format:           FormatProvider{Provider: m.Format.Provider, Options: m.Format.Options},
		SchemaString:     m.SchemaString,
		PartitionColumns: m.PartitionColumns,
		Configuration:    m.Configuration,
	}
	if m.CreatedTime != nil {
		action.CreatedTime = *m.CreatedTime
	}
	return action
}

func (a *cpAdd) toAction() *AddAction {
	action := &AddAction{
		Path:             a.Path,
		PartitionValues:  a.PartitionValues,
		Size:             a.Size,
		ModificationTime: a.ModificationTime,
		DataChange:       a.DataChange,
		Stats:            a.Stats,
		Tags:             a.Tags,
	}
	if dv := a.DeletionVector; dv != nil {
		info := &DeletionVectorInfo{
			StorageType:    dv.StorageType,
			PathOrInlineDv: dv.PathOrInlineDv,
			SizeInBytes:    int64(dv.SizeInBytes),
			Cardinality:    dv.Cardinality,
		}
		if dv.Offset != nil {
			offset := int64(*dv.Offset)
			info.Offset = &offset
		}
		action.DeletionVector = info
	}
	return action
}
