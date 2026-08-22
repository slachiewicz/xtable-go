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

// FormatProvider describes the underlying physical data format in Delta metadata.
//
// Options carries no omitempty on purpose: the Delta protocol declares it non-nullable, and
// delta-kernel-rs (the reader behind DuckDB's delta_scan and delta-rs) fails the whole log with
// "Encountered unmasked nulls in non-nullable StructArray child" when the key is absent. A nil map
// would marshal to null and fail the same way, so writers must set an empty map.
type FormatProvider struct {
	Provider string            `json:"provider"`
	Options  map[string]string `json:"options"`
}

// NewParquetFormat returns the FormatProvider every polytable-written Delta table carries. Use it
// rather than a bare literal: the empty Options map is load-bearing, see FormatProvider.
func NewParquetFormat() FormatProvider {
	return FormatProvider{Provider: "parquet", Options: map[string]string{}}
}

// ProtocolAction represents protocol versions supported by the table.
type ProtocolAction struct {
	MinReaderVersion int `json:"minReaderVersion"`
	MinWriterVersion int `json:"minWriterVersion"`
}

// MetadataAction describes table schema, partitioning, and custom configuration properties.
type MetadataAction struct {
	ID               string            `json:"id"`
	Name             string            `json:"name,omitempty"`
	Description      string            `json:"description,omitempty"`
	Format           FormatProvider    `json:"format"`
	SchemaString     string            `json:"schemaString"`
	PartitionColumns []string          `json:"partitionColumns"`
	Configuration    map[string]string `json:"configuration,omitempty"`
	CreatedTime      int64             `json:"createdTime,omitempty"`
}

// AddAction records the addition of a new data file.
//
// PartitionValues is map[string]*string, not map[string]string: the Delta protocol represents a
// null partition value as JSON null and an empty one as "", and encoding/json collapses both into
// the zero value "" when the map's value type is a bare string. A nil entry decodes a null; a
// non-nil pointer to "" decodes a genuine empty string. See T70 defect 2 / upstream #828.
type AddAction struct {
	Path             string              `json:"path"`
	PartitionValues  map[string]*string  `json:"partitionValues"`
	Size             int64               `json:"size"`
	ModificationTime int64               `json:"modificationTime"`
	DataChange       bool                `json:"dataChange"`
	Stats            string              `json:"stats,omitempty"`
	Tags             map[string]string   `json:"tags,omitempty"`
	DeletionVector   *DeletionVectorInfo `json:"deletionVector,omitempty"`
}

// DeletionVectorInfo describes a deletion vector associated with an AddAction.
type DeletionVectorInfo struct {
	StorageType    string `json:"storageType"`
	PathOrInlineDv string `json:"pathOrInlineDv"`
	Offset         *int64 `json:"offset,omitempty"`
	SizeInBytes    int64  `json:"sizeInBytes"`
	Cardinality    int64  `json:"cardinality"`
}

// RemoveAction records the removal/compaction of a data file.
type RemoveAction struct {
	Path                 string             `json:"path"`
	DeletionTimestamp    int64              `json:"deletionTimestamp,omitempty"`
	DataChange           bool               `json:"dataChange"`
	ExtendedFileMetadata bool               `json:"extendedFileMetadata,omitempty"`
	PartitionValues      map[string]*string `json:"partitionValues,omitempty"`
	Size                 *int64             `json:"size,omitempty"`
}

// CommitInfoAction stores metadata describing the commit operation.
type CommitInfoAction struct {
	Timestamp           int64             `json:"timestamp"`
	Operation           string            `json:"operation"`
	OperationParameters map[string]string `json:"operationParameters,omitempty"`
	EngineInfo          string            `json:"engineInfo,omitempty"`
	AppID               string            `json:"appId,omitempty"`
}

// SingleAction wraps any Delta log action line.
type SingleAction struct {
	Protocol   *ProtocolAction   `json:"protocol,omitempty"`
	MetaData   *MetadataAction   `json:"metaData,omitempty"`
	Add        *AddAction        `json:"add,omitempty"`
	Remove     *RemoveAction     `json:"remove,omitempty"`
	CommitInfo *CommitInfoAction `json:"commitInfo,omitempty"`
}

// StatsJSON represents the JSON structure of Delta AddAction.Stats.
type StatsJSON struct {
	NumRecords int64            `json:"numRecords"`
	MinValues  map[string]any   `json:"minValues,omitempty"`
	MaxValues  map[string]any   `json:"maxValues,omitempty"`
	NullCount  map[string]int64 `json:"nullCount,omitempty"`
}
