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

// NestedField represents a field inside an Iceberg schema.
type NestedField struct {
	ID             int    `json:"id"`
	Name           string `json:"name"`
	Type           any    `json:"type"` // string (e.g. "int", "string") or nested map/struct
	Required       bool   `json:"required"`
	Doc            string `json:"doc,omitempty"`
	InitialDefault any    `json:"initial-default,omitempty"`
	WriteDefault   any    `json:"write-default,omitempty"`
}

// TableSchema represents an Iceberg schema definition.
type TableSchema struct {
	Type               string         `json:"type"`
	SchemaID           int            `json:"schema-id"`
	IdentifierFieldIDs []int          `json:"identifier-field-ids,omitempty"`
	Fields             []*NestedField `json:"fields"`
}

// PartitionFieldDef represents a partition field in an Iceberg partition specification.
type PartitionFieldDef struct {
	SourceID  int    `json:"source-id"`
	FieldID   int    `json:"field-id"`
	Name      string `json:"name"`
	Transform string `json:"transform"` // identity, year, month, day, hour, bucket[N], truncate[W]
}

// PartitionSpec represents an Iceberg partition specification.
type PartitionSpec struct {
	SpecID int                  `json:"spec-id"`
	Fields []*PartitionFieldDef `json:"fields"`
}

// SortField represents one field of an Iceberg sort order: the source column, how it is
// transformed before comparison, and the resulting sort direction and null placement.
type SortField struct {
	Transform string `json:"transform"`
	SourceID  int    `json:"source-id"`
	Direction string `json:"direction"`
	NullOrder string `json:"null-order"`
}

// SortOrder represents an Iceberg sort order definition. Order id 0 is reserved by the
// specification for "unsorted" and must carry an empty (non-null) Fields slice rather than
// omitting the array.
type SortOrder struct {
	OrderID int          `json:"order-id"`
	Fields  []*SortField `json:"fields"`
}

// SnapshotSummary holds metadata describing the commit operation and row counts.
type SnapshotSummary struct {
	Operation       string            `json:"operation"` // append, replace, overwrite, delete
	AddedDataFiles  string            `json:"added-data-files,omitempty"`
	AddedRecords    string            `json:"added-records,omitempty"`
	TotalDataFiles  string            `json:"total-data-files,omitempty"`
	TotalRecords    string            `json:"total-records,omitempty"`
	ExtraProperties map[string]string `json:"extra-properties,omitempty"`
}

// TableSnapshot represents a snapshot entry in Iceberg table metadata.
type TableSnapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID *int64            `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64             `json:"sequence-number"`
	TimestampMs      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list"`
	Summary          map[string]string `json:"summary"`
	SchemaID         *int              `json:"schema-id,omitempty"`
}

// SnapshotLogEntry records one entry of the `snapshot-log` array: an audit trail of which snapshot
// was current at a point in time. Unlike `snapshots`, entries here are not guaranteed to be dropped
// when the snapshot they name expires — implementations vary on whether they trim the log to match
// — so `snapshot-log` is not itself a reliable test of snapshot retention. `Snapshots` is: see
// Source.IsIncrementalSyncSafeFrom, which deliberately reads that field instead.
type SnapshotLogEntry struct {
	SnapshotID  int64 `json:"snapshot-id"`
	TimestampMs int64 `json:"timestamp-ms"`
}

// TableMetadata matches the Apache Iceberg v2/v3 metadata.json specification.
type TableMetadata struct {
	FormatVersion      int              `json:"format-version"`
	TableUUID          string           `json:"table-uuid"`
	Location           string           `json:"location"`
	LastSequenceNumber int64            `json:"last-sequence-number"`
	LastUpdatedMs      int64            `json:"last-updated-ms"`
	LastColumnID       int              `json:"last-column-id"`
	CurrentSchemaID    int              `json:"current-schema-id"`
	Schemas            []*TableSchema   `json:"schemas"`
	DefaultSpecID      int              `json:"default-spec-id"`
	PartitionSpecs     []*PartitionSpec `json:"partition-specs"`
	LastPartitionID    int              `json:"last-partition-id"`
	// SortOrders carries no omitempty: format-version 2 requires the key to exist even for a
	// table with no ordering, which is written as the reserved unsorted order (id 0, empty
	// Fields). Apache Iceberg's parser -- embedded in Trino -- rejects metadata missing the key
	// outright with "sort-orders must exist in format v2"; DuckDB's reader tolerates the
	// omission, which is why that gap was invisible to this project's DuckDB-only equivalence
	// suite. See docs/improvement-plan.md T72.
	SortOrders         []*SortOrder        `json:"sort-orders"`
	DefaultSortOrderID int                 `json:"default-sort-order-id"`
	Properties         map[string]string   `json:"properties,omitempty"`
	CurrentSnapshotID  *int64              `json:"current-snapshot-id,omitempty"`
	Snapshots          []*TableSnapshot    `json:"snapshots,omitempty"`
	SnapshotLog        []*SnapshotLogEntry `json:"snapshot-log,omitempty"`
}

// The manifest types below carry no struct tags: manifests are Avro, not JSON, and their field
// names, ids and encoding live in manifest.go with the schemas the specification fixes.

// ManifestEntry represents a data file entry inside an Iceberg manifest.
type ManifestEntry struct {
	Status     int // 0: EXISTING, 1: ADDED, 2: DELETED
	SnapshotID int64
	// SequenceNumber and FileSequenceNumber are null on an added entry, which is how the
	// specification says a reader must inherit them from the snapshot that added the file.
	SequenceNumber     *int64
	FileSequenceNumber *int64
	DataFile           *ManifestDataFile
}

// ManifestDataFile represents the metadata of a data file inside a manifest.
type ManifestDataFile struct {
	// Content is 0 for a data file; the delete-file contents are not written by this port.
	Content         int
	FilePath        string
	FileFormat      string
	Partition       map[string]any
	RecordCount     int64
	FileSizeInBytes int64
	ColumnSizes     map[int]int64
	ValueCounts     map[int]int64
	NullValueCounts map[int]int64
	NanValueCounts  map[int]int64
	// LowerBounds and UpperBounds hold the specification's single-value binary serialization,
	// keyed by field id.
	LowerBounds map[int][]byte
	UpperBounds map[int][]byte
}

// ManifestListEntry represents an entry inside a manifest list file.
type ManifestListEntry struct {
	ManifestPath       string
	ManifestLength     int64
	PartitionSpecID    int
	Content            int
	SequenceNumber     int64
	MinSequenceNumber  int64
	AddedSnapshotID    int64
	AddedFilesCount    int
	ExistingFilesCount int
	DeletedFilesCount  int
	AddedRowsCount     int64
	ExistingRowsCount  int64
	DeletedRowsCount   int64
}
