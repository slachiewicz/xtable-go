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

package paimon

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// Manifest files live under manifest/ in the table directory, named after
// FileStorePathFactory.MANIFEST_PREFIX and MANIFEST_LIST_PREFIX (paimon-bundle 1.3.1).
const (
	manifestDir             = "manifest"
	manifestPrefix          = "manifest-"
	manifestListPrefix      = "manifest-list-"
	snapshotDir             = "snapshot"
	snapshotPrefix          = "snapshot-"
	schemaDir               = "schema"
	schemaPrefix            = "schema-"
	latestHintFile          = "LATEST"
	earliestHintFile        = "EARLIEST"
	snapshotFormatVersion   = 3
	commitKindAppend        = "APPEND"
	commitKindOverwrite     = "OVERWRITE"
	polytableCommitUser     = "polytable"
	manifestEntryKindAdd    = 0
	manifestEntryKindDelete = 1
)

// FileMeta mirrors org.apache.paimon.io.DataFileMeta's record schema, keeping the field names
// Paimon gives it (_FILE_NAME, _FILE_SIZE, _ROW_COUNT, ...).
//
// Two deliberate deviations from Paimon, both because the manifest encoding here is JSON where
// Paimon writes Avro: _FILE_NAME holds the file's path relative to the table base path rather than
// a bare file name, and partition values travel as a string map instead of a binary row.
type FileMeta struct {
	FileName     string            `json:"_FILE_NAME"`
	FileSize     int64             `json:"_FILE_SIZE"`
	RowCount     int64             `json:"_ROW_COUNT"`
	SchemaID     int64             `json:"_SCHEMA_ID"`
	Level        int               `json:"_LEVEL"`
	CreationTime int64             `json:"_CREATION_TIME"`
	FileFormat   string            `json:"_FILE_FORMAT,omitempty"`
	Partition    map[string]string `json:"_PARTITION,omitempty"`
}

// ManifestEntry mirrors org.apache.paimon.manifest.ManifestEntry.
type ManifestEntry struct {
	Kind         int               `json:"_KIND"`
	Partition    map[string]string `json:"_PARTITION,omitempty"`
	Bucket       int               `json:"_BUCKET"`
	TotalBuckets int               `json:"_TOTAL_BUCKETS"`
	File         FileMeta          `json:"_FILE"`
}

// ManifestFile is one manifest: the entries a single commit contributed.
type ManifestFile struct {
	Entries []ManifestEntry `json:"entries"`
}

// ManifestFileMeta mirrors org.apache.paimon.manifest.ManifestFileMeta: the manifest list's
// per-manifest record.
type ManifestFileMeta struct {
	FileName        string `json:"_FILE_NAME"`
	FileSize        int64  `json:"_FILE_SIZE"`
	NumAddedFiles   int64  `json:"_NUM_ADDED_FILES"`
	NumDeletedFiles int64  `json:"_NUM_DELETED_FILES"`
	SchemaID        int64  `json:"_SCHEMA_ID"`
}

// ManifestList is the set of manifests a snapshot points at.
type ManifestList struct {
	Manifests []ManifestFileMeta `json:"manifests"`
}

// ParseManifestFileJSON parses a manifest written by this package.
func ParseManifestFileJSON(data []byte) (*ManifestFile, error) {
	var mf ManifestFile
	if err := json.Unmarshal(data, &mf); err != nil {
		return nil, err
	}
	return &mf, nil
}

// ParseManifestListJSON parses a manifest list written by this package.
func ParseManifestListJSON(data []byte) (*ManifestList, error) {
	var ml ManifestList
	if err := json.Unmarshal(data, &ml); err != nil {
		return nil, err
	}
	return &ml, nil
}

// entryForDataFile converts a canonical data file into a manifest entry of the given kind. A
// manifest entry names a file relative to the table directory, which dataFileForEntry joins back
// onto the base path, so a file outside the table fails the commit rather than being recorded.
func entryForDataFile(basePath string, file *model.DataFile, kind int, schemaID int64) (ManifestEntry, error) {
	fileName, err := io.RelativizePath(file.PhysicalPath, basePath)
	if err != nil {
		return ManifestEntry{}, fmt.Errorf("failed to place data file under %s: %w", basePath, err)
	}

	partition := partitionMap(file)
	return ManifestEntry{
		Kind:         kind,
		Partition:    partition,
		Bucket:       0,
		TotalBuckets: 1,
		File: FileMeta{
			FileName:     fileName,
			FileSize:     file.FileSizeBytes,
			RowCount:     file.RecordCount,
			SchemaID:     schemaID,
			Level:        0,
			CreationTime: file.LastModified,
			FileFormat:   string(file.FileFormat),
			Partition:    partition,
		},
	}, nil
}

// dataFileForEntry rebuilds a canonical data file from a manifest entry, resolving the recorded
// relative path against basePath.
func dataFileForEntry(basePath string, entry ManifestEntry, partitionFields map[string]*model.PartitionField) *model.DataFile {
	file := &model.DataFile{
		PhysicalPath:  io.JoinPath(basePath, entry.File.FileName),
		FileSizeBytes: entry.File.FileSize,
		RecordCount:   entry.File.RowCount,
		LastModified:  entry.File.CreationTime,
	}
	if entry.File.FileFormat != "" {
		file.FileFormat = model.FileFormat(entry.File.FileFormat)
	} else {
		file.FileFormat = model.FileFormatParquet
	}

	values := entry.Partition
	if len(values) == 0 {
		values = entry.File.Partition
	}
	for _, name := range sortedKeys(values) {
		field, ok := partitionFields[name]
		if !ok {
			continue
		}
		file.PartitionValues = append(file.PartitionValues, &model.PartitionValue{
			PartitionField: field,
			Range:          model.NewScalarRange(values[name]),
		})
	}

	return file
}

// partitionMap flattens a data file's partition values into the string map the manifest carries.
func partitionMap(file *model.DataFile) map[string]string {
	if len(file.PartitionValues) == 0 {
		return nil
	}
	values := make(map[string]string, len(file.PartitionValues))
	for _, pv := range file.PartitionValues {
		if pv == nil || pv.PartitionField == nil || pv.PartitionField.SourceField == nil || pv.Range == nil {
			continue
		}
		// A nil MinValue is a genuine null partition value (T70 defect 2), not the string "<nil>"
		// that fmt.Sprintf("%v", nil) would fabricate. Paimon's own default-partition marker is
		// configurable (partition.default-name) and this manifest reader does not read that
		// config, so this format already cannot distinguish null from an empty partition value on
		// read (entry.Partition is map[string]string) — folding a null into "" here is consistent
		// with that existing limitation rather than inventing a marker this reader would not
		// recognise. See T70's report for the residual gap.
		if pv.Range.MinValue == nil {
			values[pv.PartitionField.SourceField.Name] = ""
			continue
		}
		values[pv.PartitionField.SourceField.Name] = fmt.Sprintf("%v", pv.Range.MinValue)
	}
	if len(values) == 0 {
		return nil
	}
	return values
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
