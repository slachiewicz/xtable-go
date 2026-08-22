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

package hudi_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/formats/hudi"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
)

// TestHudi_CommitSnapshotWritesTimelineLayoutVersion is T71: a real Apache Hudi reader --
// HoodieTableMetaClient, embedded in Trino, Spark and any other real Hudi connector -- throws
// TableNotFoundException("Table does not exist") the moment hoodie.properties carries no
// hoodie.timeline.layout.version and the caller supplies none either
// (HoodieTableMetaClient.java:202-209 in hudi-common 1.2.0, the version Java XTable itself compiles
// against). This asserts the value, not just its presence: hoodie.table.version 6 (what this target
// writes) must pair with timeline layout version 1, the classic 0.x timeline
// (TimelineLayoutVersion.LAYOUT_VERSION_1 in Hudi's own source) -- not layout version 2, which
// belongs to the 8/9 (Hudi 1.x) range MaxReadableTableVersion already refuses to read.
func TestHudi_CommitSnapshotWritesTimelineLayoutVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		partitioned    bool
		wantLayout     string
		wantPopulate   string
		wantTableVer   string
		wantBaseFormat string
	}{
		{
			name:           "unpartitioned table",
			partitioned:    false,
			wantLayout:     "1",
			wantPopulate:   "false",
			wantTableVer:   "6",
			wantBaseFormat: "PARQUET",
		},
		{
			name:           "partitioned table",
			partitioned:    true,
			wantLayout:     "1",
			wantPopulate:   "false",
			wantTableVer:   "6",
			wantBaseFormat: "PARQUET",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			memStorage := io.NewMemoryStorage()
			basePath := "mem://lake/" + tt.name

			idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
			cityField := &model.Field{Name: "city", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
			schema := model.NewRecordSchema("t", []*model.Field{idField, cityField}, false)

			table := &model.Table{
				Name:             "t",
				TableFormat:      model.TableFormatHudi,
				ReadSchema:       schema,
				BasePath:         basePath,
				LatestCommitTime: time.Now().UnixMilli(),
			}

			dataFile := &model.DataFile{
				PhysicalPath:  io.JoinPath(basePath, "data_0.parquet"),
				FileFormat:    model.FileFormatParquet,
				FileSizeBytes: 1024,
				RecordCount:   10,
				LastModified:  time.Now().UnixMilli(),
			}

			if tt.partitioned {
				partField := &model.PartitionField{
					SourceField:   cityField,
					TransformType: model.PartitionTransformValue,
				}
				table.PartitioningFields = []*model.PartitionField{partField}
				dataFile.PhysicalPath = io.JoinPath(basePath, "city=SF", "data_0.parquet")
				dataFile.PartitionValues = []*model.PartitionValue{
					{PartitionField: partField, Range: model.NewScalarRange("city=SF")},
				}
			}

			target := hudi.NewTarget(memStorage)
			require.NoError(t, target.Init(ctx, table))
			require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
				Table:            table,
				DataFiles:        []*model.DataFile{dataFile},
				SourceIdentifier: "20260822000000000",
			}))

			raw, err := memStorage.Read(ctx, io.JoinPath(basePath, ".hoodie", "hoodie.properties"))
			require.NoError(t, err)

			props, err := hudi.ParseProperties(raw)
			require.NoError(t, err)

			assert.Equal(t, tt.wantLayout, props.Get(hudi.PropTimelineLayoutVersion),
				"hoodie.timeline.layout.version must be set, and set to the layout that hoodie.table.version 6 requires -- "+
					"its absence is exactly what makes HoodieTableMetaClient throw TableNotFoundException")
			assert.Equal(t, tt.wantPopulate, props.Get(hudi.PropPopulateMetaFields),
				"hoodie.populate.meta.fields must be false: polytable's data files are foreign Parquet with no _hoodie_* meta columns")
			assert.Equal(t, tt.wantTableVer, props.Get(hudi.PropTableVersion))
			assert.Equal(t, tt.wantBaseFormat, props.Get(hudi.PropBaseFileFormat))
			assert.Equal(t, "t", props.Get(hudi.PropTableName))
			assert.Equal(t, "COPY_ON_WRITE", props.Get(hudi.PropTableType))
		})
	}
}

// TestHudi_CommitSnapshotRepairsExistingTableMissingLayoutVersion covers the resync case: a table
// this project wrote before T71 has hoodie.properties on disk with no layout version at all. The
// next CommitSnapshot must add it rather than leave the existing, table-not-found-inducing file
// alone -- CommitSnapshot loads and reuses existing properties (see target.go's "refusing to
// overwrite unreadable hoodie.properties" branch), so the fix must apply on every subsequent write,
// not only to a table's first one.
func TestHudi_CommitSnapshotRepairsExistingTableMissingLayoutVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	memStorage := io.NewMemoryStorage()
	basePath := "mem://lake/pre_t71_table"

	// Simulate a table written before this fix: hoodie.properties with everything T71 found except
	// the layout version.
	staleProps := "hoodie.table.name=t\nhoodie.table.type=COPY_ON_WRITE\nhoodie.table.version=6\nhoodie.table.base.file.format=PARQUET\n"
	require.NoError(t, memStorage.Write(ctx, io.JoinPath(basePath, ".hoodie", "hoodie.properties"), []byte(staleProps)))

	idField := &model.Field{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	schema := model.NewRecordSchema("t", []*model.Field{idField}, false)
	table := &model.Table{
		Name:             "t",
		TableFormat:      model.TableFormatHudi,
		ReadSchema:       schema,
		BasePath:         basePath,
		LatestCommitTime: time.Now().UnixMilli(),
	}
	dataFile := &model.DataFile{
		PhysicalPath:  io.JoinPath(basePath, "data_0.parquet"),
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: 512,
		RecordCount:   5,
		LastModified:  time.Now().UnixMilli(),
	}

	target := hudi.NewTarget(memStorage)
	require.NoError(t, target.Init(ctx, table))
	require.NoError(t, target.CommitSnapshot(ctx, &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "20260822000000001",
	}))

	raw, err := memStorage.Read(ctx, io.JoinPath(basePath, ".hoodie", "hoodie.properties"))
	require.NoError(t, err)
	props, err := hudi.ParseProperties(raw)
	require.NoError(t, err)

	assert.Equal(t, "1", props.Get(hudi.PropTimelineLayoutVersion),
		"a resync of a table written before T71 must add the layout version, not just new tables")
	assert.Equal(t, "false", props.Get(hudi.PropPopulateMetaFields))
}
