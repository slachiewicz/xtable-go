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

package test_test

import (
	"context"
	"fmt"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

// glueMotoDatabaseName is the Glue database this suite creates in moto. Fixed rather than
// run-unique: the container itself is single-use (fresh per test run, purged on exit), so nothing
// is gained by randomizing the name inside it.
const glueMotoDatabaseName = "polytable_glue_moto_test"

// TestDockertest_Glue_MotoCatalogSync closes docs/improvement-plan.md T15: polytable's Glue catalog
// sync client (pkg/catalog/glue.go) and its partition sync (pkg/catalog/glue_partition.go) have only
// ever been exercised against fakes standing in for the AWS Glue API, never against any
// implementation of it. motoserver/moto implements enough of the Glue API -- CreateDatabase,
// CreateTable, UpdateTable, GetTable, GetTables, GetPartitions and BatchCreatePartition -- to close
// that gap without an AWS account.
//
// polytable needs no code change to reach it. The AWS SDK for Go v2 honors the service-specific
// endpoint environment variable AWS_ENDPOINT_URL_GLUE (normalized from the Glue client's SDK ID,
// "Glue"), which awsconfig.LoadDefaultConfig -- what NewGlueCatalogSyncClient and
// NewGlueConversionSource both call -- picks up on its own. pkg/catalog/glue.go has no endpoint
// option of its own, so this variable is the only lever available to point it at an emulator; the
// next reader who wants to add one should know this is why none exists yet.
func TestDockertest_Glue_MotoCatalogSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dockertest integration test in short mode")
	}

	ctx := context.Background()
	pool := dockertest.NewPoolT(t, "")

	// 1. Run moto. dockertest v4 replaces v3's server-side Expire(120)-then-Purge with RunT's
	// automatic t.Cleanup: the container is still single-use per test run (WithoutReuse), but a
	// SIGKILL'd test process now leaks it instead of the daemon reaping it on a timer -- see
	// dockertest_trino_test.go's package comment for the full v3/v4 tradeoff this tree accepted when
	// the other dockertest_*.go files were migrated to v4. moto serves every emulated AWS API from
	// the single port 5000, distinguishing services by the request's target header rather than by
	// port, so no service-specific Cmd or Env is needed to reach Glue specifically.
	resource := pool.RunT(t, "motoserver/moto", dockertest.WithTag("latest"), dockertest.WithoutReuse(),
		dockertest.WithPortBindings(network.PortMap{
			network.MustParsePort("5000/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: ""}},
		}))

	motoPort := resource.GetPort("5000/tcp")
	motoEndpoint := fmt.Sprintf("http://127.0.0.1:%s", motoPort)

	// 2. Wait for moto readiness. moto has no dedicated health endpoint, so any HTTP response from
	// the root -- moto answers 200 there -- proves the listener is up. http.NewRequestWithContext is
	// used rather than http.Get on the variable endpoint URL, which gosec's G107 flags.
	err := pool.Retry(ctx, 60*time.Second, func() error {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, motoEndpoint, nil)
		if reqErr != nil {
			return reqErr
		}
		resp, getErr := http.DefaultClient.Do(req)
		if getErr != nil {
			return getErr
		}
		defer func() { _ = resp.Body.Close() }()
		return nil
	})
	require.NoError(t, err, "moto failed to become ready in time")

	// 3. Point the AWS SDK's Glue client at moto. moto validates none of these credential values,
	// but the SDK refuses to sign a request without something present in all three.
	t.Setenv("AWS_ENDPOINT_URL_GLUE", motoEndpoint)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	// 4. Create the Glue database with a raw glue.Client, mirroring how the Azurite and MinIO
	// suites create their own containers/buckets with a raw azblob/s3 client before handing control
	// to polytable's own code paths.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	require.NoError(t, err)
	rawClient := glue.NewFromConfig(awsCfg)

	_, err = rawClient.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String(glueMotoDatabaseName)},
	})
	require.NoError(t, err, "failed to create Glue database in moto")

	// 5. Drive a real conversion through pkg/conversion, targeting the delta-rs-checkpoint/orders
	// fixture: partitioned by region with four values (east, north, south, west), which is what
	// makes the partition-sync assertions below meaningful. loadFixture copies the fixture into a
	// scratch directory, since a conversion writes its target metadata into the table's base path.
	tableDir, manifest := loadFixture(t, "delta-rs-checkpoint")
	storage := io.NewLocalStorage()

	datasetConfig := &conversion.DatasetConfig{
		SourceFormat:  model.TableFormatDelta,
		TargetFormats: []model.TableFormat{model.TableFormatIceberg},
		TableBasePath: tableDir,
		TableName:     manifest.TableName,
		SyncMode:      spi.SyncModeFull,
		Catalogs: []catalog.Config{
			{Type: catalog.CatalogTypeGlue, DatabaseName: glueMotoDatabaseName},
		},
	}

	controller := conversion.NewController(storage)

	results, err := controller.Sync(ctx, datasetConfig)
	require.NoError(t, err)
	require.Len(t, results, 1)
	icebergResult := results[model.TableFormatIceberg]
	require.NotNil(t, icebergResult)
	require.Equal(t, spi.SyncStatusSuccess, icebergResult.StatusCode, icebergResult.Error)

	// 6. Registration: the table exists in Glue afterwards, with table_type matching the target
	// format and polytable_synced_time present.
	t.Run("Registration", func(t *testing.T) {
		out, err := rawClient.GetTable(ctx, &glue.GetTableInput{
			DatabaseName: aws.String(glueMotoDatabaseName),
			Name:         aws.String(manifest.TableName),
		})
		require.NoError(t, err)
		require.NotNil(t, out.Table)

		assert.Equal(t, "ICEBERG", out.Table.Parameters[catalog.PropTableType],
			"table_type must match the Iceberg target format")
		assert.NotEmpty(t, out.Table.Parameters["polytable_synced_time"],
			"polytable_synced_time must be recorded")
	})

	// 7. Schema mapping: the Glue table's StorageDescriptor.Columns carry the data columns with
	// Glue type names, and region appears in PartitionKeys and not in Columns. That split is the
	// part most likely to regress: buildTableInput in pkg/catalog/glue.go computes it by excluding
	// any field named in table.PartitioningFields from the Columns list.
	t.Run("SchemaMapping", func(t *testing.T) {
		out, err := rawClient.GetTable(ctx, &glue.GetTableInput{
			DatabaseName: aws.String(glueMotoDatabaseName),
			Name:         aws.String(manifest.TableName),
		})
		require.NoError(t, err)
		require.NotNil(t, out.Table.StorageDescriptor)

		columns := make(map[string]string, len(out.Table.StorageDescriptor.Columns))
		for _, c := range out.Table.StorageDescriptor.Columns {
			columns[aws.ToString(c.Name)] = aws.ToString(c.Type)
		}
		assert.Equal(t, "bigint", columns["id"], "the delta-rs-checkpoint/orders id column is LONG")
		assert.Equal(t, "double", columns["amount"], "the delta-rs-checkpoint/orders amount column is DOUBLE")

		_, regionInColumns := columns["region"]
		assert.False(t, regionInColumns,
			"the partition column region must not be duplicated into StorageDescriptor.Columns")

		require.Len(t, out.Table.PartitionKeys, 1)
		assert.Equal(t, "region", aws.ToString(out.Table.PartitionKeys[0].Name))
		assert.Equal(t, "string", aws.ToString(out.Table.PartitionKeys[0].Type))
	})

	// 8. Partition sync: all four partitions exist, with the right values and locations. This
	// exercises catalog.SyncPartitions and pkg/catalog/glue_partition.go's AddPartitions against a
	// real Glue API for the first time.
	t.Run("PartitionSync", func(t *testing.T) {
		out, err := rawClient.GetPartitions(ctx, &glue.GetPartitionsInput{
			DatabaseName: aws.String(glueMotoDatabaseName),
			TableName:    aws.String(manifest.TableName),
		})
		require.NoError(t, err)
		require.Len(t, out.Partitions, 4)

		wantRegions := map[string]bool{"east": true, "north": true, "south": true, "west": true}
		seen := make(map[string]bool, 4)
		for _, p := range out.Partitions {
			require.Len(t, p.Values, 1)
			region := p.Values[0]
			assert.True(t, wantRegions[region], "unexpected partition value %q", region)
			seen[region] = true

			require.NotNil(t, p.StorageDescriptor)
			wantLocation := tableDir + "/region=" + region
			assert.Equal(t, wantLocation, aws.ToString(p.StorageDescriptor.Location),
				"partition %q must point at its own region= subdirectory", region)
		}
		assert.Len(t, seen, 4, "expected all four region values to be present exactly once")
	})

	// 9. Idempotency: a second sync against unchanged source data must leave exactly one table and
	// still four partitions, not duplicates. CreateOrUpdateTable already falls back to UpdateTable
	// on EntityNotFoundException, and catalog.SyncPartitions diffs desired against
	// GetAllPartitions rather than re-adding blindly -- but neither code path had ever met a real
	// Glue API before this test, so a duplicate here would have gone unnoticed until production.
	t.Run("Idempotency", func(t *testing.T) {
		results2, err := controller.Sync(ctx, datasetConfig)
		require.NoError(t, err)
		require.Len(t, results2, 1)
		icebergResult2 := results2[model.TableFormatIceberg]
		require.NotNil(t, icebergResult2)
		require.Equal(t, spi.SyncStatusSuccess, icebergResult2.StatusCode, icebergResult2.Error)

		tablesOut, err := rawClient.GetTables(ctx, &glue.GetTablesInput{
			DatabaseName: aws.String(glueMotoDatabaseName),
		})
		require.NoError(t, err)
		count := 0
		for _, tb := range tablesOut.TableList {
			if aws.ToString(tb.Name) == manifest.TableName {
				count++
			}
		}
		assert.Equal(t, 1, count, "a second sync must not register a duplicate table")

		partsOut, err := rawClient.GetPartitions(ctx, &glue.GetPartitionsInput{
			DatabaseName: aws.String(glueMotoDatabaseName),
			TableName:    aws.String(manifest.TableName),
		})
		require.NoError(t, err)
		assert.Len(t, partsOut.Partitions, 4, "a second sync against unchanged data must not duplicate partitions")
	})

	// 10. Discovery: mark the table with catalog.PropTargetFormats and assert
	// catalog.GlueConversionSource.ListTables finds it and skips an unmarked table.
	t.Run("Discovery", func(t *testing.T) {
		// UpdateTable replaces the whole TableInput, so every field Glue's Table carries that
		// TableInput also accepts must be copied forward here -- only Parameters is actually being
		// changed.
		got, err := rawClient.GetTable(ctx, &glue.GetTableInput{
			DatabaseName: aws.String(glueMotoDatabaseName),
			Name:         aws.String(manifest.TableName),
		})
		require.NoError(t, err)

		params := make(map[string]string, len(got.Table.Parameters)+1)
		for k, v := range got.Table.Parameters {
			params[k] = v
		}
		params[catalog.PropTargetFormats] = "DELTA"

		_, err = rawClient.UpdateTable(ctx, &glue.UpdateTableInput{
			DatabaseName: aws.String(glueMotoDatabaseName),
			TableInput: &gluetypes.TableInput{
				Name:              got.Table.Name,
				TableType:         got.Table.TableType,
				StorageDescriptor: got.Table.StorageDescriptor,
				PartitionKeys:     got.Table.PartitionKeys,
				Parameters:        params,
			},
		})
		require.NoError(t, err, "failed to mark the table with polytable_target_formats")

		// An unmarked table in the same database, so the assertion below proves the filter
		// actually excludes something rather than passing vacuously because every table in the
		// database happens to qualify.
		_, err = rawClient.CreateTable(ctx, &glue.CreateTableInput{
			DatabaseName: aws.String(glueMotoDatabaseName),
			TableInput:   &gluetypes.TableInput{Name: aws.String("unmarked_probe")},
		})
		require.NoError(t, err)

		source, err := catalog.NewGlueConversionSource(ctx, &catalog.Config{
			Type:         catalog.CatalogTypeGlue,
			DatabaseName: glueMotoDatabaseName,
		})
		require.NoError(t, err)
		defer func() { _ = source.Close() }()

		var discovered []catalog.TableIdentifier
		for id, listErr := range source.ListTables(ctx, glueMotoDatabaseName, catalog.TableFilter{RequireConversionMarkers: true}) {
			require.NoError(t, listErr)
			discovered = append(discovered, id)
		}

		require.Len(t, discovered, 1, "expected exactly the marked table to be discovered, and unmarked_probe skipped")
		assert.Equal(t, manifest.TableName, discovered[0].Table)
		assert.Equal(t, glueMotoDatabaseName, discovered[0].Database)
	})
}
