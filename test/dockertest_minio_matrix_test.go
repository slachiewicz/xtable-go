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
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/conversion"
	"github.com/slachiewicz/polytable/pkg/formats/delta"
	"github.com/slachiewicz/polytable/pkg/formats/hudi"
	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/io"
	"github.com/slachiewicz/polytable/pkg/model"
	"github.com/slachiewicz/polytable/pkg/spi"
)

const (
	minioUser     = "minioadmin"
	minioPassword = "minioadminpassword"
	testBucket    = "lakehouse-e2e"
)

func TestDockertest_MinIO_FullLakehouseMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dockertest integration test in short mode")
	}

	ctx := context.Background()
	pool := dockertest.NewPoolT(t, "")

	// 1. Run MinIO. dockertest v4 replaces v3's server-side Expire(120)-then-Purge with RunT's
	// automatic t.Cleanup: the container is still single-use per test run (WithoutReuse), but a
	// SIGKILL'd test process now leaks it instead of the daemon reaping it on a timer -- see
	// dockertest_trino_test.go's package comment for the full v3/v4 tradeoff this tree accepted when
	// the other dockertest_*.go files were migrated to v4.
	resource := pool.RunT(t, "minio/minio", dockertest.WithTag("latest"), dockertest.WithoutReuse(),
		dockertest.WithCmd([]string{"server", "/data"}),
		dockertest.WithEnv([]string{
			"MINIO_ROOT_USER=" + minioUser,
			"MINIO_ROOT_PASSWORD=" + minioPassword,
		}),
		dockertest.WithPortBindings(network.PortMap{
			network.MustParsePort("9000/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: ""}},
		}))

	minioPort := resource.GetPort("9000/tcp")
	minioEndpoint := fmt.Sprintf("http://127.0.0.1:%s", minioPort)

	// 2. Wait for MinIO readiness
	err := pool.Retry(ctx, 60*time.Second, func() error {
		resp, err := http.Get(fmt.Sprintf("%s/minio/health/live", minioEndpoint))
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("minio healthcheck returned status %d", resp.StatusCode)
		}
		return nil
	})
	require.NoError(t, err, "minio failed to become ready in time")

	// 3. Set AWS credentials for MinIO via environment variables for config path testing
	_ = os.Setenv("AWS_ACCESS_KEY_ID", minioUser)
	_ = os.Setenv("AWS_SECRET_ACCESS_KEY", minioPassword)
	_ = os.Setenv("AWS_REGION", "us-east-1")
	defer func() { _ = os.Unsetenv("AWS_ACCESS_KEY_ID") }()
	defer func() { _ = os.Unsetenv("AWS_SECRET_ACCESS_KEY") }()
	defer func() { _ = os.Unsetenv("AWS_REGION") }()

	// 4. Create S3 Test Bucket using config path
	storageConfig := conversion.StorageConfig{
		Region:       "us-east-1",
		Endpoint:     minioEndpoint,
		UsePathStyle: true,
	}

	testStorage, err := io.NewStorageForPathWithOptions(ctx, fmt.Sprintf("s3://%s", testBucket),
		storageConfig.ToOptionFuncs()...)
	require.NoError(t, err)

	s3Client, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(minioUser, minioPassword, "")),
	)
	require.NoError(t, err)

	s3svc := s3.NewFromConfig(s3Client, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioEndpoint)
		o.UsePathStyle = true
	})

	_, err = s3svc.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	require.NoError(t, err, "failed to create test bucket in MinIO")
	tableBasePath := fmt.Sprintf("s3://%s/tables/financial_events", testBucket)

	// Write mock physical Parquet data file into MinIO
	mockParquetBytes := []byte("PAR1-MOCK-PARQUET-BINARY-PAYLOAD-FOR-TEST-ROW-COUNT-500")
	parquetFilePath := fmt.Sprintf("%s/region=EU/data-001.parquet", tableBasePath)
	err = testStorage.Write(ctx, parquetFilePath, mockParquetBytes)
	require.NoError(t, err)

	// 6. Build initial Delta Table Seed on MinIO
	idField := &model.Field{Name: "transaction_id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	userField := &model.Field{Name: "user_uuid", Schema: model.NewPrimitiveSchema(model.TypeUUID, false)}
	amountField := &model.Field{Name: "amount", Schema: model.NewDecimalSchema(18, 2, false)}
	regionField := &model.Field{Name: "region", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	schema := model.NewRecordSchema("financial_events", []*model.Field{idField, userField, amountField, regionField}, false)

	partField := &model.PartitionField{
		SourceField:   regionField,
		TransformType: model.PartitionTransformValue,
	}

	table := &model.Table{
		Name:               "financial_events",
		TableFormat:        model.TableFormatDelta,
		ReadSchema:         schema,
		BasePath:           tableBasePath,
		PartitioningFields: []*model.PartitionField{partField},
		LatestCommitTime:   time.Now().UnixMilli(),
	}

	dataFile := &model.DataFile{
		PhysicalPath:  parquetFilePath,
		FileFormat:    model.FileFormatParquet,
		FileSizeBytes: int64(len(mockParquetBytes)),
		RecordCount:   500,
		PartitionValues: []*model.PartitionValue{
			{PartitionField: partField, Range: model.NewScalarRange("region=EU")},
		},
		LastModified: time.Now().UnixMilli(),
	}

	snapshot := &model.Snapshot{
		Table:            table,
		DataFiles:        []*model.DataFile{dataFile},
		SourceIdentifier: "0",
	}

	// Commit initial Delta snapshot on MinIO
	deltaTarget := delta.NewTarget(testStorage)
	err = deltaTarget.Init(ctx, table)
	require.NoError(t, err)
	err = deltaTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 7. RUN FULL MATRIX CONVERSIONS ON MINIO
	controller := conversion.NewController(testStorage)

	t.Run("DeltaToIcebergAndHudi_OnMinIO", func(t *testing.T) {
		datasetConfig := &conversion.DatasetConfig{
			SourceFormat:  model.TableFormatDelta,
			TargetFormats: []model.TableFormat{model.TableFormatIceberg, model.TableFormatHudi},
			TableName:     "financial_events",
			TableBasePath: tableBasePath,
			SyncMode:      spi.SyncModeFull,
			Storage:       &storageConfig,
		}

		results, syncErr := controller.Sync(ctx, datasetConfig)
		require.NoError(t, syncErr)
		require.Len(t, results, 2)

		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatHudi].StatusCode)

		// 8. Verify Iceberg Metadata on MinIO
		icebergSource := iceberg.NewSource(testStorage, tableBasePath)
		icebergTable, err := icebergSource.GetCurrentTable(ctx)
		require.NoError(t, err)
		assert.Equal(t, model.TableFormatIceberg, icebergTable.TableFormat)
		assert.Len(t, icebergTable.ReadSchema.Fields, 4)

		icebergSnap, err := icebergSource.GetCurrentSnapshot(ctx)
		require.NoError(t, err)
		require.Len(t, icebergSnap.DataFiles, 1)
		assert.Equal(t, int64(500), icebergSnap.DataFiles[0].RecordCount)

		// 9. Verify Hudi Metadata on MinIO
		hudiSource := hudi.NewSource(testStorage, tableBasePath)
		hudiTable, err := hudiSource.GetCurrentTable(ctx)
		require.NoError(t, err)
		assert.Equal(t, model.TableFormatHudi, hudiTable.TableFormat)

		hudiSnap, err := hudiSource.GetCurrentSnapshot(ctx)
		require.NoError(t, err)
		require.Len(t, hudiSnap.DataFiles, 1)
		assert.Equal(t, int64(500), hudiSnap.DataFiles[0].RecordCount)
	})

	t.Run("HudiToDeltaAndIceberg_OnMinIO", func(t *testing.T) {
		datasetConfig := &conversion.DatasetConfig{
			SourceFormat:  model.TableFormatHudi,
			TargetFormats: []model.TableFormat{model.TableFormatDelta, model.TableFormatIceberg},
			TableName:     "financial_events",
			TableBasePath: tableBasePath,
			SyncMode:      spi.SyncModeFull,
		}

		results, syncErr := controller.Sync(ctx, datasetConfig)
		require.NoError(t, syncErr)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)
	})
}
