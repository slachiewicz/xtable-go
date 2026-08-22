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
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
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

// Azurite's well-known development storage account. These are fixed, publicly documented test
// credentials shipped with the emulator, not secrets.
const (
	azuriteAccountName = "devstoreaccount1"
	azuriteAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	azuriteContainer   = "lakehouse-e2e"
	azuriteHost        = "devstoreaccount1.dfs.core.windows.net"
)

// azuriteRunOptions builds the container options both Azurite instances in this file use. Only the
// blob service is started, and two flags are load-bearing. --blobHost 0.0.0.0 makes the listener
// reachable through the published port: the image's default binds the container's own loopback, so
// a published port accepts the connection and then resets it. --skipApiVersionCheck is required
// because azblob sends a newer x-ms-version than Azurite recognizes, which Azurite rejects with
// InvalidHeaderValue on the first request; the emulator trails the service, so expect this to stay
// true after each SDK bump.
func azuriteRunOptions(env []string) []dockertest.RunOption {
	return []dockertest.RunOption{
		dockertest.WithTag("latest"),
		dockertest.WithCmd([]string{"azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000", "--skipApiVersionCheck"}),
		dockertest.WithEnv(env),
		dockertest.WithoutReuse(),
		dockertest.WithPortBindings(network.PortMap{
			network.MustParsePort("10000/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: ""}},
		}),
	}
}

// runAzurite starts an Azurite container, retrying a host-port collision.
//
// An empty HostPort asks Docker for a free ephemeral port, so a collision should not happen — and
// locally it does not. On GitHub's runners it did: the container failed to start with "failed to
// listen on TCP socket: address already in use", while the MinIO and Iceberg REST suites, which use
// the identical binding shape on different ports, started fine. The cause is not established, so
// this retries with a fresh allocation rather than pretending to know it. If a retry also fails,
// the error surfaces with the attempt count, which distinguishes a transient collision from a port
// that is permanently occupied on that host.
//
// This does not use pool.RunT: RunT calls t.Fatalf on the first error, which would abort the test on
// the very first collision rather than allow the retry this function exists to do. pool.Run is used
// directly instead, with r.Cleanup(t) called by hand on the success path so the container is still
// torn down through the same t.Cleanup mechanism every other container in this tree uses.
func runAzurite(ctx context.Context, t *testing.T, pool dockertest.Pool, env []string) dockertest.Resource {
	t.Helper()
	const attempts = 3

	var lastErr error
	for i := 1; i <= attempts; i++ {
		r, err := pool.Run(ctx, "mcr.microsoft.com/azure-storage/azurite", azuriteRunOptions(env)...)
		if err == nil {
			r.Cleanup(t)
			return r
		}
		lastErr = err
		if !strings.Contains(err.Error(), "address already in use") {
			t.Fatalf("failed to start azurite container: %v", err)
		}
	}
	t.Fatalf("azurite container failed to bind a host port after %d attempts: %v", attempts, lastErr)
	return nil
}

func TestDockertest_Azurite_FullLakehouseMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dockertest integration test in short mode")
	}

	ctx := context.Background()
	pool := dockertest.NewPoolT(t, "")

	// 1. Run Azurite. dockertest v4 replaces v3's server-side Expire(120)-then-Purge with
	// runAzurite's r.Cleanup(t) (see that function's doc comment): the container is still single-use
	// per test run, but a SIGKILL'd test process now leaks it instead of the daemon reaping it on a
	// timer -- see dockertest_trino_test.go's package comment for the full v3/v4 tradeoff this tree
	// accepted when the other dockertest_*.go files were migrated to v4.
	resource := runAzurite(ctx, t, pool, nil)

	azuritePort := resource.GetPort("10000/tcp")
	blobServiceURL := fmt.Sprintf("http://127.0.0.1:%s/%s", azuritePort, azuriteAccountName)

	// 2. Wait for Azurite readiness. Azurite has no health endpoint, so any HTTP response —
	// including the 400 Azurite returns for an unauthenticated GET on the service root — proves
	// the listener is up.
	err := pool.Retry(ctx, 60*time.Second, func() error {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, blobServiceURL, nil)
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
	require.NoError(t, err, "azurite failed to become ready in time")

	// 3. Configure the Azure-backed storage through conversion.StorageConfig, the same path the
	// CLI, daemon and REST server use. AccountKey has no StorageConfig field — credentials are
	// deliberately excluded from that type — so it is appended as an extra option func alongside
	// the ones ToOptionFuncs produces, rather than reimplementing the closures it already builds.
	storageConfig := conversion.StorageConfig{
		Azure: &conversion.AzureStorageConfig{
			Endpoint:    blobServiceURL,
			AccountName: azuriteAccountName,
		},
	}
	optFns := storageConfig.ToOptionFuncs()
	optFns = append(optFns, func(opts *io.Options) { opts.Azure.AccountKey = azuriteAccountKey })

	tableBasePath := fmt.Sprintf("abfss://%s@%s/tables/financial_events", azuriteContainer, azuriteHost)

	testStorage, err := io.NewStorageForPathWithOptions(ctx, tableBasePath, optFns...)
	require.NoError(t, err)

	// 4. Create the Azure test container using a raw azblob client, mirroring how the MinIO suite
	// makes its bucket with a raw s3.Client.
	cred, err := azblob.NewSharedKeyCredential(azuriteAccountName, azuriteAccountKey)
	require.NoError(t, err)
	azClient, err := azblob.NewClientWithSharedKeyCredential(blobServiceURL, cred, nil)
	require.NoError(t, err)

	_, err = azClient.CreateContainer(ctx, azuriteContainer, nil)
	require.NoError(t, err, "failed to create test container in Azurite")

	// Write mock physical Parquet data file into Azurite
	mockParquetBytes := []byte("PAR1-MOCK-PARQUET-BINARY-PAYLOAD-FOR-TEST-ROW-COUNT-500")
	parquetFilePath := fmt.Sprintf("%s/region=EU/data-001.parquet", tableBasePath)
	err = testStorage.Write(ctx, parquetFilePath, mockParquetBytes)
	require.NoError(t, err)

	// 5. Build initial Delta Table Seed on Azurite
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

	// Commit initial Delta snapshot on Azurite
	deltaTarget := delta.NewTarget(testStorage)
	err = deltaTarget.Init(ctx, table)
	require.NoError(t, err)
	err = deltaTarget.CommitSnapshot(ctx, snapshot)
	require.NoError(t, err)

	// 6. RUN FULL MATRIX CONVERSIONS ON AZURITE
	controller := conversion.NewController(testStorage)

	t.Run("DeltaToIcebergAndHudi_OnAzurite", func(t *testing.T) {
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

		// 7. Verify Iceberg Metadata on Azurite
		icebergSource := iceberg.NewSource(testStorage, tableBasePath)
		icebergTable, err := icebergSource.GetCurrentTable(ctx)
		require.NoError(t, err)
		assert.Equal(t, model.TableFormatIceberg, icebergTable.TableFormat)
		assert.Len(t, icebergTable.ReadSchema.Fields, 4)

		icebergSnap, err := icebergSource.GetCurrentSnapshot(ctx)
		require.NoError(t, err)
		require.Len(t, icebergSnap.DataFiles, 1)
		assert.Equal(t, int64(500), icebergSnap.DataFiles[0].RecordCount)

		// 8. Verify Hudi Metadata on Azurite
		hudiSource := hudi.NewSource(testStorage, tableBasePath)
		hudiTable, err := hudiSource.GetCurrentTable(ctx)
		require.NoError(t, err)
		assert.Equal(t, model.TableFormatHudi, hudiTable.TableFormat)

		hudiSnap, err := hudiSource.GetCurrentSnapshot(ctx)
		require.NoError(t, err)
		require.Len(t, hudiSnap.DataFiles, 1)
		assert.Equal(t, int64(500), hudiSnap.DataFiles[0].RecordCount)
	})

	t.Run("HudiToDeltaAndIceberg_OnAzurite", func(t *testing.T) {
		datasetConfig := &conversion.DatasetConfig{
			SourceFormat:  model.TableFormatHudi,
			TargetFormats: []model.TableFormat{model.TableFormatDelta, model.TableFormatIceberg},
			TableName:     "financial_events",
			TableBasePath: tableBasePath,
			SyncMode:      spi.SyncModeFull,
			Storage:       &storageConfig,
		}

		results, syncErr := controller.Sync(ctx, datasetConfig)
		require.NoError(t, syncErr)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatDelta].StatusCode)
		assert.Equal(t, spi.SyncStatusSuccess, results[model.TableFormatIceberg].StatusCode)
	})

	t.Run("RoundTripsAbfssPaths", func(t *testing.T) {
		roundTripPath := fmt.Sprintf("%s/roundtrip/probe.txt", tableBasePath)
		err := testStorage.Write(ctx, roundTripPath, []byte("abfss round trip probe"))
		require.NoError(t, err)

		listPrefix := fmt.Sprintf("%s/roundtrip", tableBasePath)
		infos, err := testStorage.List(ctx, listPrefix)
		require.NoError(t, err)
		require.NotEmpty(t, infos)

		for _, info := range infos {
			assert.True(t, len(info.Path) > len("abfss://") && info.Path[:len("abfss://")] == "abfss://",
				"expected FileInfo.Path %q to start with abfss://", info.Path)

			container, blobPath, host, scheme, parseErr := io.ParseAzureURI(info.Path)
			require.NoError(t, parseErr, "expected %q to parse back through ParseAzureURI", info.Path)
			assert.Equal(t, "abfss", scheme)
			assert.Equal(t, azuriteContainer, container)
			assert.Equal(t, azuriteHost, host)
			assert.Equal(t, "tables/financial_events/roundtrip/probe.txt", blobPath)
		}
	})

	// 9. Credential-mode and URI-scheme coverage. These share the container and the raw
	// shared-key credential the setup above already built, rather than standing up a second
	// Azurite container. generateContainerSAS signs a container-scoped SAS against the
	// devstoreaccount1 shared key -- the same credential used to create the container -- so
	// every SAS subtest below exercises a real signature Azurite has to validate, not a
	// hand-built query string.
	generateContainerSAS := func(t *testing.T, perms sas.ContainerPermissions) string {
		t.Helper()
		// Protocol is left at its zero value deliberately: Azurite is reached over plain HTTP in
		// this suite, and sas.ProtocolHTTPS would bake "spr=https" into the signature, which
		// Azurite would then reject against an http:// request.
		qp, sasErr := sas.BlobSignatureValues{
			StartTime:     time.Now().UTC().Add(-10 * time.Second),
			ExpiryTime:    time.Now().UTC().Add(1 * time.Hour),
			Permissions:   perms.String(),
			ContainerName: azuriteContainer,
		}.SignWithSharedKey(cred)
		require.NoError(t, sasErr)
		return qp.Encode()
	}

	fullSASPermissions := sas.ContainerPermissions{Read: true, Write: true, List: true, Delete: true}

	t.Run("SASTokenCredential", func(t *testing.T) {
		sasToken := generateContainerSAS(t, fullSASPermissions)

		optFnsSAS := storageConfig.ToOptionFuncs()
		optFnsSAS = append(optFnsSAS, func(opts *io.Options) { opts.Azure.SASToken = sasToken })

		sasStorage, err := io.NewStorageForPathWithOptions(ctx, tableBasePath, optFnsSAS...)
		require.NoError(t, err)

		probePath := fmt.Sprintf("%s/credmatrix/sas/probe.txt", tableBasePath)
		probeData := []byte("sas token credential probe")

		err = sasStorage.Write(ctx, probePath, probeData)
		require.NoError(t, err, "write with only SASToken set (no AccountKey) must succeed")

		got, err := sasStorage.Read(ctx, probePath)
		require.NoError(t, err)
		assert.Equal(t, probeData, got)

		exists, err := sasStorage.Exists(ctx, probePath)
		require.NoError(t, err)
		assert.True(t, exists)

		listPrefix := fmt.Sprintf("%s/credmatrix/sas", tableBasePath)
		infos, err := sasStorage.List(ctx, listPrefix)
		require.NoError(t, err)
		assert.NotEmpty(t, infos)

		err = sasStorage.Delete(ctx, probePath)
		require.NoError(t, err)

		exists, err = sasStorage.Exists(ctx, probePath)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("SASTokenWithLeadingQuestionMark", func(t *testing.T) {
		sasToken := generateContainerSAS(t, fullSASPermissions)

		optFnsSAS := storageConfig.ToOptionFuncs()
		// NewAzureStorage trims a leading "?" from SASToken. Passing it here proves that trim,
		// since a SAS string copied out of the Azure portal carries one and a caller that forgets
		// to strip it should not get an opaque auth failure.
		optFnsSAS = append(optFnsSAS, func(opts *io.Options) { opts.Azure.SASToken = "?" + sasToken })

		sasStorage, err := io.NewStorageForPathWithOptions(ctx, tableBasePath, optFnsSAS...)
		require.NoError(t, err)

		probePath := fmt.Sprintf("%s/credmatrix/sas-leading-qmark/probe.txt", tableBasePath)
		probeData := []byte("sas token with leading question mark probe")

		err = sasStorage.Write(ctx, probePath, probeData)
		require.NoError(t, err, "a SAS token with a leading ? must work identically to one without")

		got, err := sasStorage.Read(ctx, probePath)
		require.NoError(t, err)
		assert.Equal(t, probeData, got)
	})

	t.Run("AnonymousAccessFailsClosed", func(t *testing.T) {
		// NewAzureStorage checks AZURE_STORAGE_SAS_TOKEN and AZURE_STORAGE_KEY before it reaches
		// the Anonymous case. Clear both so ambient environment on the machine running the test
		// cannot silently authenticate this subtest through a different path.
		t.Setenv("AZURE_STORAGE_SAS_TOKEN", "")
		t.Setenv("AZURE_STORAGE_KEY", "")

		optFnsAnon := storageConfig.ToOptionFuncs()
		optFnsAnon = append(optFnsAnon, func(opts *io.Options) { opts.Azure.Anonymous = true })

		anonStorage, err := io.NewStorageForPathWithOptions(ctx, tableBasePath, optFnsAnon...)
		require.NoError(t, err)

		listPrefix := fmt.Sprintf("%s/roundtrip", tableBasePath)
		_, err = anonStorage.List(ctx, listPrefix)
		require.Error(t, err, "anonymous access to a private container must fail, not return an empty list")

		// Exists is probed separately, and deliberately not asserted either way: pkg/io/azure.go's
		// Exists maps bloberror.BlobNotFound to (false, nil). If Azurite's anonymous-access
		// rejection on a private container surfaces with that same error code (which real Azure's
		// anonymous-access-disabled response sometimes does, depending on the exact failure mode),
		// then an auth failure and a genuinely missing blob become indistinguishable through this
		// method -- a real table would look empty instead of inaccessible. This is a candidate bug
		// in pkg/io/azure.go, reported here rather than fixed: that file belongs to another task.
		existsResult, existsErr := anonStorage.Exists(ctx, parquetFilePath)
		if existsErr != nil {
			t.Logf("Exists on a private container under anonymous access returned an error, as expected: %v", existsErr)
		} else {
			t.Logf("Exists on a private container under anonymous access returned (%v, nil) instead of an "+
				"error -- see the comment above this probe: pkg/io/azure.go's BlobNotFound mapping makes an "+
				"auth failure indistinguishable from a missing blob", existsResult)
		}
	})

	t.Run("SASBeatsAccountKey", func(t *testing.T) {
		sasToken := generateContainerSAS(t, fullSASPermissions)

		optFnsPrecedence := storageConfig.ToOptionFuncs()
		optFnsPrecedence = append(optFnsPrecedence,
			func(opts *io.Options) { opts.Azure.SASToken = sasToken },
			// Deliberately wrong, and not even valid base64 (shared keys are base64 and "-" is
			// outside that alphabet). If the credential switch in NewAzureStorage ever stops
			// picking SAS first, this makes it fail loudly at NewSharedKeyCredential rather than
			// silently authenticating with a key that happens to parse.
			func(opts *io.Options) { opts.Azure.AccountKey = "NOT-A-VALID-BASE64-KEY-DELIBERATELY-WRONG" },
		)

		precedenceStorage, err := io.NewStorageForPathWithOptions(ctx, tableBasePath, optFnsPrecedence...)
		require.NoError(t, err, "SAS must win first-match-wins over AccountKey; a wrong key must not even be reached")

		probePath := fmt.Sprintf("%s/credmatrix/precedence/probe.txt", tableBasePath)
		probeData := []byte("sas beats account key probe")

		err = precedenceStorage.Write(ctx, probePath, probeData)
		require.NoError(t, err, "SAS token must win first-match-wins over a wrong account key")

		got, err := precedenceStorage.Read(ctx, probePath)
		require.NoError(t, err)
		assert.Equal(t, probeData, got)
	})

	t.Run("AllFourSchemes", func(t *testing.T) {
		schemeProbeData := []byte("scheme parity probe payload")
		writePath := fmt.Sprintf("abfss://%s@%s/credmatrix/schemes/probe.txt", azuriteContainer, azuriteHost)

		err := testStorage.Write(ctx, writePath, schemeProbeData)
		require.NoError(t, err)

		schemes := []string{"abfss", "abfs", "wasbs", "wasb"}
		for _, scheme := range schemes {
			t.Run(scheme, func(t *testing.T) {
				readPath := fmt.Sprintf("%s://%s@%s/credmatrix/schemes/probe.txt", scheme, azuriteContainer, azuriteHost)

				schemeStorage, err := io.NewStorageForPathWithOptions(ctx, readPath, optFns...)
				require.NoError(t, err)

				got, err := schemeStorage.Read(ctx, readPath)
				require.NoError(t, err, "expected %s:// to read back the blob written through abfss://", scheme)
				assert.Equal(t, schemeProbeData, got)
			})
		}
	})

	t.Run("ListPagination", func(t *testing.T) {
		t.Skip("pkg/io/azure.go's List hard-codes azblob.ListBlobsFlatOptions{Prefix: &blobPath} with no " +
			"MaxResults, so the list page size cannot be lowered from outside pkg/ -- and pkg/ is out of " +
			"scope for this file. The un-overridden server default page size for ListBlobs is 5000 on both " +
			"Azure and Azurite, so forcing a second page needs more than 5000 sequential blob uploads " +
			"against a single container, which is impractical for a unit test. Skipped rather than faked " +
			"with a lowered page size that would not actually exercise pagination.")
	})

	// 10. T55's last unmet acceptance criterion: two datasets naming different account-key
	// environment variables must sync to different accounts in one process. A second, genuinely
	// distinct Azurite container proves both the credential and the endpoint are per-dataset, not
	// only the credential -- one Azurite instance with two AZURITE_ACCOUNTS entries would leave the
	// endpoint shared and prove less.
	t.Run("TwoAccountsOneProcess", func(t *testing.T) {
		const (
			secondAccountName = "polytableaccountb"
			// Valid base64, and deliberately not devstoreaccount1's key: azblob.NewSharedKeyCredential
			// and Azurite's own AZURITE_ACCOUNTS parsing both require valid base64, but the byte
			// content is otherwise arbitrary.
			//nolint:gosec // not a credential: a fixed, non-secret key for a throwaway Azurite container
			secondAccountKey = "cG9seXRhYmxlLXNlY29uZC1hY2NvdW50LXNoYXJlZC1rZXktZm9yLWF6dXJpdGUtdGVzdA=="
		)

		// Start a second Azurite container with its own account, via AZURITE_ACCOUNTS, so the two
		// datasets below really do talk to different stores rather than the same devstoreaccount1
		// reachable on two ports.
		resourceB := runAzurite(ctx, t, pool, []string{fmt.Sprintf("AZURITE_ACCOUNTS=%s:%s", secondAccountName, secondAccountKey)})

		secondPort := resourceB.GetPort("10000/tcp")
		secondBlobServiceURL := fmt.Sprintf("http://127.0.0.1:%s/%s", secondPort, secondAccountName)

		err := pool.Retry(ctx, 60*time.Second, func() error {
			req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, secondBlobServiceURL, nil)
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
		require.NoError(t, err, "second azurite container failed to become ready in time")

		// Create the container on the second account with a raw azblob client, mirroring the
		// top-level setup above.
		secondCred, err := azblob.NewSharedKeyCredential(secondAccountName, secondAccountKey)
		require.NoError(t, err)
		secondAzClient, err := azblob.NewClientWithSharedKeyCredential(secondBlobServiceURL, secondCred, nil)
		require.NoError(t, err)
		_, err = secondAzClient.CreateContainer(ctx, azuriteContainer, nil)
		require.NoError(t, err, "failed to create test container on the second Azurite account")

		// The three environment variables that make this test mean something. AZURE_STORAGE_KEY is
		// deliberately wrong, though still valid base64: if credential resolution for either
		// dataset ever falls back to the well-known variable instead of respecting its own
		// AccountKeyEnv, that dataset fails loudly (an auth error against the real accounts) rather
		// than silently passing against the wrong one. Do not simplify this away.
		t.Setenv("POLYTABLE_TEST_AZURE_KEY_A", azuriteAccountKey)
		t.Setenv("POLYTABLE_TEST_AZURE_KEY_B", secondAccountKey)
		//nolint:gosec // not a credential: deliberately wrong, valid-base64 sentinel value for the well-known fallback var
		t.Setenv("AZURE_STORAGE_KEY", "dGhpcy1pcy1hLWRlbGliZXJhdGVseS13cm9uZy1henVyZS1zdG9yYWdlLWtleS12YWx1ZQ==")

		secondTableBasePath := fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/tables/other_events",
			azuriteContainer, secondAccountName)

		// Two conversion.StorageConfig values, each naming its own Azure endpoint, account and
		// AccountKeyEnv, run through ToOptionFuncs() -- the same path the CLI, daemon and REST
		// server use -- rather than constructing io.AzureOptions directly, since the per-dataset
		// plumbing through StorageConfig is the actual subject of this test.
		configA := conversion.StorageConfig{
			Azure: &conversion.AzureStorageConfig{
				Endpoint:      blobServiceURL,
				AccountName:   azuriteAccountName,
				AccountKeyEnv: "POLYTABLE_TEST_AZURE_KEY_A",
			},
		}
		configB := conversion.StorageConfig{
			Azure: &conversion.AzureStorageConfig{
				Endpoint:      secondBlobServiceURL,
				AccountName:   secondAccountName,
				AccountKeyEnv: "POLYTABLE_TEST_AZURE_KEY_B",
			},
		}

		storageA, err := io.NewStorageForPathWithOptions(ctx, tableBasePath, configA.ToOptionFuncs()...)
		require.NoError(t, err)
		storageB, err := io.NewStorageForPathWithOptions(ctx, secondTableBasePath, configB.ToOptionFuncs()...)
		require.NoError(t, err)

		pathA := fmt.Sprintf("%s/two-accounts/probe-a.txt", tableBasePath)
		dataA := []byte("account A payload, only visible through storageA")
		pathB := fmt.Sprintf("%s/two-accounts/probe-b.txt", secondTableBasePath)
		dataB := []byte("account B payload, only visible through storageB")

		require.NoError(t, storageA.Write(ctx, pathA, dataA))
		require.NoError(t, storageB.Write(ctx, pathB, dataB))

		gotA, err := storageA.Read(ctx, pathA)
		require.NoError(t, err)
		assert.Equal(t, dataA, gotA, "storageA must read back byte-identically what it wrote")

		gotB, err := storageB.Read(ctx, pathB)
		require.NoError(t, err)
		assert.Equal(t, dataB, gotB, "storageB must read back byte-identically what it wrote")

		listA, err := storageA.List(ctx, fmt.Sprintf("%s/two-accounts", tableBasePath))
		require.NoError(t, err)
		assert.NotEmpty(t, listA)

		listB, err := storageB.List(ctx, fmt.Sprintf("%s/two-accounts", secondTableBasePath))
		require.NoError(t, err)
		assert.NotEmpty(t, listB)

		// The cross-check that proves the two are really separate stores: a blob written only to
		// account A must not be visible from the storage built for account B.
		existsOnB, err := storageB.Exists(ctx, pathA)
		require.NoError(t, err)
		assert.False(t, existsOnB, "a blob written only to account A must not be visible from account B's storage")

		// Negative case pinning the no-fall-through rule: an AccountKeyEnv naming an unset variable
		// must fail construction, with an error naming that variable -- the rule the wrong
		// AZURE_STORAGE_KEY above would otherwise hide if it were ever silently consulted instead.
		configMissing := conversion.StorageConfig{
			Azure: &conversion.AzureStorageConfig{
				Endpoint:      blobServiceURL,
				AccountName:   azuriteAccountName,
				AccountKeyEnv: "POLYTABLE_TEST_AZURE_KEY_UNSET",
			},
		}
		_, err = io.NewStorageForPathWithOptions(ctx, tableBasePath, configMissing.ToOptionFuncs()...)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "POLYTABLE_TEST_AZURE_KEY_UNSET")
	})
}
