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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/model"
)

func TestDockertest_IcebergRESTCatalogSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dockertest integration test in short mode")
	}

	ctx := context.Background()
	pool := dockertest.NewPoolT(t, "")

	// 1. Run Tabular Iceberg REST Catalog container. dockertest v4 replaces v3's server-side
	// Expire(120)-then-Purge with RunT's automatic t.Cleanup: the container is still single-use per
	// test run (WithoutReuse), but a SIGKILL'd test process now leaks it instead of the daemon
	// reaping it on a timer -- see dockertest_trino_test.go's package comment for the full v3/v4
	// tradeoff this tree accepted when the other dockertest_*.go files were migrated to v4.
	resource := pool.RunT(t, "tabulario/iceberg-rest", dockertest.WithTag("latest"), dockertest.WithoutReuse(),
		dockertest.WithPortBindings(network.PortMap{
			network.MustParsePort("8181/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: ""}},
		}))

	restPort := resource.GetPort("8181/tcp")
	restURL := fmt.Sprintf("http://127.0.0.1:%s", restPort)

	// 2. Wait for Iceberg REST Catalog readiness
	err := pool.Retry(ctx, 60*time.Second, func() error {
		resp, err := http.Get(fmt.Sprintf("%s/v1/config", restURL))
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("iceberg rest catalog returned status %d", resp.StatusCode)
		}
		return nil
	})
	require.NoError(t, err, "iceberg rest catalog failed to become ready in time")

	// Create namespace 'analytics' in Iceberg REST Catalog
	nsBody := []byte(`{"namespace": ["analytics"]}`)
	nsReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/v1/namespaces", restURL), bytes.NewReader(nsBody))
	require.NoError(t, err)
	nsReq.Header.Set("Content-Type", "application/json")
	nsResp, err := http.DefaultClient.Do(nsReq)
	require.NoError(t, err)
	_ = nsResp.Body.Close()

	// 3. Initialize Iceberg REST Catalog client pointing to REST Catalog
	catalogConfig := &catalog.Config{
		Type:         catalog.CatalogTypeIcebergREST,
		DatabaseName: "analytics",
		URI:          restURL,
	}

	client, err := catalog.NewIcebergRESTCatalogClient(catalogConfig)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// 4. Create sample canonical Table descriptor
	idField := &model.Field{Name: "account_id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)}
	nameField := &model.Field{Name: "holder_name", Schema: model.NewPrimitiveSchema(model.TypeString, false)}
	balanceField := &model.Field{Name: "balance", Schema: model.NewDecimalSchema(15, 2, false)}
	schema := model.NewRecordSchema("accounts", []*model.Field{idField, nameField, balanceField}, false)

	table := &model.Table{
		Name:             "accounts",
		TableFormat:      model.TableFormatIceberg,
		ReadSchema:       schema,
		BasePath:         "/tmp/warehouse/accounts",
		LatestCommitTime: time.Now().UnixMilli(),
	}

	// 5. Register table to Nessie Iceberg REST Catalog
	err = client.CreateOrUpdateTable(ctx, table, nil)
	require.NoError(t, err, "failed to register Iceberg table in Nessie REST catalog")

	assert.Equal(t, catalog.CatalogTypeIcebergREST, client.CatalogType())
}
