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
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/network"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/catalog"
	"github.com/slachiewicz/polytable/pkg/model"
)

// Fixed, publicly documented bootstrap credentials for a throwaway Apache Polaris container --
// not secrets. They come verbatim from Apache Polaris's own quickstart, the same way Azurite's
// well-known devstoreaccount1 key is treated in dockertest_azurite_test.go.
const (
	polarisRealm              = "POLARIS"
	polarisRootClientID       = "root"
	polarisRootSecret         = "secret" //nolint:gosec // not a credential: fixed bootstrap secret for a throwaway container
	polarisCatalogName        = "pt_catalog"
	polarisNamespace          = "pt_ns"
	polarisClientSecretEnvVar = "POLYTABLE_TEST_POLARIS_SECRET" //nolint:gosec // env var name, not a credential
)

// polarisFetchToken exchanges clientSecret for a bearer token at catalogBaseURI's OAuth2
// client-credentials endpoint, using the raw HTTP form the Iceberg REST specification defines.
// It is used only to bootstrap the catalog and namespace before polytable's own OAuth2 code path
// (exercised separately, in the OAuth2EndToEnd subtest) is ever invoked.
func polarisFetchToken(ctx context.Context, t *testing.T, catalogBaseURI, clientID, clientSecret string) (token string, status int, body []byte) {
	t.Helper()

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)
	form.Set("scope", "PRINCIPAL_ROLE:ALL")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, catalogBaseURI+"/v1/oauth/tokens", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Polaris-Realm", polarisRealm)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "failed to reach Polaris token endpoint")
	defer func() { _ = resp.Body.Close() }()

	var respBody struct {
		AccessToken string `json:"access_token"`
	}
	raw := make([]byte, 0, 4096)
	buf := make([]byte, 4096)
	for {
		n, readErr := resp.Body.Read(buf)
		raw = append(raw, buf[:n]...)
		if readErr != nil {
			break
		}
	}
	_ = json.Unmarshal(raw, &respBody)
	return respBody.AccessToken, resp.StatusCode, raw
}

// polarisCreateCatalog registers polarisCatalogName as an INTERNAL catalog with dummy S3 storage
// config, following the verified recipe: storageType FILE is rejected outright by Polaris, and
// nothing in this suite reads or writes actual S3 data, so the dummy values are sufficient.
func polarisCreateCatalog(ctx context.Context, t *testing.T, managementBaseURI, token string) {
	t.Helper()

	body := `{"catalog":{"name":"` + polarisCatalogName + `","type":"INTERNAL",` +
		`"properties":{"default-base-location":"s3://pt-bucket/wh"},` +
		`"storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://pt-bucket/wh"],` +
		`"roleArn":"arn:aws:iam::000000000000:role/dummy"}}}`

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, managementBaseURI+"/catalogs", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Polaris-Realm", polarisRealm)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "failed to reach Polaris management API")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusCreated, resp.StatusCode, "failed to create Polaris catalog %q", polarisCatalogName)
}

// polarisCreateNamespace creates a namespace inside polarisCatalogName. polytable has no
// CreateNamespace of its own (IcebergRESTCatalogClient only creates and updates tables), so both
// the OAuth2EndToEnd and RoundTrip subtests need one to already exist.
func polarisCreateNamespace(ctx context.Context, t *testing.T, catalogBaseURI, token, namespace string) {
	t.Helper()

	body := `{"namespace":["` + namespace + `"]}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		catalogBaseURI+"/v1/"+polarisCatalogName+"/namespaces", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Polaris-Realm", polarisRealm)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "failed to reach Polaris catalog API")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode, "failed to create Polaris namespace %q", namespace)
}

// recordingTransport wraps an http.RoundTripper and records every request's URL.Path before
// delegating to it -- it does not fake or short-circuit the round trip, only observes it. This is
// the assertable seam for TestDockertest_Polaris/PrefixIsNegotiatedNotAssumed: restCatalogEndpoint
// (pkg/catalog/rest_config.go) is unexported, and NewIcebergRESTConversionSourceWithClient (the
// only constructor that accepts a caller-built *http.Client directly) carries no warehouse and so
// can never negotiate a prefix at all. Both oauth2Transport and headerTransport fall back to
// http.DefaultTransport when built with no explicit base (see pkg/catalog/oauth2.go and
// pkg/catalog/rest_auth.go), so swapping the package-level http.DefaultTransport variable for the
// duration of one subtest observes the real, production oauth2-authenticated request path without
// reaching into any unexported state.
type recordingTransport struct {
	base http.RoundTripper

	mu    sync.Mutex
	paths []string
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.paths = append(r.paths, req.URL.Path)
	r.mu.Unlock()
	return r.base.RoundTrip(req)
}

func (r *recordingTransport) recordedPaths() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.paths))
	copy(out, r.paths)
	return out
}

// TestDockertest_Polaris closes docs/improvement-plan.md T59's one unmet acceptance criterion: its
// OAuth2 client-credentials support (landed in 0576d69) was verified by hand against a live Apache
// Polaris container, but nothing pinned that by a test. Snowflake Open Catalog is Polaris -- its
// endpoint path is literally /polaris/api/catalog -- so this is also the closest thing to Snowflake
// coverage available without a Snowflake account.
func TestDockertest_Polaris(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping dockertest integration test in short mode")
	}

	ctx := context.Background()
	pool := dockertest.NewPoolT(t, "")

	// Apache Polaris takes roughly 20-40s to boot (Quarkus startup), well past dockertest's default
	// 60s Retry window, so the readiness Retry below is given 2 minutes rather than a v3-style
	// pool-wide MaxWait override.
	//
	// dockertest v4 replaces v3's server-side Expire(300)-then-Purge with RunT's automatic
	// t.Cleanup: the container is still single-use per test run (WithoutReuse), but a SIGKILL'd test
	// process now leaks it instead of the daemon reaping it on a timer -- see
	// dockertest_trino_test.go's package comment for the full v3/v4 tradeoff this tree accepted when
	// the other dockertest_*.go files were migrated to v4.
	resource := pool.RunT(t, "apache/polaris", dockertest.WithTag("latest"), dockertest.WithoutReuse(),
		dockertest.WithEnv([]string{
			"POLARIS_BOOTSTRAP_CREDENTIALS=" + polarisRealm + "," + polarisRootClientID + "," + polarisRootSecret,
			"polaris.realm-context.realms=" + polarisRealm,
		}),
		dockertest.WithPortBindings(network.PortMap{
			network.MustParsePort("8181/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: ""}},
		}))

	polarisPort := resource.GetPort("8181/tcp")
	catalogBaseURI := fmt.Sprintf("http://127.0.0.1:%s/api/catalog", polarisPort)
	managementBaseURI := fmt.Sprintf("http://127.0.0.1:%s/api/management/v1", polarisPort)

	// Readiness: GET /v1/config must answer exactly 401 -- that means the route is live and auth
	// is enforced. Any other response (including a 404, which /q/health always gives on this
	// image, or a 503 mid-startup) is not evidence of readiness and must not be treated as such.
	err := pool.Retry(ctx, 2*time.Minute, func() error {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, catalogBaseURI+"/v1/config", nil)
		if reqErr != nil {
			return reqErr
		}
		resp, getErr := http.DefaultClient.Do(req)
		if getErr != nil {
			return getErr
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusUnauthorized {
			return fmt.Errorf("polaris /v1/config answered %d, not the expected 401", resp.StatusCode)
		}
		return nil
	})
	require.NoError(t, err, "polaris failed to become ready in time")

	// Bootstrap: a token for the root principal, a catalog, and a namespace inside it. This uses
	// raw HTTP throughout, deliberately not polytable's own OAuth2 client -- that code path is the
	// subject under test in the OAuth2EndToEnd subtest below, and bootstrapping through it would
	// make that subtest prove nothing beyond "the same code worked twice."
	rootToken, tokenStatus, tokenBody := polarisFetchToken(ctx, t, catalogBaseURI, polarisRootClientID, polarisRootSecret)
	require.Equal(t, http.StatusOK, tokenStatus, "failed to bootstrap a root token: %s", tokenBody)
	require.NotEmpty(t, rootToken)

	polarisCreateCatalog(ctx, t, managementBaseURI, rootToken)
	polarisCreateNamespace(ctx, t, catalogBaseURI, rootToken, polarisNamespace)

	// 1. OAuth2 end to end: polytable authenticates with no static token supplied at all. Polaris
	// answers /v1/config with 401 unauthenticated (confirmed above), so a successful ListTables
	// here demonstrably went through the token exchange.
	t.Run("OAuth2EndToEnd", func(t *testing.T) {
		t.Setenv(polarisClientSecretEnvVar, polarisRootSecret)

		cfg := &catalog.Config{
			Type:         catalog.CatalogTypeIcebergREST,
			DatabaseName: polarisNamespace,
			URI:          catalogBaseURI,
			Properties: map[string]string{
				catalog.PropCatalogAuth:                           "oauth2",
				catalog.PropCatalogOAuth2ClientID:                 polarisRootClientID,
				catalog.PropCatalogOAuth2ClientSecretEnv:          polarisClientSecretEnvVar,
				catalog.PropCatalogScope:                          "PRINCIPAL_ROLE:ALL",
				catalog.PropCatalogWarehouse:                      polarisCatalogName,
				catalog.PropCatalogHeaderPrefix + "Polaris-Realm": polarisRealm,
			},
		}

		src, err := catalog.NewIcebergRESTConversionSource(cfg)
		require.NoError(t, err)
		defer func() { _ = src.Close() }()

		var ids []catalog.TableIdentifier
		for id, listErr := range src.ListTables(ctx, polarisNamespace, catalog.TableFilter{}) {
			require.NoError(t, listErr, "ListTables must succeed once the OAuth2 exchange has happened")
			ids = append(ids, id)
		}
		// An empty listing is still a meaningful pass here: negotiatePrefix (GET /v1/config) and
		// the tables listing are both authenticated calls against a catalog that 401s without
		// auth, so completing them at all -- regardless of how many tables exist -- proves the
		// token exchange happened.
		t.Logf("ListTables succeeded, %d table(s) visible in namespace %q", len(ids), polarisNamespace)
	})

	// 2. A wrong secret must fail, and name the authentication failure rather than silently
	// degrading to an empty listing.
	t.Run("WrongSecretFailsWithNamedAuthError", func(t *testing.T) {
		const wrongSecretEnvVar = "POLYTABLE_TEST_POLARIS_WRONG_SECRET" //nolint:gosec // env var name, not a credential
		t.Setenv(wrongSecretEnvVar, "definitely-not-the-secret")

		cfg := &catalog.Config{
			Type:         catalog.CatalogTypeIcebergREST,
			DatabaseName: polarisNamespace,
			URI:          catalogBaseURI,
			Properties: map[string]string{
				catalog.PropCatalogAuth:                           "oauth2",
				catalog.PropCatalogOAuth2ClientID:                 polarisRootClientID,
				catalog.PropCatalogOAuth2ClientSecretEnv:          wrongSecretEnvVar,
				catalog.PropCatalogScope:                          "PRINCIPAL_ROLE:ALL",
				catalog.PropCatalogWarehouse:                      polarisCatalogName,
				catalog.PropCatalogHeaderPrefix + "Polaris-Realm": polarisRealm,
			},
		}

		src, err := catalog.NewIcebergRESTConversionSource(cfg)
		require.NoError(t, err, "construction itself does not attempt the token exchange")
		defer func() { _ = src.Close() }()

		var ids []catalog.TableIdentifier
		var listErr error
		for id, e := range src.ListTables(ctx, polarisNamespace, catalog.TableFilter{}) {
			if e != nil {
				listErr = e
				break
			}
			ids = append(ids, id)
		}

		require.Error(t, listErr, "a wrong client secret must fail the listing, not silently return one")
		assert.Empty(t, ids, "no table identifiers must be yielded when authentication failed")
		// Pinned to polytable's own error framing and the OAuth2 status/body Polaris returns for
		// an unauthorized client, observed against this image: an "unauthorized_client" 401 from
		// POST /v1/oauth/tokens, wrapped by oauth2Transport.fetchToken (pkg/catalog/oauth2.go).
		// Polaris's full response text is not pinned beyond that, since the image floats at
		// :latest.
		assert.Contains(t, listErr.Error(), "oauth2: token endpoint")
		assert.Contains(t, listErr.Error(), "401")
		assert.Contains(t, listErr.Error(), "unauthorized_client")
	})

	// 3. The prefix Polaris advertises (pt_catalog, from GET /v1/config's overrides.prefix) must
	// be negotiated and actually used in the request path, not assumed to be empty. This drives
	// the real, production oauth2-authenticated code path (NewIcebergRESTConversionSource) and
	// observes its outbound requests via recordingTransport -- see that type's doc comment for why
	// this is the seam available, given restCatalogEndpoint's fields are unexported and the
	// with-client constructor cannot negotiate a warehouse-scoped prefix at all.
	t.Run("PrefixIsNegotiatedNotAssumed", func(t *testing.T) {
		t.Setenv(polarisClientSecretEnvVar, polarisRootSecret)

		origTransport := http.DefaultTransport
		rec := &recordingTransport{base: origTransport}
		http.DefaultTransport = rec
		defer func() { http.DefaultTransport = origTransport }()

		cfg := &catalog.Config{
			Type:         catalog.CatalogTypeIcebergREST,
			DatabaseName: polarisNamespace,
			URI:          catalogBaseURI,
			Properties: map[string]string{
				catalog.PropCatalogAuth:                           "oauth2",
				catalog.PropCatalogOAuth2ClientID:                 polarisRootClientID,
				catalog.PropCatalogOAuth2ClientSecretEnv:          polarisClientSecretEnvVar,
				catalog.PropCatalogScope:                          "PRINCIPAL_ROLE:ALL",
				catalog.PropCatalogWarehouse:                      polarisCatalogName,
				catalog.PropCatalogHeaderPrefix + "Polaris-Realm": polarisRealm,
			},
		}

		src, err := catalog.NewIcebergRESTConversionSource(cfg)
		require.NoError(t, err)
		defer func() { _ = src.Close() }()

		for id, listErr := range src.ListTables(ctx, polarisNamespace, catalog.TableFilter{}) {
			require.NoError(t, listErr)
			_ = id
		}

		paths := rec.recordedPaths()
		require.NotEmpty(t, paths, "expected at least one outbound request to have been recorded")

		wantPrefixed := "/api/catalog/v1/" + polarisCatalogName + "/namespaces/" + polarisNamespace + "/tables"
		wantUnprefixed := "/api/catalog/v1/namespaces/" + polarisNamespace + "/tables"

		var sawPrefixed, sawUnprefixed bool
		for _, p := range paths {
			if p == wantPrefixed {
				sawPrefixed = true
			}
			if p == wantUnprefixed {
				sawUnprefixed = true
			}
		}
		assert.True(t, sawPrefixed, "expected a request against the negotiated prefix %q; recorded paths: %v", wantPrefixed, paths)
		assert.False(t, sawUnprefixed, "an unprefixed path must never be built once negotiation has resolved a non-empty prefix; recorded paths: %v", paths)
	})

	// 4. Round trip through the catalog: create a namespace (already done, shared above), register
	// a table through polytable's own IcebergRESTCatalogClient.CreateOrUpdateTable, and read it back
	// through IcebergRESTConversionSource.GetSourceTable.
	t.Run("RoundTripThroughCatalog", func(t *testing.T) {
		t.Setenv(polarisClientSecretEnvVar, polarisRootSecret)

		cfg := &catalog.Config{
			Type:         catalog.CatalogTypeIcebergREST,
			DatabaseName: polarisNamespace,
			URI:          catalogBaseURI,
			Properties: map[string]string{
				catalog.PropCatalogAuth:                           "oauth2",
				catalog.PropCatalogOAuth2ClientID:                 polarisRootClientID,
				catalog.PropCatalogOAuth2ClientSecretEnv:          polarisClientSecretEnvVar,
				catalog.PropCatalogScope:                          "PRINCIPAL_ROLE:ALL",
				catalog.PropCatalogWarehouse:                      polarisCatalogName,
				catalog.PropCatalogHeaderPrefix + "Polaris-Realm": polarisRealm,
			},
		}

		writeClient, err := catalog.NewIcebergRESTCatalogClient(cfg)
		require.NoError(t, err)
		defer func() { _ = writeClient.Close() }()

		const tableName = "roundtrip_table"
		table := &model.Table{
			Name:        tableName,
			TableFormat: model.TableFormatDelta,
			BasePath:    "s3://pt-bucket/wh/" + polarisNamespace + "/" + tableName,
			ReadSchema: model.NewRecordSchema(tableName, []*model.Field{
				{Name: "id", Schema: model.NewPrimitiveSchema(model.TypeLong, false)},
			}, false),
			LatestCommitTime: time.Now().UnixMilli(),
		}

		createErr := writeClient.CreateOrUpdateTable(ctx, table, nil)
		if createErr != nil {
			// Confirmed empirically against this exact image (apache/polaris:latest): Polaris's
			// INTERNAL catalog tries to resolve an AWS region to write the initial table metadata
			// file to the dummy s3://pt-bucket/wh location, and refusing to guess one region --
			// there is no live account behind this container -- fails with a 500
			// SdkClientException naming the AWS region provider chain, not a 4xx naming a request
			// problem. That is exactly the storage-credential gap this task's instructions
			// anticipated, so it is skipped with the observed reason rather than weakened into a
			// vacuous assertion. Any other kind of failure (a 401, a 404, a schema error) is not
			// this gap and must fail the test.
			if strings.Contains(createErr.Error(), "SdkClientException") || strings.Contains(createErr.Error(), "region") {
				t.Skipf("Polaris refuses to create a table without a resolvable AWS region for its dummy S3 storage config, "+
					"confirmed against apache/polaris:latest; skipping the round trip rather than weakening it: %v", createErr)
			}
			require.NoError(t, createErr)
		}

		readSource, err := catalog.NewIcebergRESTConversionSource(cfg)
		require.NoError(t, err)
		defer func() { _ = readSource.Close() }()

		got, err := readSource.GetSourceTable(ctx, catalog.TableIdentifier{Database: polarisNamespace, Table: tableName})
		require.NoError(t, err)
		assert.Equal(t, table.BasePath, got.BasePath)
		assert.Equal(t, model.TableFormatIceberg, got.Format)
	})

	// 5. namespace-separator: Polaris advertises "%1F" (the ASCII unit separator, URL-encoded) in
	// GET /v1/config's overrides, and docs/improvement-plan.md T61 records that polytable's
	// restConfigResponse (pkg/catalog/rest_config.go) does not read that field at all -- it
	// hardcodes the same separator as nestedNamespaceSeparator, which happens to be correct only
	// because no catalog observed so far advertises a different one. This subtest documents that
	// gap so it is visible, and is not itself the regression test T61 asks for: it must not fail
	// the build over a known, filed limitation.
	t.Run("NamespaceSeparatorIsAdvertisedButIgnored_T61", func(t *testing.T) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			catalogBaseURI+"/v1/config?warehouse="+polarisCatalogName, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+rootToken)
		req.Header.Set("Polaris-Realm", polarisRealm)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var parsed struct {
			Overrides map[string]any `json:"overrides"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&parsed))

		separator, ok := parsed.Overrides["namespace-separator"]
		require.True(t, ok, "expected Polaris to advertise overrides.namespace-separator")
		assert.Equal(t, "%1F", separator)

		t.Logf("Polaris advertises namespace-separator %q in GET /v1/config, which polytable's "+
			"restConfigResponse (pkg/catalog/rest_config.go) does not read -- it uses a hardcoded "+
			"nestedNamespaceSeparator instead. See docs/improvement-plan.md T61. This is filed and "+
			"deliberately not failed here; this subtest exists so the gap stays visible and so it "+
			"can be flipped to a real regression test once T61 is fixed.", separator)
	})
}
