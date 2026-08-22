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

package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/slachiewicz/polytable/pkg/formats/iceberg"
	"github.com/slachiewicz/polytable/pkg/model"
)

// IcebergRESTCatalogClient synchronizes Iceberg table metadata with standard Iceberg REST Catalogs (Polaris, Unity, Tabular, Nessie).
type IcebergRESTCatalogClient struct {
	endpoint  *restCatalogEndpoint
	namespace string
}

var _ SyncClient = (*IcebergRESTCatalogClient)(nil)

// NewIcebergRESTCatalogClient creates a new client for an Iceberg REST Catalog.
func NewIcebergRESTCatalogClient(cfg *Config) (*IcebergRESTCatalogClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.URI == "" {
		return nil, fmt.Errorf("URI is required for Iceberg REST catalog")
	}

	client, token, err := restHTTPClient(cfg, 30*time.Second)
	if err != nil {
		return nil, err
	}

	return &IcebergRESTCatalogClient{
		endpoint:  newRESTCatalogEndpoint(client, cfg.URI, token, cfg.Properties),
		namespace: cfg.DatabaseName,
	}, nil
}

// NewIcebergRESTCatalogClientWithHTTPClient creates a client using a caller-supplied HTTP client.
// This mirrors NewIcebergRESTConversionSourceWithClient in rest_conversion.go; that seam existed
// for the read side only until now, with no equivalent for the write side. It carries no
// warehouse: callers needing prefix negotiation with a warehouse go through NewIcebergRESTCatalogClient.
func NewIcebergRESTCatalogClientWithHTTPClient(client *http.Client, baseURI, namespace, authToken string) *IcebergRESTCatalogClient {
	return &IcebergRESTCatalogClient{
		endpoint:  newRESTCatalogEndpoint(client, baseURI, authToken, nil),
		namespace: namespace,
	}
}

// CatalogType returns ICEBERG_REST.
func (c *IcebergRESTCatalogClient) CatalogType() CatalogType {
	return CatalogTypeIcebergREST
}

// CreateOrUpdateTable registers or commits the Iceberg table metadata to the REST catalog.
func (c *IcebergRESTCatalogClient) CreateOrUpdateTable(ctx context.Context, table *model.Table, _ *model.Snapshot) error {
	if table == nil {
		return fmt.Errorf("table cannot be nil")
	}

	if err := c.endpoint.negotiatePrefix(ctx); err != nil {
		return fmt.Errorf("failed to negotiate Iceberg REST catalog config: %w", err)
	}
	if known, advertised := c.endpoint.writeEndpointAdvertised(); known && !advertised {
		return readOnlyCatalogError(c.endpoint.baseURI, "CreateOrUpdateTable")
	}

	schemaID := 0
	// No previous Iceberg schema exists yet -- this call registers or fully replaces the table's
	// metadata with the REST catalog -- so field ids are assigned fresh from 1 rather than kept
	// stable against a prior commit (see SchemaToIceberg's doc comment, T69).
	tableSchema, _, err := iceberg.SchemaToIceberg(table.ReadSchema, schemaID, nil, 0)
	if err != nil {
		return fmt.Errorf("failed to convert schema to iceberg: %w", err)
	}

	reqBody := map[string]any{
		"name":     table.Name,
		"location": table.BasePath,
		"schema":   tableSchema,
		"properties": map[string]string{
			"xtable_last_instant_synced": fmt.Sprintf("%d", table.LatestCommitTime),
			"xtable_source_format":       string(table.TableFormat),
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	url := c.endpoint.path("namespaces", c.namespace, "tables")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(reqBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.endpoint.setAuth(req)

	resp, err := c.endpoint.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request to iceberg rest catalog: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusConflict {
		// Table exists, issue commit updates endpoint: POST /v1/{prefix}/namespaces/{ns}/tables/{table}
		updateURL := c.endpoint.path("namespaces", c.namespace, "tables", table.Name)
		commitUpdateBody := map[string]any{
			"identifier": map[string]any{
				"namespace": []string{c.namespace},
				"name":      table.Name,
			},
			"updates": []map[string]any{
				{
					"action": "set-properties",
					"updates": map[string]string{
						"xtable_last_instant_synced": fmt.Sprintf("%d", table.LatestCommitTime),
					},
				},
			},
		}
		updBytes, _ := json.Marshal(commitUpdateBody)
		updReq, err := http.NewRequestWithContext(ctx, http.MethodPost, updateURL, bytes.NewReader(updBytes))
		if err != nil {
			return err
		}
		updReq.Header.Set("Content-Type", "application/json")
		c.endpoint.setAuth(updReq)

		updResp, err := c.endpoint.httpClient.Do(updReq)
		if err != nil {
			return fmt.Errorf("failed to update table in iceberg rest catalog: %w", err)
		}
		defer func() { _ = updResp.Body.Close() }()
		if updResp.StatusCode == http.StatusMethodNotAllowed {
			return readOnlyCatalogError(c.endpoint.baseURI, "CreateOrUpdateTable (commit update)")
		}
		if updResp.StatusCode >= 400 {
			body, _ := io.ReadAll(updResp.Body)
			return fmt.Errorf("iceberg rest catalog returned error %d: %s", updResp.StatusCode, string(body))
		}
		return nil
	}

	if resp.StatusCode == http.StatusMethodNotAllowed {
		return readOnlyCatalogError(c.endpoint.baseURI, "CreateOrUpdateTable")
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("iceberg rest catalog create returned error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DropTable removes the table registration from the REST catalog.
func (c *IcebergRESTCatalogClient) DropTable(ctx context.Context, databaseName, tableName string) error {
	if err := c.endpoint.negotiatePrefix(ctx); err != nil {
		return fmt.Errorf("failed to negotiate Iceberg REST catalog config: %w", err)
	}
	if known, advertised := c.endpoint.writeEndpointAdvertised(); known && !advertised {
		return readOnlyCatalogError(c.endpoint.baseURI, "DropTable")
	}

	ns := c.namespace
	if databaseName != "" {
		ns = databaseName
	}
	url := c.endpoint.path("namespaces", ns, "tables", tableName)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	c.endpoint.setAuth(req)

	resp, err := c.endpoint.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusMethodNotAllowed {
		return readOnlyCatalogError(c.endpoint.baseURI, "DropTable")
	}
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to drop table: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// Close is a no-op.
func (c *IcebergRESTCatalogClient) Close() error {
	return nil
}
