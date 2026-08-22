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

package io_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/slachiewicz/polytable/pkg/io"
)

func TestNewStorageForPath_SchemeRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		want    any
		wantErr string
	}{
		{name: "s3", path: "s3://bucket/table", want: &io.S3Storage{}},
		{name: "s3a", path: "s3a://bucket/table", want: &io.S3Storage{}},
		{name: "memory", path: "mem://table", want: &io.MemoryStorage{}},
		{name: "file scheme", path: "file:///tmp/table", want: &io.LocalStorage{}},
		{name: "plain absolute path", path: "/tmp/table", want: &io.LocalStorage{}},
		{name: "plain relative path", path: "data/table", want: &io.LocalStorage{}},
		{name: "hdfs", path: "hdfs://namenode/table", wantErr: `"hdfs://"`},
		{name: "https", path: "https://example.com/table", wantErr: `"https://"`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			storage, err := io.NewStorageForPath(context.Background(), tt.path)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorIs(t, err, io.ErrInvalidPath)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, storage)
				return
			}
			require.NoError(t, err)
			assert.IsType(t, tt.want, storage)
		})
	}
}

// TestNewStorageForPath_AzureRouting is separate from the table above because every Azure scheme
// has to be constructed with an explicit credential mode: the default is the Entra ID chain, which
// depends on ambient machine state, and a routing test must not.
func TestNewStorageForPath_AzureRouting(t *testing.T) {
	t.Parallel()

	// Until T51 these four schemes were refused by NewStorageForPathWithOptions, and this file
	// pinned the refusal. The pin is inverted rather than deleted: it now proves each spelling of
	// the same store reaches the Azure backend.
	paths := []string{
		"abfss://container@account.dfs.core.windows.net/table",
		"abfs://container@account.dfs.core.windows.net/table",
		"wasbs://container@account.blob.core.windows.net/table",
		"wasb://container@account.blob.core.windows.net/table",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			storage, err := io.NewStorageForPathWithOptions(context.Background(), path,
				func(o *io.Options) { o.Azure.Anonymous = true })
			require.NoError(t, err)
			assert.IsType(t, &io.AzureStorage{}, storage)
			require.NoError(t, storage.Close())
		})
	}
}

// TestNewStorageForPath_GCSRouting is separate from the table above for the same reason
// TestNewStorageForPath_AzureRouting is: the default credential mode is the Application Default
// Credentials chain, which depends on ambient machine state, and a routing test must not.
//
// Until this backend landed, gs:// was refused by NewStorageForPathWithOptions, and this file
// pinned the refusal. The pin is inverted rather than deleted, the way the abfss:// one was when
// Azure landed: it now proves gs:// reaches the GCS backend.
func TestNewStorageForPath_GCSRouting(t *testing.T) {
	t.Parallel()

	storage, err := io.NewStorageForPathWithOptions(context.Background(), "gs://bucket/table",
		func(o *io.Options) { o.GCS.AnonymousAccess = true })
	require.NoError(t, err)
	assert.IsType(t, &io.GCSStorage{}, storage)
	require.NoError(t, storage.Close())
}

func TestParseAzureURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		uri           string
		wantContainer string
		wantBlob      string
		wantHost      string
		wantScheme    string
		wantErr       string
	}{
		{
			name: "abfss with a path", uri: "abfss://fs@acct.dfs.core.windows.net/db/table/f.parquet",
			wantContainer: "fs", wantBlob: "db/table/f.parquet", wantHost: "acct.dfs.core.windows.net", wantScheme: "abfss",
		},
		{
			name:          "onelake addresses the workspace as the container",
			uri:           "abfss://myworkspace@onelake.dfs.fabric.microsoft.com/lake.Lakehouse/Tables/sales",
			wantContainer: "myworkspace", wantBlob: "lake.Lakehouse/Tables/sales",
			wantHost: "onelake.dfs.fabric.microsoft.com", wantScheme: "abfss",
		},
		{
			name: "container root has an empty blob path", uri: "abfss://fs@acct.dfs.core.windows.net/",
			wantContainer: "fs", wantBlob: "", wantHost: "acct.dfs.core.windows.net", wantScheme: "abfss",
		},
		{
			name: "no path at all", uri: "wasbs://fs@acct.blob.core.windows.net",
			wantContainer: "fs", wantBlob: "", wantHost: "acct.blob.core.windows.net", wantScheme: "wasbs",
		},
		{name: "wrong scheme", uri: "s3://bucket/key", wantErr: "must start with"},
		{name: "missing container", uri: "abfss://acct.dfs.core.windows.net/table", wantErr: "container"},
		{name: "empty container", uri: "abfss://@acct.dfs.core.windows.net/table", wantErr: "container"},
		{name: "empty host", uri: "abfss://fs@/table", wantErr: "host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			container, blob, host, scheme, err := io.ParseAzureURI(tt.uri)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorIs(t, err, io.ErrInvalidPath)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantContainer, container)
			assert.Equal(t, tt.wantBlob, blob)
			assert.Equal(t, tt.wantHost, host)
			assert.Equal(t, tt.wantScheme, scheme)
		})
	}
}

// TestRelativizePath_AzureEndpointAliases pins a defect found by converting a Snowflake Iceberg
// table on Azure and reading the result with two independent Delta readers.
//
// ADLS Gen2 serves one account on both a dfs and a blob endpoint. Snowflake writes its Iceberg
// locations against ".blob."; a caller typically addresses the table as ".dfs.". Comparing the
// hosts as written failed to see the file was under its own table, so an absolute URI was written
// into the Delta log's add.path -- and both DuckDB (delta-kernel-rs) and delta-rs resolve that
// relative to the table root, producing a path with the root prepended to an absolute URI, and fail
// with BlobNotFound. The same conversion on S3 produced a relative path, which is what showed this
// was an inconsistency rather than a deliberate choice.
func TestRelativizePath_AzureEndpointAliases(t *testing.T) {
	t.Parallel()

	const dfsBase = "abfss://c@acct.dfs.core.windows.net/tbl"
	const blobBase = "abfss://c@acct.blob.core.windows.net/tbl"

	tests := []struct {
		name string
		file string
		base string
		want string
	}{
		{"blob file under dfs base", blobBase + "/data/f.parquet", dfsBase, "data/f.parquet"},
		{"dfs file under blob base", dfsBase + "/data/f.parquet", blobBase, "data/f.parquet"},
		{"same spelling still works", dfsBase + "/data/f.parquet", dfsBase, "data/f.parquet"},
		{
			// The authority is normalised, never the path: a directory that happens to be spelled
			// like an endpoint must not be rewritten.
			name: "a path component spelled like an endpoint is untouched",
			file: dfsBase + "/.blob.core.windows.net/f.parquet",
			base: dfsBase,
			want: ".blob.core.windows.net/f.parquet",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := io.RelativizePath(tc.file, tc.base)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestRelativizePath_DifferentAccountStillRefused guards the fix from becoming too generous: two
// different accounts are two different stores whichever endpoint spells them.
func TestRelativizePath_DifferentAccountStillRefused(t *testing.T) {
	t.Parallel()

	_, err := io.RelativizePath(
		"abfss://c@other.blob.core.windows.net/tbl/data/f.parquet",
		"abfss://c@acct.dfs.core.windows.net/tbl",
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, io.ErrPathNotUnderBase)
}

// TestRelativizePath_SchemeAliasesAcrossBackends records what each backend's alias pairs actually
// do, after the Azure blob/dfs case turned out to be a real defect. Two of these were already
// correct and one was silently wrong, which is why the table is written as a record rather than
// only covering the fix.
func TestRelativizePath_SchemeAliasesAcrossBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		base    string
		want    string
		wantErr error
	}{
		// Already correct: both spellings are in uriSchemes, so TrimScheme strips either. This is
		// the pair that matters most in practice, since Hadoop and Spark write s3a:// while the AWS
		// tooling writes s3://, so a Java-written table read against an s3:// base lands here.
		{name: "s3a file under an s3 base", file: "s3a://b/tbl/data/f.parquet", base: "s3://b/tbl", want: "data/f.parquet"},
		{name: "s3 file under an s3a base", file: "s3://b/tbl/data/f.parquet", base: "s3a://b/tbl", want: "data/f.parquet"},
		{name: "wasbs file under an abfss base", file: "wasbs://c@a.blob.core.windows.net/tbl/data/f.parquet", base: "abfss://c@a.dfs.core.windows.net/tbl", want: "data/f.parquet"},

		// Silently wrong before this was fixed. An unrecognised scheme was mistaken for a relative
		// path, and path.Clean had already collapsed the "//", so "gcs://b/tbl/data/f.parquet" was
		// returned as the relative path "gcs:/b/tbl/data/f.parquet" -- neither relative nor a URI,
		// and undetectable downstream. Snowflake writes external volume locations as "gcs://", so
		// this was reachable, not theoretical.
		{name: "gcs scheme is refused, not mangled", file: "gcs://b/tbl/data/f.parquet", base: "gs://b/tbl", wantErr: io.ErrInvalidPath},
		{name: "an https object URL is refused, not mangled", file: "https://storage.googleapis.com/b/tbl/data/f.parquet", base: "gs://b/tbl", wantErr: io.ErrInvalidPath},
		{name: "an s3 virtual-hosted URL is refused, not mangled", file: "https://b.s3.eu-north-1.amazonaws.com/tbl/data/f.parquet", base: "s3://b/tbl", wantErr: io.ErrInvalidPath},

		// A genuinely relative path must still pass through untouched.
		{name: "a relative path is unaffected", file: "data/f.parquet", base: "s3://b/tbl", want: "data/f.parquet"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := io.RelativizePath(tc.file, tc.base)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
