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

package io

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// Standard sentinel storage errors.
var (
	ErrNotFound         = errors.New("file not found")
	ErrAlreadyExists    = errors.New("file already exists")
	ErrInvalidPath      = errors.New("invalid storage path")
	ErrPermissionDenied = errors.New("permission denied")
	ErrPathNotUnderBase = errors.New("path is not under the base path")
)

// uriSchemes are the URI prefixes the path helpers (JoinPath, TrimScheme, RelativizePath)
// recognize.
//
// The Azure schemes have to be here whether or not a backend exists, and that is the load-bearing
// part: RelativizePath treats a path carrying no recognized scheme as already relative, so an
// unlisted abfss:// path would be silently mangled by the very helper that exists to stop paths
// being mangled.
var uriSchemes = []string{"s3://", "s3a://", "gs://", "abfss://", "abfs://", "wasbs://", "wasb://", "mem://", "file://"}

// azureSchemes are the subset of uriSchemes the Azure backend serves. Hadoop's ABFS driver spells
// the same store four ways — TLS or not, Data Lake Storage Gen2 or the older Blob endpoint — and
// foreign metadata carries whichever the writing engine was configured with.
var azureSchemes = []string{"abfss://", "abfs://", "wasbs://", "wasb://"}

// IsAzurePath reports whether a path addresses Azure storage.
func IsAzurePath(p string) bool {
	for _, scheme := range azureSchemes {
		if strings.HasPrefix(p, scheme) {
			return true
		}
	}
	return false
}

// FileInfo represents metadata for an object or file in storage.
type FileInfo struct {
	Path    string    `json:"path"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"modTime"`
	IsDir   bool      `json:"isDir"`
}

// Storage defines the unified storage interface across Local FS, Amazon S3, Azure Data Lake
// Storage, Google Cloud Storage and Memory.
type Storage interface {
	// Read reads the entire content of the file at the specified path.
	Read(ctx context.Context, path string) ([]byte, error)

	// Write writes the given data to the specified path atomically.
	Write(ctx context.Context, path string, data []byte) error

	// List lists all files and directories matching the prefix.
	List(ctx context.Context, prefix string) ([]FileInfo, error)

	// Exists checks if an object or directory exists at path.
	Exists(ctx context.Context, path string) (bool, error)

	// Delete deletes the file or object at path.
	Delete(ctx context.Context, path string) error

	// Close releases any open network connections or resources.
	Close() error
}

// JoinPath safely joins path elements while preserving URI schemes (e.g. s3://, mem://, file://).
func JoinPath(base string, elem ...string) string {
	for _, scheme := range uriSchemes {
		if strings.HasPrefix(base, scheme) {
			trimmed := strings.TrimPrefix(base, scheme)
			parts := append([]string{trimmed}, elem...)
			joined := strings.Join(parts, "/")
			// Clean duplicate slashes but keep URI intact
			for strings.Contains(joined, "//") {
				joined = strings.ReplaceAll(joined, "//", "/")
			}
			// file:// carries an absolute path after the scheme, and dropping its leading
			// separator would turn file:///data/events into the relative file://data/events.
			if strings.HasPrefix(trimmed, "/") {
				return scheme + joined
			}
			return scheme + strings.TrimPrefix(joined, "/")
		}
	}
	parts := append([]string{base}, elem...)
	return filepath.Join(parts...)
}

// TrimScheme removes a recognized URI scheme from a path, returning the scheme and the remainder.
// An unrecognized or absent scheme yields an empty scheme and the path unchanged.
func TrimScheme(p string) (scheme, rest string) {
	for _, s := range uriSchemes {
		if strings.HasPrefix(p, s) {
			return s, strings.TrimPrefix(p, s)
		}
	}
	return "", p
}

// RelativizePath returns physicalPath expressed relative to basePath, which is what every target
// stores in its metadata so that a table survives being copied or moved.
//
// The comparison is scheme-aware: a scheme is stripped from either side before comparing, because
// formats disagree about whether to record one. An Iceberg manifest reports
// file:///data/events/f.parquet for a table whose base path is /data/events, and a plain string
// prefix does not match — the bug this exists to prevent. Stripping the scheme from both sides also
// makes s3:// and s3a:// equivalent, which is right: they name the same object store.
//
// A path that is already relative comes back unchanged. A path outside the base path is an error
// wrapping ErrPathNotUnderBase, never a silently returned absolute path: every caller has to decide
// what such a file means for its format.
func RelativizePath(physicalPath, basePath string) (string, error) {
	_, base := TrimScheme(basePath)
	scheme, file := TrimScheme(physicalPath)

	if base == "" {
		return "", fmt.Errorf("%w: base path %q has no path component", ErrInvalidPath, basePath)
	}
	if file == "" {
		return "", fmt.Errorf("%w: file path %q has no path component", ErrInvalidPath, physicalPath)
	}

	// Clean collapses duplicate separators, drops a trailing one and resolves "..", so that a base
	// path written with or without a trailing slash compares the same.
	base = path.Clean(base)
	file = path.Clean(file)

	// A path carrying a scheme this package does not recognise must be refused, not mistaken for a
	// relative one. TrimScheme reports no scheme for an unknown spelling, and the branch below then
	// returns the whole thing verbatim as though it were relative -- after path.Clean has collapsed
	// the "//" -- so "gcs://b/t/f.parquet" becomes the relative path "gcs:/b/t/f.parquet". That is
	// neither relative nor a valid URI, and nothing downstream can detect it. Reachable in
	// practice: Snowflake writes external volume locations as "gcs://", and object stores are
	// commonly addressed as "https://<bucket>.s3.<region>.amazonaws.com/..." too.
	if scheme == "" && strings.Contains(physicalPath, "://") {
		return "", fmt.Errorf("%w: %q carries a scheme this package does not recognise; recognised schemes are %s",
			ErrInvalidPath, physicalPath, strings.Join(uriSchemes, ", "))
	}

	// model.DataFile documents PhysicalPath as a fully qualified URI or a relative path. Carrying no
	// scheme and not starting at the root is what makes it the second kind, and such a path is
	// already relative to the table — with the exception of one that climbs out of it.
	if scheme == "" && !strings.HasPrefix(file, "/") {
		if file == ".." || strings.HasPrefix(file, "../") {
			return "", fmt.Errorf("%w: %q climbs out of %q", ErrPathNotUnderBase, physicalPath, basePath)
		}
		return file, nil
	}

	// One store can be addressed by more than one host spelling, and comparing the strings as
	// written then fails to see that a file sits under its own table. Normalise both sides for the
	// comparison only -- the path component is untouched, so the suffix returned below is the same
	// either way, and nothing a source deliberately wrote gets rewritten.
	base = canonicalHostForCompare(base)
	file = canonicalHostForCompare(file)

	if file == base {
		return "", fmt.Errorf("%w: %q is the base path itself", ErrPathNotUnderBase, physicalPath)
	}
	rest, found := strings.CutPrefix(file, base)
	// The match has to end on a separator, or /data/events would claim /data/events2/f.parquet.
	// A base path of "/" is the one case that already ends in one.
	if !found || (!strings.HasPrefix(rest, "/") && !strings.HasSuffix(base, "/")) {
		return "", fmt.Errorf("%w: %q is not under %q", ErrPathNotUnderBase, physicalPath, basePath)
	}
	return strings.TrimPrefix(rest, "/"), nil
}

// canonicalHostForCompare rewrites host spellings that denote one store into a single form, so that
// RelativizePath can tell a file is under its table when the two were written against different
// endpoints of the same account.
//
// ADLS Gen2 serves the same account on a dfs and a blob endpoint, and writers disagree about which
// to record: Snowflake writes Iceberg locations against ".blob." while a caller typically addresses
// the table as ".dfs.". Before this, such a file failed to relativize and an absolute URI was
// written into the Delta log, which delta-kernel-rs and delta-rs both resolve relative to the table
// root and then cannot find. Verified against both readers.
//
// Only the authority is touched, and only up to the first path separator, so a path component that
// happens to contain ".blob." is left alone.
func canonicalHostForCompare(p string) string {
	authority := p
	rest := ""
	if i := strings.IndexByte(strings.TrimPrefix(p, "/"), '/'); i >= 0 {
		off := len(p) - len(strings.TrimPrefix(p, "/"))
		authority, rest = p[:off+i], p[off+i:]
	}
	authority = strings.Replace(authority, ".blob.core.windows.net", ".dfs.core.windows.net", 1)
	// OneLake exposes the same pair, documented alongside the endpoint swap in pkg/io/azure.go.
	authority = strings.Replace(authority, "onelake.blob.fabric.microsoft.com", "onelake.dfs.fabric.microsoft.com", 1)
	return authority + rest
}

// NewStorageForPath automatically resolves and instantiates the appropriate Storage implementation for a path URI.
func NewStorageForPath(ctx context.Context, path string) (Storage, error) {
	return NewStorageForPathWithOptions(ctx, path)
}

// Options carries per-backend configuration for NewStorageForPathWithOptions.
//
// It is one struct rather than one option type per backend because the router picks the backend
// from the path scheme, which the caller generally has not inspected: a caller fills in whatever it
// was configured with and the router uses the half that applies.
type Options struct {
	// S3 configures the Amazon S3 and S3-compatible backend.
	S3 S3Options
	// Azure configures the Azure Data Lake Storage and OneLake backend.
	Azure AzureOptions
	// GCS configures the Google Cloud Storage backend.
	GCS GCSOptions
}

// NewStorageForPathWithOptions automatically resolves and instantiates Storage with optional
// backend configuration.
func NewStorageForPathWithOptions(ctx context.Context, path string, optFns ...func(*Options)) (Storage, error) {
	var opts Options
	for _, fn := range optFns {
		fn(&opts)
	}

	if strings.HasPrefix(path, "s3://") || strings.HasPrefix(path, "s3a://") {
		return NewS3Storage(ctx, func(o *S3Options) { *o = opts.S3 })
	}
	if IsAzurePath(path) {
		return NewAzureStorage(ctx, path, func(o *AzureOptions) { *o = opts.Azure })
	}
	if strings.HasPrefix(path, "gs://") {
		return NewGCSStorage(ctx, func(o *GCSOptions) { *o = opts.GCS })
	}
	if strings.HasPrefix(path, "mem://") {
		return NewMemoryStorage(), nil
	}
	// Any other URI scheme must fail here rather than fall through: an unbacked scheme such as
	// "hdfs://namenode/table" would otherwise be treated by local storage as a relative
	// directory and create a literal "hdfs:" directory on the first write.
	if scheme, _, found := strings.Cut(path, "://"); found && scheme != "file" {
		return nil, fmt.Errorf("%w: no storage backend for scheme %q (supported: s3://, s3a://, abfss://, abfs://, wasbs://, wasb://, gs://, mem://, file://, or a plain local path)",
			ErrInvalidPath, scheme+"://")
	}
	return NewLocalStorage(), nil
}
