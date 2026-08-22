# Snowflake-written Iceberg fixture

`orders_s3.1qPtC7st` was written by **Snowflake** on 2026-08-22, not by any tool in this
repository, as the worked example in [`docs/snowflake.md`](../../../../docs/snowflake.md) —
a `CREATE ICEBERG TABLE` on an external volume backed by S3. The `_delta_log/` directory
alongside it is polytable's own output from syncing that table to Delta, kept so the input
and the result stay together.

It is archived here because the sandbox it came from is gone: the bucket
(`polytable-snowflake-iceberg-798855026998`) and the IAM role Snowflake assumed were deleted
during teardown, and reproducing this table means rebuilding the whole external-volume and
IAM trust handshake against a live Snowflake account.

Two things to know before using it.

**The paths are absolute and point at the deleted bucket.** `00001-*.metadata.json` carries
`s3://polytable-snowflake-iceberg-798855026998/...` in its `location`, its
`metadata-log` entry and its snapshot's `manifest-list`. Rewrite those three to a local path
before any reader will open it. The Parquet data file and the Avro manifests need no change.

**The `.1qPtC7st` suffix on the table directory is Snowflake's, not ours.** Snowflake appends
a generated suffix to the `BASE_LOCATION` you give it, which is why the real path has to be
read back with `SYSTEM$GET_ICEBERG_TABLE_INFORMATION` rather than reconstructed. The suffix is
preserved here as evidence of that behavior.
