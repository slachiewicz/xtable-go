<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Snowflake Horizon Catalog

Snowflake's Iceberg REST catalog speaks the same
[Iceberg REST catalog protocol](iceberg-rest-catalog.md) as Nessie, Apache Polaris, Unity Catalog,
and R2 Data Catalog, because it is Apache Polaris running inside Snowflake. This page is the
reference and the setup recipe together, the same shape as
[Cloudflare R2 and R2 Data Catalog](cloudflare.md), because the surface here is one connection and
one catalog entry — not enough to justify a separate test-environment page the way
[Azure](azure.md) and [Azure test environment](azure-test-environment.md) split theirs.

## Status

This page was rewritten from a live Snowflake account on 2026-08-22, replacing an earlier version
written from documentation before anything had connected. Verified against that account:

- Token exchange, catalog listing, and table resolution all work end to end, using `auth: oauth2`
  (`pkg/catalog/oauth2.go`), the same code path documented for
  [Apache Polaris](iceberg-rest-catalog.md) and covered by a live-Polaris dockertest suite
  (`test/dockertest_polaris_test.go`, T59 in `docs/improvement-plan.md`).
- Reaching Snowflake surfaced one real defect, since fixed: polytable sent an OAuth2 `client_id`
  that Snowflake rejects. See [Authentication is a token exchange](#authentication-is-a-token-exchange)
  below.
- **Reading a Snowflake-managed table's data does not work.** A table created without an external
  volume stores its data in a bucket only Snowflake holds credentials for. See
  [Tables resolve, but reading their data does not work yet](#tables-resolve-but-reading-their-data-does-not-work-yet).
- **Reading a table on an external volume works today, with no code changes.** When the table's
  data lands in storage you control, polytable reads it with the same credentials you'd give any
  other S3, Azure, or GCS path. See
  [External volumes put the data in your own bucket](#external-volumes-put-the-data-in-your-own-bucket)
  below, verified end to end against a live account.

Not verified: key-pair/JWT authentication, External OAuth, the Azure and GCS external volume
shapes (documented but not run — see
[Azure and GCS external volumes](#azure-and-gcs-external-volumes-documented-not-run)), and anything
beyond a single account's behavior — Snowflake's rollout, region, or edition could differ.

## Open Catalog is closed to new customers

If you've read Snowflake's older material, you'll come looking for a **Snowflake Open Catalog**
account. Don't. Snowflake's own documentation states it plainly: customers who haven't previously
created an Open Catalog account can't sign up for their first one. It directs new customers to
**Horizon Catalog** instead. This is confirmed in the Snowsight UI: the account-creation dropdown
offers only "Create Organization Account", with no Open Catalog option.

## Horizon Catalog serves the same endpoint

Snowflake integrated Polaris into Horizon Catalog, and it serves the same path Open Catalog always
did:

```
https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog
```

Same protocol, same routes. Everything documented for
[Sync to an Iceberg REST catalog](iceberg-rest-catalog.md) applies unchanged; this page covers only
what's specific to Snowflake's deployment of it.

## A network policy is required before anything else

Before you touch authentication, attach a network policy to the user or account you'll use.
Without one, Snowflake refuses to issue a programmatic access token anywhere — including the SQL
API — with:

```
390432 Fail : Network policy is required
```

The error doesn't mention the catalog at all, so if nothing below works, check this first. Create a
policy and attach it:

```sql
CREATE NETWORK POLICY <policy_name> ALLOWED_IP_LIST = ('0.0.0.0/0');
ALTER USER <user> SET NETWORK_POLICY = <policy_name>;
```

`ALLOWED_IP_LIST = ('0.0.0.0/0')` works, and it is **wide open** — say so plainly rather than
burying it. Narrow it to your own address if you can, and unset it once you're done:

```sql
ALTER USER <user> UNSET NETWORK_POLICY;
```

## Authentication is a token exchange

A Snowflake programmatic access token (PAT) is the *credential*, not the bearer token polytable
sends on each request. Exchange it for a short-lived access token at the catalog's own OAuth2
endpoint:

```
POST https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog/v1/oauth/tokens

grant_type=client_credentials
scope=session:role:<ROLE>
client_secret=<the PAT>
```

**Send no `client_id`.** Snowflake rejects a request that carries one, and the rejection is
`invalid_scope` — an error naming a field that isn't actually the problem. polytable required
`clientId` until commit `c4c4863` made it optional and, when empty, omits the field from the
request entirely rather than sending it blank. A reader on an older build will hit exactly this
`invalid_scope` error and should upgrade rather than debug `scope`.

## Configure polytable

Set on the catalog entry's `properties`:

- **`auth: oauth2`**
- **`clientSecretEnv`**: the name of an environment variable holding the PAT — never the PAT
  itself. An unset or empty variable is a named error, the same discipline
  [`AzureOptions.AccountKeyEnv`](azure.md#authentication) follows: a dataset config gets committed
  to git and logged, so the config points at a variable name, never a value.
- **`scope`**: `session:role:<ROLE>`.
- **No `clientId`.** Leave the property out entirely.

`oauth2TokenEndpoint` is optional and, left unset, defaults to `<uri>/v1/oauth/tokens`, which is
already the address shown above — there's no need to override it for Snowflake.

## Worked configuration

```yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: s3://<bucket>/tables/people
    tableName: people
    catalogs:
      - type: ICEBERG_REST
        uri: https://<org>-<account>.snowflakecomputing.com/polaris/api/catalog
        databaseName: <schema>
        properties:
          warehouse: <database>
          auth: oauth2
          clientSecretEnv: SNOWFLAKE_PAT
          scope: session:role:<role>
```

```shell
export SNOWFLAKE_PAT=<the_programmatic_access_token>
./bin/polytable sync --datasetConfig snowflake.yaml
```

`<org>-<account>` is your account identifier as Snowflake shows it in the account's connection
details — read it from there rather than guessing its shape.

## The warehouse property is the database, not a warehouse

**`properties.warehouse` is the Snowflake database name.** This is unlike every other catalog
documented in this repository:

- AWS Glue's Iceberg REST endpoint takes the account id (see [Amazon S3 and AWS Glue](aws.md)).
- AWS S3 Tables takes the table bucket's ARN.
- Cloudflare R2 Data Catalog takes `<account_id>_<bucket>` (see
  [Cloudflare R2 and R2 Data Catalog](cloudflare.md)).
- A self-hosted Apache Polaris catalog takes the catalog's own name.
- Snowflake takes the database name — not a virtual warehouse, not an account, not an ARN.

Watch for a naming collision this creates with polytable's own config: `databaseName` on the
catalog entry is the Iceberg *namespace* inside the catalog, the same rule as every other REST
catalog documented here (see
[Resolve a source table from the catalog](iceberg-rest-catalog.md#resolve-a-source-table-from-the-catalog)).
In Snowflake terms that namespace is a schema living inside the database you named in `warehouse`.
`warehouse` is the database; `databaseName` is the schema underneath it. The two config keys and
the two Snowflake concepts do not line up one-to-one, so don't read `databaseName` as "the
Snowflake database."

## What /v1/config returns

`GET /v1/config?warehouse=<database>` against a live account returned:

- `overrides.prefix` set to the database name.
- `defaults` carrying only an empty `default-base-location`.
- Real write routes alongside the read routes.
- **No `namespace-separator`.** Self-hosted Polaris advertises one (`%1F` by default, see T61 in
  `docs/improvement-plan.md`); Snowflake's deployment does not. So Snowflake and self-hosted Polaris
  are the same software but not identical in every field it advertises — treat that as the rule,
  not the exception.

## Rehearse against a local Polaris container

Because Snowflake's catalog is Polaris, a local Polaris container exercises the same client code
path polytable uses against Snowflake, at no cost:

```shell
docker run -p 8181:8181 -p 8182:8182 apache/polaris:latest
```

Apache Polaris's own [quickstart](https://polaris.apache.org/releases/1.0.0/getting-started/quickstart/)
covers bootstrapping a principal and granting it `CATALOG_MANAGE_CONTENT` on a catalog — the same
privilege a Snowflake connection needs. `pkg/catalog/oauth2.go`'s own comments note that a
self-hosted Polaris container also needed a `Polaris-Realm` header, attachable through the
`header.<Name>` config property (`PropCatalogHeaderPrefix`, documented in
[Sync to an Iceberg REST catalog](iceberg-rest-catalog.md)); nothing observed against Snowflake so
far indicates it needs an equivalent.

`test/dockertest_polaris_test.go` already drives this rehearsal in CI against a real Polaris
container, including a wrong-secret case that must fail named rather than silently degrade — that
suite is what caught the `client_id` defect before this page did.

## Tables resolve, but reading their data does not work yet

A Snowflake-managed Iceberg table — created with `CREATE ICEBERG TABLE`, no external volume needed
— was found by `ListTables` and resolved by `GetSourceTable` as `format=ICEBERG`, with a base path
in Snowflake's own managed bucket. The catalog path works end to end.

Reading that table's data does not. The base path points into a bucket only Snowflake holds
credentials for; no customer can be given bucket-wide access to it. Running `polytable inspect`
against that path fails with:

```
failed to list s3 objects ...: api error PermanentRedirect: The bucket you are attempting to access
must be addressed using the specified endpoint.
```

Snowflake does vend scoped credentials for exactly this: requesting the table with the header
`X-Iceberg-Access-Delegation: vended-credentials` returns a `storage-credentials` block with an S3
access key, secret, session token, expiry, and `client.region`. **polytable does not consume vended
credentials yet.** This is tracked as **T64** in `docs/improvement-plan.md`. Until it lands, you can
list and resolve Snowflake-managed Iceberg tables through the catalog, but you cannot read their
data through polytable. This is not a configuration problem to work around — there is no bucket-wide
credential to configure instead.

## External volumes put the data in your own bucket

The limitation above only applies to a table Snowflake stores in its own managed bucket. An
**external volume** changes where the data lands: a Snowflake Iceberg table created on an
external volume writes its data files and metadata into a bucket or container in *your* cloud
account, not Snowflake's. Because the storage is yours, polytable reads it with ordinary
credentials — the same S3, Azure, or GCS configuration documented in
[Amazon S3 and AWS Glue](aws.md), [Azure Data Lake Storage Gen2](azure.md), and
[Cloud Storage](cloud-storage.md). No vended-credential support is needed for this path; T64 in
`docs/improvement-plan.md` remains open only for the managed-bucket case above.

This is the difference between polytable *listing* a Snowflake table and polytable *converting*
one. It's also a way to produce a genuine Iceberg fixture in any of the three clouds using
Snowflake as the writer, independent of whatever else creates test data for this project.

The S3 path below was performed end to end against a live Snowflake account and a real AWS
account on 2026-08-22. The Azure and Google shapes that follow it were not run; they're marked
as such.

### Worked example: a Snowflake Iceberg table on S3

1. Create an S3 bucket in the account you'll read from. It doesn't need to exist before the next
   step — an external volume can name a bucket that doesn't exist yet.
2. Create the external volume, naming the bucket and an IAM role that doesn't have to exist yet
   either:

   ```sql
   CREATE EXTERNAL VOLUME <volume_name>
     STORAGE_LOCATIONS =
       (
         (
           NAME = '<location_name>'
           STORAGE_PROVIDER = 'S3'
           STORAGE_BASE_URL = 's3://<bucket>/<prefix>/'
           STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<account_id>:role/<role_name>'
         )
       );
   ```

   This is a chicken-and-egg handshake, and it surprises people the first time: Snowflake accepts
   a role ARN for a role you haven't created yet, because the next two steps use the volume's own
   output to write that role's trust policy.

3. Read back the identity Snowflake will assume the role as:

   ```sql
   DESC EXTERNAL VOLUME <volume_name>;
   ```

   Look for `STORAGE_AWS_IAM_USER_ARN` (an IAM user in **Snowflake's own** AWS account, not yours)
   and `STORAGE_AWS_EXTERNAL_ID`. Both values come back inside a JSON blob in the
   `STORAGE_LOCATION_1` property's row, not as separate top-level rows, which makes them awkward
   to pull out with a script rather than by reading the output.

4. Create the IAM role named in step 2, with a trust policy that allows only that Snowflake IAM
   user to assume it, conditioned on the external ID:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": { "AWS": "<storage_aws_iam_user_arn>" },
         "Action": "sts:AssumeRole",
         "Condition": {
           "StringEquals": { "sts:ExternalId": "<storage_aws_external_id>" }
         }
       }
     ]
   }
   ```

   Attach a permissions policy granting the S3 object actions on `<bucket>/<prefix>/*`, plus
   `s3:ListBucket` and `s3:GetBucketLocation` on the bucket itself.

5. Verify the volume before creating anything on it:

   ```sql
   SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('<volume_name>');
   ```

   This returns a JSON result with `"success": true` and per-check results, including a
   `writeResult`. **Run this before creating a table on the volume.** It's the difference between
   finding out the role or policy is wrong right now and finding out when a table creation fails
   with a less specific error. Allow a few seconds for IAM propagation before the first attempt —
   an immediate check after creating the role can still fail while the trust policy propagates.

6. Create the table on the volume:

   ```sql
   CREATE ICEBERG TABLE <db>.<schema>.<table>
     CATALOG = 'SNOWFLAKE'
     EXTERNAL_VOLUME = '<volume_name>'
     BASE_LOCATION = '<prefix>/';
   ```

7. Find where the data actually landed:

   ```sql
   SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('<db>.<schema>.<table>');
   ```

   The `metadataLocation` in the result is **not** `<prefix>/` as given in `BASE_LOCATION`.
   Snowflake appends a generated suffix, for example `<prefix>/<random_suffix>/`. A reader who
   assumes the base location they gave is the location Snowflake used will look in the wrong
   place — always read this back rather than reconstructing the path from the `CREATE` statement.

### Reading it back with polytable

With the bucket location from step 7, polytable reads the table directly, using the reader's own
credentials the same way it would for any other S3-backed Iceberg table:

```shell
./bin/polytable inspect --basePath s3://<bucket>/<prefix>/<random_suffix> --format ICEBERG \
  --storage-region <region>
```

This returned the correct schema and the correct data file count against the table created above.
One thing to expect: **Snowflake writes column names in upper case** (`ID`, `AMOUNT`, `REGION`),
which is worth knowing if you're coming from Delta tables, where lower-cased names are the norm.

### Two things Snowflake's Iceberg output does differently

**Snowflake writes no `metadata/version-hint.text`.** polytable doesn't need one — it resolves
the current metadata version by listing the metadata directory and taking the highest version it
finds, the same mechanism documented in [What /v1/config returns](#what-v1config-returns) and in
T39 of `docs/improvement-plan.md` — so it reads a Snowflake-written table without complaint.
DuckDB's `iceberg_scan`, by contrast, refuses one outright:

```
No version was provided and no version-hint could be found, globbing the filesystem to locate the
latest version is disabled by default as this is considered unsafe and could result in reading
uncommitted data.
```

That refusal is a real safety measure, not pedantry — DuckDB is declining to guess at which
metadata file is current rather than silently picking a possibly-stale or uncommitted one. It has
an escape hatch, `SET unsafe_enable_version_guessing = true;`, and the name is the warning. Knowing
both facts places polytable correctly: it is the more permissive of the two readers here, not the
more careful one.

**Snowflake writes `format-version: 2`** in the table metadata. Worth recording since Iceberg v3
is arriving; nothing here says more than what was observed on this one table.

### Cross-checked with DuckDB

The same table was read independently through DuckDB's `iceberg_scan`, which is worth doing
whenever you want more than "polytable's own count agrees with itself." Snowflake reported six
rows for the table; DuckDB's `iceberg_scan` over the same S3 path independently reported the same
six rows, the same sum, and the same distinct-region count.

```sql
SET unsafe_enable_version_guessing = true;

CREATE SECRET (
  TYPE S3,
  KEY_ID '<access_key_id>',
  SECRET '<secret_access_key>',
  SESSION_TOKEN '<session_token>',
  REGION '<region>'
);

SELECT * FROM iceberg_scan('s3://<bucket>/<prefix>/<random_suffix>');
```

If you're using temporary AWS credentials (an `ASIA...` access key rather than an `AKIA...` one),
`SESSION_TOKEN` is required in `CREATE SECRET` along with the key and secret. Leaving it out
doesn't fail with a missing-token error — it fails with a confusing "Invalid Access Key", which
sends you looking at the key and secret instead of the field you actually omitted.

### Azure and GCS external volumes (documented, not run)

Only the S3 path above was performed. The shapes below come from Snowflake's own documentation,
not from a live run — treat them as a starting point to verify against your account, not as
copy-paste-ready recipes the way the S3 steps are.

For Azure, an external volume names a container instead of a bucket and takes a tenant ID instead
of a role ARN:

```sql
CREATE EXTERNAL VOLUME <volume_name>
  STORAGE_LOCATIONS =
    (
      (
        NAME = '<location_name>'
        STORAGE_PROVIDER = 'AZURE'
        STORAGE_BASE_URL = 'azure://<account>.blob.core.windows.net/<container>/<prefix>/'
        AZURE_TENANT_ID = '<tenant_id>'
      )
    );
```

Where the S3 flow authorizes an IAM role, Azure requires a consent step: `DESC EXTERNAL VOLUME`
returns a Snowflake service principal (an application it registered in your Azure AD tenant), and
an Azure AD administrator must grant that principal access to the container before
`SYSTEM$VERIFY_EXTERNAL_VOLUME` can succeed. See Snowflake's
[Azure external volume documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-azure)
for the exact consent flow, since it involves an Azure-side approval this run never exercised.

For Google Cloud, the provider is `'GCS'`:

```sql
CREATE EXTERNAL VOLUME <volume_name>
  STORAGE_LOCATIONS =
    (
      (
        NAME = '<location_name>'
        STORAGE_PROVIDER = 'GCS'
        STORAGE_BASE_URL = 'gcs://<bucket>/<prefix>/'
      )
    );
```

Here, `DESC EXTERNAL VOLUME` returns a **Snowflake-generated service account**; grant it the
appropriate role (for example, object admin) on the bucket, then run
`SYSTEM$VERIFY_EXTERNAL_VOLUME` the same way as the S3 flow. See Snowflake's
[GCS external volume documentation](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-gcs)
for the exact role name and any additional steps, since this run never exercised the GCS path
either.

Whichever cloud the external volume points at, the pattern is the same: Snowflake writes into
storage the reader controls, and polytable's existing S3, Azure, and GCS backends read it with no
Snowflake-specific code. That also makes Snowflake, on any of the three clouds, a way to produce
genuine Iceberg fixtures for this project without any other engine in the loop.

## Key-pair and external OAuth are not implemented

Snowflake also supports key-pair (JWT) authentication and External OAuth. Key-pair signs a JWT and
posts it as `client_secret` to the same `/v1/oauth/tokens` endpoint with the same
`grant_type=client_credentials` — so supporting it would mean adding a JWT signer to the existing
OAuth2 code path, not a new auth mode. Neither is built.

## Teardown

- Unset the network policy if you widened it for testing: `ALTER USER <user> UNSET NETWORK_POLICY;`.
- Drop the network policy itself if it was created only for this test:
  `DROP NETWORK POLICY <policy_name>;`.
- Revoke or rotate the programmatic access token from Snowsight if it was created only for this
  test.
- Drop anything the S3 worked example created on the Snowflake side, in this order:
  `DROP ICEBERG TABLE <db>.<schema>.<table>;` then `DROP EXTERNAL VOLUME <volume_name>;`. Dropping
  the volume does not remove the data files it wrote into your bucket.
- **The AWS side of the worked example is not a Snowflake resource and survives every step above.**
  The external volume's bucket holds the table's data and metadata, and the IAM role it assumes is
  a standing trust relationship with Snowflake's AWS account:
  ```shell
  aws s3 rm "s3://<bucket>" --recursive
  aws s3 rb "s3://<bucket>"
  aws iam delete-role-policy --role-name <role_name> --policy-name <inline_policy_name>
  aws iam delete-role --role-name <role_name>
  ```
  The role carries an **inline** policy, so it needs `delete-role-policy`; the `detach-role-policy`
  shape in [AWS test environment](aws-test-environment.md#teardown) applies to managed policies and
  fails here. Deleting the role breaks the external volume immediately, so drop the volume first if
  you want the Snowflake objects to tear down cleanly.

## Troubleshooting

- **`390432 Fail : Network policy is required`.** This blocks every programmatic access token,
  including the SQL API — it is not specific to the catalog. See
  [A network policy is required before anything else](#a-network-policy-is-required-before-anything-else).
- **`invalid_scope` from the token endpoint.** Snowflake rejects a client-credentials request that
  carries a `client_id`. Confirm the running polytable build is at or past commit `c4c4863`, which
  omits `clientId` from the request when unset; an older build always sent it.
- **`PermanentRedirect` when inspecting or reading a resolved table's data.** Expected today for a
  Snowflake-managed table — see
  [Tables resolve, but reading their data does not work yet](#tables-resolve-but-reading-their-data-does-not-work-yet).
  It is not a credentials or endpoint mistake to fix in your own config.
- **A dataset config's `clientSecretEnv`-named variable is unset at sync time.** polytable refuses
  to build the client and names both the property and the missing variable, rather than falling
  through to an unauthenticated request.
- **A table creation on an external volume fails, or fails later when queried.** Run
  `SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('<volume_name>')` before creating anything on the volume —
  see [Worked example: a Snowflake Iceberg table on S3](#worked-example-a-snowflake-iceberg-table-on-s3).
  A verification run immediately after creating the IAM role can still fail while the trust policy
  propagates; wait a few seconds and retry before assuming the policy itself is wrong.
- **`polytable inspect` looks in the wrong place for a table on an external volume.** The data
  isn't at the plain `BASE_LOCATION` you gave `CREATE ICEBERG TABLE`; Snowflake appends a generated
  suffix. Read the real path back with
  `SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('<db>.<schema>.<table>')` rather than reconstructing
  it.
- **DuckDB's `iceberg_scan` refuses a Snowflake-written table with "no version-hint could be
  found".** Expected — Snowflake never writes `metadata/version-hint.text`. Set
  `unsafe_enable_version_guessing = true` in DuckDB. polytable needs no equivalent setting; see
  [Two things Snowflake's Iceberg output does differently](#two-things-snowflakes-iceberg-output-does-differently).
- **DuckDB's `CREATE SECRET` fails with "Invalid Access Key" against a temporary credential.** The
  access key and secret are probably fine; the missing field is `SESSION_TOKEN`, required whenever
  the access key starts with `ASIA` rather than `AKIA`.

## What's next

- [External volumes put the data in your own bucket](#external-volumes-put-the-data-in-your-own-bucket)
  above for the path that lets polytable read a Snowflake Iceberg table's data today, without T64.
- [Sync to an Iceberg REST catalog](iceberg-rest-catalog.md) for the complete `auth` / `token` /
  `warehouse` / `header.<Name>` property reference shared by every REST catalog polytable talks to.
- [Cloudflare R2 and R2 Data Catalog](cloudflare.md) for the closest page to this one in shape.
- [Query a synced table](query-engines.md#snowflake) for the reverse, unrelated path: reading a
  polytable-written Iceberg table from Snowflake SQL through an external volume. That path writes
  through this catalog's protocol at neither end, and it is Snowflake reading polytable's output
  rather than the other way around.
- [Features and limitations](features-and-limitations.md) for the honest, dated summary of what is
  and is not verified across the whole project.
- `docs/improvement-plan.md`, task **T64**, for consuming catalog-vended storage credentials — the
  gap that currently blocks reading a Snowflake-managed table's data.
