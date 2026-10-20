# IAM policy examples

Least-privilege policies for the roles that use the SDK. Replace `ACCOUNT`, `REGION`, the bucket names, prefixes and tenant id. Grant one statement per tenant, or use one role per tenant if your platform allows it.

The SDK never creates buckets, tables or keys.

## Ingestion (`BronzeIngestionClient`)

Writes Bronze objects and quarantine objects for tenant `acme-001`. With `PiiProtection.kms_envelope`, it also obtains data keys from the tenant's KMS key.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "WriteBronzeAndQuarantine",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:AbortMultipartUpload"],
      "Resource": [
        "arn:aws:s3:::acme-lake/bronze/tenant_id=acme-001/*",
        "arn:aws:s3:::acme-lake/quarantine/tenant_id=acme-001/*",
        "arn:aws:s3:::acme-lake/bronze/_manifests/tenant_id=acme-001/*"
      ]
    },
    {
      "Sid": "TenantDataKeys",
      "Effect": "Allow",
      "Action": "kms:GenerateDataKey",
      "Resource": "arn:aws:kms:REGION:ACCOUNT:key/KEY-ID",
      "Condition": {
        "StringEquals": {
          "kms:EncryptionContext:tenant_id": "acme-001",
          "kms:EncryptionContext:purpose": "datalake-medallion-sdk/pii"
        }
      }
    }
  ]
}
```

The last resource is only needed with `manifests=True` (commit manifests live under `_manifests/` at the root of the Bronze location). These paths are for the default `partition_order="tenant_first"`. With `data_type_first` the tenant partition comes after `data_type=`, so use `bronze/data_type=*/tenant_id=acme-001/*`.

## Reader (`LakeReader`)

Lists and reads tenant `acme-001`'s data in every layer. Add the location of each shared Iceberg table the tenant reads; the reader still filters those tables to the tenant.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListTenantPrefixes",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::acme-lake",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "bronze/tenant_id=acme-001/*",
            "bronze/_manifests/tenant_id=acme-001/*",
            "silver/tenant_id=acme-001/*",
            "gold/tenant_id=acme-001/*"
          ]
        }
      }
    },
    {
      "Sid": "ReadTenantObjects",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::acme-lake/bronze/tenant_id=acme-001/*",
        "arn:aws:s3:::acme-lake/bronze/_manifests/tenant_id=acme-001/*",
        "arn:aws:s3:::acme-lake/silver/tenant_id=acme-001/*",
        "arn:aws:s3:::acme-lake/gold/tenant_id=acme-001/*"
      ]
    }
  ]
}
```

Iceberg tables named in a catalog also need read access to it, for AWS Glue `glue:GetDatabase` and `glue:GetTable` on the catalog, the database and the table.

## Silver and Gold writer (`LakeWriter`)

Writes Iceberg tables through the AWS Glue catalog. `glue:CreateDatabase` and `glue:CreateTable` are only needed by `create_table`; grant them to a provisioning role instead if tables are created ahead of time.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CatalogCommits",
      "Effect": "Allow",
      "Action": ["glue:GetDatabase", "glue:GetTable", "glue:UpdateTable", "glue:CreateTable", "glue:CreateDatabase"],
      "Resource": [
        "arn:aws:glue:REGION:ACCOUNT:catalog",
        "arn:aws:glue:REGION:ACCOUNT:database/silver",
        "arn:aws:glue:REGION:ACCOUNT:table/silver/*"
      ]
    },
    {
      "Sid": "TableFiles",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": [
        "arn:aws:s3:::shared-lake/warehouse/silver.db/*",
        "arn:aws:s3:::acme-lake/silver/tenant_id=acme-001/*"
      ]
    }
  ]
}
```

The first S3 resource is the location of tables shared by tenants, the second the tenant's own tables. In a shared table, tenant isolation is enforced by the writer and the reader (every row carries `tenant_id`; every write and read is scoped to it), not by IAM, because the table's files belong to every tenant. Where tenants must be separated by IAM as well, give each one its own tables (`TableRef.iceberg("silver", name)` without `identifier`), which live below the tenant's location.

## Maintenance (`LakeMaintenance`)

Compaction and snapshot expiry commit like the writer. Orphan file removal also lists and deletes files under the table location.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CatalogCommits",
      "Effect": "Allow",
      "Action": ["glue:GetTable", "glue:UpdateTable"],
      "Resource": [
        "arn:aws:glue:REGION:ACCOUNT:catalog",
        "arn:aws:glue:REGION:ACCOUNT:database/silver",
        "arn:aws:glue:REGION:ACCOUNT:table/silver/*"
      ]
    },
    {
      "Sid": "TableFiles",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::shared-lake/warehouse/silver.db/*"
    },
    {
      "Sid": "ListTableFiles",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::shared-lake",
      "Condition": {"StringLike": {"s3:prefix": ["warehouse/silver.db/*"]}}
    }
  ]
}
```

## Decryptor (`PiiDecryptor`)

Recovers the data keys of tenant `acme-001` to read encrypted PII. Grant it only to the jobs that must see clear values.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DecryptTenantDataKeys",
      "Effect": "Allow",
      "Action": "kms:Decrypt",
      "Resource": "arn:aws:kms:REGION:ACCOUNT:key/KEY-ID",
      "Condition": {
        "StringEquals": {
          "kms:EncryptionContext:tenant_id": "acme-001",
          "kms:EncryptionContext:purpose": "datalake-medallion-sdk/pii"
        }
      }
    }
  ]
}
```

Enable automatic rotation on the KMS key. Data keys issued before a rotation stay decryptable.
