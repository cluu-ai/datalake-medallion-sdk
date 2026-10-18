# IAM policy examples

Least-privilege policies for the three roles that use the SDK. Replace `ACCOUNT`, `REGION`, the bucket names, prefixes and tenant id. Grant one statement per tenant, or use one role per tenant if your platform allows it.

The SDK never creates buckets, tables or keys.

## Writer (`BronzeIngestionClient`)

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
        "arn:aws:s3:::acme-lake/quarantine/tenant_id=acme-001/*"
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

These paths are for the default `partition_order="tenant_first"`. With `data_type_first` the tenant partition comes after `data_type=`, so use `bronze/data_type=*/tenant_id=acme-001/*`.

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
        "arn:aws:s3:::acme-lake/silver/tenant_id=acme-001/*",
        "arn:aws:s3:::acme-lake/gold/tenant_id=acme-001/*"
      ]
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
