#!/usr/bin/env bash
# Runs inside LocalStack when it is ready: creates the development bucket.
set -euo pipefail

awslocal s3 mb s3://sdk-dev-bucket || true
awslocal s3api put-bucket-versioning --bucket sdk-dev-bucket \
  --versioning-configuration Status=Enabled
echo "LocalStack ready: s3://sdk-dev-bucket"
