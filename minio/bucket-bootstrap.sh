#!/usr/bin/env bash
set -euo pipefail
mc alias set minio $MC_HOST_minio
mc mb -p minio/$S3_BUCKET_RAW || true
mc mb -p minio/$S3_BUCKET_FEATURES || true
mc anonymous set download minio/$S3_BUCKET_FEATURES || true
