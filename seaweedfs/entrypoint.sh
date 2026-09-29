#!/bin/sh
# Render the SeaweedFS S3 identity config from env vars so no credentials live in git,
# then start an all-in-one SeaweedFS server (master + volume + filer + S3 gateway).
set -eu

: "${S3_ACCESS_KEY:?S3_ACCESS_KEY is required}"
: "${S3_SECRET_KEY:?S3_SECRET_KEY is required}"

mkdir -p /etc/seaweedfs
cat > /etc/seaweedfs/s3.json <<EOF
{
  "identities": [
    {
      "name": "pipeline",
      "credentials": [{ "accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}" }],
      "actions": ["Admin", "Read", "List", "Tagging", "Write"]
    }
  ]
}
EOF

exec weed server \
  -dir=/data \
  -ip=seaweedfs \
  -ip.bind=0.0.0.0 \
  -volume.max=0 \
  -master.volumeSizeLimitMB=256 \
  -filer \
  -s3 \
  -s3.port=8333 \
  -s3.config=/etc/seaweedfs/s3.json
